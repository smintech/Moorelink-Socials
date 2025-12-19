# utils.py - Standalone version (no app.py dependency)
import os
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor

# ===================== CONFIG =====================
DB_URL = os.getenv("DATABASE_URL")
CACHE_HOURS = 24
POST_LIMIT = 5

# ===================== DB CONNECTION =====================
def get_db():
    """Connect to Postgres"""
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

# ===================== HASH & SAVE =====================
def generate_url_hash(account: str, url: str) -> str:
    key = f"{account.lower()}:{url}"
    return hashlib.sha256(key.encode()).hexdigest()

def save_url(platform: str, account: str, url: str):
    conn = get_db()
    cur = conn.cursor()

    post_id = generate_url_hash(account, url)

    cur.execute("""
        INSERT INTO social_posts (id, platform, account_name, post_url, fetched_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET fetched_at = NOW()
    """, (post_id, platform.lower(), account.lower(), url))

    conn.commit()
    cur.close()
    conn.close()

def get_recent_urls(platform: str, account: str) -> List[str]:
    conn = get_db()
    cur = conn.cursor()

    time_limit = datetime.utcnow() - timedelta(hours=CACHE_HOURS)

    cur.execute("""
        SELECT post_url
        FROM social_posts
        WHERE platform = %s 
          AND account_name = %s 
          AND fetched_at >= %s
        ORDER BY fetched_at DESC
        LIMIT %s
    """, (platform.lower(), account.lower(), time_limit, POST_LIMIT))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [row[0] for row in rows]

# ===================== FETCHER =====================
def fetch_x_urls(account: str) -> List[str]:
    account = account.lstrip('@')
    urls = []

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(f"https://x.com/{account}", headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for tweet in soup.find_all("article", {"data-testid": "tweet"}):
            link = tweet.find("a", href=True)
            if link and "/status/" in link["href"]:
                full_url = f"https://x.com{link['href']}"
                urls.append(full_url)

        print(f"Fetched {len(urls)} tweet URLs from @{account}")

    except Exception as e:
        print(f"URL fetch error for @{account}: {e}")

    return urls[:POST_LIMIT]

# ===================== MAIN LOGIC =====================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@')

    cached_urls = get_recent_urls(platform, account)
    if cached_urls:
        return cached_urls

    new_urls = fetch_x_urls(account)

    if not new_urls:
        return []

    for url in new_urls:
        save_url(platform, account, url)

    fresh_urls = get_recent_urls(platform, account)
    return fresh_urls