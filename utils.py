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
NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.fdn.fr"
]

def fetch_x_urls(account: str):
    account = account.lstrip('@').lower()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for base in NITTER_INSTANCES:
        try:
            url = f"{base}/{account}"
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if len(response.text) < 5000:
                continue  # blocked page

            soup = BeautifulSoup(response.text, "html.parser")
            urls = []

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if f"/{account}/status/" in href:
                    clean = href.split("#")[0]
                    full_url = f"https://x.com{clean}"
                    if full_url not in urls:
                        urls.append(full_url)

            if urls:
                return urls[:POST_LIMIT]

        except Exception as e:
            print(f"Nitter fail {base}: {e}")

    return []

# ===================== MAIN LOGIC =====================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@').lower()

    # ALWAYS fetch when requested
    if platform == "x":
        new_urls = fetch_x_urls(account)
    else:
        return []

    if not new_urls:
        return get_recent_urls(platform, account)

    for url in new_urls:
        save_url(platform, account, url)

    return new_urls