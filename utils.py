# utils.py - On-demand public URL extraction for X (uses your existing table)
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List
from bs4 import BeautifulSoup
from app import get_db

# ===================== CONFIG =====================
CACHE_HOURS = 24
POST_LIMIT = 5

# ===================== HASH & ID =====================
def generate_url_hash(account: str, url: str) -> str:
    """Generate unique hash for URL deduplication"""
    key = f"{account.lower()}:{url}"
    return hashlib.sha256(key.encode()).hexdigest()

def save_url(platform: str, account: str, url: str):
    """Save URL to your existing social_posts table (upsert)"""
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
    """Fetch recent URLs from your existing table"""
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

# ===================== PUBLIC URL FETCHER =====================
def fetch_x_urls(account: str) -> List[str]:
    """Fetch public tweet URLs from X profile page (no login, no API)"""
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

        # Find tweet links (X uses data-testid="tweet" for tweets)
        for tweet in soup.find_all("article", {"data-testid": "tweet"}):
            link = tweet.find("a", href=True)
            if link and "/status/" in link["href"]:
                full_url = f"https://x.com{link['href']}"
                urls.append(full_url)

        print(f"Fetched {len(urls)} tweet URLs from @{account}")

    except Exception as e:
        print(f"URL fetch error for @{account}: {e}")

    return urls[:POST_LIMIT]  # Limit to avoid overload

# ===================== MAIN FETCH LOGIC =====================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    """Main function: DB cache → fresh fetch"""
    account = account.lstrip('@')

    # 1. Try DB cache
    cached_urls = get_recent_urls(platform, account)
    if cached_urls:
        return cached_urls

    # 2. No cache → fetch fresh URLs
    new_urls = fetch_x_urls(account)

    if not new_urls:
        return []

    # Save to your existing table
    for url in new_urls:
        save_url(platform, account, url)

    # Get fresh from DB
    fresh_urls = get_recent_urls(platform, account)
    return fresh_urls