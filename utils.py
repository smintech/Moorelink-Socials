# utils.py
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Optional
from bs4 import BeautifulSoup
from app import get_db

# ===================== CONFIG =====================
CACHE_DURATION = timedelta(minutes=30)
REQUEST_LIMIT_PER_ACCOUNT = 1  # per 30 mins

# In-memory cache: { "account": {"last_fetch": datetime, "urls": list} }
_cache = {}

def _cache_key(account: str) -> str:
    return account.lower()

# ===================== HASH =====================
def generate_url_hash(account: str, url: str) -> str:
    key = f"{account.lower()}|{url}"
    return hashlib.sha256(key.encode()).hexdigest()

# ===================== DB =====================
def save_urls(account: str, urls: list):
    conn = get_db()
    cur = conn.cursor()
    
    for url in urls:
        url_hash = generate_url_hash(account, url)
        cur.execute("""
            INSERT INTO tweet_urls (hash, account_name, url, fetched_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (hash) DO UPDATE
            SET fetched_at = NOW()
        """, (url_hash, account.lower(), url))
    
    conn.commit()
    cur.close()
    conn.close()

def get_cached_urls(account: str) -> Optional[List[str]]:
    conn = get_db()
    cur = conn.cursor()
    
    time_limit = datetime.utcnow() - timedelta(hours=24)
    
    cur.execute("""
        SELECT url FROM tweet_urls
        WHERE account_name = %s AND fetched_at >= %s
        ORDER BY fetched_at DESC
        LIMIT 10
    """, (account.lower(), time_limit))
    
    urls = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    return urls if urls else None

# ===================== PUBLIC HTML FETCHER =====================
def fetch_tweet_urls(account: str) -> List[str]:
    """Fetch public tweet URLs from profile page (no login)"""
    account = account.lstrip('@').lower()
    url = f"https://x.com/{account}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch @{account}: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find tweet links (data-testid="tweet" or status links)
        tweet_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith(f'/{account}/status/'):
                full_url = f"https://x.com{href}"
                tweet_links.append(full_url)

        # Dedup and limit
        unique_urls = list(dict.fromkeys(tweet_links))[:10]
        print(f"Extracted {len(unique_urls)} tweet URLs for @{account}")

        return unique_urls

    except Exception as e:
        print(f"Fetch error for @{account}: {e}")
        return []

# ===================== MAIN FETCH LOGIC =====================
def get_latest_tweet_urls(account: str) -> List[str]:
    account = account.lstrip('@').lower()

    # 1. In-memory cache
    key = _cache_key(account)
    if key in _cache:
        entry = _cache[key]
        if datetime.utcnow() - entry["last_fetch"] < CACHE_DURATION:
            return entry["urls"]

    # 2. DB cache
    db_urls = get_cached_urls(account)
    if db_urls:
        _cache[key] = {"last_fetch": datetime.utcnow(), "urls": db_urls}
        return db_urls

    # 3. Fresh fetch
    fresh_urls = fetch_tweet_urls(account)
    if fresh_urls:
        save_urls(account, fresh_urls)
        _cache[key] = {"last_fetch": datetime.utcnow(), "urls": fresh_urls}
        return fresh_urls

    return []