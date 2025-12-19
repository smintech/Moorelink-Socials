# utils.py - Standalone version (no app.py dependency)
import os
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import time

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

def get_recent_urls(platform: str, account: str) -> list:
    conn = get_db()
    cur = conn.cursor()
    time_limit = datetime.utcnow() - timedelta(hours=CACHE_HOURS)

    try:
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
        return [row["post_url"] for row in rows]

    finally:
        cur.close()
        conn.close()

# ===================== FETCHER =====================
NITTER_INSTANCES = [
    "https://xcancel.com",          # Top one right now, high uptime
    "https://nitter.net",           # Official, back strong
    "https://nitter.poast.org",
    "https://nitter.space",
    "https://nuku.trabun.org",
    "https://lightbrd.com",
    "https://nitter.privacyredirect.com"
]

def fetch_x_urls(account: str):
    account = account.lstrip('@').lower()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    for base in NITTER_INSTANCES:
        try:
            url = f"{base}/{account}"
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()

            if len(resp.text) < 5000:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            urls = []
            for item in soup.select("div.timeline-item"):
                link = item.select_one("a.tweet-link")
                if link and "/status/" in link["href"]:
                    clean = link["href"].split("#")[0]
                    full_url = f"https://x.com{clean}"
                    if full_url not in urls:
                        urls.append(full_url)

            if urls:
                print(f"Fetched {len(urls)} tweets from {base} @{account}")
                return urls[:POST_LIMIT]

        except requests.RequestException as e:
            print(f"Nitter fail {base}: {e}")
            time.sleep(2)
            continue  # try next mirror

    print(f"No Nitter mirrors available for @{account}")
    return []

# ===================== INSTAGRAM FETCHER =====================
IG_MIRRORS = [
    "https://imginn.com",               # Most reliable right now
    "https://pixnoy.com",               # Strong, new pixwox
    "https://greatfon.com",             # Good fallback
    "https://insta-stories-viewer.com"  # Last resort
]

def fetch_ig_urls(account: str) -> List[str]:
    account = account.lstrip('@').lower()
    profile_fallback = f"https://www.instagram.com/{account}/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    for base in IG_MIRRORS:
        try:
            profile_url = f"{base}/{account}"
            resp = requests.get(profile_url, headers=headers, timeout=12)
            resp.raise_for_status()

            if len(resp.text) < 8000:  # Too small = blocked or empty
                print(f"Page too small from {base} @{account}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            urls = []

            # Different mirrors use different structures
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                
                if href.startswith("/"):
                    href = base + href
                    
                if any(k in href for k in ("/p/", "/reel/", "/tv/")):
                    parts = [p for p in href.split("/") if p]
                    
                    for key in ("p", "reel", "tv"):
                        if key in parts:
                            idx = parts.index(key) + 1
                            if idx < len(parts):
                                shortcode = parts[idx]
                                if key == "reel":
                                    original_url = f"https://www.instagram.com/reel/{shortcode}/"
                                else:
                                    original_url = f"https://www.instagram.com/p/{shortcode}/"
                                    
                                if original_url not in urls:
                                    urls.append(original_url)

            if urls:
                print(f"Fetched {len(urls)} IG posts from {base} @{account}")
                return urls[:POST_LIMIT]

        except requests.RequestException as e:
            print(f"IG mirror fail {base}: {e}")
            time.sleep(5)
            continue

    print(f"No working IG mirror for @{account}")
    return [profile_fallback]

# ===================== MAIN LOGIC =====================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@').lower()

    if platform == "x":
        new_urls = fetch_x_urls(account)
    elif platform == "ig":
        new_urls = fetch_ig_urls(account)
    else:
        return []

    if not new_urls:
        return get_recent_urls(platform, account)

    # Save new ones
    for url in new_urls:
        save_url(platform, account, url)

    return new_urls

# ===================== PREVIEW FETCHER =====================