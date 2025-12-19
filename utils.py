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
        return [row["post_url"] for row in rows]  # ✅ Use dict key

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

# ===================== PREVIEW FETCHER =====================
def fetch_preview(url: str) -> dict:
    """
    Fetch Open Graph / Twitter card info for an X post.
    Returns: {"title": str, "description": str, "image": str}
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, timeout=8, headers=headers)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Open Graph / Twitter Card tags
        title_tag = (
            soup.find("meta", property="og:title") or
            soup.find("meta", property="twitter:title") or
            soup.find("title")
        )
        desc_tag = (
            soup.find("meta", property="og:description") or
            soup.find("meta", property="twitter:description") or
            soup.find("meta", name="description")
        )
        image_tag = (
            soup.find("meta", property="og:image") or
            soup.find("meta", property="twitter:image") or
            soup.find("meta", property="twitter:image:src")
        )

        title = title_tag["content"].strip() if title_tag and title_tag.get("content") else "X Post"
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""
        image = image_tag["content"].strip() if image_tag and image_tag.get("content") else ""

        return {
            "title": title,
            "description": description,
            "image": image
        }

    except Exception as e:
        print(f"Preview fetch failed for {url}: {e}")
        return {
            "title": "X Post",
            "description": "",
            "image": ""
        }

# ===================== PREVIEW CACHE =====================
PREVIEW_CACHE = {}  # Simple in-memory cache (RAM) - reset when bot restart
PREVIEW_CACHE_HOURS = 10  # Cache preview for 6 hours

def fetch_preview(url: str) -> dict:
    """
    Fetch Open Graph info with caching
    """
    global PREVIEW_CACHE

    # Check cache first
    if url in PREVIEW_CACHE:
        cached_time, data = PREVIEW_CACHE[url]
        if datetime.utcnow() - cached_time < timedelta(hours=PREVIEW_CACHE_HOURS):
            print(f"Preview cache hit for {url}")
            return data

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, timeout=8, headers=headers)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        title_tag = soup.find("meta", property="og:title") or soup.find("meta", property="twitter:title") or soup.find("title")
        desc_tag = soup.find("meta", property="og:description") or soup.find("meta", property="twitter:description") or soup.find("meta", name="description")
        image_tag = soup.find("meta", property="og:image") or soup.find("meta", property="twitter:image") or soup.find("meta", property="twitter:image:src")

        title = title_tag["content"].strip() if title_tag and title_tag.get("content") else "X Post"
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""
        image = image_tag["content"].strip() if image_tag and image_tag.get("content") else ""

        result = {
            "title": title,
            "description": description,
            "image": image
        }

        # Save to cache
        PREVIEW_CACHE[url] = (datetime.utcnow(), result)
        print(f"Preview fetched and cached for {url}")

        return result

    except Exception as e:
        print(f"Preview fetch failed for {url}: {e}")
        return {
            "title": "X Post",
            "description": "",
            "image": ""
        }