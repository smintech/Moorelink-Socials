# utils.py - Standalone version (no app.py dependency)
import os
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import instaloader

# ===================== CONFIG =====================
DB_URL = os.getenv("DATABASE_URL")                # existing social_posts DB
TG_DB_URL = os.getenv("USERS_DATABASE_URL")  # new separate DB for tg_users
CACHE_HOURS = 24
POST_LIMIT = 5

# ===================== DB CONNECTIONS =====================
def get_db():
    """Connect to Postgres used for social_posts (existing DB)."""
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def get_tg_db():
    """Connect to a separate Postgres DB used solely for tg_users."""
    if not TG_DB_URL:
        raise RuntimeError("TG_DATABASE_URL or TG_DB_URL not set")
    return psycopg2.connect(TG_DB_URL, cursor_factory=RealDictCursor)

# ===================== OPTIONAL: INIT TABLES =====================
def init_tg_db():
    """Create tg_users table in the separate TG DB if it doesn't exist."""
    conn = get_tg_db()
    cur = conn.cursor()
    # Postgres-compatible schema based on your provided table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tg_users (
        telegram_id BIGINT UNIQUE NOT NULL,
        first_name TEXT,
        is_active INTEGER DEFAULT 1,     -- 1 = active, 0 = inactive
        is_banned INTEGER DEFAULT 0,     -- 1 = banned
        request_count INTEGER DEFAULT 0,
        last_request_at TIMESTAMP,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

def init_social_posts_table_if_needed():
    """(Optional) Create social_posts table in the main DB if you want the cache table auto-created."""
    if not DB_URL:
        # skip if main DB not configured
        return
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS social_posts (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        account_name TEXT NOT NULL,
        post_url TEXT NOT NULL,
        fetched_at TIMESTAMP NOT NULL
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

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

# ===================== TELEGRAM USER HELPERS (separate DB) =====================
def add_or_update_tg_user(telegram_id: int, first_name: str) -> None:
    """Insert or update a Telegram user in the separate tg_users DB."""
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tg_users (telegram_id, first_name)
        VALUES (%s, %s)
        ON CONFLICT (telegram_id)
        DO UPDATE SET first_name = EXCLUDED.first_name
    """, (telegram_id, first_name))
    conn.commit()
    cur.close()
    conn.close()

def ban_tg_user(telegram_id: int) -> None:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("UPDATE tg_users SET is_banned = 1 WHERE telegram_id = %s", (telegram_id,))
    conn.commit()
    cur.close()
    conn.close()

def unban_tg_user(telegram_id: int) -> None:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("UPDATE tg_users SET is_banned = 0 WHERE telegram_id = %s", (telegram_id,))
    conn.commit()
    cur.close()
    conn.close()

def set_tg_user_active(telegram_id: int, active: bool) -> None:
    val = 1 if active else 0
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("UPDATE tg_users SET is_active = %s WHERE telegram_id = %s", (val, telegram_id))
    conn.commit()
    cur.close()
    conn.close()

def increment_tg_request_count(telegram_id: int) -> None:
    """Increment request_count and set last_request_at for auditing / rate-limiting."""
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tg_users
        SET request_count = COALESCE(request_count, 0) + 1,
            last_request_at = NOW()
        WHERE telegram_id = %s
    """, (telegram_id,))
    # If the user doesn't exist yet, create them with defaults (no first_name)
    if cur.rowcount == 0:
        cur.execute("""
            INSERT INTO tg_users (telegram_id, request_count, last_request_at)
            VALUES (%s, 1, NOW())
            ON CONFLICT (telegram_id) DO NOTHING
        """, (telegram_id,))
    conn.commit()
    cur.close()
    conn.close()

def get_tg_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tg_users WHERE telegram_id = %s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None

def list_active_tg_users(limit: Optional[int] = 100) -> List[Dict[str, Any]]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT telegram_id, first_name, is_active, is_banned, request_count, last_request_at, joined_at
        FROM tg_users
        WHERE is_active = 1
        ORDER BY joined_at DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

# ===================== FETCHERS =====================
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
def fetch_ig_urls(account: str) -> list:
    account = account.lstrip('@').lower()
    posts = []

    try:
        L = instaloader.Instaloader()
        profile = instaloader.Profile.from_username(L.context, account)

        for i, post in enumerate(profile.get_posts()):
            if i >= POST_LIMIT:
                break

            media_url = post.video_url if post.is_video else post.url

            posts.append({
                "url": f"https://www.instagram.com/p/{post.shortcode}/",
                "caption": post.caption or "",
                "media_url": media_url,
                "is_video": post.is_video
            })

        print(f"Fetched {len(posts)} IG posts from @{account}")

    except Exception as e:
        print(f"IG fetch error: {e}")

    return posts

# ===================== MAIN LOGIC =====================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    """Main function: DB cache → fresh fetch"""
    account = account.lstrip('@').lower()

    # 1. Try DB cache
    cached_urls = get_recent_urls(platform, account)
    if cached_urls:
        return cached_urls

    # 2. No cache → fetch fresh
    new_urls = []

    if platform == "x":
        new_urls = fetch_x_urls(account)
    elif platform == "ig":
        new_urls = fetch_ig_urls(account)
    else:
        return []

    if not new_urls:
        return []

    # Save to DB
    for url in new_urls:
        save_url(platform, account, url)

    # Get fresh from DB
    fresh_urls = get_recent_urls(platform, account)
    return fresh_urls

# ===================== INIT TG DB ON IMPORT =====================
# This will create the tg_users table in the TG DB automatically at import.
# Comment out if you prefer to run initialization elsewhere.
try:
    init_tg_db()
except Exception as e:
    # don't crash on import if TG DB not configured; surface a helpful message in logs
    print(f"[utils] init_tg_db() skipped or failed: {e}")
