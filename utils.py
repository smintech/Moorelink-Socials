# utils.py
import os
import hashlib
import requests
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import instaloader

# ================ CONFIG ================
DB_URL = os.getenv("DATABASE_URL")                       # main cache DB
TG_DB_URL = os.getenv("USERS_DATABASE_URL")   # separate TG DB
CACHE_HOURS = 24
POST_LIMIT = 5

# ================ DB CONNECTIONS ============
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def get_tg_db():
    if not TG_DB_URL:
        raise RuntimeError("TG_DATABASE_URL or TG_DB_URL not set")
    return psycopg2.connect(TG_DB_URL, cursor_factory=RealDictCursor)

# ================ INIT TABLES ================
def init_tg_db():
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tg_users (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE NOT NULL,
        first_name TEXT,
        is_active INTEGER DEFAULT 1,
        is_banned INTEGER DEFAULT 0,
        request_count INTEGER DEFAULT 0,
        last_request_at TIMESTAMP,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS saved_accounts (
        id SERIAL PRIMARY KEY,
        owner_telegram_id BIGINT NOT NULL,
        platform TEXT NOT NULL,            -- 'x' or 'ig'
        account_name TEXT NOT NULL,
        label TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(owner_telegram_id, platform, account_name)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

def init_social_posts_table_if_needed():
    if not DB_URL:
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

# ================ FETCH/CACHE HELPERS ============
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

# ================ EXTERNAL FETCHERS (X + IG) ============
NITTER_INSTANCES = [
    "https://xcancel.com",
    "https://nitter.net",
    "https://nitter.poast.org",
    "https://nitter.space",
    "https://nuku.trabun.org",
    "https://lightbrd.com",
    "https://nitter.privacyredirect.com"
]

def fetch_x_urls(account: str) -> List[str]:
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
            if len(resp.text) < 4000:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            urls = []
            for item in soup.select("div.timeline-item"):
                link = item.select_one("a.tweet-link")
                if link and "/status/" in link.get("href", ""):
                    clean = link["href"].split("#")[0]
                    full_url = f"https://x.com{clean}"
                    if full_url not in urls:
                        urls.append(full_url)
            if urls:
                return urls[:POST_LIMIT]
        except Exception as e:
            # try next mirror
            time.sleep(1)
            continue
    return []

def fetch_ig_urls(account: str) -> List[Dict[str, Any]]:
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
    except Exception:
        # IG fetch problems happen often; return empty
        pass
    return posts

def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@').lower()
    cached = get_recent_urls(platform, account)
    if cached:
        return cached
    new = []
    if platform == "x":
        new = fetch_x_urls(account)
    elif platform == "ig":
        # for consistent return type to callers, store IG as list of post URLs in cache (but bot expects dicts)
        new_ig = fetch_ig_urls(account)
        # save IG post URLs too
        for p in new_ig:
            save_url("ig", account, p["url"])
        return [p["url"] for p in new_ig]
    else:
        return []
    # save X urls
    for u in new:
        save_url("x", account, u)
    return new

# ================ TG USER HELPERS (tg DB) ============
def add_or_update_tg_user(telegram_id: int, first_name: str) -> None:
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
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tg_users
        SET request_count = COALESCE(request_count, 0) + 1,
            last_request_at = NOW()
        WHERE telegram_id = %s
    """, (telegram_id,))
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

def list_active_tg_users(limit: int = 100) -> List[Dict[str, Any]]:
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

def list_all_tg_users(limit: int = 1000) -> List[Dict[str, Any]]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT telegram_id, first_name, is_active, is_banned, request_count, last_request_at, joined_at
        FROM tg_users
        ORDER BY joined_at DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

# ================ SAVED ACCOUNTS HELPERS ============
def save_user_account(owner_telegram_id: int, platform: str, account_name: str, label: Optional[str]=None) -> Dict[str, Any]:
    platform = platform.lower()
    account_name = account_name.lstrip('@').lower()
    conn = get_tg_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO saved_accounts (owner_telegram_id, platform, account_name, label)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (owner_telegram_id, platform, account_name) DO UPDATE
            SET label = COALESCE(EXCLUDED.label, saved_accounts.label)
            RETURNING *
        """, (owner_telegram_id, platform, account_name, label))
        row = cur.fetchone()
        conn.commit()
        return dict(row) if row else {}
    finally:
        cur.close()
        conn.close()

def list_saved_accounts(owner_telegram_id: int) -> List[Dict[str, Any]]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, owner_telegram_id, platform, account_name, label, created_at
        FROM saved_accounts
        WHERE owner_telegram_id = %s
        ORDER BY created_at DESC
    """, (owner_telegram_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

def get_saved_account(owner_telegram_id: int, saved_id: int) -> Optional[Dict[str, Any]]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, owner_telegram_id, platform, account_name, label, created_at
        FROM saved_accounts
        WHERE owner_telegram_id = %s AND id = %s
    """, (owner_telegram_id, saved_id))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None

def remove_saved_account(owner_telegram_id: int, saved_id: int) -> bool:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM saved_accounts
        WHERE owner_telegram_id = %s AND id = %s
    """, (owner_telegram_id, saved_id))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return deleted > 0

def count_saved_accounts(owner_telegram_id: int) -> int:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) as cnt FROM saved_accounts WHERE owner_telegram_id = %s", (owner_telegram_id,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    return int(r["cnt"]) if r else 0

def update_saved_account_label(owner_telegram_id: int, saved_id: int, new_label: str) -> bool:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE saved_accounts
        SET label = %s
        WHERE owner_telegram_id = %s AND id = %s
    """, (new_label, owner_telegram_id, saved_id))
    ok = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return ok > 0

# ================ INIT ON IMPORT ================
try:
    init_tg_db()
except Exception as e:
    print(f"[utils] init_tg_db skipped or failed: {e}")