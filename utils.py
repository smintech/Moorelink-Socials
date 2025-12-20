import os
import logging
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
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        
        # tg_users table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            first_name TEXT,
            is_active INTEGER DEFAULT 1,
            is_banned INTEGER DEFAULT 0,
            request_count INTEGER DEFAULT 0,
            last_request_at TIMESTAMP,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            invite_count INTEGER DEFAULT 0,
            is_admin INTEGER DEFAULT 0
        );
        """)
        
        # saved_accounts table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_accounts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL CHECK (platform IN ('x', 'ig')),
            account_name TEXT NOT NULL,
            label TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_telegram_id, platform, account_name)
        );
        """)
        
        # tg_rate_limits table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_rate_limits (
            telegram_id BIGINT PRIMARY KEY,
            minute_count INTEGER DEFAULT 0,
            hour_count INTEGER DEFAULT 0,
            day_count INTEGER DEFAULT 0,
            minute_reset TIMESTAMP,
            hour_reset TIMESTAMP,
            day_reset TIMESTAMP
        );
        """)
        
        # Foreign key - safe for older PostgreSQL versions (no "IF NOT EXISTS" for constraints)
        try:
            cur.execute("""
            ALTER TABLE saved_accounts 
            ADD CONSTRAINT fk_saved_owner 
            FOREIGN KEY (owner_telegram_id) 
            REFERENCES tg_users(telegram_id) 
            ON DELETE CASCADE;
            """)
            print("[DB] Foreign key constraint added.")
        except Exception as fk_e:
            # Ignore if constraint already exists (PostgreSQL raises error if duplicate)
            if "already exists" in str(fk_e).lower() or "duplicate" in str(fk_e).lower():
                print("[DB] Foreign key constraint already exists.")
            else:
                print(f"[DB] Could not add foreign key (non-critical): {fk_e}")
        
        conn.commit()
        print("[DB] All tables created/verified successfully.")
        logging.info("Database tables initialized successfully.")
        
    except Exception as e:
        print(f"[DB ERROR] Failed to initialize tables: {e}")
        logging.error(f"Database initialization failed: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

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

# ================ BADGE AND INVITE HELPERS ================

BADGE_LEVELS = [
    {"name": "Basic", "emoji": "🪪", "invites_needed": 0, "save_slots": 5, "limits": {"min": 2, "hour": 15, "day": 40}},
    {"name": "Bronze", "emoji": "🥉", "invites_needed": 5, "save_slots": 7, "limits": {"min": 3, "hour": 30, "day": 80}},
    {"name": "Silver", "emoji": "🥈", "invites_needed": 15, "save_slots": 10, "limits": {"min": 5, "hour": 60, "day": 150}},
    {"name": "Gold", "emoji": "🥇", "invites_needed": 40, "save_slots": 15, "limits": {"min": 8, "hour": 120, "day": 300}},
    {"name": "Diamond", "emoji": "💎", "invites_needed": 100, "save_slots": float('inf'), "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}},
    {"name": "Admin", "emoji": "👑", "invites_needed": None, "save_slots": float('inf'), "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}},
]

def get_user_badge(telegram_id: int) -> Dict[str, Any]:
    user = get_tg_user(telegram_id)
    if not user:
        return BADGE_LEVELS[0]
    if user['is_admin']:
        return BADGE_LEVELS[-1]
    invites = user.get('invite_count', 0)
    for level in reversed(BADGE_LEVELS[:-1]):
        if invites >= level['invites_needed']:
            return level
    return BADGE_LEVELS[0]

def increment_invite_count(telegram_id: int, amount: int = 1) -> int:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tg_users
        SET invite_count = COALESCE(invite_count, 0) + %s
        WHERE telegram_id = %s
        RETURNING invite_count
    """, (amount, telegram_id))
    new_count = cur.fetchone()['invite_count']
    conn.commit()
    cur.close()
    conn.close()
    return new_count

def set_admin(telegram_id: int, is_admin: bool) -> None:
    val = 1 if is_admin else 0
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("UPDATE tg_users SET is_admin = %s WHERE telegram_id = %s", (val, telegram_id))
    conn.commit()
    cur.close()
    conn.close()

# ================ COOLDOWN HELPERS ================

def get_rate_limits(telegram_id: int) -> Dict[str, Any]:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tg_rate_limits WHERE telegram_id = %s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else {
        'telegram_id': telegram_id,
        'minute_count': 0,
        'hour_count': 0,
        'day_count': 0,
        'minute_reset': None,
        'hour_reset': None,
        'day_reset': None
    }

def update_rate_limits(telegram_id: int, data: Dict[str, Any]) -> None:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tg_rate_limits (telegram_id, minute_count, hour_count, day_count, minute_reset, hour_reset, day_reset)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            minute_count = EXCLUDED.minute_count,
            hour_count = EXCLUDED.hour_count,
            day_count = EXCLUDED.day_count,
            minute_reset = EXCLUDED.minute_reset,
            hour_reset = EXCLUDED.hour_reset,
            day_reset = EXCLUDED.day_reset
    """, (telegram_id, data['minute_count'], data['hour_count'], data['day_count'],
          data['minute_reset'], data['hour_reset'], data['day_reset']))
    conn.commit()
    cur.close()
    conn.close()

def reset_cooldown(telegram_id: int) -> None:
    conn = get_tg_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tg_rate_limits SET
            minute_count = 0, hour_count = 0, day_count = 0,
            minute_reset = NULL, hour_reset = NULL, day_reset = NULL
        WHERE telegram_id = %s
    """, (telegram_id,))
    conn.commit()
    cur.close()
    conn.close()

def check_and_increment_cooldown(telegram_id: int) -> Optional[str]:
    """
    Returns None if allowed, else a block message string.
    """
    user = get_tg_user(telegram_id)
    if not user or user['is_banned']:
        return "You are banned."
    badge = get_user_badge(telegram_id)
    if badge['name'] == 'Admin':
        increment_tg_request_count(telegram_id)
        return None
    
    limits = badge['limits']
    now = datetime.utcnow()
    rl = get_rate_limits(telegram_id)
    
    # Initialize resets if None
    if rl['minute_reset'] is None:
        rl['minute_reset'] = now + timedelta(minutes=1)
    if rl['hour_reset'] is None:
        rl['hour_reset'] = now + timedelta(hours=1)
    if rl['day_reset'] is None:
        rl['day_reset'] = now + timedelta(days=1)
    
    # Reset counters if expired
    if now >= rl['minute_reset']:
        rl['minute_count'] = 0
        rl['minute_reset'] = now + timedelta(minutes=1)
    if now >= rl['hour_reset']:
        rl['hour_count'] = 0
        rl['hour_reset'] = now + timedelta(hours=1)
    if now >= rl['day_reset']:
        rl['day_count'] = 0
        rl['day_reset'] = now + timedelta(days=1)
    
    # Check limits
    if rl['minute_count'] >= limits['min']:
        seconds_left = (rl['minute_reset'] - now).total_seconds()
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['min']} / minute\n⏱ Try again in {int(seconds_left)} seconds\n\nInvite friends to unlock higher badges 🚀"
    if rl['hour_count'] >= limits['hour']:
        minutes_left = (rl['hour_reset'] - now).total_seconds() / 60
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['hour']} / hour\n⏱ Try again in {int(minutes_left)} minutes\n\nInvite friends to unlock higher badges 🚀"
    if rl['day_count'] >= limits['day']:
        hours_left = (rl['day_reset'] - now).total_seconds() / 3600
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['day']} / day\n⏱ Try again in {int(hours_left)} hours\n\nInvite friends to unlock higher badges 🚀"
    
    # Check abuse
    if rl['day_count'] > limits['day'] * 2:
        # Throttle harder, e.g., reset day count to max, extend reset
        rl['day_count'] = limits['day']
        rl['day_reset'] = now + timedelta(days=2)
        update_rate_limits(telegram_id, rl)
        return "🚫 Excessive usage detected. Cooldown extended."
    
    # Increment
    rl['minute_count'] += 1
    rl['hour_count'] += 1
    rl['day_count'] += 1
    update_rate_limits(telegram_id, rl)
    increment_tg_request_count(telegram_id)
    return None

# ================ ADMIN HELPERS ================

def get_user_stats(telegram_id: int) -> Dict[str, Any]:
    user = get_tg_user(telegram_id)
    if not user:
        return {}
    badge = get_user_badge(telegram_id)
    rl = get_rate_limits(telegram_id)
    saves = count_saved_accounts(telegram_id)
    return {
        'user': user,
        'badge': badge,
        'rate_limits': rl,
        'save_count': saves
    }

# ================ INIT ON IMPORT ================
try:
    init_tg_db()
except Exception as e:
    print(f"[utils] init_tg_db skipped or failed: {e}")