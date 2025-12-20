# utils.py
import os
import hashlib
import time
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import instaloader

# ================ CONFIG ================
DB_URL = os.getenv("DATABASE_URL")                       # main cache DB (social posts)
TG_DB_URL = os.getenv("USERS_DATABASE_URL") or os.getenv("TG_DB_URL")   # separate TG DB
CACHE_HOURS = int(os.getenv("CACHE_HOURS", "24"))
POST_LIMIT = int(os.getenv("POST_LIMIT", "5"))

# ================ DB CONNECTIONS ============
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def get_tg_db():
    if not TG_DB_URL:
        raise RuntimeError("USERS_DATABASE_URL / TG_DB_URL not set")
    return psycopg2.connect(TG_DB_URL, cursor_factory=RealDictCursor)

# ================ INIT TABLES ================
def init_tg_db():
    """
    Create all tg-related tables idempotently. Safe to call every startup.
    """
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
        # rate limits table
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
        # badges table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_badges (
            telegram_id BIGINT PRIMARY KEY,
            badge TEXT,
            assigned_at TIMESTAMP DEFAULT NOW()
        );
        """)
        # social_posts caching table (in separate DB if DB_URL provided)
        # (only create if DB_URL set)
        if DB_URL:
            db_conn = get_db()
            db_cur = db_conn.cursor()
            db_cur.execute("""
            CREATE TABLE IF NOT EXISTS social_posts (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                account_name TEXT NOT NULL,
                post_url TEXT NOT NULL,
                fetched_at TIMESTAMP NOT NULL
            );
            """)
            db_conn.commit()
            db_cur.close()
            db_conn.close()

        # Add FK constraint only if not present (safe on reruns)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'fk_saved_owner'
            ) THEN
                ALTER TABLE saved_accounts
                ADD CONSTRAINT fk_saved_owner
                FOREIGN KEY (owner_telegram_id)
                REFERENCES tg_users(telegram_id)
                ON DELETE CASCADE;
            END IF;
        END
        $$;
        """)

        conn.commit()
        cur.close()
        conn.close()
        logging.info("[utils.init_tg_db] tg DB tables created/verified successfully.")
    except Exception as e:
        logging.exception(f"[utils.init_tg_db] Failed to initialize tg DB tables: {e}")
        # If anything fails, raise so caller can decide; but here we swallow to let bot run
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# ================ FETCH/CACHE HELPERS ============
def generate_url_hash(account: str, url: str) -> str:
    key = f"{account.lower()}:{url}"
    return hashlib.sha256(key.encode()).hexdigest()

def save_url(platform: str, account: str, url: str):
    if not DB_URL:
        return
    try:
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
    except Exception:
        # don't crash the bot for caching errors
        logging.debug("save_url failed", exc_info=True)

def get_recent_urls(platform: str, account: str) -> list:
    if not DB_URL:
        return []
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
        except Exception:
            time.sleep(0.7)
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
        logging.debug("fetch_ig_urls failed", exc_info=True)
    return posts

def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@').lower()
    cached = get_recent_urls(platform, account)
    if cached:
        return cached
    if platform == "x":
        new = fetch_x_urls(account)
        for u in new:
            save_url("x", account, u)
        return new
    elif platform == "ig":
        new_ig = fetch_ig_urls(account)
        for p in new_ig:
            save_url("ig", account, p["url"])
        return [p["url"] for p in new_ig]
    return []

# ================ TG USER HELPERS (tg DB) ============
def add_or_update_tg_user(telegram_id: int, first_name: str) -> Dict[str, Any]:
    """
    Insert or update a tg user; return the row as dict.
    """
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tg_users (telegram_id, first_name)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET first_name = EXCLUDED.first_name
            RETURNING telegram_id, first_name, is_admin, invite_count, request_count, is_banned, is_active, joined_at;
        """, (telegram_id, first_name))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        logging.debug("add_or_update_tg_user failed", exc_info=True)
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return {}

def create_user_if_missing(telegram_id: int, first_name: str) -> bool:
    """
    Try to insert a user; returns True if inserted (new), False if existed.
    This is used for detecting whether /start with inviter is a new signup.
    """
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tg_users (telegram_id, first_name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING telegram_id;
        """, (telegram_id, first_name))
        r = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return bool(r)
    except Exception:
        logging.debug("create_user_if_missing failed", exc_info=True)
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return False

def ban_tg_user(telegram_id: int) -> None:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("UPDATE tg_users SET is_banned = 1 WHERE telegram_id = %s", (telegram_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        logging.debug("ban_tg_user failed", exc_info=True)

def unban_tg_user(telegram_id: int) -> None:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("UPDATE tg_users SET is_banned = 0 WHERE telegram_id = %s", (telegram_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        logging.debug("unban_tg_user failed", exc_info=True)

def set_tg_user_active(telegram_id: int, active: bool) -> None:
    val = 1 if active else 0
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("UPDATE tg_users SET is_active = %s WHERE telegram_id = %s", (val, telegram_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        logging.debug("set_tg_user_active failed", exc_info=True)

def increment_tg_request_count(telegram_id: int) -> None:
    try:
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
    except Exception:
        logging.debug("increment_tg_request_count failed", exc_info=True)

def get_tg_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tg_users WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        logging.debug("get_tg_user failed", exc_info=True)
        return None

def list_active_tg_users(limit: int = 100) -> List[Dict[str, Any]]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT telegram_id, first_name, is_active, is_banned, request_count, last_request_at, joined_at, invite_count
            FROM tg_users
            WHERE is_active = 1
            ORDER BY joined_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        logging.debug("list_active_tg_users failed", exc_info=True)
        return []

def list_all_tg_users(limit: int = 1000) -> List[Dict[str, Any]]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT telegram_id, first_name, is_active, is_banned, request_count, last_request_at, joined_at, invite_count
            FROM tg_users
            ORDER BY joined_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        logging.debug("list_all_tg_users failed", exc_info=True)
        return []

# ================ SAVED ACCOUNTS HELPERS ============
def save_user_account(owner_telegram_id: int, platform: str, account_name: str, label: Optional[str]=None) -> Dict[str, Any]:
    platform = platform.lower()
    account_name = account_name.lstrip('@').lower()
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO saved_accounts (owner_telegram_id, platform, account_name, label)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (owner_telegram_id, platform, account_name) DO UPDATE
            SET label = COALESCE(EXCLUDED.label, saved_accounts.label)
            RETURNING *
        """, (owner_telegram_id, platform, account_name, label))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        logging.debug("save_user_account failed", exc_info=True)
        return {}

def list_saved_accounts(owner_telegram_id: int) -> List[Dict[str, Any]]:
    try:
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
    except Exception:
        logging.debug("list_saved_accounts failed", exc_info=True)
        return []

def get_saved_account(owner_telegram_id: int, saved_id: int) -> Optional[Dict[str, Any]]:
    try:
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
    except Exception:
        logging.debug("get_saved_account failed", exc_info=True)
        return None

def remove_saved_account(owner_telegram_id: int, saved_id: int) -> bool:
    try:
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
    except Exception:
        logging.debug("remove_saved_account failed", exc_info=True)
        return False

def count_saved_accounts(owner_telegram_id: int) -> int:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM saved_accounts WHERE owner_telegram_id = %s", (owner_telegram_id,))
        r = cur.fetchone()
        cur.close()
        conn.close()
        return int(r["cnt"]) if r else 0
    except Exception:
        logging.debug("count_saved_accounts failed", exc_info=True)
        return 0

def update_saved_account_label(owner_telegram_id: int, saved_id: int, new_label: str) -> bool:
    try:
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
    except Exception:
        logging.debug("update_saved_account_label failed", exc_info=True)
        return False

# ================ BADGE AND INVITE HELPERS ================
BADGE_LEVELS = [
    {"name": "Basic", "emoji": "🪪", "invites_needed": 0, "save_slots": 5, "limits": {"min": 2, "hour": 15, "day": 40}},
    {"name": "Bronze", "emoji": "🥉", "invites_needed": 5, "save_slots": 7, "limits": {"min": 3, "hour": 30, "day": 80}},
    {"name": "Silver", "emoji": "🥈", "invites_needed": 15, "save_slots": 10, "limits": {"min": 5, "hour": 60, "day": 150}},
    {"name": "Gold", "emoji": "🥇", "invites_needed": 40, "save_slots": 15, "limits": {"min": 8, "hour": 120, "day": 300}},
    {"name": "Diamond", "emoji": "💎", "invites_needed": 100, "save_slots": float('inf'), "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}},
    {"name": "Admin", "emoji": "👑", "invites_needed": None, "save_slots": float('inf'), "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}},
]

def get_explicit_badge(telegram_id: int) -> Optional[str]:
    """Return explicit badge set in tg_badges table or None."""
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("SELECT badge FROM tg_badges WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row['badge'] if row else None
    except Exception:
        logging.debug("get_explicit_badge failed (maybe table missing)", exc_info=True)
        return None

def get_user_badge(telegram_id: int) -> Dict[str, Any]:
    """
    Determine user's badge:
      - explicit badge in tg_badges overrides
      - is_admin flag -> Admin badge
      - otherwise badge by invite_count
    """
    user = get_tg_user(telegram_id) or {}
    # explicit override
    explicit = get_explicit_badge(telegram_id)
    if explicit:
        # try to match explicit to known badges
        for b in BADGE_LEVELS:
            if b['name'].lower() == explicit.lower() or b['emoji'] == explicit:
                return b
        # fallback: return Basic
        return BADGE_LEVELS[0]
    # admin flag
    try:
        if int(user.get('is_admin', 0)) == 1:
            return BADGE_LEVELS[-1]  # Admin
    except Exception:
        pass
    invites = int(user.get('invite_count') or 0)
    # pick highest badge satisfying invites_needed
    chosen = BADGE_LEVELS[0]
    for level in BADGE_LEVELS:
        if level.get('invites_needed') is None:
            continue
        if invites >= level['invites_needed']:
            chosen = level
    return chosen

def increment_invite_count(telegram_id: int, amount: int = 1) -> int:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE tg_users
            SET invite_count = COALESCE(invite_count, 0) + %s
            WHERE telegram_id = %s
            RETURNING invite_count
        """, (amount, telegram_id))
        r = cur.fetchone()
        if r:
            new_count = r['invite_count']
        else:
            # maybe no row yet
            cur.execute("""
                INSERT INTO tg_users (telegram_id, invite_count)
                VALUES (%s, %s)
                ON CONFLICT (telegram_id) DO UPDATE
                SET invite_count = tg_users.invite_count + %s
                RETURNING invite_count
            """, (telegram_id, amount, amount))
            new_count = cur.fetchone()['invite_count']
        conn.commit()
        cur.close()
        conn.close()
        return int(new_count)
    except Exception:
        logging.debug("increment_invite_count failed", exc_info=True)
        return 0

def set_admin(telegram_id: int, is_admin: bool) -> None:
    val = 1 if is_admin else 0
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("UPDATE tg_users SET is_admin = %s WHERE telegram_id = %s", (val, telegram_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        logging.debug("set_admin failed", exc_info=True)

# ================ COOLDOWN HELPERS ================
def get_rate_limits(telegram_id: int) -> Dict[str, Any]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tg_rate_limits WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return dict(row)
    except Exception:
        logging.debug("get_rate_limits failed", exc_info=True)
    # default structure
    return {
        'telegram_id': telegram_id,
        'minute_count': 0,
        'hour_count': 0,
        'day_count': 0,
        'minute_reset': None,
        'hour_reset': None,
        'day_reset': None
    }

def update_rate_limits(telegram_id: int, data: Dict[str, Any]) -> None:
    try:
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
    except Exception:
        logging.debug("update_rate_limits failed", exc_info=True)

def reset_cooldown(telegram_id: int) -> None:
    try:
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
    except Exception:
        logging.debug("reset_cooldown failed", exc_info=True)

def check_and_increment_cooldown(telegram_id: int) -> Optional[str]:
    """
    Returns None if allowed, else a block message string.
    """
    user = get_tg_user(telegram_id)
    if user and int(user.get('is_banned', 0)) == 1:
        return "You are banned."
    badge = get_user_badge(telegram_id)
    if badge['name'] == 'Admin':
        increment_tg_request_count(telegram_id)
        return None

    limits = badge['limits']
    now = datetime.utcnow()
    rl = get_rate_limits(telegram_id)

    # fill resets if None
    if rl.get('minute_reset') is None:
        rl['minute_reset'] = now + timedelta(minutes=1)
    if rl.get('hour_reset') is None:
        rl['hour_reset'] = now + timedelta(hours=1)
    if rl.get('day_reset') is None:
        rl['day_reset'] = now + timedelta(days=1)

    # convert from psycopg2 timestamps (if present)
    minute_reset = rl['minute_reset']
    hour_reset = rl['hour_reset']
    day_reset = rl['day_reset']

    # Reset counters if expired
    if now >= minute_reset:
        rl['minute_count'] = 0
        rl['minute_reset'] = now + timedelta(minutes=1)
    if now >= hour_reset:
        rl['hour_count'] = 0
        rl['hour_reset'] = now + timedelta(hours=1)
    if now >= day_reset:
        rl['day_count'] = 0
        rl['day_reset'] = now + timedelta(days=1)

    # Check limits
    if isinstance(limits.get('min'), (int, float)) and rl['minute_count'] >= limits['min']:
        seconds_left = int((rl['minute_reset'] - now).total_seconds())
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['min']} / minute\n⏱ Try again in {seconds_left} seconds\n\nInvite friends to unlock higher badges 🚀"
    if isinstance(limits.get('hour'), (int, float)) and rl['hour_count'] >= limits['hour']:
        minutes_left = int((rl['hour_reset'] - now).total_seconds() / 60)
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['hour']} / hour\n⏱ Try again in {minutes_left} minutes\n\nInvite friends to unlock higher badges 🚀"
    if isinstance(limits.get('day'), (int, float)) and rl['day_count'] >= limits['day']:
        hours_left = int((rl['day_reset'] - now).total_seconds() / 3600)
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['day']} / day\n⏱ Try again in {hours_left} hours\n\nInvite friends to unlock higher badges 🚀"

    # Abuse detection
    if isinstance(limits.get('day'), (int, float)) and rl['day_count'] > limits['day'] * 2:
        rl['day_count'] = limits['day']
        rl['day_reset'] = now + timedelta(days=2)
        update_rate_limits(telegram_id, rl)
        return "🚫 Excessive usage detected. Cooldown extended."

    # Increment counters & persist
    rl['minute_count'] = int(rl.get('minute_count', 0)) + 1
    rl['hour_count'] = int(rl.get('hour_count', 0)) + 1
    rl['day_count'] = int(rl.get('day_count', 0)) + 1

    update_rate_limits(telegram_id, rl)
    increment_tg_request_count(telegram_id)
    return None

# ================ ADMIN HELPERS ================
def get_user_stats(telegram_id: int) -> Dict[str, Any]:
    user = get_tg_user(telegram_id) or {}
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
except Exception:
    logging.exception("[utils] init_tg_db skipped or failed at import")