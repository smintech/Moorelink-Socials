import os
import hashlib
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from config import * # for DB URLs

# ================ DB CONNECTIONS ============
def get_db():
    if not config.DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(config.DB_URL, cursor_factory=RealDictCursor)

def get_tg_db():
    if not config.TG_DB_URL:
        raise RuntimeError("USERS_DATABASE_URL / TG_DB_URL not set")
    return psycopg2.connect(config.TG_DB_URL, cursor_factory=RealDictCursor)

# ================ INIT TABLES ================
def init_tg_db():
    """
    Create/patch tg-related tables and required columns idempotently.
    Safe to call every startup.
    """
    conn = None
    try:
        conn = get_tg_db()
        cur = conn.cursor()

        # Core table (create if missing)
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

        # Ensure optional columns exist
        cur.execute("ALTER TABLE tg_users ADD COLUMN IF NOT EXISTS invite_count INTEGER DEFAULT 0;")
        cur.execute("ALTER TABLE tg_users ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0;")

        # saved_accounts table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_accounts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL CHECK (platform IN ('x', 'ig', 'fb')),
            account_name TEXT NOT NULL,
            label TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_telegram_id, platform, account_name)
        );
        """)

        # Rate limits table
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

        # platform_types table for FK robustness
        cur.execute("""
        CREATE TABLE IF NOT EXISTS platform_types (
            platform TEXT PRIMARY KEY
        );
        """)

        # Insert allowed platforms
        cur.execute("""
        INSERT INTO platform_types (platform)
        VALUES ('x'), ('ig'), ('fb'), ('yt')
        ON CONFLICT DO NOTHING;
        """)

        # seen_posts table for deduping new posts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_posts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL REFERENCES platform_types(platform),
            account_name TEXT NOT NULL,
            post_id TEXT NOT NULL,
            post_url TEXT NOT NULL,
            seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_telegram_id, platform, account_name, post_id)
        );
        """)

        # Index for fast lookups
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_seen_user_account 
        ON seen_posts(owner_telegram_id, platform, account_name);
        """)
        
        # Badges table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_badges (
            telegram_id BIGINT PRIMARY KEY,
            badge TEXT,
            assigned_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # social_posts in main DB only if DB_URL is set
        if config.DB_URL:
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

        # Add FK constraint if not present
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
        logging.info("[persistence.init_tg_db] tg DB tables created/verified successfully.")
    except Exception:
        logging.exception("[persistence.init_tg_db] Failed to initialize tg DB tables")
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# ================ CACHE HELPERS ============
def generate_url_hash(account: str, url: str) -> str:
    key = f"{account.lower()}:{url}"
    return hashlib.sha256(key.encode()).hexdigest()

def save_url(platform: str, account: str, url: str):
    if not config.DB_URL:
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
        logging.debug("save_url failed", exc_info=True)

def get_recent_urls(platform: str, account: str) -> list:
    if not config.DB_URL:
        return []
    conn = get_db()
    cur = conn.cursor()
    time_limit = datetime.utcnow() - timedelta(hours=config.CACHE_HOURS)
    try:
        cur.execute("""
            SELECT post_url
            FROM social_posts
            WHERE platform = %s
              AND account_name = %s
              AND fetched_at >= %s
            ORDER BY fetched_at DESC
            LIMIT %s
        """, (platform.lower(), account.lower(), time_limit, config.POST_LIMIT))
        rows = cur.fetchall()
        return [row["post_url"] for row in rows]
    finally:
        cur.close()
        conn.close()

# ================ TG USER HELPERS ============
def add_or_update_tg_user(telegram_id: int, first_name: str) -> Dict[str, Any]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tg_users (telegram_id, first_name)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET first_name = EXCLUDED.first_name;
        """, (telegram_id, first_name))
        conn.commit()

        cur.execute("""
            SELECT telegram_id, first_name,
                   COALESCE(is_admin, 0) AS is_admin,
                   COALESCE(invite_count, 0) AS invite_count,
                   COALESCE(request_count, 0) AS request_count,
                   COALESCE(is_banned, 0) AS is_banned,
                   COALESCE(is_active, 1) AS is_active,
                   joined_at
            FROM tg_users
            WHERE telegram_id = %s
        """, (telegram_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        logging.exception("add_or_update_tg_user failed")
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return {}

def create_user_if_missing(telegram_id: int, first_name: str) -> bool:
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
    account_name = account_name.lstrip('@')
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

# ================ BADGE HELPERS (DB only) ================
def get_explicit_badge(telegram_id: int) -> Optional[str]:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("SELECT badge FROM tg_badges WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row['badge'] if row else None
    except Exception:
        logging.debug("get_explicit_badge failed", exc_info=True)
        return None

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

# ================ POST DEDUP HELPERS ================
def extract_post_id(platform: str, url: str) -> str:
    if platform == "x":
        return url.split("/")[-1].split("?")[0]
    elif platform == "ig":
        return url.split("/p/")[1].split("/")[0]
    return ""

def is_post_new(owner_id: int, platform: str, account: str, post_id: str) -> bool:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM seen_posts
            WHERE owner_telegram_id = %s
              AND platform = %s
              AND account_name = %s
              AND post_id = %s
        """, (owner_id, platform, account, post_id))
        exists = cur.fetchone()
        cur.close()
        conn.close()
        return exists is None
    except Exception:
        logging.debug("is_post_new failed", exc_info=True)
        return True

def ensure_platform_exists(platform: str) -> bool:
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO platform_types (platform)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (platform,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        logging.debug("ensure_platform_exists failed for '%s'", platform, exc_info=True)
        return False

def mark_posts_seen(owner_id: int, platform: str, account: str, posts: List[Dict[str, str]]):
    if not posts:
        return

    platform = platform.lower()
    account = account.lstrip('@')

    if not ensure_platform_exists(platform):
        logging.warning(f"Skipping mark_posts_seen due to platform '{platform}' insert failure")
        return

    try:
        conn = get_tg_db()
        cur = conn.cursor()

        values = [
            (owner_id, platform, account, p['post_id'], p['post_url'])
            for p in posts
            if p.get('post_id') and p.get('post_url')
        ]

        if values:
            cur.executemany("""
                INSERT INTO seen_posts (
                    owner_telegram_id, 
                    platform, 
                    account_name, 
                    post_id, 
                    post_url
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (owner_telegram_id, platform, account_name, post_id) 
                DO NOTHING
            """, values)

        conn.commit()
        logging.info(
            "Marked %d %s posts as seen for user %d (@%s)",
            len(values), platform.upper(), owner_id, account
        )

    except psycopg2.IntegrityError as e:
        logging.warning("Integrity error marking posts seen: %s", e)
    except Exception as e:
        logging.error("Failed to mark posts seen: %s", e, exc_info=True)
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# ================ INIT ON IMPORT ================
try:
    init_tg_db()
except Exception:
    logging.exception("[persistence] init_tg_db skipped or failed at import")