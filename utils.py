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
from openai import AsyncOpenAI, OpenAIError
import json
import urllib.parse
import re
from html import unescape
from googleapiclient.discovery import build
import random
from ntscraper import Nitter
import json
# ================ CONFIG ================
DB_URL = os.getenv("DATABASE_URL")                       # main cache DB (social posts)
TG_DB_URL = os.getenv("USERS_DATABASE_URL") or os.getenv("TG_DB_URL")   # separate TG DB
CACHE_HOURS = 24
POST_LIMIT = 10
GROQ_API_KEY=os.getenv("GROQ_KEY")
RAPIDAPI_KEY = os.getenv("RAPID_API")
RAPIDAPI_HOST = 'facebook-pages-scraper3.p.rapidapi.com'
RAPIDAPI_BASE = f"https://{RAPIDAPI_HOST}"
APIFY_FALLBACK_TIMEOUT = 8
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY")  # Add your Apify token to env
APIFY_ACTOR_ID = "gentle_cloud~twitter-tweets-scraper"
APIFY_BASE = "https://api.apify.com/v2"
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

        # Ensure optional columns exist (safe on existing DBs)
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
                # seen_posts table for deduping new posts (AI gatekeeper)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_posts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL CHECK (platform IN ('x', 'ig', 'fb')),
            account_name TEXT NOT NULL,
            post_id TEXT NOT NULL,                  -- X: tweet ID, IG: shortcode
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
        
        # Badges table (needed by get_explicit_badge)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_badges (
            telegram_id BIGINT PRIMARY KEY,
            badge TEXT,
            assigned_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # social_posts in main DB only if DB_URL is set (keeps previous behavior)
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

        # Add FK constraint if not present (idempotent)
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
    except Exception:
        logging.exception("[utils.init_tg_db] Failed to initialize tg DB tables")
        try:
            if conn:
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
def fetch_x_urls(account: str, limit: int = POST_LIMIT) -> List[str]:
    account = account.lstrip("@")
    profile_url = f"https://x.com/{account}"

    if not APIFY_API_TOKEN:
        logging.warning("APIFY_API_TOKEN not set – skipping X fetch")
        return []

    actor_id = APIFY_ACTOR_ID.strip().strip("/")
    run_url = (
        f"{APIFY_BASE}/acts/{actor_id}/runs"
        f"?token={APIFY_API_TOKEN}"
    )

    # ✅ MATCHES ACTOR INPUT SCHEMA
    payload = {
        "startUrls": [{"url": profile_url}],
        "maxTweets": limit,
        "sort": "Latest"
    }

    logging.info("Starting Apify run for @%s", account)

    try:
        run_resp = requests.post(run_url, json=payload, timeout=30)
    except Exception as e:
        logging.warning("Apify POST failed: %s", e)
        return []

    if run_resp.status_code not in (200, 201):
        logging.warning(
            "Apify run start failed (%s): %s",
            run_resp.status_code,
            run_resp.text[:800]
        )
        return []

    try:
        run_data = run_resp.json()["data"]
        run_id = run_data["id"]
        dataset_id = run_data["defaultDatasetId"]
    except Exception:
        logging.warning("Invalid Apify run response: %s", run_resp.text[:800])
        return []

    # ────────────────────────────
    # Poll run status
    # ────────────────────────────
    status_url = (
        f"{APIFY_BASE}/acts/{actor_id}/runs/{run_id}"
        f"?token={APIFY_API_TOKEN}"
    )

    status = None
    for _ in range(36):  # 6 minutes max
        try:
            r = requests.get(status_url, timeout=15)
            r.raise_for_status()
            status = r.json()["data"]["status"]
        except Exception:
            time.sleep(10)
            continue

        if status in ("SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"):
            break

        time.sleep(10)

    if status != "SUCCEEDED":
        logging.warning("Apify run ended with status=%s for @%s", status, account)
        return []

    # ────────────────────────────
    # Fetch dataset items
    # ────────────────────────────
    items_url = (
        f"{APIFY_BASE}/datasets/{dataset_id}/items"
        f"?clean=true&token={APIFY_API_TOKEN}"
    )

    try:
        items_resp = requests.get(items_url, timeout=30)
        items_resp.raise_for_status()
        items = items_resp.json()
    except Exception as e:
        logging.warning("Failed to fetch Apify dataset: %s", e)
        return []

    urls = []

    for item in items[:limit]:
        tweet_id = (
            item.get("id")
            or item.get("tweetId")
            or item.get("tweet_id")
        )
        if tweet_id:
            url = f"https://x.com/{account}/status/{tweet_id}"
            urls.append(url)
            save_url("x", account, url)

    logging.info(
        "Apify success: fetched %d posts for @%s",
        len(urls),
        account
    )

    return urls

def fetch_ig_urls(account: str) -> List[Dict[str, Any]]:
    """
    Reliable Instagram scraper Dec 2025 – uses i.instagram.com/api/v1/users/web_profile_info/
    No login needed, direct media URLs wey Telegram go accept.
    """
    account = account.lstrip('@')
    posts = []

    headers = {
        "User-Agent": "Instagram 219.0.0.12.119 Android (30/11; 480dpi; 1080x1920; samsung; SM-G998B; beyond2; exynos990; en_US)",
        "Accept": "*/*",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate",
        "X-IG-App-ID": "936619743392459",   # Current App ID (Dec 2025)
        "X-IG-WWW-Claim": "0",              # Optional but helps
        "Connection": "keep-alive"
    }

    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={account}"

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logging.warning(f"IG fetch failed for @{account}: status {resp.status_code}")
            return []

        data = resp.json()
        user_data = data.get("data", {}).get("user", {})
        if not user_data:
            logging.warning(f"No user data for @{account}")
            return []

        edges = user_data.get("edge_owner_to_timeline_media", {}).get("edges", [])
        for edge in edges[:POST_LIMIT]:
            node = edge.get("node", {})
            shortcode = node.get("shortcode")
            if not shortcode:
                continue

            post_url = f"https://www.instagram.com/p/{shortcode}/"
            is_video = node.get("is_video", False)
            caption = ""
            if node.get("edge_media_to_caption", {}).get("edges"):
                caption_edges = node["edge_media_to_caption"]["edges"]
                if caption_edges:
                    caption = caption_edges[0]["node"].get("text", "")

            # Best media URL
            if is_video:
                media_url = node.get("video_url", "")
            else:
                resources = node.get("display_resources", [])
                media_url = resources[-1]["src"] if resources else node.get("display_url", "")

            if media_url:
                posts.append({
                    "url": post_url,
                    "caption": caption,
                    "media_url": media_url,
                    "is_video": is_video
                })

        logging.info(f"Successfully fetched {len(posts)} IG posts for @{account}")

    except Exception as e:
        logging.warning(f"fetch_ig_urls exception for @{account}: {e}")

    return posts

def rapidapi_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20, retries: int = 2) -> Dict[str, Any]:
    """
    Call a RapidAPI product endpoint hosted at RAPIDAPI_HOST.
    Example: rapidapi_get("get-profile-home-page-details", params={"urlSupplier": "..."})
    """
    if not RAPIDAPI_KEY:
        raise RuntimeError("RAPIDAPI_KEY not set in environment")

    url = f"{RAPIDAPI_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Bot/1.0; +https://example.com/bot)"
    }

    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
            if resp.status_code != 200:
                logging.warning(
                    "rapidapi_get non-200 status %s for %s (attempt %d). Body: %.500s",
                    resp.status_code, url, attempt + 1, resp.text[:500]
                )
            resp.raise_for_status()
            # return parsed json
            return resp.json()
        except requests.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)
            if status in (429, 502, 503, 504):
                backoff = 1.5 * (attempt + 1)
                logging.warning(
                    "RapidAPI temporary error %s for %s — backing off %.1fs",
                    status, url, backoff
                )
                time.sleep(backoff)
                continue
            logging.exception("RapidAPI HTTP error for %s: %s", url, e)
            raise
        except Exception as e:
            last_exc = e
            logging.exception("RapidAPI request failed for %s (attempt %d): %s", url, attempt + 1, e)
            # jittered backoff
            time.sleep(1.0 * (attempt + 1))
            continue

    # all retries failed -> raise a clear error
    raise RuntimeError(f"RapidAPI call failed after {retries + 1} attempts")

def extract_og_meta(html: str, name: str) -> Optional[str]:
    """Extract content of og:name or name meta tags. Returns first match or None."""
    # look for property="og:image" or name="twitter:image"
    m = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og:|twitter:)?' + re.escape(name) + r'["\'][^>]*content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return unescape(m.group(1))
    # fallback: generic content attr
    return None

def fetch_fb_urls(account_or_url: str, limit: int = POST_LIMIT) -> List[Dict[str, Any]]:
    """
    Final version for Facebook fetching using RapidAPI (facebook-pages-scraper3).
    - Accepts @handle, plain handle, or full profile URL
    - Strips @ completely, uses clean name in URL for accuracy
    - Dedupes posts by post_id
    - Uses www.facebook.com links (best for public posts in 2025)
    - Handles videos properly
    - Direct high-quality media URLs (no login needed for images/videos)
    """
    input_str = account_or_url.strip()

    # Handle single post share links separately
    if ("share/" in input_str or "mibextid=" in input_str or 
        "/posts/" in input_str or "/photo.php" in input_str or "/reel/" in input_str):
        # Clean the share link (remove tracking params)
        clean_url = input_str.split("?")[0].rstrip("/")
        return [{
            "post_id": "",
            "post_url": clean_url,
            "caption": "Single shared Facebook post",
            "media_url": "",
            "is_video": False,
            "likes": 0,
            "comments": 0,
            "shares": 0,
        }]

    # Normal profile fetch
    # Extract clean account name (no @, no path)
    clean_account = input_str.lstrip("@").split("/")[-1].split("?")[0]
    if not clean_account:
        return []

    profile_url = f"https://www.facebook.com/{clean_account}"

    path = "get-profile-home-page-details"
    params = {"urlSupplier": profile_url}

    posts: List[Dict[str, Any]] = []
    seen_ids = set()

    try:
        data = rapidapi_get(path, params=params, timeout=30)
    except Exception as e:
        logging.warning("RapidAPI FB fetch failed for %s: %s", profile_url, e)
        return []

    if not isinstance(data, dict):
        return []

    photos = data.get("PHOTOS") or data.get("photos") or []

    for it in photos:
        if len(posts) >= limit:
            break

        post_id = it.get("id") or ""
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        media_type = it.get("media", "Photo")
        is_video = bool(it.get("is_playable")) or media_type.lower() == "video"
        
        media_url = it.get("uri") or it.get("thumb") or ""
        if not media_url:
            continue

        # Clean post_url without @
        if post_id:
            post_url = f"https://www.facebook.com/{clean_account}/posts/{post_id}"
        else:
            post_url = profile_url

        posts.append({
            "post_id": post_id,
            "post_url": post_url,
            "caption": "",  # Endpoint doesn't provide text/caption
            "media_url": media_url,
            "is_video": is_video,
            "likes": it.get("likes", 0),
            "comments": it.get("comments", 0),
            "shares": it.get("shares", 0),
        })

    logging.info("fetch_fb_urls -> returned %d unique posts for facebook.com/%s", len(posts), clean_account)
    return posts

def fetch_yt_videos(channel_handle: str, max_results: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch latest videos from a YouTube channel using a handle (e.g. "@MrBeast").
    Returns newest-first list of dicts (up to max_results). Uses uploads playlist.
    """
    if max_results is None:
        max_results = POST_LIMIT

    if not YOUTUBE_API_KEY:
        logging.warning("YOUTUBE_API_KEY not set")
        return []

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    videos: List[Dict[str, Any]] = []

    try:
        # Get channel ID from handle (strip @ if present)
        handle = channel_handle.lstrip('@')
        channel_response = youtube.channels().list(
            part='contentDetails',
            forHandle=handle
        ).execute()

        items = channel_response.get('items', [])
        if not items:
            logging.info(f"No channel found for @{handle}")
            return []

        uploads_playlist_id = items[0]['contentDetails']['relatedPlaylists']['uploads']

        # Fetch videos – newest first with pagination
        next_page_token = None
        fetched = 0

        while fetched < max_results:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=min(50, max_results - fetched),  # API max 50 per page
                pageToken=next_page_token
            )
            playlist_response = request.execute()

            for item in playlist_response.get('items', []):
                snippet = item.get('snippet', {})
                title = snippet.get('title', '')
                if title in ('Private video', 'Deleted video'):
                    continue  # skip private/deleted

                resource = snippet.get('resourceId', {})
                video_id = resource.get('videoId')
                if not video_id:
                    continue

                # thumbnail selection with fallbacks
                thumbs = snippet.get('thumbnails', {})
                thumb_url = (
                    thumbs.get('maxres', {}).get('url') or
                    thumbs.get('standard', {}).get('url') or
                    thumbs.get('high', {}).get('url') or
                    thumbs.get('default', {}).get('url') or
                    None
                )

                videos.append({
                    "post_id": video_id,
                    "post_url": f"https://www.youtube.com/watch?v={video_id}",
                    "caption": f"<b>{title}</b>\n\n{snippet.get('description', '')[:800]}",
                    "media_url": thumb_url,
                    "is_video": True,   # this is a YouTube video
                    "title": title,
                    "published_at": snippet.get('publishedAt'),
                    "channel_title": snippet.get('channelTitle')
                })
                fetched += 1
                if fetched >= max_results:
                    break

            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break  # no more pages

        # Sort by publishedAt descending (newest first)
        videos.sort(key=lambda x: x.get('published_at') or "", reverse=True)

        logging.info(f"Fetched {len(videos)} latest YouTube videos from @{handle}")

    except Exception as e:
        logging.exception(f"YouTube fetch error for @{channel_handle}: {e}")

    return videos

def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@')
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
    elif platform == "fb":
        new_fb = fetch_fb_urls(account)
        for p in new_fb:
            save_url("fb", account, p["post_url"])
        return [p["post_url"] for p in new_fb]
    return []

# ================ TG USER HELPERS (tg DB) ============
def add_or_update_tg_user(telegram_id: int, first_name: str) -> Dict[str, Any]:
    """
    Upsert the user and return the current row from tg_users.
    This avoids relying on RETURNING columns that may not exist on older schemas.
    """
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

        # Now SELECT the row explicitly (choose only columns we expect to exist).
        # Using explicit column list reduces chance of issues if new columns are added/removed.
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

# ================ BADGE AND INVITE HELPERS ================
BADGE_LEVELS = [
    {
        "name": "Basic",
        "emoji": "🪪",
        "invites_needed": 0,
        "save_slots": 5,                     # Fair starter
        "limits": {"min": 2, "hour": 12, "day": 30}   # Stricter: only 2/min, 12/hour
    },
    {
        "name": "Bronze",
        "emoji": "🥉",
        "invites_needed": 8,
        "save_slots": 10,
        "limits": {"min": 4, "hour": 30, "day": 80}
    },
    {
        "name": "Silver",
        "emoji": "🥈",
        "invites_needed": 25,
        "save_slots": 15,
        "limits": {"min": 8, "hour": 70, "day": 200}
    },
    {
        "name": "Gold",
        "emoji": "🥇",
        "invites_needed": 60,
        "save_slots": 25,
        "limits": {"min": 15, "hour": 150, "day": 500}
    },
    {
        "name": "Diamond",
        "emoji": "💎",
        "invites_needed": 150,
        "save_slots": float('inf'),          # Unlimited
        "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}
    },
    {
        "name": "Admin",
        "emoji": "👑",
        "invites_needed": None,
        "save_slots": float('inf'),
        "limits": {"min": float('inf'), "hour": float('inf'), "day": float('inf')}
    },
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
    Determine badge for a user.
    - Honors tg_users.is_admin column if present.
    - Honors ADMIN_IDS environment variable (comma-separated list) so bot admins always get Admin badge.
    """
    user = get_tg_user(telegram_id)
    # check env-admin list too (helps when DB hasn't been updated)
    admin_ids_env = os.getenv("ADMIN_IDS", "")
    admin_ids = []
    if admin_ids_env:
        try:
            admin_ids = [int(x.strip()) for x in admin_ids_env.split(",") if x.strip()]
        except Exception:
            admin_ids = []

    # Admin if flagged in DB or present in ADMIN_IDS env var
    if (user and int(user.get("is_admin", 0)) == 1) or (telegram_id in admin_ids):
        # find Admin badge entry (fallback to last)
        for b in BADGE_LEVELS:
            if b.get("name") == "Admin":
                return b
        return BADGE_LEVELS[-1]

    invites = user.get("invite_count", 0) if user else 0

    # Walk levels (exclude Admin) and pick highest that fits
    non_admin_levels = [lvl for lvl in BADGE_LEVELS if lvl.get("name") != "Admin"]
    for level in reversed(non_admin_levels):
        # invites_needed could be 0 for Basic
        needed = level.get("invites_needed") or 0
        if invites >= needed:
            return level

    return BADGE_LEVELS[0]

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

# ================ COOLDOWN HELPERS (ONLY FOR FETCH REQUESTS) ================
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

# Extract post_id from URL
def extract_post_id(platform: str, url: str) -> str:
    if platform == "x":
        return url.split("/")[-1].split("?")[0]  # tweet ID
    elif platform == "ig":
        return url.split("/p/")[1].split("/")[0]  # shortcode
    return ""

# Check if post is new for user
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
        return True  # safe default

# Mark posts as seen
def mark_posts_seen(owner_id: int, platform: str, account: str, posts: List[Dict[str, str]]):
    """posts = [{'post_id': ..., 'post_url': ...}, ...]"""
    if not posts:
        return
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        for p in posts:
            cur.execute("""
                INSERT INTO seen_posts (owner_telegram_id, platform, account_name, post_id, post_url)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (owner_id, platform, account, p['post_id'], p['post_url']))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        logging.debug("mark_posts_seen failed", exc_info=True)

def check_and_increment_cooldown(telegram_id: int) -> Optional[str]:
    """
    Returns None if allowed, else a block message string.
    ONLY called when a real fetch (X/IG posts) is about to happen.
    """
    user = get_tg_user(telegram_id)
    if user and int(user.get('is_banned', 0)) == 1:
        return "You are banned."
    badge = get_user_badge(telegram_id)
    if badge['name'] == 'Admin':
        # Admins bypass cooldown but still count as activity
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
    increment_tg_request_count(telegram_id)  # Still track general activity
    return None

logging.basicConfig(level=logging.INFO)

async def call_social_ai(platform: str, account: str, posts: List[Dict]) -> str:
    if not posts:
        return "No new posts to analyze."

    captions_text = "\n---\n".join([p.get("caption", "No caption") for p in posts if p.get("caption")])

    prompt = f"""
You are a sharp Nigerian social media analyst wey sabi X , IG, FB, YT well-well. Analyze these {platform.upper()} post(s) from @{account}.

Post captions:
{captions_text}

Answer ONLY in short, sweet Pidgin-mixed English:

1. Wetin be the content of the pic or video(check deep)? (Main message or purpose(check the pic or video well)

2. Tone & intent: Promotion, Drama, Political, Education, Memes, Awareness, Campaign, or na saga?

3. Trend signal: Going viral, Mid viral, People talking about it, or neutral?

Keep am short – max 5 sentences. Use Naija vibe and slang where e fit!
"""

    api_key = os.getenv("GROQ_KEY") or os.getenv("GROQ_KEY")
    if not api_key:
        logging.warning("GROQ API key missing")
        return "🤖 AI analysis unavailable (missing API key)."

    MODEL_CANDIDATES = [
        "llama-3.3-70b-versatile",     # Current flagship (Dec 2025)
        "llama-3.1-70b-versatile",     # Still available fallback
        "llama-3.1-8b-instant",        # Fast lightweight
        "gemma2-9b-it",                # Reliable alternative
    ]

    try:
        client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1"
        )

        for model in MODEL_CANDIDATES:
            try:
                logging.info(f"Trying Groq model: {model}")
                response = await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.8,
                    max_tokens=400
                )
                result = response.choices[0].message.content.strip()
                logging.info(f"AI analysis succeeded with model: {model}")
                return result
            except Exception as e:
                err = str(e).lower()
                if "not found" in err or "decommissioned" in err:
                    logging.info(f"Model {model} unavailable – skipping to next")
                    continue
                else:
                    logging.warning(f"Model {model} failed: {e}")
                    continue

        return "🤖 AI analysis unavailable – all models failed or unavailable right now."

    except Exception as e:
        logging.exception(f"Groq API unexpected error: {e}")
        return "🤖 AI analysis unavailable right now. Try again later!"


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