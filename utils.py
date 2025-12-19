# utils.py
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from app import get_db  # Import your Flask app's get_db function

# ===================== CONFIG =====================
CACHE_DURATION = timedelta(minutes=30)  # How long to keep in-memory cache
POST_FETCH_LIMIT = 5                   # Max posts to return per request

# ===================== IN-MEMORY CACHE =====================
# Format: { "platform|account": { "last_fetch": datetime, "posts": List[dict], "hashes": set() } }
_cache: Dict[str, Dict] = {}

def _cache_key(platform: str, account: str) -> str:
    return f"{platform.lower()}|{account.lower()}"

# ===================== HASH & ID HELPERS =====================
def generate_post_hash(platform: str, account: str, post_url: str) -> str:
    """Generate unique hash for deduplication."""
    key = f"{platform.lower()}|{account.lower()}|{post_url}"
    return hashlib.sha256(key.encode()).hexdigest()

def generate_post_id(post_hash: str) -> str:
    """Optional: shorter ID if needed (first 16 chars of hash)"""
    return post_hash[:16]

# ===================== DATABASE OPERATIONS =====================
def save_post(platform: str, account: str, post_data: dict):
    """
    Save or update a post in the database.
    post_data should contain: url, text (optional), media_urls (list), top_comments (list)
    """
    db = get_db()
    cur = db.cursor()

    post_hash = generate_post_hash(platform, account, post_data["url"])
    post_id = generate_post_id(post_hash)

    cur.execute("""
        INSERT INTO social_posts (
            id, platform, account_name, post_url, content_text, 
            media_urls, top_comments, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET fetched_at = NOW(),
            content_text = EXCLUDED.content_text,
            media_urls = EXCLUDED.media_urls,
            top_comments = EXCLUDED.top_comments
    """, (
        post_id,
        platform.lower(),
        account.lower(),
        post_data["url"],
        post_data.get("text", "")[:1000],
        post_data.get("media_urls", []),
        post_data.get("top_comments", [])[:5]  # limit comments
    ))

    db.commit()
    cur.close()
    db.close()

def get_recent_posts(platform: str, account: str, limit: int = POST_FETCH_LIMIT) -> List[dict]:
    """Fetch recent posts from DB for a specific account and platform."""
    db = get_db()
    cur = db.cursor()

    time_limit = datetime.utcnow() - timedelta(hours=24)  # only last 24 hours

    cur.execute("""
        SELECT post_url, content_text, media_urls, top_comments, fetched_at
        FROM social_posts
        WHERE platform = %s 
          AND account_name = %s 
          AND fetched_at >= %s
        ORDER BY fetched_at DESC
        LIMIT %s
    """, (platform.lower(), account.lower(), time_limit, limit))

    rows = cur.fetchall()
    cur.close()
    db.close()

    return [dict(row) for row in rows]

# ===================== CACHE OPERATIONS =====================
def get_cached_posts(platform: str, account: str) -> Optional[List[dict]]:
    """Return cached posts if still valid."""
    key = _cache_key(platform, account)
    if key in _cache:
        entry = _cache[key]
        if datetime.utcnow() - entry["last_fetch"] < CACHE_DURATION:
            return entry["posts"]
    return None

def update_cache(platform: str, account: str, posts: List[dict]):
    """Update in-memory cache with new posts."""
    key = _cache_key(platform, account)
    hashes = {generate_post_hash(platform, account, p["url"]) for p in posts}

    _cache[key] = {
        "last_fetch": datetime.utcnow(),
        "posts": posts,
        "hashes": hashes
    }

# ===================== MAIN FETCH LOGIC =====================
def fetch_latest_posts(platform: str, account: str) -> List[dict]:
    """
    Main function called by bot.
    1. Check in-memory cache
    2. Check DB for recent posts
    3. If none → fetch from platform API (placeholder)
    4. Save new posts + update cache
    """
    account = account.lstrip('@')

    # 1. Try cache
    cached = get_cached_posts(platform, account)
    if cached:
        return cached

    # 2. Try DB
    db_posts = get_recent_posts(platform, account)
    if db_posts:
        update_cache(platform, account, db_posts)
        return db_posts

    # 3. No cache/DB → fetch fresh (PLACEHOLDER — replace with real fetcher)
    print(f"Fetching fresh posts from {platform} for @{account}...")
    
    # === DUMMY DATA FOR TESTING ===
    dummy_posts = [
        {
            "url": f"https://x.com/{account}/status/1234567890{i}",
            "text": f"Sample post {i} from @{account} #testing",
            "media_urls": [],
            "top_comments": [f"Comment {j} on post {i}" for j in range(1, 4)]
        }
        for i in range(1, 6)
    ]
    # =================================

    # Save new posts to DB
    for post in dummy_posts:
        save_post(platform, account, post)

    # Get fresh from DB and cache
    fresh_posts = get_recent_posts(platform, account)
    update_cache(platform, account, fresh_posts)

    return fresh_posts

# ===================== PLATFORM-SPECIFIC FETCHERS (TO ADD LATER) =====================
# Example placeholder for real implementation
# async def fetch_x_posts(account: str) -> List[dict]:
#     # Use snscrape, tweepy, or X API
#     pass

# async def fetch_instagram_posts(account: str) -> List[dict]:
#     # Use instaloader or unofficial API
#     pass