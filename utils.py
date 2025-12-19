# utils.py
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from app import get_db  # Import your Flask app's get_db function
from twikit import Client

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
def fetch_x_posts(account: str) -> list:
    """Fetch real latest posts from X using twikit"""
    account = account.lstrip('@')
    posts = []

    try:
        client = Client('en-US')
        # No login needed for public tweets
        user = client.get_user_by_screen_name(account)
        if not user:
            print(f"User @{account} not found")
            return []

        tweets = client.get_user_tweets(user.id, count=10)

        for tweet in tweets:
            # Skip replies/retweets
            if tweet.in_reply_to or tweet.is_retweet:
                continue

            media_urls = []
            if tweet.media:
                for media in tweet.media:
                    media_urls.append(media.get('media_url_https', ''))

            posts.append({
                "url": f"https://x.com/{account}/status/{tweet.id}",
                "text": tweet.text or "",
                "media_urls": media_urls,
                "top_comments": []  # twikit no get replies easy
            })

        print(f"Fetched {len(posts)} posts from @{account}")

    except Exception as e:
        print(f"Twikit error for @{account}: {e}")

    return posts