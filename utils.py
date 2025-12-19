# utils.py
from app import get_db
import hashlib
from datetime import datetime, timedelta

# ====== DATABASE HELPERS ======

def generate_id(account, post_url):
    """Generate a unique hash ID for a post."""
    data = f"{account}:{post_url}"
    return hashlib.sha256(data.encode()).hexdigest()

def save_post(account, post_url, top_comments=None):
    """Save or update post in the database using hash ID."""
    db = get_db()
    cur = db.cursor()
    
    post_id = generate_id(account, post_url)
    
    cur.execute("""
        INSERT INTO social_posts (id, account_name, post_url, top_comments)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET post_url = EXCLUDED.post_url,
            top_comments = EXCLUDED.top_comments,
            fetched_at = NOW()
    """, (post_id, account, post_url, top_comments))
    
    db.commit()
    cur.close()
    db.close()


def fetch_posts_from_db(account, limit=5):
    """Fetch latest posts from the database."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT post_url FROM social_posts
        WHERE account_name = %s
        ORDER BY fetched_at DESC
        LIMIT %s
    """, (account, limit))
    rows = cur.fetchall()
    cur.close()
    db.close()
    return [r[0] for r in rows]  # list of URLs


# ====== IN-MEMORY CACHE ======
# Structure: { "account": { "last_fetch": datetime, "urls": [...], "hashes": set() } }
cache = {}
CACHE_DURATION = timedelta(minutes=30)

# ====== HASH GENERATOR ======
def generate_hash(platform: str, account: str, post_url: str) -> str:
    """Generate SHA256 hash key for a post URL."""
    key = f"{platform}|{account}|{post_url}"
    return hashlib.sha256(key.encode()).hexdigest()


# ====== FETCH POSTS (DUMMY / SIMULATION) ======
X_BASE_URL = "https://x.com"  # Replace with real API endpoint later

def fetch_posts(account: str, hours: int = 24):
    """
    Fetch public post URLs for an account (last `hours`).
    Uses in-memory cache to avoid redundant fetching.
    """
    now = datetime.utcnow()

    # Check cache first
    if account in cache:
        last_fetch = cache[account]["last_fetch"]
        if now - last_fetch < CACHE_DURATION:
            return cache[account]["urls"]  # Return cached URLs

    # ===== PLACEHOLDER FETCHING LOGIC =====
    # Replace this with real API calls or scraping (safe)
    simulated_posts = [
        f"{X_BASE_URL}/{account}/status/{i}" for i in range(1, 6)
    ]

    # Filter new posts using hashes
    new_posts = []
    account_hashes = cache.get(account, {}).get("hashes", set())

    for url in simulated_posts:
        post_hash = generate_hash("x", account, url)
        if post_hash not in account_hashes:
            new_posts.append(url)
            account_hashes.add(post_hash)
            # Save post to DB
            save_post(account, url)

    # Update cache
    cache[account] = {
        "last_fetch": now,
        "urls": new_posts,
        "hashes": account_hashes,
    }

    return new_posts


# ====== USAGE EXAMPLE ======
if __name__ == "__main__":
    account = "VDM"
    posts = fetch_posts(account)
    print("Fetched posts:", posts)

    db_posts = fetch_posts_from_db(account)
    print("Posts in DB:", db_posts)