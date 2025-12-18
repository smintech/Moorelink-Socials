import hashlib
import requests
from datetime import datetime, timedelta

# ===== In-memory cache =====
# Structure: { "VDM": { "last_fetch": datetime, "urls": [list of URLs], "hashes": set() } }
cache = {}

# ===== Constants =====
CACHE_DURATION = timedelta(minutes=30)  # Cache fresh for 30 mins
X_BASE_URL = "https://x.com"  # Replace with actual X base if needed


# ===== Generate SHA256 hash for a post URL =====
def generate_hash(platform: str, account: str, post_url: str) -> str:
    key = f"{platform}|{account}|{post_url}"
    return hashlib.sha256(key.encode()).hexdigest()


# ===== Fetch public post URLs (dummy example) =====
def fetch_posts(account: str, hours: int = 24):
    now = datetime.utcnow()

    # Check cache first
    if account in cache:
        last_fetch = cache[account]["last_fetch"]
        if now - last_fetch < CACHE_DURATION:
            return cache[account]["urls"]  # Return cached URLs

    # ===== FETCHING LOGIC =====
    # Placeholder: Replace with actual X API or scraping safe endpoint
    # Here we simulate 5 posts per account
    simulated_posts = [
        f"{X_BASE_URL}/{account}/status/{i}" for i in range(1, 6)
    ]

    # Filter by time if needed (for now we skip timestamps)
    new_posts = []
    account_hashes = cache.get(account, {}).get("hashes", set())

    for url in simulated_posts:
        post_hash = generate_hash("x", account, url)
        if post_hash not in account_hashes:
            new_posts.append(url)
            account_hashes.add(post_hash)

    # Update cache
    cache[account] = {
        "last_fetch": now,
        "urls": new_posts,
        "hashes": account_hashes,
    }

    return new_posts