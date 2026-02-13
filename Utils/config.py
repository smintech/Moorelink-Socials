import os

# ================ CONFIG ================
DB_URL = os.getenv("DATABASE_URL")                       # main cache DB (social posts)
TG_DB_URL = os.getenv("USERS_DATABASE_URL") or os.getenv("TG_DB_URL")   # separate TG DB
CACHE_HOURS = 24
POST_LIMIT = 10
GROQ_API_KEY = os.getenv("GROQ_KEY")
RAPIDAPI_KEY = os.getenv("RAPID_API")
RAPIDAPI_HOST = 'facebook-pages-scraper2.p.rapidapi.com'
RAPIDAPI_BASE = f"https://{RAPIDAPI_HOST}"
RAPIDAPIHOST = "twitter-x-api.p.rapidapi.com"
APIFY_FALLBACK_TIMEOUT = 8
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY")  # Add your Apify token to env
APIFY_ACTOR_ID = "apidojo~tweet-scraper"
APIFY_BASE = "https://api.apify.com/v2"
TWEETS_URL = "https://twitter-x-api.p.rapidapi.com/api/user/tweets"

# Admin IDs from environment (comma-separated)
ADMIN_IDS_ENV = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = []
if ADMIN_IDS_ENV:
    try:
        ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_ENV.split(",") if x.strip()]
    except Exception:
        ADMIN_IDS = []

# Badge levels definition
BADGE_LEVELS = [
    {
        "name": "Basic",
        "emoji": "🪪",
        "invites_needed": 0,
        "save_slots": 5,
        "limits": {"min": 2, "hour": 5, "day": 10}
    },
    {
        "name": "Bronze",
        "emoji": "🥉",
        "invites_needed": 5,
        "save_slots": 10,
        "limits": {"min": 4, "hour": 10, "day": 15}
    },
    {
        "name": "Silver",
        "emoji": "🥈",
        "invites_needed": 15,
        "save_slots": 15,
        "limits": {"min": 8, "hour": 15, "day": 20}
    },
    {
        "name": "Gold",
        "emoji": "🥇",
        "invites_needed": 25,
        "save_slots": 25,
        "limits": {"min": 15, "hour": 20, "day": 25}
    },
    {
        "name": "Diamond",
        "emoji": "💎",
        "invites_needed": 100,
        "save_slots": float('inf'),
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