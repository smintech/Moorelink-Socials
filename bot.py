import os
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ===================== CONFIG =====================
DB_URL = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

POST_FETCH_LIMIT = 5      # How many recent posts to show
CACHE_HOURS = 24          # Cache validity period

# ===================== DB HELPERS =====================
def get_db_connection():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def generate_post_id(account: str, post_url: str) -> str:
    """Generate unique ID for deduplication"""
    return hashlib.sha256(f"{account.lower()}:{post_url}".encode()).hexdigest()

def save_posts(platform: str, account: str, posts_data: list):
    """Save new posts to DB, skip duplicates"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    for post in posts_data:
        post_id = generate_post_id(account, post['url'])
        cur.execute("""
            INSERT INTO social_posts (id, platform, account_name, post_url, content_text, media_urls, top_comments, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE
            SET fetched_at = NOW(),
                top_comments = EXCLUDED.top_comments
        """, (
            post_id,
            platform,
            account.lower(),
            post['url'],
            post.get('text', '')[:500],  # truncate long text
            post.get('media_urls', []),
            post.get('top_comments', [])
        ))
    
    conn.commit()
    cur.close()
    conn.close()

def get_recent_posts(account: str, platform: str = None) -> list:
    """Fetch cached recent posts for an account"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    time_limit = datetime.utcnow() - timedelta(hours=CACHE_HOURS)
    
    query = """
        SELECT post_url, content_text, media_urls, top_comments, fetched_at
        FROM social_posts
        WHERE account_name = %s
          AND fetched_at >= %s
    """
    params = [account.lower(), time_limit]
    
    if platform:
        query += " AND platform = %s"
        params.append(platform)
    
    query += " ORDER BY fetched_at DESC LIMIT %s"
    params.append(POST_FETCH_LIMIT)
    
    cur.execute(query, params)
    posts = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(p) for p in posts]

# ===================== PLATFORM FETCHERS =====================
# Start with X (Twitter) — easiest to implement first
async def fetch_x_posts(account: str) -> list:
    """Fetch latest posts from X (Twitter) — placeholder for now"""
    # TODO: Use snscrape, tweepy, or unofficial API
    # For now, return dummy data for testing
    return [
        {
            "url": f"https://x.com/{account}/status/123456789",
            "text": "This is a sample post from @{account}",
            "media_urls": [],
            "top_comments": ["Great post!", "True talk", "🔥"]
        }
    ]

# You can add Instagram, TikTok later like this:
# async def fetch_instagram_posts(account: str) -> list: ...
# async def fetch_tiktok_posts(account: str) -> list: ...

# ===================== COMMAND HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome! 👋\n\n"
        "Use /latest <username> to get recent posts from any account.\n"
        "Examples:\n"
        "/latest vdm\n"
        "/latest davido\n"
        "/latest burnaboy"
    )

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"  # Default to X for now — extend later

    await update.message.reply_chat_action("typing")

    # 1. Try cache first
    cached_posts = get_recent_posts(account, platform)

    if cached_posts:
        await send_posts(update, cached_posts, account)
        return

    # 2. No cache → fetch fresh
    await update.message.reply_text(f"Fetching latest posts from @{account}... ⏳")

    try:
        if platform == "x":
            new_posts = await fetch_x_posts(account)
        # elif platform == "instagram":
        #     new_posts = await fetch_instagram_posts(account)
        # elif platform == "tiktok":
        #     new_posts = await fetch_tiktok_posts(account)
        else:
            await update.message.reply_text("Platform not supported yet.")
            return

        if not new_posts:
            await update.message.reply_text(f"No public posts found for @{account}.")
            return

        # Save to DB
        save_posts(platform, account, new_posts)

        # Get fresh from DB (now cached)
        fresh_posts = get_recent_posts(account, platform)
        await send_posts(update, fresh_posts, account)

    except Exception as e:
        print(f"Fetch error: {e}")
        await update.message.reply_text("Sorry, couldn't fetch posts right now. Try again later.")

async def send_posts(update: Update, posts: list, account: str):
    """Send posts beautifully to user"""
    await update.message.reply_text(f"Latest from @{account} ({len(posts)} posts):")

    for post in posts:
        msg = f"🔗 <a href='{post['post_url']}'>View Post</a>\n"

        if post['content_text']:
            msg += f"\n{post['content_text'][:300]}{'...' if len(post['content_text']) > 300 else ''}\n"

        if post['top_comments']:
            msg += "\n💬 <b>Top comments:</b>\n"
            for comment in post['top_comments'][:3]:
                msg += f"• {comment}\n"

        # Send text
        await update.message.reply_html(msg)

        # Send media if any
        if post['media_urls']:
            for media_url in post['media_urls'][:4]:  # Max 4 media
                try:
                    if media_url.endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                        await update.message.reply_photo(media_url)
                    elif media_url.endswith(('.mp4', '.mov')):
                        await update.message.reply_video(media_url)
                except:
                    pass  # Skip broken media

# ===================== MAIN =====================
if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")
    if not DB_URL:
        raise ValueError("DATABASE_URL environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("latest", latest))

    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)