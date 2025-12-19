# bot.py - Complete Telegram Bot with Real X Fetching (snscrape)
import os
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict

import psycopg2
from psycopg2.extras import RealDictCursor
import snscrape.modules.twitter as sntwitter

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ===================== CONFIG =====================
DB_URL = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

POST_FETCH_LIMIT = 5      # Max posts to show per request
CACHE_HOURS = 24          # Cache validity period

# ===================== DB HELPERS =====================
def get_db():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def generate_post_id(account: str, post_url: str) -> str:
    """Generate unique hash ID for deduplication"""
    data = f"{account.lower()}:{post_url}"
    return hashlib.sha256(data.encode()).hexdigest()

def save_posts(platform: str, account: str, posts_data: list):
    """Save new posts to DB, skip duplicates"""
    conn = get_db()
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
            post.get('text', '')[:500],
            post.get('media_urls', []),
            post.get('top_comments', [])
        ))
    
    conn.commit()
    cur.close()
    conn.close()

def get_recent_posts(account: str, platform: str = None) -> list:
    """Fetch cached recent posts for an account"""
    conn = get_db()
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

# ===================== REAL X FETCHER =====================
def fetch_x_posts(account: str) -> list:
    """Fetch real latest posts from X (Twitter) using snscrape"""
    account = account.lstrip('@')
    posts = []

    try:
        # Fetch up to 10 recent tweets
        for i, tweet in enumerate(sntwitter.TwitterUserScraper(account).get_items()):
            if i >= 10:
                break

            # Skip replies and retweets (optional)
            if tweet.inReplyToTweetId or tweet.retweetedTweet:
                continue

            # Extract media URLs
            media_urls = []
            if tweet.media:
                for media in tweet.media:
                    if hasattr(media, 'fullUrl'):
                        media_urls.append(media.fullUrl)
                    elif hasattr(media, 'previewUrl'):
                        media_urls.append(media.previewUrl)

            # Top comments - snscrape doesn't fetch replies easily, leave empty
            top_comments = []

            posts.append({
                "url": tweet.url,
                "text": tweet.rawContent or "",
                "media_urls": media_urls,
                "top_comments": top_comments
            })

        print(f"Fetched {len(posts)} posts from @{account}")

    except Exception as e:
        print(f"Error fetching @{account}: {e}")

    return posts

# ===================== COMMAND HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to PostBot!\n\n"
        "Use: /latest <username>\n"
        "Example: /latest vdm\n\n"
        "Supports X (Twitter) for now — more coming soon!"
    )

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"  # Default to X

    await update.message.reply_chat_action("typing")

    # 1. Try DB cache first
    cached_posts = get_recent_posts(account, platform)

    if cached_posts:
        await send_posts(update, cached_posts, account)
        return

    # 2. No cache → fetch fresh
    await update.message.reply_text(f"Fetching latest posts from @{account}... ⏳")

    try:
        new_posts = fetch_x_posts(account)

        if not new_posts:
            await update.message.reply_text(f"No public posts found for @{account}.")
            return

        # Save to DB
        save_posts(platform, account, new_posts)

        # Get fresh from DB
        fresh_posts = get_recent_posts(account, platform)
        await send_posts(update, fresh_posts, account)

    except Exception as e:
        print(f"Fetch error: {e}")
        await update.message.reply_text("Sorry, couldn't fetch posts right now. Try again later.")

async def send_posts(update: Update, posts: list, account: str):
    """Send posts beautifully to user"""
    await update.message.reply_text(f"Latest {len(posts)} posts from @{account}:")

    for post in posts:
        msg = f"🔗 <a href='{post['post_url']}'>View Post</a>\n"

        if post['content_text']:
            text = post['content_text'][:300]
            msg += f"\n{text}{'...' if len(post['content_text']) > 300 else ''}\n"

        if post['top_comments']:
            msg += "\n💬 <b>Top comments:</b>\n"
            for comment in post['top_comments'][:3]:
                msg += f"• {comment}\n"

        # Send text
        await update.message.reply_html(msg)

        # Send media if any (max 4)
        for media_url in post['media_urls'][:4]:
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