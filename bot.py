import os
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from datetime import datetime, timedelta

# ===== CONFIG =====
DB_URL = os.getenv("DATABASE_URL")  # Your Postgres URL
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")  # Bot token from BotFather
POST_FETCH_LIMIT = 5  # How many posts to deliver per request

# ===== DB HELPER =====
def get_db():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def fetch_recent_posts(account: str, hours: int = 24):
    db = get_db()
    cur = db.cursor()
    time_limit = datetime.utcnow() - timedelta(hours=hours)
    cur.execute("""
        SELECT account_name, post_url, top_comments, fetched_at
        FROM social_posts
        WHERE account_name = %s AND fetched_at >= %s
        ORDER BY fetched_at DESC
        LIMIT %s
    """, (account, time_limit, POST_FETCH_LIMIT))
    posts = cur.fetchall()
    cur.close()
    db.close()
    return posts

# ===== TELEGRAM COMMAND =====
async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) == 0:
        await update.message.reply_text("Usage: /latest <account>")
        return

    account = context.args[0]
    posts = fetch_recent_posts(account)

    if not posts:
        await update.message.reply_text(f"No recent posts found for {account}.")
        return

    for post in posts:
        text = f"{post['post_url']}"
        if post['top_comments']:
            text += f"\nTop comments: {post['top_comments']}"
        await update.message.reply_text(text)

# ===== START BOT =====
if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("latest", latest))
    print("Bot started...")
    app.run_polling()