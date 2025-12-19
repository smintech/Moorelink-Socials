import os
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.constants import ChatAction
from utils import fetch_latest_urls

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to TweetLinkBot!\n\n"
        "Use: /latest <username>\n"
        "Example: /latest vdm or /latest elonmusk\n\n"
        "Shows recent posts with full preview (text, images, videos, likes, etc.)"
    )

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"

    await update.message.chat.send_action(ChatAction.TYPING)

    urls = fetch_latest_urls(platform, account)

    if not urls:
        await update.message.reply_text(f"No recent public posts found for @{account} 😕\nTry later or check spelling.")
        return

    await update.message.reply_text(f"🔥 Latest {len(urls)} posts from @{account}:")

    for url in urls:
        # Convert to vxtwitter for perfect Telegram preview
        fixed_url = url.replace("x.com", "fixupx.com").replace("twitter.com", "fixupx.com")

        # Send only the fixed link — Telegram go show rich embed automatic
        await update.message.reply_text(
            fixed_url,
            disable_web_page_preview=False  # MUST be False to allow preview
        )

        await asyncio.sleep(5)  # Prevent flood + give time for preview to load

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("latest", latest))

    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)