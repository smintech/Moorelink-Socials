# bot.py - Standalone Telegram Bot
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from utils import fetch_latest_urls

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to TweetLinkBot!\n\n"
        "Use: /latest <username>\n"
        "Example: /latest vdm\n"
        "Shows recent tweet links only (no text/media)."
    )

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"

    await update.message.reply_chat_action("typing")

    urls = fetch_latest_urls(platform, account)

    if not urls:
        await update.message.reply_text(f"No recent public tweets found for @{account}.")
        return

    msg = f"Latest {len(urls)} public tweet links from @{account}:\n\n"
    for i, url in enumerate(urls, 1):
        msg += f"{i}. {url}\n"

    await update.message.reply_text(msg)

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("latest", latest))

    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)