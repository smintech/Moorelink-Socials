import os
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.constants import ChatAction
from utils import fetch_latest_urls, fetch_ig_urls
from telegram.ext import JobQueue
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to TweetLinkBot!\n\n"
        "Use: /latest <username>\n"
        "Example: /xlatest vdm or /xlatest elonmusk\n\n"
        "Shows recent posts with full preview (text, images, videos, likes, etc.)"
    )

async def xlatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /xlatest <username>\nExample: /xlatest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"

    await update.message.chat.send_action(ChatAction.TYPING)

    urls = fetch_latest_urls(platform, account)

    if not urls:
        no_posts_msg = await update.message.reply_text(f"No recent public posts found for @{account} 😕\nTry later or check spelling.")
        # Auto delete "no posts" message after 24 hours
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": no_posts_msg.chat.id, "message_id": no_posts_msg.message_id})
        return

    # Send intro message
    intro_msg = await update.message.reply_text(f"🔥 Latest {len(urls)} posts from @{account}:")

    sent_message_ids = []  # Collect all sent message IDs

    for url in urls:
        fixed_url = url.replace("x.com", "fixupx.com").replace("twitter.com", "fixupx.com")  # or vxtwitter/fxtwitter

        sent_msg = await update.message.reply_text(
            fixed_url,
            disable_web_page_preview=False
        )

        sent_message_ids.append(sent_msg.message_id)

        await asyncio.sleep(5)

    # Auto-delete the intro message after 24 hours
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})

    # Auto-delete each post link after 24 hours
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

# Helper function to delete message
async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data["chat_id"]
    message_id = job_data["message_id"]
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        print(f"Auto-deleted message {message_id} in chat {chat_id}")
    except Exception as e:
        print(f"Failed to delete message {message_id}: {e}")  # Prevent flood + give time for preview to load

async def iglatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: /iglatest <username>\nExample: /iglatest chiomaavril"
        )
        return

    account = context.args[0].lstrip('@').lower()

    # Show typing indicator
    await update.message.chat.send_action(ChatAction.TYPING)

    # Fetch real IG public post URLs using instaloader
    urls = fetch_ig_urls(account)

    if not urls:
        await update.message.reply_text(
            f"No recent public posts found for @{account} on Instagram 😕\n"
            "Account might be private, no posts, or temporarily unavailable."
        )
        return

    # Send intro message
    intro_msg = await update.message.reply_text(
        f"🔥 Latest {len(urls)} public IG posts from @{account}:"
    )

    sent_message_ids = []

    # Send each URL with 5-second delay
    for url in urls:
        sent_msg = await update.message.reply_text(
            url,
            disable_web_page_preview=False
        )
        sent_message_ids.append(sent_msg.message_id)
        await asyncio.sleep(5)  # Avoid Telegram flood

    # Schedule auto-delete after 24 hours (86400 seconds)
    context.job_queue.run_once(
        delete_message,
        86400,
        data={"chat_id": intro_msg.chat_id, "message_id": intro_msg.message_id}
    )

    for msg_id in sent_message_ids:
        context.job_queue.run_once(
            delete_message,
            86400,
            data={"chat_id": update.message.chat_id, "message_id": msg_id}
        )

    await update.message.reply_text("Posts sent! They will auto-delete in 24 hours.")

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("xlatest", xlatest))
    app.add_handler(CommandHandler("iglatest", iglatest))
    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)