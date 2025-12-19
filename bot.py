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
        await update.message.reply_text("Usage: /iglatest <username>\nExample: /iglatest chiomaavril")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "ig"

    await update.message.chat.send_action(ChatAction.TYPING)

    # Fetch real IG data with instaloader
    posts = fetch_ig_urls(account)  # This now returns full post data (url, text, media_urls)

    if not posts:
        await update.message.reply_text(f"No recent public posts found for @{account} on Instagram 😕")
        return

    intro_msg = await update.message.reply_text(f"🔥 Latest {len(posts)} public IG posts from @{account}:")

    sent_message_ids = []

    for post in posts:
        caption = post.get('caption', '').strip()
        if len(caption) > 1000:
            caption = caption[:997] + "..."

        first_media = post.get('media_urls', [None])[0]  # First media

        try:
            if first_media:
                if any(first_media.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    sent_msg = await update.message.reply_photo(
                        photo=first_media,
                        caption=f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>",
                        parse_mode="HTML"
                    )
                elif any(first_media.lower().endswith(ext) for ext in ['.mp4', '.mov']):
                    sent_msg = await update.message.reply_video(
                        video=first_media,
                        caption=f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>",
                        parse_mode="HTML"
                    )
                else:
                    sent_msg = await update.message.reply_text(
                        f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>",
                        parse_mode="HTML"
                    )
            else:
                sent_msg = await update.message.reply_text(
                    f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>",
                    parse_mode="HTML"
                )
        except Exception as e:
            print(f"IG media send error: {e}")
            sent_msg = await update.message.reply_text(
                f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>",
                parse_mode="HTML"
            )

        sent_message_ids.append(sent_msg.message_id)
        await asyncio.sleep(5)

    # Auto-delete intro and messages after 24 hours
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

    await update.message.reply_text("Posts sent! They will auto-delete in 24 hours.")

async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    try:
        await context.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except Exception as e:
        print(f"Delete failed: {e}")

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("xlatest", xlatest))
    app.add_handler(CommandHandler("iglatest", iglatest))
    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)