# bot.py - Standalone Telegram Bot
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from utils import fetch_latest_urls
from utils import fetch_preview
from telegram.constants import ChatAction
from telegram import InputMediaPhoto
import asyncio
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

    await update.message.chat.send_action(ChatAction.TYPING)

    urls = fetch_latest_urls(platform, account)

    if not urls:
        await update.message.reply_text(f"No recent public tweets found for @{account} 😕\nTry later or check spelling.")
        return

    await update.message.reply_text(f"🔥 Latest {len(urls)} public posts from @{account}:\n\nFetching previews... ⏳")

    for url in urls:
        preview = await asyncio.get_event_loop().run_in_executor(None, fetch_preview, url)

        title = preview["title"].strip() or "X Post"
        desc = preview["description"].strip()
        image = preview["image"].strip()

        link_line = f"\n🔗 <a href='{url}'>View on X</a>"

        if image:
            caption = f"<b>{title}</b>\n\n"
            if desc:
                if len(desc) > 200:
                    desc = desc[:200] + "..."
                caption += f"{desc}\n"
            caption += link_line

            try:
                await update.message.reply_photo(
                    photo=image,
                    caption=caption,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            except Exception as e:
                print(f"Photo send failed: {e}")
                fallback_msg = f"<b>{title}</b>\n\n{desc}{link_line}"
                await update.message.reply_text(fallback_msg, parse_mode="HTML", disable_web_page_preview=False)
        else:
            msg = f"<b>{title}</b>\n\n{desc}{link_line}"
            await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=False)

        await asyncio.sleep(1)  # Prevent flood

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("latest", latest))

    print("🤖 Bot started! Waiting for commands...")
    app.run_polling(drop_pending_updates=True)