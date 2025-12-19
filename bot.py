# bot.py - Full Powerful & UI-Friendly Version
import os
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram.constants import ChatAction
from utils import fetch_latest_urls, fetch_ig_urls

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

# ===================== PAGINATION & NAVIGATION =====================
POSTS_PER_PAGE = 5  # How many posts per page

def build_pagination_keyboard(page: int, total_pages: int, platform: str, account: str) -> InlineKeyboardMarkup:
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{platform}_{account}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{platform}_{account}"))

    return InlineKeyboardMarkup([buttons])

def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("X (Twitter)", callback_data="menu_x")],
        [InlineKeyboardButton("Instagram", callback_data="menu_ig")],
        [InlineKeyboardButton("Help / Guide", callback_data="help")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ===================== HELPER FUNCTIONS =====================
async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    try:
        await context.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except:
        pass

# ===================== COMMANDS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Welcome to MoorelinkBot! 🔥\n\n"
        "This bot dey help you see latest posts from X (Twitter) and Instagram sharp sharp.\n\n"
        "Commands:\n"
        "/menu - See main menu with buttons\n"
        "/xlatest <username> - Get X posts\n"
        "/iglatest <username> - Get IG posts\n"
        "/help - Full guide\n\n"
        "Enjoy! 😎"
    )
    await update.message.reply_text(text, reply_markup=build_main_menu())

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Choose platform:", reply_markup=build_main_menu())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 MoorelinkBot Guide (English + Pidgin)\n\n"
        "Commands:\n"
        "/start - Welcome + menu\n"
        "/menu - Open main menu\n"
        "/xlatest <username> - X (Twitter) latest posts\n"
        "Example: /xlatest vdm\n\n"
        "/iglatest <username> - Instagram latest posts\n"
        "Example: /iglatest davido\n\n"
        "/help - This message\n\n"
        "Pidgin Version:\n"
        "Bros, use /xlatest <name> make you see Twitter posts\n"
        "/iglatest <name> make you see IG posts\n"
        "Posts go auto delete after 24 hours.\n"
        "Enjoy the vibe! 😎🔥"
    )
    await update.message.reply_text(help_text)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "menu_x":
        await query.edit_message_text("Send /xlatest <username> to get X posts!")
    elif data == "menu_ig":
        await query.edit_message_text("Send /iglatest <username> to get IG posts!")
    elif data == "help":
        await help_command(update, context)
    elif data.startswith("page_"):
        # Handle pagination
        parts = data.split("_")
        page = int(parts[1])
        platform = parts[2]
        account = parts[3]

        # Re-fetch posts (assume you have posts in context.user_data or DB)
        # For simplicity, let's assume we re-fetch
        posts = fetch_latest_urls(platform, account) if platform == "x" else fetch_ig_urls(account)

        start = page * POSTS_PER_PAGE
        end = start + POSTS_PER_PAGE
        page_posts = posts[start:end]

        msg = f"Page {page + 1} of {len(posts) // POSTS_PER_PAGE + 1}\n\n"
        for post in page_posts:
            msg += f"🔗 {post['url']}\n"

        keyboard = build_pagination_keyboard(page, len(posts) // POSTS_PER_PAGE + 1, platform, account)
        await query.edit_message_text(msg, reply_markup=keyboard)

# ===================== MAIN FETCH HANDLER =====================
async def xlatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /xlatest <username>\nExample: /xlatest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "x"

    await update.message.chat.send_action(ChatAction.TYPING)

    posts = fetch_latest_urls(platform, account)

    if not posts:
        await update.message.reply_text(f"No recent public posts found for @{account} on X 😕")
        return

    intro_msg = await update.message.reply_text(f"🔥 Latest {len(posts)} posts from @{account} on X:")

    sent_message_ids = []

    for url in posts:
        fixed_url = url.replace("x.com", "fixupx.com").replace("twitter.com", "fixupx.com")
        sent_msg = await update.message.reply_text(fixed_url, disable_web_page_preview=False)
        sent_message_ids.append(sent_msg.message_id)
        await asyncio.sleep(3)

    # Auto-delete
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

async def iglatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /iglatest <username>\nExample: /iglatest chiomaavril")
        return

    account = context.args[0].lstrip('@').lower()

    await update.message.chat.send_action(ChatAction.TYPING)

    posts = fetch_ig_urls(account)

    if not posts:
        await update.message.reply_text(f"No recent public posts found for @{account} on IG 😕")
        return

    intro_msg = await update.message.reply_text(f"🔥 Latest {len(posts)} public IG posts from @{account}:")

    sent_message_ids = []

    for post in posts:
        caption = post.get('caption', '').strip()[:1024]
        media_url = post.get('media_url')

        msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>"

        try:
            if post.get('is_video'):
                sent_msg = await update.message.reply_video(
                    video=media_url,
                    caption=msg,
                    parse_mode="HTML"
                )
            else:
                sent_msg = await update.message.reply_photo(
                    photo=media_url,
                    caption=msg,
                    parse_mode="HTML"
                )
        except:
            sent_msg = await update.message.reply_text(msg, parse_mode="HTML")

        sent_message_ids.append(sent_msg.message_id)
        await asyncio.sleep(5)

    # Auto-delete
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

    await update.message.reply_text("Posts sent! They will auto delete in 24hrs.")

# ===================== MAIN =====================
if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("xlatest", xlatest))
    app.add_handler(CommandHandler("iglatest", iglatest))
    app.add_handler(CallbackQueryHandler(button_handler))

    print("🤖 PostBot started! Powerful & UI-friendly mode ON!")
    app.run_polling(drop_pending_updates=True)