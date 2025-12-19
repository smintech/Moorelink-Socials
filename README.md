# Moorelink-Socials
# bot.py - Powerful, UI-Friendly, Paginated Version (Pidgin Style)
import os
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram.constants import ChatAction
from utils import fetch_latest_urls, fetch_ig_urls

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")

# ===================== PAGINATION HELPER =====================
POSTS_PER_PAGE = 5

def get_pagination_keyboard(page: int, total: int, command: str, account: str):
    keyboard = []
    if page > 0:
        keyboard.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{command}_{account}_{page-1}"))
    if (page + 1) * POSTS_PER_PAGE < total:
        keyboard.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{command}_{account}_{page+1}"))
    return InlineKeyboardMarkup([keyboard]) if keyboard else None

# ===================== COMMANDS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to MoorelinkBot!\n\n"
        "Dis bot dey help you see latest posts from X or IG sharp sharp!\n\n"
        "Commands:\n"
        "/menu - Open main menu\n"
        "/latest <username> - Auto detect X or IG\n"
        "/xlatest <username> - X (Twitter) posts\n"
        "/iglatest <username> - Instagram posts with media & caption\n"
        "/help - Full guide\n\n"
        "E go auto-delete messages after 24hrs make chat clean 😎"
    )

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("X (Twitter) Latest", callback_data="menu_xlatest")],
        [InlineKeyboardButton("IG Latest", callback_data="menu_iglatest")],
        [InlineKeyboardButton("Help / Guide", callback_data="menu_help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose wetin you wan do:", reply_markup=reply_markup)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 MoorelinkBot Guide (Pidgin)\n\n"
        "How e dey work:\n"
        "- /latest <username> → auto detect if na X or IG\n"
        "- /xlatest <username> → only X (Twitter) posts\n"
        "- /iglatest <username> → IG posts with pics/videos & caption\n\n"
        "Navigation:\n"
        "If plenty posts, you go see ⬅️ Previous / Next ➡️ buttons\n"
        "Click am to see more\n\n"
        "Extra:\n"
        "- Messages auto-delete after 24hrs\n"
        "- Type /menu anytime to see buttons\n"
        "- Enjoy am free & clean 😎\n\n"
        "Questions? Just ask!"
    )
    await update.message.reply_text(help_text)

# ===================== BUTTON HANDLER =====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("page_"):
        _, command, account, page_str = data.split("_")
        page = int(page_str)

        if command == "xlatest":
            urls = fetch_latest_urls("x", account)
            paginated = urls[page * POSTS_PER_PAGE:(page + 1) * POSTS_PER_PAGE]
            keyboard = get_pagination_keyboard(page, len(urls), command, account)

            await query.edit_message_text(
                f"Latest X posts from @{account} (page {page+1}):",
                reply_markup=keyboard
            )
            for url in paginated:
                await query.message.reply_text(url)

    elif data == "menu_xlatest":
        await query.edit_message_text("Send /xlatest <username> to get X posts!")
    elif data == "menu_iglatest":
        await query.edit_message_text("Send /iglatest <username> to get IG posts!")
    elif data == "menu_help":
        await help_command(update, context)

# ===================== MAIN FETCH HANDLER =====================
async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()

    # Auto-detect platform (simple rule - customize later)
    platform = "ig" if account in ["davido", "verydarkblackman", "wizkidayo"] else "x"

    await update.message.chat.send_action(ChatAction.TYPING)

    if platform == "x":
        urls = fetch_latest_urls(platform, account)
        if not urls:
            await update.message.reply_text(f"No recent public posts found for @{account} on X 😕")
            return

        intro_msg = await update.message.reply_text(f"🔥 Latest {len(urls)} posts from @{account} on X:")

        sent_ids = []
        for url in urls:
            sent_msg = await update.message.reply_text(url)
            sent_ids.append(sent_msg.message_id)
            await asyncio.sleep(5)

    else:  # IG
        posts = fetch_ig_urls(account)
        if not posts:
            await update.message.reply_text(f"No recent public posts found for @{account} on IG 😕")
            return

        intro_msg = await update.message.reply_text(f"🔥 Latest {len(posts)} public IG posts from @{account}:")

        sent_ids = []
        for post in posts:
            caption = post.get('caption', '').strip()[:1024]
            msg_caption = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>"

            try:
                if post.get('is_video'):
                    sent_msg = await update.message.reply_video(
                        video=post['media_url'],
                        caption=msg_caption,
                        parse_mode="HTML"
                    )
                else:
                    sent_msg = await update.message.reply_photo(
                        photo=post['media_url'],
                        caption=msg_caption,
                        parse_mode="HTML"
                    )
            except Exception as e:
                print(f"Send error: {e}")
                sent_msg = await update.message.reply_text(msg_caption, parse_mode="HTML")

            sent_ids.append(sent_msg.message_id)
            await asyncio.sleep(5)

    # Auto-delete
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

    await update.message.reply_text("Posts sent! They will auto-delete in 24hrs.")

async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    try:
        await context.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except:
        pass

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("latest", latest))
    app.add_handler(CallbackQueryHandler(button_handler))

    print("🤖 PostBot started! Powerful & UI-friendly mode ON!")
    app.run_polling(drop_pending_updates=True)