# bot.py - Full Powerful & UI-Friendly Version (with Admin Panel)
import os
import asyncio
import io
import csv
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from telegram.constants import ChatAction

# utils functions (expects these in your utils.py)
from utils import (
    fetch_latest_urls,
    fetch_ig_urls,
    add_or_update_tg_user,
    increment_tg_request_count,
    get_tg_user,
    ban_tg_user,
    unban_tg_user,
    list_active_tg_users,
)

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# ===================== CONFIG =====================
POSTS_PER_PAGE = 5
PAGE_SIZE_USERS = 10  # users per page in admin list

# ===================== UI BUILDERS =====================
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
        [InlineKeyboardButton("Help / Guide", callback_data="help")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_admin_menu():
    keyboard = [
        [InlineKeyboardButton("👥 List active users", callback_data="admin_list_users_0")],
        [InlineKeyboardButton("📤 Broadcast message", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("📥 Export users CSV", callback_data="admin_export_csv")],
        [InlineKeyboardButton("↩️ Back to main", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_user_row_buttons(telegram_id: int, first_name: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("View", callback_data=f"admin_view_{telegram_id}"),
            InlineKeyboardButton("Ban", callback_data=f"admin_ban_{telegram_id}"),
            InlineKeyboardButton("Unban", callback_data=f"admin_unban_{telegram_id}"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ===================== AUTH HELPERS =====================
def is_admin(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    return user_id in ADMIN_IDS

# ===================== USER DB HELPERS (wrapper) =====================
async def record_user_and_check_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Ensure the user exists in tg_users, increment request_count, and check ban status.
    Returns True if user is allowed to continue, False if banned.
    """
    user = update.effective_user
    if not user:
        return True  # allow unknown user

    telegram_id = user.id
    first_name = user.first_name or ""

    # Add or update user row
    try:
        add_or_update_tg_user(telegram_id, first_name)
    except Exception as e:
        print(f"[utils] add_or_update_tg_user error: {e}")

    # Increment request count
    try:
        increment_tg_request_count(telegram_id)
    except Exception as e:
        print(f"[utils] increment_tg_request_count error: {e}")

    # Fetch user row to see if banned
    try:
        row = get_tg_user(telegram_id)
        if row and ("is_banned" in row) and int(row.get("is_banned", 0)) == 1:
            return False
    except Exception as e:
        print(f"[utils] get_tg_user error: {e}")

    return True

# ===================== HELPER: CSV EXPORT =====================
def users_to_csv_bytes(users: List[Dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["telegram_id", "first_name", "is_active", "is_banned", "request_count", "last_request_at", "joined_at"])
    for u in users:
        writer.writerow([
            u.get("telegram_id"),
            u.get("first_name"),
            u.get("is_active"),
            u.get("is_banned"),
            u.get("request_count"),
            u.get("last_request_at"),
            u.get("joined_at"),
        ])
    return buf.getvalue().encode("utf-8")

# ===================== DELETION JOB =====================
async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    try:
        await context.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except:
        pass

# ===================== COMMAND HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    text = (
        "👋 Welcome to MooreLinkBot! 🔥\n\n"
        "This bot dey help you see latest posts from X (Twitter) and Instagram sharp sharp.\n\n"
        "Commands:\n"
        "/menu - See main menu with buttons\n"
        "/latest <username> - Auto-detect X or IG\n"
        "/xlatest <username> - Get X posts\n"
        "/iglatest <username> - Get IG posts\n"
        "/help - Full guide\n\n"
        "Admin: /admin (admins only)\n\n"
        "Enjoy! 😎"
    )
    await update.message.reply_text(text, reply_markup=build_main_menu())

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return
    await update.message.reply_text("Choose platform:", reply_markup=build_main_menu())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        if update.message:
            await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    help_text = (
        "📖 MooreLinkBot Guide (English + Pidgin)\n\n"
        "Commands:\n"
        "/start - Welcome + menu\n"
        "/menu - Open main menu\n"
        "/latest <username> - Auto-detect X or IG\n"
        "/xlatest <username> - Get X (Twitter) latest posts\n"
        "/iglatest <username> - Get Instagram latest posts\n"
        "/help - This message\n\n"
        "Admin: /admin to open admin panel (admins only)\n"
    )
    await update.message.reply_text(help_text)

# ===================== ADMIN: SHOW PANEL =====================
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ You are not an admin.")
        return

    await update.message.reply_text("Admin panel:", reply_markup=build_admin_menu())

# ===================== CALLBACK HANDLER (including admin actions) =====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user

    # record user normally (this will create/update user and increment request_count)
    await record_user_and_check_ban(update, context)

    data = query.data

    # Admin panel entry
    if data == "menu_main":
        await query.edit_message_text("Main menu:", reply_markup=build_main_menu())
        return

    if data == "help":
        await help_command(update, context)
        return

    if data == "menu_x":
        context.user_data["platform"] = "x"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("🐦 Send the X (Twitter) username:")
        return

    if data == "menu_ig":
        context.user_data["platform"] = "ig"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("📸 Send the Instagram username:")
        return

    # ---------------- Admin actions ----------------
    if data.startswith("admin_"):
        # Only admins allowed
        if not is_admin(user.id):
            await query.edit_message_text("❌ Admins only.")
            return

        # admin_list_users_{page}
        if data.startswith("admin_list_users_"):
            _, _, page_s = data.partition("admin_list_users_")
            page = int(page_s or "0")
            users = list_active_tg_users(limit=1000)  # fetch many, paginate locally
            total = len(users)
            start = page * PAGE_SIZE_USERS
            end = start + PAGE_SIZE_USERS
            page_users = users[start:end]

            text = f"Active users (page {page+1}):\n\n"
            if not page_users:
                text += "No users found on this page."
            else:
                for u in page_users:
                    tid = u.get("telegram_id")
                    name = u.get("first_name") or ""
                    text += f"- {name} ({tid})\n"

            # build nav buttons
            buttons = []
            if page > 0:
                buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_users_{page-1}"))
            if end < total:
                buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users_{page+1}"))
            # per-user quick actions (to save space, add View/Ban/Unban under same message)
            for u in page_users:
                uid = u.get("telegram_id")
                buttons.append(InlineKeyboardButton(f"View {uid}", callback_data=f"admin_view_{uid}"))
            buttons.append(InlineKeyboardButton("↩️ Back", callback_data="admin_back"))

            # slice keyboard into rows of 3
            rows = []
            # first pagination row:
            rows.append(buttons[:2])  # prev/next
            # then user view buttons each on its own row
            for btn in buttons[2:-1]:
                rows.append([btn])
            rows.append([buttons[-1]])  # back

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
            return

        if data == "admin_back":
            await query.edit_message_text("Admin panel:", reply_markup=build_admin_menu())
            return

        if data == "admin_export_csv":
            await query.edit_message_text("Preparing CSV...")

            # fetch active users and export CSV
            users = list_active_tg_users(limit=10000)
            csv_bytes = users_to_csv_bytes(users)
            bio = io.BytesIO(csv_bytes)
            bio.name = "tg_users.csv"

            await context.bot.send_document(chat_id=user.id, document=InputFile(bio), filename="tg_users.csv")
            await query.edit_message_text("CSV exported and sent to your chat.")
            return

        if data == "admin_broadcast_start":
            context.user_data["admin_broadcast"] = True
            await query.edit_message_text("Send the message you want to broadcast to all active users. Send /cancel to abort.")
            return

        # view/ban/unban specific user
        if data.startswith("admin_view_"):
            _, _, id_s = data.partition("admin_view_")
            try:
                tid = int(id_s)
            except:
                await query.edit_message_text("Invalid user id.")
                return
            row = get_tg_user(tid)
            if not row:
                await query.edit_message_text(f"No user found with id {tid}.")
                return

            text = (
                f"User: {row.get('first_name')}\n"
                f"telegram_id: {row.get('telegram_id')}\n"
                f"is_active: {row.get('is_active')}\n"
                f"is_banned: {row.get('is_banned')}\n"
                f"request_count: {row.get('request_count')}\n"
                f"last_request_at: {row.get('last_request_at')}\n"
                f"joined_at: {row.get('joined_at')}\n"
            )
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Ban", callback_data=f"admin_ban_{tid}"),
                    InlineKeyboardButton("Unban", callback_data=f"admin_unban_{tid}"),
                    InlineKeyboardButton("Back", callback_data="admin_list_users_0"),
                ]
            ])
            await query.edit_message_text(text, reply_markup=keyboard)
            return

        if data.startswith("admin_ban_"):
            _, _, id_s = data.partition("admin_ban_")
            try:
                tid = int(id_s)
            except:
                await query.edit_message_text("Invalid user id.")
                return
            try:
                ban_tg_user(tid)
                await query.edit_message_text(f"User {tid} banned.")
            except Exception as e:
                await query.edit_message_text(f"Error banning user: {e}")
            return

        if data.startswith("admin_unban_"):
            _, _, id_s = data.partition("admin_unban_")
            try:
                tid = int(id_s)
            except:
                await query.edit_message_text("Invalid user id.")
                return
            try:
                unban_tg_user(tid)
                await query.edit_message_text(f"User {tid} unbanned.")
            except Exception as e:
                await query.edit_message_text(f"Error unbanning user: {e}")
            return

    # ---------------- Pagination for posts (existing) ----------------
    if data.startswith("page_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await query.edit_message_text("Invalid page data.")
            return
        page = int(parts[1])
        platform = parts[2]
        account = parts[3]
        posts = fetch_latest_urls(platform, account) if platform == "x" else fetch_ig_urls(account)
        start = page * POSTS_PER_PAGE
        end = start + POSTS_PER_PAGE
        page_posts = posts[start:end]
        total_pages = max(1, (len(posts) + POSTS_PER_PAGE - 1) // POSTS_PER_PAGE)
        msg = f"Page {page + 1} of {total_pages}\n\n"
        for post in page_posts:
            if isinstance(post, dict):
                msg += f"🔗 {post.get('url')}\n"
            else:
                msg += f"🔗 {post}\n"
        keyboard = build_pagination_keyboard(page, total_pages, platform, account)
        await query.edit_message_text(msg, reply_markup=keyboard)
        return

    # default fallback
    await query.edit_message_text("Unknown action.")

# ===================== MESSAGE HANDLER (including admin broadcast) =====================
async def handle_username_or_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # First, admin broadcast flow (admins only)
    if context.user_data.get("admin_broadcast"):
        user = update.effective_user
        if not is_admin(user.id):
            context.user_data.pop("admin_broadcast", None)
            await update.message.reply_text("❌ Only admins can broadcast.")
            return

        text_to_send = update.message.text
        await update.message.reply_text("Broadcast started. Sending to active users...")

        # fetch active users
        users = list_active_tg_users(limit=10000)
        sent = 0
        failed = 0
        for u in users:
            try:
                await context.bot.send_message(chat_id=u.get("telegram_id"), text=text_to_send)
                sent += 1
                # small delay to avoid hitting API limits
                await asyncio.sleep(0.05)
            except Exception as e:
                failed += 1
                print(f"[broadcast] failed to send to {u.get('telegram_id')}: {e}")

        context.user_data.pop("admin_broadcast", None)
        await update.message.reply_text(f"Broadcast finished. Sent: {sent}, failed: {failed}")
        return

    # Otherwise follow normal username flow (awaiting username after menu)
    if context.user_data.get("awaiting_username"):
        # reuse your existing logic from before
        account = update.message.text.strip().lstrip("@").lower()
        platform = context.user_data.get("platform")
        context.user_data["awaiting_username"] = False  # reset
        await update.message.chat.send_action(ChatAction.TYPING)

        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.message.reply_text(f"No recent public posts for @{account} on X 😕")
                return

            await update.message.reply_text(f"🔥 Latest posts from @{account} on X:")
            for url in posts:
                fixed = url.replace("x.com", "fixupx.com").replace("twitter.com", "fixupx.com")
                await update.message.reply_text(fixed)
                await asyncio.sleep(0.3)

        elif platform == "ig":
            posts = fetch_ig_urls(account)
            if not posts:
                await update.message.reply_text(f"No recent public IG posts for @{account} 😕")
                return

            await update.message.reply_text(f"🔥 Latest IG posts from @{account}:")
            for post in posts:
                caption = post.get("caption", "")[:1024]
                msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}"
                try:
                    if post.get("is_video"):
                        await update.message.reply_video(post["media_url"], caption=msg, parse_mode="HTML")
                    else:
                        await update.message.reply_photo(post["media_url"], caption=msg, parse_mode="HTML")
                except:
                    await update.message.reply_text(msg, parse_mode="HTML")
                await asyncio.sleep(0.3)
        return

    # Not awaiting username and not admin broadcast -> ignore or send help
    # but still record user & ban check
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    # ignore free text (or you could add a fallback)
    await update.message.reply_text("Use the menu or commands. /help for guide.")

# ===================== QUICK ADMIN COMMANDS (ban/unban) =====================
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ You are not an admin.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /ban <telegram_id>")
        return

    try:
        tid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid telegram id.")
        return

    try:
        ban_tg_user(tid)
        await update.message.reply_text(f"User {tid} banned.")
    except Exception as e:
        await update.message.reply_text(f"Error banning user: {e}")

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ You are not an admin.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /unban <telegram_id>")
        return

    try:
        tid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid telegram id.")
        return

    try:
        unban_tg_user(tid)
        await update.message.reply_text(f"User {tid} unbanned.")
    except Exception as e:
        await update.message.reply_text(f"Error unbanning user: {e}")

# ===================== POSTS COMMANDS (existing) =====================

async def xlatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

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
        await asyncio.sleep(0.3)

    # Auto-delete
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

async def iglatest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

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
                sent_msg = await update.message.reply_video(video=media_url, caption=msg, parse_mode="HTML")
            else:
                sent_msg = await update.message.reply_photo(photo=media_url, caption=msg, parse_mode="HTML")
        except:
            sent_msg = await update.message.reply_text(msg, parse_mode="HTML")
        sent_message_ids.append(sent_msg.message_id)
        await asyncio.sleep(0.3)

    # Auto-delete
    context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

    await update.message.reply_text("Posts sent! They will auto-delete in 24hrs.")

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /latest <username>\nExample: /latest vdm")
        return

    account = context.args[0].lstrip('@').lower()
    platform = "ig" if account in ["davido", "chiomaavril", "wizkidayo", "burnaboy"] else "x"
    await update.message.chat.send_action(ChatAction.TYPING)

    sent_message_ids = []
    intro_msg = None

    if platform == "x":
        urls = fetch_latest_urls(platform, account)
        if not urls:
            await update.message.reply_text(f"No recent public posts found for @{account} on X 😕")
            return
        intro_msg = await update.message.reply_text(f"🔥 Latest {len(urls)} posts from @{account} on X:")
        for url in urls:
            fixed_url = url.replace("x.com", "fixupx.com").replace("twitter.com", "fixupx.com")
            sent_msg = await update.message.reply_text(fixed_url, disable_web_page_preview=False)
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.3)
    else:
        posts = fetch_ig_urls(account)
        if not posts:
            await update.message.reply_text(f"No recent public posts found for @{account} on IG 😕")
            return
        intro_msg = await update.message.reply_text(f"🔥 Latest {len(posts)} public IG posts from @{account}:")
        for post in posts:
            caption = post.get('caption', '').strip()[:1024]
            media_url = post.get('media_url')
            msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}" if caption else f"<a href='{post['url']}'>View on IG</a>"
            try:
                if post.get('is_video'):
                    sent_msg = await update.message.reply_video(video=media_url, caption=msg, parse_mode="HTML")
                else:
                    sent_msg = await update.message.reply_photo(photo=media_url, caption=msg, parse_mode="HTML")
            except:
                sent_msg = await update.message.reply_text(msg, parse_mode="HTML")
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.3)

    # Auto-delete
    if intro_msg:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro_msg.chat.id, "message_id": intro_msg.message_id})
    for msg_id in sent_message_ids:
        context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": msg_id})

    await update.message.reply_text("Posts sent! They will auto-delete in 24hrs.")

# ===================== MAIN =====================
if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        raise ValueError("BOTTOKEN environment variable not set!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Public commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("latest", latest))
    app.add_handler(CommandHandler("xlatest", xlatest))
    app.add_handler(CommandHandler("iglatest", iglatest))

    # Admin
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))

    # Callbacks for buttons (menus + admin)
    app.add_handler(CallbackQueryHandler(button_handler))

    # Messages (admin broadcast + normal username flow)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_username_or_admin))

    print("🤖 MooreLinkBot (with Admin Panel) started!")
    app.run_polling(drop_pending_updates=True)