# bot.py
import os
import asyncio
import io
import csv
from typing import Optional, List, Dict, Any
from functools import wraps

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from telegram.constants import ChatAction

# utils: make sure your utils.py exports these functions (and init_tg_db if you use DB)
from utils import (
    fetch_latest_urls,
    fetch_ig_urls,
    add_or_update_tg_user,
    increment_tg_request_count,
    get_tg_user,
    ban_tg_user,
    unban_tg_user,
    list_active_tg_users,
    list_all_tg_users,
    save_user_account,
    list_saved_accounts,
    get_saved_account,
    remove_saved_account,
    count_saved_accounts,
    update_saved_account_label,
    init_tg_db,
)

# ================ CONFIG ================
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("BOTTOKEN env var not set")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
MAX_SAVED_PER_USER = 10

POSTS_PER_PAGE = 5
PAGE_SIZE_USERS = 10

# ================ HELPERS ================
def is_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in ADMIN_IDS)

def admin_only(handler_func):
    """Decorator for async handlers — blocks non-admins early and sends an error."""
    @wraps(handler_func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        user_id = user.id if user else None
        if not is_admin(user_id):
            if update.callback_query:
                # ephemeral message to non-admin trying admin callback
                await update.callback_query.answer("❌ You are not authorized.", show_alert=True)
            elif update.message:
                await update.message.reply_text("❌ You are not authorized to use this command.")
            return
        return await handler_func(update, context, *args, **kwargs)
    return wrapper

# ================ UI BUILDERS ================
def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("X (Twitter)", callback_data="menu_x")],
        [InlineKeyboardButton("Instagram", callback_data="menu_ig")],
        [InlineKeyboardButton("Saved usernames", callback_data="saved_menu")],
        [InlineKeyboardButton("Help / Guide", callback_data="help")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_saved_menu():
    keyboard = [
        [InlineKeyboardButton("➕ Add saved username", callback_data="saved_add_start")],
        [InlineKeyboardButton("📋 My saved usernames", callback_data="saved_list")],
        [InlineKeyboardButton("↩️ Back", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_admin_menu():
    keyboard = [
        [InlineKeyboardButton("👥 List users", callback_data="admin_list_users_0")],
        [InlineKeyboardButton("📤 Broadcast", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("📥 Export CSV", callback_data="admin_export_csv")],
        [InlineKeyboardButton("↩️ Back", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

# Small reusable back/cancel/confirm markups
def build_back_markup(target="menu_main", label="↩️ Back"):
    """Simple inline back button to jump to a callback action."""
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=target)]])

def build_cancel_and_back(cancel_cb="admin_broadcast_cancel", back_cb="admin_back"):
    """Cancel button (for broadcast) plus a Back button."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel", callback_data=cancel_cb)],
        [InlineKeyboardButton("↩️ Back", callback_data=back_cb)],
    ])

def build_confirm_markup(action: str, obj_id: Optional[int] = None, yes_label="Confirm", no_label="Cancel"):
    """
    action: short token like 'ban', 'unban', 'export_csv'
    obj_id: optional integer id to include in callback payload
    Returns markup with Confirm / Cancel.
    """
    if obj_id is None:
        yes_cb = f"confirm_{action}"
    else:
        yes_cb = f"confirm_{action}_{obj_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(yes_label, callback_data=yes_cb)],
        [InlineKeyboardButton(no_label, callback_data="admin_back")]
    ])

# ================ UTIL: CSV ================
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

# ================ DB / USER RECORDS ================
async def record_user_and_check_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Ensure user exists and increment request count.
    Return False if user is banned.
    """
    user = update.effective_user
    if not user:
        return True
    tid = user.id
    first_name = user.first_name or ""
    try:
        add_or_update_tg_user(tid, first_name)
    except Exception:
        pass
    try:
        increment_tg_request_count(tid)
    except Exception:
        pass
    try:
        row = get_tg_user(tid)
        if row and int(row.get("is_banned", 0)) == 1:
            return False
    except Exception:
        pass
    return True

# ================ DELETE JOB ================
async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    try:
        await context.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except:
        pass

# ================ COMMANDS ================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned.")
        return
    text = (
        "👋 Welcome to MooreLinkBot!\n\n"
        "Commands & quick actions available in the menu.\n"
        "Saved accounts: /save /saved_list /saved_send /saved_remove /saved_rename\n\n"
        "Admins: /admin\n"
    )
    await update.message.reply_text(text, reply_markup=build_main_menu())

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned.")
        return
    await update.message.reply_text("Choose:", reply_markup=build_main_menu())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned.")
        return
    help_text = (
        "Guide:\n"
        "/save <platform> <username> [label]\n"
        "/saved_list\n"
        "/saved_send <id>\n"
        "/saved_remove <id>\n"
        "/saved_rename <id> <new label>\n\n"
        "Use the Saved menu for one-tap actions."
    )
    await update.message.reply_text(help_text)

# quick /latest that accepts args or asks for username (small convenience)
async def latest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Usage: /latest x|ig username  OR run command and follow prompt"""
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.message.reply_text("🚫 You are banned.")
        return

    args = context.args or []
    if len(args) >= 2:
        platform = args[0].lower()
        account = args[1].lstrip('@').lower()
        await update.message.chat.send_action(ChatAction.TYPING)
        if platform in ("twitter",):
            platform = "x"
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.message.reply_text(f"No recent public posts for @{account} on X.")
                return
            for url in posts:
                await update.message.reply_text(url.replace("x.com", "fixupx.com"))
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await update.message.reply_text(f"No recent IG posts for @{account}.")
                return
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
        return

    # otherwise prompt and set awaiting_username state (default platform x)
    context.user_data["awaiting_username"] = True
    context.user_data["platform"] = "x"
    await update.message.reply_text("Send username (without @) — default platform X. Use /cancel to abort.", reply_markup=build_back_markup("menu_main"))

# ================ ADMIN (with confirmations) ================
@admin_only
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ Not an admin.")
        return
    await update.message.reply_text("Admin panel:", reply_markup=build_admin_menu())

@admin_only
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid id.")
        return
    # ask for confirmation
    await update.message.reply_text(
        f"Are you sure you want to ban {tid}?",
        reply_markup=build_confirm_markup("ban", tid)
    )

@admin_only
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /unban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid id.")
        return
    # ask for confirmation
    await update.message.reply_text(
        f"Are you sure you want to unban {tid}?",
        reply_markup=build_confirm_markup("unban", tid)
    )

@admin_only
async def export_csv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # confirmation before exporting potentially large data
    await update.message.reply_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))

# ================ CANCEL COMMAND ================
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generic cancel: clears any awaiting_* or admin_broadcast states and returns to menu."""
    user = update.effective_user
    ctx = context.user_data
    cleared = []
    for k in ("admin_broadcast", "awaiting_save", "awaiting_username", "awaiting_rename_id"):
        if k in ctx:
            ctx.pop(k, None)
            cleared.append(k)
    # respond
    await update.message.reply_text("Cancelled.", reply_markup=build_main_menu())

# ================ CALLBACKS ================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    uid = user.id if user else None

    # record user (non-admin)
    await record_user_and_check_ban(update, context)

    data = query.data

    # -- Confirmation callbacks for admin actions --
    if data.startswith("confirm_ban_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        _, _, tid_s = data.partition("confirm_ban_")
        try:
            tid = int(tid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        ban_tg_user(tid)
        await query.edit_message_text(f"User {tid} banned.", reply_markup=build_admin_menu())
        return

    if data.startswith("confirm_unban_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        _, _, tid_s = data.partition("confirm_unban_")
        try:
            tid = int(tid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        unban_tg_user(tid)
        await query.edit_message_text(f"User {tid} unbanned.", reply_markup=build_admin_menu())
        return

    if data == "confirm_export_csv":
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        await query.edit_message_text("Preparing CSV...")
        users = list_all_tg_users(limit=10000)
        csv_bytes = users_to_csv_bytes(users)
        bio = io.BytesIO(csv_bytes)
        bio.name = "tg_users.csv"
        try:
            await context.bot.send_document(chat_id=uid, document=InputFile(bio))
            await query.edit_message_text("CSV sent.")
        except Exception as e:
            await query.edit_message_text(f"Failed to send CSV: {e}")
        return

    # main menu navigation
    if data == "menu_main":
        await query.edit_message_text("Main menu:", reply_markup=build_main_menu())
        return

    if data == "menu_x":
        context.user_data["platform"] = "x"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the X username (without @):", reply_markup=build_back_markup("menu_main"))
        return
    if data == "menu_ig":
        context.user_data["platform"] = "ig"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the Instagram username (without @):", reply_markup=build_back_markup("menu_main"))
        return
    if data == "help":
        await help_command(update, context)
        return

    # saved menu
    if data == "saved_menu":
        await query.edit_message_text("Saved usernames:", reply_markup=build_saved_menu())
        return
    if data == "saved_add_start":
        context.user_data["awaiting_save"] = True
        await query.edit_message_text(
            "Send: <platform> <username> [label]\nExample: `x vdm fav`",
            reply_markup=build_back_markup("saved_menu")
        )
        return
    if data == "saved_list":
        owner = uid
        items = list_saved_accounts(owner)
        if not items:
            await query.edit_message_text("You have no saved accounts.", reply_markup=build_saved_menu())
            return
        text = "Your saved accounts:\n\n"
        rows = []
        for it in items:
            sid = it["id"]
            plat = it["platform"]
            acc = it["account_name"]
            lbl = it.get("label") or ""
            text += f"{sid}. [{plat}] @{acc} {('- '+lbl) if lbl else ''}\n"
            rows.append([
                InlineKeyboardButton(f"Send {sid}", callback_data=f"saved_sendcb_{sid}"),
                InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")
            ])
        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
        return

    # saved quick send via callback
    if data.startswith("saved_sendcb_"):
        _, _, sid_s = data.partition("saved_sendcb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid saved id.")
            return
        owner = uid
        saved = get_saved_account(owner, sid)
        if not saved:
            await query.edit_message_text("Saved account not found.")
            return
        platform = saved["platform"]
        account = saved["account_name"]
        await query.edit_message_text(f"Sending latest posts from [{platform}] @{account} ...", reply_markup=build_back_markup("saved_list"))
        await asyncio.sleep(0.5)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await query.edit_message_text(f"No recent posts for @{account} on X.")
                return
            for url in posts:
                await query.message.reply_text(url.replace("x.com", "fixupx.com"))
                await asyncio.sleep(0.2)
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await query.edit_message_text(f"No recent IG posts for @{account}.")
                return
            for post in posts:
                caption = post.get("caption", "")[:1024]
                msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}"
                try:
                    if post.get("is_video"):
                        await query.message.reply_video(post["media_url"], caption=msg, parse_mode="HTML")
                    else:
                        await query.message.reply_photo(post["media_url"], caption=msg, parse_mode="HTML")
                except:
                    await query.message.reply_text(msg, parse_mode="HTML")
                await asyncio.sleep(0.3)
        return

    # saved remove via callback
    if data.startswith("saved_removecb_"):
        _, _, sid_s = data.partition("saved_removecb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        owner = uid
        ok = remove_saved_account(owner, sid)
        if ok:
            await query.edit_message_text(f"Removed saved account {sid}.", reply_markup=build_saved_menu())
        else:
            await query.edit_message_text("Could not remove saved account.", reply_markup=build_saved_menu())
        return

    # saved rename start via callback (interactive)
    if data.startswith("saved_rename_start_"):
        _, _, sid_s = data.partition("saved_rename_start_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        context.user_data["awaiting_rename_id"] = sid
        await query.edit_message_text(
            "Send the new label for this saved account (single message):",
            reply_markup=build_back_markup("saved_list")
        )
        return

    # ---------------- Admin callbacks ----------------
    if data.startswith("admin_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        # admin_list_users_{page}
        if data.startswith("admin_list_users_"):
            _, _, page_s = data.partition("admin_list_users_")
            page = int(page_s or "0")
            users = list_all_tg_users(limit=10000)
            total = len(users)
            start = page * PAGE_SIZE_USERS
            end = start + PAGE_SIZE_USERS
            page_users = users[start:end]
            text = f"Users (page {page+1}):\n\n"
            for u in page_users:
                text += f"- {u.get('first_name') or ''} ({u.get('telegram_id')}) banned={u.get('is_banned')} reqs={u.get('request_count')}\n"
            rows = []
            if page > 0:
                rows.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_users_{page-1}"))
            if end < total:
                rows.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users_{page+1}"))
            rows2 = [[b] for b in rows] if rows else []
            rows2.append([InlineKeyboardButton("↩️ Back", callback_data="admin_back")])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows2))
            return
        if data == "admin_back":
            await query.edit_message_text("Admin panel:", reply_markup=build_admin_menu())
            return
        if data == "admin_export_csv":
            # ask for confirmation
            await query.edit_message_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))
            return
        if data == "admin_broadcast_start":
            context.user_data["admin_broadcast"] = True
            await query.edit_message_text(
                "Send the message to broadcast. Use /cancel or press Cancel below to abort.",
                reply_markup=build_cancel_and_back("admin_broadcast_cancel", "admin_back")
            )
            return
        if data == "admin_broadcast_cancel":
            # cancel the waiting broadcast prompt
            context.user_data.pop("admin_broadcast", None)
            await query.edit_message_text("Broadcast cancelled.", reply_markup=build_admin_menu())
            return

    # pagination / posts callbacks (like page_x)
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
        msg = f"Page {page+1} of {total_pages}\n\n"
        for p in page_posts:
            if isinstance(p, dict):
                msg += f"{p.get('url')}\n"
            else:
                msg += f"{p}\n"
        keyboard = []
        if page > 0:
            keyboard.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{platform}_{account}"))
        if page < total_pages - 1:
            keyboard.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{platform}_{account}"))
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([keyboard]) if keyboard else None)
        return

    await query.edit_message_text("Unknown action or handled elsewhere.")

# ================ MESSAGE HANDLER (saved add, rename, broadcast, username flows) ================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ignore empty messages
    if not update.message or not update.message.text:
        return

    # admin broadcast flow (cooperative & cancelable)
    if context.user_data.get("admin_broadcast"):
        user = update.effective_user
        if not is_admin(user.id):
            context.user_data.pop("admin_broadcast", None)
            await update.message.reply_text("❌ Only admins can broadcast.")
            return

        text_to_send = update.message.text
        # reassure admin that broadcast started & how to cancel mid-run
        await update.message.reply_text("Broadcast starting... (send /cancel to abort while it runs)")
        users = list_active_tg_users(limit=10000)
        sent = 0
        failed = 0
        cancelled = False

        for u in users:
            # allow graceful cancellation while the loop yields (we sleep between sends)
            if not context.user_data.get("admin_broadcast"):
                cancelled = True
                break
            try:
                await context.bot.send_message(chat_id=u.get("telegram_id"), text=text_to_send)
                sent += 1
                # small sleep to yield event loop so /cancel can be received
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1

        # clear the flag if it still exists
        context.user_data.pop("admin_broadcast", None)

        if cancelled:
            await update.message.reply_text(f"Broadcast cancelled. Sent so far: {sent}, failed: {failed}")
        else:
            await update.message.reply_text(f"Broadcast done. Sent: {sent}, failed: {failed}")
        return

    # awaiting rename label
    if context.user_data.get("awaiting_rename_id"):
        sid = context.user_data.pop("awaiting_rename_id")
        new_label = update.message.text.strip()
        owner = update.effective_user.id
        ok = update_saved_account_label(owner, sid, new_label)
        if ok:
            await update.message.reply_text(f"Saved account {sid} renamed to: {new_label}", reply_markup=build_saved_menu())
        else:
            await update.message.reply_text("Could not rename saved account (not found or permission).", reply_markup=build_saved_menu())
        return

    # awaiting save interactive flow
    if context.user_data.get("awaiting_save"):
        text = update.message.text.strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await update.message.reply_text("Send: <platform> <username> [label]")
            return
        platform = parts[0].lower()
        if platform in ("twitter",):
            platform = "x"
        if platform in ("instagram",):
            platform = "ig"
        if platform not in ("x", "ig"):
            await update.message.reply_text("Platform must be x or ig.")
            return
        account = parts[1].lstrip('@').lower()
        label = parts[2] if len(parts) == 3 else None
        owner = update.effective_user.id
        current_count = count_saved_accounts(owner)
        if current_count >= MAX_SAVED_PER_USER:
            await update.message.reply_text(f"You reached saved limit ({MAX_SAVED_PER_USER}). Remove some or ask admin to increase.")
            context.user_data.pop("awaiting_save", None)
            return
        try:
            saved = save_user_account(owner, platform, account, label)
            await update.message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})", reply_markup=build_saved_menu())
        except Exception as e:
            await update.message.reply_text(f"Error saving: {e}", reply_markup=build_saved_menu())
        context.user_data.pop("awaiting_save", None)
        return

    # awaiting username (main menu flows)
    if context.user_data.get("awaiting_username"):
        account = update.message.text.strip().lstrip("@").lower()
        platform = context.user_data.get("platform")
        context.user_data["awaiting_username"] = False
        await update.message.chat.send_action(ChatAction.TYPING)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.message.reply_text(f"No recent public posts for @{account} on X.")
                return
            intro = await update.message.reply_text(f"Latest posts from @{account} on X:", reply_markup=build_back_markup("menu_main"))
            sent_ids = []
            for url in posts:
                sent = await update.message.reply_text(url.replace("x.com", "fixupx.com"))
                sent_ids.append(sent.message_id)
                await asyncio.sleep(0.2)
            # schedule deletion
            context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro.chat.id, "message_id": intro.message_id})
            for mid in sent_ids:
                context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": mid})
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await update.message.reply_text(f"No recent IG posts for @{account}.")
                return
            intro = await update.message.reply_text(f"Latest IG posts from @{account}:", reply_markup=build_back_markup("menu_main"))
            sent_ids = []
            for post in posts:
                caption = post.get("caption", "")[:1024]
                msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}"
                try:
                    if post.get("is_video"):
                        sent = await update.message.reply_video(post["media_url"], caption=msg, parse_mode="HTML")
                    else:
                        sent = await update.message.reply_photo(post["media_url"], caption=msg, parse_mode="HTML")
                except:
                    sent = await update.message.reply_text(msg, parse_mode="HTML")
                sent_ids.append(sent.message_id)
                await asyncio.sleep(0.3)
            context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro.chat.id, "message_id": intro.message_id})
            for mid in sent_ids:
                context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.message.chat.id, "message_id": mid})
        return

    # saved_send via text (/saved_send <id>)
    text = update.message.text.strip()
    if text.startswith("/saved_send"):
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("Usage: /saved_send <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.message.reply_text("Invalid id.")
            return
        owner = update.effective_user.id
        saved = get_saved_account(owner, sid)
        if not saved:
            await update.message.reply_text("Saved account not found.")
            return
        platform = saved["platform"]
        account = saved["account_name"]
        await update.message.chat.send_action(ChatAction.TYPING)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.message.reply_text(f"No recent posts for @{account}.")
                return
            for url in posts:
                await update.message.reply_text(url.replace("x.com", "fixupx.com"))
                await asyncio.sleep(0.2)
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await update.message.reply_text(f"No recent IG posts for @{account}.")
                return
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

    # saved_remove via text
    if text.startswith("/saved_remove"):
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("Usage: /saved_remove <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.message.reply_text("Invalid id.")
            return
        owner = update.effective_user.id
        ok = remove_saved_account(owner, sid)
        if ok:
            await update.message.reply_text(f"Removed saved account {sid}.")
        else:
            await update.message.reply_text("Could not remove saved account.")
        return

    # saved_rename via text
    if text.startswith("/saved_rename"):
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            await update.message.reply_text("Usage: /saved_rename <id> <new label>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.message.reply_text("Invalid id.")
            return
        new_label = parts[2].strip()
        owner = update.effective_user.id
        ok = update_saved_account_label(owner, sid, new_label)
        if ok:
            await update.message.reply_text(f"Renamed saved account {sid} -> {new_label}")
        else:
            await update.message.reply_text("Could not rename saved account.")
        return

    # save via command text (/save)
    if text.startswith("/save"):
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            await update.message.reply_text("Usage: /save <platform> <username> [label]")
            return
        platform = parts[1].lower()
        if platform in ("twitter",):
            platform = "x"
        if platform in ("instagram",):
            platform = "ig"
        if platform not in ("x", "ig"):
            await update.message.reply_text("Platform must be x or ig.")
            return
        account = parts[2].lstrip('@').lower()
        label = parts[3] if len(parts) == 4 else None
        owner = update.effective_user.id
        current_count = count_saved_accounts(owner)
        if current_count >= MAX_SAVED_PER_USER:
            await update.message.reply_text(f"You've reached the saved limit ({MAX_SAVED_PER_USER}).")
            return
        try:
            saved = save_user_account(owner, platform, account, label)
            await update.message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})")
        except Exception as e:
            await update.message.reply_text(f"Error saving: {e}")
        return

    # saved_list via command
    if text.startswith("/saved_list"):
        owner = update.effective_user.id
        items = list_saved_accounts(owner)
        if not items:
            await update.message.reply_text("No saved accounts. Use /save or the Saved menu.")
            return
        text_out = "Your saved accounts:\n\n"
        rows = []
        for it in items:
            sid = it["id"]; plat = it["platform"]; acc = it["account_name"]; lbl = it.get("label") or ""
            text_out += f"{sid}. [{plat}] @{acc} {('- '+lbl) if lbl else ''}\n"
            rows.append([InlineKeyboardButton(f"Send {sid}", callback_data=f"saved_sendcb_{sid}"),
                         InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                         InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")])
        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])
        await update.message.reply_text(text_out, reply_markup=InlineKeyboardMarkup(rows))
        return

    # default: not a managed command -> show guide
    await record_user_and_check_ban(update, context)
    await update.message.reply_text("Use the menu or /help for commands.")

# ================ REGISTER & RUN ================
if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Public commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("latest", latest_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("export_csv", export_csv_command))
    app.add_handler(CommandHandler("cancel", cancel_command))

    # Saved shortcuts routed to the same message handler (it parses the /save etc. commands)
    app.add_handler(CommandHandler("save", message_handler))
    app.add_handler(CommandHandler("saved_list", message_handler))
    app.add_handler(CommandHandler("saved_send", message_handler))
    app.add_handler(CommandHandler("saved_remove", message_handler))
    app.add_handler(CommandHandler("saved_rename", message_handler))

    # core handlers
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # ================ COMMAND VISIBILITY (hide admin commands from non-admins) ================
    async def set_command_visibility(application):
        """
        Runs during application.post_init in the same event loop as the bot.
        Registers public commands for everyone and admin-only commands per admin private chat.
        """
        public_cmds = [
            BotCommand("start", "Show welcome / menu"),
            BotCommand("menu", "Open main menu"),
            BotCommand("latest", "Get latest posts for a username"),
            BotCommand("saved_list", "List your saved usernames"),
            BotCommand("save", "Save a username for quick sending"),
            BotCommand("help", "Show help"),
        ]
        try:
            await application.bot.set_my_commands(public_cmds, scope=BotCommandScopeDefault())
        except Exception as e:
            print(f"[commands] failed to set public commands: {e}")

        admin_cmds = [
            BotCommand("admin", "Open admin panel"),
            BotCommand("ban", "Ban a user (admin only)"),
            BotCommand("unban", "Unban a user (admin only)"),
            BotCommand("broadcast", "Start a broadcast (admin only)"),
            BotCommand("export_csv", "Export users CSV (admin only)"),
        ]

        for admin_id in ADMIN_IDS:
            try:
                scope = BotCommandScopeChat(chat_id=admin_id)
                await application.bot.set_my_commands(admin_cmds, scope=scope)
                print(f"[commands] admin commands set for private chat {admin_id}")
            except Exception as e:
                print(f"[commands] failed to set admin commands for {admin_id}: {e}. Falling back to default scope.")
                try:
                    await application.bot.set_my_commands(admin_cmds, scope=BotCommandScopeDefault())
                except Exception as e2:
                    print(f"[commands] fallback failed: {e2}")

    # robustly combine with any existing post_init value
    existing_post_init = getattr(app, "post_init", None)

    if existing_post_init is None:
        app.post_init = set_command_visibility
    else:
        # create a single coroutine that will call the existing post_init (list/callable) then ours
        async def _combined_post_init(application):
            # call existing post_init in whichever shape it is
            try:
                if isinstance(existing_post_init, list):
                    for item in existing_post_init:
                        try:
                            res = item(application)
                            if asyncio.iscoroutine(res):
                                await res
                        except Exception as e:
                            print(f"[post_init] one existing item failed: {e}")
                elif callable(existing_post_init):
                    res = existing_post_init(application)
                    if asyncio.iscoroutine(res):
                        await res
                else:
                    print("[post_init] existing_post_init is neither None, list nor callable — skipping it.")
            except Exception as e:
                print(f"[post_init] existing_post_init wrapper failed: {e}")

            # then call our function
            try:
                res2 = set_command_visibility(application)
                if asyncio.iscoroutine(res2):
                    await res2
            except Exception as e:
                print(f"[post_init] set_command_visibility failed: {e}")

        app.post_init = _combined_post_init

    print("[startup] post_init registered")

    # Initialize DB (will log/skip if env not set or if init fails)
    try:
        init_tg_db()
    except Exception as e:
        # log but continue — bot can run without DB, but some features will be disabled
        print(f"[startup] init_tg_db() failed or not available: {e}")

    print("🤖 MooreLinkBot (full) started — admin + saved accounts + quick-send enabled")
    app.run_polling(drop_pending_updates=True)