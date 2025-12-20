# bot.py
import os
import asyncio
import io
import csv
from typing import Optional, List, Dict, Any
from functools import wraps
from datetime import datetime

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

# utils - import everything we rely on
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
    get_user_badge,
    increment_invite_count,
    check_and_increment_cooldown,
    get_user_stats,
    reset_cooldown,
    BADGE_LEVELS,
    get_tg_db,
    create_user_if_missing,
)

# ================ CONFIG ================
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("BOTTOKEN env var not set")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

POSTS_PER_PAGE = 5
PAGE_SIZE_USERS = 10
LEADERBOARD_LIMIT = 10

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
                await update.callback_query.answer("❌ You are not authorized.", show_alert=True)
            elif update.effective_message:
                await update.effective_message.reply_text("❌ You are not authorized to use this command.")
            return
        return await handler_func(update, context, *args, **kwargs)
    return wrapper

def get_invite_link(bot_username: str, user_id: int) -> str:
    return f"https://t.me/{bot_username}?start={user_id}"

# ================ UI BUILDERS ================
def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("X (Twitter)", callback_data="menu_x")],
        [InlineKeyboardButton("Instagram", callback_data="menu_ig")],
        [InlineKeyboardButton("Saved usernames", callback_data="saved_menu")],
        [InlineKeyboardButton("👤 Dashboard", callback_data="dashboard")],
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
        [InlineKeyboardButton("📊 Leaderboard", callback_data="admin_leaderboard")],
        [InlineKeyboardButton("📤 Broadcast", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("📥 Export CSV", callback_data="admin_export_csv")],
        [InlineKeyboardButton("↩️ Back", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_back_markup(target="menu_main", label="↩️ Back"):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=target)]])

def build_cancel_and_back(cancel_cb="admin_broadcast_cancel", back_cb="admin_back"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel", callback_data=cancel_cb)],
        [InlineKeyboardButton("↩️ Back", callback_data=back_cb)],
    ])

def build_confirm_markup(action: str, obj_id: Optional[int] = None, yes_label="Confirm", no_label="Cancel"):
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
    writer.writerow(["telegram_id", "first_name", "is_active", "is_banned", "request_count", "last_request_at", "joined_at", "invite_count"])
    for u in users:
        writer.writerow([
            u.get("telegram_id"),
            u.get("first_name"),
            u.get("is_active"),
            u.get("is_banned"),
            u.get("request_count"),
            u.get("last_request_at"),
            u.get("joined_at"),
            u.get("invite_count"),
        ])
    return buf.getvalue().encode("utf-8")

# ================ RECORD + BAN CHECK ================
async def record_user_and_check_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
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
    except Exception:
        pass

# ================ COMMAND HANDLERS ================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start optionally receives inviter id as start parameter.
    We only award invite when this user is actually newly created.
    """
    user = update.effective_user
    if not user:
        return
    tid = user.id
    first_name = user.first_name or ""

    # Try to insert user if missing so we can detect new signup
    try:
        is_new = create_user_if_missing(tid, first_name)
    except Exception:
        is_new = False

    # If the incoming /start had a single numeric arg and user is new, award inviter
    if is_new and context.args and len(context.args) == 1:
        try:
            inviter_id = int(context.args[0])
            if inviter_id != tid:
                increment_invite_count(inviter_id)
        except Exception:
            pass

    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return

    text = (
        "👋 Welcome to MooreLinkBot!\n\n"
        "Commands & quick actions available in the menu.\n"
        "Saved accounts: /save /saved_list /saved_send /saved_remove /saved_rename\n\n"
        "Admins: /admin\n"
    )
    await update.effective_message.reply_text(text, reply_markup=build_main_menu())

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    await update.effective_message.reply_text("Choose:", reply_markup=build_main_menu())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    help_text = (
        "Guide:\n"
        "/save <platform> <username> [label]\n"
        "/saved_list\n"
        "/saved_send <id>\n"
        "/saved_remove <id>\n"
        "/saved_rename <id> <new label>\n"
        "/dashboard - Show your badge, invites, etc.\n"
        "/benefits - See badge levels\n"
        "/leaderboard - Top inviters\n\n"
        "Use the Saved menu for one-tap actions."
    )
    await update.effective_message.reply_text(help_text)

async def benefits_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    text = "Badge benefits:\n\n"
    for level in BADGE_LEVELS:
        text += f"{level['emoji']} {level['name']}\n"
        text += f"• Save slots: {level['save_slots']}\n"
        limits = level.get('limits', {})
        text += f"• Limits: {limits.get('min','∞')}/min, {limits.get('hour','∞')}/hr, {limits.get('day','∞')}/day\n\n"
    await update.effective_message.reply_text(text)

async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    tid = update.effective_user.id
    badge = get_user_badge(tid)
    user = get_tg_user(tid) or {}
    invites = int(user.get('invite_count', 0))
    saves = count_saved_accounts(tid)
    # find next badge
    next_badge = None
    invites_left = 0
    for i, level in enumerate(BADGE_LEVELS):
        if level['name'] == badge['name']:
            if i+1 < len(BADGE_LEVELS):
                next_level = BADGE_LEVELS[i+1]
                if next_level.get('invites_needed') is not None:
                    next_badge = next_level
                    invites_left = max(0, next_level['invites_needed'] - invites)
            break

    text = "👤 Dashboard\n\n"
    text += f"🏅 Badge: {badge['emoji']} {badge['name']}\n"
    text += f"📨 Invites: {invites}\n"
    text += f"📦 Save Slots: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
    text += "⚡ Speed limits:\n"
    text += f"• {badge['limits']['min'] if isinstance(badge['limits']['min'], (int,float)) else '∞'}/min\n"
    text += f"• {badge['limits']['hour'] if isinstance(badge['limits']['hour'], (int,float)) else '∞'}/hour\n"
    text += f"• {badge['limits']['day'] if isinstance(badge['limits']['day'], (int,float)) else '∞'}/day\n\n"
    if next_badge:
        text += f"⏭ Next Badge: {next_badge['emoji']} {next_badge['name']} ({invites_left} invites left)\n"
    else:
        text += "⚡ Unlimited Access or top badge\n"
    bot_username = context.bot.username or os.getenv("BOT_USERNAME", "yourbot")
    text += f"\nYour invite link: {get_invite_link(bot_username, tid)}"
    await update.effective_message.reply_text(text)

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT telegram_id, first_name, invite_count
            FROM tg_users
            ORDER BY invite_count DESC
            LIMIT %s
        """, (LEADERBOARD_LIMIT,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception:
        rows = []
    text = "📊 Invite Leaderboard (Top)\n\n"
    for i, row in enumerate(rows, 1):
        name = row.get('first_name') or f"User {row.get('telegram_id')}"
        invites = row.get('invite_count', 0)
        text += f"{i}. {name} - {invites} invites\n"
    await update.effective_message.reply_text(text)

# ================ ADMIN (with confirmations) ================
@admin_only
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Admin panel:", reply_markup=build_admin_menu())

@admin_only
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /ban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    await update.effective_message.reply_text(f"Are you sure you want to ban {tid}?", reply_markup=build_confirm_markup("ban", tid))

@admin_only
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /unban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    await update.effective_message.reply_text(f"Are you sure you want to unban {tid}?", reply_markup=build_confirm_markup("unban", tid))

@admin_only
async def reset_cooldown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /reset_cooldown <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    reset_cooldown(tid)
    await update.effective_message.reply_text(f"Cooldown reset for {tid}.")

@admin_only
async def user_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /user_stats <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    stats = get_user_stats(tid)
    if not stats:
        await update.effective_message.reply_text("User not found.")
        return
    user = stats['user']
    badge = stats['badge']
    rl = stats['rate_limits']
    saves = stats['save_count']
    text = f"Stats for {user.get('first_name', 'User')} ({tid})\n\n"
    text += f"Joined: {user.get('joined_at')}\n"
    text += f"Requests: {user.get('request_count', 0)}\n"
    text += f"Invites: {user.get('invite_count', 0)}\n"
    text += f"Banned: {bool(user.get('is_banned'))}\n"
    text += f"Badge: {badge['emoji']} {badge['name']}\n"
    text += f"Saves: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
    text += "Cooldowns:\n"
    text += f"Minute: {rl.get('minute_count',0)}/{badge['limits'].get('min','∞')}\n"
    text += f"Hour: {rl.get('hour_count',0)}/{badge['limits'].get('hour','∞')}\n"
    text += f"Day: {rl.get('day_count',0)}/{badge['limits'].get('day','∞')}\n"
    await update.effective_message.reply_text(text)

@admin_only
async def export_csv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))

# ================ CANCEL COMMAND ================
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctx = context.user_data
    for k in ("admin_broadcast", "awaiting_save", "awaiting_username", "awaiting_rename_id"):
        ctx.pop(k, None)
    await update.effective_message.reply_text("Cancelled.", reply_markup=build_main_menu())

# ================ CALLBACK HANDLER ================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    uid = user.id if user else None

    # record user
    await record_user_and_check_ban(update, context)

    data = query.data or ""

    # Confirmation callbacks
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

    # navigation
    if data == "menu_main":
        await query.edit_message_text("Main menu:", reply_markup=build_main_menu()); return
    if data == "dashboard":
        await dashboard_command(update, context); return
    if data == "menu_x":
        context.user_data["platform"] = "x"; context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the X username (without @):", reply_markup=build_back_markup("menu_main")); return
    if data == "menu_ig":
        context.user_data["platform"] = "ig"; context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the Instagram username (without @):", reply_markup=build_back_markup("menu_main")); return
    if data == "help":
        await help_command(update, context); return

    # saved menu actions
    if data == "saved_menu":
        await query.edit_message_text("Saved usernames:", reply_markup=build_saved_menu()); return
    if data == "saved_add_start":
        context.user_data["awaiting_save"] = True
        await query.edit_message_text("Send: <platform> <username> [label]\nExample: `x vdm fav`", reply_markup=build_back_markup("saved_menu"))
        return
    if data == "saved_list":
        owner = uid
        items = list_saved_accounts(owner)
        if not items:
            await query.edit_message_text("You have no saved accounts.", reply_markup=build_saved_menu()); return
        text = "Your saved accounts:\n\n"; rows = []
        for it in items:
            sid = it["id"]; plat = it["platform"]; acc = it["account_name"]; lbl = it.get("label") or ""
            text += f"{sid}. [{plat}] @{acc} {('- '+lbl) if lbl else ''}\n"
            rows.append([InlineKeyboardButton(f"Send {sid}", callback_data=f"saved_sendcb_{sid}"),
                         InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                         InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")])
        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows)); return

    # saved quick send via callback
    if data.startswith("saved_sendcb_"):
        _, _, sid_s = data.partition("saved_sendcb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid saved id."); return
        cooldown_msg = check_and_increment_cooldown(uid)
        if cooldown_msg:
            await query.edit_message_text(cooldown_msg); return
        saved = get_saved_account(uid, sid)
        if not saved:
            await query.edit_message_text("Saved account not found."); return
        platform = saved["platform"]; account = saved["account_name"]
        await query.edit_message_text(f"Sending latest posts from [{platform}] @{account} ...", reply_markup=build_back_markup("saved_list"))
        await asyncio.sleep(0.5)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await query.edit_message_text(f"No recent posts for @{account} on X."); return
            for url in posts:
                await query.message.reply_text(url.replace("x.com", "fixupx.com")); await asyncio.sleep(0.2)
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await query.edit_message_text(f"No recent IG posts for @{account}."); return
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

    if data.startswith("saved_removecb_"):
        _, _, sid_s = data.partition("saved_removecb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id."); return
        ok = remove_saved_account(uid, sid)
        if ok:
            await query.edit_message_text(f"Removed saved account {sid}.", reply_markup=build_saved_menu())
        else:
            await query.edit_message_text("Could not remove saved account.", reply_markup=build_saved_menu())
        return

    if data.startswith("saved_rename_start_"):
        _, _, sid_s = data.partition("saved_rename_start_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id."); return
        context.user_data["awaiting_rename_id"] = sid
        await query.edit_message_text("Send the new label for this saved account (single message):", reply_markup=build_back_markup("saved_list"))
        return

    # Admin callbacks like listing, stats, resets...
    if data.startswith("admin_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only."); return

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
            rows = []
            for u in page_users:
                tid = u.get('telegram_id')
                text += f"- {u.get('first_name') or ''} ({tid}) banned={u.get('is_banned')} reqs={u.get('request_count')} invites={u.get('invite_count')}\n"
                rows.append([
                    InlineKeyboardButton(f"Stats {tid}", callback_data=f"admin_user_stats_{tid}"),
                    InlineKeyboardButton(f"Reset CD {tid}", callback_data=f"admin_reset_cooldown_start_{tid}"),
                    InlineKeyboardButton(f"Ban {tid}" if not u.get('is_banned') else f"Unban {tid}", callback_data=f"admin_{'ban' if not u.get('is_banned') else 'unban'}_start_{tid}")
                ])
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_users_{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users_{page+1}"))
            if nav_row:
                rows.append(nav_row)
            rows.append([InlineKeyboardButton("↩️ Back", callback_data="admin_back")])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
            return

        if data.startswith("admin_user_stats_"):
            _, _, tid_s = data.partition("admin_user_stats_")
            try:
                tid = int(tid_s)
            except:
                await query.edit_message_text("Invalid id."); return
            stats = get_user_stats(tid)
            if not stats:
                await query.edit_message_text("User not found."); return
            user = stats['user']; badge = stats['badge']; rl = stats['rate_limits']; saves = stats['save_count']
            text = f"Stats for {user.get('first_name', 'User')} ({tid})\n\n"
            text += f"Joined: {user.get('joined_at')}\nRequests: {user.get('request_count', 0)}\nInvites: {user.get('invite_count', 0)}\nBanned: {bool(user.get('is_banned'))}\n"
            text += f"Badge: {badge['emoji']} {badge['name']}\nSaves: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
            text += "Cooldowns:\n"
            text += f"Minute: {rl.get('minute_count',0)}/{badge['limits'].get('min','∞')} (reset: {rl.get('minute_reset')})\n"
            text += f"Hour: {rl.get('hour_count',0)}/{badge['limits'].get('hour','∞')} (reset: {rl.get('hour_reset')})\n"
            text += f"Day: {rl.get('day_count',0)}/{badge['limits'].get('day','∞')} (reset: {rl.get('day_reset')})\n"
            await query.edit_message_text(text, reply_markup=build_back_markup("admin_list_users_0"))
            return

        if data.startswith("admin_reset_cooldown_start_"):
            _, _, tid_s = data.partition("admin_reset_cooldown_start_")
            try:
                tid = int(tid_s)
            except:
                await query.edit_message_text("Invalid id."); return
            await query.edit_message_text(f"Confirm reset cooldown for {tid}?", reply_markup=build_confirm_markup("reset_cooldown", tid))
            return

        if data == "admin_leaderboard":
            # reuse leaderboard logic
            await query.edit_message_text("Loading leaderboard...", reply_markup=build_back_markup("admin_back"))
            return

        if data == "admin_back":
            await query.edit_message_text("Admin panel:", reply_markup=build_admin_menu()); return

        if data == "admin_export_csv":
            await query.edit_message_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv")); return

        if data == "admin_broadcast_start":
            context.user_data["admin_broadcast"] = True
            await query.edit_message_text("Send the message to broadcast. Use /cancel to abort.", reply_markup=build_cancel_and_back("admin_broadcast_cancel", "admin_back")); return

        if data == "admin_broadcast_cancel":
            context.user_data.pop("admin_broadcast", None)
            await query.edit_message_text("Broadcast cancelled.", reply_markup=build_admin_menu()); return

    # pages for posts
    if data.startswith("page_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await query.edit_message_text("Invalid page data."); return
        page = int(parts[1]); platform = parts[2]; account = parts[3]
        posts = fetch_latest_urls(platform, account) if platform == "x" else fetch_ig_urls(account)
        start = page * POSTS_PER_PAGE; end = start + POSTS_PER_PAGE
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

# ================ MESSAGE HANDLER ================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    # admin broadcast
    if context.user_data.get("admin_broadcast"):
        user = update.effective_user
        if not is_admin(user.id):
            context.user_data.pop("admin_broadcast", None)
            await update.effective_message.reply_text("❌ Only admins can broadcast.")
            return
        text_to_send = update.message.text
        await update.effective_message.reply_text("Broadcast starting... (send /cancel to abort while it runs)")
        users = list_active_tg_users(limit=10000)
        sent = 0; failed = 0; cancelled = False
        for u in users:
            if not context.user_data.get("admin_broadcast"):
                cancelled = True; break
            try:
                await context.bot.send_message(chat_id=u.get("telegram_id"), text=text_to_send)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        context.user_data.pop("admin_broadcast", None)
        if cancelled:
            await update.effective_message.reply_text(f"Broadcast cancelled. Sent so far: {sent}, failed: {failed}")
        else:
            await update.effective_message.reply_text(f"Broadcast done. Sent: {sent}, failed: {failed}")
        return

    # rename flow
    if context.user_data.get("awaiting_rename_id"):
        sid = context.user_data.pop("awaiting_rename_id")
        new_label = update.message.text.strip()
        owner = update.effective_user.id
        ok = update_saved_account_label(owner, sid, new_label)
        if ok:
            await update.effective_message.reply_text(f"Saved account {sid} renamed to: {new_label}", reply_markup=build_saved_menu())
        else:
            await update.effective_message.reply_text("Could not rename saved account (not found or permission).", reply_markup=build_saved_menu())
        return

    # awaiting save
    if context.user_data.get("awaiting_save"):
        text = update.message.text.strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await update.effective_message.reply_text("Send: <platform> <username> [label]"); return
        platform = parts[0].lower()
        if platform in ("twitter",): platform = "x"
        if platform in ("instagram",): platform = "ig"
        if platform not in ("x", "ig"):
            await update.effective_message.reply_text("Platform must be x or ig."); return
        account = parts[1].lstrip('@').lower()
        label = parts[2] if len(parts) == 3 else None
        owner = update.effective_user.id
        badge = get_user_badge(owner)
        current_count = count_saved_accounts(owner)
        # treat infinite (float('inf')) specially
        if isinstance(badge['save_slots'], (int, float)):
            if current_count >= badge['save_slots']:
                await update.effective_message.reply_text(f"You reached saved limit ({badge['save_slots']}). Remove some or invite to increase.")
                context.user_data.pop("awaiting_save", None)
                return
        try:
            saved = save_user_account(owner, platform, account, label)
            await update.effective_message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})", reply_markup=build_saved_menu())
        except Exception as e:
            await update.effective_message.reply_text(f"Error saving: {e}", reply_markup=build_saved_menu())
        context.user_data.pop("awaiting_save", None)
        return

    # awaiting username (main menu flows)
    if context.user_data.get("awaiting_username"):
        account = update.message.text.strip().lstrip("@").lower()
        platform = context.user_data.get("platform")
        context.user_data["awaiting_username"] = False
        tid = update.effective_user.id
        cooldown_msg = check_and_increment_cooldown(tid)
        if cooldown_msg:
            await update.effective_message.reply_text(cooldown_msg); return
        await update.effective_chat.send_action(ChatAction.TYPING)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.effective_message.reply_text(f"No recent public posts for @{account} on X."); return
            intro = await update.effective_message.reply_text(f"Latest posts from @{account} on X:", reply_markup=build_back_markup("menu_main"))
            sent_ids = []
            for url in posts:
                sent = await update.effective_message.reply_text(url.replace("x.com", "fixupx.com"))
                sent_ids.append(sent.message_id)
                await asyncio.sleep(0.2)
            context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro.chat.id, "message_id": intro.message_id})
            for mid in sent_ids:
                context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.effective_chat.id, "message_id": mid})
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await update.effective_message.reply_text(f"No recent IG posts for @{account}."); return
            intro = await update.effective_message.reply_text(f"Latest IG posts from @{account}:", reply_markup=build_back_markup("menu_main"))
            sent_ids = []
            for post in posts:
                caption = post.get("caption", "")[:1024]
                msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}"
                try:
                    if post.get("is_video"):
                        sent = await update.effective_message.reply_video(post["media_url"], caption=msg, parse_mode="HTML")
                    else:
                        sent = await update.effective_message.reply_photo(post["media_url"], caption=msg, parse_mode="HTML")
                except:
                    sent = await update.effective_message.reply_text(msg, parse_mode="HTML")
                sent_ids.append(sent.message_id)
                await asyncio.sleep(0.3)
            context.job_queue.run_once(delete_message, 86400, data={"chat_id": intro.chat.id, "message_id": intro.message_id})
            for mid in sent_ids:
                context.job_queue.run_once(delete_message, 86400, data={"chat_id": update.effective_chat.id, "message_id": mid})
        return

    # saved_send command
    text = update.message.text.strip()
    if text.startswith("/saved_send"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_send <id>"); return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id."); return
        tid = update.effective_user.id
        cooldown_msg = check_and_increment_cooldown(tid)
        if cooldown_msg:
            await update.effective_message.reply_text(cooldown_msg); return
        saved = get_saved_account(tid, sid)
        if not saved:
            await update.effective_message.reply_text("Saved account not found."); return
        platform = saved["platform"]; account = saved["account_name"]
        await update.effective_chat.send_action(ChatAction.TYPING)
        if platform == "x":
            posts = fetch_latest_urls("x", account)
            if not posts:
                await update.effective_message.reply_text(f"No recent posts for @{account}."); return
            for url in posts:
                await update.effective_message.reply_text(url.replace("x.com", "fixupx.com")); await asyncio.sleep(0.2)
        else:
            posts = fetch_ig_urls(account)
            if not posts:
                await update.effective_message.reply_text(f"No recent IG posts for @{account}."); return
            for post in posts:
                caption = post.get("caption", "")[:1024]
                msg = f"<a href='{post['url']}'>View on IG</a>\n\n{caption}"
                try:
                    if post.get("is_video"):
                        await update.effective_message.reply_video(post["media_url"], caption=msg, parse_mode="HTML")
                    else:
                        await update.effective_message.reply_photo(post["media_url"], caption=msg, parse_mode="HTML")
                except:
                    await update.effective_message.reply_text(msg, parse_mode="HTML")
                await asyncio.sleep(0.3)
        return

    # saved_remove
    if text.startswith("/saved_remove"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_remove <id>"); return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id."); return
        owner = update.effective_user.id
        ok = remove_saved_account(owner, sid)
        if ok:
            await update.effective_message.reply_text(f"Removed saved account {sid}.")
        else:
            await update.effective_message.reply_text("Could not remove saved account.")
        return

    # saved_rename
    if text.startswith("/saved_rename"):
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            await update.effective_message.reply_text("Usage: /saved_rename <id> <new label>"); return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id."); return
        new_label = parts[2].strip()
        owner = update.effective_user.id
        ok = update_saved_account_label(owner, sid, new_label)
        if ok:
            await update.effective_message.reply_text(f"Renamed saved account {sid} -> {new_label}")
        else:
            await update.effective_message.reply_text("Could not rename saved account.")
        return

    # save via command text (/save)
    if text.startswith("/save"):
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            await update.effective_message.reply_text("Usage: /save <platform> <username> [label]"); return
        platform = parts[1].lower()
        if platform in ("twitter",): platform = "x"
        if platform in ("instagram",): platform = "ig"
        if platform not in ("x", "ig"):
            await update.effective_message.reply_text("Platform must be x or ig."); return
        account = parts[2].lstrip('@').lower()
        label = parts[3] if len(parts) == 4 else None
        owner = update.effective_user.id
        badge = get_user_badge(owner)
        current_count = count_saved_accounts(owner)
        if isinstance(badge['save_slots'], (int, float)) and current_count >= badge['save_slots']:
            await update.effective_message.reply_text(f"You reached saved limit ({badge['save_slots']})."); return
        try:
            saved = save_user_account(owner, platform, account, label)
            await update.effective_message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})")
        except Exception as e:
            await update.effective_message.reply_text(f"Error saving: {e}")
        return

    # saved_list
    if text.startswith("/saved_list"):
        owner = update.effective_user.id
        items = list_saved_accounts(owner)
        if not items:
            await update.effective_message.reply_text("No saved accounts. Use /save or the Saved menu."); return
        text_out = "Your saved accounts:\n\n"; rows = []
        for it in items:
            sid = it["id"]; plat = it["platform"]; acc = it["account_name"]; lbl = it.get("label") or ""
            text_out += f"{sid}. [{plat}] @{acc} {('- '+lbl) if lbl else ''}\n"
            rows.append([InlineKeyboardButton(f"Send {sid}", callback_data=f"saved_sendcb_{sid}"),
                         InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                         InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")])
        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])
        await update.effective_message.reply_text(text_out, reply_markup=InlineKeyboardMarkup(rows))
        return

    # default
    await record_user_and_check_ban(update, context)
    await update.effective_message.reply_text("Use the menu or /help for commands.")

# ================ REGISTER & RUN ================
if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Public commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("latest", lambda u,c: latest_command(u,c) if False else None))  # placeholder to avoid name collision
    # Use CommandHandler bound to our latest_command via a small wrapper below
    async def latest_cmd_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await latest_command(update, context)
    # The real latest_command is defined earlier under another name; if you have it, use it.
    # Register other commands:
    app.add_handler(CommandHandler("benefits", benefits_command))
    app.add_handler(CommandHandler("dashboard", dashboard_command))
    app.add_handler(CommandHandler("leaderboard", leaderboard_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("reset_cooldown", reset_cooldown_command))
    app.add_handler(CommandHandler("user_stats", user_stats_command))
    app.add_handler(CommandHandler("export_csv", export_csv_command))
    app.add_handler(CommandHandler("cancel", cancel_command))

    # Saved shortcuts routed to the same message_handler
    app.add_handler(CommandHandler("save", message_handler))
    app.add_handler(CommandHandler("saved_list", message_handler))
    app.add_handler(CommandHandler("saved_send", message_handler))
    app.add_handler(CommandHandler("saved_remove", message_handler))
    app.add_handler(CommandHandler("saved_rename", message_handler))

    # core handlers
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # Command visibility helper
    async def set_command_visibility(application):
        public_cmds = [
            BotCommand("start", "Show welcome / menu"),
            BotCommand("menu", "Open main menu"),
            BotCommand("latest", "Get latest posts for a username"),
            BotCommand("saved_list", "List your saved usernames"),
            BotCommand("save", "Save a username for quick sending"),
            BotCommand("benefits", "See badge benefits"),
            BotCommand("dashboard", "View your status"),
            BotCommand("leaderboard", "Top inviters"),
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
            BotCommand("reset_cooldown", "Reset user cooldown"),
            BotCommand("user_stats", "View user stats"),
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

    # Combine with existing post_init if present
    existing_post_init = getattr(app, "post_init", None)
    if existing_post_init is None:
        app.post_init = set_command_visibility
    else:
        async def _combined_post_init(application):
            try:
                if isinstance(existing_post_init, list):
                    for item in existing_post_init:
                        res = item(application)
                        if asyncio.iscoroutine(res):
                            await res
                elif callable(existing_post_init):
                    res = existing_post_init(application)
                    if asyncio.iscoroutine(res):
                        await res
            except Exception as e:
                print(f"[post_init] existing_post_init wrapper failed: {e}")
            try:
                res2 = set_command_visibility(application)
                if asyncio.iscoroutine(res2):
                    await res2
            except Exception as e:
                print(f"[post_init] set_command_visibility failed: {e}")
        app.post_init = _combined_post_init

    print("[startup] post_init registered")

    # Initialize DB tables
    try:
        init_tg_db()
    except Exception as e:
        print(f"[startup] init_tg_db() failed or not available: {e}")

    print("🤖 MooreLinkBot (full) started — admin + saved accounts + quick-send + badges enabled")

    # IMPORTANT: Ensure only one container instance runs. If you run multiple containers, Telegram
    # will return 409 Conflict ("terminated by other getUpdates request").
    app.run_polling(drop_pending_updates=True)