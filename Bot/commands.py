import math
import logging
from telegram import Update, BotCommand, BotCommandScopeDefault, BotCommandScopeChat
from telegram.ext import ContextTypes
from .settings import *
from .helpers import *
from .ui import *
from .fetch import *
from .ai import ai_tasks
from Utils.utils import *

logger = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    tid = user.id
    first_name = user.first_name or ""

    try:
        is_new = create_user_if_missing(tid, first_name)
    except Exception:
        is_new = False

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
    "🔥 <b>Welcome to MooreLinkBot – Your Ultimate Social Media Tracker!</b> 🔥\n\n"
    
    "Get the latest posts from supported platforms instantly – no login needed! "
    "Save your favorite accounts for one-tap access, climb the ranks with invites, and unlock unlimited power 💎\n\n"
    
    "<b>🚀 Quick Commands</b>\n"
    "• /save &lt;platform&gt; &lt;username&gt; [label] → Save an account for lightning-fast access\n"
    "   Example: <code>/save x elonmusk My GOAT</code>\n"
    "• /saved_list → View all your saved accounts\n"
    "• /saved_send &lt;id&gt; → Instantly fetch latest posts from a saved account\n"
    "• /saved_remove &lt;id&gt; → Delete a saved account\n"
    "• /saved_rename &lt;id&gt; &lt;new label&gt; → Give it a cool nickname\n\n"
    
    "<b>📊 Track Your Progress</b>\n"
    "• /dashboard → See your badge 🏅, invites 📨, save slots 📦, and speed limits ⚡\n"
    "• /benefits → Full breakdown of what each badge unlocks\n"
    "• /leaderboard → Check the top inviters – will YOU claim the throne? 👑\n\n"
    "• /forcemode on|off → Force show latest posts (even previously seen).!"
    "<b>💥 Pro Tip: Invite Friends = Power Up!</b>\n"
    "Every person who joins using <b>your personal invite link</b> boosts your invite count.\n"
    "Higher invites = higher badge = MORE saved slots + FASTER fetching (no waiting!)\n"
    "Reach Diamond 💎 for <u>unlimited everything</u> – no cooldowns, infinite saves!\n\n"
    "Your invite link is in /dashboard. Share it everywhere – groups, bio, stories – and watch your power grow! 🚀\n\n"
    
    "<i>Best way to use me? Tap the menu button below for one-tap magic! ✨</i>"
)
    await update.effective_message.reply_text(help_text, parse_mode="HTML")

async def benefits_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return
    text = (
    "🏆 <b>Badge Levels & Perks</b> 🏆\n\n"
    "Invite friends → level up → get massive boosts!\n\n"
    )
    for level in BADGE_LEVELS[:-1]:
        slots = "Unlimited ♾️" if isinstance(level['save_slots'], float) and math.isinf(level['save_slots']) else level['save_slots']
        min_lim = "Unlimited ♾️" if isinstance(level['limits']['min'], float) and math.isinf(level['limits']['min']) else level['limits']['min']
        hour_lim = "Unlimited ♾️" if isinstance(level['limits']['hour'], float) and math.isinf(level['limits']['hour']) else level['limits']['hour']
        day_lim = "Unlimited ♾️" if isinstance(level['limits']['day'], float) and math.isinf(level['limits']['day']) else level['limits']['day']

        text += f"{level['emoji']} <b>{level['name']}</b> ({level.get('invites_needed', 0)} invites needed)\n"
        text += f"• Save slots: {slots}\n"
        text += f"• Speed: {min_lim}/min | {hour_lim}/hour | {day_lim}/day\n\n"

    text += "💎 <b>Diamond</b>: Truly unlimited – fetch as much as you want, save everything! 👑\n\n"
    text += "<i>Share your invite link (in /dashboard) and climb the ranks today! 🚀</i>"

    await update.effective_message.reply_text(text, parse_mode="HTML")

async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return

    tid = update.effective_user.id
    badge = get_user_badge(tid)
    user = get_tg_user(tid) or {}
    invites = int(user.get('invite_count', 0) or 0)
    saves = count_saved_accounts(tid)

    next_badge = None
    invites_left = 0
    for i, level in enumerate(BADGE_LEVELS):
        if level.get("name") == badge.get("name"):
            if i + 1 < len(BADGE_LEVELS):
                cand = BADGE_LEVELS[i + 1]
                if cand.get("invites_needed") is not None:
                    next_badge = cand
                    invites_left = max(0, cand["invites_needed"] - invites)
            break

    allowed_slots = badge.get("save_slots")
    if isinstance(allowed_slots, (int, float)) and not math.isinf(allowed_slots):
        allowed_str = str(int(allowed_slots))
    else:
        allowed_str = "∞"

    def lim_str(val):
        if isinstance(val, (int, float)) and not math.isinf(val):
            return str(int(val))
        return "∞"

    over_text = ""
    if isinstance(allowed_slots, (int, float)) and not math.isinf(allowed_slots) and saves > allowed_slots:
        over_text = " (over limit — remove some or invite to increase)"

    lines = []
    lines.append("👤 Dashboard\n")
    lines.append(f"🏅 Badge: {badge.get('emoji','')} {badge.get('name','')}")
    lines.append(f"📨 Invites: {invites}")
    lines.append(f"📦 Save Slots: {saves}/{allowed_str}{over_text}\n")
    lines.append("⚡ Speed limits:")
    lines.append(f"• {lim_str(badge.get('limits', {}).get('min'))}/min")
    lines.append(f"• {lim_str(badge.get('limits', {}).get('hour'))}/hour")
    lines.append(f"• {lim_str(badge.get('limits', {}).get('day'))}/day\n")

    if next_badge:
        lines.append(f"⏭ Next Badge: {next_badge.get('emoji','')} {next_badge.get('name','')} ({invites_left} invites left)")
    else:
        lines.append("⚡ Unlimited Access")

    bot_username = context.bot.username or ""
    lines.append(f"\nYour invite link: {get_invite_link(bot_username, tid)}")

    if TEST_MODE.get("enabled"):
        lines.append("🧪 Force Mode: ON (show latest posts even if seen before)")
    else:
        lines.append("🧪 Force Mode: OFF")

    await update.effective_message.reply_text("\n".join(lines))

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

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    await update.effective_message.reply_text("Admin panel:", reply_markup=build_admin_menu())

async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /ban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    await update.effective_message.reply_text(f"Are you sure you want to ban {tid}?", reply_markup=build_confirm_markup("ban", tid))

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /unban <telegram_id>")
        return
    try:
        tid = int(context.args[0])
    except Exception:
        await update.effective_message.reply_text("Invalid id.")
        return
    await update.effective_message.reply_text(f"Are you sure you want to unban {tid}?", reply_markup=build_confirm_markup("unban", tid))

async def reset_cooldown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
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

async def user_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
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
    text += f"Joined: {user.get('joined_at')}\nRequests: {user.get('request_count', 0)}\nInvites: {user.get('invite_count', 0)}\nBanned: {bool(user.get('is_banned'))}\n"
    text += f"Badge: {badge['emoji']} {badge['name']}\nSaves: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
    text += "Cooldowns:\n"
    text += f"Minute: {rl.get('minute_count',0)}/{badge['limits'].get('min','∞')}\n"
    text += f"Hour: {rl.get('hour_count',0)}/{badge['limits'].get('hour','∞')}\n"
    text += f"Day: {rl.get('day_count',0)}/{badge['limits'].get('day','∞')}\n"
    await update.effective_message.reply_text(text)

async def export_csv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    await update.effective_message.reply_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ctx = context.user_data

    cancelled_something = False

    if ctx.pop("awaiting_manual_ai", None):
        await update.effective_message.reply_text("Manual AI input cancelled.", reply_markup=build_admin_menu())
        cancelled_something = True

    task = ai_tasks.get(uid) or ctx.get("ai_task")
    if task and not task.done():
        task.cancel()
        ai_tasks.pop(uid, None)
        ctx.pop("ai_task", None)
        await update.effective_message.reply_text("Running AI analysis cancelled.")
        cancelled_something = True

    for k in ("admin_broadcast", "awaiting_save", "awaiting_username", "awaiting_rename_id", "ai_chat_active"):
        if ctx.pop(k, None):
            cancelled_something = True

    if cancelled_something:
        await update.effective_message.reply_text("All actions cancelled.", reply_markup=build_main_menu())
    else:
        await update.effective_message.reply_text("Nothing to cancel.", reply_markup=build_main_menu())

async def latest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return

    args = context.args or []
    if len(args) >= 2:
        platform = args[0].lower()
        if platform in ("twitter",):
            platform = "x"
        account = args[1].lstrip('@')

        await handle_fetch_and_ai(update, context, platform, account)
        return

    context.user_data["awaiting_username"] = True
    context.user_data["platform"] = "x"
    await update.effective_message.reply_text("Send username (without @) — default platform X. Use /cancel to abort.", reply_markup=build_back_markup("menu_main"))

async def testmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only toggle for test mode."""
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    args = context.args or []
    if not args:
        status = "ON" if TEST_MODE["enabled"] else "OFF"
        await update.effective_message.reply_text(
            f"Test mode is currently: {status}\n\nUsage: /testmode on|off|toggle|status"
        )
        return

    cmd = args[0].lower()
    if cmd in ("on", "enable", "1"):
        TEST_MODE["enabled"] = True
        await update.effective_message.reply_text("✅ Test mode ENABLED — bot will force-send posts seen before.")
    elif cmd in ("off", "disable", "0"):
        TEST_MODE["enabled"] = False
        await update.effective_message.reply_text("❌ Test mode DISABLED — normal behavior restored.")
    elif cmd in ("toggle", "switch"):
        TEST_MODE["enabled"] = not TEST_MODE["enabled"]
        await update.effective_message.reply_text(f"Test mode now: {'ON' if TEST_MODE['enabled'] else 'OFF'}")
    elif cmd == "status":
        await update.effective_message.reply_text(f"Test mode is {'ON' if TEST_MODE['enabled'] else 'OFF'}")
    else:
        await update.effective_message.reply_text("Unknown arg. Use: on|off|toggle|status")

async def reset_all_cooldowns_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /reset_all_cooldowns"""
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("❌ Admins only.")
        return
    if not context.args or context.args[0].lower() != "confirm":
        await update.effective_message.reply_text(
            "⚠️ <b>Dangerous action:</b> This will reset rate limits for <u>ALL users</u>.\n\n"
            "To proceed, type:\n"
            "<code>/reset_all_cooldowns confirm</code>",
            parse_mode="HTML"
        )
        return

    try:
        conn = get_tg_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE tg_rate_limits
            SET 
                minute_count = 0,
                hour_count = 0,
                day_count = 0,
                minute_reset = NULL,
                hour_reset = NULL,
                day_reset = NULL
        """)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        await update.effective_message.reply_text(
            f"✅ <b>Global cooldown reset complete!</b>\n"
            f"Rate limits cleared for <b>{affected}</b> user(s).",
            parse_mode="HTML"
        )

    except Exception as e:
        logging.error(f"reset_all_cooldowns failed: {e}")
        await update.effective_message.reply_text(
            f"❌ Failed to reset global cooldowns:\n<code>{str(e)}</code>",
            parse_mode="HTML"
        )

async def privacy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed = await record_user_and_check_ban(update, context)
    if not allowed:
        await update.effective_message.reply_text("🚫 You are banned.")
        return

    privacy_text = (
        "🔒 <b>Privacy Policy – MooreLinkBot (Social Helper)</b>\n\n"
        "Your privacy matters. Here's exactly what we do (and don't do):\n\n"
        "<b>✅ What we collect</b>\n"
        "• Your Telegram user ID and chat ID – needed to send you updates\n"
        "• The public usernames/handle you ask us to track (e.g., @elonmusk)\n"
        "• Optional labels you give saved accounts\n"
        "• Your invite count and referral data (to unlock benefits)\n\n"
        "<b>🚫 What we NEVER collect</b>\n"
        "• No passwords or login credentials\n"
        "• No access to your private accounts or DMs\n"
        "• No phone number, email, or personal details beyond Telegram basics\n"
        "• No browsing history or unrelated chat data\n\n"
        "<b>🔍 How your data is used</b>\n"
        "• Solely to fetch and deliver public posts from the accounts you choose\n"
        "• To manage your saved list, badges, and invite rewards\n"
        "• Everything stays tied to your Telegram ID – nothing is shared or sold\n\n"
        "<b>🗄️ Storage & Deletion</b>\n"
        "• Data is stored securely in an encrypted database\n"
        "• Remove any saved account anytime with /saved_remove\n"
        "• Stop using the bot or block it → we automatically clean inactive data\n"
        "• Want full deletion? Just message the developer from /dashboard\n\n"
        "<b>🌐 Third parties</b>\n"
        "We only fetch publicly available posts from X and Instagram. "
        "No private APIs, no cookies, no logins required from you.\n\n"
        "<b>✨ Our promise</b>\n"
        "Built as a non-commercial tool to help you escape the noise – "
        "not to create more of it. We respect your attention and your privacy.\n\n"
        "<i>Questions? Reach out anytime via @israelmoorenewcomer → Contact Developer.</i> ❤️"
    )

    await update.effective_message.reply_text(privacy_text, parse_mode="HTML", disable_web_page_preview=True)

# Command visibility function (kept here or moved to a separate file)
async def set_command_visibility(application):
    """Set bot commands for public and admin users."""
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
        BotCommand("export_csv", "Export users CSV (admin only)"),
        BotCommand("reset_all_cooldowns", "Reset cooldown for ALL users (admin only)"),
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