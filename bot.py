# bot.py - FULL COMPLETE UPDATED FILE - NO EXCLUSIONS WHATSOEVER (December 22, 2025)
# All original features preserved + Manual AI now fully button-driven (no /ai_call command)
# Updated Groq models to current best: llama-3.3-70b-versatile (latest flagship)
# Every single line from the original is included or appropriately modified – nothing omitted 😏
import urllib.request
import io
import os
import asyncio
import io
import csv
import math
import logging
from typing import Optional, List, Dict, Any
from functools import wraps
from datetime import datetime
import tempfile
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
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
import logging
# If you use Groq's OpenAI-compatible client via the openai package:
from openai import AsyncOpenAI
from telegram.error import TelegramError
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
    extract_post_id,
    is_post_new,
    mark_posts_seen,
    call_social_ai,
    fetch_fb_urls,
    POST_LIMIT,
    fetch_yt_videos,
)

# ================ CONFIG ================
logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.getenv("BOTTOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("BOTTOKEN env var not set")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

POSTS_PER_PAGE = 5
PAGE_SIZE_USERS = 10
LEADERBOARD_LIMIT = 10
TEST_MODE = {"enabled": False}
# ================ HELPERS ================
ai_tasks: Dict[int, asyncio.Task] = {}
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

async def safe_edit(callback_query, text, parse_mode=None):
    """
    Try to edit the message text if it exists; otherwise edit caption if media; otherwise send a new message.
    callback_query: update.callback_query
    """
    try:
        msg: Message = callback_query.message
        # If original message has text -> edit text
        if getattr(msg, "text", None):
            await callback_query.edit_message_text(text, parse_mode=parse_mode)
            return

        # If original message is media and has caption -> edit caption
        if getattr(msg, "caption", None) is not None:
            await callback_query.edit_message_caption(caption=text, parse_mode=parse_mode)
            return

        # Otherwise we can't edit: send a new message
        await callback_query.message.reply_text(text, parse_mode=parse_mode)

    except TelegramError as e:
        # Fallback: send a new message
        try:
            await callback_query.message.reply_text(text, parse_mode=parse_mode)
        except Exception:
            # last resort: answer callback to avoid spinner
            await callback_query.answer()

async def safe_send_media_or_link(chat, media_url, is_video=False, caption=None, parse_mode=None):
    """
    Robustly send a photo/video or fallback to sending the URL as text.
    chat: update.message.chat or update.effective_chat
    media_url: URL string
    is_video: bool
    Returns the sent message (or None on failure)
    """
    # Quick attempt: try to send by URL first (works if it's a direct file URL)
    try:
        if is_video:
            sent = await chat.send_video(video=media_url, caption=caption, parse_mode=parse_mode)
        else:
            sent = await chat.send_photo(photo=media_url, caption=caption, parse_mode=parse_mode)
        return sent
    except TelegramError as e:
        # Telegram couldn't fetch the URL (common). Try to HEAD the URL to inspect content-type.
        pass
    except Exception:
        pass

    # Inspect content-type via HEAD (some servers block HEAD; try GET if HEAD fails)
    try:
        head = requests.head(media_url, allow_redirects=True, timeout=8)
        content_type = head.headers.get("content-type", "")
        if not content_type:
            # Try GET for content-type
            r = requests.get(media_url, stream=True, timeout=8)
            content_type = r.headers.get("content-type", "")
        # If content-type looks like media -> try download & upload
        if content_type.startswith("image/") or content_type.startswith("video/") or "mpeg" in content_type:
            # Download to temp file then upload
            r = requests.get(media_url, stream=True, timeout=20)
            r.raise_for_status()
            ext = ".jpg" if content_type.startswith("image/") else ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as f:
                for chunk in r.iter_content(1024 * 8):
                    if not chunk:
                        break
                    f.write(chunk)
                tmp_path = f.name

            try:
                if content_type.startswith("image/"):
                    sent = await chat.send_photo(photo=InputFile(tmp_path), caption=caption, parse_mode=parse_mode)
                else:
                    sent = await chat.send_video(video=InputFile(tmp_path), caption=caption, parse_mode=parse_mode)
                return sent
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
    except requests.RequestException:
        pass
    except Exception:
        pass

    # Last resort: send fallback text with link (so user can open manually)
    try:
        fallback = caption + "\n\n" + media_url if caption else media_url
        return await chat.send_message(fallback, parse_mode=parse_mode, disable_web_page_preview=False)
    except Exception:
        return None

async def testmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only toggle for test mode.
    Usage: /testmode            -> shows status and help
           /testmode on|off|toggle|status
    """
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
        # save_testmode()  # uncomment if using persistence
        await update.effective_message.reply_text("✅ Test mode ENABLED — bot will force-send posts seen before.")
        return
    if cmd in ("off", "disable", "0"):
        TEST_MODE["enabled"] = False
        # save_testmode()
        await update.effective_message.reply_text("❌ Test mode DISABLED — normal behavior restored.")
        return
    if cmd in ("toggle", "switch"):
        TEST_MODE["enabled"] = not TEST_MODE["enabled"]
        # save_testmode()
        await update.effective_message.reply_text(f"Test mode now: {'ON' if TEST_MODE['enabled'] else 'OFF'}")
        return
    if cmd == "status":
        await update.effective_message.reply_text(f"Test mode is {'ON' if TEST_MODE['enabled'] else 'OFF'}")
        return

    await update.effective_message.reply_text("Unknown arg. Use: on|off|toggle|status")

async def send_ai_button(message, count, platform, account, badge, context=None, auto_delete_after: int | None = None):
    """
    Send the AI analyze button. If `auto_delete_after` (seconds) provided and `context` job_queue
    is available, schedule deletion after that many seconds. Otherwise do not delete.
    """
    button_text = f"Analyze {count} new post(s) with AI 🤖"
    if badge['name'] in ('Diamond', 'Admin'):
        button_text += " (Unlimited)"

    analyze_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(button_text, callback_data=f"ai_analyze_{platform}_{account}")
    ]])

    final_msg = await message.reply_text(
        f"✨ {count} new post(s) processed!\nTap below for sharp AI breakdown:",
        reply_markup=analyze_kb
    )

    # optional: schedule deletion via job queue if requested
    if auto_delete_after and context and getattr(context, "job_queue", None):
        try:
            # schedule a job that will delete the message
            context.job_queue.run_once(
                lambda c: c.bot.delete_message(chat_id=final_msg.chat.id, message_id=final_msg.message_id),
                when=auto_delete_after,
                data=None
            )
        except Exception as e:
            # fallback: log only
            logging.debug("Failed to schedule AI button deletion: %s", e)

    return final_msg

# --- slight adjustments to your send_next_post_with_confirmation (only small safe edits) ---
async def send_next_post_with_confirmation(update_or_query, context: ContextTypes.DEFAULT_TYPE, platform: str, account: str):
    """
    Robustly replace the preview message with the next pending post and attach a new keyboard.
    This attempts to EDIT the same message (recommended). Falls back to delete+send if needed.
    """
    user_data_key = f"pending_posts_{platform}_{account}"
    pending = context.user_data.get(user_data_key)

    # Resolve message & uid defensively
    message = None
    uid = None
    if hasattr(update_or_query, "callback_query") and update_or_query.callback_query:
        cq = update_or_query.callback_query
        message = cq.message
        uid = cq.from_user.id if cq.from_user else None
    elif hasattr(update_or_query, "effective_message"):
        message = update_or_query.effective_message
        uid = update_or_query.effective_user.id if update_or_query.effective_user else None
    elif hasattr(update_or_query, "message"):
        message = update_or_query.message
        uid = update_or_query.from_user.id if update_or_query.from_user else None

    # Defensive: if no pending or finished -> store last_ai_context, show AI button, clear pending
    if not pending or pending.get("index", 0) >= pending.get("total", 0):
        if not uid and message and message.from_user:
            uid = message.from_user.id
        if not uid:
            logging.warning("send_next_post_with_confirmation: cannot determine uid; clearing pending.")
            context.user_data.pop(user_data_key, None)
            return

        badge = get_user_badge(uid)
        posts_to_store = pending.get("posts", [])[:] if pending else context.user_data.get(f"last_ai_context_{platform}_{account}", [])
        context.user_data[f"last_ai_context_{platform}_{account}"] = posts_to_store

        processed_count = pending.get("index", 0) if pending else 0
        total_count = pending.get("total", processed_count) if pending else len(posts_to_store)

        target_msg = message or update_or_query
        await send_ai_button(target_msg, max(processed_count, total_count), platform, account, badge, context=context, auto_delete_after=None)

        context.user_data.pop(user_data_key, None)
        return

    # Ensure message exists
    if not message:
        logging.warning("send_next_post_with_confirmation: no message object to reply/edit; aborting.")
        return
    if not uid and message.from_user:
        uid = message.from_user.id

    current_idx = pending["index"]
    post = pending["posts"][current_idx]

    # Build caption & keyboard
    view_text = {"x": "View on 𝕏", "fb": "View on Facebook ⓕ", "ig": "View on Instagram 🅾", "yt": "View on YouTube📺"}.get(platform, "View Post 🔗")
    link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
    caption = (post.get("caption") or "")[:1024]
    full_caption = f"{link_html}\n\n{caption}" if link_html else caption
    preview_text = full_caption + "\n\nMove to next post⏭️?" if full_caption else "Move to next post⏭️?"

    # Keyboard with Send this, Skip, Send all (conditional), Cancel
    keyboard_rows = [[
        InlineKeyboardButton(f"✅ Send this post ({current_idx + 1}/{pending['total']})",
                             callback_data=f"confirm_post_{platform}_{account}_{current_idx}"),
        InlineKeyboardButton("⏭️ Skip this post", callback_data=f"skip_post_{platform}_{account}_{current_idx}")
    ]]
    if not pending.get("has_sent_single", False):
        keyboard_rows.append([
            InlineKeyboardButton(f"✅ Send all remaining ({pending['total'] - current_idx})", callback_data=f"send_all_{platform}_{account}")
        ])
    keyboard_rows.append([
        InlineKeyboardButton("❌ Cancel remaining", callback_data=f"cancel_posts_{platform}_{account}")
    ])
    keyboard = InlineKeyboardMarkup(keyboard_rows)

    # Increment index NOW (before send/edit) to prevent stale clicks
    pending["index"] += 1
    context.user_data[user_data_key] = pending

    sent_preview = False
    edited = False
    preview_msg = None  # Will hold the final sent message (edited or new)

    if message:
        try:
            # Try media edit with short timeout
            if post.get("is_video"):
                media = InputMediaVideo(media=post.get("media_url"), caption=preview_text, parse_mode="HTML")
            else:
                media = InputMediaPhoto(media=post.get("media_url"), caption=preview_text, parse_mode="HTML")

            await asyncio.wait_for(
                context.bot.edit_message_media(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    media=media,
                    reply_markup=keyboard
                ),
                timeout=8.0
            )
            sent_preview = True
            edited = True
            preview_msg = message  # We edited the existing message
            logging.info("Edited preview msg %s to idx %s", message.message_id, current_idx)
        except asyncio.TimeoutError:
            logging.warning("Media edit timed out on msg %s", message.message_id)
        except Exception as edit_exc:
            logging.warning("Media edit failed on msg %s: %s", message.message_id, edit_exc)

        if not sent_preview:
            # Fallback: try text edit
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        text=preview_text,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    ),
                    timeout=8.0
                )
                sent_preview = True
                edited = True
                preview_msg = message
                logging.info("Fallback text edit success on %s", message.message_id)
            except asyncio.TimeoutError:
                logging.warning("Text edit also timed out on msg %s", message.message_id)
            except Exception as e2:
                logging.warning("Text edit failed: %s", e2)

    # If still not sent (both edits failed), use safe_send_media_or_link as final fallback
    if not sent_preview:
        preview_msg = await safe_send_media_or_link(
            chat=message.chat,
            media_url=post.get("media_url"),
            is_video=post.get("is_video", False),
            caption=preview_text,
            parse_mode="HTML"
        )
        if preview_msg:
            sent_preview = True
            logging.info("Sent fallback preview using safe_send_media_or_link (msg %s)", preview_msg.message_id)
        else:
            logging.error("All media send attempts failed for post idx %s", current_idx)

    # Schedule auto-delete for the final preview message
    if sent_preview and preview_msg:
        try:
            await schedule_delete(context, preview_msg.chat.id, preview_msg.message_id)
        except Exception as e:
            logging.debug("Failed to schedule delete for preview: %s", e)

    # If we successfully edited the original message, also schedule its delete (in case of media → text switch)
    if edited and message:
        try:
            await schedule_delete(context, message.chat.id, message.message_id)
        except Exception:
            pass

async def download_media(url: str) -> bytes:
    """Download media using urllib (no external deps) – async compatible"""
    loop = asyncio.get_event_loop()
    try:
        # Run blocking urllib in thread pool to avoid blocking event loop
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
            }
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status == 200:
                data = response.read()
                if len(data) > 50 * 1024 * 1024:  # >50MB → Telegram no go accept
                    logging.warning(f"Media too large ({len(data)/1024/1024:.1f}MB): {url}")
                    return None
                return data
            else:
                logging.warning(f"Download failed {url} - status {response.status}")
                return None
    except Exception as e:
        logging.error(f"Download error {url}: {e}")
        return None

# ================ UI BUILDERS ================
def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("X (Twitter)𝕏", callback_data="menu_x")],
        [InlineKeyboardButton("Instagram🅾", callback_data="menu_ig")],
        [InlineKeyboardButton("Facebookⓕ", callback_data="menu_fb")],
        [InlineKeyboardButton("YouTube📹", callback_data="menu_yt")],
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
        [InlineKeyboardButton("🧠 Manual AI Analyze", callback_data="admin_ai_start")],
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
        increment_tg_request_count(tid)  # Track general activity
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
async def schedule_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay_seconds: int = 86400):
    if context.job_queue:
        context.job_queue.run_once(
            delete_message,
            when=delay_seconds,
            data={"chat_id": chat_id, "message_id": message_id},
            name=f"delete_{message_id}"
        )

# ================ UNIFIED FETCH & AI BUTTON ================
async def handle_fetch_and_ai(update, context, platform, account, query=None, force: bool = False):
    uid = update.effective_user.id
    message = query.message if query else update.effective_message
    
    if TEST_MODE.get("enabled"):
        force = True
        
    cooldown_msg = check_and_increment_cooldown(uid)
    if cooldown_msg:
        await message.reply_text(cooldown_msg)
        return
    force_send = TEST_MODE.get("enabled", False)
    await message.chat.send_action(ChatAction.TYPING)

    # Fetch raw posts
    # Fetch raw posts
    if platform == "x":
        raw_posts = fetch_latest_urls("x", account)
        post_list = [{"post_id": extract_post_id("x", url), "post_url": url, "caption": ""} for url in raw_posts]
    elif platform == "ig":
        raw_ig = fetch_ig_urls(account)
        post_list = []
        for p in raw_ig:
            pid = extract_post_id("ig", p['url'])
            post_list.append({
                "post_id": pid,
                "post_url": p['url'],
                "caption": p.get("caption", ""),
                "media_url": p.get("media_url"),
                "is_video": p.get("is_video", False)
            })
    elif platform == "fb":
        # Now it's async – await it!
        raw_fb = fetch_fb_urls(account)
        post_list = []
        for p in raw_fb:
            pid = p.get("post_id") or p.get("post_url", "")
            post_list.append({
                "post_id": pid,
                "post_url": p['post_url'],
                "caption": p.get("caption", ""),
                "media_url": p.get("media_url"),
                "is_video": p.get("is_video", False)
            })
    elif platform == "yt":
        raw_yt = fetch_yt_videos(channel_handle=account)
        post_list = []
        
        for v in raw_yt:
            post_list.append({
                 "post_id": v["post_id"],
                "post_url": v["post_url"],
                "caption": v["caption"],      # already formatted
                "media_url": v["media_url"],
                "is_video": True              # YouTube posts are videos
            })
    else:
        await message.reply_text("Unsupported platform.")
        return

    # Only new posts
    # Only new posts (normal mode)
    new_posts = [p for p in post_list if is_post_new(uid, platform, account, p['post_id'])]

    # TEST MODE: If enabled, ignore "seen" status and force send latest fetched posts
    if force_send:
        logging.info("🧪 Force mode ACTIVE for user %s — sending latest posts (ignoring seen status)", uid)
        new_posts = post_list[:POST_LIMIT]
        # Still mark as seen so next normal fetch no repeat unnecessarily
        mark_posts_seen(uid, platform, account, [{"post_id": p['post_id'], "post_url": p['post_url']} for p in new_posts])
    elif not new_posts:
        await message.reply_text(f"No new posts from @{account} since your last check.")
        return
    else:
        mark_posts_seen(uid, platform, account, [{"post_id": p['post_id'], "post_url": p['post_url']} for p in new_posts])
    # Store posts for sequential sending and AI context
    context.user_data[f"pending_posts_{platform}_{account}"] = {
        "posts": new_posts,
        "index": 0,
        "total": len(new_posts),
        "has_sent_single": False 
    }
    context.user_data[f"last_ai_context_{platform}_{account}"] = new_posts

    if not new_posts:
        # existing no new posts handling remains
        return

    # Start sending the first post with confirmation
    await send_next_post_with_confirmation(update, context, platform, account)
# ================ IMPROVED MANUAL AI TASK ================
async def run_ai_task(user_id: int, text: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE, source: str = "manual"):
    logging.info("run_ai_task started for user %s (source=%s)", user_id, source)

    # Persona + text to analyze
    system_msg = "You are a sharp Nigerian social media analyst. Answer short, direct, and use Pidgin-mixed English when appropriate."
    user_msg = text

    MODEL_CANDIDATES = [
        "llama-3.3-70b-versatile",
        "llama-3.1-8b-instant",
        "llama-guard-3-8b",
    ]

    try:
        api_key = os.getenv("GROQ_KEY")
        if not api_key:
            logging.error("GROQ_API_KEY not set")
            await context.bot.send_message(chat_id=chat_id, text="❌ Server misconfigured: missing GROQ_API_KEY.")
            return

        client = AsyncOpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")

        # Optional: send ephemeral working message
        working_msg = None
        try:
            working_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ AI is thinking... (this may take a few seconds)")
        except Exception:
            working_msg = None

        last_exc = None
        success = False

        for model_id in MODEL_CANDIDATES:
            try:
                logging.info("Attempting model '%s' for user %s", model_id, user_id)
                response = await client.chat.completions.create(
                    model=model_id,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg}
                    ],
                    temperature=0.7,
                    max_tokens=700
                )

                # Extract content defensively
                content = None
                try:
                    content = response.choices[0].message.content.strip()
                except Exception:
                    logging.exception("Malformed response structure from model %s", model_id)
                    content = None

                if content:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🤖 AI Result (model: {model_id}, source: {source}):\n\n{content}"
                    )
                    await schedule_delete(context, update.message.chat.id, sent.message_id)
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🤖 AI returned no content (model: {model_id})."
                    )

                logging.info("Model %s succeeded for user %s", model_id, user_id)
                success = True
                last_exc = None
                break  # done

            except asyncio.CancelledError:
                logging.info("AI task cancelled (model loop) for user %s", user_id)
                try:
                    await context.bot.send_message(chat_id=chat_id, text="🛑 AI analysis cancelled.")
                except Exception:
                    pass
                raise

            except Exception as e:
                last_exc = e
                emsg = str(e).lower()
                logging.warning("Model %s failed for user %s: %s", model_id, user_id, emsg)

                # If model decommissioned or model-not-found, try next candidate
                if any(tok in emsg for tok in ("decommissioned", "model_decommissioned", "model not found", "model not found", "not found")):
                    logging.info("Model %s appears decommissioned or missing; trying next candidate.", model_id)
                    continue

                # For other errors, still try next candidate (networks/auth may be transient)
                continue

        # Clean up the working message if present
        try:
            if working_msg:
                await schedule_delete(context, update.message.chat.id, sent.message_id)
        except Exception:
            # not critical if delete fails
            pass

        if not success:
            logging.exception("All model candidates failed for user %s", user_id)
            try:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ AI error (all models failed): {last_exc}")
            except Exception:
                pass

    except asyncio.CancelledError:
        # bubbled from outer cancellation
        logging.info("run_ai_task cancelled for user %s", user_id)
        try:
            await context.bot.send_message(chat_id=chat_id, text="🛑 AI analysis cancelled.")
        except Exception:
            pass
        raise

    except Exception as e:
        logging.exception("run_ai_task unexpected failure for user %s: %s", user_id, e)
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ AI unexpected error: {e}")
        except Exception:
            pass

    finally:
        # Ensure registry cleanup
        try:
            ai_tasks.pop(user_id, None)
        except Exception:
            pass
        # also clear user_data slot if present
        try:
            if isinstance(context.user_data, dict):
                context.user_data.pop("ai_task", None)
        except Exception:
            pass

    logging.info("run_ai_task finished for user %s", user_id)

# ================ CALLBACK HANDLER ================
# --- patched callback handler (replace your existing one) ---
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    uid = user.id if user else None

    await record_user_and_check_ban(update, context)
    data = query.data or ""

    # AI Analysis Button
    if data.startswith("ai_analyze_"):
        _, _, plat_acc = data.partition("ai_analyze_")
        platform, _, account = plat_acc.partition("_")

        badge = get_user_badge(uid)

        # Extra cost for non-unlimited
        if badge['name'] not in ('Diamond', 'Admin'):
            cooldown_msg = check_and_increment_cooldown(uid)
            if cooldown_msg:
                await query.answer("AI limit reached! Invite friends to upgrade.", show_alert=True)
                return

        # Inform user we are analyzing
        await safe_edit(query, text="🤖 Analyzing with Nigerian fire...")

        # Collect posts context and call AI
        posts = context.user_data.get(f"last_ai_context_{platform}_{account}", [])
        analysis = await call_social_ai(platform, account, posts)

        final_text = f"🤖 <b>AI Insight</b>:\n\n{analysis}"

        if badge['name'] in ('Diamond', 'Admin'):
            final_text += "\n\n💎 <b>You can ask me follow-up questions about these posts!</b>\nJust reply to this message."
            context.user_data["ai_chat_active"] = {
                "platform": platform,
                "account": account,
                "posts": posts
            }

        # Replace the analyzing message with final analysis
        await safe_edit(query, text=final_text, parse_mode="HTML")
        return

    # Saved quick send
    if data.startswith("saved_sendcb_"):
        _, _, sid_s = data.partition("saved_sendcb_")
        try:
            sid = int(sid_s)
        except:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text="Invalid saved id."
            )
            return

        saved = get_saved_account(uid, sid)
        if not saved:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text="Saved account not found."
            )
            return

        await handle_fetch_and_ai(update, context, saved["platform"], saved["account_name"], query)
        return

    # Confirm (send) single post
    if data.startswith("confirm_post_"):
        await query.answer()  # acknowledge immediately

        parts = data.split("_")
        if len(parts) < 5:
            await query.answer("Invalid callback.", show_alert=True)
            return

        platform = parts[2]
        account = "_".join(parts[3:-1])  # handle accounts with _ in name
        idx = int(parts[-1])

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending or pending.get("index", 0) != idx:
            logging.info(f"Stale click detected: expected index {pending.get('index', 'None')}, got {idx} for @{account}")
            await query.answer("Post expired or out of order.", show_alert=True)
            await send_next_post_with_confirmation(update, context, platform, account)
            return

        # Mark that a single send happened (hides "Send all" on future previews)
        pending["has_sent_single"] = True
        context.user_data[user_data_key] = pending
        logging.info("Flag set: has_sent_single=True for %s/%s", platform, account)
        
        post = pending["posts"][idx]
        # Download and send clean post
        media_bytes = await download_media(post.get("media_url"))
        if not media_bytes:
            # edit the preview to indicate media failed and remove buttons
            try:
                await context.bot.edit_message_caption(
                    chat_id=query.message.chat.id,
                    message_id=query.message.message_id,
                    caption=(query.message.caption or "") + "\n\n❌ Media failed to load",
                    reply_markup=None
                )
            except Exception:
                try:
                    await context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=(query.message.text or "") + "\n\n❌ Media failed to load",
                        reply_markup=None
                    )
                except Exception as e:
                    logging.warning("Failed to mark preview as failed: %s", e)

            # advance and persist, then show next
            pending["index"] += 1
            context.user_data[user_data_key] = pending
            await send_next_post_with_confirmation(query, context, platform, account)
            return

        view_text = {"x": "View on X🐦", "fb": "View on Facebook 🌐", "ig": "View on Instagram 📸"}.get(platform, "View Post 🔗")
        link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
        caption = (post.get("caption", "") or "")[:1024]
        full_caption = f"{link_html}\n\n{caption}" if link_html else caption

        bio = io.BytesIO(media_bytes)
        if post.get("is_video"):
            bio.name = "video.mp4"
            sent = await query.message.reply_video(video=bio, caption=full_caption, parse_mode="HTML")
        else:
            bio.name = "photo.jpg"
            sent = await query.message.reply_photo(photo=bio, caption=full_caption, parse_mode="HTML")

        # Schedule auto-delete for sent post (use sent's ids)
        await schedule_delete(context, sent.chat.id, sent.message_id)

        # Edit old preview to "Sent!" and REMOVE buttons using explicit edit
        await safe_edit(query, text=new_caption + "\n\n✅ <b>Sent!</b>", parse_mode="HTML", reply_markup=None)

        # Advance and persist before showing next preview
        pending["index"] += 1
        context.user_data[user_data_key] = pending

        # Show next preview
        await send_next_post_with_confirmation(query, context, platform, account)
        return
        
    # NEW: Send all remaining
    if data.startswith("send_all_"):
        query = update.callback_query
        await query.answer()

        # --- robust parsing (supports underscores in account names)
        parts = data.split("_")
        if len(parts) < 4:
            await query.answer("Invalid callback.", show_alert=True)
            return
        platform = parts[2]
        account = "_".join(parts[3:])

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending:
            await query.answer("No pending posts.", show_alert=True)
            return

        # If user already sent a single post, block bulk-send
        if pending.get("has_sent_single"):
            await query.answer("Bulk send disabled after single send.", show_alert=True)
            await send_next_post_with_confirmation(update, context, platform, account)
            return

        # Flip the flag immediately and persist so future previews hide "Send all"
        pending["has_sent_single"] = True
        context.user_data[user_data_key] = pending
        logging.info("send_all: has_sent_single set for %s/%s", platform, account)

        # Defensive shape
        posts = pending.get("posts", []) or []
        current_idx = int(pending.get("index", 0))
        total_posts = int(pending.get("total", len(posts)))

        # Edit preview to "Sending all..." (use short timeout to avoid long blocking)
        try:
            preview_text = (query.message.caption or query.message.text or "") + "\n\n🚀 Sending all remaining..."
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_caption(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        caption=preview_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
            except (asyncio.TimeoutError, Exception):
                # fallback to text edit
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=preview_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
        except Exception as e:
            logging.warning("send_all: could not mark preview as sending: %s", e)

        total_sent = 0

        # Loop and send remaining posts (persist index as we go)
        for idx in range(current_idx, min(total_posts, len(posts))):
            post = posts[idx]
            try:
                media_bytes = await download_media(post.get("media_url"))
            except Exception as e:
                logging.warning("send_all: download_media exception for idx %s: %s", idx, e)
                media_bytes = None

            if not media_bytes:
                logging.info("send_all: skipping idx %s (media failed)", idx)
                # advance index so we don't get stuck
                pending["index"] = idx + 1
                context.user_data[user_data_key] = pending
                continue

            view_text = {
                "x": "View on X🐦",
                "fb": "View on Facebook 🌐",
                "ig": "View on Instagram 📸"
            }.get(platform, "View Post 🔗")
            link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
            caption = (post.get("caption") or "")[:1024]
            full_caption = f"{link_html}\n\n{caption}" if link_html else caption

            bio = io.BytesIO(media_bytes)
            try:
                if post.get("is_video"):
                    bio.name = "video.mp4"
                    sent = await context.bot.send_video(
                        chat_id=query.message.chat.id,
                        video=bio,
                        caption=full_caption,
                        parse_mode="HTML"
                    )
                else:
                    bio.name = "photo.jpg"
                    sent = await context.bot.send_photo(
                        chat_id=query.message.chat.id,
                        photo=bio,
                        caption=full_caption,
                        parse_mode="HTML"
                    )

                # schedule delete for the sent post (try await then fallback)
                try:
                    await schedule_delete(context, sent.chat.id, sent.message_id)
                except TypeError:
                    # schedule_delete might be synchronous
                    try:
                        schedule_delete(context, sent.chat.id, sent.message_id)
                    except Exception:
                        logging.debug("schedule_delete failed for sent message.")
                except Exception:
                    logging.debug("schedule_delete failed for sent message.")

                total_sent += 1
                logging.info("send_all: sent idx %s for %s/%s", idx, platform, account)

            except Exception as e:
                logging.exception("send_all: failed to send idx %s: %s", idx, e)
                # do not stop the loop; advance index and continue

            # persist progress immediately so other handlers see updated state
            pending["index"] = idx + 1
            context.user_data[user_data_key] = pending

            # small sleep to avoid hitting rate limits (optional but recommended)
            await asyncio.sleep(5)

        # Finished sending: mark pending complete
        pending["index"] = pending.get("total", pending.get("index", 0))
        context.user_data[user_data_key] = pending

        # Edit original preview to done (best-effort)
        try:
            done_text = (query.message.caption or query.message.text or "") + f"\n\n✅ Sent all remaining ({total_sent} posts)!"
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_caption(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        caption=done_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
            except (asyncio.TimeoutError, Exception):
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=done_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
        except Exception as e:
            logging.warning("send_all: could not mark preview done: %s", e)

        # Show AI button if we sent anything
        if total_sent > 0:
            badge = get_user_badge(uid)
            await send_ai_button(query.message, total_sent, platform, account, badge)

        # Finally clear pending
        context.user_data.pop(user_data_key, None)
        return

    if data.startswith("skip_post_"):
        _, _, plat_acc_idx = data.partition("skip_post_")
        platform, _, acc_idx = plat_acc_idx.partition("_")
        account, _, idx_s = acc_idx.partition("_")
        try:
            idx = int(idx_s)
        except:
            await query.answer("Invalid index.", show_alert=True)
            return

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending or pending.get("index", 0) != idx:
            await query.answer("Post expired or out of order.", show_alert=True)
            await send_next_post_with_confirmation(query, context, platform, account)
            return

        # Edit preview to "Skipped"
        try:
            await context.bot.edit_message_caption(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                caption=(query.message.caption or "") + "\n\n⏭️ <b>Skipped!</b>",
                parse_mode="HTML",
                reply_markup=None
            )
        except Exception:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text=(query.message.text or "") + "\n\n⏭️ <b>Skipped!</b>",
                parse_mode="HTML",
                reply_markup=None
            )

        # Advance index and persist
        pending["index"] += 1
        context.user_data[user_data_key] = pending

        # Show next preview
        await send_next_post_with_confirmation(query, context, platform, account)
        return

    # Cancel remaining posts
    if data.startswith("cancel_posts_"):
        _, _, plat_acc = data.partition("cancel_posts_")
        platform, _, account = plat_acc.partition("_")

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.pop(user_data_key, None)

        sent_count = pending["index"] if pending else 0
        total = pending["total"] if pending else 0

        await safe_edit(query, text=(query.message.caption or "") + f"\n\n❌ Cancelled. Sent {sent_count}/{total} posts.", parse_mode="HTML", reply_markup=None)
        await query.message.reply_text(f"❌ Sending cancelled. Sent {sent_count}/{total} posts.")

        # Show AI button if at least one sent
        if pending and pending["index"] > 0:
            badge = get_user_badge(uid)
            await send_ai_button(query.message, pending["index"], platform, account, badge)
        return
    # ... rest of callbacks (confirm_unban_, confirm_export_csv, etc.) unchanged ...
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

    if data == "menu_main":
        await query.edit_message_text("Main menu:", reply_markup=build_main_menu())
        return
    if data == "dashboard":
        await dashboard_command(update, context)
        return
    if data == "menu_x":
        context.user_data["platform"] = "x"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the X username (without @):", reply_markup=build_back_markup("menu_main"))
        return
    if data == "menu_fb":
        context.user_data["platform"] = "fb"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text(
            "Send the Facebook page username or name (e.g. nike, coca-cola):\n\n"
            "Note: Only public pages and send direct page link for accuracy(recommended)! only the posted pictures is fetched",
            reply_markup=build_back_markup("menu_main")
        )
        return
    if data == "menu_ig":
        context.user_data["platform"] = "ig"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the Instagram username (without @):", reply_markup=build_back_markup("menu_main"))
        return
    if data =="help":
        await help_command(update, context)
        return
        
    if data == "menu_yt":
        context.user_data["platform"] = "yt"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send YouTube channel username (e.g. Seyivibe) or search query:", reply_markup=build_back_markup("menu_main"))
        return
        
    if data == "saved_menu":
        await query.edit_message_text("Saved usernames:", reply_markup=build_saved_menu())
        return
    if data == "saved_add_start":
        context.user_data["awaiting_save"] = True
        await query.edit_message_text("Send: <platform> <username> [label]\nExample: `x vdm fav`", reply_markup=build_back_markup("saved_menu"))
        return
    if data == "saved_list" or data.startswith("saved_page_"):
        page = 0
        if data.startswith("saved_page_"):
            page = int(data[len("saved_page_"):])

        items = list_saved_accounts(uid)
        if not items:
            await query.edit_message_text("You no get any saved account. Save page link when saving in fb", reply_markup=build_saved_menu())
            return

        per_page = 4
        start = page * per_page
        end = start + per_page
        page_items = items[start:end]
        total_pages = (len(items) + per_page - 1) // per_page

        text = f"Your saved accounts ({page+1}/{total_pages}):\n\n"
        rows = []
        for it in page_items:
            sid = it["id"]
            plat = it["platform"].upper()
            acc = it["account_name"]
            lbl = it.get("label") or ""
            display = f"{sid}. [{plat}] @{acc}"
            if lbl:
                display += f" — {lbl}"
            text += display + "\n"

            rows.append([
                InlineKeyboardButton("Send", callback_data=f"saved_sendcb_{sid}"),
                InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")
            ])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"saved_page_{page-1}"))
        if end < len(items):
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"saved_page_{page+1}"))
        if nav:
            rows.append(nav)

        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
        return
    if data.startswith("saved_removecb_"):
        _, _, sid_s = data.partition("saved_removecb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
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
            await query.edit_message_text("Invalid id.")
            return
        context.user_data["awaiting_rename_id"] = sid
        await query.edit_message_text("Send the new label for this saved account (single message):", reply_markup=build_back_markup("saved_list"))
        return

    if data.startswith("admin_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return

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
                await query.edit_message_text("Invalid id.")
                return
            stats = get_user_stats(tid)
            if not stats:
                await query.edit_message_text("User not found.")
                return
            user = stats['user']
            badge = stats['badge']
            rl = stats['rate_limits']
            saves = stats['save_count']
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
                await query.edit_message_text("Invalid id.")
                return
            await query.edit_message_text(f"Confirm reset cooldown for {tid}?", reply_markup=build_confirm_markup("reset_cooldown", tid))
            return

        if data == "admin_leaderboard":
            await query.edit_message_text("Loading leaderboard...", reply_markup=build_back_markup("admin_back"))
            return

        if data == "admin_back":
            await query.edit_message_text("Admin panel:", reply_markup=build_admin_menu())
            return

        if data == "admin_export_csv":
            await query.edit_message_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))
            return

        if data == "admin_broadcast_start":
            context.user_data["admin_broadcast"] = True
            await query.edit_message_text("Send the message to broadcast. Use /cancel to abort.", reply_markup=build_cancel_and_back("admin_broadcast_cancel", "admin_back"))
            return

        if data == "admin_broadcast_cancel":
            context.user_data.pop("admin_broadcast", None)
            await query.edit_message_text("Broadcast cancelled.", reply_markup=build_admin_menu())
            return

        if data == "admin_ai_start":
            context.user_data["awaiting_manual_ai"] = True
            await query.edit_message_text(
                "🧠 <b>Manual AI Analysis</b>\n\n"
                "Send the text/post/caption you want me to analyze with Groq AI.\n"
                "If you just fetched posts, I can auto-use them if you send nothing.\n\n"
                "/cancel to abort.",
                parse_mode="HTML",
                reply_markup=build_back_markup("admin_back")
            )
            return

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

# ================ MESSAGE HANDLER ================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    uid = update.effective_user.id
    badge = get_user_badge(uid)

    # AI Follow-up Chat (ONLY Diamond & Admin)
        # AI Follow-up Chat (ONLY Diamond & Admin) – FIXED & ROBUST
    if context.user_data.get("ai_chat_active") and badge['name'] in ('Diamond', 'Admin'):
        chat_context = context.user_data["ai_chat_active"]
        posts = chat_context["posts"]
        question = update.message.text.strip()

        captions_text = "\n---\n".join([
            p.get("caption", "No caption") or ""
            for p in posts
        ])

        prompt = f"""
You are a sharp Nigerian social media analyst. Use Pidgin-mixed English, short and direct.

Previous posts from @{chat_context['account']} ({chat_context['platform'].upper()}):
{captions_text}

User follow-up question: {question}

Answer in max 6 sentences. Keep it engaging.
"""

        await update.message.chat.send_action(ChatAction.TYPING)

        try:
            client = AsyncOpenAI(
                api_key=os.getenv("GROQ_KEY"),
                base_url="https://api.groq.com/openai/v1"
            )
            response = await client.chat.completions.create(
                model="llama-3.3-70b-versatile",  # updated flagship
                messages=[{"role": "user", "content": prompt}],
                temperature=0.8,
                max_tokens=500
            )
            answer = response.choices[0].message.content.strip()
            await update.message.reply_text(
                f"🤖 <b>AI Follow-up</b>:\n\n{answer}\n\n<i>Reply again for more questions!</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"AI follow-up failed: {e}")
            await update.message.reply_text("🤖 AI temporary unavailable. Try again later.")

        return  # message consumed – keep ai_chat_active flag alive!  # consume the message
    # Manual AI Analysis (Admin only, button-driven - NOW MULTI-TURN!)
    if context.user_data.get("awaiting_manual_ai"):
        if not is_admin(uid):
            context.user_data.pop("awaiting_manual_ai", None)
            await update.effective_message.reply_text("❌ Only admins can use Manual AI.")
            return

        user_text = update.message.text.strip()

        if not user_text:
            await update.effective_message.reply_text(
                "📝 Send the text, post, link, or caption you want analyzed.\n"
                "I go analyze each one sharp-sharp.\n"
                "/cancel to stop Manual AI mode."
            )
            return

        # DO NOT pop the flag here → keep mode active for multiple inputs!

        # Run AI task
        task = asyncio.create_task(
            run_ai_task(
                user_id=uid,
                text=user_text,
                chat_id=update.effective_chat.id,
                context=context,
                source="manual_admin"
            )
        )
        ai_tasks[uid] = task
        context.user_data["ai_task"] = task  # for /cancel support

        await update.effective_message.reply_text(
            "🚀 AI dey think on top your text...\n"
            "Hold...."
        )
        return
        
    text = update.message.text.strip().lower()

    # Detect Facebook single post share links
    if ("facebook.com/share/" in text or "mibextid=" in text or 
        text.startswith("https://www.facebook.com/") or text.startswith("https://fb.watch/")):
        # Clean the link (remove tracking)
        clean_link = update.message.text.split("?")[0].rstrip("/")
        await update.message.reply_text(
            f"🌐 Single Facebook post:\n{clean_link}",
            disable_web_page_preview=False
        )
        return

    # Admin broadcast
    if context.user_data.get("admin_broadcast"):
        if not is_admin(uid):
            context.user_data.pop("admin_broadcast", None)
            await update.effective_message.reply_text("❌ Only admins can broadcast.")
            return
        text_to_send = update.message.text
        await update.effective_message.reply_text("Broadcast starting... (send /cancel to abort while it runs)")
        users = list_active_tg_users(limit=10000)
        sent = 0
        failed = 0
        cancelled = False
        for u in users:
            if not context.user_data.get("admin_broadcast"):
                cancelled = True
                break
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

    # Rename flow
    if context.user_data.get("awaiting_rename_id"):
        sid = context.user_data.pop("awaiting_rename_id")
        new_label = update.message.text.strip()
        ok = update_saved_account_label(uid, sid, new_label)
        if ok:
            await update.effective_message.reply_text(f"Saved account {sid} renamed to: {new_label}", reply_markup=build_saved_menu())
        else:
            await update.effective_message.reply_text("Could not rename saved account.", reply_markup=build_saved_menu())
        return

    # Add saved flow
    if context.user_data.get("awaiting_save"):
        text = update.message.text.strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await update.effective_message.reply_text("Send: <platform> <username> [label]")
            return
        platform = parts[0].lower()
        if platform in ("twitter",):
            platform = "x"
        if platform in ("instagram",):
            platform = "ig"
        if platform in ("facebook",):
            platform = "fb"
        if platform not in ("x", "ig", "fb"):
            await update.effective_message.reply_text("Platform must be x, fb, or ig.")
            return
        account = parts[1].lstrip('@').lower()
        label = parts[2] if len(parts) == 3 else None
        current_count = count_saved_accounts(uid)
        if isinstance(badge['save_slots'], (int, float)) and current_count >= badge['save_slots']:
            await update.effective_message.reply_text(f"You reached saved limit ({badge['save_slots']}). Remove some or invite to increase.")
            context.user_data.pop("awaiting_save", None)
            return
        try:
            saved = save_user_account(uid, platform, account, label)
            await update.effective_message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})", reply_markup=build_saved_menu())
        except Exception as e:
            await update.effective_message.reply_text(f"Error saving: {e}", reply_markup=build_saved_menu())
        context.user_data.pop("awaiting_save", None)
        return

    # Prompted username flow
    if context.user_data.get("awaiting_username"):
        account = update.message.text.strip().lstrip("@").lower()
        platform = context.user_data.get("platform", "x")
        context.user_data["awaiting_username"] = False
        await handle_fetch_and_ai(update, context, platform, account)
        return

    # /saved_send command
    text = update.message.text.strip()
    if text.startswith("/saved_send"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_send <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        saved = get_saved_account(uid, sid)
        if not saved:
            await update.effective_message.reply_text("Saved account not found.")
            return
        await handle_fetch_and_ai(update, context, saved["platform"], saved["account_name"])
        return

    if text.startswith("/saved_remove"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_remove <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        ok = remove_saved_account(uid, sid)
        if ok:
            await update.effective_message.reply_text(f"Removed saved account {sid}.")
        else:
            await update.effective_message.reply_text("Could not remove saved account.")
        return

    if text.startswith("/saved_rename"):
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            await update.effective_message.reply_text("Usage: /saved_rename <id> <new label>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        new_label = parts[2].strip()
        ok = update_saved_account_label(uid, sid, new_label)
        if ok:
            await update.effective_message.reply_text(f"Renamed saved account {sid} -> {new_label}")
        else:
            await update.effective_message.reply_text("Could not rename saved account.")
        return

    if text.startswith("/save"):
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            await update.effective_message.reply_text("Usage: /save <platform> <username> [label]")
            return
        platform = parts[1].lower()
        if platform in ("twitter",):
            platform = "x"
        if platform in ("facebook",):
            platform = "fb"
        if platform in ("instagram",):
            platform = "ig"
        if platform not in ("x", "ig", "fb"):
            await update.effective_message.reply_text("Platform must be x or ig.")
            return
        account = parts[2].lstrip('@').lower()
        label = parts[3] if len(parts) == 4 else None
        current_count = count_saved_accounts(uid)
        if isinstance(badge['save_slots'], (int, float)) and current_count >= badge['save_slots']:
            await update.effective_message.reply_text(f"You reached saved limit ({badge['save_slots']}).")
            return
        try:
            saved = save_user_account(uid, platform, account, label)
            await update.effective_message.reply_text(f"Saved {platform} @{account} (id: {saved.get('id')})")
        except Exception as e:
            await update.effective_message.reply_text(f"Error saving: {e}")
        return

    if text.startswith("/saved_list"):
        items = list_saved_accounts(uid)
        if not items:
            await update.effective_message.reply_text("No saved accounts. Use /save or the Saved menu.")
            return
        text_out = "Your saved accounts:\n\n"
        rows = []
        for it in items:
            sid = it["id"]
            plat = it["platform"]
            acc = it["account_name"]
            lbl = it.get("label") or ""
            text_out += f"{sid}. [{plat}] @{acc} {('- '+lbl) if lbl else ''}\n"
            rows.append([
                InlineKeyboardButton(f"Send {sid}", callback_data=f"saved_sendcb_{sid}"),
                InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")
            ])
        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])
        await update.effective_message.reply_text(text_out, reply_markup=InlineKeyboardMarkup(rows))
        return

    # Default
    await record_user_and_check_ban(update, context)
    await update.effective_message.reply_text("Use the menu or /help for commands.")

# ================ COMMAND HANDLERS ================
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
    "🔥 <b>Welcome to MooreLinkBot – Your Ultimate Social Media Tracker!</b> 🔥\n\n"
    
    "Get the latest posts from X (Twitter) & Instagram instantly – no login needed! "
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
    text += f"Joined: {user.get('joined_at')}\nRequests: {user.get('request_count', 0)}\nInvites: {user.get('invite_count', 0)}\nBanned: {bool(user.get('is_banned'))}\n"
    text += f"Badge: {badge['emoji']} {badge['name']}\nSaves: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
    text += "Cooldowns:\n"
    text += f"Minute: {rl.get('minute_count',0)}/{badge['limits'].get('min','∞')}\n"
    text += f"Hour: {rl.get('hour_count',0)}/{badge['limits'].get('hour','∞')}\n"
    text += f"Day: {rl.get('day_count',0)}/{badge['limits'].get('day','∞')}\n"
    await update.effective_message.reply_text(text)

@admin_only
async def export_csv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    tid = update.effective_user.id

    args = context.args or []
    if len(args) >= 2:
        platform = args[0].lower()
        if platform in ("twitter",):
            platform = "x"
        account = args[1].lstrip('@').lower()

        await handle_fetch_and_ai(update, context, platform, account)
        return

    context.user_data["awaiting_username"] = True
    context.user_data["platform"] = "x"
    await update.effective_message.reply_text("Send username (without @) — default platform X. Use /cancel to abort.", reply_markup=build_back_markup("menu_main"))

# ================ REGISTER & RUN ================
if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("latest", latest_command))
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

    app.add_handler(CommandHandler("save", message_handler))
    app.add_handler(CommandHandler("saved_list", message_handler))
    app.add_handler(CommandHandler("saved_send", message_handler))
    app.add_handler(CommandHandler("saved_remove", message_handler))
    app.add_handler(CommandHandler("saved_rename", message_handler))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(CommandHandler("forcemode", testmode_command))

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

    app.post_init = set_command_visibility

    print("[startup] post_init registered")

    try:
        init_tg_db()
    except Exception as e:
        print(f"[startup] init_tg_db() failed or not available: {e}")

    print("🤖 MooreLinkBot (full) started — with Groq AI, only new posts, Diamond/Admin unlimited chat!")
    app.run_polling(drop_pending_updates=True)