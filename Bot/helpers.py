import urllib.request
from urllib.parse import urlparse
import io
import asyncio
import csv
import logging
from typing import Dict, Optional, Any, Tuple, Callable, List
from telegram import Update, Message, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import TelegramError
from .settings import TEST_MODE
from Utils.utils import *

logger = logging.getLogger(__name__)

def normalize_account(account: str, platform: str) -> str:
    acct = (account or "").strip()
    acct = acct.split('?')[0].rstrip('/')
    if acct.startswith('http'):
        parsed = urlparse(acct)
        path = parsed.path.strip('/')
        parts = [p for p in path.split('/') if p]
        if parts:
            if platform in ('ig', 'x', 'fb'):
                acct = parts[0]
            elif platform == 'yt':
                acct = parts[-1]
            else:
                acct = parts[0]
        else:
            acct = parsed.netloc
    acct = acct.lstrip("@").strip()
    return acct

def get_invite_link(bot_username: str, user_id: int) -> str:
    return f"https://t.me/{bot_username}?start={user_id}"

async def safe_edit(callback_query, text: str, parse_mode=None, reply_markup=None):
    """Safely edit text or caption. Falls back to new message if impossible."""
    try:
        msg = callback_query.message
        if msg.text:
            await callback_query.edit_message_text(
                text=text, parse_mode=parse_mode, reply_markup=reply_markup
            )
            return
        if msg.caption:
            await callback_query.edit_message_caption(
                caption=text, parse_mode=parse_mode, reply_markup=reply_markup
            )
            return
        await msg.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramError as e:
        logging.warning("safe_edit failed: %s", e.message)
        try:
            await callback_query.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception:
            await callback_query.answer("Action completed.", show_alert=True)

async def safe_send_media_or_link(
    chat: Any,
    context: ContextTypes.DEFAULT_TYPE,
    media_url: str,
    is_video: bool = False,
    caption: str = "",
    parse_mode: Optional[str] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None
) -> Optional[Message]:
    """Robustly send media by URL with fallbacks."""
    bot = context.bot
    reply_target = chat if isinstance(chat, Message) else None
    chat_id = chat.chat.id if isinstance(chat, Message) else getattr(getattr(chat, "chat", None), "id", None)
    if chat_id is None:
        logger.error("safe_send_media_or_link: cannot determine chat_id")
        return None

    media_url = (media_url or "").strip()
    caption = (caption or "").strip() or "Post preview"

    def is_valid_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
            return parsed.scheme in ("http", "https") and bool(parsed.netloc)
        except Exception:
            return False

    async def send(method: str, **kwargs):
        if reply_target:
            return await getattr(reply_target, f"reply_{method}")(**kwargs)
        else:
            return await getattr(bot, f"send_{method}")(chat_id=chat_id, **kwargs)

    try:
        if media_url and is_valid_url(media_url):
            if is_video:
                try:
                    return await send("video", video=media_url, caption=caption,
                                      parse_mode=parse_mode, reply_markup=reply_markup)
                except Exception as e:
                    logger.debug("send_video failed: %s → trying document", e)
            else:
                try:
                    return await send("photo", photo=media_url, caption=caption,
                                      parse_mode=parse_mode, reply_markup=reply_markup)
                except Exception as e:
                    logger.debug("send_photo failed: %s → trying document", e)

            return await send("document", document=media_url, caption=caption,
                              parse_mode=parse_mode, reply_markup=reply_markup)

    except Exception as e:
        logger.warning("Media send failed for %s: %s", media_url, e)

    try:
        fallback_text = f"{caption}\n\n🔗 <a href='{media_url}'>View original media</a>" if media_url and is_valid_url(media_url) else caption
        parse_mode_fallback = "HTML" if "<a href" in fallback_text else parse_mode
        if reply_target:
            return await reply_target.reply_text(
                text=fallback_text,
                parse_mode=parse_mode_fallback,
                disable_web_page_preview=False,
                reply_markup=reply_markup
            )
        else:
            return await bot.send_message(
                chat_id=chat_id,
                text=fallback_text,
                parse_mode=parse_mode_fallback,
                disable_web_page_preview=False,
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error("Final text fallback failed: %s", e)
        return None

async def download_media(url: str) -> bytes:
    """Download media using urllib (no external deps) – async compatible."""
    loop = asyncio.get_event_loop()
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
                "Referer": "https://www.facebook.com/"
            }
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status == 200:
                data = response.read()
                if len(data) > 50 * 1024 * 1024:
                    logging.warning(f"Media too large ({len(data)/1024/1024:.1f}MB): {url}")
                    return None
                return data
            else:
                logging.warning(f"Download failed {url} - status {response.status}")
                return None
    except Exception as e:
        logging.error(f"Download error {url}: {e}")
        return None

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