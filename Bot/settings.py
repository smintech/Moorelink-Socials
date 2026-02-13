import os
import logging
from typing import Optional
from functools import wraps
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Core configuration
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("BOTTOKEN env var not set")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# UI pagination
POSTS_PER_PAGE = 5
PAGE_SIZE_USERS = 10
LEADERBOARD_LIMIT = 10

# Global test mode (force-send posts even if seen)
TEST_MODE = {"enabled": False}

def is_admin(user_id: Optional[int]) -> bool:
    """Check if a user ID belongs to an admin."""
    return bool(user_id and user_id in ADMIN_IDS)

def admin_only(handler_func):
    """Decorator for async handlers — blocks non‑admins early and sends an error."""
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