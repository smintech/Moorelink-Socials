import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from config import *
from persistence import *

# Import fetchers
from fetchers.x import *
from fetchers.ig import *
from fetchers.fb import *
from fetchers.yt import *
from fetchers.ai import *

logging.basicConfig(level=logging.INFO)

# ================ MAIN FETCH DISPATCHER ================
def fetch_latest_urls(platform: str, account: str) -> List[str]:
    account = account.lstrip('@')
    cached = persistence.get_recent_urls(platform, account)
    if cached:
        return cached
    if platform == "x":
        new = fetch_x_urls(account)
        for u in new:
            persistence.save_url("x", account, u)
        return new
    elif platform == "ig":
        new_ig = fetch_ig_urls(account)
        for p in new_ig:
            persistence.save_url("ig", account, p["url"])
        return [p["url"] for p in new_ig]
    elif platform == "fb":
        new_fb = fetch_fb_urls(account)
        for p in new_fb:
            persistence.save_url("fb", account, p["post_url"])
        return [p["post_url"] for p in new_fb]
    return []

# ================ BADGE AND COOLDOWN LOGIC ================
def get_user_badge(telegram_id: int) -> Dict[str, Any]:
    user = persistence.get_tg_user(telegram_id)

    if (user and int(user.get("is_admin", 0)) == 1) or (telegram_id in config.ADMIN_IDS):
        for b in config.BADGE_LEVELS:
            if b.get("name") == "Admin":
                return b
        return config.BADGE_LEVELS[-1]

    invites = user.get("invite_count", 0) if user else 0

    non_admin_levels = [lvl for lvl in config.BADGE_LEVELS if lvl.get("name") != "Admin"]
    for level in reversed(non_admin_levels):
        needed = level.get("invites_needed") or 0
        if invites >= needed:
            return level

    return config.BADGE_LEVELS[0]

def check_and_increment_cooldown(telegram_id: int) -> Optional[str]:
    user = persistence.get_tg_user(telegram_id)
    if user and int(user.get('is_banned', 0)) == 1:
        return "You are banned."
    badge = get_user_badge(telegram_id)
    if badge['name'] == 'Admin':
        persistence.increment_tg_request_count(telegram_id)
        return None

    limits = badge['limits']
    now = datetime.utcnow()
    rl = persistence.get_rate_limits(telegram_id)

    if rl.get('minute_reset') is None:
        rl['minute_reset'] = now + timedelta(minutes=1)
    if rl.get('hour_reset') is None:
        rl['hour_reset'] = now + timedelta(hours=1)
    if rl.get('day_reset') is None:
        rl['day_reset'] = now + timedelta(days=1)

    minute_reset = rl['minute_reset']
    hour_reset = rl['hour_reset']
    day_reset = rl['day_reset']

    if now >= minute_reset:
        rl['minute_count'] = 0
        rl['minute_reset'] = now + timedelta(minutes=1)
    if now >= hour_reset:
        rl['hour_count'] = 0
        rl['hour_reset'] = now + timedelta(hours=1)
    if now >= day_reset:
        rl['day_count'] = 0
        rl['day_reset'] = now + timedelta(days=1)

    if isinstance(limits.get('min'), (int, float)) and rl['minute_count'] >= limits['min']:
        seconds_left = int((rl['minute_reset'] - now).total_seconds())
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['min']} / minute\n⏱ Try again in {seconds_left} seconds\n\nInvite friends to unlock higher badges 🚀"
    if isinstance(limits.get('hour'), (int, float)) and rl['hour_count'] >= limits['hour']:
        minutes_left = int((rl['hour_reset'] - now).total_seconds() / 60)
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['hour']} / hour\n⏱ Try again in {minutes_left} minutes\n\nInvite friends to unlock higher badges 🚀"
    if isinstance(limits.get('day'), (int, float)) and rl['day_count'] >= limits['day']:
        hours_left = int((rl['day_reset'] - now).total_seconds() / 3600)
        return f"⏳ Slow down a bit\n\n🏅 Badge: {badge['emoji']} {badge['name']}\n📨 Limit: {limits['day']} / day\n⏱ Try again in {hours_left} hours\n\nInvite friends to unlock higher badges 🚀"

    if isinstance(limits.get('day'), (int, float)) and rl['day_count'] > limits['day'] * 2:
        rl['day_count'] = limits['day']
        rl['day_reset'] = now + timedelta(days=2)
        persistence.update_rate_limits(telegram_id, rl)
        return "🚫 Excessive usage detected. Cooldown extended."

    rl['minute_count'] = int(rl.get('minute_count', 0)) + 1
    rl['hour_count'] = int(rl.get('hour_count', 0)) + 1
    rl['day_count'] = int(rl.get('day_count', 0)) + 1

    persistence.update_rate_limits(telegram_id, rl)
    persistence.increment_tg_request_count(telegram_id)
    return None

# ================ ADMIN HELPERS ================
def get_user_stats(telegram_id: int) -> Dict[str, Any]:
    user = persistence.get_tg_user(telegram_id) or {}
    badge = get_user_badge(telegram_id)
    rl = persistence.get_rate_limits(telegram_id)
    saves = persistence.count_saved_accounts(telegram_id)
    return {
        'user': user,
        'badge': badge,
        'rate_limits': rl,
        'save_count': saves
    }
