from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import Optional

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