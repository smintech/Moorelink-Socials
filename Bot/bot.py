from fastapi import FastAPI
import os
import logging
from datetime import datetime, timezone
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from telegram import BotCommand, BotCommandScopeDefault, BotCommandScopeChat

# Local imports
from settings import TELEGRAM_TOKEN, ADMIN_IDS, TEST_MODE
from commands import (
    start,
    menu,
    help_command,
    benefits_command,
    dashboard_command,
    leaderboard_command,
    admin_command,
    ban_command,
    unban_command,
    reset_cooldown_command,
    user_stats_command,
    export_csv_command,
    cancel_command,
    latest_command,
    testmode_command,
    reset_all_cooldowns_command,
    privacy_command,
    callback_handler,
    message_handler,
    set_command_visibility,
    init_tg_db,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
    }

if __name__ == "__main__":
    # Build the Telegram application
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("latest", latest_command))
    application.add_handler(CommandHandler("benefits", benefits_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("ban", ban_command))
    application.add_handler(CommandHandler("unban", unban_command))
    application.add_handler(CommandHandler("reset_cooldown", reset_cooldown_command))
    application.add_handler(CommandHandler("user_stats", user_stats_command))
    application.add_handler(CommandHandler("export_csv", export_csv_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("save", message_handler))
    application.add_handler(CommandHandler("saved_list", message_handler))
    application.add_handler(CommandHandler("saved_send", message_handler))
    application.add_handler(CommandHandler("saved_remove", message_handler))
    application.add_handler(CommandHandler("saved_rename", message_handler))
    application.add_handler(CommandHandler("privacy", privacy_command))
    application.add_handler(CommandHandler("forcemode", testmode_command))
    application.add_handler(CommandHandler("reset_all_cooldowns", reset_all_cooldowns_command))

    # Callback and message handlers
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # Set command visibility after initialization
    application.post_init = set_command_visibility

    # Initialize database
    try:
        init_tg_db()
    except Exception as e:
        print(f"[startup] init_tg_db() failed: {e}")

    print("🤖 MooreLinkBot (full) started — with Groq AI, only new posts, Diamond/Admin unlimited chat!")

    # Run webhook
    port = int(os.getenv("PORT", "8000"))
    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=TELEGRAM_TOKEN,
        webhook_url=f"https://moorelink-socials-435o.onrender.com/{TELEGRAM_TOKEN}",
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True
    )