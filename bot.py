import os
from telegram import Update, Bot
from telegram.ext import Updater, CommandHandler, CallbackContext
from utils import fetch_posts

# ===== ENV VARIABLES =====
TELEGRAM_TOKEN = os.getenv("BOTTOKEN")  # Bot token from BotFather

# ===== COMMAND HANDLERS =====
def start(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Hello! I can fetch recent posts from your favorite accounts. "
        "Use /posts <account> to get latest posts."
    )


def posts(update: Update, context: CallbackContext):
    if len(context.args) == 0:
        update.message.reply_text("Please provide an account name. Example: /posts VDM")
        return

    account = context.args[0].strip()
    try:
        urls = fetch_posts(account)
        if not urls:
            update.message.reply_text(f"No new posts for {account} at the moment.")
            return

        # Send posts in a container-like format
        reply_text = f"Recent posts from {account}:\n"
        for i, url in enumerate(urls, 1):
            reply_text += f"{i}. {url}\n"
        update.message.reply_text(reply_text)

    except Exception as e:
        update.message.reply_text(f"Failed to fetch posts: {e}")


# ===== MAIN FUNCTION =====
def main():
    updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    # Commands
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("posts", posts))

    # Start bot
    print("Bot is running...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()