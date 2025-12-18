import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from twscrape import API, gather

TOKEN = os.getenv("BOTTOKEN")

api = API()  # Uses default accounts.db

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hafa! Send me X username (e.g., @VDM__ or elonmusk) make I fetch recent posts sharp sharp! 🚀"
    )

async def fetch_timeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip('@')
    if not username:
        await update.message.reply_text("Bros, send username na! 😭")
        return
    
    await update.message.reply_text(f"Fetching recent posts from @{username}... ⏳")
    
    try:
        # Fetch user tweets (latest 10)
        tweets = await gather(
            api.user_tweets_by_username(username, limit=10)
        )
        
        if not tweets:
            await update.message.reply_text("No recent posts found or account private. 😕")
            return
        
        await update.message.reply_text(f"Recent posts from @{username} ({len(tweets)} found):")
        
        for tweet in tweets:
            text = tweet.rawContent or "(Media only)"
            date = tweet.date.strftime("%Y-%m-%d %H:%M")
            link = f"https://x.com/{username}/status/{tweet.id}"
            
            msg = f"📢 {text}\n\n🕒 {date}\n🔗 {link}"
            await update.message.reply_text(msg)
            
            # Media
            if tweet.media:
                for media in tweet.media:
                    if media.type == "photo":
                        await update.message.reply_photo(media.url)
                    elif media.type in ["video", "gif"]:
                        await update.message.reply_video(media.previewUrl or media.url)
    
    except Exception as e:
        await update.message.reply_text(f"Wahala: {str(e)}. Try again later! 😭")

# Webhook for Render
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    webhook_path = f"/{TOKEN}"
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}{webhook_path}"
    
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fetch_timeline))
    
    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=webhook_path,
        webhook_url=webhook_url
    )