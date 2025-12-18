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
        # Get user by username
        user = await api.user_by_login(username)
        if not user:
            await update.message.reply_text("Account no dey or private/blocked. Check username well. 😕")
            return
        
        # Fetch recent tweets (up to 10)
        tweets = await gather(api.user_tweets(user.id, limit=10))
        
        if not tweets:
            await update.message.reply_text("No recent posts found. Try later o. 😕")
            return
        
        await update.message.reply_text(f"Recent posts from @{username} ({len(tweets)} found):")
        
        for tweet in tweets:
            text = tweet.rawContent or "(Media or quote post)"
            date = tweet.date.strftime("%b %d, %Y · %I:%M %p")
            link = f"https://x.com/{username}/status/{tweet.id}"
            
            msg = f"📢 {text}\n\n🕒 {date}\n🔗 {link}"
            await update.message.reply_text(msg, disable_web_page_preview=True)
            
            # Send media if dey
            if tweet.media:
                for media in tweet.media:
                    if media.type == "photo":
                        await update.message.reply_photo(media.url)
                    elif media.type in ["video", "gif"]:
                        # Best video URL
                        video_url = max(media.videoVariants, key=lambda v: v.bitrate).url if media.videoVariants else media.url
                        await update.message.reply_video(video_url)
    
    except Exception as e:
        await update.message.reply_text(f"Last wahala: {str(e)}. Try again later! 😭")

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