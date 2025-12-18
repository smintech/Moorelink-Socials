import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from twikit import Client

TOKEN = os.getenv("BOTTOKEN")

# Create Twikit client (guest mode - no login for public timelines)
client = Client('en-US')

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hafa! Send me X username (e.g., @VDM__ or elonmusk) make I fetch recent posts sharp sharp! 🚀"
    )

async def fetch_timeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip('@')
    if not username:
        await update.message.reply_text("Bros, send username na! 😭")
        return
    
    await update.message.reply_text(f"Searching for recent posts from @{username}... ⏳")
    
    try:
        # Get user
        user = await client.get_user_by_screen_name(username)
        if not user:
            await update.message.reply_text("Account no dey or private. Check username well. 😕")
            return
        
        # Fetch recent tweets (up to 20 latest)
        tweets = await user.get_tweets('Latest', count=10)
        
        if not tweets:
            await update.message.reply_text("No recent posts found. Try later o. 😕")
            return
        
        await update.message.reply_text(f"Recent posts from @{username} ({len(tweets)} found):")
        
        for tweet in tweets:
            text = tweet.text or "(Media only)"
            date = tweet.created_at
            link = f"https://x.com/{username}/status/{tweet.id}"
            
            msg = f"📢 {text}\n\n🕒 {date}\n🔗 {link}"
            await update.message.reply_text(msg)
            
            # Send media if dey
            if tweet.media:
                for media in tweet.media:
                    if media.get('type') == 'photo':
                        await update.message.reply_photo(media['url'])
                    elif media.get('type') in ['video', 'gif']:
                        await update.message.reply_video(media['video_url'] or media['url'])
    
    except Exception as e:
        await update.message.reply_text(f"Wahala occur: {str(e)}. Try again later or different username. 😭")

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