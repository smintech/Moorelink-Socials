import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from twikit import Client

TOKEN = os.getenv("BOT_TOKEN")

# Twikit client
client = Client('en-US')

# Login credentials - put in Render env variables for security!
USERNAME = os.getenv("TWITTER_USERNAME")   # e.g., yourusername
EMAIL = os.getenv("TWITTER_EMAIL")         # email for account
PASSWORD = os.getenv("TWITTER_PASSWORD")   # password

async def login_twikit():
    if os.path.exists('cookies.json'):
        client.load_cookies('cookies.json')
        print("Cookies loaded!")
    else:
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD,
            cookies_file='cookies.json',  # New argument for easy save
            enable_ui_metrics=True  # THIS ONE NA KEY FOR 2025 BLOCKS
        )
        print("Logged in fresh!")
# Run login once when bot start
loop = asyncio.get_event_loop()
loop.run_until_complete(login_twikit())

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
        user = await client.get_user_by_screen_name(username)
        if not user:
            await update.message.reply_text("Account no dey or private. Check username. 😕")
            return
        
        tweets = await user.get_tweets('Tweets', count=10)  # Or 'Latest'
        
        if not tweets:
            await update.message.reply_text("No recent posts. Try later. 😕")
            return
        
        await update.message.reply_text(f"Recent posts from @{username} ({len(tweets)} found):")
        
        for tweet in tweets:
            text = tweet.text or "(Media/post only)"
            date = tweet.created_at
            link = f"https://x.com/{username}/status/{tweet.id}"
            
            msg = f"📢 {text}\n\n🕒 {date}\n🔗 {link}"
            await update.message.reply_text(msg)
            
            if tweet.media:
                for media in tweet.media:
                    if media.get('type') == 'photo':
                        await update.message.reply_photo(media['url'])
                    elif media.get('type') in ['video', 'gif']:
                        video_url = media.get('video_url') or media.get('url')
                        if video_url:
                            await update.message.reply_video(video_url)
    
    except Exception as e:
        await update.message.reply_text(f"Wahala: {str(e)}. Maybe rate limit try again later! 😭")

# Webhook setup for Render
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