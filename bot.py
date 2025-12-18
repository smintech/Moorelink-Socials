import os
import feedparser
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

TOKEN = os.getenv("BOTTOKEN")

# List of strong instances (fallback if one down)
INSTANCES = [
    "https://nitter.net",          # Official, 94% uptime
    "https://nitter.space",        # 96%
    "https://nuku.trabun.org",     # 95%
    "https://nitter.poast.org",    # 85%
    "https://lightbrd.com",        # 95%
    "https://nitter.privacyredirect.com"  # 94%
]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hafa! Send me X username (e.g., @VDM__ or elonmusk) make I fetch recent posts sharp sharp! 🚀"
    )

async def fetch_timeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().lstrip('@')
    if not username:
        await update.message.reply_text("Bros, send username na! 😭")
        return
    
    posts_found = False
    for instance in INSTANCES:
        rss_url = f"{instance}/{username}/rss"
        feed = feedparser.parse(rss_url, request_headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'})
        
        if feed.entries:
            posts_found = True
            await update.message.reply_text(f"Recent posts from @{username} (via {instance}):")
            
            for entry in feed.entries[:10]:  # Top 10 latest
                text = entry.title
                date = entry.published
                link = entry.link  # Clean Nitter link
                
                msg = f"📢 {text}\n\n🕒 {date}\n🔗 {link}"
                await update.message.reply_text(msg)
                
                # Send images if dey
                if 'media_content' in entry:
                    for media in entry.media_content:
                        if 'url' in media:
                            try:
                                if media['medium'] == 'image':
                                    await update.message.reply_photo(media['url'])
                                elif 'video' in media['type']:
                                    await update.message.reply_video(media['url'])
                            except:
                                pass  # Skip if no send
            break  # Stop if success
    
    if not posts_found:
        await update.message.reply_text("No posts found or account private. Try later or check username. 😕")

# Run bot
application = Application.builder().token(TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fetch_timeline))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render give PORT, default any
    webhook_path = f"/{TOKEN}"
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}{webhook_path}"
    
    # Set webhook
    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=webhook_path,
        webhook_url=webhook_url
    )
