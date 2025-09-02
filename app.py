import os
import logging
import requests
import feedparser
from bs4 import BeautifulSoup
from pytz import timezone
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = Bot(token=BOT_TOKEN)

# Flask app (for Render web service health check)
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!", 200

# Function to clean text
def clean_text(text):
    return " ".join(text.split())

# Example function: fetch deals/news (you can add more feeds here)
def fetch_news():
    feeds = [
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    ]
    news_items = []
    for url in feeds:
        try:
            d = feedparser.parse(url)
            for entry in d.entries[:5]:
                title = clean_text(entry.title)
                link = entry.link
                news_items.append(f"📰 {title}\n{link}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
    return news_items

# Posting function (2 news each run)
def fetch_and_post_news():
    logger.info("Fetching news to post...")
    news_items = fetch_news()
    if not news_items:
        return
    # Pick 2 items
    for item in news_items[:2]:
        try:
            bot.send_message(chat_id=CHANNEL_ID, text=item)
        except Exception as e:
            logger.error(f"Error posting to Telegram: {e}")

# Scheduler
scheduler = BackgroundScheduler(timezone=timezone("Asia/Kolkata"))
scheduler.add_job(fetch_and_post_news, "cron", minute="0,30")  # every 30 minutes
scheduler.start()

# Alive message at startup
try:
    bot.send_message(chat_id=CHANNEL_ID, text="✅ Bot started and scheduler running!")
except Exception as e:
    logger.error(f"Error sending alive message: {e}")

# Run Flask (Render entry point)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
