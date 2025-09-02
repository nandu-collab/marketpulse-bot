from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import requests
import time
import os
import telegram
import feedparser
from bs4 import BeautifulSoup

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)

# ---------------- Telegram Bot Setup ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = telegram.Bot(token=BOT_TOKEN)

# ---------------- Flask App ----------------
app = Flask(__name__)

@app.route("/ping")
def ping():
    return "pong", 200

# ---------------- Your Task Function ----------------
def fetch_and_post():
    try:
        # Example dummy post (replace with your deal/news scraping logic)
        msg = f"Bot alive ✅ {time.strftime('%Y-%m-%d %H:%M:%S')}"
        bot.send_message(chat_id=CHANNEL_ID, text=msg)
        logging.info("Message posted successfully")
    except Exception as e:
        logging.error(f"Error posting: {e}")

# ---------------- Scheduler ----------------
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
scheduler.add_job(fetch_and_post, "interval", hours=1)
scheduler.start()

# ---------------- Run Flask ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
