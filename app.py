import os
import logging
import requests
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz

# ------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("MarketPulseBot")

# ------------------------------------------------------------
# Telegram Config
# ------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")          # Add in Render Environment
CHANNEL_ID = os.getenv("CHANNEL_ID")        # Example: "@mychannel" or chat_id
NEWS_API_KEY = os.getenv("NEWS_API_KEY")    # For news API (if required)

# ------------------------------------------------------------
# Flask App (needed by Render)
# ------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

# ------------------------------------------------------------
# Telegram Helper
# ------------------------------------------------------------
def send_message(text: str):
    """Send text message to Telegram channel."""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            log.error(f"Telegram error: {r.text}")
    except Exception as e:
        log.error(f"send_message failed: {e}")

# ------------------------------------------------------------
# News Fetcher (dummy or API-based)
# ------------------------------------------------------------
def fetch_news():
    """Fetch top headlines (replace with API logic)."""
    try:
        # Example placeholder (replace with API call)
        return [
            "Market opens higher led by IT and Banks.",
            "Gold prices steady ahead of US Fed meeting."
        ]
    except Exception as e:
        log.error(f"fetch_news failed: {e}")
        return []

# ------------------------------------------------------------
# Scheduled Jobs
# ------------------------------------------------------------
def job_post_news():
    log.info("Fetching news for scheduled post...")
    news_items = fetch_news()
    if not news_items:
        send_message("⚠️ No news available right now.")
        return
    for item in news_items[:2]:   # Post top 2
        send_message(f"📰 {item}")

def job_pre_market():
    send_message("📊 Pre-market update: Key levels & SGX Nifty trends.")

def job_ipo_update():
    send_message("💡 IPO Watch: Latest subscriptions & listings.")

def job_post_market():
    send_message("📌 Post-market summary: Index moves & top gainers/losers.")

def job_fii_dii():
    send_message("🏦 FII/DII trading activity update.")

# ------------------------------------------------------------
# Scheduler Setup
# ------------------------------------------------------------
sched = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))

def schedule_jobs():
    # Every 30 minutes between 08:30–21:30
    sched.add_job(job_post_news, "cron", minute="0,30", hour="8-21")

    # Specific slots
    sched.add_job(job_pre_market, "cron", hour=9, minute=0)
    sched.add_job(job_ipo_update, "cron", hour="10,11", minute=30)
    sched.add_job(job_post_market, "cron", hour=15, minute=45)
    sched.add_job(job_fii_dii, "cron", hour=21, minute=0)

def announce_start():
    send_message(
        "✅ MarketPulse bot restarted and schedule loaded.\n"
        "Window: 08:30–21:30 • Every 30 min (2 posts/slot)\n"
        "Weekdays: 09:00 Pre-market • 10:30/11:00 IPO • 15:45 Post-market • 21:00 FII/DII"
    )

def start_scheduler_once():
    """Ensure scheduler starts only once (even with Gunicorn workers)."""
    if not sched.running:
        schedule_jobs()
        sched.start()
        log.info("Scheduler started.")
        announce_start()

# ------------------------------------------------------------
# Start scheduler immediately on app load (Render way)
# ------------------------------------------------------------
start_scheduler_once()
