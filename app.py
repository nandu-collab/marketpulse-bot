import os
import requests
import pytz
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

# =====================
# FastAPI App
# =====================
app = FastAPI()

@app.get("/ping")
def ping():
    return {"status": "pong"}

# =====================
# Telegram Bot Config
# =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

def send_message(text: str):
    """Send text message to Telegram channel"""
    try:
        payload = {"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML"}
        requests.post(TELEGRAM_URL, data=payload)
    except Exception as e:
        print("Error sending message:", e)

# =====================
# Job Functions
# =====================
def post_market_news():
    # TODO: Replace with real scraping/news fetch logic
    send_message("📰 Market Update: Sample news goes here.\n\n<a href='https://www.moneycontrol.com/'>Read More</a>")

def post_pre_market():
    send_message("📊 Pre-Market Report: (Add data here)")

def post_post_market():
    send_message("📈 Post-Market Report: (Add data here)")

def post_fii_dii():
    send_message("💰 FII/DII Data: (Add data here)")

def post_ipo_details():
    send_message("🚀 IPO Update: (Add details here)")

# =====================
# Scheduler Setup
# =====================
scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))

# Regular news every 1 hour (2 posts per hour → schedule at :00 and :30)
scheduler.add_job(post_market_news, "cron", minute=0, hour="8-21")
scheduler.add_job(post_market_news, "cron", minute=30, hour="8-21")

# Pre/Post Market reports
scheduler.add_job(post_pre_market, "cron", hour=8, minute=30)
scheduler.add_job(post_post_market, "cron", hour=15, minute=45)

# FII/DII Data (evening)
scheduler.add_job(post_fii_dii, "cron", hour=20, minute=0)

# IPO details (late morning ~10:30 AM)
scheduler.add_job(post_ipo_details, "cron", hour=10, minute=30)

scheduler.start()

# =====================
# Run (for local dev only, NOT on Render)
# =====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
