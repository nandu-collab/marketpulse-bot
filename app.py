from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import logging

# Telegram + News imports here
# from telegram.ext import Updater
# from your_functions import fetch_and_post_news

app = Flask(__name__)

# Logging (important to debug)
logging.basicConfig(level=logging.INFO)

# Global scheduler
scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))

def start_jobs():
    # Avoid duplicate jobs
    if not scheduler.running:
        # Example job (you will add all your news jobs here)
        scheduler.add_job(fetch_and_post_news, "interval", minutes=30)
        scheduler.start()
        logging.info("✅ Scheduler started with all jobs")

@app.route("/")
def home():
    return "Bot is live"

# Run scheduler immediately when app starts (important for Render)
start_jobs()
