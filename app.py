import os
import requests
from datetime import datetime
from telegram import Bot
from telegram.ext import Updater, CallbackContext
from apscheduler.schedulers.background import BackgroundScheduler

# BOT TOKEN and CHANNEL ID (set them as environment variables in Render)
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # Example: "@MarketPulse_India"

bot = Bot(token=BOT_TOKEN)

def send_message(message):
    bot.send_message(chat_id=CHANNEL_ID, text=message)

def post_morning_update():
    now = datetime.now().strftime("%d-%m-%Y %H:%M")
    send_message(f"📢 Good Morning!\nDate: {now}\n\nToday's IPO & Market updates coming soon!")

def post_evening_update():
    now = datetime.now().strftime("%d-%m-%Y %H:%M")
    send_message(f"📊 Evening Market Summary ({now})\nMajor News: Coming soon!")

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(post_morning_update, 'cron', hour=9, minute=0)
    scheduler.add_job(post_evening_update, 'cron', hour=18, minute=0)
    scheduler.start()

    # Keep alive
    while True:
        pass
      
