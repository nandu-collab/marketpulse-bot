import os
import time
import threading
from datetime import datetime
import schedule
import telebot
from flask import Flask

# --- Telegram Setup ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
bot = telebot.TeleBot(BOT_TOKEN)

# --- Flask App for Render Keep-Alive ---
app = Flask(__name__)

@app.route('/')
def home():
    return "MarketPulse Bot is running!"

# --- News Posting Logic ---
def send_message(text: str):
    """Send message to Telegram channel"""
    try:
        bot.send_message(CHANNEL_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        print(f"[ERROR] Failed to send: {e}")

def job_premarket():
    send_message("📊 Pre-market update (placeholder content)")

def job_ipo():
    send_message("📝 IPO news/update (placeholder content)")

def job_postmarket():
    send_message("📉 Post-market summary (placeholder content)")

def job_fii_dii():
    send_message("💰 FII/DII flows update (placeholder content)")

def scheduler_loop():
    """Background scheduler loop"""
    # Weekday schedule
    schedule.every().monday.at("09:00").do(job_premarket)
    schedule.every().tuesday.at("09:00").do(job_premarket)
    schedule.every().wednesday.at("09:00").do(job_premarket)
    schedule.every().thursday.at("09:00").do(job_premarket)
    schedule.every().friday.at("09:00").do(job_premarket)

    # IPO news around mid-day
    schedule.every().monday.at("10:30").do(job_ipo)
    schedule.every().tuesday.at("10:30").do(job_ipo)
    schedule.every().wednesday.at("10:30").do(job_ipo)
    schedule.every().thursday.at("10:30").do(job_ipo)
    schedule.every().friday.at("10:30").do(job_ipo)

    # Post-market
    schedule.every().monday.at("15:45").do(job_postmarket)
    schedule.every().tuesday.at("15:45").do(job_postmarket)
    schedule.every().wednesday.at("15:45").do(job_postmarket)
    schedule.every().thursday.at("15:45").do(job_postmarket)
    schedule.every().friday.at("15:45").do(job_postmarket)

    # FII/DII flows after market
    schedule.every().monday.at("21:00").do(job_fii_dii)
    schedule.every().tuesday.at("21:00").do(job_fii_dii)
    schedule.every().wednesday.at("21:00").do(job_fii_dii)
    schedule.every().thursday.at("21:00").do(job_fii_dii)
    schedule.every().friday.at("21:00").do(job_fii_dii)

    send_message("✅ MarketPulse bot restarted and schedule loaded.\n"
                 "Window: 08:30–21:30 • Every 30 min (2 posts/slot)\n"
                 "Weekdays: 09:00 Pre-market • 10:30 IPO • 15:45 Post-market • 21:00 FII/DII")

    while True:
        schedule.run_pending()
        time.sleep(30)

# Run scheduler in background
t = threading.Thread(target=scheduler_loop, daemon=True)
t.start()
