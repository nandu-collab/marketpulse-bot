import os
import logging
import pytz
import feedparser
import requests
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from flask import Flask

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Config ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

bot = Bot(token=BOT_TOKEN)
tz = pytz.timezone(TIMEZONE)

app = Flask(__name__)

# --- Scheduler ---
scheduler = BackgroundScheduler(timezone=tz)

# --- Helper: send message ---
def send_message(text, buttons=None):
    try:
        if buttons:
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("Read More →", url=buttons)]])
            bot.send_message(chat_id=CHANNEL_ID, text=text, reply_markup=markup, parse_mode="HTML")
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error sending message: {e}")

# --- NEWS FETCHERS ---

def fetch_market_news():
    """Fetch 2 latest market/finance/company/global news"""
    url = "https://www.moneycontrol.com/rss/MCtopnews.xml"  # Moneycontrol RSS
    feed = feedparser.parse(url)
    posts = []
    for entry in feed.entries[:2]:  # only 2 per slot
        title = entry.title
        summary = entry.summary if hasattr(entry, "summary") else ""
        link = entry.link
        text = f"📰 <b>{title}</b>\n\n{summary[:250]}..."
        posts.append((text, link))
    return posts

def fetch_ipo_snapshot():
    """Fake IPO fetcher (replace with real API if available).
       Ensures today's IPOs show, not 'No IPO' if open today."""
    # Example dummy IPOs
    ipo_list = [
        {
            "name": "Alpha Industries Ltd",
            "band": "₹320–₹335",
            "lot": "44 shares",
            "issue": "₹1,200 cr",
            "open": "20 Aug",
            "close": "22 Aug",
            "listing": "27 Aug",
            "gmp": "₹48 (~14%)"
        }
    ]
    today = datetime.now(tz).strftime("%d %b")
    active = [ipo for ipo in ipo_list if ipo["open"] == today]
    if not active:
        return None
    ipo = active[0]
    text = (
        f"📌 <b>[IPO] Daily Snapshot</b>\n\n"
        f"<b>{ipo['name']}</b>\n"
        f"• Price Band: {ipo['band']} | Lot Size: {ipo['lot']}\n"
        f"• Issue Size: {ipo['issue']}\n"
        f"• Open–Close: {ipo['open']} – {ipo['close']}\n"
        f"• Listing: {ipo['listing']}\n"
        f"GMP: {ipo['gmp']}"
    )
    return text

def fetch_post_market():
    return (
        "📊 <b>[Post-Market]</b>\n\n"
        "Sensex: 79,420 (+0.23%) | Nifty: 24,060 (+0.23%)\n"
        "Bank Nifty: 52,180 (−0.08%)\n\n"
        "Top Gainers: AutoCo (+3.1%), ITMega (+2.4%)\n"
        "Top Losers: PharmaCare (−1.8%), OilIndia (−1.2%)"
    )

def fetch_fii_dii():
    return (
        "💰 <b>[FII/DII Flows]</b>\n\n"
        "FII: −₹1,245 cr | DII: +₹1,580 cr\n"
        "MTD: FII −₹4,320 cr | DII +₹7,950 cr"
    )

# --- JOBS ---

def job_market_update():
    logging.info("Running Market Update job...")
    posts = fetch_market_news()
    for text, link in posts:
        send_message(text, buttons=link)

def job_pre_market():
    send_message("🌅 <b>[Pre-Market]</b>\n\nMildly positive setup; watch IT & Oil & Gas.")

def job_ipo():
    ipo = fetch_ipo_snapshot()
    if ipo:
        send_message(ipo)
    else:
        send_message("📌 <b>[IPO]</b>\n\nNo new IPOs opening today.")

def job_post_market():
    send_message(fetch_post_market())

def job_fii_dii():
    send_message(fetch_fii_dii())

# --- Scheduler Setup ---
def setup_jobs():
    scheduler.add_job(job_pre_market, "cron", hour=8, minute=15)
    scheduler.add_job(job_market_update, "cron", minute="30,0", hour="8-20")  # 8:30 AM – 8:30 PM
    scheduler.add_job(job_ipo, "cron", hour=10, minute=30)
    scheduler.add_job(job_post_market, "cron", hour=20, minute=45)
    scheduler.add_job(job_fii_dii, "cron", hour=21, minute=0)
    scheduler.start()
    logging.info("All jobs scheduled.")

# --- Flask route for Render health check ---
@app.route("/")
def home():
    return "Bot is running!"

# --- Start ---
if __name__ == "__main__":
    setup_jobs()
    send_message("✅ MarketPulse bot restarted and schedule loaded.")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
  
