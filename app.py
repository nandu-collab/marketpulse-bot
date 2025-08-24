import os
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests
import feedparser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask
from telegram import Bot
from telegram.error import TelegramError

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ------------------- Env & Timezone -------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Kolkata"))

ENABLE_NEWS = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_INTERVAL = int(os.getenv("NEWS_INTERVAL", "30"))
MAX_NEWS_PER_SLOT = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))
QUIET_HOURS_START = os.getenv("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = os.getenv("QUIET_HOURS_END", "07:30")

ENABLE_IPO = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME = os.getenv("IPO_POST_TIME", "10:30")

ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1"))
MARKET_BLIPS_START = os.getenv("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END = os.getenv("MARKET_BLIPS_END", "20:30")

POSTMARKET_TIME = os.getenv("POSTMARKET_TIME", "20:45")

ENABLE_FII_DII = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME = os.getenv("FII_DII_POST_TIME", "21:00")

bot = Bot(token=BOT_TOKEN)

# ------------------- Helpers -------------------
def now():
    return datetime.now(TZ)

def in_quiet_hours():
    h, m = map(int, QUIET_HOURS_START.split(":"))
    quiet_start = time(h, m)
    h, m = map(int, QUIET_HOURS_END.split(":"))
    quiet_end = time(h, m)

    current = now().time()
    if quiet_start < quiet_end:
        return quiet_start <= current < quiet_end
    else:
        return current >= quiet_start or current < quiet_end

def send_message(text):
    if in_quiet_hours():
        logging.info("⏸ Quiet hours active, skipping post")
        return
    try:
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="HTML", disable_web_page_preview=True)
        logging.info("✅ Posted: %s", text[:60])
    except TelegramError as e:
        logging.error("Telegram error: %s", e)

def weekday_only():
    return now().weekday() < 5  # 0=Mon ... 4=Fri

# ------------------- News -------------------
NEWS_FEEDS = [
    "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "https://www.moneycontrol.com/rss/buzzingstocks.xml"
]

def fetch_news():
    news = []
    for url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:MAX_NEWS_PER_SLOT]:
                title = entry.title
                link = entry.link
                summary = getattr(entry, "summary", "")
                if len(summary) > NEWS_SUMMARY_CHARS:
                    summary = summary[:NEWS_SUMMARY_CHARS] + "..."
                news.append(f"<b>{title}</b>\n{summary}\n{link}")
        except Exception as e:
            logging.error("Error fetching news from %s: %s", url, e)
    return news

def post_news():
    if not ENABLE_NEWS:
        return
    logging.info("📢 Fetching news...")
    for item in fetch_news()[:MAX_NEWS_PER_SLOT]:
        send_message(item)

# ------------------- IPO -------------------
def post_ipo():
    if not ENABLE_IPO or not weekday_only():
        return
    # placeholder IPO details (replace with NSE/BSE API later)
    send_message("📊 <b>Upcoming IPO</b>\nCompany: Example Ltd\nOpens: 25 Aug\nCloses: 28 Aug\nPrice Band: ₹250–270\nLot: 55 shares")

# ------------------- Market Blips -------------------
def post_market_blip():
    if not ENABLE_MARKET_BLIPS or not weekday_only():
        return
    send_message("⚡ Market Blip: Sensex +120 | Nifty +35 | BankNifty +90")

# ------------------- Post-market -------------------
def post_postmarket():
    if not weekday_only():
        return
    send_message("📌 Post-market Summary\nSensex closed +110 pts, Nifty +30 pts.")

# ------------------- FII/DII -------------------
def post_fii_dii():
    if not ENABLE_FII_DII or not weekday_only():
        return
    send_message("💰 FII/DII Data\nFII: +₹1200 Cr\nDII: -₹800 Cr")

# ------------------- Scheduler -------------------
scheduler = BackgroundScheduler(timezone=TZ)

if ENABLE_NEWS:
    scheduler.add_job(post_news, "interval", minutes=NEWS_INTERVAL, id="news")

if ENABLE_IPO:
    h, m = map(int, IPO_POST_TIME.split(":"))
    scheduler.add_job(post_ipo, CronTrigger(hour=h, minute=m, timezone=TZ), id="ipo")

if ENABLE_MARKET_BLIPS:
    start_h, start_m = map(int, MARKET_BLIPS_START.split(":"))
    end_h, end_m = map(int, MARKET_BLIPS_END.split(":"))
    scheduler.add_job(post_market_blip, "cron", minute="*/60", hour=f"{start_h}-{end_h}", id="blips")

h, m = map(int, POSTMARKET_TIME.split(":"))
scheduler.add_job(post_postmarket, CronTrigger(hour=h, minute=m, timezone=TZ), id="postmarket")

h, m = map(int, FII_DII_POST_TIME.split(":"))
scheduler.add_job(post_fii_dii, CronTrigger(hour=h, minute=m, timezone=TZ), id="fii_dii")

scheduler.start()

# ------------------- Flask -------------------
app = Flask(__name__)

@app.route("/")
def index():
    return f"✅ MarketPulse bot running. Time now: {now()}"

# ------------------- Startup -------------------
if __name__ == "__main__":
    logging.info("✅ MarketPulse bot restarted and schedule loaded.")
    logging.info(f"Window: {MARKET_BLIPS_START}–{MARKET_BLIPS_END} • Every {NEWS_INTERVAL} min • Max {MAX_NEWS_PER_SLOT}/slot Quiet: {QUIET_HOURS_START}–{QUIET_HOURS_END}")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
