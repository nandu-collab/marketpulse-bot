# app.py
import os
import re
import logging
import time
from datetime import datetime
import pytz
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
import redis

# local fetcher (you must add fetch_news.py as provided)
from fetch_news import (
    get_market_news,
    get_pre_market_brief,
    get_post_market_brief,
    get_fii_dii_data,
    get_ipo_updates,
)

# ----------------------------
# CONFIG
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")  # e.g. @yourchannel or -100123...
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")
REDIS_URL = os.getenv("REDIS_URL")  # optional, for durable dedup

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("market-bot")

# Validate token format quickly
_token_pattern = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{35,}$")
if not TELEGRAM_BOT_TOKEN or not _token_pattern.match(TELEGRAM_BOT_TOKEN.strip()):
    log.error("TELEGRAM_BOT_TOKEN is missing or clearly invalid. "
              "Make sure you set the exact token from @BotFather with no quotes or extra spaces.")
    raise SystemExit(1)

if not TARGET_CHANNEL_ID:
    log.error("TARGET_CHANNEL_ID is not set. Please set to @channelusername or numeric -100... ID.")
    raise SystemExit(1)

# Setup Bot
try:
    bot = Bot(token=TELEGRAM_BOT_TOKEN.strip())
    me = bot.get_me()  # quick verify
    log.info(f"Bot OK: @{me.username} ({me.id})")
except Exception as e:
    log.exception("Failed to initialize Bot. Check token and network.")
    raise SystemExit(1)

# timezone
IST = pytz.timezone(TIMEZONE)

# optional Redis for de-duplication
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        log.info("Connected to Redis for dedup.")
    except Exception as e:
        log.warning(f"Redis connect failed: {e}")
        redis_client = None

seen_ids = set()

def dedup_key(uid: str) -> bool:
    if not uid:
        return False
    if redis_client:
        key = f"seen:{uid}"
        if redis_client.get(key):
            return False
        redis_client.setex(key, 7 * 24 * 3600, "1")
        return True
    if uid in seen_ids:
        return False
    seen_ids.add(uid)
    # keep memory set small by trimming occasionally
    if len(seen_ids) > 5000:
        seen_ids.clear()
    return True

def send_text(chat_id, text, disable_preview=True):
    try:
        bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=disable_preview)
    except TelegramError as e:
        log.error("Telegram send error: %s", e)

# ----- Jobs -----
def job_hourly_news():
    now = datetime.now(IST)
    # Only between 08:30 and 21:30 IST
    start = now.replace(hour=8, minute=30, second=0, microsecond=0)
    end = now.replace(hour=21, minute=30, second=0, microsecond=0)
    if not (start <= now <= end):
        log.info("Outside posting window; skipping hourly job.")
        return

    items = get_market_news(max_items=30)
    posted = 0
    for it in items:
        uid = it.get("uid") or it.get("link") or it.get("headline")
        if not dedup_key(uid):
            continue
        title = it.get("headline") or it.get("title") or ""
        summary = it.get("summary") or ""
        link = it.get("link") or ""
        # Build message: headline + short summary + Read button as link in text
        text = f"<b>[Market Update]</b> {html_escape(title)}\n\n{html_escape(shorten(summary, 900))}\n\n<a href=\"{link}\">Read • {source_name_from_url(link)}</a>"
        send_text(TARGET_CHANNEL_ID, text, disable_preview=True)
        posted += 1
        if posted >= 2:
            break
    log.info("Hourly job posted %d items", posted)

def job_pre_market():
    txt = get_pre_market_brief()
    text = f"<b>Pre-Market Brief</b>\n\n{html_escape(txt)}"
    send_text(TARGET_CHANNEL_ID, text, disable_preview=True)
    log.info("Posted Pre-Market brief")

def job_post_market():
    txt = get_post_market_brief()
    text = f"<b>Post-Market Wrap</b>\n\n{html_escape(txt)}"
    send_text(TARGET_CHANNEL_ID, text, disable_preview=True)
    log.info("Posted Post-Market wrap")

def job_fii_dii():
    txt = get_fii_dii_data()
    text = f"<b>FII/DII Activity (Equity)</b>\n\n{html_escape(txt)}"
    send_text(TARGET_CHANNEL_ID, text, disable_preview=True)
    log.info("Posted FII/DII data")

def job_ipo():
    txt = get_ipo_updates()
    text = f"<b>IPO Desk</b>\n\n{html_escape(txt)}"
    send_text(TARGET_CHANNEL_ID, text, disable_preview=True)
    log.info("Posted IPO desk")

# small helpers local
def shorten(s, limit=400):
    if not s:
        return ""
    s = s.strip()
    return (s[:limit-1] + "…") if len(s) > limit else s

def html_escape(s):
    import html as _html
    return _html.escape(s or "")

def source_name_from_url(url: str) -> str:
    if not url:
        return "Source"
    if "moneycontrol" in url:
        return "Moneycontrol"
    if "economictimes" in url or "indiatimes" in url:
        return "Economictimes"
    if "livemint" in url or "mint" in url:
        return "Mint"
    return "Source"

# ----------------------------
# Scheduler & Flask (HTTP health)
# ----------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "OK"

def start_scheduler():
    sched = BackgroundScheduler(timezone=IST)
    # hourly: run at :00 and :30
    sched.add_job(job_hourly_news, CronTrigger(minute="0,30"))
    # pre/post market / FII/DII / IPO times
    sched.add_job(job_pre_market, CronTrigger(hour=9, minute=5))
    sched.add_job(job_post_market, CronTrigger(hour=16, minute=10))
    sched.add_job(job_fii_dii, CronTrigger(hour=19, minute=30))
    sched.add_job(job_ipo, CronTrigger(hour=10, minute=30))
    sched.start()
    log.info("Scheduler started")

if __name__ == "__main__":
    start_scheduler()
    port = int(os.getenv("PORT", "10000"))
    log.info("Starting Flask on port %s", port)
    app.run(host="0.0.0.0", port=port)
