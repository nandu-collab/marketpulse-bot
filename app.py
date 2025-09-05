# app.py (fixed schedule version)
import os, json, re, logging, threading
from datetime import datetime
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify

# --- Telegram (PTB 13.x) ---
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# =============== CONFIG ===============
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN            = env("BOT_TOKEN")
CHANNEL_ID_RAW       = env("CHANNEL_ID")
TIMEZONE_NAME        = env("TIMEZONE", "Asia/Kolkata")

MAX_NEWS_PER_SLOT    = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS   = int(env("NEWS_SUMMARY_CHARS", "550"))

QUIET_HOURS_START    = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END      = env("QUIET_HOURS_END", "07:30")

ENABLE_IPO           = env("ENABLE_IPO", "1") == "1"
IPO_POST_TIME        = env("IPO_POST_TIME", "11:00")

ENABLE_MARKET_BLIPS  = env("ENABLE_MARKET_BLIPS", "1") == "1"
PREMARKET_TIME       = env("PREMARKET_TIME", "09:00")
POSTMARKET_TIME      = env("POSTMARKET_TIME", "16:00")

ENABLE_FII_DII       = env("ENABLE_FII_DII", "1") == "1"
FII_DII_POST_TIME    = env("FII_DII_POST_TIME", "21:00")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment!")

CHANNEL_ID = CHANNEL_ID_RAW
TZ = pytz.timezone(TIMEZONE_NAME)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("marketpulse")

bot = Bot(token=BOT_TOKEN)
app = Flask(__name__)

# ========== DEDUPE ==========
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=1200)

def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for u in data:
            seen_urls.add(u)
            seen_queue.append(u)
    except Exception:
        pass
load_seen()

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning(f"save_seen failed: {e}")

# ========== TIME UTILS ==========
def now_local():
    return datetime.now(TZ)

def parse_hhmm(s: str):
    return datetime.strptime(s, "%H:%M").time()

def is_weekday(dt=None):
    d = dt or now_local()
    return d.weekday() < 5

def within_window(start_str, end_str, dt=None):
    dt = dt or now_local()
    start = parse_hhmm(start_str)
    end = parse_hhmm(end_str)
    t = dt.time()
    if start <= end:
        return start <= t <= end
    return t >= start or t <= end

def in_quiet_hours(dt=None):
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END, dt)

# ========== FETCHERS ==========
# (kept exactly as in your pasted code)
# FEEDS, UA, clean_text, summarize, fetch_feed_entries,
# collect_news_batch, fetch_ongoing_ipos_for_today,
# fetch_fii_dii_cash, fetch_close_snapshot
# --- (not re-pasted here for brevity, use your same definitions) ---

# ========== SENDER ==========
def send_text(text: str, buttons: Optional[List[List[Dict]]] = None):
    markup = None
    if buttons:
        keyboard = [[InlineKeyboardButton(b["text"], url=b["url"]) for b in row] for row in buttons]
        markup = InlineKeyboardMarkup(keyboard)
    bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=markup
    )

# ========== JOBS ==========
def post_news_slot():
    now = now_local()
    if in_quiet_hours(now):
        return
    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    for it in items:
        if it["link"] in seen_urls:
            continue
        title = it["title"] or "Market update"
        summary = summarize(it["summary"], NEWS_SUMMARY_CHARS)
        text = f"<b>{title}</b>\n\n{summary}"
        send_text(text, buttons=[[{"text": "Read More", "url": it["link"]}]])
        seen_urls.add(it["link"])
        seen_queue.append(it["link"])
    save_seen()

def post_ipo_snapshot():
    if not ENABLE_IPO or not is_weekday():
        return
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        send_text("📌 <b>IPO</b>\nNo IPO details available today.")
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x['band']:
            seg += f" • {x['band']}"
        if x['lot']:
            seg += f" • {x['lot']}"
        lines.append(seg)
    send_text("\n".join(lines))

def post_market_close():
    if not ENABLE_MARKET_BLIPS or not is_weekday():
        return
    snap = fetch_close_snapshot()
    if not snap:
        send_text("📊 <b>Post-Market</b>\nSnapshot unavailable.")
        return
    def ln(sym, label):
        q = snap.get(sym)
        if not q: return None
        return f"{label}: {q['price']} ({q['change']:+} | {q['pct']:+}%)"
    lines = ["📊 <b>Post-Market — Closing Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        l = ln(sym, label)
        if l: lines.append(l)
    send_text("\n".join(lines))

def post_pre_market():
    if not ENABLE_MARKET_BLIPS or not is_weekday():
        return
    snap = fetch_close_snapshot()
    if not snap:
        send_text("📈 <b>Pre-Market</b>\nSnapshot unavailable.")
        return
    def ln(sym, label):
        q = snap.get(sym)
        if not q: return None
        return f"{label}: {q['price']} ({q['change']:+} | {q['pct']:+}%)"
    lines = ["📈 <b>Pre-Market — Opening Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        l = ln(sym, label)
        if l: lines.append(l)
    send_text("\n".join(lines))

def post_fii_dii():
    if not ENABLE_FII_DII or not is_weekday():
        return
    data = fetch_fii_dii_cash()
    if not data:
        send_text("🏦 <b>FII/DII</b>\nLatest activity not available yet.")
        return
    text = (
        "🏦 <b>FII/DII — Cash Market</b>\n"
        f"FII: {data['fii']:+,} cr\n"
        f"DII: {data['dii']:+,} cr\n"
        "<i>Note: Provisional numbers; subject to revision.</i>"
    )
    send_text(text)

# ========== SCHEDULER ==========
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler(
    timezone=TZ,
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
)

def schedule_jobs():
    # Hourly news at HH:30 between 08:30 and 21:30
    for hr in range(8, 22):
        scheduler.add_job(post_news_slot,
            trigger=CronTrigger(hour=hr, minute=30, timezone=TZ),
            id=f"news_{hr}_30", replace_existing=True)

    if ENABLE_IPO:
        hh, mm = IPO_POST_TIME.split(":")
        scheduler.add_job(post_ipo_snapshot, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_ipo_snapshot", replace_existing=True)

    if ENABLE_MARKET_BLIPS:
        hh, mm = PREMARKET_TIME.split(":")
        scheduler.add_job(post_pre_market, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_pre_market", replace_existing=True)
        hh, mm = POSTMARKET_TIME.split(":")
        scheduler.add_job(post_market_close, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_market_close", replace_existing=True)

    if ENABLE_FII_DII:
        hh, mm = FII_DII_POST_TIME.split(":")
        scheduler.add_job(post_fii_dii, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_fii_dii", replace_existing=True)

schedule_jobs()
scheduler.start()

# Startup announce
def announce_startup():
    msg = (
        "✅ <b>MarketPulse bot restarted with fixed schedule.</b>\n"
        "News: every hour at :30 from 08:30–21:30\n"
        f"IPO: {IPO_POST_TIME} • Pre-Market: {PREMARKET_TIME} • "
        f"Post-Market: {POSTMARKET_TIME} • FII/DII: {FII_DII_POST_TIME}\n"
        "<i>Mon–Fri only for market data.</i>"
    )
    try:
        send_text(msg)
    except Exception as ex:
        log.warning(f"startup announce failed: {ex}")

threading.Thread(target=announce_startup, daemon=True).start()

# ========== FLASK ==========
@app.route("/", methods=["GET", "HEAD"])
def root():
    jobs = []
    for j in scheduler.get_jobs():
        nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
        jobs.append({"id": j.id, "next_run": nxt})
    return jsonify({
        "ok": True,
        "tz": TIMEZONE_NAME,
        "now": now_local().strftime("%Y-%m-%d %H:%M:%S"),
        "jobs": jobs,
        "seen": len(seen_urls)
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
