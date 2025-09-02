# app.py
# Market Pulse India – Free Render Web Service bot
# Flask web + APScheduler + Telegram Bot (python-telegram-bot v13)
# Schedules:
#   - news loop every NEWS_INTERVAL minutes (2 posts per slot by default)
#   - optional quiet hours to pause news (other jobs still run)
#   - optional IPO snapshot, market blips, FII/DII
#   - keepalive: self-ping /ping every 9 minutes (prevents free-tier idle)
#
# Message format:
#   📰 [Tag]
#   Title
#
#   short summary
#   [Read more →] (button, web preview disabled)

import os, re, time, logging, textwrap, datetime as dt
import atexit
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# ========= CONFIG (env) =========
BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
CHANNEL_ID         = os.getenv("CHANNEL_ID", "")                # must be -100xxxxxxxxxxxx
TIMEZONE           = os.getenv("TIMEZONE", "Asia/Kolkata")

# — News —
ENABLE_NEWS        = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_INTERVAL      = int(os.getenv("NEWS_INTERVAL", "30"))      # minutes
MAX_NEWS_PER_SLOT  = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
MAX_NEWS_PER_DAY   = int(os.getenv("MAX_NEWS_PER_DAY", "50"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

# Optional quiet hours for NEWS only (set both to enable). Example: "22:30" and "07:30"
QUIET_HOURS_START  = os.getenv("QUIET_HOURS_START", "").strip()
QUIET_HOURS_END    = os.getenv("QUIET_HOURS_END", "").strip()

# — IPO / Blips / FII-DII (all optional) —
ENABLE_IPO         = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME      = os.getenv("IPO_POST_TIME", "09:15")        # HH:MM in TIMEZONE

ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1"))
MARKET_BLIPS_TIMES  = os.getenv("MARKET_BLIPS_TIMES", "08:15,16:15")  # first=pre, others=post

ENABLE_FII_DII     = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME  = os.getenv("FII_DII_POST_TIME", "20:45")    # HH:MM in TIMEZONE

# — Keepalive (free plan) —
KEEPALIVE_MINUTES  = int(os.getenv("KEEPALIVE_MINUTES", "9"))   # ping /ping every N minutes
EXTERNAL_URL       = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

# RSS sources (override via env RSS_SOURCES as CSV)
RSS_ENV = os.getenv("RSS_SOURCES", "").strip()
if RSS_ENV:
    RSS_SOURCES = [u.strip() for u in RSS_ENV.split(",") if u.strip()]
else:
    RSS_SOURCES = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",  # ET Markets
        "https://www.moneycontrol.com/rss/MCtopnews.xml",                        # Moneycontrol Top
        "https://www.livemint.com/rss/markets",                                  # Mint Markets
        "https://feeds.reuters.com/reuters/INTopNews",                           # Reuters India
    ]

# ========= Globals =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("marketpulse")

app  = Flask(__name__)
bot  = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
sched = BackgroundScheduler(timezone=TIMEZONE)

posted_urls_today: set[str] = set()
news_count_today: int      = 0
last_reset_date            = None


# ========= Helpers =========
def parse_hhmm(s: str) -> tuple[int, int]:
    try:
        h, m = s.split(":")
        return int(h), int(m)
    except Exception:
        return 0, 0

def now_local() -> dt.datetime:
    return dt.datetime.now()

def reset_counters_if_needed():
    global posted_urls_today, news_count_today, last_reset_date
    today = now_local().date()
    if last_reset_date != today:
        posted_urls_today = set()
        news_count_today  = 0
        last_reset_date   = today
        log.info("Daily counters reset.")

def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")

def trim(s: str, n: int) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return (s[:n] + "…") if len(s) > n else s

def in_quiet_hours_now() -> bool:
    """NEWS posts paused in quiet hours. Other scheduled posts still run."""
    if not QUIET_HOURS_START or not QUIET_HOURS_END:
        return False
    try:
        now = now_local().time()
        sh, sm = parse_hhmm(QUIET_HOURS_START); eh, em = parse_hhmm(QUIET_HOURS_END)
        start = dt.time(sh, sm); end = dt.time(eh, em)
        if start < end:  # same day
            return start <= now < end
        else:            # crosses midnight
            return now >= start or now < end
    except Exception:
        return False

def tg_send(text: str, button_url: str | None = None, button_text: str = "Read more →"):
    if not bot or not CHANNEL_ID:
        log.error("Missing BOT_TOKEN or CHANNEL_ID.")
        return
    try:
        if button_url:
            buttons = [[InlineKeyboardButton(button_text, url=button_url)]]
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,  # <- prevents the big black/blue preview card
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        else:
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

def categorize(title: str, summary: str) -> str:
    t = (title + " " + summary).lower()
    if any(k in t for k in ["tariff", "us yields", "fomc", "fed", "china", "opec", "brent", "dollar index", "geopolitics"]):
        return "[Global Impact]"
    if any(k in t for k in ["ipo", "price band", "gmp", "subscription", "listing"]):
        return "[IPO]"
    if any(k in t for k in ["q1", "q2", "q3", "q4", "results", "merger", "acquisition", "stake", "rights issue", "bonus issue"]):
        return "[Company]"
    if any(k in t for k in ["rbi", "gdp", "inflation", "cpi", "wpi", "gst", "fiscal", "budget"]):
        return "[Finance]"
    return "[Market Update]"


# ========= News job =========
def fetch_and_post_news():
    """Every slot: collect from RSS, de-dup, post up to MAX_NEWS_PER_SLOT."""
    global news_count_today
    reset_counters_if_needed()

    if not ENABLE_NEWS:
        return
    if in_quiet_hours_now():
        log.info("Quiet hours active – skipping news slot.")
        return
    if news_count_today >= MAX_NEWS_PER_DAY:
        log.info("Daily news limit reached – skipping slot.")
        return

    collected: list[tuple[str, str, str]] = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:15]:
                link = (getattr(e, "link", "") or "").strip()
                title = (getattr(e, "title", "") or "").strip()
                summary = strip_html(getattr(e, "summary", "") or getattr(e, "description", ""))
                if not link or not title:
                    continue
                if link in posted_urls_today:
                    continue
                collected.append((link, title, summary))
        except Exception as ex:
            log.warning(f"RSS fetch fail {url}: {ex}")

    if not collected:
        log.info("No fresh news this slot.")
        return

    # de-dupe by link/title
    uniq, seen = [], set()
    for link, title, summary in collected:
        key = link or title
        if key not in seen:
            uniq.append((link, title, summary))
            seen.add(key)

    # how many to post this slot
    slots_left_today = MAX_NEWS_PER_DAY - news_count_today
    to_post = min(MAX_NEWS_PER_SLOT, slots_left_today, len(uniq))

    for link, title, summary in uniq[:to_post]:
        tag = categorize(title, summary)
        body = f"📰 <b>{tag}</b>\n<b>{title}</b>\n\n{trim(summary, NEWS_SUMMARY_CHARS)}"
        tg_send(body, button_url=link, button_text="Read more →")
        posted_urls_today.add(link)
        news_count_today += 1
        time.sleep(2)  # tiny gap to avoid rate limits

    log.info(f"Posted {to_post} item(s) this slot. Total today: {news_count_today}/{MAX_NEWS_PER_DAY}")


# ========= IPO (best-effort from public feeds) =========
def ipo_snapshot():
    if not ENABLE_IPO:
        return
    items = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:20]:
                title = (getattr(e, "title", "") or "")
                summary = strip_html(getattr(e, "summary", "") or "")
                if re.search(r"\bipo\b|\bprice band\b|\bgmp\b|\bsubscription\b|\blisting\b", title + " " + summary, re.I):
                    items.append((title.strip(), summary.strip()))
        except Exception:
            pass

    if not items:
        tg_send("📌 <b>[IPO] Daily Snapshot</b>\nNo clear IPO details available today.")
        return

    for title, summary in items[:6]:
        msg = textwrap.dedent(f"""
        📌 <b>[IPO] Daily Snapshot</b>
        <b>{title}</b>

        {trim(summary, 900)}
        """).strip()
        tg_send(msg)
        time.sleep(1)


# ========= Market blips (Pre & Post) =========
def post_market_blip(label: str):
    if label == "pre":
        msg = textwrap.dedent("""
        📈 <b>[Pre-Market]</b>
        Mixed global cues. Watch IT, Oil & Gas, and PSU banks at the open.
        """).strip()
    else:
        msg = textwrap.dedent("""
        📉 <b>[Post-Market]</b>
        Indices wrap with gainers/losers; follow-through likely in next session.
        """).strip()
    tg_send(msg)


# ========= FII / DII headline sweep =========
def fii_dii_update():
    if not ENABLE_FII_DII:
        return
    hints = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:12]:
                t = (getattr(e, "title", "") + " " + strip_html(getattr(e, "summary", ""))).lower()
                if "fii" in t or "dii" in t or "domestic investors" in t:
                    hints.append(getattr(e, "title", "").strip())
        except Exception:
            pass
    if hints:
        msg = "🏦 <b>[FII/DII]</b>\n" + "\n".join(f"• {h}" for h in hints[:6])
    else:
        msg = "🏦 <b>[FII/DII]</b>\nFlows headline not clearly available today."
    tg_send(msg)


# ========= Keepalive (prevents Render free idling) =========
def keepalive_ping():
    if EXTERNAL_URL:
        url = f"{EXTERNAL_URL}/ping"
        try:
            r = requests.get(url, timeout=10)
            log.info(f"Keepalive {url} -> {r.status_code}")
        except Exception as e:
            log.warning(f"Keepalive failed: {e}")


# ========= Schedule all jobs =========
def schedule_all():
    # repeating news loop
    if ENABLE_NEWS and NEWS_INTERVAL > 0:
        sched.add_job(fetch_and_post_news, "interval",
                      minutes=NEWS_INTERVAL, id="news_loop", replace_existing=True)

    # IPO daily
    if ENABLE_IPO:
        hh, mm = parse_hhmm(IPO_POST_TIME)
        sched.add_job(ipo_snapshot, CronTrigger(hour=hh, minute=mm),
                      id="ipo", replace_existing=True)

    # Market blips
    if ENABLE_MARKET_BLIPS:
        times = [t.strip() for t in MARKET_BLIPS_TIMES.split(",") if t.strip()]
        for i, t in enumerate(times):
            hh, mm = parse_hhmm(t)
            label = "pre" if i == 0 else "post"
            sched.add_job(post_market_blip, CronTrigger(hour=hh, minute=mm),
                          args=[label], id=f"blip_{i}", replace_existing=True)

    # FII/DII
    if ENABLE_FII_DII:
        hh, mm = parse_hhmm(FII_DII_POST_TIME)
        sched.add_job(fii_dii_update, CronTrigger(hour=hh, minute=mm),
                      id="fiidii", replace_existing=True)

    # Reset counters daily
    sched.add_job(reset_counters_if_needed, CronTrigger(hour=0, minute=5),
                  id="reset", replace_existing=True)

    # Keepalive
    if KEEPALIVE_MINUTES > 0:
        sched.add_job(keepalive_ping, "interval", minutes=KEEPALIVE_MINUTES,
                      id="keepalive", replace_existing=True)

    sched.start()
    log.info("✅ Scheduler started.")
    for j in sched.get_jobs():
        log.info(f"JOB: {j}")


# ========= Flask routes (health) =========
@app.route("/")
def home():
    return "MarketPulse bot running ✅", 200

@app.route("/ping")
def ping():
    return jsonify(ok=True, ts=now_local().isoformat()), 200


# Start scheduler immediately on import (Gunicorn --workers=1)
schedule_all()

# Graceful shutdown
atexit.register(lambda: (sched.shutdown(wait=False) if sched.running else None))
        
