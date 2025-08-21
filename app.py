import os, time, re, textwrap, logging, datetime as dt
from flask import Flask
import requests, feedparser
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot, ParseMode
from telegram.error import RetryAfter, TimedOut, BadRequest
import pytz

# ----------------- Config (env + defaults) -----------------
TZ_NAME            = os.getenv("TIMEZONE", "Asia/Kolkata")
TZ                 = pytz.timezone(TZ_NAME)

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
CHANNEL_ID         = os.getenv("CHANNEL_ID", "")   # MUST be like -100xxxxxxxxxxxx

ENABLE_NEWS        = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_INTERVAL      = int(os.getenv("NEWS_INTERVAL", "30"))         # minutes
MAX_NEWS_PER_DAY   = int(os.getenv("MAX_NEWS_PER_DAY", "50"))
MAX_NEWS_PER_SLOT  = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

ENABLE_IPO         = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME      = os.getenv("IPO_POST_TIME", "11:30")            # HH:MM, IST

ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1"))
# intraday blips (pre, mid, close). Use HH:MM,IST comma separated
MARKET_BLIPS_TIMES  = os.getenv("MARKET_BLIPS_TIMES", "09:15,12:30,15:30")

ENABLE_FII_DII      = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME   = os.getenv("FII_DII_POST_TIME", "16:45")       # after close

QUIET_HOURS_START   = os.getenv("QUIET_HOURS_START", "22:30")       # HH:MM
QUIET_HOURS_END     = os.getenv("QUIET_HOURS_END", "07:30")         # HH:MM

# RSS sources (comma separated env overrides)
RSS_ENV = os.getenv("RSS_SOURCES", "").strip()
if RSS_ENV:
    RSS_SOURCES = [u.strip() for u in RSS_ENV.split(",") if u.strip()]
else:
    # Good, stable public feeds. If one fails, others still work.
    RSS_SOURCES = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", # ET Markets
        "https://www.moneycontrol.com/rss/MCtopnews.xml",                        # Moneycontrol top news
        "https://www.livemint.com/rss/markets",                                  # LiveMint markets (works on most days)
    ]

# ----------------- Globals -----------------
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None

posted_urls_today = set()
news_count_today  = 0
last_reset_date   = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ----------------- Utils -----------------
def now_ist():
    return dt.datetime.now(TZ)

def parse_hhmm(s):
    hh, mm = s.split(":")
    return int(hh), int(mm)

def in_quiet_hours():
    """
    True if we are within quiet hours (no NEWS posting).
    IPO/FII/Blips still go at exact scheduled times.
    """
    start_h, start_m = parse_hhmm(QUIET_HOURS_START)
    end_h, end_m     = parse_hhmm(QUIET_HOURS_END)
    t = now_ist().time()
    start = dt.time(start_h, start_m)
    end   = dt.time(end_h, end_m)
    if start < end:  # same-day window
        return start <= t < end
    else:            # crosses midnight
        return t >= start or t < end

def safe_send(text):
    if not bot or not CHANNEL_ID:
        logging.error("Bot token or CHANNEL_ID missing.")
        return
    try:
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except RetryAfter as e:
        time.sleep(int(e.retry_after) + 1)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (TimedOut, BadRequest) as e:
        logging.error(f"Telegram send failed: {e}")

def reset_counters_if_needed():
    global posted_urls_today, news_count_today, last_reset_date
    today = now_ist().date()
    if last_reset_date != today:
        posted_urls_today = set()
        news_count_today  = 0
        last_reset_date   = today
        logging.info("Daily counters reset.")

def trim(txt, n):
    txt = re.sub(r"\s+", " ", txt or "").strip()
    return (txt[:n] + "…") if len(txt) > n else txt

# ----------------- NEWS -----------------
def fetch_market_news():
    """Pull from multiple RSS feeds, dedupe by link, cap per slot and per day."""
    global news_count_today
    reset_counters_if_needed()

    if ENABLE_NEWS and in_quiet_hours():
        logging.info("Quiet hours: skipping regular news slot.")
        return

    if news_count_today >= MAX_NEWS_PER_DAY:
        logging.info("Daily news cap hit; skipping slot.")
        return

    collected = []
    for url in RSS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:10]:
                link = getattr(e, "link", "")
                if link and link not in posted_urls_today:
                    title = getattr(e, "title", "").strip()
                    desc  = trim(getattr(e, "summary", ""), NEWS_SUMMARY_CHARS)
                    collected.append((link, title, desc))
        except Exception as e:
            logging.warning(f"RSS fetch fail {url}: {e}")

    # sort by time if available; otherwise keep order
    # de-dupe by link and title
    uniq = []
    seen = set()
    for link, title, desc in collected:
        key = (link or title)
        if key not in seen:
            uniq.append((link, title, desc))
            seen.add(key)

    if not uniq:
        logging.info("No fresh news found this slot.")
        return

    to_post = min(MAX_NEWS_PER_SLOT, MAX_NEWS_PER_DAY - news_count_today, len(uniq))
    for link, title, desc in uniq[:to_post]:
        msg = f"📰 <b>{title}</b>\n\n{desc}\n\n🔗 Read more: {link}"
        safe_send(msg)
        posted_urls_today.add(link)
        news_count_today += 1
        time.sleep(2)  # small gap

    logging.info(f"Posted {to_post} news items this slot.")

# ----------------- IPO (best-effort from public articles) -----------------
IPO_KEYWORDS = re.compile(r"\bIPO\b|\bprice band\b|\blot\b|\bissue size\b|\bGMP\b|\bsubscription\b", re.I)

def extract_ipo_items():
    """Collect IPO-related items from the same RSS sources and try to surface details if present."""
    items = []
    for url in RSS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:20]:
                title = getattr(e, "title", "")
                summary = getattr(e, "summary", "")
                if IPO_KEYWORDS.search(title) or IPO_KEYWORDS.search(summary):
                    items.append({
                        "title": title.strip(),
                        "summary": trim(summary, 900),
                        "link": getattr(e, "link", "")
                    })
        except Exception as e:
            logging.warning(f"IPO RSS scan fail {url}: {e}")
    return items[:6]  # keep it tidy

def post_daily_ipo_digest():
    if not ENABLE_IPO:
        return
    items = extract_ipo_items()
    if not items:
        safe_send("📌 <b>IPO Watch</b>\nNo IPO details available today. Will try again tomorrow.")
        return

    # Compose one message per IPO for visibility (your preference)
    for it in items:
        body = textwrap.dedent(f"""
        💠 <b>IPO Watch</b>
        <b>{it['title']}</b>

        {it['summary']}

        🔗 Details: {it['link']}
        """).strip()
        safe_send(body)
        time.sleep(1)

# ----------------- MARKET BLIPS (pre/mid/close snapshots - textual) -----------------
def get_simple_blip():
    """
    Best-effort ‘status’ without paid APIs.
    We stitch from headlines so bot stays reliable. You can later plug in an API.
    """
    # take a couple of market headlines for flavor
    headlines = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:5]:
                t = getattr(e, "title", "")
                if any(k in t.lower() for k in ["nifty", "sensex", "bank nifty", "market", "stocks"]):
                    headlines.append(t.strip())
        except Exception:
            pass
    brief = " • ".join(headlines[:3]) if headlines else "Market moving… updates shortly."
    return f"📈 <b>Market Check</b>\n{brief}"

def post_market_blip():
    if not ENABLE_MARKET_BLIPS:
        return
    safe_send(get_simple_blip())

# ----------------- FII / DII -----------------
def post_fii_dii():
    if not ENABLE_FII_DII:
        return
    # We do a best-effort from articles; avoids scraping NSE directly.
    points = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:12]:
                t = (getattr(e, "title", "") + " " + getattr(e, "summary", "")).lower()
                if "fii" in t or "dii" in t or "domestic investors" in t:
                    points.append(getattr(e, "title", "").strip())
        except Exception:
            pass
    if points:
        msg = "🏦 <b>FII / DII Flow</b>\n" + "\n".join(f"• {p}" for p in points[:4])
    else:
        msg = "🏦 <b>FII / DII Flow</b>\nData not clearly available in public feeds today. Will try again tomorrow."
    safe_send(msg)

# ----------------- Scheduler & Web -----------------
app = Flask(__name__)
sched = BackgroundScheduler(timezone=TZ_NAME)

@app.route("/")
def home():
    return "MarketPulse bot running ✅"

def schedule_all():
    # repeating news (respect quiet hours)
    if ENABLE_NEWS:
        sched.add_job(fetch_market_news, "interval", minutes=NEWS_INTERVAL, id="news_loop", replace_existing=True)

    # IPO daily (one time)
    if ENABLE_IPO:
        hh, mm = parse_hhmm(IPO_POST_TIME)
        sched.add_job(post_daily_ipo_digest, "cron", hour=hh, minute=mm, id="ipo", replace_existing=True)

    # market blips (set of times)
    if ENABLE_MARKET_BLIPS:
        for idx, t in enumerate([s.strip() for s in MARKET_BLIPS_TIMES.split(",") if s.strip()]):
            hh, mm = parse_hhmm(t)
            sched.add_job(post_market_blip, "cron", hour=hh, minute=mm, id=f"blip_{idx}", replace_existing=True)

    # FII/DII close
    if ENABLE_FII_DII:
        hh, mm = parse_hhmm(FII_DII_POST_TIME)
        sched.add_job(post_fii_dii, "cron", hour=hh, minute=mm, id="fiidii", replace_existing=True)

    # reset counters daily just after midnight
    sched.add_job(reset_counters_if_needed, "cron", hour=0, minute=5, id="reset", replace_existing=True)

def startup_ping():
    try:
        safe_send("✅ MarketPulse bot restarted.")
    except Exception:
        pass

def main():
    schedule_all()
    sched.start()
    logging.info("Scheduler started.")
    startup_ping()

if __name__ == "__main__":
    main()
    # For local dev; on Render, use gunicorn start command.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
