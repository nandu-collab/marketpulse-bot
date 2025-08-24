import os, json, time, logging, hashlib, re
from datetime import datetime, date, timedelta
import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# -----------------------
# Config (env with sane defaults)
# -----------------------
BOT_TOKEN      = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID     = os.getenv("CHANNEL_ID", "").strip()   # -100xxxx or @handle
TZ_NAME        = os.getenv("TIMEZONE", "Asia/Kolkata")

# news cadence
NEWS_INTERVAL_MIN   = int(os.getenv("NEWS_INTERVAL", "30"))
MAX_NEWS_PER_SLOT   = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
SUMMARY_CHARS       = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

# daily fixed posts
ENABLE_IPO          = os.getenv("ENABLE_IPO", "1") == "1"
IPO_POST_TIME       = os.getenv("IPO_POST_TIME", "10:30")
POSTMARKET_TIME     = os.getenv("POSTMARKET_TIME", "20:45")
ENABLE_FII_DII      = os.getenv("ENABLE_FII_DII", "1") == "1"
FII_DII_POST_TIME   = os.getenv("FII_DII_POST_TIME", "21:00")

# posting window for rolling news
WINDOW_START        = os.getenv("MARKET_BLIPS_START", "08:30")
WINDOW_END          = os.getenv("MARKET_BLIPS_END", "20:30")

# quiet hours (optional)
QUIET_START         = os.getenv("QUIET_HOURS_START", "22:30")
QUIET_END           = os.getenv("QUIET_HOURS_END", "07:30")

# weekends: post news too (default yes so you keep activity)
POST_ON_WEEKENDS    = os.getenv("POST_ON_WEEKENDS", "1") == "1"

# optional debug force key for /force
FORCE_KEY           = os.getenv("FORCE_KEY", "")

# Render port
PORT = int(os.getenv("PORT", "10000"))

# -----------------------
# Hard-coded sources (work even if you don’t set env)
# Mix of RSS-based (reliable) + HTML fallbacks.
# -----------------------
NEWS_SOURCES = [
    # Economic Times
    {"name":"ET Markets", "type":"rss", "cat":"markets",
     "url":"https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"},
    {"name":"ET Companies", "type":"rss", "cat":"companies",
     "url":"https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms"},
    {"name":"ET World", "type":"rss", "cat":"global",
     "url":"https://economictimes.indiatimes.com/news/international/business/rssfeeds/2698317.cms"},

    # Mint
    {"name":"Mint Markets", "type":"rss", "cat":"markets",
     "url":"https://www.livemint.com/rss/markets"},
    {"name":"Mint Companies", "type":"rss", "cat":"companies",
     "url":"https://www.livemint.com/rss/companies"},
    {"name":"Mint Economy", "type":"rss", "cat":"global",
     "url":"https://www.livemint.com/rss/economy"},

    # Moneycontrol (RSS exists for top news & buzzing stocks)
    {"name":"MC Top", "type":"rss", "cat":"markets",
     "url":"https://www.moneycontrol.com/rss/MCtopnews.xml"},
    {"name":"MC Buzzing", "type":"rss", "cat":"markets",
     "url":"https://www.moneycontrol.com/rss/buzzingstocks.xml"},

    # Reuters (global -> impact)
    {"name":"Reuters Biz", "type":"rss", "cat":"global",
     "url":"https://feeds.reuters.com/reuters/businessNews"},
    {"name":"Reuters Markets", "type":"rss", "cat":"global",
     "url":"https://feeds.reuters.com/reuters/marketsNews"},
]

# IPO sources (fallback chain)
IPO_SOURCES = [
    # Moneycontrol IPO news stream (we’ll parse latest items mentioning IPOs)
    {"name":"Moneycontrol IPO", "type":"rss",
     "url":"https://www.moneycontrol.com/rss/iponews.xml"},
    # Chittorgarh pages are not RSS; we’ll try HTML if needed
    {"name":"Chittorgarh Today", "type":"html",
     "url":"https://www.chittorgarh.com/report/ipo-open-today/83/"},
]

# FII/DII source (Moneycontrol snapshot)
FII_DII_PAGE = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"

# -----------------------
# Globals
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("marketpulse")

if not BOT_TOKEN or not CHANNEL_ID:
    logger.error("BOT_TOKEN and CHANNEL_ID are required env vars.")
bot = Bot(BOT_TOKEN) if BOT_TOKEN else None

tz = pytz.timezone(TZ_NAME)

SEEN_FILE = "seen_cache.json"
seen = {}  # link_hash -> epoch

def load_seen():
    global seen
    try:
        with open(SEEN_FILE, "r") as f:
            seen = json.load(f)
    except Exception:
        seen = {}
def save_seen():
    try:
        with open(SEEN_FILE, "w") as f:
            json.dump(seen, f)
    except Exception:
        pass
load_seen()

def now_local():
    return datetime.now(tz)

def in_quiet_hours(dt):
    """True if local time is within quiet hours window."""
    s_h, s_m = map(int, QUIET_START.split(":"))
    e_h, e_m = map(int, QUIET_END.split(":"))
    start = dt.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
    end   = dt.replace(hour=e_h, minute=e_m, second=0, microsecond=0)
    if start <= end:
        return start <= dt <= end
    # wraps midnight
    return dt >= start or dt <= end

def within_window(dt):
    """True if dt within rolling news window."""
    a_h,a_m = map(int, WINDOW_START.split(":"))
    b_h,b_m = map(int, WINDOW_END.split(":"))
    start = dt.replace(hour=a_h, minute=a_m, second=0, microsecond=0)
    end   = dt.replace(hour=b_h, minute=b_m, second=0, microsecond=0)
    return start <= dt <= end

def sha(s):
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()

def clean_html(text):
    if not text:
        return ""
    text = re.sub(r"<.*?>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def trim(text, limit):
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rsplit(" ", 1)[0] + "…"

def dedup_and_remember(link):
    key = sha(link)
    if key in seen:
        return False
    seen[key] = int(time.time())
    # prune >72h old
    cutoff = int(time.time()) - 72 * 3600
    for k, v in list(seen.items()):
        if v < cutoff:
            seen.pop(k, None)
    save_seen()
    return True

# -----------------------
# Fetchers
# -----------------------
def fetch_rss(url, cap=10):
    try:
        d = feedparser.parse(url)
        out = []
        for e in d.entries[:cap]:
            title = clean_html(getattr(e, "title", ""))
            link  = getattr(e, "link", "")
            summary = clean_html(getattr(e, "summary", "") or getattr(e, "description", ""))
            if title and link:
                out.append({"title": title, "link": link, "summary": summary})
        return out
    except Exception as ex:
        logger.warning(f"RSS fail {url}: {ex}")
        return []

def fetch_html_moneycontrol_markets():
    url = "https://www.moneycontrol.com/news/business/markets/"
    items = []
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a"):
            href = (a.get("href") or "")
            title = (a.get("title") or a.get_text() or "").strip()
            if "/news/" in href and title and len(title) > 30:
                items.append({"title": title, "link": href, "summary": ""})
            if len(items) >= 10:
                break
    except Exception as ex:
        logger.warning(f"HTML fail moneycontrol markets: {ex}")
    return items

def gather_news():
    """Return list of fresh items (title, link, summary), already deduped."""
    all_items = []

    # try RSS first
    for src in NEWS_SOURCES:
        if src["type"] == "rss":
            all_items += fetch_rss(src["url"], cap=10)

    # add a simple HTML fallback to keep weekend activity
    all_items += fetch_html_moneycontrol_markets()

    # dedup by link
    fresh = []
    for it in all_items:
        link = it.get("link", "")
        if not link:
            continue
        if dedup_and_remember(link):
            fresh.append(it)
        if len(fresh) >= MAX_NEWS_PER_SLOT:
            break
    return fresh

# -----------------------
# Format & Post
# -----------------------
def make_news_message(items):
    # one or two items; each: title + short summary. Button for the first item.
    if not items:
        return None, None
    lines = ["🗞️ <b>Market Update</b>"]
    for it in items:
        lines.append(f"• <b>{trim(it['title'], 120)}</b>")
        if it.get("summary"):
            lines.append(f"  {trim(it['summary'], SUMMARY_CHARS)}")
    text = "\n".join(lines)
    # main button: first link; if two items, add second as extra button
    buttons = [[InlineKeyboardButton("Read More →", url=items[0]["link"])]]
    if len(items) > 1 and items[1].get("link"):
        buttons.append([InlineKeyboardButton("More →", url=items[1]["link"])])
    return text, InlineKeyboardMarkup(buttons)

def post_text(text, markup=None):
    if not bot:
        logger.error("Bot not initialized; BOT_TOKEN missing.")
        return
    try:
        bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=markup
        )
    except Exception as ex:
        logger.error(f"Telegram post failed: {ex}")

def fetch_and_post_news():
    dt = now_local()
    if in_quiet_hours(dt):
        logger.info("Quiet hours; skip slot.")
        return
    if not POST_ON_WEEKENDS and dt.weekday() >= 5:
        logger.info("Weekend; skip slot.")
        return
    if not within_window(dt):
        logger.info("Outside window; skip slot.")
        return

    items = gather_news()
    if not items:
        logger.info("No fresh items this slot.")
        return
    text, markup = make_news_message(items)
    if text:
        post_text(text, markup)

# -----------------------
# IPO (best-effort; never says 'No IPO' unless both sources empty)
# -----------------------
def parse_mc_ipo_rss():
    rows = []
    feed = fetch_rss("https://www.moneycontrol.com/rss/iponews.xml", cap=10)
    # Look for headlines containing 'IPO' and a price band or open/close wording
    for e in feed:
        if "ipo" in e["title"].lower():
            rows.append({"name": e["title"], "note": trim(e["summary"], 180)})
    return rows

def parse_chittorgarh_open_today():
    url = "https://www.chittorgarh.com/report/ipo-open-today/83/"
    rows = []
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        # simple table parse
        for tr in soup.select("table tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(tds) >= 5:
                name = tds[0]
                status = " ".join(tds).lower()
                if "open" in status:
                    rows.append({"name": name, "note": "Open today"})
    except Exception as ex:
        logger.warning(f"Chittorgarh parse fail: {ex}")
    return rows

def post_ipo():
    if not ENABLE_IPO:
        return
    today_list = parse_chittorgarh_open_today()
    if not today_list:
        today_list = parse_mc_ipo_rss()
    if not today_list:
        post_text("📌 <b>IPO</b>\nCouldn’t confirm today’s live IPOs right now. I’ll retry in the next news slot.")
        return
    lines = ["📌 <b>IPO — Today</b>"]
    for r in today_list[:6]:
        lines.append(f"• {r['name']}" + (f" — {r['note']}" if r.get("note") else ""))
    post_text("\n".join(lines))

# -----------------------
# Post-market snapshot (best-effort)
# -----------------------
def fetch_indexes_close():
    # very light best-effort scrape from Moneycontrol indices page
    out = {}
    try:
        url = "https://www.moneycontrol.com/markets/indian-indices/"
        r = requests.get(url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        # crude regex for Sensex/Nifty levels
        m1 = re.search(r"Sensex[^0-9]*([0-9,]{4,8})", text)
        m2 = re.search(r"Nifty\s*50[^0-9]*([0-9,]{4,8})", text)
        if m1: out["Sensex"] = m1.group(1)
        if m2: out["Nifty 50"] = m2.group(1)
    except Exception as ex:
        logger.warning(f"Indexes fetch fail: {ex}")
    return out

def post_postmarket():
    idx = fetch_indexes_close()
    if not idx:
        post_text("🔔 <b>Post-Market</b>\nClosing snapshot unavailable right now.")
        return
    lines = ["🔔 <b>Post-Market • Closing Snapshot</b>"]
    for k,v in idx.items():
        lines.append(f"{k}: {v}")
    post_text("\n".join(lines))

# -----------------------
# FII / DII (Moneycontrol snapshot, best-effort)
# -----------------------
def parse_fii_dii():
    try:
        r = requests.get(FII_DII_PAGE, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        # crude pull like "FII (Cash) : ₹1,234 cr" style
        m_fii = re.search(r"FII[^₹]*₹\s*([0-9,]+)\s*cr", text)
        m_dii = re.search(r"DII[^₹]*₹\s*([0-9,]+)\s*cr", text)
        return {
            "FII": ("₹" + m_fii.group(1) + " cr") if m_fii else None,
            "DII": ("₹" + m_dii.group(1) + " cr") if m_dii else None,
        }
    except Exception as ex:
        logger.warning(f"FII/DII fetch fail: {ex}")
        return {}

def post_fii_dii():
    data = parse_fii_dii()
    if not data.get("FII") and not data.get("DII"):
        post_text("🏦 <b>FII/DII</b>\nflow data unavailable right now.")
        return
    lines = ["🏦 <b>FII/DII — Cash Market</b>"]
    if data.get("FII"): lines.append(f"FII: {data['FII']}")
    if data.get("DII"): lines.append(f"DII: {data['DII']}")
    post_text("\n".join(lines))

# -----------------------
# Flask & Scheduler
# -----------------------
app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=tz)

@app.route("/")
def root():
    return "ok"

@app.route("/force")
def force():
    # optional debug trigger: /force?key=XYZ
    if FORCE_KEY and request.args.get("key") != FORCE_KEY:
        return "forbidden", 403
    fetch_and_post_news()
    return "forced", 200

def _add_cron(label, time_str, func):
    h, m = map(int, time_str.split(":"))
    scheduler.add_job(func, CronTrigger(hour=h, minute=m, timezone=tz), id=label, replace_existing=True)

def start_schedule():
    # rolling news every N minutes (08:30–20:30 local)
    start_h, start_m = map(int, WINDOW_START.split(":"))
    end_h, end_m     = map(int, WINDOW_END.split(":"))
    # Cron expression for "every N minutes" is minute="*/N"
    scheduler.add_job(
        fetch_and_post_news,
        "cron",
        minute=f"*/{NEWS_INTERVAL_MIN}",
        hour=f"{start_h}-{end_h}",
        timezone=tz,
        id="news_loop",
        replace_existing=True,
    )
    if ENABLE_IPO:
        _add_cron("ipo_post", IPO_POST_TIME, post_ipo)
    _add_cron("post_market", POSTMARKET_TIME, post_postmarket)
    if ENABLE_FII_DII:
        _add_cron("fii_dii", FII_DII_POST_TIME, post_fii_dii)

def announce():
    wnd = f"{WINDOW_START}–{WINDOW_END}"
    text = (
        "✅ <b>MarketPulse bot restarted and schedule loaded.</b>\n"
        f"Window: {wnd} • Every {NEWS_INTERVAL_MIN} min • Max {MAX_NEWS_PER_SLOT}/slot\n"
        f"Quiet: {QUIET_START}–{QUIET_END}\n"
        f"Fixed posts: {IPO_POST_TIME} IPO • {POSTMARKET_TIME} Post-market • {FII_DII_POST_TIME} FII/DII"
    )
    post_text(text)

def run_boot():
    # Start scheduler
    start_schedule()
    scheduler.start()
    # announce + do an immediate news try (if not quiet)
    announce()
    try:
        fetch_and_post_news()
    except Exception as ex:
        logger.warning(f"Immediate news try failed: {ex}")

# gunicorn entrypoint will import app and execute below block thanks to preload
run_boot()

if __name__ == "__main__":
    # local run
    app.run(host="0.0.0.0", port=PORT)
