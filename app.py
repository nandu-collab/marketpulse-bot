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

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Env & TZ ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # "-100..." or "@yourchannel"
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Kolkata"))

# News window (also used for schedule banner)
NEWS_WINDOW_START = os.getenv("NEWS_WINDOW_START", "08:30")
NEWS_WINDOW_END = os.getenv("NEWS_WINDOW_END", "20:30")
ENABLE_NEWS = int(os.getenv("ENABLE_NEWS", "1"))
MAX_NEWS_PER_SLOT = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

# Quiet hours (skip all posts)
QUIET_HOURS_START = os.getenv("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = os.getenv("QUIET_HOURS_END", "07:30")

# Fixed posts (weekdays only)
ENABLE_IPO = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME = os.getenv("IPO_POST_TIME", "10:30")
POSTMARKET_TIME = os.getenv("POSTMARKET_TIME", "20:45")
ENABLE_FII_DII = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME = os.getenv("FII_DII_POST_TIME", "21:00")

# ---------------- Telegram ----------------
bot = Bot(token=BOT_TOKEN)

# ---------------- Helpers ----------------
def now():
    return datetime.now(TZ)

def _parse_hhmm(s: str):
    h, m = map(int, s.split(":"))
    return h, m

def _within(day_start: str, day_end: str, dt: datetime) -> bool:
    sh, sm = _parse_hhmm(day_start)
    eh, em = _parse_hhmm(day_end)
    start_t = time(sh, sm)
    end_t = time(eh, em)
    t = dt.time()
    if start_t <= end_t:
        return start_t <= t <= end_t
    else:  # across midnight
        return t >= start_t or t <= end_t

def in_quiet_hours() -> bool:
    return _within(QUIET_HOURS_START, QUIET_HOURS_END, now())

def weekday_only() -> bool:
    return now().weekday() < 5  # Mon=0 .. Fri=4

def send_message(html: str):
    if in_quiet_hours():
        logging.info("⏸ Quiet hours active; skipping post.")
        return
    try:
        bot.send_message(
            chat_id=CHANNEL_ID,
            text=html,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        logging.info("✅ Posted: %s", html[:80].replace("\n", " "))
    except TelegramError as e:
        logging.error("Telegram error: %s", e)

# ---------------- News ----------------
# Use endpoints that usually allow scraping via UA
NEWS_FEEDS = [
    "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "https://www.moneycontrol.com/rss/buzzingstocks.xml",
    "https://www.livemint.com/rss/markets",
    "https://www.business-standard.com/rss/markets-101.rss",
    "https://feeds.reuters.com/reuters/INbusinessNews"
]

HTTP_HEADERS = {
    "User-Agent": "MarketPulseBot/1.0 (+https://t.me/)",
    "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
}

seen_links = set()        # in-memory, reset on each deploy
MAX_SEEN = 1000

def _fetch_feed(url: str):
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        r.raise_for_status()
        feed = feedparser.parse(r.content)
        return feed.entries
    except Exception as e:
        logging.warning("Feed fetch failed %s: %s", url, e)
        return []

def _summarize(text: str, limit: int) -> str:
    if not text:
        return ""
    clean = text.replace("<br>", " ").replace("<br/>", " ").replace("\n", " ").strip()
    if len(clean) > limit:
        clean = clean[:limit].rstrip() + "..."
    return clean

def fetch_news_items(max_items: int):
    items = []
    for url in NEWS_FEEDS:
        for e in _fetch_feed(url)[:max_items * 2]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            if not title or not link:
                continue
            if link in seen_links:
                continue
            summary = _summarize(getattr(e, "summary", ""), NEWS_SUMMARY_CHARS)
            items.append((title, summary, link))
            if len(items) >= max_items * 3:
                break
        if len(items) >= max_items * 3:
            break
    return items

def post_news_slot():
    if not ENABLE_NEWS:
        return
    # post only inside the configured news window
    if not _within(NEWS_WINDOW_START, NEWS_WINDOW_END, now()):
        logging.info("⏭ Outside news window; skipping slot.")
        return

    logging.info("📰 Fetching news...")
    items = fetch_news_items(MAX_NEWS_PER_SLOT * 3)
    posted = 0
    for title, summary, link in items:
        if posted >= MAX_NEWS_PER_SLOT:
            break
        msg = f"<b>{title}</b>\n{summary}\n<a href=\"{link}\">Read more</a>"
        send_message(msg)
        seen_links.add(link)
        if len(seen_links) > MAX_SEEN:
            # keep set size bounded
            for _ in range(len(seen_links) - MAX_SEEN):
                try:
                    seen_links.pop()
                except KeyError:
                    break
        posted += 1

    if posted == 0:
        logging.info("ℹ️ No fresh news to post this slot.")

# ---------------- IPO / Post-market / FII-DII ----------------
def post_ipo():
    if not ENABLE_IPO or not weekday_only():
        logging.info("⏭ Skipping IPO (disabled or weekend).")
        return
    # TODO: plug real NSE/BSE source here; placeholder text for now
    send_message(
        "📌 <b>IPO Watch</b>\n"
        "No. of live issues, price bands & lots vary daily. "
        "This placeholder will be replaced with live NSE/BSE data."
    )

def post_postmarket():
    if not weekday_only():
        logging.info("⏭ Skipping Post-market (weekend).")
        return
    send_message(
        "📊 <b>Post-Market Summary</b>\n"
        "Sensex/Nifty close, sector moves, AD ratio, VIX — summary here."
    )

def post_fii_dii():
    if not ENABLE_FII_DII or not weekday_only():
        logging.info("⏭ Skipping FII/DII (disabled or weekend).")
        return
    send_message(
        "💰 <b>FII/DII Flows</b>\n"
        "FII: — ₹ Cr  |  DII: — ₹ Cr  (cash segment)\n"
        "MTD: FII — | DII —"
    )

# ---------------- Scheduler ----------------
scheduler = BackgroundScheduler(timezone=TZ)

# 30-minute aligned news slots during the day window (every day)
start_h, start_m = _parse_hhmm(NEWS_WINDOW_START)
end_h, end_m = _parse_hhmm(NEWS_WINDOW_END)

def _add_news_cron_for_range(h1, h2):
    if h1 is None or h2 is None:
        return
    hour_expr = f"{h1}" if h1 == h2 else f"{h1}-{h2}"
    scheduler.add_job(
        post_news_slot,
        CronTrigger(minute="0,30", hour=hour_expr, timezone=TZ),
        id=f"news_{hour_expr}",
        replace_existing=True,
    )

if start_h <= end_h:
    _add_news_cron_for_range(start_h, end_h)
else:
    # window across midnight (not your case, but handled)
    _add_news_cron_for_range(0, end_h)
    _add_news_cron_for_range(start_h, 23)

# Fixed weekday posts
ih, im = _parse_hhmm(IPO_POST_TIME)
scheduler.add_job(post_ipo, CronTrigger(hour=ih, minute=im, day_of_week="mon-fri", timezone=TZ), id="ipo", replace_existing=True)

ph, pm = _parse_hhmm(POSTMARKET_TIME)
scheduler.add_job(post_postmarket, CronTrigger(hour=ph, minute=pm, day_of_week="mon-fri", timezone=TZ), id="postmarket", replace_existing=True)

fh, fm = _parse_hhmm(FII_DII_POST_TIME)
scheduler.add_job(post_fii_dii, CronTrigger(hour=fh, minute=fm, day_of_week="mon-fri", timezone=TZ), id="fii_dii", replace_existing=True)

scheduler.start()

# ---------------- Flask ----------------
app = Flask(__name__)

@app.route("/")
def index():
    banner = (
        f"✅ MarketPulse running<br>"
        f"News window: {NEWS_WINDOW_START}–{NEWS_WINDOW_END} (every 00 & 30)<br>"
        f"Max news/slot: {MAX_NEWS_PER_SLOT}<br>"
        f"Quiet hours: {QUIET_HOURS_START}–{QUIET_HOURS_END}<br>"
        f"IPO: {IPO_POST_TIME} • Post-market: {POSTMARKET_TIME} • FII/DII: {FII_DII_POST_TIME} (weekdays)"
    )
    return banner

# ---------------- Startup banner ----------------
def startup_banner():
    send_message(
        "✅ <b>MarketPulse bot restarted and schedule loaded.</b>\n"
        f"Window: {NEWS_WINDOW_START}–{NEWS_WINDOW_END} • 00/30 mins • Max {MAX_NEWS_PER_SLOT}/slot\n"
        f"Quiet: {QUIET_HOURS_START}–{QUIET_HOURS_END}\n"
        f"Fixed: {IPO_POST_TIME} IPO • {POSTMARKET_TIME} Post-market • {FII_DII_POST_TIME} FII/DII"
    )

startup_banner()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
