import os
import json
import re
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest

# -------------------------
# Settings (from ENV)
# -------------------------
TZ_NAME = os.getenv("TIMEZONE", "Asia/Kolkata")
TZ = pytz.timezone(TZ_NAME)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()   # "-100xxxxxxxxxx" or "@yourchannel"

# News window & cadence
ENABLE_NEWS = os.getenv("ENABLE_NEWS", "1") == "1"
NEWS_INTERVAL = int(os.getenv("NEWS_INTERVAL", "30"))  # minutes
MAX_NEWS_PER_SLOT = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

MARKET_BLIPS_START = os.getenv("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END = os.getenv("MARKET_BLIPS_END", "20:30")

# Quiet hours (no news) – still allows fixed posts (IPO/PostMkt/FII/DII)
QUIET_START = os.getenv("QUIET_HOURS_START", "22:30")
QUIET_END = os.getenv("QUIET_HOURS_END", "07:30")

# Fixed posts
ENABLE_IPO = os.getenv("ENABLE_IPO", "1") == "1"
IPO_POST_TIME = os.getenv("IPO_POST_TIME", "10:30")

POSTMARKET_TIME = os.getenv("POSTMARKET_TIME", "20:45")

ENABLE_FII_DII = os.getenv("ENABLE_FII_DII", "1") == "1"
FII_DII_POST_TIME = os.getenv("FII_DII_POST_TIME", "21:00")

# Render specifics
PORT = int(os.getenv("PORT", "10000"))

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("marketpulse")

# -------------------------
# Telegram
# -------------------------
if not BOT_TOKEN or not CHANNEL_ID:
    log.warning("BOT_TOKEN / CHANNEL_ID not set – the app will run but can't post.")

bot = Bot(BOT_TOKEN) if BOT_TOKEN else None

def tg_send(text: str, buttons: Optional[List[List[InlineKeyboardButton]]] = None):
    """Send a message to the channel with retries; swallow harmless errors."""
    if not bot:
        log.warning("Telegram bot not initialised; skipping send.")
        return

    markup = InlineKeyboardMarkup(buttons) if buttons else None

    for attempt in range(4):
        try:
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
            return
        except RetryAfter as e:
            wait = int(getattr(e, "retry_after", 2)) + 1
            log.warning(f"Telegram rate limited. Sleeping {wait}s.")
            time.sleep(wait)
        except (TimedOut, NetworkError) as e:
            log.warning(f"Telegram transient error: {e}. Retry {attempt+1}/3.")
            time.sleep(2 + attempt)
        except BadRequest as e:
            # e.g., "Chat not found" or malformed text
            log.error(f"Telegram BadRequest: {e}. Aborting send.")
            return
        except Exception as e:
            log.error(f"Telegram unknown error: {e}. Aborting send.")
            return

# -------------------------
# Dedup store (persist within instance)
# -------------------------
POSTED_DB_PATH = "/tmp/posted_links.json"
try:
    with open(POSTED_DB_PATH, "r", encoding="utf-8") as f:
        POSTED: Dict[str, float] = json.load(f)
except Exception:
    POSTED = {}

def save_posted():
    # keep at most last 2000 links
    if len(POSTED) > 2200:
        # drop oldest 200
        for k in sorted(POSTED, key=POSTED.get)[:200]:
            POSTED.pop(k, None)
    try:
        with open(POSTED_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(POSTED, f)
    except Exception:
        pass

def seen(url: str) -> bool:
    now = time.time()
    # expire after 7 days
    for k in list(POSTED.keys()):
        if now - POSTED[k] > 7 * 24 * 3600:
            POSTED.pop(k, None)
    if url in POSTED:
        return True
    POSTED[url] = now
    save_posted()
    return False

# -------------------------
# Helpers
# -------------------------
def ist_now() -> datetime:
    return datetime.now(TZ)

def within_time_window(now: datetime, start_hm: str, end_hm: str) -> bool:
    """Return True if now is between start and end (HH:MM) on the same day.
    Handles windows that cross midnight."""
    sh, sm = map(int, start_hm.split(":"))
    eh, em = map(int, end_hm.split(":"))
    start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end_dt = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    if end_dt >= start_dt:
        return start_dt <= now <= end_dt
    # crosses midnight
    return now >= start_dt or now <= end_dt

def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()

def trim(s: str, chars: int) -> str:
    s = s.strip()
    if len(s) <= chars:
        return s
    # try cut at sentence boundary
    cut = s.rfind(".", 0, chars)
    if cut == -1:
        cut = chars
    return s[:cut].rstrip() + "…"

# -------------------------
# News feeds
# -------------------------
NEWS_FEEDS = [
    # Market & companies
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",
    "https://www.livemint.com/rss/markets",
    "https://www.livemint.com/rss/companies",
    "https://www.cnbctv18.com/news/business/rssfeed",
]

def fetch_news_items() -> List[Dict]:
    items: List[Dict] = []
    for url in NEWS_FEEDS:
        try:
            d = feedparser.parse(url)
            for e in d.entries[:15]:
                link = e.get("link") or ""
                if not link or seen(link):
                    continue
                title = strip_html(e.get("title", ""))
                summary = strip_html(e.get("summary", "")) or strip_html(e.get("description", ""))
                if not title:
                    continue
                items.append({
                    "title": title,
                    "summary": trim(summary, NEWS_SUMMARY_CHARS),
                    "link": link
                })
        except Exception as ex:
            log.warning(f"Feed fail {url}: {ex}")
    # keep recent-most first (most feeds already sorted)
    return items[: MAX_NEWS_PER_SLOT]

def post_news_slot():
    now = ist_now()
    if not ENABLE_NEWS:
        return
    # respect quiet hours
    if within_time_window(now, QUIET_START, QUIET_END):
        log.info("Quiet hours; skipping news slot.")
        return
    # respect market-blips window
    if not within_time_window(now, MARKET_BLIPS_START, MARKET_BLIPS_END):
        log.info("Outside news window; skipping news slot.")
        return

    items = fetch_news_items()
    if not items:
        log.info("No fresh news this slot.")
        return

    for it in items:
        title = it["title"]
        body = it["summary"] or ""
        link = it["link"]
        text = f"📰 <b>{title}</b>\n\n{body}"
        btns = [[InlineKeyboardButton("Read More ▸", url=link)]]
        tg_send(text, btns)
        time.sleep(1.2)

# -------------------------
# IPO snap (Moneycontrol IPO page – simple parse, graceful fallback)
# -------------------------
def fetch_ipo_today_text() -> str:
    """
    Try to pull IPOs open today from Moneycontrol calendar.
    Parsing is best-effort and may change if the site changes.
    """
    try:
        url = "https://www.moneycontrol.com/ipo/"
        html = requests.get(url, timeout=12).text
        soup = BeautifulSoup(html, "html.parser")

        # Look for sections that mention 'Open' or 'Current'
        cards = []
        for blk in soup.find_all(["tr", "div"]):
            txt = " ".join(blk.get_text(" ").split())
            if re.search(r"\b(Open|Current)\b", txt, re.I) and re.search(r"\bIPO\b", txt, re.I):
                cards.append(txt)

        # Deduplicate
        clean = []
        for c in cards:
            if c not in clean:
                clean.append(c)

        # Try to format 3–4 lines
        lines = []
        for c in clean[:4]:
            # attempt to pick name + dates + price
            m_name = re.search(r"([A-Z][A-Za-z0-9 &.-]{2,})\s+IPO", c)
            name = m_name.group(1) if m_name else None
            m_open = re.search(r"Open(?:ing)?\s*[:\-]?\s*([0-9]{1,2}\s*[A-Za-z]{3,})", c)
            m_close = re.search(r"Close(?:ing)?\s*[:\-]?\s*([0-9]{1,2}\s*[A-Za-z]{3,})", c)
            m_price = re.search(r"Price\s*Band\s*[:\-]?\s*([\u20B9₹]?\s*[0-9]+(?:\s*–\s*[0-9]+)?)", c)
            piece = []
            if name: piece.append(f"<b>{name}</b>")
            if m_price: piece.append(f"Price: {m_price.group(1)}")
            if m_open or m_close:
                o = m_open.group(1) if m_open else "?"
                cl = m_close.group(1) if m_close else "?"
                piece.append(f"Open–Close: {o}–{cl}")
            if piece:
                lines.append(" • " + " | ".join(piece))

        if lines:
            header = "📌 <b>IPO</b> — Today’s snapshot"
            return header + "\n" + "\n".join(lines)

    except Exception as e:
        log.warning(f"IPO scrape failed: {e}")

    return "📌 <b>IPO</b>\nNo IPO details available today."

def post_ipo():
    tg_send(fetch_ipo_today_text())

# -------------------------
# Post-market snapshot (best-effort scrape)
# -------------------------
def fetch_post_market_text() -> str:
    """
    Best-effort: pull Nifty/Sensex close from Moneycontrol Markets page.
    """
    try:
        url = "https://www.moneycontrol.com/markets/indian-indices/"
        html = requests.get(url, timeout=12).text
        soup = BeautifulSoup(html, "html.parser")

        def grab(label):
            node = soup.find(string=re.compile(label, re.I))
            if not node:
                return None
            row = node.find_parent(["tr", "div"])
            txt = " ".join(row.get_text(" ").split())
            m = re.search(r"([0-9,]+\.\d+|[0-9,]+)\s*\(\s*([+\-]?[0-9.,]+)\s*\|\s*([+\-]?[0-9.,]+)\s*%\s*\)", txt)
            if m:
                return m.group(1), m.group(3)
            # fallback: just first number
            m2 = re.search(r"([0-9,]+\.\d+|[0-9,]+)", txt)
            return (m2.group(1), None) if m2 else None

        snx = grab("Sensex")
        nfy = grab("Nifty 50")
        bnk = grab("Nifty Bank")

        parts = ["📊 <b>Post-Market</b>"]
        if snx:
            parts.append(f"Sensex: {snx[0]} ({snx[1]}%)" if snx[1] else f"Sensex: {snx[0]}")
        if nfy:
            parts.append(f"Nifty 50: {nfy[0]} ({nfy[1]}%)" if nfy[1] else f"Nifty 50: {nfy[0]}")
        if bnk:
            parts.append(f"Bank Nifty: {bnk[0]} ({bnk[1]}%)" if bnk[1] else f"Bank Nifty: {bnk[0]}")

        if len(parts) > 1:
            return "\n".join(parts)

    except Exception as e:
        log.warning(f"Post-market scrape failed: {e}")

    return "📊 <b>Post-Market</b>\nSummary not available today."

def post_postmarket():
    tg_send(fetch_post_market_text())

# -------------------------
# FII/DII flows (best-effort)
# -------------------------
def fetch_fii_dii_text() -> str:
    """
    Best-effort: pull flows from Moneycontrol Markets or NSE/BSE summary pages (if present).
    """
    try:
        url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
        html = requests.get(url, timeout=12).text
        soup = BeautifulSoup(html, "html.parser")
        txt = " ".join(soup.get_text(" ").split())
        # Look for numbers like "FII: -₹1,234 cr DII: +₹567 cr"
        m_fii = re.search(r"FII(?:\s*Net\s*buy|)\s*[:\-]?\s*([+\-]?\s*₹?\s*[0-9,]+)\s*cr", txt, re.I)
        m_dii = re.search(r"DII(?:\s*Net\s*buy|)\s*[:\-]?\s*([+\-]?\s*₹?\s*[0-9,]+)\s*cr", txt, re.I)
        if m_fii or m_dii:
            fii = m_fii.group(1).replace(" ", "") if m_fii else "n/a"
            dii = m_dii.group(1).replace(" ", "") if m_dii else "n/a"
            return f"💰 <b>FII/DII Flows</b>\nFII: {fii} cr\nDII: {dii} cr"
    except Exception as e:
        log.warning(f"FII/DII scrape failed: {e}")

    return "💰 <b>FII/DII Flows</b>\nData not available today."

def post_fii_dii():
    tg_send(fetch_fii_dii_text())

# -------------------------
# Scheduler
# -------------------------
scheduler = BackgroundScheduler(timezone=TZ)

def add_news_schedule():
    # Cron like: */30 8-20 * * MON-FRI but with quiet hours considered at run-time
    # We schedule every NEWS_INTERVAL minute round-the-clock and filter in the handler.
    minute_expr = f"*/{NEWS_INTERVAL}"
    scheduler.add_job(
        post_news_slot,
        CronTrigger.from_crontab(f"{minute_expr} * * * *", timezone=TZ),
        id="post_news_slot",
        replace_existing=True,
    )

def add_fixed_jobs():
    if ENABLE_IPO:
        h, m = map(int, IPO_POST_TIME.split(":"))
        scheduler.add_job(post_ipo, "cron", hour=h, minute=m, timezone=TZ, id="post_ipo", replace_existing=True)
    h, m = map(int, POSTMARKET_TIME.split(":"))
    scheduler.add_job(post_postmarket, "cron", hour=h, minute=m, timezone=TZ, id="post_postmarket", replace_existing=True)
    if ENABLE_FII_DII:
        h, m = map(int, FII_DII_POST_TIME.split(":"))
        scheduler.add_job(post_fii_dii, "cron", hour=h, minute=m, timezone=TZ, id="post_fii_dii", replace_existing=True)

def start_scheduler():
    add_news_schedule()
    add_fixed_jobs()
    scheduler.start()
    # On boot, announce schedule
    blip = f"Window: {MARKET_BLIPS_START}–{MARKET_BLIPS_END} • Every {NEWS_INTERVAL} min • Max {MAX_NEWS_PER_SLOT}/slot\nQuiet: {QUIET_START}–{QUIET_END}\nFixed: IPO {IPO_POST_TIME} • Post-market {POSTMARKET_TIME} • FII/DII {FII_DII_POST_TIME}"
    tg_send(f"✅ <b>MarketPulse</b> bot restarted and schedule loaded.\n{blip}")

# -------------------------
# Flask (Render needs a web server)
# -------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "MarketPulse bot is alive"

# -------------------------
# Boot
# -------------------------
if __name__ == "__main__":
    log.info("Starting scheduler…")
    start_scheduler()
    app.run(host="0.0.0.0", port=PORT)
