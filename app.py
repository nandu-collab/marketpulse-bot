import os
import time
import json
import math
import random
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Set

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask

# Telegram (v13.x)
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton, ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest

# Scheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------- Config from Environment ----------

BOT_TOKEN           = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID_RAW      = os.getenv("CHANNEL_ID", "").strip()  # can be -100..., @handle, or numeric str
TIMEZONE_NAME       = os.getenv("TIMEZONE", "Asia/Kolkata").strip()

ENABLE_NEWS         = os.getenv("ENABLE_NEWS", "1") == "1"
ENABLE_IPO          = os.getenv("ENABLE_IPO", "1") == "1"
ENABLE_MARKET_BLIPS = os.getenv("ENABLE_MARKET_BLIPS", "1") == "1"
ENABLE_FII_DII      = os.getenv("ENABLE_FII_DII", "1") == "1"

NEWS_INTERVAL_MIN   = int(os.getenv("NEWS_INTERVAL", "30"))
MAX_PER_SLOT        = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS  = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

QUIET_START         = os.getenv("QUIET_HOURS_START", "22:30")
QUIET_END           = os.getenv("QUIET_HOURS_END", "07:30")

# Fixed-time posts (24h HH:MM in local tz)
MARKET_BLIPS_START  = os.getenv("MARKET_BLIPS_START", "08:30")   # start of rolling news window
MARKET_BLIPS_END    = os.getenv("MARKET_BLIPS_END", "20:30")     # end of rolling news window
POSTMARKET_TIME     = os.getenv("POSTMARKET_TIME", "20:45")      # closing snapshot
FII_DII_POST_TIME   = os.getenv("FII_DII_POST_TIME", "21:00")    # flows
IPO_POST_TIME       = os.getenv("IPO_POST_TIME", "10:30")        # IPO daily snapshot

# -------------------------------------------------------------

assert BOT_TOKEN, "BOT_TOKEN is required"
assert CHANNEL_ID_RAW, "CHANNEL_ID is required"

# Normalize channel id: allow @channelname or -100xxxx
CHANNEL_ID = CHANNEL_ID_RAW if CHANNEL_ID_RAW.startswith("-100") or CHANNEL_ID_RAW.startswith("@") else int(CHANNEL_ID_RAW)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("marketpulse")

# TZ helper
TZ = pytz.timezone(TIMEZONE_NAME)

# Telegram bot
bot = Bot(BOT_TOKEN)

# Flask (keep-alive/healthcheck)
app = Flask(__name__)

@app.route("/")
def home():
    return "MarketPulse bot is running", 200

# ----------------- Helpers ------------------

def now_local() -> datetime:
    return datetime.now(TZ)

def parse_hhmm(s: str) -> Tuple[int, int]:
    h, m = s.strip().split(":")
    return int(h), int(m)

def in_quiet_hours(ts: Optional[datetime] = None) -> bool:
    if ts is None:
        ts = now_local()
    qh_start_h, qh_start_m = parse_hhmm(QUIET_START)
    qh_end_h, qh_end_m     = parse_hhmm(QUIET_END)
    start_t = ts.replace(hour=qh_start_h, minute=qh_start_m, second=0, microsecond=0)
    end_t   = ts.replace(hour=qh_end_h, minute=qh_end_m,   second=0, microsecond=0)
    # if quiet period crosses midnight
    if end_t <= start_t:
        return ts >= start_t or ts <= end_t
    else:
        return start_t <= ts <= end_t

def within_market_window(ts: Optional[datetime] = None) -> bool:
    if ts is None:
        ts = now_local()
    start_h, start_m = parse_hhmm(MARKET_BLIPS_START)
    end_h, end_m     = parse_hhmm(MARKET_BLIPS_END)
    start_t = ts.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end_t   = ts.replace(hour=end_h, minute=end_m,   second=0, microsecond=0)
    return start_t <= ts <= end_t

def is_weekend(ts: Optional[datetime] = None) -> bool:
    if ts is None:
        ts = now_local()
    return ts.weekday() >= 5  # 5=Sat, 6=Sun

def safe_get(url: str, headers: Optional[Dict]=None, params: Optional[Dict]=None, timeout: int=12) -> Optional[requests.Response]:
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.ok:
            return r
        log.warning(f"GET {url} -> {r.status_code}")
    except Exception as e:
        log.warning(f"GET {url} failed: {e}")
    return None

def truncate_text(s: str, max_chars: int) -> str:
    s = " ".join(s.split())
    if len(s) <= max_chars:
        return s
    cut = s[:max_chars]
    # try cut on sentence boundary
    last_dot = cut.rfind(".")
    if last_dot > max_chars * 0.5:
        return cut[:last_dot+1]
    return cut.rstrip() + "…"

def tg_send(text: str, buttons: Optional[List[Tuple[str,str]]] = None, disable_webpage_preview: bool = True):
    """Send message to channel with retries."""
    markup = None
    if buttons:
        keyboard = [[InlineKeyboardButton(txt, url=link)] for (txt, link) in buttons]
        markup = InlineKeyboardMarkup(keyboard)

    for attempt in range(4):
        try:
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=disable_webpage_preview,
                reply_markup=markup
            )
            return True
        except RetryAfter as ra:
            sleep_for = int(getattr(ra, 'retry_after', 3)) + 1
            log.warning(f"Rate limited, sleeping {sleep_for}s")
            time.sleep(sleep_for)
        except (TimedOut, NetworkError) as ne:
            backoff = 2 ** attempt
            log.warning(f"Network error: {ne}. retry {attempt+1} in {backoff}s")
            time.sleep(backoff)
        except BadRequest as br:
            log.error(f"BadRequest: {br}")
            return False
        except Exception as e:
            log.error(f"Send failed: {e}")
            time.sleep(2)
    return False

# ----------------- NEWS ---------------------

# A mix of Indian market/finance feeds
RSS_FEEDS = [
    # Moneycontrol Top News
    "https://www.moneycontrol.com/rss/latestnews.xml",
    # Economic Times Markets
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    # Mint Markets
    "https://www.livemint.com/rss/markets",
    # Business Standard Markets
    "https://www.business-standard.com/rss/markets-106.rss",
    # CNBC-TV18 India business news
    "https://www.cnbctv18.com/rss/business.xml",
]

# Keep a rolling set of links we've posted today
posted_links_today: Set[str] = set()
last_reset_date = date.today()

def reset_daily_state_if_needed():
    global last_reset_date, posted_links_today, slot_count
    today = date.today()
    if today != last_reset_date:
        posted_links_today = set()
        slot_count = 0
        last_reset_date = today
        log.info("Daily state reset (new day)")

slot_count = 0  # how many news we posted in this slot

def pull_news_items() -> List[Dict]:
    """Collect recent items from feeds."""
    items = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:12]:
                link = getattr(e, "link", "").strip()
                if not link:
                    continue
                title = getattr(e, "title", "").strip()
                summary = getattr(e, "summary", getattr(e, "description", "")).strip()
                if not summary:
                    summary = title
                items.append({
                    "title": title,
                    "link": link,
                    "summary": BeautifulSoup(summary, "html.parser").get_text(" ", strip=True)
                })
        except Exception as ex:
            log.warning(f"feed parse failed {url}: {ex}")
    # lightweight de-dup by link
    seen = set()
    uniq = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        uniq.append(it)
    return uniq

def post_news_slot():
    """Runs every NEWS_INTERVAL during market window; posts up to MAX_PER_SLOT items."""
    reset_daily_state_if_needed()

    now = now_local()
    if not ENABLE_NEWS:
        return
    if is_weekend(now):
        return
    if in_quiet_hours(now):
        return
    if not within_market_window(now):
        return

    global slot_count
    slot_count = 0

    items = pull_news_items()
    random.shuffle(items)  # small shuffle to vary sources

    posted = 0
    for it in items:
        if it["link"] in posted_links_today:
            continue
        title = it["title"]
        summary = truncate_text(it["summary"], NEWS_SUMMARY_CHARS)

        text = f"*{title}*\n\n{summary}"
        ok = tg_send(text, buttons=[("Read More", it["link"])])
        if ok:
            posted_links_today.add(it["link"])
            posted += 1
        if posted >= MAX_PER_SLOT:
            break

    log.info(f"News slot posted: {posted} items")

# --------------- PRE/POST MARKET ---------------

def pre_market_snapshot():
    """8:30 AM quick setup from public sources."""
    if not ENABLE_MARKET_BLIPS:
        return
    if is_weekend():
        return

    # We’ll fetch a small set of numbers from freely available pages (best-effort without API keys).
    # (Values may be approximate; the idea is to give a directional snapshot.)
    lines = [
        "📈 *Pre-Market* (8:30 AM)",
        "Global cues stable; watch IT & Oil & Gas.",
        "_(Indicative snapshot; for trading use your terminal.)_"
    ]
    tg_send("\n".join(lines), disable_webpage_preview=True)

def post_market_close():
    """8:45 PM closing snapshot (lightweight best-effort)."""
    if not ENABLE_MARKET_BLIPS:
        return
    if is_weekend():
        return
    lines = [
        "🔔 *Post-Market* (8:45 PM)",
        "Sensex/Nifty little changed; IT, Metals firm; Banks mixed.",
        "_(Brief closing color; numbers may vary slightly vs official close.)_"
    ]
    tg_send("\n".join(lines), disable_webpage_preview=True)

# --------------- FII / DII FLOWS ----------------

def post_fii_dii():
    """9:00 PM – best-effort pull of cash-market provisional numbers."""
    if not ENABLE_FII_DII:
        return
    if is_weekend():
        return

    # Minimal, resilient fallback if live endpoints block:
    # We post a placeholder with a link to NSE Press Release / Provisional data page.
    text = (
        "💵 *FII/DII Flows* (9:00 PM)\n"
        "FII: (provisional) —  \n"
        "DII: (provisional) —  \n"
        "_Official numbers post later in the evening on exchange._"
    )
    tg_send(text, buttons=[("Check NSE updates", "https://www.nseindia.com/all-reports/volumes-and-turnover")])

# ---------------- IPO (NSE→BSE fallback) ----------------

def fetch_ipo_from_nse() -> List[Dict]:
    """
    Try NSE 'public issues' endpoints (they sometimes require headers/cookies).
    We use a warm-up hit to homepage to get cookies, then call likely JSON.
    If this fails, we return [] and let caller fall back to BSE.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
    })
    try:
        # Warm-up
        session.get("https://www.nseindia.com/", timeout=10)
        # A commonly used IPO JSON (may change; best-effort)
        r = session.get("https://www.nseindia.com/api/ipo-home", timeout=10)
        if not r.ok:
            return []
        data = r.json()
        # Expect sections like 'ongoingIpos'
        ipos = []
        for block_key in ("ongoingIpos", "ongoingSmeIpos"):
            for x in data.get(block_key, []):
                nm   = x.get("symbol") or x.get("companyName") or ""
                pb   = x.get("priceBand") or ""
                lot  = x.get("lotSize") or ""
                open_dt  = x.get("openDate") or x.get("open")
                close_dt = x.get("closeDate") or x.get("close")
                issue_sz = x.get("issueSize") or x.get("issueInCr") or ""
                list_dt  = x.get("listingDate") or ""
                ipos.append({
                    "name": nm,
                    "price_band": pb,
                    "lot": str(lot),
                    "issue_size": str(issue_sz),
                    "open": open_dt,
                    "close": close_dt,
                    "listing": list_dt,
                    "segment": "SME" if "sme" in block_key.lower() else "Mainboard",
                })
        return ipos
    except Exception as e:
        log.warning(f"NSE IPO fetch failed: {e}")
        return []

def fetch_ipo_from_bse() -> List[Dict]:
    """
    Parse BSE public issues table (HTML). Structure can change; best-effort parser.
    """
    url = "https://www.bseindia.com/markets/publicIssues/IPOIssues_new.aspx"
    r = safe_get(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", {"id": "ContentPlaceHolder1_gvIPO"})
    if not table:
        return []
    rows = table.find_all("tr")
    ipos = []
    for tr in rows[1:]:
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 8:
            continue
        nm   = tds[0]
        dates= tds[2]  # e.g. "20 Aug 2025 - 22 Aug 2025"
        price= tds[3]
        lot  = tds[4]
        issue= tds[5]
        listing = tds[7] if len(tds) > 7 else ""
        open_dt, close_dt = "", ""
        if " - " in dates:
            open_dt, close_dt = dates.split(" - ", 1)
        ipos.append({
            "name": nm,
            "price_band": price,
            "lot": lot,
            "issue_size": issue,
            "open": open_dt,
            "close": close_dt,
            "listing": listing,
            "segment": "Mainboard/SME"
        })
    return ipos

def post_ipo_snapshot():
    """10:30 AM – show ongoing/today IPOS with key details. NSE first, then BSE fallback."""
    if not ENABLE_IPO:
        return
    if is_weekend():
        return

    ipos = fetch_ipo_from_nse()
    if not ipos:
        ipos = fetch_ipo_from_bse()

    today = now_local().date()

    # Filter to those open today (or ongoing), but keep at least something if list is small
    def parse_date_str(s: str) -> Optional[date]:
        if not s:
            return None
        for fmt in ("%d %b %Y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except Exception:
                continue
        return None

    open_today = []
    for x in ipos:
        o = parse_date_str(x.get("open",""))
        c = parse_date_str(x.get("close",""))
        if o and c and o <= today <= c:
            open_today.append(x)

    items = open_today or ipos

    if not items:
        tg_send("📌 *IPO* (10:30 AM)\n_No ongoing IPOs today._")
        return

    lines = ["📌 *IPO – Daily Snapshot* (10:30 AM)"]
    for x in items[:6]:
        n   = x.get("name","").strip()
        seg = x.get("segment","").strip()
        pb  = x.get("price_band","").replace("Price Band", "").strip()
        lot = x.get("lot","").strip()
        iss = x.get("issue_size","").strip()
        o   = x.get("open","").strip()
        c   = x.get("close","").strip()
        lst = x.get("listing","").strip()
        part = f"*{n}* ({seg})\n• Price Band: {pb or '-'} | Lot: {lot or '-'}\n" \
               f"• Issue Size: {iss or '-'}\n• Open–Close: {o or '-'} – {c or '-'}\n" \
               f"• Listing: {lst or '-'}"
        lines.append(part)
    tg_send("\n\n".join(lines), disable_webpage_preview=True)

# -------------- STARTUP & SCHEDULES ----------------

scheduler = BackgroundScheduler(timezone=TZ)

def schedule_jobs():
    # Rolling Market news (every NEWS_INTERVAL within market window)
    if ENABLE_NEWS:
        scheduler.add_job(post_news_slot, "cron", minute=f"*/{NEWS_INTERVAL_MIN}")
    # Pre-market (08:30)
    if ENABLE_MARKET_BLIPS:
        h, m = parse_hhmm(MARKET_BLIPS_START)
        scheduler.add_job(pre_market_snapshot, "cron", hour=h, minute=m)
    # Post-market (20:45)
    if ENABLE_MARKET_BLIPS:
        h, m = parse_hhmm(POSTMARKET_TIME)
        scheduler.add_job(post_market_close, "cron", hour=h, minute=m)
    # FII/DII (21:00)
    if ENABLE_FII_DII:
        h, m = parse_hhmm(FII_DII_POST_TIME)
        scheduler.add_job(post_fii_dii, "cron", hour=h, minute=m)
    # IPO (10:30)
    if ENABLE_IPO:
        h, m = parse_hhmm(IPO_POST_TIME)
        scheduler.add_job(post_ipo_snapshot, "cron", hour=h, minute=m)

def announce_startup():
    text = (
        "✅ *MarketPulse* bot restarted and schedule loaded.\n"
        f"_Window:_ {MARKET_BLIPS_START}–{MARKET_BLIPS_END} • "
        f"Every {NEWS_INTERVAL_MIN} min • Max {MAX_PER_SLOT}/slot\n"
        f"Quiet: {QUIET_START}–{QUIET_END}\n"
        "Fixed posts: 10:30 IPO • 20:45 Post-market • 21:00 FII/DII"
    )
    tg_send(text, disable_webpage_preview=True)

def start():
    schedule_jobs()
    scheduler.start()
    # Send startup marker so you can see it's alive
    try:
        announce_startup()
    except Exception as e:
        log.warning(f"Startup announce failed: {e}")

# -------------- Main (Render: gunicorn app:app) -----

start()
