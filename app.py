# app.py
# MarketPulse — combined news + scheduled market posts with retries and holiday check
# Designed to run under Gunicorn: `gunicorn app:app --workers 1 --timeout 180`

import os
import json
import re
import time
import logging
import threading
from datetime import datetime, timedelta, date
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ------------------------ Configuration helpers ------------------------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN         = env("BOT_TOKEN")
CHANNEL_ID        = env("CHANNEL_ID")        # must be -100... for channels
TIMEZONE_NAME     = env("TIMEZONE", "Asia/Kolkata")

NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))
MAX_NEWS_PER_SLOT  = int(env("MAX_NEWS_PER_SLOT", "2"))

# market windows / times (defaults you requested)
MARKET_BLIPS_START = env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END   = env("MARKET_BLIPS_END", "21:30")

PREMARKET_TIME     = env("PREMARKET_TIME", "09:00")
IPO_POST_TIME      = env("IPO_POST_TIME", "11:00")
POSTMARKET_TIME    = env("POSTMARKET_TIME", "16:00")
FII_DII_POST_TIME  = env("FII_DII_POST_TIME", "21:00")

# Poll / retry settings for fixed posts (try repeatedly for this window)
POLL_RETRY_INTERVAL_SEC = int(env("POLL_RETRY_INTERVAL_SEC", "600"))  # 10 minutes
POLL_WINDOW_MIN = int(env("POLL_WINDOW_MIN", "120"))  # try for up to 2 hours after target time

# Keepalive / self-ping interval (minutes)
SELF_PING_INTERVAL_MIN = int(env("SELF_PING_INTERVAL_MIN", "10"))

# safety checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID is required")

# timezone object (pytz so APScheduler works correctly)
TZ = pytz.timezone(TIMEZONE_NAME)

# ------------------------ Logging ------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

# ------------------------ Telegram bot ------------------------
bot = Bot(token=BOT_TOKEN)

# ------------------------ Flask (health + ping) ------------------------
app = Flask(__name__)

# ------------------------ Dedupe state ------------------------
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=2000)

def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            arr = json.load(f)
        for u in arr:
            seen_urls.add(u)
            seen_queue.append(u)
        log.info("Loaded %d seen URLs", len(seen_urls))
    except Exception:
        log.info("No seen file or failed to load")

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

load_seen()

# ------------------------ Utilities ------------------------
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (compatible; MarketPulseBot/1.0; +https://example.com/bot)",
]
def ua():
    return {"User-Agent": UA_POOL[int(time.time()) % len(UA_POOL)]}

def now_local():
    return datetime.now(TZ)

def parse_hhmm(s: str):
    h, m = s.split(":")
    return int(h), int(m)

def within_window(start_str: str, end_str: str, dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    sh, sm = parse_hhmm(start_str); eh, em = parse_hhmm(end_str)
    start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0).time()
    end   = dt.replace(hour=eh, minute=em, second=0, microsecond=0).time()
    t = dt.time()
    if start <= end:
        return start <= t <= end
    # crosses midnight
    return t >= start or t <= end

def in_quiet_hours(dt: Optional[datetime] = None) -> bool:
    # quiet hours around night (if you need)
    return False  # keep simple; modify if you want.

def clean_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt)

def summarize(text: str, limit: int) -> str:
    if not text:
        return ""
    t = clean_text(text)
    if len(t) <= limit:
        return t
    cut = t[:limit]
    idx = max(cut.rfind(". "), cut.rfind("? "), cut.rfind("! "))
    if idx > 0:
        return cut[:idx+1]
    return cut.rstrip() + "…"

# ------------------------ Feeds + news collection ------------------------
FEEDS: Dict[str, List[str]] = {
    "market": [
        "https://www.livemint.com/rss/markets",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.business-standard.com/rss/markets-106.rss",
    ],
    "company": [
        "https://www.livemint.com/rss/companies",
        "https://www.moneycontrol.com/rss/latestnews.xml",
    ],
    "finance": [
        "https://www.livemint.com/rss/economy",
        "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",
    ],
    "global": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
    ],
}

def fetch_feed_entries(url: str, limit=12) -> List[dict]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:limit]:
            title = clean_text(e.get("title", ""))
            link  = e.get("link", "")
            desc  = clean_text(e.get("summary", "") or e.get("description", ""))
            if title and link:
                out.append({"title": title, "link": link, "summary": desc})
        return out
    except Exception as ex:
        log.warning("feed error %s: %s", url, ex)
        return []

def collect_news_batch(max_items: int) -> List[dict]:
    groups = ["market", "company", "finance", "global"]
    results = []
    for g in groups:
        candidates = []
        for u in FEEDS[g]:
            candidates.extend(fetch_feed_entries(u))
        uniq = []
        used = set()
        for c in candidates:
            if not c["link"] or c["link"] in used or c["link"] in seen_urls:
                continue
            used.add(c["link"])
            uniq.append(c)
        results.extend(uniq[:2])
        if len(results) >= max_items:
            break
    if len(results) < max_items:
        pool = []
        for arr in FEEDS.values():
            for u in arr:
                pool.extend(fetch_feed_entries(u))
        more = []
        used = set(x["link"] for x in results)
        for c in pool:
            if not c["link"] or c["link"] in used or c["link"] in seen_urls:
                continue
            used.add(c["link"])
            more.append(c)
        results.extend(more[: max_items - len(results)])
    return results[:max_items]

# ------------------------ Special fetchers ------------------------
# IPO fetch (chittorgarh) - best effort for mainboard IPOs
def fetch_ongoing_ipos_for_today() -> List[dict]:
    url = "https://www.chittorgarh.com/ipo/ipo_calendar.asp"
    try:
        r = requests.get(url, headers=ua(), timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        rows = []
        for tbl in tables:
            if "IPO" in tbl.get_text(" "):
                for tr in tbl.select("tr"):
                    tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
                    if len(tds) >= 5:
                        rows.append(tds)
        found = []
        today = now_local().date()
        from datetime import datetime as _dt
        for tds in rows:
            line = " | ".join(tds)
            m = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", line)
            if len(m) >= 2:
                try:
                    op = _dt.strptime(m[0], "%d %b %Y").date()
                    cl = _dt.strptime(m[1], "%d %b %Y").date()
                    if op <= today <= cl:
                        company = tds[0]
                        band = ""
                        for x in tds:
                            if "₹" in x and "-" in x:
                                band = x; break
                        lot = ""
                        for x in tds:
                            if "Lot" in x or "Shares" in x:
                                lot = x; break
                        found.append({"company": company, "open": op.strftime("%d %b"), "close": cl.strftime("%d %b"), "band": band, "lot": lot})
                except Exception:
                    continue
        return found
    except Exception as ex:
        log.warning("IPO fetch failed: %s", ex)
        return []

# FII/DII fetch — best-effort from MoneyControl; returns {"fii": int, "dii": int} or None
def fetch_fii_dii_cash() -> Optional[dict]:
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    try:
        r = requests.get(url, headers=ua(), timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        rows = []
        for tr in table.find_all("tr"):
            tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
            if len(tds) >= 4 and re.search(r"\d{1,2}\-\d{1,2}\-\d{4}", " ".join(tds)):
                rows.append(tds)
        if not rows:
            return None
        latest = rows[0]
        flat = " | ".join(latest)
        m = re.findall(r"Net\s*:?[\s₹]*([-+]?\d[\d,]*)", flat)
        if len(m) >= 2:
            try:
                fii = int(m[0].replace(",", ""))
                dii = int(m[1].replace(",", ""))
                return {"fii": fii, "dii": dii}
            except Exception:
                pass
        nums = re.findall(r"[-+]?\d[\d,]*", flat)
        if len(nums) >= 2:
            try:
                return {"fii": int(nums[-2].replace(",", "")), "dii": int(nums[-1].replace(",", ""))}
            except Exception:
                pass
        return None
    except Exception as ex:
        log.warning("FII/DII fetch failed: %s", ex)
        return None

# Post-market snapshot via Yahoo Quote API (best-effort)
def fetch_close_snapshot() -> Optional[dict]:
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=ua(), timeout=12)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        if not data:
            return None
        def fmt(x):
            return round(x, 2) if (isinstance(x, (int, float))) else None
        mp = {}
        for q in data:
            sym = q.get("symbol")
            mp[sym] = {"price": fmt(q.get("regularMarketPrice")),
                       "change": fmt(q.get("regularMarketChange")),
                       "pct": fmt(q.get("regularMarketChangePercent"))}
        return mp or None
    except Exception as ex:
        log.warning("close snapshot failed: %s", ex)
        return None

# Optionally fetch SGX/FT/futures data for premarket (best-effort)
def fetch_sgx_nifty() -> Optional[dict]:
    # best-effort: try Yahoo or other endpoints; if fails return None
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        # SGX Nifty often symbol is "^NSEI" for India; SGX symbol may differ and be hard to standardize.
        r = requests.get(url, params={"symbols": "%5ENSEI"}, headers=ua(), timeout=10)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        if data:
            q = data[0]
            return {"price": q.get("regularMarketPrice"), "change": q.get("regularMarketChange"), "pct": q.get("regularMarketChangePercent")}
    except Exception:
        pass
    return None

# ------------------------ Sending helper ------------------------
def send_text(text: str, button_url: Optional[str] = None, button_text: str = "Read more"):
    try:
        markup = None
        if button_url:
            kb = [[InlineKeyboardButton(button_text, url=button_url)]]
            markup = InlineKeyboardMarkup(kb)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=markup)
    except Exception as ex:
        log.warning("Telegram send failed: %s", ex)

# ------------------------ Scheduler jobs ------------------------
def post_news_slot():
    now = now_local()
    # news runs 7 days a week (user requested)
    if not within_window(MARKET_BLIPS_START, MARKET_BLIPS_END, now):
        log.info("news: outside window -> skip")
        return
    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news: nothing to post")
        return
    posted = 0
    for it in items:
        if it["link"] in seen_urls:
            continue
        title = it["title"] or "Market update"
        summary = summarize(it["summary"], NEWS_SUMMARY_CHARS)
        text = f"<b>{title}</b>\n\n{summary}"
        send_text(text, button_url=it["link"], button_text="Read more →")
        seen_urls.add(it["link"])
        seen_queue.append(it["link"])
        posted += 1
        time.sleep(1)
    if posted:
        save_seen()
    log.info("news_slot posted %d items", posted)

# Helper: is trading day? weekday + not in holiday set
HOLIDAYS = set()
HOLIDAYS_LAST_REFRESH = None

def fetch_nse_holidays() -> set:
    """
    Try fetch NSE holiday JSON; if fails, attempt a raw GitHub fallback that you can update;
    final fallback: empty set (no holidays).
    Returns set of date strings YYYY-MM-DD.
    """
    global HOLIDAYS_LAST_REFRESH
    try:
        # Try official NSE API (best-effort)
        # Note: endpoint details may change; many regions block programmatic requests.
        urls = [
            "https://www.nseindia.com/api/holiday-master?type=trading",  # possible endpoint (may require headers)
            # fallback raw file in your GitHub (you can provide your own)
            "https://raw.githubusercontent.com/nandan-collab/marketpulse-data/main/nse-holidays-2025.json"
        ]
        out = set()
        for u in urls:
            try:
                r = requests.get(u, headers=ua(), timeout=10)
                if r.status_code != 200:
                    continue
                j = r.json()
                # official NSE: may have 'data' with keys for 'trading' etc; try to find dates
                if isinstance(j, dict):
                    # try multiple structures
                    if "holidayDates" in j:
                        for d in j["holidayDates"]:
                            out.add(d.get("date"))
                    elif "data" in j and isinstance(j["data"], list):
                        # entries may have 'date'
                        for item in j["data"]:
                            if isinstance(item, dict) and "date" in item:
                                out.add(item["date"])
                            elif isinstance(item, str) and re.match(r"\d{4}-\d{2}-\d{2}", item):
                                out.add(item)
                    else:
                        # try scanning for yyyy-mm-dd strings anywhere
                        dates = re.findall(r"\d{4}-\d{2}-\d{2}", json.dumps(j))
                        for d in dates:
                            out.add(d)
                # if it succeeded return set
                if out:
                    HOLIDAYS_LAST_REFRESH = datetime.utcnow()
                    return out
            except Exception:
                continue
        # final fallback: empty (we will behave as "no holidays found")
        HOLIDAYS_LAST_REFRESH = datetime.utcnow()
        return set()
    except Exception as ex:
        log.warning("holiday fetch failed: %s", ex)
        return set()

def is_trading_day(dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    if dt.weekday() >= 5:
        return False
    # refresh holiday list once per day
    global HOLIDAYS, HOLIDAYS_LAST_REFRESH
    if HOLIDAYS_LAST_REFRESH is None or (datetime.utcnow() - HOLIDAYS_LAST_REFRESH).total_seconds() > 24*3600:
        try:
            HOLIDAYS = fetch_nse_holidays()
            log.info("Holidays refreshed (%d)", len(HOLIDAYS))
        except Exception as e:
            log.warning("Failed to refresh holidays: %s", e)
    ymd = dt.strftime("%Y-%m-%d")
    return ymd not in HOLIDAYS

# Common pattern: do repeated polls for up to POLL_WINDOW_MIN minutes
def attempt_with_polling(target_fn, window_minutes=POLL_WINDOW_MIN, interval_seconds=POLL_RETRY_INTERVAL_SEC):
    """
    Call target_fn() repeatedly until it returns truthy data or window expires.
    Returns the data from target_fn or None.
    """
    start = now_local()
    deadline = start + timedelta(minutes=window_minutes)
    attempt = 0
    while now_local() <= deadline:
        attempt += 1
        try:
            res = target_fn()
            if res:
                log.info("attempt_with_polling: success on attempt %d", attempt)
                return res
        except Exception as ex:
            log.warning("attempt_with_polling: attempt %d failed: %s", attempt, ex)
        time.sleep(interval_seconds)
    log.info("attempt_with_polling: window expired after %d attempts", attempt)
    return None

def post_pre_market():
    # run only on trading days
    if not is_trading_day():
        log.info("pre-market: not a trading day -> skip")
        return
    # We'll try to get a concise premarket snapshot: SGX (if available) + top cues
    def _fetch():
        # try SGX / futures via fetch_sgx_nifty()
        sgx = fetch_sgx_nifty()
        # collect 4 latest headlines for context
        headlines = collect_news_batch(4)
        bullets = []
        if sgx and sgx.get("price") is not None:
            bullets.append(f"Gift Nifty: {sgx['price']} ({sgx.get('pct'):+}%)")
        for h in headlines[:4]:
            bullets.append(h["title"])
        if bullets:
            return {"bullets": bullets}
        return None

    data = attempt_with_polling(_fetch)
    if not data:
        log.info("pre-market: no reliable data found -> skip")
        return
    text = "📈 <b>[Pre-Market Brief]</b>\n\nKey overnight / early cues:\n"
    text += "\n".join([f"• {b}" for b in data["bullets"]])
    send_text(text)
    log.info("posted pre-market")

def post_ipo_snapshot():
    if not is_trading_day():
        log.info("IPO: not a trading day -> skip")
        return
    def _fetch():
        ipos = fetch_ongoing_ipos_for_today()
        return ipos if ipos else None
    data = attempt_with_polling(_fetch)
    if not data:
        log.info("IPO: nothing found -> skip")
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in data[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        lines.append(seg)
    send_text("\n".join(lines))
    log.info("posted IPO snapshot")

def post_post_market():
    if not is_trading_day():
        log.info("post-market: not a trading day -> skip")
        return
    data = attempt_with_polling(fetch_c
