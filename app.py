# app.py
# MarketPulse — combined news + scheduled market posts + retries
import sys
import types

# Shim for deprecated 'cgi' module (removed in Python 3.13)
if "cgi" not in sys.modules:
    cgi = types.ModuleType("cgi")
    cgi.parse_header = lambda s: (s, {})
    sys.modules["cgi"] = cgi
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

# APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ----------------- CONFIG HELPERS -----------------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN            = env("BOT_TOKEN")
CHANNEL_ID_RAW       = env("CHANNEL_ID")   # "-100..." or "@handle"
TIMEZONE_NAME        = env("TIMEZONE", "Asia/Kolkata")

# Normal news
ENABLE_NEWS          = env("ENABLE_NEWS", "1") == "1"
MAX_NEWS_PER_SLOT    = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS   = int(env("NEWS_SUMMARY_CHARS", "550"))

# Quiet hours (local tz)
QUIET_HOURS_START    = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END      = env("QUIET_HOURS_END", "07:30")

# Market windows
MARKET_BLIPS_START   = env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END     = env("MARKET_BLIPS_END", "21:30")

# Poller (tries dynamic tasks repeatedly)
POLL_INTERVAL_MIN    = int(env("POLL_INTERVAL_MIN", "10"))  # how often poller tries dynamic tasks

# Pre/Post/IPO/FII settings: each has a window (start_time, end_time)
# Pre-market: try between PREMARKET_WINDOW_START and PREMARKET_WINDOW_END (local time)
PREMARKET_WINDOW_START = env("PREMARKET_WINDOW_START", "08:15")  # earliest try
PREMARKET_WINDOW_END   = env("PREMARKET_WINDOW_END", "09:10")    # stop trying after this

# Post-market: try between POSTMARKET_WINDOW_START and POSTMARKET_WINDOW_END
POSTMARKET_WINDOW_START = env("POSTMARKET_WINDOW_START", "15:30")
POSTMARKET_WINDOW_END   = env("POSTMARKET_WINDOW_END", "18:30")

# IPO: try (usually opens 10:00) — try between IPO_WINDOW_START and IPO_WINDOW_END
IPO_WINDOW_START = env("IPO_WINDOW_START", "10:00")
IPO_WINDOW_END   = env("IPO_WINDOW_END", "12:30")

# FII/DII: try between FII_WINDOW_START and FII_WINDOW_END (night)
FII_WINDOW_START = env("FII_WINDOW_START", "20:30")
FII_WINDOW_END   = env("FII_WINDOW_END", "22:30")

# Safety checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment!")
if not CHANNEL_ID_RAW:
    raise RuntimeError("CHANNEL_ID is missing in environment!")

CHANNEL_ID = CHANNEL_ID_RAW

# tz for scheduler — use pytz timezone object to avoid zoneinfo.normalize error
TZ = pytz.timezone(TIMEZONE_NAME)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

bot = Bot(token=BOT_TOKEN)
app = Flask(__name__)

# ----------------- DEDUPE / PERSISTENCE -----------------
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=1200)

def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            arr = json.load(f)
        for u in arr:
            seen_urls.add(u)
            seen_queue.append(u)
        log.info("Loaded %d seen urls", len(seen_urls))
    except Exception:
        pass

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

load_seen()

# ----------------- TIME HELPERS -----------------
def now_local() -> datetime:
    """Return timezone-aware datetime in configured TZ (pytz)."""
    return datetime.now(TZ)

def parse_hhmm(s: str):
    h, m = s.split(":")
    return int(h), int(m)

def time_in_range(start: str, end: str, dt: Optional[datetime] = None) -> bool:
    """Return True if local time is within start..end (handles midnight wrap)."""
    dt = dt or now_local()
    sh, sm = parse_hhmm(start)
    eh, em = parse_hhmm(end)
    start_t = dt.replace(hour=sh, minute=sm, second=0, microsecond=0).time()
    end_t   = dt.replace(hour=eh, minute=em, second=0, microsecond=0).time()
    t = dt.time()
    if start_t <= end_t:
        return start_t <= t <= end_t
    return t >= start_t or t <= end_t

def in_quiet_hours(dt: Optional[datetime] = None) -> bool:
    return time_in_range(QUIET_HOURS_START, QUIET_HOURS_END, dt)

# ----------------- FETCH / CLEAN -----------------
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) MarketPulseBot/1.0"}

def clean_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt)
    return txt

def summarize(text: str, limit: int) -> str:
    if not text:
        return ""
    text = clean_text(text)
    if len(text) <= limit:
        return text
    cut = text[:limit]
    idx = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if idx >= 0:
        return cut[:idx+1]
    return cut.rstrip() + "…"

# ----------------- FEEDS -----------------
FEEDS: Dict[str, List[str]] = {
    "market": [
        "https://www.livemint.com/rss/markets",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.business-standard.com/rss/markets-106.rss",
    ],
    "company": [
        "https://www.livemint.com/rss/companies",
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://www.business-standard.com/rss/companies-101.rss",
    ],
    "finance": [
        "https://www.livemint.com/rss/economy",
        "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",
        "https://www.business-standard.com/rss/economy-policy-110.rss",
    ],
    "global": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
    ],
}

def fetch_feed_entries(url: str, limit: int = 12) -> List[dict]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:limit]:
            title = clean_text(e.get("title", ""))
            link = e.get("link", "")
            desc = clean_text(e.get("summary", "") or e.get("description", ""))
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
            if not c.get("link") or c["link"] in seen_urls or c["link"] in used:
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
            if not c.get("link") or c["link"] in used or c["link"] in seen_urls:
                continue
            used.add(c["link"])
            more.append(c)
        results.extend(more[: max_items - len(results)])
    return results[:max_items]

# ----------------- SPECIAL FETCHERS -----------------
def fetch_ongoing_ipos_for_today() -> List[dict]:
    url = "https://www.chittorgarh.com/ipo/ipo_calendar.asp"
    try:
        r = requests.get(url, headers=UA, timeout=12)
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

def fetch_fii_dii_cash() -> Optional[dict]:
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    try:
        r = requests.get(url, headers=UA, timeout=12)
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
        # fallback: take last two numbers
        nums = re.findall(r"[-+]?\d[\d,]*", flat)
        if len(nums) >= 2:
            try:
                fii = int(nums[-2].replace(",", ""))
                dii = int(nums[-1].replace(",", ""))
                return {"fii": fii, "dii": dii}
            except Exception:
                return None
        return None
    except Exception as ex:
        log.warning("FII/DII fetch failed: %s", ex)
        return None

def fetch_close_snapshot() -> Optional[dict]:
    """
    Try Yahoo finance quote API. Return None on error.
    Structure: {symbol: {"price": float, "change": float, "pct": float}, ...}
    """
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=UA, timeout=12)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        mp = {}
        def fmt(x): return round(x, 2) if x is not None else None
        for q in data:
            mp[q["symbol"]] = {"price": fmt(q.get("regularMarketPrice")), "change": fmt(q.get("regularMarketChange")), "pct": fmt(q.get("regularMarketChangePercent"))}
        return mp or None
    except Exception as ex:
        # frequent 429s / 401s happen — log and return None (we will retry)
        log.warning("close snapshot failed: %s", ex)
        return None

# ----------------- SENDER -----------------
def send_text(text: str, buttons: Optional[List[List[Dict]]] = None, disable_preview: bool = True):
    try:
        markup = None
        if buttons:
            keyboard = [[InlineKeyboardButton(b["text"], url=b["url"]) for b in row] for row in buttons]
            markup = InlineKeyboardMarkup(keyboard)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=disable_preview, reply_markup=markup)
    except Exception as ex:
        log.warning("Telegram send failed: %s", ex)

# ----------------- JOBS (normal news) -----------------
def post_news_slot():
    now = now_local()
    if not ENABLE_NEWS:
        return
    if in_quiet_hours(now):
        log.info("news: quiet hours, skip")
        return
    if not time_in_range(MARKET_BLIPS_START, MARKET_BLIPS_END, now):
        log.info("news: outside window, skip")
        return

    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news: nothing new")
        return

    for it in items:
        if it["link"] in seen_urls:
            continue
        title = it["title"] or "Market update"
        summary = summarize(it["summary"], NEWS_SUMMARY_CHARS)
        text = f"<b>{title}</b>\n\n{summary}"
        try:
            send_text(text, buttons=[[{"text": "Read More", "url": it["link"]}]])
            seen_urls.add(it["link"])
            seen_queue.append(it["link"])
            time.sleep(1)
        except Exception as ex:
            log.warning("send news failed: %s", ex)
    save_seen()
    log.info("news slot done")

# ----------------- DYNAMIC TASKS WITH RETRIES -----------------
# daily state to avoid duplicates
daily_state = {
    "date": None,
    "pre_market_posted": False,
    "post_market_posted": False,
    "ipo_posted": False,
    "fii_dii_posted": False,
}

def daily_reset_if_needed():
    today = now_local().date()
    if daily_state["date"] != today:
        daily_state["date"] = today
        daily_state["pre_market_posted"] = False
        daily_state["post_market_posted"] = False
        daily_state["ipo_posted"] = False
        daily_state["fii_dii_posted"] = False
        log.info("Daily state reset for %s", today)

def try_post_pre_market() -> bool:
    """
    Return True if a pre-market post was successfully published.
    Otherwise False (we will retry until window closes).
    """
    # already posted?
    if daily_state["pre_market_posted"]:
        return True
    # attempt fetch — here we use a simple approach: collect top short headlines plus try to get a Gift Nifty level if possible
    # Gift Nifty approach: many sites publish gift nifty in news item — as fallback we just post the premarket headlines (short).
    coll = collect_news_batch(6)
    if not coll:
        log.info("pre-market: no source headlines yet")
        return False

    bullets = []
    # try to find an item mentioning "Gift Nifty" or "GIFT Nifty"
    gift_line = None
    for c in coll:
        t = c.get("title", "") + " " + c.get("summary", "")
        if "Gift Nifty" in t or "GIFT Nifty" in t or "GiftNifty" in t or "gift nifty" in t.lower():
            gift_line = t
            break

    # Create pre-market message
    # Primary: if gift_line exists, include that; else include top 4 headlines
    if gift_line:
        text = f"📈 <b>[Pre-Market Brief]</b>\n\n{gift_line}"
    else:
        top = coll[:4]
        bullets = "\n".join([f"• {clean_text(x['title'])}" for x in top])
        text = f"📈 <b>[Pre-Market Brief]</b>\n\nKey overnight / early cues:\n{bullets}"

    # Post and mark posted
    try:
        send_text(text)
        daily_state["pre_market_posted"] = True
        log.info("pre-market posted")
        return True
    except Exception as ex:
        log.warning("pre-market send failed: %s", ex)
        return False

def try_post_post_market() -> bool:
    if daily_state["post_market_posted"]:
        return True
    snap = fetch_close_snapshot()
    if not snap:
        log.info("post-market: snapshot not ready")
        return False
    parts = ["📊 <b>Post-Market — Closing Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        q = snap.get(sym)
        if q and q.get("price") is not None:
            chg = f"{q['change']:+}" if q['change'] is not None else "—"
            pct = f"{q['pct']:+}%" if q['pct'] is not None else ""
            parts.append(f"{label}: {q['price']} ({chg} | {pct})")
    if len(parts) <= 1:
        # no useful values, treat as not ready
        log.info("post-market: snapshot had no values (not ready)")
        return False
    try:
        send_text("\n".join(parts))
        daily_state["post_market_posted"] = True
        log.info("post-market posted")
        return True
    except Exception as ex:
        log.warning("post-market send failed: %s", ex)
        return False

def try_post_ipo_snapshot() -> bool:
    if daily_state["ipo_posted"]:
        return True
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        # no IPOs found — don't post "no data" — retry until window end
        log.info("ipo: none found yet")
        return False
    # Format one compact post with available details (company, open/close dates, band/lot)
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        lines.append(seg)
    try:
        send_text("\n".join(lines))
        daily_state["ipo_posted"] = True
        log.info("ipo posted")
        return True
    except Exception as ex:
        log.warning("ipo send failed: %s", ex)
        return False

def try_post_fii_dii() -> bool:
    if daily_state["fii_dii_posted"]:
        return True
    data = fetch_fii_dii_cash()
    if not data:
        log.info("fii/dii: data not ready")
        return False
    try:
        text = f"🏦 <b>FII/DII — Cash</b>\nFII: {data['fii']:+,} cr\nDII: {data['dii']:+,} cr\n<i>Provisional</i>"
        send_text(text)
        daily_state["fii_dii_posted"] = True
        log.info("fii/dii posted")
        return True
    except Exception as ex:
        log.warning("fii/dii send failed: %s", ex)
        return False

# Poller: runs every POLL_INTERVAL_MIN and attempts dynamic tasks within their windows
def poll_dynamic():
    daily_reset_if_needed()
    now = now_local()
    # PREMARKET: try if within window and not posted
    if not daily_state["pre_market_posted"] and time_in_range(PREMARKET_WINDOW_START, PREMARKET_WINDOW_END, now) and not in_quiet_hours(now):
        try_post_pre_market()

    # POSTMARKET: try if within window (and only on weekdays)
    if not daily_state["post_market_posted"] and now.weekday() < 5 and time_in_range(POSTMARKET_WINDOW_START, POSTMARKET_WINDOW_END, now):
        try_post_post_market()

    # IPO
    if not daily_state["ipo_posted"] and now.weekday() < 5 and time_in_range(IPO_WINDOW_START, IPO_WINDOW_END, now):
        try_post_ipo_snapshot()

    # FII/DII
    if not daily_state["fii_dii_posted"] and now.weekday() < 5 and time_in_range(FII_WINDOW_START, FII_WINDOW_END, now):
        try_post_fii_dii()

# ----------------- SCHEDULER SETUP -----------------
scheduler = BackgroundScheduler(timezone=TZ, job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300})

def schedule_jobs():
    # News: every hour at :30 between 08-21 (as you wanted)
    scheduler.add_job(post_news_slot, trigger=CronTrigger(hour="8-21", minute=30, timezone=TZ), id="post_news_slot", replace_existing=True)

    # Poller dynamic tasks: runs every POLL_INTERVAL_MIN minutes (always) to attempt missing posts
    scheduler.add_job(poll_dynamic, trigger=IntervalTrigger(minutes=POLL_INTERVAL_MIN), id="poll_dynamic", replace_existing=True)

schedule_jobs()
scheduler.start()
log.info
