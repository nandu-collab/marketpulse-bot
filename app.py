# app.py
"""
MarketPulse bot
- Hourly news (2 items per slot) from Mint / Moneycontrol / ET / BS (RSS)
- Pre-market, Post-market, IPO, FII/DII: retry-until-success windows, only post when data validated
- No "No data available" posts: skip until validated; post later when data appears
- Dedupe across restarts via /tmp/mpulse_seen.json and per-day posted flags
- Flask health endpoint for uptime checks (Render / UptimeRobot)
"""

import os
import re
import json
import time
import logging
import textwrap
from datetime import datetime, timedelta, date, time as dt_time
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd

from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# ---------- CONFIG ----------
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")
TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")

# News defaults (unchanged style)
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))

# Quiet / market windows
QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = env("QUIET_HOURS_END", "07:30")
NEWS_WINDOW_START = env("NEWS_WINDOW_START", "08:30")  # first news slot
NEWS_WINDOW_END = env("NEWS_WINDOW_END", "21:30")      # last news slot

# Retry windows (times are local TZ)
PREMARKET_WINDOW = (dt_time(8, 15), dt_time(9, 10))      # start, end
POSTMARKET_WINDOW = (dt_time(15, 40), dt_time(18, 0))
IPO_WINDOW = (dt_time(10, 0), dt_time(12, 0))
FII_DII_WINDOW = (dt_time(20, 0), dt_time(22, 0))

# Minimum sanity checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN required in environment")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID required in environment")

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("marketpulse")

# ---------- timezone ----------
TZ = pytz.timezone(TIMEZONE_NAME)

# ---------- bot and flask ----------
bot = Bot(token=BOT_TOKEN)
app = Flask(__name__)

# ---------- dedupe and posted flags ----------
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=2000)

POSTED_MARKS_FILE = "/tmp/mpulse_posted.json"
posted_marks = {}  # category -> date string (YYYY-MM-DD)

def load_seen():
    try:
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for u in arr:
                seen_urls.add(u)
                seen_queue.append(u)
            log.info("Loaded %d seen URLs", len(seen_urls))
    except Exception as e:
        log.warning("load_seen error: %s", e)

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen error: %s", e)

def load_posted_marks():
    global posted_marks
    try:
        if os.path.exists(POSTED_MARKS_FILE):
            with open(POSTED_MARKS_FILE, "r", encoding="utf-8") as f:
                posted_marks = json.load(f)
            log.info("Loaded posted marks")
    except Exception as e:
        log.warning("load_posted_marks error: %s", e)

def save_posted_marks():
    try:
        with open(POSTED_MARKS_FILE, "w", encoding="utf-8") as f:
            json.dump(posted_marks, f)
    except Exception as e:
        log.warning("save_posted_marks error: %s", e)

load_seen()
load_posted_marks()

# ---------- helpers ----------
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

def now_local():
    return datetime.now(TZ)

def today_str():
    return now_local().strftime("%Y-%m-%d")

def parse_hhmm(s: str):
    h, m = s.split(":"); return int(h), int(m)

def within_window(start_str, end_str, dt=None):
    dt = dt or now_local()
    sh, sm = parse_hhmm(start_str); eh, em = parse_hhmm(end_str)
    start = TZ.localize(datetime(dt.year, dt.month, dt.day, sh, sm))
    end = TZ.localize(datetime(dt.year, dt.month, dt.day, eh, em))
    if start <= end:
        return start <= dt <= end
    return dt >= start or dt <= end

def time_within_range(t: dt_time, window):
    start, end = window
    if start <= end:
        return start <= t <= end
    return t >= start or t <= end

def clean_text(html_text: str) -> str:
    if not html_text: return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()

def summarize(text: str, limit: int) -> str:
    txt = clean_text(text)
    if len(txt) <= limit:
        return txt
    cut = txt[:limit]
    idx = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if idx >= 0:
        return cut[:idx+1].strip()
    return cut.rstrip() + "…"

def send_message(text: str, button_url: Optional[str]=None, button_text="Read more →"):
    if not text or not text.strip():
        log.warning("send_message: empty text skipped")
        return False
    try:
        if button_url:
            kb = [[InlineKeyboardButton(button_text, url=button_url)]]
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(kb))
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        log.info("Posted to channel")
        return True
    except Exception as ex:
        log.exception("Telegram send failed: %s", ex)
        return False

# ---------- RSS sources (news) ----------
NEWS_SOURCES = [
    ("Mint", "https://www.livemint.com/rss/markets"),
    ("Moneycontrol", "https://www.moneycontrol.com/rss/MCtopnews.xml"),
    ("EconomicTimes", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("BusinessStandard", "https://www.business-standard.com/finance-news/rss"),
    ("Reuters", "https://feeds.reuters.com/reuters/businessNews"),
]

def fetch_latest_items(limit_per_source=6):
    items = []
    for name, url in NEWS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:limit_per_source]:
                title = clean_text(e.get("title", ""))
                link  = e.get("link", "")
                desc  = e.get("summary", "") or e.get("description", "")
                uid   = e.get("id") or e.get("guid") or link
                published = e.get("published_parsed") or e.get("updated_parsed")
                items.append({
                    "title": title,
                    "link": link,
                    "summary": summarize(desc, NEWS_SUMMARY_CHARS),
                    "source": name,
                    "uid": f"{name}:{uid}",
                    "published": published
                })
        except Exception as ex:
            log.warning("RSS error %s: %s", name, ex)
    # sort newest first if possible
    items.sort(key=lambda x: x.get("published") or time.gmtime(0), reverse=True)
    return items

def already_seen(uid: str) -> bool:
    return uid in seen_urls

def mark_seen(uid: str):
    seen_urls.add(uid)
    seen_queue.append(uid)
    save_seen()

def post_hourly_news():
    if in_quiet_hours_now():
        log.info("news: quiet hours -> skip")
        return
    if not within_window(NEWS_WINDOW_START, NEWS_WINDOW_END):
        log.info("news: outside window -> skip")
        return
    items = fetch_latest_items(limit_per_source=4)
    posted = 0
    for it in items:
        uid = it["uid"]
        if uid in seen_urls:
            continue
        title = it["title"]
        summary = it["summary"]
        link = it["link"]
        body = f"📰 <b>{title}</b>\n\n{summary}"
        ok = send_message(body, button_url=link)
        if ok:
            mark_seen(uid)
            posted += 1
        if posted >= MAX_NEWS_PER_SLOT:
            break
    log.info("news slot done, posted=%d", posted)

# ---------- Utilities: pivot/support/resistance calculation using past candles ----------
def compute_pivots_from_history(symbol: str):
    """
    Use yfinance to fetch last 3 days and compute pivot and S/R from previous day.
    Returns dict with last_price, prev_close, pivot, r1,r2,s1,s2
    """
    try:
        tk = yf.Ticker(symbol)
        # fetch last 3 days of 1d data
        hist = tk.history(period="5d", interval="1d", actions=False)
        if hist is None or hist.empty or len(hist) < 2:
            return None
        # use last two rows: prev day and latest intraday or today
        hist = hist.dropna(subset=["High","Low","Close"])
        if hist.empty:
            return None
        # prev = last complete day
        prev = hist.iloc[-2]
        last = hist.iloc[-1]
        prev_high = float(prev["High"])
        prev_low = float(prev["Low"])
        prev_close = float(prev["Close"])
        # pivot standard formulas
        pivot = (prev_high + prev_low + prev_close) / 3.0
        r1 = 2*pivot - prev_low
        s1 = 2*pivot - prev_high
        r2 = pivot + (prev_high - prev_low)
        s2 = pivot - (prev_high - prev_low)
        # last price from ticker fast info
        last_price = None
        try:
            q = tk.fast_info
            last_price = float(q.get("last_price") or q.get("last_price"))
        except Exception:
            # fallback to today's close if available
            last_price = float(last["Close"]) if "Close" in last else None
        return {
            "prev_high": round(prev_high,2),
            "prev_low": round(prev_low,2),
            "prev_close": round(prev_close,2),
            "pivot": round(pivot,2),
            "r1": round(r1,2),
            "s1": round(s1,2),
            "r2": round(r2,2),
            "s2": round(s2,2),
            "last": round(last_price,2) if last_price is not None else None
        }
    except Exception as ex:
        log.warning("compute_pivots failed for %s: %s", symbol, ex)
        return None

# ---------- Pre-market fetcher (best-effort) ----------
def fetch_premarket_data():
    """
    Attempt to build a pre-market brief:
    - SGX/SGX Nifty via yfinance symbol '^NSEI' (best-effort)
    - compute pivots/support/resistance from previous day via yfinance
    - include key global indices snapshot (S&P, Dow futures) via yfinance tickers
    """
    try:
        # Use Nifty 50 index ticker ^NSEI via yfinance
        nifty_sym = "^NSEI"
        piv = compute_pivots_from_history(nifty_sym)
        # Get premarket snapshot using yfinance history intraday (minute) if available or fast_info
        tk = yf.Ticker(nifty_sym)
        last_price = None
        change = None
        pct = None
        try:
            info = tk.fast_info
            last_price = info.get("last_price")
        except Exception:
            # fallback: try recent market data
            df = tk.history(period="1d", interval="1m")
            if df is not None and not df.empty:
                last_price = float(df["Close"].iloc[-1])
        if last_price is None:
            return None
        # compute change vs prev close if we have prev_close
        prev_close = piv.get("prev_close") if piv else None
        if prev_close:
            change = round(last_price - prev_close, 2)
            try:
                pct = round((change / prev_close) * 100, 2)
            except Exception:
                pct = None
        # Global cues: S&P (ES=ES=F? use ^GSPC), Brent crude, USDINR maybe via tickers
        # We'll fetch a small list: '^GSPC' '^DJI' 'BZ=F' 'INR=X'
        global_ticks = {"S&P 500":"^GSPC", "Dow":"^DJI", "Brent":"BZ=F", "USD/INR":"INR=X"}
        globals_out = {}
        for label, sym in global_ticks.items():
            try:
                tk2 = yf.Ticker(sym)
                li = None
                try:
                    li = tk2.fast_info.get("last_price")
                except Exception:
                    df2 = tk2.history(period="1d", interval="1m")
                    if df2 is not None and not df2.empty:
                        li = float(df2["Close"].iloc[-1])
                if li is not None:
                    globals_out[label] = round(li,2)
            except Exception:
                continue
        # Build output dict
        out = {
            "last": round(float(last_price),2),
            "change": change,
            "pct": pct,
            "pivots": piv,
            "globals": globals_out
        }
        return out
    except Exception as ex:
        log.warning("premarket fetch failed: %s", ex)
        return None

# ---------- Post-market fetcher ----------
def fetch_postmarket_data():
    """
    Try to get closing snapshot for Sensex, Nifty, Bank Nifty using yfinance.
    """
    try:
        symbols = {"^NSEI":"Nifty 50", "^BSESN":"Sensex", "^NSEBANK":"Bank Nifty"}
        out = {}
        for sym, label in symbols.items():
            try:
                tk = yf.Ticker(sym)
                # Try fast_info
                last = None
                try:
                    fi = tk.fast_info
                    last = float(fi.get("last_price") or fi.get("last_price") or None)
                except Exception:
                    hist = tk.history(period="2d", interval="1d")
                    if hist is not None and not hist.empty:
                        last = float(hist["Close"].iloc[-1])
                if last is None:
                    continue
                # Try to compute change vs previous close using history
                hist = tk.history(period="2d", interval="1d")
                prev_close = None
                if hist is not None and len(hist) >= 2:
                    prev_close = float(hist["Close"].iloc[-2])
                change = round(last - prev_close, 2) if prev_close is not None else None
                pct = round((change / prev_close) * 100, 2) if prev_close else None
                out[sym] = {"label": label, "price": round(last,2), "change": change, "pct": pct}
            except Exception as ex:
                log.warning("postmarket symbol fetch fail %s: %s", sym, ex)
                continue
        return out if out else None
    except Exception as ex:
        log.warning("postmarket fetch failed: %s", ex)
        return None

# ---------- IPO fetcher (chittorgarh) ----------
def fetch_ongoing_ipos_chittorgarh():
    url = "https://www.chittorgarh.com/ipo/ipo_calendar.asp"
    try:
        r = requests.get(url, headers=UA, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # find tables containing IPO listings
        tables = soup.find_all("table")
        rows = []
        for tbl in tables:
            text = tbl.get_text(" ")
            if "IPO" in text or "Issue" in text:
                for tr in tbl.select("tr"):
                    tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
                    if len(tds) >= 5:
                        rows.append(tds)
        today = now_local().date()
        found = []
        from datetime import datetime as _dt
        for tds in rows:
            line = " | ".join(tds)
            m = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", line)
            if len(m) >= 2:
                try:
                    open_dt = _dt.strptime(m[0], "%d %b %Y").date()
                    close_dt = _dt.strptime(m[1], "%d %b %Y").date()
                    # only include if open <= today <= close (live)
                    if open_dt <= today <= close_dt:
                        company = tds[0]
                        band = ""
                        gmp = ""
                        lot = ""
                        issue = ""
                        for x in tds:
                            if "₹" in x and "-" in x and ("/" not in x):
                                band = x
                            if "GMP" in x or "gmp" in x.lower():
                                gmp = x
                            if "Lot" in x or "Shares" in x:
                                lot = x
                            if re.search(r"\d+,\d+|\d+ cr|\d+ Crore", x, re.I):
                                issue = issue + " " + x
                        found.append({
                            "company": company,
                            "open": open_dt.strftime("%d %b %Y"),
                            "close": close_dt.strftime("%d %b %Y"),
                            "band": band.strip(),
                            "gmp": gmp.strip(),
                            "lot": lot.strip(),
                            "issue": issue.strip()
                        })
                except Exception:
                    continue
        return found
    except Exception as ex:
        log.warning("IPO fetch failed: %s", ex)
        return []

# ---------- FII/DII fetcher (try NSE then Moneycontrol fallback) ----------
def fetch_fii_dii_nse():
    """
    NSE does not provide a stable public JSON endpoint without proper headers/cookies.
    But we can try their equity market data page which sometimes contains required numbers.
    This is a best-effort attempt: we fetch a few candidate pages and try to parse numbers.
    """
    # candidate NSE pages (best-effort)
    urls = [
        "https://www.nseindia.com/api/market-data-pre-open?key=equities",  # may not exist or need cookie
        "https://www.nseindia.com/live_market/dynaContent/live_watch/stock_watch/niftyStockWatch.htm"
    ]
    headers = {
        "User-Agent": UA["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com"
    }
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=12)
            if r.status_code != 200:
                continue
            text = r.text
            # attempt to find patterns like 'FII net' or numbers with Rupee crores
            nums = re.findall(r"[-+]?\d[\d,]*", text)
            # fallback parsing: find last two large numbers
            if len(nums) >= 2:
                try:
                    fii = int(nums[-2].replace(",", ""))
                    dii = int(nums[-1].replace(",", ""))
                    return {"fii": fii, "dii": dii}
                except Exception:
                    continue
        except Exception:
            continue
    return None

def fetch_fii_dii_moneycontrol():
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    headers = {"User-Agent": UA["User-Agent"], "Referer": "https://www.moneycontrol.com"}
    try:
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        rows = []
        for tr in table.find_all("tr"):
            tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
            if len(tds) >= 3:
                rows.append(tds)
        if not rows:
            return None
        flat = " | ".join(rows[0])
        m = re.findall(r"Net\s*:?[\s₹]*([-+]?\d[\d,]*)", flat)
        if len(m) >= 2:
    try:
        return {"fii": int(m[0].replace(",", "")), "dii": int(m[1].replace(",", ""))}
    except Exception:
        return None

nums = re.findall(r"[-+]?\d[\d,]*", flat)
if len(nums) >= 2:
    try:
        return {"fii": int(nums[-2].replace(",", "")), "dii": int(nums[-1].replace(",", ""))}
    except Exception:
        return None

return None

 
