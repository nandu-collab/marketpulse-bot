# app.py
# MarketPulse: news + premarket/postmarket/IPO/FII-DII with poller + dedupe
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

# ----------------- config helpers -----------------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")   # -100...
TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")

# Normal news (unchanged behaviour)
ENABLE_NEWS = env("ENABLE_NEWS", "1") == "1"
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))

# Quiet hours
QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = env("QUIET_HOURS_END", "07:30")

# Market windows
MARKET_BLIPS_START = env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END = env("MARKET_BLIPS_END", "21:30")

# Poller (dynamic retries for fixed posts)
POLL_INTERVAL_MIN = int(env("POLL_INTERVAL_MIN", "10"))  # minutes between poll attempts

# Pre/post/IPO/FII windows (local times)
PREMARKET_WINDOW_START = env("PREMARKET_WINDOW_START", "08:15")
PREMARKET_WINDOW_END = env("PREMARKET_WINDOW_END", "09:10")

POSTMARKET_WINDOW_START = env("POSTMARKET_WINDOW_START", "15:30")
POSTMARKET_WINDOW_END = env("POSTMARKET_WINDOW_END", "18:30")

IPO_WINDOW_START = env("IPO_WINDOW_START", "10:00")
IPO_WINDOW_END = env("IPO_WINDOW_END", "12:30")

FII_WINDOW_START = env("FII_WINDOW_START", "20:30")
FII_WINDOW_END = env("FII_WINDOW_END", "22:30")

# Safety checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID is missing in environment")

# timezone object (pytz)
TZ = pytz.timezone(TIMEZONE_NAME)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

# Telegram
bot = Bot(token=BOT_TOKEN)

# Flask (health)
app = Flask(__name__)

# ------------ persistence / dedupe --------------
SEEN_FILE = "/tmp/mpulse_seen.json"
FLAGS_FILE = "/tmp/mpulse_flags.json"   # store per-day posted flags so we don't repost

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
        pass

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

# Flags: store daily booleans e.g. {"2025-09-12": {"pre": True, "post": False, "ipo": False, "fii": True}}
flags = {}

def load_flags():
    global flags
    try:
        with open(FLAGS_FILE, "r", encoding="utf-8") as f:
            flags = json.load(f)
    except Exception:
        flags = {}

def save_flags():
    try:
        with open(FLAGS_FILE, "w", encoding="utf-8") as f:
            json.dump(flags, f)
    except Exception as e:
        log.warning("save_flags failed: %s", e)

def mark_posted(key: str):
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    flags.setdefault(today, {})
    flags[today][key] = True
    # prune old days (keep 7)
    ks = sorted(flags.keys())
    if len(ks) > 7:
        for k in ks[:-7]:
            flags.pop(k, None)
    save_flags()

def was_posted_today(key: str) -> bool:
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    return bool(flags.get(today, {}).get(key))

load_seen()
load_flags()

# --------------- utilities -----------------------
UA = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
}

def now_local():
    return datetime.now(TZ)

def parse_hhmm(s: str):
    h, m = s.split(":")
    return int(h), int(m)

def within_window(start_str: str, end_str: str, dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0).time()
    end = dt.replace(hour=eh, minute=em, second=0, microsecond=0).time()
    t = dt.time()
    if start <= end:
        return start <= t <= end
    # crosses midnight
    return t >= start or t <= end

def in_quiet_hours(dt: Optional[datetime] = None) -> bool:
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END, dt)

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

# --------------- feeds & fetchers ----------------
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
            if not c["link"] or c["link"] in used or c["link"] in seen_urls:
                continue
            used.add(c["link"])
            uniq.append(c)
        results.extend(uniq[:2])
        if len(results) >= max_items:
            break
    # fallback pool if not enough
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

# --------------- special fetchers (best-effort) ---------------
# IPOs: chittorgarh + mint fallback
def fetch_ongoing_ipos_for_today() -> List[dict]:
    candidates = []
    # Primary: chittorgarh (good structured table)
    try:
        url = "https://www.chittorgarh.com/ipo/ipo_calendar.asp"
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
                        lot = ""
                        for x in tds:
                            if "₹" in x and "-" in x:
                                band = x
                                break
                        for x in tds:
                            if "Lot" in x or "Shares" in x:
                                lot = x
                                break
                        candidates.append({"company": company, "open": op.strftime("%d %b"), "close": cl.strftime("%d %b"), "band": band, "lot": lot})
                except Exception:
                    continue
    except Exception as ex:
        log.warning("IPO primary fetch failed: %s", ex)
    # fallback: search Mint headlines for "IPO" (less structured)
    if not candidates:
        try:
            url = "https://www.livemint.com/search?query=IPO"
            r = requests.get(url, headers=UA, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                hits = soup.select("article a")
                for a in hits[:6]:
                    t = clean_text(a.get_text(" "))
                    href = a.get("href")
                    if href and "http" not in href:
                        href = "https://www.livemint.com" + href
                    candidates.append({"company": t, "open": "", "close": "", "band": "", "lot": "", "link": href})
        except Exception as ex:
            log.warning("IPO fallback failed: %s", ex)
    return candidates

# FII/DII: moneycontrol best-effort
def fetch_fii_dii_cash() -> Optional[dict]:
    # Moneycontrol page might block; we try and parse table
    urls = [
        "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php",
        # fallback pages could be added
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=UA, timeout=12)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                continue
            rows = []
            for tr in table.find_all("tr"):
                tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
                if len(tds) >= 3 and re.search(r"\d{1,2}\-\d{1,2}\-\d{4}", " ".join(tds)):
                    rows.append(tds)
            if not rows:
                continue
            latest = rows[0]
            flat = " | ".join(latest)
            m = re.findall(r"Net\s*:?[\s₹]*([-+]?\d[\d,]*)", flat)
            if len(m) >= 2:
                fii = int(m[0].replace(",", ""))
                dii = int(m[1].replace(",", ""))
                return {"fii": fii, "dii": dii}
            # fallback numeric extraction
            nums = re.findall(r"[-+]?\d[\d,]*", flat)
            if len(nums) >= 2:
                return {"fii": int(nums[-2].replace(",", "")), "dii": int(nums[-1].replace(",", ""))}
        except Exception as ex:
            log.warning("FII/DII fetch attempt failed (%s): %s", url, ex)
            continue
    return None

# Indices snapshot: try Yahoo, fallback to Moneycontrol quick snapshots
def fetch_indices_snapshot() -> Optional[dict]:
    # try Yahoo finance-ish API
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=UA, timeout=10)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        mp = {}
        for q in data:
            symbol = q.get("symbol")
            if not symbol:
                continue
            price = q.get("regularMarketPrice")
            change = q.get("regularMarketChange")
            pct = q.get("regularMarketChangePercent")
            mp[symbol] = {"price": round(price, 2) if price is not None else None,
                          "change": round(change, 2) if change is not None else None,
                          "pct": round(pct, 2) if pct is not None else None}
        if mp:
            return mp
    except Exception as ex:
        log.warning("Yahoo snapshot failed: %s", ex)

    # fallback: parse moneycontrol index pages
    try:
        # moneycontrol provides index quick summary; we'll try Nifty page
        url = "https://www.moneycontrol.com/markets/indian-indices"
        r = requests.get(url, headers=UA, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # try to pick up elements that look like index blocks - best-effort
        mp = {}
        # Nifty
        nifty = soup.select_one("div#NIFTY50, div.indexbox")
        # generic approach: search for text "Nifty 50" etc
        text = soup.get_text(" ")
        # crude extractor as fallback
        m = re.search(r"Nifty\s*50\s*[:\-\s]*([\d,\.]+)\s*\(?([+\-]?\d+\.?\d*)", text)
        if m:
            price = float(m.group(1).replace(",", ""))
            ch = float(m.group(2))
            mp["^NSEI"] = {"price": round(price, 2), "change": round(ch, 2), "pct": None}
        return mp or None
    except Exception as ex:
        log.warning("Moneycontrol snapshot failed: %s", ex)
    return None

# --------------- sender --------------------------
def send_text(text: str, buttons: Optional[List[Dict]] = None):
    try:
        markup = None
        if buttons:
            kb = [[InlineKeyboardButton(b["text"], url=b["url"]) for b in row] for row in buttons]
            markup = InlineKeyboardMarkup(kb)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML,
                         disable_web_page_preview=True, reply_markup=markup)
    except Exception as ex:
        log.warning("send_text failed: %s", ex)

# --------------- jobs -----------------------------
def post_news_slot():
    now = now_local()
    if not ENABLE_NEWS:
        return
    if in_quiet_hours(now):
        log.info("news: quiet hours skip")
        return
    if not within_window(MARKET_BLIPS_START, MARKET_BLIPS_END, now):
        log.info("news: outside market window skip")
        return
    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news: no items")
        return
    posted = 0
    for it in items:
        if it["link"] in seen_urls:
            continue
        title = it["title"] or "Market update"
        summary = summarize(it.get("summary", ""), NEWS_SUMMARY_CHARS)
        text = f"<b>{title}</b>\n\n{summary}"
        send_text(text, buttons=[[{"text": "Read more", "url": it["link"]}]])
        seen_urls.add(it["link"])
        seen_queue.append(it["link"])
        posted += 1
        time.sleep(1)
    if posted:
        save_seen()
    log.info("news slot: posted %d items", posted)

# Pre-market: attempt multiple times in window, but only once per day if succeeded
def try_post_pre_market_once():
    if was_posted_today("pre"):
        log.info("pre-market: already posted today")
        return True
    if not within_window(PREMARKET_WINDOW_START, PREMARKET_WINDOW_END):
        log.info("pre-market: not in window")
        return False
    # Build premarket info: try to fetch indices snapshot and some headlines
    snap = fetch_indices_snapshot()
    bullets = []
    if snap:
        n = snap.get("^NSEI")
        if n and n["price"] is not None:
            bullets.append(f"Gift Nifty ~ {n['price']} ({'+' if n['change'] and n['change']>=0 else ''}{n['change'] or '—'})")
    # headlines
    coll = collect_news_batch(4)
    for c in coll[:4]:
        bullets.append(f"• {c['title']}")
    if not snap and not coll:
        # nothing reliable yet -> try again later; do not post
        log.info("pre-market: no reliable data yet, will retry")
        return False
    text = "📈 <b>[Pre-Market Brief]</b>\n\n"
    if bullets:
        text += "\n".join(bullets)
    else:
        text += "Key cues not available."
    send_text(text)
    mark_posted("pre")
    log.info("pre-market: posted")
    return True

# IPO: try until we find any mainboard IPOs (only post when we have structured details)
def try_post_ipo_once():
    if was_posted_today("ipo"):
        log.info("ipo: already posted today")
        return True
    if not within_window(IPO_WINDOW_START, IPO_WINDOW_END):
        log.info("ipo: not in window")
        return False
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        log.info("ipo: none found yet, retrying later")
        return False
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x.get('company','')}</b>"
        if x.get("open") and x.get("close"):
            seg += f" • Open {x['open']} – Close {x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        if x.get("link"):
            seg += f" • [link]"
        lines.append(seg)
    send_text("\n".join(lines))
    mark_posted("ipo")
    log.info("ipo: posted")
    return True

# Post-market: try until we get close snapshot
def try_post_post_market_once():
    if was_posted_today("post"):
        log.info("post-market: already posted today")
        return True
    if not within_window(POSTMARKET_WINDOW_START, POSTMARKET_WINDOW_END):
        log.info("post-market: not in window")
        return False
    snap = fetch_indices_snapshot()
    if not snap:
        log.info("post-market: snapshot not available yet")
        return False
    parts = ["📊 <b>Post-Market — Snapshot</b>"]
    mapping = [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]
    got_any = False
    for sym, label in mapping:
        q = snap.get(sym)
        if q and q.get("price") is not None:
            ch = f"{q['change']:+}" if q['change'] is not None else "—"
            pct = f"{q['pct']:+}%" if q.get("pct") is not None else ""
            parts.append(f"{label}: {q['price']} ({ch} | {pct})")
            got_any = True
    if not got_any:
        log.info("post-market: no index values extracted")
        return False
    send_text("\n".join(parts))
    mark_posted("post")
    log.info("post-market: posted")
    return True

# FII/DII: try until fetch
def try_post_fii_once():
    if was_posted_today("fii"):
        log.info("fii/dii: already posted today")
        return True
    if not within_window(FII_WINDOW_START, FII_WINDOW_END):
        log.info("fii/dii: not in window")
        return False
    data = fetch_fii_dii_cash()
    if not data:
        log.info("fii/dii: data not available yet")
        return False
    text = (f"🏦 <b>FII/DII — Cash</b>\n"
            f"FII: {data['fii']:+,} cr\n"
            f"DII: {data['dii']:+,} cr\n"
            "<i>Provisional</i>")
    send_text(text)
