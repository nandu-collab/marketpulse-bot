# app.py — full working version (fixed hourly schedule)
import os, json, re, logging, threading
from datetime import datetime
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify

# --- Telegram (PTB 13.x) ---
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# =============== CONFIG HELPERS ===============
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

# =============== ENV ===============
BOT_TOKEN            = env("BOT_TOKEN")
CHANNEL_ID           = env("CHANNEL_ID")  # "-100..." or "@yourchannel"
TIMEZONE_NAME        = env("TIMEZONE", "Asia/Kolkata")

# content settings
MAX_NEWS_PER_SLOT    = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS   = int(env("NEWS_SUMMARY_CHARS", "550"))

# quiet hours (skip news)
QUIET_HOURS_START    = env("QUIET_HOURS_START", "22:30")   # HH:MM local
QUIET_HOURS_END      = env("QUIET_HOURS_END", "07:30")

# feature toggles + fixed times
ENABLE_IPO           = env("ENABLE_IPO", "1") == "1"
IPO_POST_TIME        = env("IPO_POST_TIME", "11:00")

ENABLE_MARKET_BLIPS  = env("ENABLE_MARKET_BLIPS", "1") == "1"
PREMARKET_TIME       = env("PREMARKET_TIME", "09:00")
POSTMARKET_TIME      = env("POSTMARKET_TIME", "16:00")     # set to "17:00" if you want 5 pm

ENABLE_FII_DII       = env("ENABLE_FII_DII", "1") == "1"
FII_DII_POST_TIME    = env("FII_DII_POST_TIME", "21:00")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment!")

# =============== GLOBALS ===============
TZ = pytz.timezone(TIMEZONE_NAME)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

bot = Bot(token=BOT_TOKEN)
app = Flask(__name__)

# ========== DEDUPE ==========
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=1200)

def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for u in data:
            seen_urls.add(u)
            seen_queue.append(u)
        log.info(f"Loaded {len(seen_urls)} seen URLs")
    except Exception:
        pass
load_seen()

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning(f"save_seen failed: {e}")

# ========== TIME UTILS ==========
def now_local():
    return datetime.now(TZ)

def parse_hhmm(s: str):
    return datetime.strptime(s, "%H:%M").time()

def is_weekday(dt=None):
    d = dt or now_local()
    return d.weekday() < 5  # Mon=0..Sun=6

def within_window(start_str, end_str, dt=None):
    dt = dt or now_local()
    start = parse_hhmm(start_str)
    end = parse_hhmm(end_str)
    t = dt.time()
    if start <= end:
        return start <= t <= end
    return t >= start or t <= end  # crosses midnight

def in_quiet_hours(dt=None):
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END, dt)

# ========== FETCHERS ==========
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
        "https://www.aljazeera.com/xml/rss/all.xml",
    ],
}

UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

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
        cut = cut[:idx+1]
    return cut

def fetch_feed_entries(url: str) -> List[dict]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:15]:
            title = clean_text(e.get("title", ""))
            link  = e.get("link", "")
            desc  = clean_text(e.get("summary", "") or e.get("description", ""))
            out.append({"title": title, "link": link, "summary": desc})
        return out
    except Exception as ex:
        log.warning(f"feed error {url}: {ex}")
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
            if not c["link"] or c["link"] in seen_urls or c["link"] in used:
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

def fetch_ongoing_ipos_for_today() -> List[dict]:
    url = "https://www.chittorgarh.com/ipo/ipo_calendar.asp"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        rows = []
        for tbl in tables:
            if "IPO" in tbl.get_text(" "):
                for tr in tbl.select("tr"):
                    tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
                    if len(tds) >= 6:
                        rows.append(tds)
        today = now_local().date()
        found = []
        for tds in rows:
            line = " | ".join(tds)
            try:
                m = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", line)
                if len(m) >= 2:
                    from datetime import datetime as _dt
                    open_dt = _dt.strptime(m[0], "%d %b %Y").date()
                    close_dt = _dt.strptime(m[1], "%d %b %Y").date()
                    if open_dt <= today <= close_dt:
                        company = tds[0]
                        band = next((x for x in tds if "₹" in x and "-" in x), "")
                        lot  = next((x for x in tds if ("Lot" in x or "Shares" in x)), "")
                        found.append({
                            "company": company,
                            "open": open_dt.strftime("%d %b"),
                            "close": close_dt.strftime("%d %b"),
                            "band": band or "",
                            "lot": lot or "",
                            "source": url
                        })
            except Exception:
                continue
        return found
    except Exception as ex:
        log.warning(f"IPO fetch failed: {ex}")
        return []

def fetch_fii_dii_cash() -> Optional[dict]:
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        rows = []
        for tr in table.find_all("tr"):
            tds = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]
            if len(tds) >= 5 and re.search(r"\d{1,2}\-\d{1,2}\-\d{4}", tds[0]):
                rows.append(tds)
        if not rows:
            return None
        latest = rows[0]
        flat = " | ".join(latest)
        m = re.findall(r"Net\s*:?[\s₹]*([-+]?\d[\d,]*)", flat)
        if len(m) >= 2:
            fii_net = m[0].replace(",", "")
            dii_net = m[1].replace(",", "")
            return {"fii": int(fii_net), "dii": int(dii_net)}
        nums = [int(x.replace(",", "")) for x in re.findall(r"[-+]?\d[\d,]*", flat)[-2:]]
        if len(nums) == 2:
            return {"fii": nums[0], "dii": nums[1]}
        return None
    except Exception as ex:
        log.warning(f"FII/DII fetch failed: {ex}")
        return None

def fetch_close_snapshot() -> Optional[dict]:
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=UA, timeout=12)
        r.raise_for_status()
        data = r.json()["quoteResponse"]["result"]
        def fmt(x):
            if x is None:
                return None
            return round(x, 2)
        mp = {}
        for q in data:
            name = q.get("shortName") or q.get("symbol")
            mp[q["symbol"]] = {
                "name": name,
                "price": fmt(q.get("regularMarketPrice")),
                "change": fmt(q.get("regularMarketChange")),
                "pct": fmt(q.get("regularMarketChangePercent")),
            }
        return mp if mp else None
    except Exception as ex:
        log.warning(f"close snapshot failed: {ex}")
        return None

# ========== SENDER ==========
def send_text(text: str, buttons: Optional[List[List[Dict]]] = None):
    markup = None
    if buttons:
        keyboard = [[InlineKeyboardButton(b["text"], url=b["url"]) for b in row] for row in buttons]
        markup = InlineKeyboardMarkup(keyboard)
    bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=markup
    )

# ========== JOBS ==========
def post_news_slot():
    now = now_local()
    if in_quiet_hours(now):
        log.info("news: quiet hours, skip")
        return
    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news: nothing new")
        return
    for it in items:
        link = it["link"]
        if not link or link in seen_urls:
            continue
        title = it["title"] or "Market update"
        summary = summarize(it["summary"], NEWS_SUMMARY_CHARS)
        text = f"<b>{title}</b>\n\n{summary}"
        try:
            send_text(text, buttons=[[{"text": "Read More", "url": link}]])
            seen_urls.add(link)
            seen_queue.append(link)
        except Exception as ex:
            log.warning(f"send news failed: {ex}")
    save_seen()

def post_ipo_snapshot():
    if not ENABLE_IPO or not is_weekday():
        return
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        send_text("📌 <b>IPO</b>\nNo IPO details available today.")
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x['band']: seg += f"
