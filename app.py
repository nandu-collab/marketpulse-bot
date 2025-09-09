# app.py
# MarketPulse Telegram broadcaster
# Uses APScheduler + Flask keepalive + python-telegram-bot v13 style Bot.send_message
import os
import json
import re
import time
import logging
import threading
from datetime import datetime
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# ------------- CONFIG helpers -------------
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")         # keep as string, e.g. -100xxxxxxxxxx
TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")

# News behaviour (you can keep these as env overrides if you want)
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))

# Quiet / window (used by news only)
QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = env("QUIET_HOURS_END", "07:30")
NEWS_WINDOW_START = env("NEWS_WINDOW_START", "08:30")   # first news slot
NEWS_WINDOW_END = env("NEWS_WINDOW_END", "21:30")       # last news slot

# Fixed-post times (HH:MM local)
PREMARKET_TIME = env("PREMARKET_TIME", "09:00")
POSTMARKET_TIME = env("POSTMARKET_TIME", "16:00")   # you can change to 17:00 if required
IPO_POST_TIME = env("IPO_POST_TIME", "11:00")
FII_DII_TIME = env("FII_DII_TIME", "21:00")

# Safety
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN required in env")

if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID required in env")

# Use pytz for APScheduler compatibility
TZ = pytz.timezone(TIMEZONE_NAME)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

# Telegram
bot = Bot(token=BOT_TOKEN)

# Flask keepalive
app = Flask(__name__)

# Dedupe (persist short list)
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=1500)

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

load_seen()

# ---- Feeds & HTTP helpers ----
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

UA = {"User-Agent": "MarketPulseBot/1.0 (+https://example.com) Mozilla/5.0"}

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
    # cut at nearest sentence end
    idx = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if idx >= 0:
        cut = cut[:idx+1]
    return cut.rstrip() + "…"

def fetch_feed_entries(url: str) -> List[dict]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:12]:
            title = clean_text(e.get("title", ""))
            link = e.get("link", "")
            desc = clean_text(e.get("summary", "") or e.get("description", ""))
            out.append({"title": title, "link": link, "summary": desc})
        return out
    except Exception as ex:
        log.warning("feed error %s: %s", url, ex)
        return []

def collect_news_batch(max_items: int) -> List[dict]:
    # round-robin groups for diversity
    results = []
    for g in ["market", "company", "finance", "global"]:
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
        used = set(x["link"] for x in results)
        more = [c for c in pool if c["link"] and c["link"] not in used and c["link"] not in seen_urls]
        results.extend(more[: max_items - len(results)])
    return results[:max_items]

# ---- Fixed-data fetchers (best-effort; skip posting if fail) ----
def fetch_close_snapshot() -> Optional[dict]:
    # try yahoo finance JSON endpoint; many blocks may return 401; if so return None
    symbols = "^NSEI,^BSESN,^NSEBANK"
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": symbols}, headers=UA, timeout=10)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        if not data:
            return None
        mp = {}
        for q in data:
            sym = q.get("symbol")
            mp[sym] = {
                "name": q.get("shortName") or sym,
                "price": q.get("regularMarketPrice"),
                "change": q.get("regularMarketChange"),
                "pct": q.get("regularMarketChangePercent"),
            }
        return mp
    except Exception as ex:
        log.warning("close snapshot failed: %s", ex)
        return None

def fetch_fii_dii_cash() -> Optional[dict]:
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    try:
        r = requests.get(url, headers=UA, timeout=12)
        if r.status_code != 200:
            log.warning("FII/DII fetch returned status %s", r.status_code)
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
        # heuristics: search last numbers in text
        flat = " | ".join(rows[0])
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
                    if len(tds) >= 6:
                        rows.append(tds)
        today = datetime.now(TZ).date()
        found = []
        for tds in rows:
            line = " | ".join(tds)
            m = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", line)
            if len(m) >= 2:
                try:
                    from datetime import datetime as _dt
                    open_dt = _dt.strptime(m[0], "%d %b %Y").date()
                    close_dt = _dt.strptime(m[1], "%d %b %Y").date()
                    if open_dt <= today <= close_dt:
                        company = tds[0]
                        band = ""
                        for x in tds:
                            if "₹" in x and "-" in x:
                                band = x
                                break
                        lot = ""
                        for x in tds:
                            if "Lot" in x or "Shares" in x:
                                lot = x
                                break
                        found.append({
                            "company": company,
                            "open": open_dt.strftime("%d %b"),
                            "close": close_dt.strftime("%d %b"),
                            "band": band or "",
                            "lot": lot or "",
                        })
                except Exception:
                    continue
        return found
    except Exception as ex:
        log.warning("IPO fetch failed: %s", ex)
        return []

# ---- Telegram sender ----
def tg_send(text: str, button_url: Optional[str] = None, button_text: str = "Read more"):
    try:
        if button_url:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=button_url)]])
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML,
                             disable_web_page_preview=True, reply_markup=kb)
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML,
                             disable_web_page_preview=True)
    except Exception as ex:
        log.exception("tg_send failed: %s", ex)

# ---- Time utilities ----
def parse_hhmm(s: str):
    h, m = s.split(":")
    return int(h), int(m)

def now_local():
    return datetime.now(TZ)

def is_weekday(dt: Optional[datetime] = None):
    d = dt or now_local()
    return d.weekday() < 5

def within_window(start_str: str, end_str: str, dt: Optional[datetime] = None):
    dt = dt or now_local()
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = datetime(dt.year, dt.month, dt.day, sh, sm, tzinfo=TZ)
    end = datetime(dt.year, dt.month, dt.day, eh, em, tzinfo=TZ)
    t = dt
    if start <= end:
        return start <= t <= end
    # crosses midnight
    return t >= start or t <= end

def in_quiet_hours(dt: Optional[datetime] = None):
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END, dt)

# ---- Jobs ----
def post_news_slot():
    # This job will run once per hour at :30 between NEWS_WINDOW_START and NEWS_WINDOW_END
    if in_quiet_hours():
        log.info("news slot: quiet hours, skipping")
        return
    # ensure inside allowed window
    if not within_window(NEWS_WINDOW_START, NEWS_WINDOW_END):
        log.info("news slot: outside news window, skipping")
        return
    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news slot: no items to post")
        return
    posted = 0
    for it in items:
        if it["link"] in seen_urls:
            continue
        body = f"<b>{it['title']}</b>\n\n{summarize(it['summary'], NEWS_SUMMARY_CHARS)}"
        try:
            tg_send(body, button_url=it["link"], button_text="Read more →")
            seen_urls.add(it["link"])
            seen_queue.append(it["link"])
            posted += 1
            time.sleep(1)
        except Exception as ex:
            log.warning("post_news_slot send failed: %s", ex)
    if posted:
        save_seen()
    log.info("news slot posted %d items", posted)

def post_premarket():
    if not is_weekday():
        log.info("premarket: weekend, skip")
        return
    # Try to make a short premarket using headlines + quick cues
    try:
        # attempt some numeric cues (best-effort); we prefer not to post if not available
        # Attempt gift/futures via Reuters or fallback to headlines
        # For safety, we will use headlines fallback so we always post some value.
        items = collect_news_batch(4)
        bullets = "\n".join([f"• {i['title']}" for i in items[:4]])
        text = f"📈 <b>[Pre-Market Brief]</b>\n\nKey overnight cues:\n{bullets}"
        tg_send(text)
        log.info("Posted pre-market brief")
    except Exception as ex:
        log.warning("premarket failed: %s", ex)

def post_postmarket():
    if not is_weekday():
        log.info("postmarket: weekend, skip")
        return
    snap = fetch_close_snapshot()
    if not snap:
        log.info("postmarket: snapshot fetch failed; skipping post")
        return
    # Build text
    def fmt_sym(sym, label):
        q = snap.get(sym)
        if not q:
            return None
        pr = q.get("price")
        ch = q.get("change")
        pct = q.get("pct")
        return f"{label}: {pr} ({ch:+} | {pct:+}%)"
    pieces = []
    for s, L in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        p = fmt_sym(s, L)
        if p:
            pieces.append(p)
    if not pieces:
        log.info("postmarket: no index data, skip")
        return
    text = "📊 <b>[Post-Market] Closing Snapshot</b>\n\n" + "\n".join(pieces)
    tg_send(text)
    log.info("Posted post-market snapshot")

def post_ipo_snapshot():
    if not is_weekday():
        log.info("ipo: weekend skip")
        return
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        log.info("ipo: none found, skip posting")
        return
    lines = ["📌 <b>[IPO] Daily Snapshot</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • {x['open']}–{x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        lines.append(seg)
    tg_send("\n".join(lines))
    log.info("Posted IPO snapshot")

def post_fii_dii_job():
    if not is_weekday():
        log.info("fii/dii: weekend skip")
        return
    data = fetch_fii_dii_cash()
    if not data:
        log.info("fii/dii: fetch failed, skip posting")
        return
    def fmt(x):
        try:
            return f"{x:,}"
        except Exception:
            return str(x)
    text = ("🏦 <b>[FII/DII] Flows (Cash)</b>\n\n"
            f"FII: {fmt(data['fii'])} cr\n"
            f"DII: {fmt(data['dii'])} cr")
    tg_send(text)
    log.info("Posted FII/DII")

# ---- Scheduler (APScheduler) ----
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler(timezone=TZ, job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 120})

def schedule_all():
    # News: run once per hour at :30 between NEWS_WINDOW_START and NEWS_WINDOW_END (hours inclusive)
    # parse hours
    hs, ms = parse_hhmm(NEWS_WINDOW_START)
    he, me = parse_hhmm(NEWS_WINDOW_END)
    # Add hourly cron from start hour to end hour at minute 30
    scheduler.add_job(post_news_slot,
                      trigger=CronTrigger(minute=30, hour=f"{hs}-{he}", timezone=TZ),
                      id="post_news_slot",
                      replace_existing=True)

    # Fixed jobs
    ph, pm = parse_hhmm(PREMARKET_TIME)
    scheduler.add_job(post_premarket, trigger=CronTrigger(hour=int(ph), minute=int(pm), timezone=TZ),
                      id="post_premarket", replace_existing=True)

    oh, om = parse_hhmm(POSTMARKET_TIME)
    scheduler.add_job(post_postmarket, trigger=CronTrigger(hour=int(oh), minute=int(om), timezone=TZ),
                      id="post_postmarket", replace_existing=True)

    ih, im = parse_hhmm(IPO_POST_TIME)
    scheduler.add_job(post_ipo_snapshot, trigger=CronTrigger(hour=int(ih), minute=int(im), timezone=TZ),
                      id="post_ipo_snapshot", replace_existing=True)

    fh, fm = parse_hhmm(FII_DII_TIME)
    scheduler.add_job(post_fii_dii_job, trigger=CronTrigger(hour=int(fh), minute=int(fm), timezone=TZ),
                      id="post_fii_dii", replace_existing=True)

schedule_all()

# Start scheduler
scheduler.start()
log.info("Scheduler started. Jobs:")
for j in scheduler.get_jobs():
    try:
        nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
    except Exception:
        nxt = str(j.next_run_time)
    log.info("  %s -> next %s", j.id, nxt)

# Announce startup (non-blocking)
def announce_startup():
    try:
        msg = (f"✅ <b>MarketPulse restarted.</b>\n"
               f"News slots: hourly at :30 between {NEWS_WINDOW_START} and {NEWS_WINDOW_END}\n"
               f"Max per slot: {MAX_NEWS_PER_SLOT}\n"
               f"Fixed posts: Pre-market {PREMARKET_TIME} • Post-market {POSTMARKET_TIME} • IPO {IPO_POST_TIME} • FII/DII {FII_DII_TIME}")
        tg_send(msg)
    except Exception as ex:
        log.warning("announce_startup failed: %s", ex)

threading.Thread(target=announce_startup, daemon=True).start()

# Flask health
@app.route("/", methods=["GET", "HEAD"])
def home():
    jobs = []
    for j in scheduler.get_jobs():
        try:
            nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
        except Exception:
            nxt = str(j.next_run_time)
        jobs.append({"id": j.id, "next_run": nxt})
    return jsonify({"ok": True, "tz": TIMEZONE_NAME, "now": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"), "jobs": jobs})

# For direct run (useful for local testing)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
