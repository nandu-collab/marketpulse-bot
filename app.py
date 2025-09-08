# app.py
# MarketPulse scheduler + Telegram poster (one-file)
import os, json, re, time, logging, threading
from datetime import datetime
from collections import deque
from typing import List, Dict, Optional

# Use pytz for APScheduler compatibility
import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# ---------- Config helpers ----------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN         = env("BOT_TOKEN")
CHANNEL_ID        = env("CHANNEL_ID")            # must be -100...
TIMEZONE_NAME     = env("TIMEZONE", "Asia/Kolkata")

# Feature toggles & times (defaults chosen per your request)
ENABLE_NEWS       = env("ENABLE_NEWS", "1") == "1"
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))   # number of items posted per scheduled news slot
NEWS_SUMMARY_CHARS= int(env("NEWS_SUMMARY_CHARS", "550"))

QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END   = env("QUIET_HOURS_END", "07:30")

# Fixed-times (Mon-Fri)
PREMARKET_TIME    = env("PREMARKET_TIME", "09:00")
IPO_POST_TIME     = env("IPO_POST_TIME", "11:00")
POSTMARKET_TIME   = env("POSTMARKET_TIME", "16:00")
FII_DII_POST_TIME = env("FII_DII_POST_TIME", "21:00")

# News schedule window (only post news inside this window)
MARKET_BLIPS_START = env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END   = env("MARKET_BLIPS_END", "21:30")

# Safety checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required in environment")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID is required in environment")

# Use pytz timezone object for APScheduler (avoids zoneinfo.normalize issue)
TZ = pytz.timezone(TIMEZONE_NAME)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

# Telegram bot
bot = Bot(token=BOT_TOKEN)

# Flask app (health check)
app = Flask(__name__)

# ---------- dedupe / persistence ----------
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
        log.info("Loaded %d seen items", len(seen_urls))
    except Exception:
        pass

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

load_seen()

# ---------- utilities ----------
UA = {"User-Agent": "Mozilla/5.0 (compatible; MarketPulseBot/1.0)"}

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
    return t >= start or t <= end

def in_quiet_hours(dt: Optional[datetime] = None) -> bool:
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END, dt)

def clean_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt)

def summarize(text: str, limit: int) -> str:
    if not text:
        return ""
    text = clean_text(text)
    if len(text) <= limit:
        return text
    cut = text[:limit]
    idx = max(cut.rfind(". "), cut.rfind("? "), cut.rfind("! "))
    return (cut[:idx+1] if idx > 0 else cut.rstrip() + "…")

# ---------- feeds & fetchers ----------
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
        cand = []
        for u in FEEDS[g]:
            cand.extend(fetch_feed_entries(u))
        uniq = []
        used = set()
        for c in cand:
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

# ---------- special fetchers ----------
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
            fii = int(m[0].replace(",", ""))
            dii = int(m[1].replace(",", ""))
            return {"fii": fii, "dii": dii}
        # fallback: last two numbers
        nums = [int(x.replace(",", "")) for x in re.findall(r"[-+]?\d[\d,]*", flat)[-2:]]
        if len(nums) == 2:
            return {"fii": nums[0], "dii": nums[1]}
        return None
    except Exception as ex:
        log.warning("FII/DII fetch failed: %s", ex)
        return None

def fetch_close_snapshot() -> Optional[dict]:
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=UA, timeout=12)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        mp = {}
        def fmt(x):
            return round(x,2) if x is not None else None
        for q in data:
            mp[q["symbol"]] = {"price": fmt(q.get("regularMarketPrice")), "change": fmt(q.get("regularMarketChange")), "pct": fmt(q.get("regularMarketChangePercent"))}
        return mp or None
    except Exception as ex:
        log.warning("close snapshot failed: %s", ex)
        return None

# ---------- sender ----------
def send_text(text: str, button_url: Optional[str] = None, button_text: str = "Read more"):
    try:
        markup = None
        if button_url:
            kb = [[InlineKeyboardButton(button_text, url=button_url)]]
            markup = InlineKeyboardMarkup(kb)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=markup)
    except Exception as ex:
        log.warning("Telegram send failed: %s", ex)

# ---------- jobs ----------
def post_news_slot():
    now = now_local()
    if not ENABLE_NEWS:
        return
    if in_quiet_hours(now):
        log.info("news slot: quiet hours -> skip")
        return
    if not within_window(MARKET_BLIPS_START, MARKET_BLIPS_END, now):
        log.info("news slot: outside market window -> skip")
        return

    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("news slot: no items")
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
    log.info("news slot: posted %d items", posted)

def post_pre_market():
    now = now_local()
    if in_quiet_hours(now):
        log.info("pre-market: quiet hours -> skip")
        return
    # quick headlines stack
    coll = collect_news_batch(4)
    bullets = "\n".join([f"• {c['title']}" for c in coll[:4]])
    text = f"📈 <b>[Pre-Market Brief]</b>\n\nKey overnight / early cues:\n{bullets}"
    send_text(text)
    log.info("posted pre-market")

def post_ipo_snapshot():
    if not (now_local().weekday() < 5):
        return
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        send_text("📌 <b>IPO</b>\nNo Mainboard IPOs open for subscription today")
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        lines.append(seg)
    send_text("\n".join(lines))
    log.info("posted IPO snapshot")

def post_post_market():
    if not (now_local().weekday() < 5):
        return
    snap = fetch_close_snapshot()
    if not snap:
        send_text("📊 <b>Post-Market</b>\nSnapshot unavailable.")
        return
    parts = ["📊 <b>Post-Market — Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        q = snap.get(sym)
        if q:
            chg = f"{q['change']:+}" if q['change'] is not None else "—"
            pct = f"{q['pct']:+}%" if q['pct'] is not None else ""
            parts.append(f"{label}: {q['price']} ({chg} | {pct})")
    send_text("\n".join(parts))
    log.info("posted post-market")

def post_fii_dii():
    if not (now_local().weekday() < 5):
        return
    data = fetch_fii_dii_cash()
    if not data:
        send_text("🏦 <b>FII/DII</b>\nData not available.")
        return
    text = f"🏦 <b>FII/DII — Cash</b>\nFII: {data['fii']:+,} cr\nDII: {data['dii']:+,} cr\n<i>Provisional</i>"
    send_text(text)
    log.info("posted fii/dii")

# ---------- scheduler ----------
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

sched = BackgroundScheduler(timezone=TZ, job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300})

def schedule_jobs():
    # News: once at minute 30 each hour between 8 and 21 (this posts up to MAX_NEWS_PER_SLOT items)
    sched.add_job(post_news_slot, trigger=CronTrigger(minute=30, hour="8-21", timezone=TZ), id="news_half_hour", replace_existing=True)

    # Pre-market at PREMARKET_TIME
    hh, mm = parse_hhmm(PREMARKET_TIME)
    sched.add_job(post_pre_market, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ), id="pre_market", replace_existing=True)

    # IPO at IPO_POST_TIME
    hh, mm = parse_hhmm(IPO_POST_TIME)
    sched.add_job(post_ipo_snapshot, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ), id="ipo_snapshot", replace_existing=True)

    # Post-market at POSTMARKET_TIME
    hh, mm = parse_hhmm(POSTMARKET_TIME)
    sched.add_job(post_post_market, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ), id="post_market", replace_existing=True)

    # FII/DII at FII_DII_POST_TIME
    hh, mm = parse_hhmm(FII_DII_POST_TIME)
    sched.add_job(post_fii_dii, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ), id="fii_dii", replace_existing=True)

schedule_jobs()
sched.start()
log.info("Scheduler started. Jobs:")
for j in sched.get_jobs():
    try:
        n = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
    except Exception:
        n = str(j.next_run_time)
    log.info(" - %s next_run=%s", j.id, n)

# Announce startup (optional)
def announce_startup():
    try:
        text = ("✅ <b>MarketPulse started</b>\n"
                f"News: every hour at :30 (08:30–21:30) • Up to {MAX_NEWS_PER_SLOT}/slot\n"
                f"Pre-market: {PREMARKET_TIME} • IPO: {IPO_POST_TIME} • Post-market: {POSTMARKET_TIME} • FII/DII: {FII_DII_POST_TIME}\n"
                f"Quiet hours: {QUIET_HOURS_START}–{QUIET_HOURS_END}")
        send_text(text)
    except Exception as ex:
        log.warning("announce failed: %s", ex)

threading.Thread(target=announce_startup, daemon=True).start()

# Health route for Render / UptimeRobot
@app.route("/")
def home():
    return "Bot is running ✅", 200

@app.route("/status")
def status():
    jobs = []
    for j in sched.get_jobs():
        try:
            nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
        except Exception:
            nxt = str(j.next_run_time)
        jobs.append({"id": j.id, "next_run": nxt})
    return jsonify({"ok": True, "tz": TIMEZONE_NAME, "jobs": jobs, "seen": len(seen_urls)})

# For local run (not used by gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
