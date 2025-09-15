# app.py
# MarketPulse — combined news + scheduled market posts with retries and holiday check
# Run with: gunicorn app:app --workers 1 --timeout 180

import os
import sys
import types
import json
import re
import time
import logging
import threading
from datetime import datetime, timedelta
from collections import deque
from typing import List, Dict, Optional

# ----------------- CGI shim for Python 3.13 (feedparser expects cgi) -----------------
# This must be before importing feedparser
if "cgi" not in sys.modules:
    cgi = types.ModuleType("cgi")
    # minimal compatibility: feedparser imports cgi and may call escape in some cases
    def _dummy_escape(s, quote=True):
        return str(s)
    cgi.escape = _dummy_escape
    sys.modules["cgi"] = cgi

# ----------------- third-party imports -----------------
import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ----------------- CONFIG HELPERS (env) -----------------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")   # channel id like -100...
TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")

NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))

MARKET_BLIPS_START = env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END = env("MARKET_BLIPS_END", "21:30")

PREMARKET_TIME = env("PREMARKET_TIME", "09:00")
IPO_POST_TIME = env("IPO_POST_TIME", "11:00")
POSTMARKET_TIME = env("POSTMARKET_TIME", "16:00")
FII_DII_POST_TIME = env("FII_DII_POST_TIME", "21:00")

# Poll / retry behaviour
POLL_RETRY_INTERVAL_SEC = int(env("POLL_RETRY_INTERVAL_SEC", "600"))   # seconds between retries (default 10m)
POLL_WINDOW_MIN = int(env("POLL_WINDOW_MIN", "120"))                  # minutes to keep trying (default 120m)

SELF_PING_INTERVAL_MIN = int(env("SELF_PING_INTERVAL_MIN", "10"))     # background self-ping interval

# validation
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required in environment")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID is required in environment")

# timezone (use pytz so APScheduler works correctly)
TZ = pytz.timezone(TIMEZONE_NAME)

# ----------------- logging -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

# ----------------- telegram bot -----------------
bot = Bot(token=BOT_TOKEN)

# ----------------- flask (health) -----------------
app = Flask(__name__)

# ----------------- dedupe state -----------------
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
        log.info("No seen file or failed to load (starting fresh)")

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

load_seen()

# ----------------- utilities -----------------
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

# ----------------- feeds + news collection -----------------
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

# ----------------- special fetchers -----------------
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

def fetch_sgx_nifty() -> Optional[dict]:
    # best-effort SGX / premarket indicator
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        # common SGX symbol attempt; may or may not work depending on Yahoo coverage
        r = requests.get(url, params={"symbols": "%5ENSEI"}, headers=ua(), timeout=10)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        if data:
            q = data[0]
            return {"price": q.get("regularMarketPrice"), "change": q.get("regularMarketChange"), "pct": q.get("regularMarketChangePercent")}
    except Exception:
        pass
    return None

# ----------------- sending helper -----------------
def send_text(text: str, button_url: Optional[str] = None, button_text: str = "Read more"):
    try:
        markup = None
        if button_url:
            kb = [[InlineKeyboardButton(button_text, url=button_url)]]
            markup = InlineKeyboardMarkup(kb)
        bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=markup)
    except Exception as ex:
        log.warning("Telegram send failed: %s", ex)

# ----------------- scheduler jobs -----------------
def post_news_slot():
    now = now_local()
    # news runs 7 days/week
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

# ----------------- trading day / holiday helpers -----------------
HOLIDAYS = set()
HOLIDAYS_LAST_REFRESH = None

def fetch_nse_holidays() -> set:
    """
    Attempt to fetch NSE holiday list. If unreachable, returns empty set.
    You can replace fallback raw URL with your own hosted JSON for reliability.
    """
    global HOLIDAYS_LAST_REFRESH
    urls = [
        "https://www.nseindia.com/api/holiday-master?type=trading",   # may be blocked
        "https://raw.githubusercontent.com/nandan-collab/marketpulse-data/main/nse-holidays-2025.json"
    ]
    out = set()
    for u in urls:
        try:
            r = requests.get(u, headers=ua(), timeout=10)
            if r.status_code != 200:
                continue
            j = r.json()
            if isinstance(j, dict):
                # common shapes
                if "holidayDates" in j and isinstance(j["holidayDates"], list):
                    for x in j["holidayDates"]:
                        d = x.get("date")
                        if d:
                            out.add(d)
                elif "data" in j and isinstance(j["data"], list):
                    for item in j["data"]:
                        if isinstance(item, dict) and "date" in item:
                            out.add(item["date"])
                        elif isinstance(item, str) and re.match(r"\d{4}-\d{2}-\d{2}", item):
                            out.add(item)
                else:
                    # brute force
                    for d in re.findall(r"\d{4}-\d{2}-\d{2}", json.dumps(j)):
                        out.add(d)
            elif isinstance(j, list):
                for item in j:
                    if isinstance(item, str) and re.match(r"\d{4}-\d{2}-\d{2}", item):
                        out.add(item)
            if out:
                HOLIDAYS_LAST_REFRESH = datetime.utcnow()
                return out
        except Exception:
            continue
    HOLIDAYS_LAST_REFRESH = datetime.utcnow()
    return set()

def is_trading_day(dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    if dt.weekday() >= 5:
        return False
    global HOLIDAYS, HOLIDAYS_LAST_REFRESH
    if HOLIDAYS_LAST_REFRESH is None or (datetime.utcnow() - HOLIDAYS_LAST_REFRESH).total_seconds() > 24*3600:
        try:
            HOLIDAYS = fetch_nse_holidays()
            log.info("Holidays refreshed (%d)", len(HOLIDAYS))
        except Exception as e:
            log.warning("Failed to refresh holidays: %s", e)
    ymd = dt.strftime("%Y-%m-%d")
    return ymd not in HOLIDAYS

# ----------------- polling helper -----------------
def attempt_with_polling(target_fn, window_minutes=POLL_WINDOW_MIN, interval_seconds=POLL_RETRY_INTERVAL_SEC):
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

# ----------------- fixed posts -----------------
def post_pre_market():
    if not is_trading_day():
        log.info("pre-market: not a trading day -> skip")
        return

    def _fetch():
        sgx = fetch_sgx_nifty()
        headlines = collect_news_batch(4)
        bullets = []
        if sgx and sgx.get("price") is not None:
            pct = sgx.get("pct")
            pct_str = f" ({pct:+}%)" if pct is not None else ""
            bullets.append(f"Gift Nifty: {sgx['price']}{pct_str}")
        for h in headlines[:4]:
            bullets.append(h["title"])
        return {"bullets": bullets} if bullets else None

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
    data = attempt_with_polling(fetch_ongoing_ipos_for_today)
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
    data = attempt_with_polling(fetch_close_snapshot)
    if not data:
        log.info("post-market: snapshot unavailable -> skip")
        return
    parts = ["📊 <b>Post-Market — Closing Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        q = data.get(sym)
        if q and q.get("price") is not None:
            chg = f"{q['change']:+}" if q['change'] is not None else "—"
            pct = f"{q['pct']:+}%" if q['pct'] is not None else ""
            parts.append(f"{label}: {q['price']} ({chg} | {pct})")
    send_text("\n".join(parts))
    log.info("posted post-market")

def post_fii_dii():
    if not is_trading_day():
        log.info("FII/DII: not a trading day -> skip")
        return
    data = attempt_with_polling(fetch_fii_dii_cash)
    if not data:
        log.info("FII/DII: data not found -> skip")
        return
    def fmt(n):
        sign = "+" if n >= 0 else ""
        return f"{sign}{n:,}"
    text = f"🏦 <b>FII/DII — Cash</b>\nFII: {fmt(data['fii'])} cr\nDII: {fmt(data['dii'])} cr\n<i>Provisional</i>"
    send_text(text)
    log.info("posted FII/DII")

# ----------------- scheduler setup -----------------
sched = BackgroundScheduler(timezone=TZ, job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300})

def schedule_jobs():
    # schedule news: every hour at :30 between MARKET_BLIPS_START and MARKET_BLIPS_END (inclusive)
    sh, sm = parse_hhmm(MARKET_BLIPS_START)
    eh, em = parse_hhmm(MARKET_BLIPS_END)
    # assume same day window and hours integer range
    if sh <= eh:
        hour_expr = f"{sh}-{eh}"
    else:
        # crosses midnight; APScheduler cron can't represent wrap easily; schedule two ranges
        hour_expr = f"{sh}-23,0-{eh}"
    sched.add_job(post_news_slot, trigger=CronTrigger(hour=hour_expr, minute=30, timezone=TZ),
                  id="post_news_slot", replace_existing=True)

    # Pre-market
    hh, mm = parse_hhmm(PREMARKET_TIME)
    sched.add_job(post_pre_market, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ),
                  id="pre_market", replace_existing=True)

    # IPO
    hh, mm = parse_hhmm(IPO_POST_TIME)
    sched.add_job(post_ipo_snapshot, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ),
                  id="ipo_snapshot", replace_existing=True)

    # Post-market
    hh, mm = parse_hhmm(POSTMARKET_TIME)
    sched.add_job(post_post_market, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ),
                  id="post_market", replace_existing=True)

    # FII/DII
    hh, mm = parse_hhmm(FII_DII_POST_TIME)
    sched.add_job(post_fii_dii, trigger=CronTrigger(hour=hh, minute=mm, timezone=TZ),
                  id="fii_dii", replace_existing=True)

schedule_jobs()
sched.start()
log.info("Scheduler started. Jobs:")
for j in sched.get_jobs():
    try:
        nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
    except Exception:
        nxt = str(j.next_run_time)
    log.info(" - %s next_run=%s", j.id, nxt)
# ----------------- keepalive / self-ping -----------------
def self_ping_once():
    try:
        url = env("SELF_PING_URL") or None
        if url:
            requests.get(url, timeout=6)
            return
        base = env("SERVICE_URL") or None
        if base:
            try:
                requests.get(base + "/ping", timeout=6)
            except Exception:
                requests.get(base, timeout=6)
    except Exception as ex:
        log.debug("self_ping failed: %s", ex)

def schedule_self_ping():
    def run():
        while True:
            self_ping_once()
            time.sleep(max(30, SELF_PING_INTERVAL_MIN) * 60)
    t = threading.Thread(target=run, daemon=True)
    t.start()

schedule_self_ping()
# ----------------- startup announce -----------------
def announce_startup():
    try:
        text = (
            "✅ <b>MarketPulse started</b>\n"
            f"News: every hour at :30 between {MARKET_BLIPS_START}–{MARKET_BLIPS_END}\n"
            f"Pre-market: {PREMARKET_TIME} • IPO: {IPO_POST_TIME} • Post-market: {POSTMARKET_TIME} • FII/DII: {FII_DII_POST_TIME}\n"
            "<i>Market posts only on trading days (Mon–Fri + NSE holiday check).</i>"
        )
        send_text(text)
    except Exception as ex:
        log.warning("announce failed: %s", ex)

threading.Thread(target=announce_startup, daemon=True).start()

# ----------------- Flask endpoints -----------------
@app.route("/", methods=["GET", "HEAD"])
def root():
    return "MarketPulse running", 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

@app.route("/status", methods=["GET"])
def status():
    jobs = []
    for j in sched.get_jobs():
        try:
            nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
        except Exception:
            nxt = str(j.next_run_time)
        jobs.append({"id": j.id, "next_run": nxt})
    return jsonify({"ok": True, "tz": TIMEZONE_NAME, "now": now_local().strftime("%Y-%m-%d %H:%M:%S"), "jobs": jobs, "seen": len(seen_urls)})

# ----------------- local run -----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
