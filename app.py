# app.py (corrected timezone handling – uses pytz for APScheduler)
import os, json, re, time, logging, threading
from datetime import datetime, timedelta, date
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify

# --- Telegram (PTB 13.x) ---
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# =============== CONFIG ===============
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN            = env("BOT_TOKEN")
CHANNEL_ID_RAW       = env("CHANNEL_ID")  # "-100..." or "@yourchannel"
TIMEZONE_NAME        = env("TIMEZONE", "Asia/Kolkata")

ENABLE_NEWS          = env("ENABLE_NEWS", "1") == "1"
NEWS_INTERVAL        = int(env("NEWS_INTERVAL", "59"))        # minutes; default 60 as you set
MAX_NEWS_PER_SLOT    = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS   = int(env("NEWS_SUMMARY_CHARS", "550"))

QUIET_HOURS_START    = env("QUIET_HOURS_START", "22:30")       # HH:MM (local)
QUIET_HOURS_END      = env("QUIET_HOURS_END", "07:30")         # HH:MM (local)

ENABLE_IPO           = env("ENABLE_IPO", "1") == "1"
IPO_POST_TIME        = env("IPO_POST_TIME", "10:30")           # HH:MM (Mon–Fri)

ENABLE_MARKET_BLIPS  = env("ENABLE_MARKET_BLIPS", "1") == "1"
MARKET_BLIPS_START   = env("MARKET_BLIPS_START", "08:30")      # HH:MM
MARKET_BLIPS_END     = env("MARKET_BLIPS_END", "20:30")        # HH:MM

POSTMARKET_TIME      = env("POSTMARKET_TIME", "19:00")         # HH:MM (Mon–Fri)

ENABLE_FII_DII       = env("ENABLE_FII_DII", "1") == "1"
FII_DII_POST_TIME    = env("FII_DII_POST_TIME", "21:00")       # HH:MM (Mon–Fri)

# ====== Safety checks ======
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment!")

# CHANNEL_ID as str
CHANNEL_ID = CHANNEL_ID_RAW

# IMPORTANT: use pytz timezone here (not zoneinfo) so APScheduler does not crash
TZ = pytz.timezone(TIMEZONE_NAME)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("marketpulse")

# Telegram bot
bot = Bot(token=BOT_TOKEN)

# Flask (health + keep-alive)
app = Flask(__name__)

# ========== DEDUPE STATE ==========
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=1200)  # remember the last ~1200 links

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

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning(f"save_seen failed: {e}")

load_seen()

# ========== TIME UTILS ==========
def now_local():
    # return aware datetime in TZ (pytz)
    return datetime.now(TZ)

def parse_hhmm(s: str) -> datetime.time:
    h, m = s.split(":")
    return datetime.strptime(f"{int(h):02d}:{int(m):02d}", "%H:%M").time()

def is_weekday(dt: Optional[datetime] = None) -> bool:
    d = dt or now_local()
    return d.weekday() < 5  # Mon=0 .. Sun=6

def within_window(start_str: str, end_str: str, dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    start = parse_hhmm(start_str)
    end   = parse_hhmm(end_str)
    t = dt.time()
    if start <= end:
        return start <= t <= end
    # crosses midnight
    return t >= start or t <= end

def in_quiet_hours(dt: Optional[datetime] = None) -> bool:
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
    "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
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

# IPO fetcher – best-effort
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
                            "source": url
                        })
            except Exception:
                continue
        return found
    except Exception as ex:
        log.warning(f"IPO fetch failed: {ex}")
        return []

# FII/DII (Moneycontrol best-effort)
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

# Post-market snapshot via Yahoo
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
    if not ENABLE_NEWS:
        return
    if in_quiet_hours(now):
        log.info("news: quiet hours, skip")
        return
    if not within_window(MARKET_BLIPS_START, MARKET_BLIPS_END, now):
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
            send_text(
                text,
                buttons=[[{"text": "Read More", "url": it["link"]}]],
            )
            seen_urls.add(it["link"])
            seen_queue.append(it["link"])
        except Exception as ex:
            log.warning(f"send news failed: {ex}")

    save_seen()

def post_ipo_snapshot():
    if not ENABLE_IPO or not is_weekday():
        return
    ipos = fetch_ongoing_ipos_for_today()
    if not ipos:
        text = "📌 <b>IPO</b>\nNo IPO details available today."
        send_text(text)
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in ipos[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x['band']:
            seg += f" • {x['band']}"
        if x['lot']:
            seg += f" • {x['lot']}"
        lines.append(seg)
    send_text("\n".join(lines))

def post_market_close():
    if not ENABLE_MARKET_BLIPS or not is_weekday():
        return
    snap = fetch_close_snapshot()
    if not snap:
        send_text("📊 <b>Post-Market</b>\nMarket closed. (Snapshot unavailable)")
        return
    def ln(sym, label):
        q = snap.get(sym)
        if not q:
            return None
        chg = f"{q['change']:+}" if q['change'] is not None else "—"
        pct = f"{q['pct']:+}%" if q['pct'] is not None else ""
        return f"{label}: {q['price']} ({chg} | {pct})"
    lines = ["📊 <b>Post-Market — Closing Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        l = ln(sym, label)
        if l:
            lines.append(l)
    send_text("\n".join(lines))

def post_fii_dii():
    if not ENABLE_FII_DII or not is_weekday():
        return
    data = fetch_fii_dii_cash()
    if not data:
        send_text("🏦 <b>FII/DII</b>\nLatest activity not available yet.")
        return
    def fmt(x):
        s = f"{x:+,}"
        return s.replace(",", ",")
    text = (
        "🏦 <b>FII/DII — Cash Market</b>\n"
        f"FII: {fmt(data['fii'])} cr\n"
        f"DII: {fmt(data['dii'])} cr\n"
        "<i>Note: Provisional numbers; subject to revision.</i>"
    )
    send_text(text)

# ========== SCHEDULER ==========
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler(
    timezone=TZ,
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
)

def schedule_jobs():
    # every NEWS_INTERVAL minutes (e.g., 60 -> once per hour). Job posts up to MAX_NEWS_PER_SLOT items.
    scheduler.add_job(
        post_news_slot,
        trigger=CronTrigger(minute=f"*/{NEWS_INTERVAL}", timezone=TZ),
        id="post_news_slot",
        replace_existing=True,
    )

    if ENABLE_IPO:
        hh, mm = IPO_POST_TIME.split(":")
        scheduler.add_job(post_ipo_snapshot, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_ipo_snapshot", replace_existing=True)

    if ENABLE_MARKET_BLIPS:
        hh, mm = POSTMARKET_TIME.split(":")
        scheduler.add_job(post_market_close, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_market_close", replace_existing=True)

    if ENABLE_FII_DII:
        hh, mm = FII_DII_POST_TIME.split(":")
        scheduler.add_job(post_fii_dii, trigger=CronTrigger(hour=int(hh), minute=int(mm), timezone=TZ),
                          id="post_fii_dii", replace_existing=True)

schedule_jobs()
scheduler.start()
log.info("✅ Scheduler started.")
for j in scheduler.get_jobs():
    # next_run_time may be None; guard it
    try:
        nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
    except Exception:
        nxt = str(j.next_run_time)
    log.info(f"JOB: {j.id} next_run={nxt}")

# Announce on startup (async thread)
def announce_startup():
    start = f"{MARKET_BLIPS_START}–{MARKET_BLIPS_END}"
    msg = (
        "✅ <b>MarketPulse bot restarted and schedule loaded.</b>\n"
        f"Window: {MARKET_BLIPS_START}–{MARKET_BLIPS_END} • Every {NEWS_INTERVAL} min • Max {MAX_NEWS_PER_SLOT}/slot\n"
        f"Quiet: {QUIET_HOURS_START}–{QUIET_HOURS_END}\n"
        f"Fixed posts: {IPO_POST_TIME} IPO • {POSTMARKET_TIME} Post-market • {FII_DII_POST_TIME} FII/DII\n"
        "<i>News runs daily; fixed posts Mon–Fri.</i>"
    )
    try:
        send_text(msg)
    except Exception as ex:
        log.warning(f"startup announce failed: {ex}")

threading.Thread(target=announce_startup, daemon=True).start()

# ========== FLASK ==========
@app.route("/", methods=["GET", "HEAD"])
def root():
    jobs = []
    for j in scheduler.get_jobs():
        try:
            nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
        except Exception:
            nxt = str(j.next_run_time)
        jobs.append({
            "id": j.id,
            "next_run": nxt
        })
    return jsonify({
        "ok": True,
        "tz": TIMEZONE_NAME,
        "now": now_local().strftime("%Y-%m-%d %H:%M:%S"),
        "jobs": jobs,
        "news_window": [MARKET_BLIPS_START, MARKET_BLIPS_END],
        "quiet": [QUIET_HOURS_START, QUIET_HOURS_END],
        "seen": len(seen_urls)
    })

# For gunicorn
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
