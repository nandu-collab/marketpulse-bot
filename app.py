# app.py
import os
import re
import json
import time
import logging
import textwrap
from datetime import datetime, timedelta
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

# ===== CONFIG =====
def env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")
TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")

ENABLE_NEWS = int(env("ENABLE_NEWS", "1"))
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS = int(env("NEWS_SUMMARY_CHARS", "550"))

QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END = env("QUIET_HOURS_END", "07:30")

ENABLE_IPO = int(env("ENABLE_IPO", "1"))
IPO_POST_TIME = env("IPO_POST_TIME", "11:00")

ENABLE_MARKET_BLIPS = int(env("ENABLE_MARKET_BLIPS", "1"))
PREMARKET_TIME = env("PREMARKET_TIME", "09:00")
POSTMARKET_TIME = env("POSTMARKET_TIME", "16:00")
MARKET_WINDOW_START = env("MARKET_BLIPS_START", "08:30")
MARKET_WINDOW_END = env("MARKET_BLIPS_END", "21:30")

ENABLE_FII_DII = int(env("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME = env("FII_DII_POST_TIME", "21:00")

# Feeds (you had these earlier)
FEEDS = {
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

UA = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

# ===== sanity checks =====
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not provided in env")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID not provided in env")

TZ = pytz.timezone(TIMEZONE_NAME)

# ===== logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("marketpulse")

# ===== flask + bot =====
app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)

@app.route("/", methods=["GET", "HEAD"])
def root():
    # basic status for uptime monitors
    jobs = []
    try:
        for j in scheduler.get_jobs():
            nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
            jobs.append({"id": j.id, "next_run": nxt})
    except Exception:
        pass
    return jsonify({"ok": True, "tz": TIMEZONE_NAME, "now": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"), "jobs": jobs})

# ===== dedupe state =====
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=2000)

def load_seen():
    try:
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for u in arr:
                seen_urls.add(u)
                seen_queue.append(u)
        log.info("Loaded seen URLs: %d", len(seen_urls))
    except Exception as e:
        log.warning("load_seen failed: %s", e)

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e:
        log.warning("save_seen failed: %s", e)

load_seen()

# ===== helpers =====
def now_local():
    return datetime.now(TZ)

def parse_hhmm(s):
    try:
        h, m = s.split(":")
        return int(h), int(m)
    except Exception:
        return 0, 0

def within_window(start_str, end_str, dt=None):
    dt = dt or now_local()
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = TZ.localize(datetime(dt.year, dt.month, dt.day, sh, sm)).timetz()
    end = TZ.localize(datetime(dt.year, dt.month, dt.day, eh, em)).timetz()
    t = dt.timetz()
    if start <= end:
        return start <= t <= end
    else:
        return t >= start or t <= end

def in_quiet_hours_now():
    if not QUIET_HOURS_START or not QUIET_HOURS_END:
        return False
    return within_window(QUIET_HOURS_START, QUIET_HOURS_END)

def clean_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()

def trim(s, n):
    s = re.sub(r"\s+", " ", (s or "").strip())
    return (s[:n] + "…") if len(s) > n else s

def send_message(text, url_button=None, button_text="Read more →"):
    if not text or not text.strip():
        return
    try:
        if url_button:
            keyboard = [[InlineKeyboardButton(button_text, url=url_button)]]
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML,
                             disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        log.error("Telegram send failed: %s", e)

# ===== news fetch + selection =====
def fetch_feed_entries(url: str) -> List[dict]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:15]:
            title = clean_text(e.get("title", ""))
            link = e.get("link", "")
            desc = clean_text(e.get("summary", "") or e.get("description", ""))
            out.append({"title": title, "link": link, "summary": desc})
        return out
    except Exception as ex:
        log.warning("feed error %s: %s", url, ex)
        return []

def collect_news_batch(max_items: int) -> List[dict]:
    # Round-robin for diversity
    groups = ["market", "company", "finance", "global"]
    results = []
    for g in groups:
        if g not in FEEDS:
            continue
        candidates = []
        for u in FEEDS[g]:
            candidates.extend(fetch_feed_entries(u))
        uniq = []
        used = set()
        for c in candidates:
            link = c.get("link") or ""
            if not link or link in seen_urls or link in used:
                continue
            used.add(link)
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
        more = []
        for c in pool:
            if not c["link"] or c["link"] in used or c["link"] in seen_urls:
                continue
            used.add(c["link"])
            more.append(c)
        results.extend(more[: max_items - len(results)])
    return results[:max_items]

def categorize(title, summary):
    t = (title + " " + summary).lower()
    if any(k in t for k in ["tariff", "us yields", "fomc", "fed", "china", "opec", "brent", "dollar index", "geopolitics"]):
        return "[Global Impact]"
    if any(k in t for k in ["ipo", "price band", "gmp", "subscription", "listing"]):
        return "[IPO]"
    if any(k in t for k in ["q1", "q2", "q3", "q4", "results", "merger", "acquisition", "stake", "rights issue", "bonus issue"]):
        return "[Company]"
    if any(k in t for k in ["rbi", "gdp", "inflation", "cpi", "wpi", "gst", "fiscal", "budget"]):
        return "[Finance]"
    return "[Market Update]"

# ===== fixed data fetchers =====
# IPO: chittorgarh (best-effort)
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
        today = now_local().date()
        found = []
        from datetime import datetime as _dt
        for tds in rows:
            line = " | ".join(tds)
            try:
                m = re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", line)
                if len(m) >= 2:
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
        log.warning("IPO fetch failed: %s", ex)
        return []

# FII/DII: moneycontrol (best-effort)
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
            if len(tds) >= 5 and re.search(r"\d{1,2}\-\d{1,2}\-\d{4}", tds[0]):
                rows.append(tds)
        if not rows:
            return None
        latest = rows[0]
        flat = " | ".join(latest)
        m = re.findall(r"Net\s*:?[\s₹]*([-+]?\d[\d,]*)", flat)
        if len(m) >= 2:
            fii_net = int(m[0].replace(",", ""))
            dii_net = int(m[1].replace(",", ""))
            return {"fii": fii_net, "dii": dii_net}
        nums = re.findall(r"[-+]?\d[\d,]*", flat)
        if len(nums) >= 2:
            try:
                return {"fii": int(nums[-2].replace(",", "")), "dii": int(nums[-1].replace(",", ""))}
            except Exception:
                return None
        return None
    except Exception as ex:
        log.warning("FII/DII fetch failed: %s", ex)
        return None

# Post-market snapshot: Yahoo (best-effort)
def fetch_close_snapshot() -> Optional[dict]:
    symbols = ["^NSEI", "^BSESN", "^NSEBANK"]
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    try:
        r = requests.get(url, params={"symbols": ",".join(symbols)}, headers=UA, timeout=10)
        r.raise_for_status()
        data = r.json().get("quoteResponse", {}).get("result", [])
        if not data:
            return None
        def fmt(x):
            return round(x, 2) if x is not None else None
        mp = {}
        for q in data:
            sym = q.get("symbol")
            mp[sym] = {
                "name": q.get("shortName") or sym,
                "price": fmt(q.get("regularMarketPrice")),
                "change": fmt(q.get("regularMarketChange")),
                "pct": fmt(q.get("regularMarketChangePercent")),
            }
        return mp if mp else None
    except Exception as ex:
        log.warning("close snapshot failed: %s", ex)
        return None

# ===== jobs =====
def reset_daily_seen_if_needed():
    # Optionally reset seen URLs at midnight IST to avoid indefinite forever-blocking
    pass  # left intentionally — you may keep seen across days

def post_news_slot():
    # This is called exactly once per hour at :30 (we schedule per-hour cron)
    if not ENABLE_NEWS:
        return
    if in_quiet_hours_now():
        log.info("Quiet hours active — skipping news slot.")
        return
    if not within_window(MARKET_WINDOW_START, MARKET_WINDOW_END):
        log.info("Outside news window — skipping.")
        return

    items = collect_news_batch(MAX_NEWS_PER_SLOT)
    if not items:
        log.info("No fresh news this slot.")
        return

    posted = 0
    for it in items:
        link = it.get("link")
        title = it.get("title") or "Market update"
        summary = trim(it.get("summary", ""), NEWS_SUMMARY_CHARS)
        if not link or link in seen_urls:
            continue
        tag = categorize(title, summary)
        body = f"📰 <b>{tag}</b>\n<b>{title}</b>\n\n{summary}"
        try:
            send_message(body, url_button=link)
            seen_urls.add(link)
            seen_queue.append(link)
            posted += 1
            time.sleep(1)
        except Exception as ex:
            log.error("Failed posting news item: %s", ex)
    if posted:
        save_seen()
        log.info("Posted %d news items", posted)
    else:
        log.info("No news posted (dedupe or empty).")

def post_ipo_snapshot():
    if not ENABLE_IPO:
        return
    if datetime.now(TZ).weekday() >= 5:
        # skip weekends for IPO snapshot
        return
    items = fetch_ongoing_ipos_for_today()
    if not items:
        log.info("No active IPOs today — skipping IPO post.")
        return
    lines = ["📌 <b>IPO — Ongoing Today</b>"]
    for x in items[:6]:
        seg = f"<b>{x['company']}</b> • Open {x['open']} – Close {x['close']}"
        if x.get("band"):
            seg += f" • {x['band']}"
        if x.get("lot"):
            seg += f" • {x['lot']}"
        lines.append(seg)
    send_message("\n".join(lines))

def post_premarket():
    if not ENABLE_MARKET_BLIPS:
        return
    if datetime.now(TZ).weekday() >= 5:
        # skip weekends
        log.info("Weekend — skipping premarket")
        return
    # best-effort premarket: we'll use top headlines from FEEDS as a fallback
    # Attempt to build a short premarket snapshot from global & market feeds
    try:
        # Use FEEDS['global'] and FEEDS['market'] first headline summaries
        cand = []
        for u in FEEDS.get("market", []) + FEEDS.get("global", []):
            e = fetch_feed_entries(u)
            if e:
                cand.extend(e)
        if not cand:
            log.info("No feed entries for premarket — skipping")
            return
        top = cand[0]
        title = top.get("title", "Pre-market brief")
        summary = trim(top.get("summary", ""), 400)
        body = (f"📈 <b>[Pre-Market Brief]</b>\n\n"
                f"<b>{title}</b>\n\n{summary}\n\n"
                "<i>Note: Snapshot constructed from headlines (best-effort).</i>")
        send_message(body)
    except Exception as ex:
        log.warning("Premarket failed: %s", ex)

def post_postmarket():
    if not ENABLE_MARKET_BLIPS:
        return
    if datetime.now(TZ).weekday() >= 5:
        log.info("Weekend — skipping post-market")
        return
    snap = fetch_close_snapshot()
    if not snap:
        log.info("Postmarket snapshot fetch failed — skipping post.")
        return
    # format
    def ln(sym, label):
        q = snap.get(sym)
        if not q:
            return None
        chg = f"{q['change']:+}" if q['change'] is not None else "—"
        pct = f"{q['pct']:+}%" if q['pct'] is not None else ""
        return f"{label}: {q['price']} ({chg} | {pct})"
    lines = ["📊 <b>[Post-Market] Closing Snapshot</b>"]
    for sym, label in [("^BSESN", "Sensex"), ("^NSEI", "Nifty 50"), ("^NSEBANK", "Bank Nifty")]:
        l = ln(sym, label)
        if l:
            lines.append(l)
    # attempt to add top gainers/losers as text by scraping a market article (best-effort)
    send_message("\n".join(lines))

def post_fii_dii():
    if not ENABLE_FII_DII:
        return
    if datetime.now(TZ).weekday() >= 5:
        log.info("Weekend — skipping FII/DII")
        return
    data = fetch_fii_dii_cash()
    if not data:
        log.info("FII/DII fetch failed — skipping.")
        return
    text = (f"🏦 <b>[FII/DII] — Cash Market</b>\n\n"
            f"FII: {data['fii']:+,} cr\n"
            f"DII: {data['dii']:+,} cr\n\n"
            "<i>Provisional numbers; best-effort scrape.</i>")
    send_message(text)

# ===== SCHEDULER =====
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(timezone=TZ, job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300})

def schedule_all():
    # schedule news exactly at :30 for hours 08..21 inclusive
    for hr in range(8, 22):  # 8..21
        scheduler.add_job(post_news_slot, "cron", hour=hr, minute=30, id=f"news_{hr}_30", replace_existing=True)

    # fixed posts
    ph, pm = parse_hhmm(PREMARKET_TIME)
    scheduler.add_job(post_premarket, "cron", hour=ph, minute=pm, id="pre_market", replace_existing=True)

    hh, mm = parse_hhmm(POSTMARKET_TIME)
    scheduler.add_job(post_postmarket, "cron", hour=hh, minute=mm, id="post_market", replace_existing=True)

    ih, im = parse_hhmm(IPO_POST_TIME)
    scheduler.add_job(post_ipo_snapshot, "cron", hour=ih, minute=im, id="ipo_snapshot", replace_existing=True)

    fh, fm = parse_hhmm(FII_DII_POST_TIME)
    scheduler.add_job(post_fii_dii, "cron", hour=fh, minute=fm, id="fiidii", replace_existing=True)

schedule_all()
scheduler.start()
log.info("Scheduler started. Jobs:")
for j in scheduler.get_jobs():
    try:
        nxt = j.next_run_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if j.next_run_time else None
    except Exception:
        nxt = str(j.next_run_time)
    log.info("JOB %s next=%s", j.id, nxt)

# announce (non-blocking)
def announce_startup():
    msg = ("✅ <b>MarketPulse restarted</b>.\n"
           f"News slots: hourly at :30 between {MARKET_WINDOW_START} and {MARKET_WINDOW_END}\n"
           f"Max per slot: {MAX_NEWS_PER_SLOT}\n"
           f"Fixed posts: Pre-market {PREMARKET_TIME} • Post-market {POSTMARKET_TIME} • IPO {IPO_POST_TIME} • FII/DII {FII_DII_POST_TIME}")
    try:
        send_message(msg)
    except Exception as e:
        log.warning("announce failed: %s", e)

try:
    # fire-and-forget
    import threading
    threading.Thread(target=announce_startup, daemon=True).start()
except Exception:
    pass

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
