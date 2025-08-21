# app.py — MarketPulse final (full features)
import os, re, time, logging, textwrap, datetime as dt
from flask import Flask
import requests, feedparser
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, TimedOut, BadRequest
import pytz

# ---------------- CONFIG (environment variables with sensible defaults) ----------------
BOT_TOKEN          = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID         = os.getenv("CHANNEL_ID", "").strip()    # MUST be numeric -100...
TIMEZONE           = os.getenv("TIMEZONE", "Asia/Kolkata")

ENABLE_NEWS        = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_INTERVAL      = int(os.getenv("NEWS_INTERVAL", "30"))       # minutes
MAX_NEWS_PER_SLOT  = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
MAX_NEWS_PER_DAY   = int(os.getenv("MAX_NEWS_PER_DAY", "50"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

ENABLE_IPO         = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME      = os.getenv("IPO_POST_TIME", "09:15")         # HH:MM IST

ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1"))
MARKET_BLIPS_TIMES  = os.getenv("MARKET_BLIPS_TIMES", "08:15,12:30,15:30")  # CSV of HH:MM IST

ENABLE_FII_DII      = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME   = os.getenv("FII_DII_POST_TIME", "20:45")    # HH:MM IST

QUIET_HOURS_START   = os.getenv("QUIET_HOURS_START", "22:30").strip()  # NEWS only
QUIET_HOURS_END     = os.getenv("QUIET_HOURS_END", "07:30").strip()

RSS_ENV = os.getenv("RSS_SOURCES", "").strip()
if RSS_ENV:
    RSS_SOURCES = [u.strip() for u in RSS_ENV.split(",") if u.strip()]
else:
    RSS_SOURCES = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://www.livemint.com/rss/markets",
        "https://www.business-standard.com/rss/markets-106.rss"
    ]

# ---------------- Logging & Globals ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("marketpulse")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
sched = BackgroundScheduler(timezone=TIMEZONE)

posted_urls_today = set()
news_count_today = 0
last_reset_date = None

# ---------------- Helpers ----------------
def now_ist():
    return dt.datetime.now(pytz.timezone(TIMEZONE))

def parse_hhmm(s):
    try:
        h, m = s.split(":")
        return int(h), int(m)
    except:
        return 0, 0

def reset_daily_counters():
    global posted_urls_today, news_count_today, last_reset_date
    today = now_ist().date()
    if last_reset_date != today:
        posted_urls_today = set()
        news_count_today = 0
        last_reset_date = today
        log.info("Daily counters reset.")

def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "")

def trim_text(s, n):
    s = re.sub(r"\s+", " ", (s or "").strip())
    return (s[:n].rstrip() + "…") if len(s) > n else s

def in_quiet_hours():
    if not QUIET_HOURS_START or not QUIET_HOURS_END:
        return False
    try:
        tnow = now_ist().time()
        sh, sm = parse_hhmm(QUIET_HOURS_START)
        eh, em = parse_hhmm(QUIET_HOURS_END)
        start = dt.time(sh, sm); end = dt.time(eh, em)
        if start < end:
            return start <= tnow < end
        else:
            return tnow >= start or tnow < end
    except Exception:
        return False

def safe_send(text, url=None, button_text="Read more"):
    if not bot or not CHANNEL_ID:
        log.error("BOT_TOKEN or CHANNEL_ID missing. Set them in environment.")
        return False
    try:
        if url:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=url)]])
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except RetryAfter as e:
        log.warning("Telegram RetryAfter, sleeping %s", e.retry_after)
        time.sleep(int(e.retry_after) + 1)
        try:
            bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="HTML", disable_web_page_preview=True)
            return True
        except Exception as e2:
            log.error("Telegram send failed after retry: %s", e2)
            return False
    except (TimedOut, BadRequest) as e:
        log.error("Telegram send failed: %s", e)
        return False
    except Exception as e:
        log.exception("Unexpected Telegram send error: %s", e)
        return False

def pick_source_name(link):
    try:
        host = re.sub(r"^https?://(www\.)?", "", link).split("/")[0]
        return host.split(".")[0].title()
    except:
        return "Source"

def categorize(title, summary):
    t = (title + " " + summary).lower()
    if any(k in t for k in ["tariff","fed","fomc","dollar","brent","crude","opec","us yields","china"]):
        return "[Global Impact]"
    if any(k in t for k in ["ipo","price band","gmp","subscription","lot size","listing"]):
        return "[IPO]"
    if any(k in t for k in ["q1","q2","q3","q4","results","earnings","merger","acquisition","stake"]):
        return "[Company]"
    if any(k in t for k in ["rbi","gdp","inflation","cpi","wpi","gst","fiscal","budget","rate cut","rate hike"]):
        return "[Finance]"
    return "[Market Update]"

# ---------------- NEWS job ----------------
def fetch_and_post_news():
    global news_count_today
    reset_daily_counters()
    if not ENABLE_NEWS:
        log.info("News disabled (ENABLE_NEWS=0)")
        return
    if in_quiet_hours():
        log.info("Quiet hours active — skipping news slot.")
        return
    if news_count_today >= MAX_NEWS_PER_DAY:
        log.info("Daily news cap reached — skipping slot.")
        return

    collected = []
    for url in RSS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:12]:
                link = getattr(entry, "link", "")
                title = (getattr(entry, "title", "") or "").strip()
                summary = strip_html(getattr(entry, "summary", "") or "")
                if not link or not title:
                    continue
                if link in posted_urls_today:
                    continue
                collected.append((link, title, summary))
        except Exception as e:
            log.warning("RSS fetch fail %s : %s", url, e)

    if not collected:
        log.info("No collected news this slot.")
        return

    # dedupe
    uniq = []
    seen = set()
    for link, title, summary in collected:
        key = link or title
        if key not in seen:
            uniq.append((link, title, summary))
            seen.add(key)

    slots_left = MAX_NEWS_PER_DAY - news_count_today
    to_post = min(MAX_NEWS_PER_SLOT, slots_left, len(uniq))

    for link, title, summary in uniq[:to_post]:
        tag = categorize(title, summary)
        body = f"📰 {tag}\n<b>{title}</b>\n\n{trim_text(summary, NEWS_SUMMARY_CHARS)}"
        source = pick_source_name(link)
        ok = safe_send(body, url=link, button_text=f"Read • {source}")
        if ok:
            posted_urls_today.add(link)
            news_count_today += 1
        time.sleep(1.0)

    log.info("News slot done — posted %d, total today %d", to_post, news_count_today)

# ---------------- IPO scraping & posts ----------------
def fetch_ipo_calendar_chittorgarh(limit=8):
    """Scrape Chittorgarh IPO timetable for open/upcoming IPOs (best-effort)."""
    out = []
    try:
        url = "https://www.chittorgarh.com/report/ipo-list-by-time-table-and-lot-size/118/all/"
        r = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")[1:] if table else []
        for rrow in rows[:limit]:
            cols = [c.get_text(" ", strip=True) for c in rrow.find_all(["td","th"])]
            # table structure varies; best-effort mapping
            if len(cols) >= 7:
                company = cols[0]
                open_dt = cols[2]
                close_dt = cols[3]
                price_band = cols[5]
                lot = cols[6]
                link_el = rrow.find("a")
                detail = link_el.get("href") if link_el and link_el.get("href") else ""
                if detail and detail.startswith("/"):
                    detail = "https://www.chittorgarh.com" + detail
                out.append({
                    "company": company,
                    "open": open_dt,
                    "close": close_dt,
                    "price_band": price_band,
                    "lot": lot,
                    "detail": detail
                })
    except Exception as e:
        log.warning("Chittorgarh IPO fetch failed: %s", e)
    return out

def fetch_gmp_map_investorgain():
    m = {}
    try:
        url = "https://www.investorgain.com/report/live-ipo-gmp/331/"
        r = requests.get(url, timeout=12, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")[1:] if table else []
        for rrow in rows:
            cols = [c.get_text(" ", strip=True) for c in rrow.find_all("td")]
            if len(cols) >= 2:
                m[cols[0].lower()] = cols[1]
    except Exception as e:
        log.debug("GMP fetch failed: %s", e)
    return m

def fetch_subscription_from_detail(detail_url):
    if not detail_url:
        return None
    try:
        r = requests.get(detail_url, timeout=12, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        for t in tables:
            txt = t.get_text(" ", strip=True).lower()
            if "qib" in txt or "retail" in txt:
                # parse simple numeric values in table rows
                rows = [[c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])] for tr in t.find_all("tr")]
                if len(rows) >= 2:
                    header = [h.lower() for h in rows[0]]
                    last = rows[-1]
                    def get_val(key):
                        for i,h in enumerate(header):
                            if key in h: 
                                return last[i] if i < len(last) else "NA"
                        return "NA"
                    return {
                        "qib": get_val("qib"),
                        "nii": get_val("nii") or get_val("hni"),
                        "retail": get_val("retail"),
                        "total": get_val("total")
                    }
    except Exception as e:
        log.debug("Subscription fetch failed for %s: %s", detail_url, e)
    return None

def post_ipos_full():
    if not ENABLE_IPO:
        log.info("IPO disabled")
        return
    ipos = fetch_ipo_calendar_chittorgarh(limit=8)
    gmp_map = fetch_gmp_map_investorgain()
    if not ipos:
        safe_send("📌 <b>[IPO]</b>\nNo IPO details available today.")
        return
    for it in ipos:
        name = it.get("company", "NA")
        price_band = it.get("price_band", "NA")
        lot = it.get("lot", "NA")
        open_d = it.get("open", "NA")
        close_d = it.get("close", "NA")
        detail = it.get("detail", "")
        gmp = gmp_map.get(name.lower(), "N/A")
        subs = fetch_subscription_from_detail(detail)
        lines = [
            f"📌 <b>[IPO] {name}</b>",
            f"• Price Band: {price_band}",
            f"• Lot Size: {lot}",
            f"• Open: {open_d}   Close: {close_d}",
            f"• GMP: {gmp}"
        ]
        if subs:
            lines.append(f"• Subscription: Total {subs.get('total','NA')} • QIB {subs.get('qib','NA')} • NII {subs.get('nii','NA')} • Retail {subs.get('retail','NA')}")
        msg = "\n".join(lines)
        safe_send(msg)
        time.sleep(1.0)

# ---------------- Market blips ----------------
def get_market_snapshot_text(kind="pre"):
    if kind == "pre":
        return "<b>[Pre-Market]</b>\nGift Nifty & global cues: mild setup. Watch IT & Oil."
    else:
        return "<b>[Market Close]</b>\nSensex/Nifty/BankNifty closing snapshot, top movers."

def post_market_blip(kind="pre"):
    safe_send(get_market_snapshot_text(kind=kind))

# ---------------- FII/DII ----------------
def post_fii_dii():
    # best-effort scavenge headlines for hints; full reliable data needs paid source
    hints = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:10]:
                txt = (getattr(e, "title", "") + " " + strip_html(getattr(e, "summary", ""))).lower()
                if "fii" in txt or "dii" in txt:
                    hints.append(getattr(e, "title", "").strip())
        except:
            pass
    if hints:
        msg = "🏦 <b>[FII/DII]</b>\n" + "\n".join("• " + h for h in hints[:5])
    else:
        msg = "🏦 <b>[FII/DII]</b>\nDaily flows not clearly available in public feeds."
    safe_send(msg)

# ---------------- Scheduling ----------------
def schedule_all_jobs():
    # news loop
    if ENABLE_NEWS and NEWS_INTERVAL > 0:
        sched.add_job(fetch_and_post_news, "interval", minutes=NEWS_INTERVAL, id="news_loop", replace_existing=True)

    # IPO
    if ENABLE_IPO:
        hh, mm = parse_hhmm(IPO_POST_TIME)
        sched.add_job(post_ipos_full, "cron", hour=hh, minute=mm, id="ipo_daily", replace_existing=True)

    # Market blips (first is pre-market, last is post-market)
    if ENABLE_MARKET_BLIPS:
        times = [t.strip() for t in MARKET_BLIPS_TIMES.split(",") if t.strip()]
        for idx, t in enumerate(times):
            hh, mm = parse_hhmm(t)
            kind = "pre" if idx == 0 else "post"
            sched.add_job(lambda k=kind: post_market_blip(k), "cron", hour=hh, minute=mm, id=f"blip_{idx}", replace_existing=True)

    # FII/DII
    if ENABLE_FII_DII:
        hh, mm = parse_hhmm(FII_DII_POST_TIME)
        sched.add_job(post_fii_dii, "cron", hour=hh, minute=mm, id="fiidii", replace_existing=True)

    # Reset counters daily
    sched.add_job(reset_daily_counters, "cron", hour=0, minute=5, id="reset_daily", replace_existing=True)

# Start scheduler and print jobs (runs at import time; OK as we will use single worker)
schedule_all_jobs()
sched.start()
log.info("Scheduler started. Jobs:")
for j in sched.get_jobs():
    log.info("  %s -> next run: %s", j.id, j.next_run_time)

# startup ping (safe)
try:
    safe_send("✅ MarketPulse bot restarted and schedule loaded.")
except Exception:
    pass

@app.route("/")
def home():
    return "MarketPulse running ✅"

# If running directly (local) keep Flask up
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
