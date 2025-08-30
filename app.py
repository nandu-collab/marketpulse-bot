import os, time, json, sqlite3, logging, textwrap, re, html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests, feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
import uvicorn

from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode

# -------------------- setup --------------------
load_dotenv()
BOT_TOKEN   = os.environ["BOT_TOKEN"].strip()
CHANNEL_ID  = os.environ["CHANNEL_ID"].strip()
TZ_NAME     = os.getenv("TIMEZONE", "Asia/Kolkata")
TZ          = ZoneInfo(TZ_NAME)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("market-bot")

bot = Bot(BOT_TOKEN)

DB_PATH = "seen.db"

def ensure_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS seen(
        id TEXT PRIMARY KEY,
        source TEXT,
        title TEXT,
        ts INTEGER
    )""")
    con.commit()
    con.close()
ensure_db()

def already_seen(item_id: str) -> bool:
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE id=?", (item_id,))
    row = cur.fetchone()
    con.close()
    return bool(row)

def mark_seen(item_id: str, source: str, title: str):
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO seen(id, source, title, ts) VALUES(?,?,?,?)",
                (item_id, source, title, int(time.time())))
    con.commit(); con.close()

# -------------------- news sources --------------------
# Use stable RSS feeds (no keys, good uptime). Add/remove as you like.
NEWS_SOURCES = [
    # (name, rss_url)
    ("EconomicTimes", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Moneycontrol",  "https://www.moneycontrol.com/rss/MCtopnews.xml"),
    ("Mint",          "https://www.livemint.com/rss/markets"),
    ("BusinessStandard", "https://www.business-standard.com/finance-news/rss"),
    ("Reuters India", "https://feeds.reuters.com/reuters/INTopNews"),  # global/India mix, good for macro
]

MAX_SUMMARY_CHARS = 550  # tune to your taste

def clean_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"<.*?>", " ", s)      # drop tags
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def summarize(desc: str, fallback: str) -> str:
    txt = clean_text(desc) or clean_text(fallback)
    if len(txt) <= MAX_SUMMARY_CHARS:
        return txt
    # cut on sentence/end punctuation if we can
    cut = txt[:MAX_SUMMARY_CHARS]
    m = re.search(r"[.!?]\s+\S*$", cut)
    if m:
        return cut[:m.end()].strip()
    return cut.rstrip() + "…"

def fetch_latest_items(limit_per_source=6):
    """Yield dicts with title, link, summary, source, uid in reverse-chron order."""
    items = []
    for name, url in NEWS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:limit_per_source]:
                title = clean_text(e.get("title", ""))
                link  = e.get("link", "")
                desc  = e.get("summary", "") or e.get("description", "")
                uid   = e.get("id") or e.get("guid") or link
                if not (title and link and uid):
                    continue
                items.append({
                    "title": title,
                    "link": link,
                    "summary": summarize(desc, title),
                    "source": name,
                    "uid": f"{name}:{uid}",
                    "published": e.get("published_parsed") or e.get("updated_parsed")
                })
        except Exception as ex:
            log.warning("RSS error %s: %s", name, ex)
    # sort newest first if we have times; else keep as-is
    items.sort(key=lambda x: x.get("published") or time.gmtime(0), reverse=True)
    return items

def make_post_text(title: str, summary: str) -> str:
    # replicate your screenshot style
    return f"*Market Pulse India*\n[Market Update] *{title}*\n\n{summary}"

def send_news_post(item):
    if already_seen(item["uid"]):
        return False
    text = make_post_text(item["title"], item["summary"])
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Read • {item['source']}", url=item["link"])]
    ])
    bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
        reply_markup=kb
    )
    mark_seen(item["uid"], item["source"], item["title"])
    log.info("Posted: %s | %s", item["source"], item["title"])
    return True

# -------------------- schedules & logic --------------------
def market_is_open_today(dt: datetime) -> bool:
    # Weekdays only. (Add NSE holiday list if you want—hook provided.)
    return dt.weekday() < 5

def post_two_news_now():
    now = datetime.now(TZ)
    items = fetch_latest_items()
    posted = 0
    for it in items:
        if send_news_post(it):
            posted += 1
        if posted >= 2:
            break
    if posted == 0:
        log.info("Nothing new to post this slot.")

# ---- Fixed-time specialty posts (with safe fallbacks) ----

def post_pre_market_brief():
    """Quick pre-market brief. Uses headlines as fallback if data not available."""
    now = datetime.now(TZ)
    try:
        # Optional: scrape global cues (US futures/Brent/INR) if you like.
        # To keep robust, we do a short curated headline stack:
        items = fetch_latest_items(limit_per_source=3)[:5]
        bullets = "\n".join([f"• {it['title']}" for it in items[:4]])
        text = "*Pre-Market Brief*\n\nKey overnight cues:\n" + bullets
        bot.send_message(CHANNEL_ID, text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        log.info("Posted Pre-Market Brief")
    except Exception as ex:
        log.exception("Pre-market failed: %s", ex)

def post_post_market_wrap():
    """End-of-day wrap using headlines as fallback."""
    try:
        items = fetch_latest_items(limit_per_source=3)[:6]
        bullets = "\n".join([f"• {it['title']}" for it in items[:6]])
        text = "*Post-Market Wrap*\n\n" + bullets
        bot.send_message(CHANNEL_ID, text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        log.info("Posted Post-Market Wrap")
    except Exception as ex:
        log.exception("Post-market failed: %s", ex)

def fetch_fii_dii_moneycontrol():
    """
    Scrape FII/DII activity (net buy/sell) from Moneycontrol page.
    This selector tends to be stable but tweak if page changes.
    """
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
    hdrs = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.moneycontrol.com/"
    }
    r = requests.get(url, headers=hdrs, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = {}
    # Try to find latest table rows
    tbl = soup.find("table")
    if not tbl:
        return None
    rows = tbl.find_all("tr")
    # naive parse: look for 'FII/FPI' and 'DII' rows
    for tr in rows:
        tds = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if len(tds) < 3: 
            continue
        line = " ".join(tds).lower()
        if "fii" in line or "fpi" in line:
            out["FII"] = ", ".join(tds[1:])
        if "dii" in line:
            out["DII"] = ", ".join(tds[1:])
    return out or None

def post_fii_dii():
    try:
        data = fetch_fii_dii_moneycontrol()
        if not data:
            text = "*FII/DII Activity*\n\nData not available right now."
        else:
            text = "*FII/DII Activity (Cash Market)*\n\n" + "\n".join([f"• {k}: {v}" for k,v in data.items()])
        bot.send_message(CHANNEL_ID, text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        log.info("Posted FII/DII")
    except Exception as ex:
        log.exception("FII/DII failed: %s", ex)

def fetch_ipo_moneycontrol_cards():
    """
    Scrape latest active IPO cards (issue size, price band, dates).
    Adjust selectors if they change.
    """
    url = "https://www.moneycontrol.com/ipo/"
    hdrs = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.moneycontrol.com/"}
    r = requests.get(url, headers=hdrs, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    cards = []
    for card in soup.select(".ipo_listing, .ipo_list, .ipo_box"):
        title = card.get_text(" ", strip=True)
        if not title: 
            continue
        # crude text squeeze; real build would map fields by label
        txt = re.sub(r"\s+", " ", title)
        cards.append(txt)
    # Return a few lines cleaned
    return cards[:3]

def post_ipo_digest():
    try:
        cards = fetch_ipo_moneycontrol_cards()
        if not cards:
            text = "*IPO Watch (Live)*\n\nNo live details available right now."
        else:
            bullets = "\n\n".join([f"• {c}" for c in cards])
            text = "*IPO Watch (Live)*\n\n" + bullets
        bot.send_message(CHANNEL_ID, text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        log.info("Posted IPO digest")
    except Exception as ex:
        log.exception("IPO digest failed: %s", ex)

# -------------------- scheduler --------------------
scheduler = BackgroundScheduler(timezone=TZ)

def schedule_jobs():
    # Hourly news windows (two posts per hour) between 08:30 and 21:30.
    # We'll schedule at minute 30 and minute 00 within the window.
    # 08:30 -> 08:30; 09:00 -> 21:00; 21:30 -> 21:30
    scheduler.add_job(post_two_news_now, CronTrigger(minute="30", hour="8-21"))
    scheduler.add_job(post_two_news_now, CronTrigger(minute="0",  hour="9-21"))

    # Pre/Post market & data posts (fixed time daily, weekdays only check inside)
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_pre_market_brief(),
                      CronTrigger(hour=9, minute=0))     # 09:00
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_post_market_wrap(),
                      CronTrigger(hour=16, minute=0))    # 16:00
    scheduler.add_job(post_fii_dii, CronTrigger(hour=20, minute=0))   # 20:00
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_ipo_digest(),
                      CronTrigger(hour=10, minute=45))   # 10:45

# -------------------- web keepalive (Render) --------------------
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "time": datetime.now(TZ).isoformat()}

def main():
    schedule_jobs()
    scheduler.start()
    log.info("Scheduler started. Bot ready.")
    # Run a tiny web server so Render keeps the worker alive
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")

if __name__ == "__main__":
    main()
