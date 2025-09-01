import os, re, time, logging, textwrap, datetime as dt
from flask import Flask
import feedparser
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# ========= CONFIG (env) =========
BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
CHANNEL_ID         = os.getenv("CHANNEL_ID", "")          # must be -100xxxxxxxxxxxx
TIMEZONE           = os.getenv("TIMEZONE", "Asia/Kolkata")

ENABLE_NEWS        = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_INTERVAL      = int(os.getenv("NEWS_INTERVAL", "30"))  # minutes
MAX_NEWS_PER_SLOT  = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))
MAX_NEWS_PER_DAY   = int(os.getenv("MAX_NEWS_PER_DAY", "50"))
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

ENABLE_IPO         = int(os.getenv("ENABLE_IPO", "1"))
IPO_POST_TIME      = os.getenv("IPO_POST_TIME", "09:15")     # HH:MM IST

ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1"))
MARKET_BLIPS_TIMES  = os.getenv("MARKET_BLIPS_TIMES", "08:15,20:30")

ENABLE_FII_DII      = int(os.getenv("ENABLE_FII_DII", "1"))
FII_DII_POST_TIME   = os.getenv("FII_DII_POST_TIME", "20:45")

QUIET_HOURS_START   = os.getenv("QUIET_HOURS_START", "").strip()
QUIET_HOURS_END     = os.getenv("QUIET_HOURS_END", "").strip()

RSS_ENV = os.getenv("RSS_SOURCES", "").strip()
if RSS_ENV:
    RSS_SOURCES = [u.strip() for u in RSS_ENV.split(",") if u.strip()]
else:
    RSS_SOURCES = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://www.livemint.com/rss/markets",
    ]

# ========= Globals =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
app = Flask(__name__)
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
sched = BackgroundScheduler(timezone=TIMEZONE)

posted_urls_today = set()
news_count_today  = 0
last_reset_date   = None

# ========= Helpers =========
def parse_hhmm(s):
    try:
        h, m = s.split(":")
        return int(h), int(m)
    except Exception:
        return 0, 0

def reset_counters_if_needed():
    global posted_urls_today, news_count_today, last_reset_date
    today = dt.datetime.now().date()
    if last_reset_date != today:
        posted_urls_today = set()
        news_count_today  = 0
        last_reset_date   = today
        logging.info("Daily counters reset.")

def strip_html(s):
    return re.sub(r"<[^>]+>", "", s or "")

def trim(s, n):
    s = re.sub(r"\s+", " ", (s or "").strip())
    return (s[:n] + "…") if len(s) > n else s

def in_quiet_hours_now():
    if not QUIET_HOURS_START or not QUIET_HOURS_END:
        return False
    try:
        now = dt.datetime.now().time()
        sh, sm = parse_hhmm(QUIET_HOURS_START); eh, em = parse_hhmm(QUIET_HOURS_END)
        start = dt.time(sh, sm); end = dt.time(eh, em)
        if start < end:
            return start <= now < end
        else:
            return now >= start or now < end
    except Exception:
        return False

def tg_send(text, button_url=None, button_text="Read more"):
    if not bot or not CHANNEL_ID:
        logging.error("Missing BOT_TOKEN or CHANNEL_ID.")
        return
    try:
        if button_url:
            buttons = [[InlineKeyboardButton(button_text, url=button_url)]]
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        else:
            bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    except Exception as e:
        logging.error(f"Telegram send failed: {e}")

def categorize(title, summary):
    t = (title + " " + summary).lower()
    if any(k in t for k in ["ipo", "gmp", "subscription", "listing"]):
        return "[IPO]"
    if any(k in t for k in ["rbi", "gdp", "inflation", "budget"]):
        return "[Finance]"
    return "[Market Update]"

# ========= News job =========
def fetch_and_post_news():
    global news_count_today
    reset_counters_if_needed()

    if not ENABLE_NEWS or in_quiet_hours_now():
        return
    if news_count_today >= MAX_NEWS_PER_DAY:
        return

    collected = []
    for url in RSS_SOURCES:
        try:
            f = feedparser.parse(url)
            for e in f.entries[:12]:
                link = getattr(e, "link", "")
                title = (getattr(e, "title", "") or "").strip()
                summary = strip_html(getattr(e, "summary", "") or "")
                if not link or not title:
                    continue
                if link in posted_urls_today:
                    continue
                collected.append((link, title, summary))
        except Exception as ex:
            logging.warning(f"RSS fetch fail {url}: {ex}")

    if not collected:
        return

    uniq = []
    seen = set()
    for link, title, summary in collected:
        key = link or title
        if key not in seen:
            uniq.append((link, title, summary))
            seen.add(key)

    slots_left_today = MAX_NEWS_PER_DAY - news_count_today
    to_post = min(MAX_NEWS_PER_SLOT, slots_left_today, len(uniq))

    for link, title, summary in uniq[:to_post]:
        tag = categorize(title, summary)
        body = f"<b>{tag}</b>\n<b>{title}</b>\n\n{trim(summary, NEWS_SUMMARY_CHARS)}"
        tg_send(body, button_url=link, button_text="Read more →")
        posted_urls_today.add(link)
        news_count_today += 1
        time.sleep(2)

# ========= Schedule =========
def schedule_all():
    if ENABLE_NEWS:
        sched.add_job(fetch_and_post_news, "interval", minutes=NEWS_INTERVAL, id="news_loop", replace_existing=True)
    sched.add_job(reset_counters_if_needed, "cron", hour=0, minute=5, id="reset", replace_existing=True)

schedule_all()
sched.start()

# ========= Flask route =========
@app.route("/")
def home():
    return "MarketPulse bot running ✅"
