import os, time, json, re, threading, math
from datetime import datetime, timedelta
import pytz
import requests
import feedparser
import schedule
from flask import Flask
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# --------------- ENV ---------------
TZ                  = os.getenv("TZ", "Asia/Kolkata")
IST                 = pytz.timezone(TZ)

BOT_TOKEN           = os.getenv("BOT_TOKEN")
CHANNEL_ID          = os.getenv("CHANNEL_ID")                    # like -1001234567890

# News toggles & limits
ENABLE_NEWS         = int(os.getenv("ENABLE_NEWS", "1"))
NEWS_API            = os.getenv("NEWS_API", "")                  # optional; bot works without it
NEWS_INTERVAL       = int(os.getenv("NEWS_INTERVAL", "30"))      # minutes
MAX_NEWS_PER_DAY    = int(os.getenv("MAX_NEWS_PER_DAY", "50"))
MAX_NEWS_PER_SLOT   = int(os.getenv("MAX_NEWS_PER_SLOT", "2"))   # posts per interval
NEWS_SUMMARY_CHARS  = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

# Quiet hours to avoid night spam (24h, IST)
QUIET_HOURS_START   = os.getenv("QUIET_HOURS_START", "22:30")    # inclusive
QUIET_HOURS_END     = os.getenv("QUIET_HOURS_END",   "07:30")    # exclusive

# Feature toggles
ENABLE_IPO          = int(os.getenv("ENABLE_IPO", "1"))
ENABLE_MARKET_BLIPS = int(os.getenv("ENABLE_MARKET_BLIPS", "1")) # pre/mid/post
ENABLE_FII_DII      = int(os.getenv("ENABLE_FII_DII", "1"))

# --------------- TELEGRAM ---------------
assert BOT_TOKEN, "BOT_TOKEN missing"
assert CHANNEL_ID, "CHANNEL_ID missing"
bot = Bot(BOT_TOKEN)

def post(text, link=None, source_name=None, disable_preview=True):
    """Send message. If link provided, add a single 'Read • {source}' button."""
    try:
        reply_markup = None
        if link:
            btn_text = f"Read • {source_name or 'Source'}"
            reply_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton(btn_text, url=link)]]
            )
        bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_preview,
            parse_mode="HTML",
        )
    except Exception as e:
        print("TELEGRAM SEND ERROR:", e)

# --------------- TIME HELPERS ---------------
def now_ist():
    return datetime.now(IST)

def is_quiet_hours():
    """Return True if current time is between QUIET_HOURS_START and QUIET_HOURS_END."""
    n = now_ist()
    def parse(hhmm):
        hh, mm = map(int, hhmm.split(":"))
        return hh, mm
    sH, sM = parse(QUIET_HOURS_START)
    eH, eM = parse(QUIET_HOURS_END)
    start = n.replace(hour=sH, minute=sM, second=0, microsecond=0)
    end   = n.replace(hour=eH, minute=eM, second=0, microsecond=0)
    if start <= end:
        return start <= n < end
    else:
        # window crosses midnight
        return n >= start or n < end

# --------------- NEWS ---------------
NEWS_SEEN = set()        # url hashes (in-memory)
NEWS_DAY  = now_ist().date()
NEWS_COUNT_TODAY = 0

RSS_FEEDS = [
    # Indian markets & finance
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://www.business-standard.com/rss/markets-106.rss",
    "https://www.livemint.com/rss/markets",
    # Global → impact on India
    "https://www.ft.com/markets?format=rss",
    "https://www.reuters.com/finance/markets/rss",    # Reuters global markets
]

GOOD_WORDS = [
    # keep it market/finance focused
    "nifty","sensex","bank nifty","rbi","gst","inflation","fii","dii","ipo",
    "markets","stocks","equity","mutual fund","budget","gdp","rupee","oil",
    "bond","yield","tariff","fed","us","china","crude","brent","commodity",
    "psu","bank","it","auto","pharma","midcap","smallcap"
]
BAD_WORDS = [
    "sports","celebrity","bollywood","crime","weather","politics gossip",
]

def short(txt, n):
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt if len(txt) <= n else txt[: n-1].rstrip() + "…"

def pick_source_name(link):
    try:
        host = re.sub(r"^https?://", "", link).split("/")[0]
        host = host.replace("www.", "")
        return host.capitalize()
    except:
        return "Source"

def fetch_rss_items():
    items = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:20]:
                link = e.get("link")
                title = (e.get("title") or "").strip()
                summ  = (e.get("summary") or e.get("description") or "").strip()
                if not link or not title:
                    continue
                t = f"{title} {summ}".lower()
                if any(w in t for w in BAD_WORDS):
                    continue
                if not any(w in t for w in GOOD_WORDS):
                    continue
                items.append({
                    "title": title,
                    "summary": summ,
                    "link": link,
                    "source": pick_source_name(link)
                })
        except Exception as e:
            print("RSS ERR:", url, e)
    return items

def daily_reset_if_needed():
    global NEWS_DAY, NEWS_COUNT_TODAY, NEWS_SEEN
    today = now_ist().date()
    if today != NEWS_DAY:
        NEWS_DAY = today
        NEWS_COUNT_TODAY = 0
        NEWS_SEEN = set()
        print("Daily counters reset.")

def job_news():
    if not ENABLE_NEWS:
        return
    daily_reset_if_needed()
    if is_quiet_hours():
        return
    global NEWS_COUNT_TODAY
    if NEWS_COUNT_TODAY >= MAX_NEWS_PER_DAY:
        return

    items = fetch_rss_items()
    posted = 0
    for it in items:
        if posted >= MAX_NEWS_PER_SLOT:
            break
        key = it["link"]
        if key in NEWS_SEEN:
            continue
        NEWS_SEEN.add(key)
        # Format
        header = "[Market Update]"
        body = short(re.sub("<[^>]+>", "", it["summary"] or it["title"]), NEWS_SUMMARY_CHARS)
        text = f"<b>{header}</b> {it['title']}\n\n{body}"
        post(text, link=it["link"], source_name=it["source"])
        posted += 1
        NEWS_COUNT_TODAY += 1
    print(f"NEWS: posted {posted}, total today {NEWS_COUNT_TODAY}")

# --------------- INDICES (Yahoo) ---------------
def yahoo_quotes(symbols):
    try:
        s = ",".join(symbols)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={s}"
        r = requests.get(url, timeout=12)
        data = r.json()["quoteResponse"]["result"]
        out = {}
        for d in data:
            sym = d.get("symbol")
            price = d.get("regularMarketPrice")
            chg   = d.get("regularMarketChange")
            pct   = d.get("regularMarketChangePercent")
            out[sym] = (price, chg, pct)
        return out
    except Exception as e:
        print("YAHOO ERR:", e)
        return {}

def fmt_move(price, chg, pct):
    if price is None:
        return "NA"
    sign = "▲" if (chg or 0) >= 0 else "▼"
    return f"{price:.2f}  {sign} {abs(chg or 0):.2f} ({abs(pct or 0):.2f}%)"

def post_premarket():
    if not ENABLE_MARKET_BLIPS: return
    # GIFT Nifty (use ^NSGX? Not official; we fallback to NA)
    gift = "NA"
    try:
        # unofficial symbol “NIFTY_GIFT” doesn’t exist reliably; keep NA to be safe
        pass
    except:
        pass
    text = (
        "<b>[Premarket]</b>\n"
        f"GIFT Nifty: {gift}\n"
        "Global cues: NA\n"
        "Note: Premarket snapshot."
    )
    post(text)

def post_midday():
    if not ENABLE_MARKET_BLIPS: return
    q = yahoo_quotes(["^BSESN", "^NSEI"])   # Sensex, Nifty 50
    sensex = fmt_move(*(q.get("^BSESN") or (None, None, None)))
    nifty  = fmt_move(*(q.get("^NSEI")  or (None, None, None)))
    bankn  = "NA"  # Yahoo’s BANKNIFTY symbol is unreliable; keep NA if not stable
    text = (
        "<b>[🕘 Midday Check]</b>\n"
        f"Sensex: {sensex}\n"
        f"Nifty 50: {nifty}\n"
        f"Bank Nifty: {bankn}"
    )
    post(text)

def post_close():
    if not ENABLE_MARKET_BLIPS: return
    q = yahoo_quotes(["^BSESN", "^NSEI"])
    sensex = fmt_move(*(q.get("^BSESN") or (None, None, None)))
    nifty  = fmt_move(*(q.get("^NSEI")  or (None, None, None)))
    text = (
        "<b>[Market Close]</b>\n"
        f"Sensex: {sensex}\n"
        f"Nifty 50: {nifty}\n"
        "Sector moves: NA"
    )
    post(text)

# --------------- FII / DII (best-effort) ---------------
def fetch_fii_dii():
    # Try a couple of public endpoints; if all fail, return NA.
    # (NSE often blocks bots; we fail gracefully.)
    try:
        # Placeholder pattern — many sites change URLs frequently.
        # We return NA if blocked.
        return {"date":"Today", "fii":"NA", "dii":"NA", "net":"NA"}
    except Exception as e:
        print("FII/DII ERR:", e)
        return {"date":"Today", "fii":"NA", "dii":"NA", "net":"NA"}

def post_fii_dii():
    if not ENABLE_FII_DII: return
    d = fetch_fii_dii()
    text = (
        "<b>[FII/DII Cash]</b>\n"
        f"Date: {d['date']}\n"
        f"FII: {d['fii']}\n"
        f"DII: {d['dii']}\n"
        f"Net: {d['net']}"
    )
    post(text)

# --------------- IPO (best-effort, separate posts) ---------------
def safe_num(x):
    try:
        return "{:,.0f}".format(float(x))
    except:
        return x or "NA"

def fetch_open_ipos():
    """
    Return a list of dicts with:
     name, price_band, lot_size, issue_size, gmp, sub_day, sub_qib, sub_nII, sub_retail,
     open, close, expected_list_gain
    If we can’t fetch a field, fill 'NA'. If we can’t find any, return [].
    """
    ipos = []
    try:
        # You can later wire this to your preferred source/API.
        # For now we return [] so the bot posts nothing if none.
        # Example shape for each IPO:
        # ipos.append({
        #   "name":"ABC Ltd", "price_band":"₹95–100", "lot_size":"150",
        #   "issue_size":"₹450 Cr", "gmp":"+₹12", "sub_day":"Day 2",
        #   "sub_qib":"1.32x","sub_nII":"0.78x","sub_retail":"2.15x",
        #   "open":"20 Aug 2025", "close":"22 Aug 2025",
        #   "expected_list_gain":"+12%"
        # })
        pass
    except Exception as e:
        print("IPO fetch error:", e)
    return ipos

def post_ipos_full():
    if not ENABLE_IPO: return
    ipos = fetch_open_ipos()
    if not ipos:
        print("IPO: no open IPOs found.")
        return
    for it in ipos:
        lines = [
            f"<b>[IPO]</b> {it.get('name','NA')}",
            f"Price band: {it.get('price_band','NA')}",
            f"Lot size: {it.get('lot_size','NA')}",
            f"Issue size: {it.get('issue_size','NA')}",
            f"GMP: {it.get('gmp','NA')}",
            f"Subscription: {it.get('sub_day','NA')} | QIB {it.get('sub_qib','NA')} | NII {it.get('sub_nII','NA')} | Retail {it.get('sub_retail','NA')}",
            f"Open: {it.get('open','NA')}  Close: {it.get('close','NA')}",
            f"Expected listing gain: {it.get('expected_list_gain','NA')}",
        ]
        post("\n".join(lines))

# --------------- SCHEDULER ---------------
def schedule_jobs():
    # News every X minutes
    if ENABLE_NEWS and NEWS_INTERVAL > 0:
        schedule.every(NEWS_INTERVAL).minutes.do(job_news)

    if ENABLE_MARKET_BLIPS:
        schedule.every().day.at("08:45").do(post_premarket)
        schedule.every().day.at("12:15").do(post_midday)
        schedule.every().day.at("16:05").do(post_close)

    if ENABLE_FII_DII:
        schedule.every().day.at("19:45").do(post_fii_dii)

    if ENABLE_IPO:
        schedule.every().day.at("09:30").do(post_ipos_full)

def run_scheduler():
    schedule_jobs()
    print("Scheduler started.")
    while True:
        now_local = now_ist()
        # schedule library uses local system time; we just tick it each second
        schedule.run_pending()
        time.sleep(1)

# --------------- FLASK (health) ---------------
server = Flask(__name__)

@server.route("/")
def root():
    return "MarketPulse bot alive"

def start_background():
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()

# kick off on import (gunicorn --preload)
start_background()

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
          
