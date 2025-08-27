import os
import re
import logging
from datetime import datetime, time
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# =========================
# Config / ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()

# Optional (safe defaults)
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata").strip()
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))

if not BOT_TOKEN or not CHANNEL_ID:
    raise SystemExit("Missing BOT_TOKEN or CHANNEL_ID environment variables.")

TZ = timezone(TIMEZONE)

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("marketpulse")

# =========================
# Telegram
# =========================
bot = Bot(token=BOT_TOKEN)

def send_message(text: str, link: str = None) -> None:
    """Sync send (PTB 13.x). One optional 'Read more' button."""
    try:
        if link:
            kb = [[InlineKeyboardButton("📖 Read more", url=link)]]
            bot.send_message(chat_id=CHANNEL_ID, text=text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text)
    except Exception as e:
        log.exception(f"send_message failed: {e}")

# =========================
# Helpers
# =========================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
})

def get_html(url: str, timeout: int = 12) -> str:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        log.warning(f"GET {url} failed: {e}")
    return ""

def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s

def within_window(now_local: datetime) -> bool:
    """08:30–21:30 inclusive."""
    return time(8, 30) <= now_local.time() <= time(21, 30)

# =========================
# News scraping (2 posts / slot)
# =========================
SKIP_IN_HREF = (
    "epaper", "subscribe", "live-tv", "podcast", "photo", "video",
    "masthead", "about", "privacy", "terms", "careers"
)

def _looks_ok(link: str, title: str) -> bool:
    if not link or not title:
        return False
    L = link.lower()
    if any(bad in L for bad in SKIP_IN_HREF):
        return False
    # Keep only clear market/business news
    return any(x in L for x in ("/markets", "/market", "/news/business/markets"))

def summarize_article(url: str) -> str:
    """Fetch article body; fall back to truncated title-only summary."""
    html = get_html(url)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    paragraphs = [clean_text(p.get_text()) for p in soup.find_all("p")]
    body = " ".join(p for p in paragraphs if len(p) > 40)
    return (body[:NEWS_SUMMARY_CHARS] + "…") if len(body) > NEWS_SUMMARY_CHARS else body

def scrape_from(url: str, a_selector: str) -> List[Tuple[str, str]]:
    """Return list of (title, absolute_link)."""
    base = url.rstrip("/")
    html = get_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select(a_selector):
        title = clean_text(a.get_text())
        link = a.get("href") or ""
        if not link.startswith("http"):
            link = base + "/" + link.lstrip("/")
        if _looks_ok(link, title) and 25 <= len(title) <= 160:
            out.append((title, link))
    return out

def collect_news() -> List[Dict]:
    items: List[Dict] = []

    # Economic Times (markets)
    try:
        for t, l in scrape_from(
            "https://economictimes.indiatimes.com/markets",
            "a"
        ):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning(f"ET scrape failed: {e}")

    # Moneycontrol markets
    try:
        for t, l in scrape_from(
            "https://www.moneycontrol.com/news/business/markets/",
            "a"
        ):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning(f"MC scrape failed: {e}")

    # Mint (market)
    try:
        for t, l in scrape_from(
            "https://www.livemint.com/market",
            "a"
        ):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning(f"Mint scrape failed: {e}")

    # Deduplicate by link
    dedup, seen = [], set()
    for it in items:
        if it["link"] not in seen:
            seen.add(it["link"])
            dedup.append(it)
    return dedup

POSTED_TODAY = set()

def reset_daily_memory():
    POSTED_TODAY.clear()
    log.info("Daily memory cleared.")

def post_two_news():
    now_local = datetime.now(TZ)
    if not within_window(now_local):
        return

    items = collect_news()
    posted = 0

    for it in items:
        if posted >= 2:
            break
        link = it["link"]
        if link in POSTED_TODAY:
            continue

        title = it["title"]
        summary = summarize_article(link)
        if not summary:
            summary = title  # fallback

        text = f"📰 {title}\n\n{summary}"
        send_message(text, link=link)
        POSTED_TODAY.add(link)
        posted += 1

    if posted == 0:
        send_message("📰 No fresh market headlines at the moment. Will try again in the next slot.")

# =========================
# IPO (with GMP from Chittorgarh)
# =========================
ALLOW_SME = os.getenv("ALLOW_SME_IPO", "0").strip() == "1"

def fetch_ipo_gmp():
    """Parse IPO table from Chittorgarh (best effort)."""
    url = "https://www.chittorgarh.com/report/latest-ipo-gmp/56/"
    html = get_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    ipos = []
    for tr in table.find_all("tr")[1:]:
        tds = [clean_text(td.get_text()) for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        name, price, dates, gmp = tds[0], tds[1], tds[2], tds[3]
        # crude SME filter
        is_sme = "sme" in name.lower()
        if is_sme and not ALLOW_SME:
            continue
        ipos.append({
            "name": name,
            "price": price,
            "dates": dates,
            "gmp": gmp
        })
    return ipos[:3]

def post_ipo():
    data = fetch_ipo_gmp()
    if not data:
        send_message("📌 IPO\nCouldn’t confirm today’s mainboard IPOs right now. Will retry later.")
        return
    for d in data:
        txt = (
            "📌 IPO Update\n\n"
            f"{d['name']}\n"
            f"Price Band: {d['price']}\n"
            f"Dates: {d['dates']}\n"
            f"GMP: {d['gmp']}"
        )
        send_message(txt)

# =========================
# Pre / Post market & FII/DII (best-effort scrapes with safe fallback)
# =========================
def try_scrape_premarket_summary() -> str:
    # Best-effort headline pull; if missing, return ""
    url = "https://www.moneycontrol.com/news/business/markets/"
    html = get_html(url)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Pick something like "Market setup/10 things before market…"
    for a in soup.find_all("a"):
        t = clean_text(a.get_text())
        if re.search(r"(market setup|ahead of market|things to know|opening bell)", t, re.I):
            return t
    return ""

def post_premarket():
    head = try_scrape_premarket_summary()
    if head:
        send_message(f"🌅 Pre-Market\n\n{head}")
    else:
        send_message("🌅 Pre-Market\n\nKey cues, global signals and bulk deals summary will be updated shortly.")

def try_scrape_postmarket_summary() -> str:
    url = "https://www.moneycontrol.com/news/business/markets/"
    html = get_html(url)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        t = clean_text(a.get_text())
        if re.search(r"(closing bell|market highlights|post\-market|wrap)", t, re.I):
            return t
    return ""

def post_postmarket():
    head = try_scrape_postmarket_summary()
    if head:
        send_message(f"🌇 Post-Market\n\n{head}")
    else:
        send_message("🌇 Post-Market\n\nIndices summary and top movers will be posted when available.")

def try_scrape_fii_dii() -> str:
    # Very simple pull; use your preferred reliable source later.
    url = "https://www.moneycontrol.com/news/business/markets/"
    html = get_html(url)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        t = clean_text(a.get_text())
        if re.search(r"(FII|DII).*(buy|sell|net)", t, re.I):
            return t
    return ""

def post_fii_dii():
    line = try_scrape_fii_dii()
    if line:
        send_message(f"🏦 FII/DII\n\n{line}")
    else:
        send_message("🏦 FII/DII\n\nOfficial cash market numbers aren’t published yet. I’ll post when they drop.")

# =========================
# Scheduler (APScheduler + Cron)
# =========================
sched = BackgroundScheduler(timezone=TZ)

def schedule_jobs():
    # Two news posts every 30 mins between 08:30–21:30 (all days)
    # At minute 0 and 30; function itself checks time window again.
    sched.add_job(post_two_news, CronTrigger(minute="0,30", day_of_week="mon-sun"))

    # Weekdays only fixed posts
    sched.add_job(post_premarket,  CronTrigger(hour=9,  minute=0,  day_of_week="mon-fri"))
    sched.add_job(post_ipo,        CronTrigger(hour=10, minute=30, day_of_week="mon-fri"))
    sched.add_job(post_ipo,        CronTrigger(hour=11, minute=0,  day_of_week="mon-fri"))  # fallback
    sched.add_job(post_postmarket, CronTrigger(hour=15, minute=45, day_of_week="mon-fri"))
    sched.add_job(post_fii_dii,    CronTrigger(hour=21, minute=0,  day_of_week="mon-fri"))

    # Midnight memory reset
    sched.add_job(reset_daily_memory, CronTrigger(hour=0, minute=5, day_of_week="mon-sun"))

# =========================
# Flask (to satisfy Render Web Service)
# =========================
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

def announce_start():
    send_message(
        "✅ MarketPulse bot restarted and schedule loaded.\n"
        "Window: 08:30–21:30 • Every 30 min (2 posts/slot)\n"
        "Weekdays: 09:00 Pre-market • 10:30/11:00 IPO • 15:45 Post-market • 21:00 FII/DII"
    )

def main():
    schedule_jobs()
    sched.start()
    log.info("Scheduler started.")
    announce_start()

if __name__ == "__main__":
    main()
