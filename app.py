# app.py  — MarketPulse final
import os
import re
import logging
from datetime import datetime, time
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from pytz import timezone as pytz_timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# -----------------------
# Config / ENV
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()   # -100xxxxxxxxxx
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata").strip()
NEWS_SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))
ALLOW_SME_IPO = os.getenv("ALLOW_SME_IPO", "0").strip() == "1"

if not BOT_TOKEN or not CHANNEL_ID:
    raise SystemExit("Missing BOT_TOKEN or CHANNEL_ID environment variables.")

TZ = pytz_timezone(TIMEZONE)

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("marketpulse")

# -----------------------
# Telegram
# -----------------------
bot = Bot(token=BOT_TOKEN)

def send_message(text: str, link: str = None) -> None:
    """Synchronous send. If link provided, add one 'Read more' button."""
    try:
        if link:
            kb = [[InlineKeyboardButton("📖 Read more", url=link)]]
            bot.send_message(chat_id=CHANNEL_ID, text=text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            bot.send_message(chat_id=CHANNEL_ID, text=text)
        log.info("Sent message (len=%d) ...", len(text))
    except Exception as e:
        log.exception("send_message failed: %s", e)

# -----------------------
# HTTP helper
# -----------------------
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
        log.warning("GET %s failed: %s", url, e)
    return ""

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def within_window(now_local: datetime) -> bool:
    """08:30–21:30 inclusive window for normal news."""
    return time(8, 30) <= now_local.time() <= time(21, 30)

# -----------------------
# News scraping (two separate posts per slot)
# -----------------------
SKIP_IN_HREF = ("epaper", "subscribe", "live-tv", "podcast", "photo", "video", "epaper",
                "masthead", "about", "privacy", "terms", "careers")

def _looks_ok(link: str, title: str) -> bool:
    if not link or not title:
        return False
    L = link.lower()
    if any(bad in L for bad in SKIP_IN_HREF):
        return False
    # quick filter to focus market/business related pages
    return any(x in L for x in ("/markets", "/market", "/news/business", "/news/markets", "/market/"))

def summarize_article(url: str) -> str:
    html = get_html(url)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    paragraphs = [clean_text(p.get_text()) for p in soup.find_all("p")]
    body = " ".join(p for p in paragraphs if len(p) > 40)
    if not body:
        # fallback: pull meta description
        desc = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property":"og:description"})
        if desc and desc.get("content"):
            body = clean_text(desc["content"])
    if not body:
        return ""
    return (body[:NEWS_SUMMARY_CHARS] + "…") if len(body) > NEWS_SUMMARY_CHARS else body

def scrape_from(url: str, a_selector: str="a") -> List[Tuple[str, str]]:
    base = url.rstrip("/")
    html = get_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select(a_selector):
        title = clean_text(a.get_text())
        link = a.get("href") or ""
        if link and not link.startswith("http"):
            link = base + "/" + link.lstrip("/")
        if _looks_ok(link, title) and 25 <= len(title) <= 160:
            out.append((title, link))
    return out

def collect_news() -> List[Dict]:
    items: List[Dict] = []
    # EconomicTimes
    try:
        for t, l in scrape_from("https://economictimes.indiatimes.com/markets"):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning("ET scrape failed: %s", e)
    # Moneycontrol
    try:
        for t, l in scrape_from("https://www.moneycontrol.com/news/business/markets/"):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning("MC scrape failed: %s", e)
    # LiveMint
    try:
        for t, l in scrape_from("https://www.livemint.com/market"):
            items.append({"title": t, "link": l})
    except Exception as e:
        log.warning("Mint scrape failed: %s", e)
    # dedupe by link
    dedup = []
    seen = set()
    for it in items:
        if it["link"] and it["link"] not in seen:
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
        log.info("Outside normal news window, skipping normal news post.")
        return
    items = collect_news()
    posted = 0
    for it in items:
        if posted >= 2:
            break
        link = it["link"]
        if not link or link in POSTED_TODAY:
            continue
        title = it["title"]
        summary = summarize_article(link)
        if not summary:
            summary = title
        text = f"📰 {title}\n\n{summary}"
        send_message(text, link=link)
        POSTED_TODAY.add(link)
        posted += 1
    if posted == 0:
        send_message("📰 No fresh market headlines at the moment. Will try again in the next slot.")

# -----------------------
# IPO (Chittorgarh) — mainboard only by default
# -----------------------
def fetch_ipo_gmp():
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
        tds = [clean_text(td.get_text()) for td in row.find_all("td")]

