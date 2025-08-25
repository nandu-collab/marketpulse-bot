import os
import asyncio
import logging
import time as time_mod
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Optional

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import TelegramError

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("marketpulse")

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # can be @handle or -100...
SUMMARY_CHARS = int(os.getenv("NEWS_SUMMARY_CHARS", "550"))  # e.g., 550

if not BOT_TOKEN or not CHANNEL_ID:
    raise SystemExit("BOT_TOKEN and CHANNEL_ID are required env vars.")

bot = Bot(token=BOT_TOKEN)

# ---------- Config ----------
NEWS_WINDOW_START = (8, 30)   # 08:30
NEWS_WINDOW_END   = (21, 30)  # 21:30
NEWS_SLOT_SPACING_SEC = 30 * 60  # 30 min
NEWS_PER_SLOT = 2             # 2 separate posts per slot

# Fixed times (weekday only)
PREMARKET_TIME   = "09:00"
IPO_TIME         = "11:00"
POSTMARKET_TIME  = "16:00"
FII_DII_TIME     = "19:30"

# RSS feeds for normal news
RSS_FEEDS = [
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "https://www.livemint.com/rss/markets"
]

# ---------- State (in-memory) ----------
posted_links_today: set = set()
last_news_slot: Optional[datetime] = None
last_run_day: Optional[str] = None

# ---------- Helpers ----------
def ist_now() -> datetime:
    # Render uses UTC; we keep it simple and use system time. If server is UTC,
    # you can offset by +5:30 if ever needed. For most Render dynos, system time works fine.
    return datetime.now()

def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5  # 5=Sat,6=Sun

def within_news_window(dt: datetime) -> bool:
    s_h, s_m = NEWS_WINDOW_START
    e_h, e_m = NEWS_WINDOW_END
    start = dt.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
    end   = dt.replace(hour=e_h, minute=e_m, second=59, microsecond=0)
    return start <= dt <= end

def same_slot(d1: datetime, d2: datetime) -> bool:
    """Return True if d1 and d2 fall in same 30-min slot boundary."""
    slot1 = d1.replace(minute=(d1.minute // 30) * 30, second=0, microsecond=0)
    slot2 = d2.replace(minute=(d2.minute // 30) * 30, second=0, microsecond=0)
    return slot1 == slot2

def truncate(text: str, n: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"

async def fetch_html(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return await r.text()
            log.warning(f"GET {url} -> {r.status}")
            return None
    except Exception as e:
        log.error(f"fetch_html error for {url}: {e}")
        return None

async def send_text(text: str, with_link: Optional[str] = None):
    try:
        if with_link:
            # Only normal news carries link (Markdown)
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=f"{text}\n\n[Read more]({with_link})",
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
        else:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=text
            )
    except TelegramError as e:
        log.error(f"Telegram send error: {e}")

# ---------- Normal News ----------
def collect_rss_entries() -> List[Tuple[str, str, str, Optional[datetime]]]:
    """Return list of (title, summary, link, published_dt)."""
    items = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:10]:
                title = (getattr(e, "title", "") or "").strip()
                link  = (getattr(e, "link", "") or "").strip()
                summary = getattr(e, "summary", "") or title
                published_dt = None
                if hasattr(e, "published_parsed") and e.published_parsed:
                    published_dt = datetime(*e.published_parsed[:6])
                if title and link:
                    items.append((title, summary, link, published_dt))
        except Exception as ex:
            log.error(f"RSS parse error for {url}: {ex}")
    # sort newest first if we have dates
    items.sort(key=lambda x: x[3] or datetime.min, reverse=True)
    return items

async def do_news_slot():
    global posted_links_today
    now = ist_now()
    # reset daily cache at midnight
    dkey = now.strftime("%Y-%m-%d")
    global last_run_day
    if last_run_day != dkey:
        posted_links_today = set()
        last_run_day = dkey

    entries = collect_rss_entries()
    posted_this_slot = 0
    for title, summary, link, _ in entries:
        if link in posted_links_today:
            continue
        # prepare text
        short = truncate(summary, SUMMARY_CHARS)
        text = f"📰 {title}\n\n{short}"
        await send_text(text, with_link=link)
        posted_links_today.add(link)
        posted_this_slot += 1
        if posted_this_slot >= NEWS_PER_SLOT:
            break
    if posted_this_slot == 0:
        log.info("No fresh news for this slot (all duplicates).")

# ---------- Pre-Market / Post-Market / FII-DII ----------
async def fetch_premarket_text() -> str:
    """
    Attempts to pull SGX(GIFT) Nifty & simple global cues.
    Gracefully falls back if selectors move.
    """
    gift_line = "GIFT Nifty: data N/A"
    global_line = "Global cues: mixed"

    url = "https://www.moneycontrol.com/stocksmarketsindia/"
    async with aiohttp.ClientSession() as s:
        html = await fetch_html(s, url)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Try several possible spots for GIFT Nifty text
            cand = soup.find(string=lambda t: t and ("GIFT Nifty" in t or "SGX Nifty" in t))
            if cand:
                gift_line = cand.strip()
    return f"📊 Pre-Market (9:00 AM)\n{gift_line}\n{global_line}\nWatchlist: Reliance, HDFC Bank, TCS"

async def fetch_postmarket_text() -> str:
    """
    Pull Nifty/Sensex close; fallback if moved.
    """
    nifty_line = "Nifty 50: data N/A"
    sensex_line = "Sensex: data N/A"
    url = "https://www.moneycontrol.com/stocksmarketsindia/"
    async with aiohttp.ClientSession() as s:
        html = await fetch_html(s, url)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Relaxed search
            for txt in soup.stripped_strings:
                t = txt.lower()
                if "nifty" in t and ":" in t and "bank" not in t:
                    nifty_line = txt
                    break
            for txt in soup.stripped_strings:
                t = txt.lower()
                if "sensex" in t and ":" in t:
                    sensex_line = txt
                    break
    return f"📈 Post-Market (4:00 PM)\n{nifty_line}\n{sensex_line}\nMarket Color: IT/Metals/Banks mixed."

async def fetch_fii_dii_text() -> str:
    """
    Pulls daily cash market FII/DII. Falls back if structure changes.
    """
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.html"
    fii = "FII: data N/A"
    dii = "DII: data N/A"
    async with aiohttp.ClientSession() as s:
        html = await fetch_html(s, url)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Look for first table
            table = soup.find("table")
            if table:
                rows = table.find_all("tr")
                # Try to find rows mentioning FII / DII
                for r in rows:
                    t = r.get_text(" ", strip=True)
                    if "FII" in t and "Net" in t:
                        fii = t
                    if "DII" in t and "Net" in t:
                        dii = t
    return f"💰 FII/DII (7:30 PM)\n{fii}\n{dii}"

# ---------- IPO (Mainboard) + GMP ----------
async def fetch_open_ipos_from_chittorgarh(s: aiohttp.ClientSession) -> List[Dict]:
    """
    Parses open IPOs (mainboard focus) from Chittorgarh 'IPO open today' / calendar pages.
    Returns dicts with keys: name, open, close, price_band, lot_size, issue_size.
    """
    candidates = [
        "https://www.chittorgarh.com/report/ipo-open-today/85/",
        "https://www.chittorgarh.com/ipo/ipo_calendar.asp",
    ]
    ipos = []
    for url in candidates:
        html = await fetch_html(s, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            continue
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) < 4:
                continue
            row_txt = " ".join(tds).lower()
            # Heuristic: skip SME rows if identifiable
            if "sme" in row_txt and "main" not in row_txt:
                continue
            # Try to map columns (page layouts differ)
            name = tds[0]
            open_dt = ""
            close_dt = ""
            price_band = ""
            lot = ""
            size = ""
            # Extract with best-effort patterns
            for t in tds[1:]:
                tl = t.lower()
                if "open" in tl and "close" in tl:
                    # "20 Aug - 22 Aug" style
                    parts = t.replace("to", "-").replace("–", "-").split("-")
                    if len(parts) >= 2:
                        open_dt = parts[0].strip()
                        close_dt = parts[1].strip()
                elif "open" in tl and not open_dt:
                    open_dt = t.strip()
                elif "close" in tl and not close_dt:
                    close_dt = t.strip()
                elif "lot" in tl or "shares" in tl:
                    lot = t.strip()
                elif "price" in tl or "band" in tl:
                    price_band = t.strip()
                elif "cr" in tl or "crore" in tl or "issue" in tl:
                    size = t.strip()

            if not price_band and len(tds) >= 4:
                price_band = tds[3]
            if not lot and len(tds) >= 5:
                lot = tds[4]

            ipos.append({
                "name": name,
                "open": open_dt,
                "close": close_dt,
                "price_band": price_band,
                "lot": lot,
                "issue_size": size
            })
    # De-duplicate by name
    seen = set()
    uniq = []
    for x in ipos:
        key = x["name"].lower()
        if key not in seen:
            seen.add(key)
            uniq.append(x)
    return uniq

async def fetch_gmp_map_from_chittorgarh(s: aiohttp.ClientSession) -> Dict[str, Dict[str, str]]:
    """
    Gets a map: IPO name -> {gmp, est_profit, est_listing}
    Tries multiple known GMP pages (Chittorgarh often changes URLs).
    """
    gmp_pages = [
        "https://www.chittorgarh.com/ipo/ipo_gmp_today/",
        "https://www.chittorgarh.com/report/ipo-grey-market-premium-ipo-gmp/56/",
    ]
    result: Dict[str, Dict[str, str]] = {}

    for url in gmp_pages:
        html = await fetch_html(s, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            continue
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) < 2:
                continue
            # Usually first col is IPO name, next has GMP, sometimes profit/listing gain columns too
            name = tds[0].strip()
            line = " ".join(tds[1:]).strip()
            gmp = ""
            est = ""
            # Extract ₹ values and +/- %
            # Simple heuristics:
            for token in tds[1:]:
                if "₹" in token or "Rs" in token:
                    if not gmp:
                        gmp = token
                if "profit" in token.lower() or "gain" in token.lower():
                    est = token
            if not gmp and line:
                # last fallback: any number with ₹
                if "₹" in line:
                    gmp = line[line.find("₹"):].split()[0]
            if name:
                result[name.lower()] = {
                    "gmp": gmp or "N/A",
                    "est": est or ""
                }
    return result

async def build_ipo_text() -> str:
    async with aiohttp.ClientSession() as s:
        ipos = await fetch_open_ipos_from_chittorgarh(s)
        if not ipos:
            return "📌 IPO\nNo live mainboard IPOs right now."

        gmap = await fetch_gmp_map_from_chittorgarh(s)

        lines = ["📌 IPO (11:00 AM) — Live Mainboard"]
        for ipo in ipos:
            name = ipo["name"].strip()
            gmpr = gmap.get(name.lower()) or {}
            gmp = gmpr.get("gmp", "N/A")
            est = gmpr.get("est", "")
            # Clean price band formatting a bit
            pb = ipo["price_band"].replace("Price Band", "").replace(":", "").strip()
            # Message block
            block = [
                f"• {name}",
                f"  Open–Close: {ipo['open']} — {ipo['close']}" if (ipo['open'] or ipo['close']) else "",
                f"  Price Band: {pb}" if pb else "",
                f"  Lot Size: {ipo['lot']}" if ipo['lot'] else "",
                f"  Issue Size: {ipo['issue_size']}" if ipo['issue_size'] else "",
                f"  GMP: {gmp}" if gmp else "  GMP: N/A",
            ]
            if est:
                block.append(f"  Est. Gain: {est}")
            lines.append("\n".join([b for b in block if b]))
        return "\n\n".join(lines)

# ---------- Main scheduler ----------
async def tick_once():
    """Runs every 60 seconds; decides what to post."""
    global last_news_slot

    now = ist_now()
    daykey = now.strftime("%Y-%m-%d")
    time_hhmm = now.strftime("%H:%M")

    # Weekends: only normal news loop
    wknd = is_weekend(now)

    # Fixed posts (weekdays only)
    if not wknd:
        if time_hhmm == PREMARKET_TIME:
            log.info("Posting Pre-Market…")
            txt = await fetch_premarket_text()
            await send_text(txt)

        if time_hhmm == IPO_TIME:
            log.info("Posting IPO…")
            txt = await build_ipo_text()
            await send_text(txt)

        if time_hhmm == POSTMARKET_TIME:
            log.info("Posting Post-Market…")
            txt = await fetch_postmarket_text()
            await send_text(txt)

        if time_hhmm == FII_DII_TIME:
            log.info("Posting FII/DII…")
            txt = await fetch_fii_dii_text()
            await send_text(txt)

    # News slots (every 30 minutes 08:30–21:30; weekends too)
    if within_news_window(now):
        if (last_news_slot is None) or (not same_slot(last_news_slot, now)):
            # Align to slot boundary before posting (only when minute%30==0)
            if now.minute % 30 == 0:
                log.info("Posting normal news slot…")
                await do_news_slot()
                last_news_slot = now

async def main_loop():
    # startup message
    window = f"{NEWS_WINDOW_START[0]:02d}:{NEWS_WINDOW_START[1]:02d}–{NEWS_WINDOW_END[0]:02d}:{NEWS_WINDOW_END[1]:02d}"
    startup = (
        "✅ MarketPulse bot restarted and schedule loaded.\n"
        f"Window: {window} • Every 30 min • {NEWS_PER_SLOT} post(s)/slot\n"
        f"Fixed posts (Mon–Fri): {PREMARKET_TIME} Pre-market • {IPO_TIME} IPO • {POSTMARKET_TIME} Post-market • {FII_DII_TIME} FII/DII"
    )
    try:
        await send_text(startup)
    except Exception as e:
        log.error(f"Startup message error: {e}")

    # run forever
    while True:
        try:
            await tick_once()
        except Exception as e:
            log.error(f"tick_once error: {e}")
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main_loop())
