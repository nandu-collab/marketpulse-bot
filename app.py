import os
import asyncio
import logging
from datetime import datetime, time
import requests
from bs4 import BeautifulSoup
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ENV variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = Bot(token=BOT_TOKEN)

# =============== NEWS SOURCES ===============
NEWS_SOURCES = [
    "https://economictimes.indiatimes.com/markets",
    "https://www.moneycontrol.com/news/business/markets/",
    "https://www.livemint.com/market"
]

# =============== HELPERS ===============
async def send_message(text, link=None):
    """Send message with optional read more link."""
    try:
        if link:
            keyboard = [[InlineKeyboardButton("📖 Read more", url=link)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await bot.send_message(chat_id=CHANNEL_ID, text=text, reply_markup=reply_markup)
        else:
            await bot.send_message(chat_id=CHANNEL_ID, text=text)
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def fetch_html(url):
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
    return None

# =============== NORMAL NEWS ===============
def scrape_news():
    news_items = []
    for url in NEWS_SOURCES:
        html = fetch_html(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        headlines = soup.find_all("a")[:3]  # get top 3
        for h in headlines:
            title = h.get_text().strip()
            link = h.get("href")
            if title and link:
                if not link.startswith("http"):
                    link = url + link
                summary = title[:550]  # summary limit
                news_items.append((title, summary, link))
    return news_items[:2]  # 2 news per slot

async def post_normal_news():
    news = scrape_news()
    if not news:
        await send_message("📰 No fresh news available right now.")
        return
    for title, summary, link in news:
        text = f"📰 {title}\n\n{summary}"
        await send_message(text, link)

# =============== PRE/POST MARKET ===============
async def post_premarket():
    await send_message("🌅 Pre-Market Report\n\n(Example data — replace with real API if available)")

async def post_postmarket():
    await send_message("🌇 Post-Market Report\n\n(Example data — replace with real API if available)")

# =============== FII/DII DATA ===============
async def post_fii_dii():
    await send_message("🏦 FII/DII Data\n\n(Example data — replace with real API if available)")

# =============== IPO UPDATES ===============
def fetch_ipo_data():
    html = fetch_html("https://www.chittorgarh.com/report/latest-ipo-gmp/56/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tr")
    ipo_list = []
    for row in rows[1:3]:  # top 2 IPOs
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) >= 4:
            name = cols[0]
            price = cols[1]
            dates = cols[2]
            gmp = cols[3]
            ipo_list.append((name, price, dates, gmp))
    return ipo_list

async def post_ipo():
    ipos = fetch_ipo_data()
    if not ipos:
        await send_message("📌 IPO\nCouldn’t confirm today’s IPOs right now.")
        return
    for name, price, dates, gmp in ipos:
        text = f"📌 IPO Update\n\n{name}\nPrice Band: {price}\nDates: {dates}\nGMP: {gmp}"
        await send_message(text)

# =============== SCHEDULER ===============
async def scheduler():
    while True:
        now = datetime.now()
        weekday = now.weekday()  # 0=Mon, 6=Sun

        # Weekdays
        if weekday < 5:
            # Pre-market (9:00 AM)
            if now.time().hour == 9 and now.time().minute == 0:
                await post_premarket()

            # IPO (10:30 AM, 11:00 AM check)
            if now.time().hour in [10, 11] and now.time().minute == 30:
                await post_ipo()

            # Post-market (3:45 PM)
            if now.time().hour == 15 and now.time().minute == 45:
                await post_postmarket()

            # FII/DII (8:00 PM)
            if now.time().hour == 20 and now.time().minute == 0:
                await post_fii_dii()

        # Normal news (weekdays + weekends, every 30min between 8:30am–9:30pm)
        if time(8, 30) <= now.time() <= time(21, 30) and now.minute % 30 == 0:
            await post_normal_news()

        await asyncio.sleep(60)  # check every minute

# =============== MAIN ===============
if __name__ == "__main__":
    logger.info("Bot started...")
    asyncio.run(scheduler())
