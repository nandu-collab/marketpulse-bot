import asyncio
import logging
import schedule
import time
from telegram import Bot
from telegram.constants import ParseMode
import requests
import pytz
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Configuration ---
BOT_TOKEN = "YOUR_BOT_TOKEN"
CHANNEL_ID = "@YOUR_CHANNEL_ID"  # Or numeric ID
TIMEZONE = pytz.timezone("Asia/Kolkata")

bot = Bot(token=BOT_TOKEN)

# --- Fetch news function (dummy example, replace with real API) ---
def fetch_news():
    return [
        {
            "title": "Nifty Closes Higher",
            "summary": "Nifty ended the session up by 150 points amid global cues.",
            "url": "https://example.com/nifty-news"
        },
        {
            "title": "FII and DII Data Update",
            "summary": "FII bought ₹500 Cr and DII sold ₹300 Cr in today's session.",
            "url": "https://example.com/fii-dii"
        }
    ]

# --- Async function to post news ---
async def post_news():
    news_items = fetch_news()
    for item in news_items:
        message = f"**{item['title']}**\n{item['summary']}\n[Read More]({item['url']})"
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN
        )
    log.info(f"News posted at {datetime.now(TIMEZONE)}")

# --- Job runner for schedule ---
def run_scheduled():
    asyncio.run(post_news())

# --- Main Entry ---
async def main():
    me = await bot.get_me()
    log.info(f"Bot OK: @{me.username} ({me.id})")

    # Schedule posting every 30 minutes
    schedule.every(30).minutes.do(run_scheduled)

    # Keep bot running
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
