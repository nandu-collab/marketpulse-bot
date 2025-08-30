import os
import logging
import pytz
import schedule
import time
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from fetch_news import get_market_news, get_pre_market_brief, get_post_market_brief, get_fii_dii_data, get_ipo_updates

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

bot = Bot(token=BOT_TOKEN)
tz = pytz.timezone(TIMEZONE)

def send_message(text):
    try:
        bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except TelegramError as e:
        logger.error(f"Error sending message: {e}")

def job_market_news():
    news_items = get_market_news()
    for item in news_items:
        text = f"📈 <b>{item['headline']}</b>\n\n{item['summary']}\n<a href='{item['link']}'>Read • Source</a>"
        send_message(text)

def job_pre_market():
    text = get_pre_market_brief()
    send_message(f"🔹 <b>Pre-Market Brief</b>\n\n{text}")

def job_post_market():
    text = get_post_market_brief()
    send_message(f"🔹 <b>Post-Market Brief</b>\n\n{text}")

def job_fii_dii():
    text = get_fii_dii_data()
    send_message(f"💰 <b>FII/DII Data</b>\n\n{text}")

def job_ipo_updates():
    text = get_ipo_updates()
    send_message(f"📢 <b>IPO Desk</b>\n\n{text}")

# Scheduler setup
schedule.every().hour.at(":00").do(job_market_news)
schedule.every().hour.at(":30").do(job_market_news)

schedule.every().day.at("09:05").do(job_pre_market)
schedule.every().day.at("16:10").do(job_post_market)
schedule.every().day.at("19:30").do(job_fii_dii)
schedule.every().day.at("10:30").do(job_ipo_updates)

if __name__ == "__main__":
    logger.info("Bot is running...")
    while True:
        schedule.run_pending()
        time.sleep(30)
