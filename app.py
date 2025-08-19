import os
import time
import requests
import schedule
from datetime import datetime

# Telegram details
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Settings
NEWS_INTERVAL = int(os.getenv("NEWS_INTERVAL", 30))  # default 30 minutes
MAX_NEWS = int(os.getenv("MAX_NEWS", 50))            # default 50 news per day

# Internal counter
news_count = 0

# ===================== Helper Functions ===================== #

def send_message(text):
    """Send message to Telegram channel"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print("Error sending message:", e)


def fetch_news():
    """Dummy example: Replace with your actual news API call"""
    return [
        {"title": "Market Update", "link": "https://example.com/news1"},
        {"title": "Global Impact", "link": "https://example.com/news2"},
        {"title": "Company Earnings", "link": "https://example.com/news3"},
    ]


def fetch_premarket():
    """Dummy example for Gift Nifty"""
    return "📊 Gift Nifty indicates a positive start with +75 points."


def fetch_ipo_details():
    """Dummy example IPOs (replace with API or scraping later)"""
    return [
        {
            "name": "ABC IPO",
            "price": "₹200 - ₹210",
            "lot": "70 shares",
            "issue_size": "₹1,000 Cr",
            "gmp": "+₹50",
            "sub": "2.5x",
            "open": "19 Aug 2025",
            "close": "21 Aug 2025"
        },
        {
            "name": "XYZ IPO",
            "price": "₹450 - ₹460",
            "lot": "30 shares",
            "issue_size": "₹500 Cr",
            "gmp": "+₹120",
            "sub": "1.8x",
            "open": "20 Aug 2025",
            "close": "22 Aug 2025"
        }
    ]


def fetch_market_close():
    """Dummy market close data"""
    return "📉 Sensex: 76,120 (-0.45%)\n📉 Nifty: 23,050 (-0.52%)\n📉 BankNifty: 49,200 (-0.80%)"


def fetch_fii_dii():
    """Dummy FII/DII data"""
    return "FII: +₹1,250 Cr\nDII: -₹800 Cr"

# ===================== Tasks ===================== #

def post_news():
    global news_count
    if news_count >= MAX_NEWS:
        return
    news_items = fetch_news()[:2]  # max 2 news at a time
    for item in news_items:
        send_message(f"📰 <b>{item['title']}</b>\n🔗 {item['link']}")
        news_count += 1


def post_premarket():
    data = fetch_premarket()
    send_message(f"🌅 <b>Pre-Market Update</b>\n{data}")


def post_ipos():
    ipos = fetch_ipo_details()
    for ipo in ipos:
        msg = (
            f"💰 <b>{ipo['name']} IPO Details</b>\n\n"
            f"📌 Price Band: {ipo['price']}\n"
            f"📌 Lot Size: {ipo['lot']}\n"
            f"📌 Issue Size: {ipo['issue_size']}\n"
            f"📌 GMP: {ipo['gmp']}\n"
            f"📌 Subscription: {ipo['sub']}\n"
            f"📌 Open: {ipo['open']}\n"
            f"📌 Close: {ipo['close']}"
        )
        send_message(msg)


def post_market_close():
    data = fetch_market_close()
    send_message(f"🔔 <b>Market Close</b>\n{data}")


def post_fii_dii():
    data = fetch_fii_dii()
    send_message(f"🏦 <b>FII/DII Data</b>\n{data}")

# ===================== Scheduler ===================== #

schedule.every(NEWS_INTERVAL).minutes.do(post_news)
schedule.every().day.at("08:15").do(post_premarket)
schedule.every().day.at("09:30").do(post_ipos)
schedule.every().day.at("20:30").do(post_market_close)
schedule.every().day.at("20:45").do(post_fii_dii)

# ===================== Main Loop ===================== #

while True:
    schedule.run_pending()
    time.sleep(30)
# Reset news counter every midnight
def reset_daily_limit():
    global news_count
    news_count = 0
    print("🔄 News counter reset for new day")

schedule.every().day.at("00:01").do(reset_daily_limit)

