import os
import time
import requests
import schedule
from bs4 import BeautifulSoup
from datetime import datetime

# === ENVIRONMENT VARIABLES ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
CUELINKS_ID = os.getenv("CUELINKS_ID")

NEWS_INTERVAL = 1800  # 30 min

# === TELEGRAM POST FUNCTION ===
def send_telegram_post(message, buttons=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": [[{"text": "Read more", "url": buttons}]]}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print("Error posting to Telegram:", e)

# === FETCH GENERAL NEWS ===
def fetch_general_news():
    try:
        url = "https://economictimes.indiatimes.com/markets"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        articles = soup.select("div.eachStory")
        news_items = []
        for a in articles[:2]:  # take 2 news each 30 min
            headline = a.find("h3").get_text(strip=True)
            summary = a.find("p").get_text(strip=True)[:550]
            link = "https://economictimes.indiatimes.com" + a.find("a")["href"]
            msg = f"📰 <b>{headline}</b>\n\n{summary}"
            news_items.append((msg, link))

        for news, link in news_items:
            send_telegram_post(news, link)

    except Exception as e:
        print("Error fetching general news:", e)

# === FETCH PRE MARKET ===
def fetch_pre_market():
    try:
        url = "https://www.moneycontrol.com/markets/pre-market"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        headline = soup.find("h1").get_text(strip=True)
        summary = soup.find("p").get_text(strip=True)[:600]
        msg = f"📊 <b>Pre-Market Update</b>\n\n{headline}\n{summary}"
        send_telegram_post(msg)
    except:
        send_telegram_post("📊 Pre-Market data not available today.")

# === FETCH POST MARKET ===
def fetch_post_market():
    try:
        url = "https://www.moneycontrol.com/markets/post-market"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        headline = soup.find("h1").get_text(strip=True)
        summary = soup.find("p").get_text(strip=True)[:600]
        msg = f"📌 <b>Post-Market Report</b>\n\n{headline}\n{summary}"
        send_telegram_post(msg)
    except:
        send_telegram_post("📌 Post-Market report not available today.")

# === FETCH FII/DII ===
def fetch_fii_dii():
    try:
        url = "https://www.moneycontrol.com/stocks/marketstats/fii-dii-activity"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")[1:3]
        data = []
        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            data.append(" | ".join(cols))
        msg = "💰 <b>FII/DII Data</b>\n\n" + "\n".join(data)
        send_telegram_post(msg)
    except:
        send_telegram_post("💰 FII/DII data not available today.")

# === FETCH IPO WITH GMP ===
def fetch_ipo():
    try:
        url = "https://www.chittorgarh.com/report/ipo-open-today/86/"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table", {"class": "table"})
        rows = table.find_all("tr")[1:]

        if not rows:
            send_telegram_post("📌 No live Mainboard IPOs today.")
            return

        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 6:
                continue
            name, open_date, close_date, lot_size, price, gmp = cols[:6]
            msg = (
                f"📌 <b>IPO Alert – {name}</b>\n\n"
                f"Price Band: {price}\n"
                f"Lot Size: {lot_size}\n"
                f"Open: {open_date} – Close: {close_date}\n"
                f"GMP: {gmp if gmp else 'Not Available'}"
            )
            send_telegram_post(msg)

    except Exception as e:
        print("Error fetching IPO:", e)
        send_telegram_post("📌 Couldn’t fetch IPO details today.")

# === SCHEDULER SETUP ===
def run_scheduler():
    # weekdays
    if datetime.today().weekday() < 5:
        schedule.every(NEWS_INTERVAL).seconds.do(fetch_general_news)
        schedule.every().day.at("09:00").do(fetch_pre_market)
        schedule.every().day.at("15:45").do(fetch_post_market)
        schedule.every().day.at("20:00").do(fetch_fii_dii)
        schedule.every().day.at("11:00").do(fetch_ipo)
    else:
        # weekends only normal news
        schedule.every(NEWS_INTERVAL).seconds.do(fetch_general_news)

    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
