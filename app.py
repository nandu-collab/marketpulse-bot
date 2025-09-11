# app.py
import os, json, re, time, logging, threading
from datetime import datetime, date
from collections import deque
from typing import List, Dict, Optional

import pytz
import requests
import feedparser
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------- Config ----------
def env(name: str, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

BOT_TOKEN  = env("BOT_TOKEN")
CHANNEL_ID = env("CHANNEL_ID")  # must be -100...

TIMEZONE_NAME = env("TIMEZONE", "Asia/Kolkata")
ENABLE_NEWS   = env("ENABLE_NEWS", "1") == "1"
MAX_NEWS_PER_SLOT = int(env("MAX_NEWS_PER_SLOT", "2"))
NEWS_SUMMARY_CHARS= int(env("NEWS_SUMMARY_CHARS", "550"))

QUIET_HOURS_START = env("QUIET_HOURS_START", "22:30")
QUIET_HOURS_END   = env("QUIET_HOURS_END", "07:30")
MARKET_BLIPS_START= env("MARKET_BLIPS_START", "08:30")
MARKET_BLIPS_END  = env("MARKET_BLIPS_END", "21:30")

if not BOT_TOKEN or not CHANNEL_ID:
    raise RuntimeError("BOT_TOKEN and CHANNEL_ID required")

TZ = pytz.timezone(TIMEZONE_NAME)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketpulse")

bot = Bot(token=BOT_TOKEN)
app = Flask(__name__)

# ---------- Persistence ----------
SEEN_FILE = "/tmp/mpulse_seen.json"
seen_urls = set()
seen_queue = deque(maxlen=2000)

def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            arr = json.load(f)
        for u in arr:
            seen_urls.add(u); seen_queue.append(u)
    except: pass
def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_queue), f)
    except Exception as e: log.warning("save_seen failed: %s", e)
load_seen()

UA = {"User-Agent": "Mozilla/5.0 (compatible; MarketPulseBot/1.0)"}
def now_local(): return datetime.now(TZ)

def parse_hhmm(s: str): h,m = s.split(":"); return int(h),int(m)
def within_window(start_str,end_str,dt=None):
    dt = dt or now_local()
    sh,sm = parse_hhmm(start_str); eh,em = parse_hhmm(end_str)
    start = dt.replace(hour=sh,minute=sm).time()
    end   = dt.replace(hour=eh,minute=em).time()
    t = dt.time()
    return (start<=t<=end) if start<=end else (t>=start or t<=end)
def in_quiet_hours(dt=None): return within_window(QUIET_HOURS_START,QUIET_HOURS_END,dt)

def clean_text(html): 
    if not html: return ""
    txt = BeautifulSoup(html,"html.parser").get_text(" ",strip=True)
    return re.sub(r"\s+"," ",txt)
def summarize(text,limit):
    text=clean_text(text)
    if len(text)<=limit: return text
    cut=text[:limit]; idx=max(cut.rfind(". "),cut.rfind("? "),cut.rfind("! "))
    return cut[:idx+1] if idx>0 else cut.rstrip()+"…"

# ---------- Feeds ----------
FEEDS = {
 "market":[
   "https://www.livemint.com/rss/markets",
   "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
   "https://www.business-standard.com/rss/markets-106.rss"],
 "company":[
   "https://www.livemint.com/rss/companies",
   "https://www.moneycontrol.com/rss/latestnews.xml"],
 "finance":[
   "https://www.livemint.com/rss/economy",
   "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms"],
 "global":[
   "https://feeds.reuters.com/reuters/businessNews",
   "https://feeds.bbci.co.uk/news/business/rss.xml"]
}
def fetch_feed_entries(url,limit=12):
    try:
        feed=feedparser.parse(url); out=[]
        for e in feed.entries[:limit]:
            title=clean_text(e.get("title","")); link=e.get("link","")
            desc=clean_text(e.get("summary","") or e.get("description",""))
            if title and link: out.append({"title":title,"link":link,"summary":desc})
        return out
    except Exception as ex: log.warning("feed error %s: %s",url,ex); return []

def collect_news_batch(max_items):
    groups=["market","company","finance","global"]; results=[]
    for g in groups:
        cand=[]; 
        for u in FEEDS[g]: cand.extend(fetch_feed_entries(u))
        uniq=[]; used=set()
        for c in cand:
            if not c["link"] or c["link"] in used or c["link"] in seen_urls: continue
            used.add(c["link"]); uniq.append(c)
        results.extend(uniq[:2])
        if len(results)>=max_items: break
    return results[:max_items]

# ---------- Special Fetchers ----------
def fetch_ongoing_ipos_for_today():
    try:
        r=requests.get("https://www.chittorgarh.com/ipo/ipo_calendar.asp",headers=UA,timeout=12)
        soup=BeautifulSoup(r.text,"html.parser")
        rows=[]
        for tr in soup.select("table tr"):
            tds=[clean_text(td.get_text(" ")) for td in tr.find_all("td")]
            if len(tds)>=5: rows.append(tds)
        today=now_local().date(); found=[]
        for tds in rows:
            line=" ".join(tds); m=re.findall(r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})",line)
            if len(m)>=2:
                op=datetime.strptime(m[0],"%d %b %Y").date()
                cl=datetime.strptime(m[1],"%d %b %Y").date()
                if op<=today<=cl:
                    found.append({"company":tds[0],"open":op.strftime("%d %b"),"close":cl.strftime("%d %b")})
        return found
    except Exception as ex: log.warning("IPO fetch failed: %s",ex); return []

def fetch_fii_dii_cash():
    try:
        r=requests.get("https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php",headers=UA,timeout=12)
        soup=BeautifulSoup(r.text,"html.parser"); table=soup.find("table"); 
        if not table: return None
        rows=[]
        for tr in table.find_all("tr"):
            tds=[clean_text(td.get_text(" ")) for td in tr.find_all("td")]
            if len(tds)>=4: rows.append(tds)
        if not rows: return None
        flat=" ".join(rows[0]); nums=[int(x.replace(",","")) for x in re.findall(r"[-+]?\d[\d,]*",flat)]
        if len(nums)>=2: return {"fii":nums[-2],"dii":nums[-1]}
    except Exception as ex: log.warning("FII/DII fetch failed: %s",ex)
    return None

def fetch_close_snapshot():
    try:
        r=requests.get("https://query1.finance.yahoo.com/v7/finance/quote",params={"symbols":"^NSEI,^BSESN,^NSEBANK"},headers=UA,timeout=12)
        data=r.json()["quoteResponse"]["result"]; mp={}
        for q in data:
            mp[q["symbol"]]={"price":q.get("regularMarketPrice"),"change":q.get("regularMarketChange"),"pct":q.get("regularMarketChangePercent")}
        return mp or None
    except Exception as ex: log.warning("close snapshot failed: %s",ex); return None

# ---------- Sender ----------
def send_text(text,url=None,btn="Read more"):
    try:
        markup=None
        if url: markup=InlineKeyboardMarkup([[InlineKeyboardButton(btn,url=url)]])
        bot.send_message(chat_id=CHANNEL_ID,text=text,parse_mode=ParseMode.HTML,disable_web_page_preview=True,reply_markup=markup)
    except Exception as ex: log.warning("Telegram send failed: %s",ex)

# ---------- Jobs ----------
def post_news_slot():
    if not ENABLE_NEWS or in_quiet_hours() or not within_window(MARKET_BLIPS_START,MARKET_BLIPS_END): return
    items=collect_news_batch(MAX_NEWS_PER_SLOT); posted=0
    for it in items:
        if it["link"] in seen_urls: continue
        text=f"<b>{it['title']}</b>\n\n{summarize(it['summary'],NEWS_SUMMARY_CHARS)}"
        send_text(text,it["link"],"Read more →")
        seen_urls.add(it["link"]); seen_queue.append(it["link"]); posted+=1; time.sleep(1)
    if posted: save_seen()

# dynamic polling (IPO, Pre/Post market, FII/DII)
_last={"ipo":None,"fii_dii":None,"close":None,"premarket":None}
def poll_dynamic():
    # IPO
    ipos=fetch_ongoing_ipos_for_today()
    if ipos and ipos!=_last["ipo"]:
        lines=["📌 <b>IPO — Ongoing Today</b>"]+[f"{x['company']} • {x['open']}–{x['close']}" for x in ipos]
        send_text("\n".join(lines)); _last["ipo"]=ipos

    # Post-market snapshot
    snap=fetch_close_snapshot()
    if snap and snap!=_last["close"] and now_local().hour>=16:
        parts=["📊 <b>Post-Market — Snapshot</b>"]
        for sym,label in [("^BSESN","Sensex"),("^NSEI","Nifty 50"),("^NSEBANK","Bank Nifty")]:
            q=snap.get(sym); 
            if q: parts.append(f"{label}: {q['price']} ({q['change']:+} | {q['pct']:+}%)")
        send_text("\n".join(parts)); _last["close"]=snap

    # FII/DII
    fd=fetch_fii_dii_cash()
    if fd and fd!=_last["fii_dii"] and now_local().hour>=20:
        send_text(f"🏦 <b>FII/DII — Cash</b>\nFII: {fd['fii']:+,} cr\nDII: {fd['dii']:+,} cr"); _last["fii_dii"]=fd

# ---------- Scheduler ----------
sched=BackgroundScheduler(timezone=TZ)
sched.add_job(post_news_slot,CronTrigger(minute=30,hour="8-21",timezone=TZ))
sched.add_job(poll_dynamic,"interval",minutes=10)  # dynamic polling
sched.start()

# Announce startup
threading.Thread(target=lambda: send_text("✅ <b>MarketPulse started</b>"),daemon=True).start()

# Flask routes
@app.route("/")
def home(): return "Bot running ✅",200
@app.route("/status")
def status():
    jobs=[{"id":j.id,"next":str(j.next_run_time)} for j in sched.get_jobs()]
    return jsonify(ok=True,tz=TIMEZONE_NAME,jobs=jobs,seen=len(seen_urls))

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT",10000)))
