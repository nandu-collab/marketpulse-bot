# fetch_news.py
import feedparser
import html
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz

# RSS feeds (common Indian market + global impacting India)
RSS_FEEDS = [
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://www.moneycontrol.com/rss/marketplus.xml",
    "https://www.moneycontrol.com/rss/business.xml",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",
    "https://www.livemint.com/rss/markets",
    "https://www.livemint.com/rss/companies",
]

IPO_FEEDS = [
    "https://www.chittorgarh.com/ipo/ipo_news_rss.xml",
]

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}

def _strip_html(text: str) -> str:
    if not text:
        return ""
    t = html.unescape(text)
    return "".join(BeautifulSoup(t, "html.parser").stripped_strings)

def get_market_news(max_items=30):
    items = []
    for url in RSS_FEEDS:
        try:
            d = feedparser.parse(url)
            for e in d.entries[:15]:
                title = _strip_html(getattr(e, "title", "") or "")
                summary = _strip_html(getattr(e, "summary", getattr(e, "description", "")) or "")
                link = getattr(e, "link", "")
                uid = getattr(e, "id", link)
                if title and link:
                    items.append({
                        "uid": uid,
                        "title": title,
                        "summary": summary,
                        "link": link,
                        "source": url,
                        "published": getattr(e, "published", "")
                    })
        except Exception:
            continue
    # keep unique by link and return newest first (feed order tends to be newest)
    seen = set()
    out = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        out.append({"uid": it["uid"], "headline": it["title"], "summary": it["summary"], "link": it["link"], "source": it["source"]})
        if len(out) >= max_items:
            break
    return out

def _collect_headlines(limit=6):
    items = get_market_news(max_items=30)
    headlines = []
    seen = set()
    for it in items:
        h = it["headline"]
        if h in seen:
            continue
        seen.add(h)
        headlines.append(h)
        if len(headlines) >= limit:
            break
    return headlines

def get_pre_market_brief():
    hs = _collect_headlines(6)
    if not hs:
        return "Pre-market data temporarily unavailable."
    return "\n".join("• " + h for h in hs)

def get_post_market_brief():
    hs = _collect_headlines(6)
    if not hs:
        return "Post-market data temporarily unavailable."
    return "\n".join("• " + h for h in hs)

def get_ipo_updates(limit=8):
    lines = []
    for url in IPO_FEEDS:
        try:
            d = feedparser.parse(url)
            for e in d.entries[:12]:
                title = _strip_html(getattr(e, "title", "") or "")
                summary = _strip_html(getattr(e, "summary", getattr(e, "description", "")) or "")
                pub = getattr(e, "published_parsed", None)
                if pub:
                    pdate = datetime(*pub[:6]).date()
                    # keep recent 3 days
                    if (datetime.now(pytz.timezone("Asia/Kolkata")).date() - pdate).days > 3:
                        continue
                if title:
                    lines.append(f"• {title} — {summary[:250]}")
                if len(lines) >= limit:
                    break
        except Exception:
            continue
    if not lines:
        return "IPO feed temporarily unavailable."
    return "\n".join(lines[:limit])

def get_fii_dii_data():
    # best-effort fetch from NSE; if fails return friendly message
    endpoints = [
        "https://www.nseindia.com/api/fiidiiTrade?type=equity",
        "https://www.nseindia.com/api/fiidiiTradeReact?type=equity",
    ]
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=6)
    except Exception:
        pass
    for url in endpoints:
        try:
            r = s.get(url, timeout=8)
            if r.ok:
                j = r.json()
                rows = j.get("data") if isinstance(j, dict) else j
                if isinstance(rows, list) and rows:
                    latest = rows[0]
                    def pick(d, keys, default="-"):
                        for k in keys:
                            if k in d and d[k] not in (None, ""):
                                return d[k]
                        return default
                    date = pick(latest, ["date", "Date", "date_"])
                    fii_buy = pick(latest, ["buyValue", "FII_Buy", "FII_Buy_Value"])
                    fii_sell = pick(latest, ["sellValue", "FII_Sell", "FII_Sell_Value"])
                    fii_net = pick(latest, ["netValue", "FII_Net", "FII_Net_Value"])
                    dii_buy = pick(latest, ["diiBuyValue", "DII_Buy"])
                    dii_sell = pick(latest, ["diiSellValue", "DII_Sell"])
                    dii_net = pick(latest, ["diiNetValue", "DII_Net"])
                    return (
                        f"Date: {date}\n\n"
                        f"FII Buy: {fii_buy}\nFII Sell: {fii_sell}\nFII Net: {fii_net}\n\n"
                        f"DII Buy: {dii_buy}\nDII Sell: {dii_sell}\nDII Net: {dii_net}"
                    )
        except Exception:
            continue
    return "FII/DII data currently unavailable."
