# ... [all your imports and code above stay same] ...

# -------------------- scheduler --------------------
scheduler = BackgroundScheduler(timezone=TZ)

def schedule_jobs():
    # Hourly news windows (two posts per hour) between 08:30 and 21:30.
    scheduler.add_job(post_two_news_now, CronTrigger(minute="30", hour="8-21"))
    scheduler.add_job(post_two_news_now, CronTrigger(minute="0",  hour="9-21"))

    # Pre/Post market & data posts
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_pre_market_brief(),
                      CronTrigger(hour=9, minute=0))
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_post_market_wrap(),
                      CronTrigger(hour=16, minute=0))
    scheduler.add_job(post_fii_dii, CronTrigger(hour=20, minute=0))
    scheduler.add_job(lambda: market_is_open_today(datetime.now(TZ)) and post_ipo_digest(),
                      CronTrigger(hour=10, minute=45))


# -------------------- web keepalive (Render) --------------------
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "time": datetime.now(TZ).isoformat()}

# ✅ Add /ping endpoint for uptime pinger
@app.get("/ping")
def ping():
    return {"pong": True, "time": datetime.now(TZ).isoformat()}


# -------------------- startup --------------------
@app.on_event("startup")
def startup_event():
    schedule_jobs()
    scheduler.start()
    log.info("Scheduler started via FastAPI startup. Bot ready.")


def main():
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")


if __name__ == "__main__":
    main()
