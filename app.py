# =========================
# Flask (to satisfy Render Web Service)
# =========================
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

def announce_start():
    send_message(
        "✅ MarketPulse bot restarted and schedule loaded.\n"
        "Window: 08:30–21:30 • Every 30 min (2 posts/slot)\n"
        "Weekdays: 09:00 Pre-market • 10:30/11:00 IPO • 15:45 Post-market • 21:00 FII/DII"
    )

def start_scheduler_once():
    """Ensure scheduler starts only once (even with Gunicorn workers)."""
    if not sched.running:
        schedule_jobs()
        sched.start()
        log.info("Scheduler started.")
        announce_start()

# Start scheduler immediately when app is imported (not only __main__)
start_scheduler_once()
