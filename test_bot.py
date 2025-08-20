import os
import telegram

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = telegram.Bot(token=BOT_TOKEN)
bot.send_message(chat_id=CHANNEL_ID, text="✅ Test message from MarketPulse bot!")
print("Message sent successfully")
