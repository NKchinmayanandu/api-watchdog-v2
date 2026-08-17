from telethon import TelegramClient

from app.core.config import settings


telegram_client = TelegramClient(
    settings.TELEGRAM_SESSION_PATH,
    settings.TELEGRAM_API_ID,
    settings.TELEGRAM_API_HASH
)

async def start_telegram_client():
    await telegram_client.start()

async def stop_telegram_client():
    await telegram_client.disconnect()