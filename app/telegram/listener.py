from telethon import events

from app.telegram.client import telegram_client
from app.services.telegram_linking import link_telegram_user

def setup_telegram_listener():
    @telegram_client.on(events.NewMessage)
    async def handle_new_message(event):
        raw_message = event.raw_text.strip()

        if not raw_message.startswith("/start "):
            return
        token = raw_message.split(maxsplit=1)[1]
        sender = await event.get_sender()
        success = await link_telegram_user(
            token=token,
            telegram_user_id=sender.id,
            telegram_access_hash=sender.access_hash,
        )

        if success:
            print("Telegram account linked successfully")
        else:
            print("Invalid or expired Telegram linking token")