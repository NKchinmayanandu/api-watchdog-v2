from telethon import events

from app.telegram.client import telegram_client


@telegram_client.on(events.NewMessage)
async def handle_new_message(event):
    raw_message = event.raw_text.strip()

    print("Received Telegram message:")
    print("Sender:", event.sender_id)
    print("Message:", raw_message)

