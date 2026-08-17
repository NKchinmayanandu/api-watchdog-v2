import asyncio

from telethon import events

from app.telegram.client import (
    telegram_client,
    start_telegram_client,
    stop_telegram_client,
)


@telegram_client.on(events.NewMessage)
async def message_handler(event):
    token = event.raw_text.strip()

    print("Received token:", token)
    print("Telegram user ID:", event.sender_id)

    sender = await event.get_sender()

    print("Access hash:", getattr(sender, "access_hash", None))


async def main():
    await start_telegram_client()

    print("Telegram listener is running...")
    print("Waiting for incoming messages...")

    try:
        await telegram_client.run_until_disconnected()
    finally:
        await stop_telegram_client()


if __name__ == "__main__":
    asyncio.run(main())