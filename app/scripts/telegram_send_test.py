import asyncio
from telethon.tl.types import InputPeerUser
from app.telegram.client import (
    telegram_client,
    start_telegram_client,
    stop_telegram_client,
)


async def main():
    await start_telegram_client()
    recipient = InputPeerUser(
        6198305681,
        613789124211762829,
    )
    message = await telegram_client.send_message(
        recipient,
        "Hello from API Watchdog MTProto 🚀",
    )
    print("Message sent successfully")
    print(f"Message ID: {message.id}")
    print(f"Chat ID: {message.chat_id}")
    print(f"Text: {message.text}")
    
    print("Message sent successfully")

    await stop_telegram_client()


if __name__ == "__main__":
    asyncio.run(main())