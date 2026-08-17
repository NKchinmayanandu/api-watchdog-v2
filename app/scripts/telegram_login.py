import asyncio  
import asyncio
from app.telegram.client import (
    start_telegram_client,
    stop_telegram_client,
    telegram_client,
)


async def main():
    await start_telegram_client()

    me = await telegram_client.get_me()

    print("Telegram authentication successful")
    print(f"ID: {me.id}")
    print(f"Username: {me.username}")

    await stop_telegram_client()


if __name__ == "__main__":
    asyncio.run(main())