import asyncio

from app.telegram.client import (
    telegram_client,
    start_telegram_client,
    stop_telegram_client,
)


async def main():
    await start_telegram_client()

    entity = await telegram_client.get_entity("nagaraja_cb")

    print("TYPE:", type(entity))
    print("ID:", entity.id)
    print("ACCESS HASH:", getattr(entity, "access_hash", None))
    print("USERNAME:", getattr(entity, "username", None))
    print("PHONE:", getattr(entity, "phone", None))
    print("FIRST NAME:", getattr(entity, "first_name", None))
    print("LAST NAME:", getattr(entity, "last_name", None))

    await stop_telegram_client()


if __name__ == "__main__":
    asyncio.run(main())