from telethon.tl.types import InputPeerUser

from app.telegram.client import telegram_client


async def send_telegram_message(
    telegram_user_id: int,
    telegram_access_hash: int,
    message: str,
):
    recipient = InputPeerUser(
        user_id=telegram_user_id,
        access_hash=telegram_access_hash,
    )

    return await telegram_client.send_message(
        recipient,
        message,
    )