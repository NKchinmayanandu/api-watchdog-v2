from telethon import functions, types
from app.telegram.client import telegram_client

async def send_telegram_message(
    telegram_user_id: int,
    telegram_access_hash: int,
    message: str,
    idempotency_key: int,
):
    recipient = types.InputPeerUser(
        user_id=telegram_user_id,
        access_hash=telegram_access_hash,
    )

    result = await telegram_client(
        functions.messages.SendMessageRequest(
            peer=recipient,
            message=message,
            random_id=idempotency_key,
        )
    )
    return result