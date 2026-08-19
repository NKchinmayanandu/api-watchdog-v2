from app.repositories.telegram import get_telegram_account
from app.telegram.sender import send_telegram_message
from telethon.tl.types import InputPeerUser
from app.redis.streams import ack_notification_job
async def process_notification(notification):
    message_id,fields = notification
    telegram_account = await get_telegram_account(
        user_id=fields["owner_id"]
    )

    if not telegram_account:
        await ack_notification_job(message_id)
        return
    
    message = build_notification_message(fields)

    result = await send_telegram_message(
        telegram_user_id=telegram_account.telegram_user_id,
        telegram_access_hash=telegram_account.telegram_access_hash,
        message=message,
        idempotency_key=int(fields["telegram_random_id"]),
    )   

    await ack_notification_job(message_id)
    return result


def build_notification_message(fields):
    return (
        f"🚨 Endpoint {fields['endpoint_id']} "
        f"is {fields['current_status']}\n"
        f"URL: {fields['endpoint_url']}\n"
        f"Latency: {fields['latency_ms']} ms"
    )