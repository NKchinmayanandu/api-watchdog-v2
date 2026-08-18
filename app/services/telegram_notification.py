from app.repositories.telegram import get_telegram_account
from app.telegram.sender import send_telegram_message
from telethon.tl.types import InputPeerUser

async def process_notification(notification):
    telegram_account = await get_telegram_account(
        user_id=notification.user_id
    )

    if not telegram_account:
        return

    message = build_notification_message(notification)

    result = await send_telegram_message(
        telegram_user_id=telegram_account.telegram_user_id,
        telegram_access_hash=telegram_account.telegram_access_hash,
        message=message,
    )

    return result

def build_notification_message(notification):
    return (
        f"🚨 Endpoint {notification.endpoint_id} "
        f"is {notification.current_status}"
    )   