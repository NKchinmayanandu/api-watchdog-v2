from datetime import datetime, timezone
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.telegram_accounts import TelegramAccount
from app.models.telegram_linking import telegram_link_token
from app.core.security import hash_telegram_link_token
from app.telegram.sender import send_telegram_message

async def link_telegram_user(
    token: str,
    telegram_user_id: int,
    telegram_access_hash: int,
):
    token_hash = hash_telegram_link_token(token)

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(telegram_link_token).where(
                telegram_link_token.token == token_hash,
                telegram_link_token.used_at.is_(None),
                telegram_link_token.expires_at > datetime.now(timezone.utc),
            )
        )

        link_token = result.scalar_one_or_none()

        if not link_token:
            return False

        telegram_account = TelegramAccount(
            user_id=link_token.user_id,
            telegram_user_id=telegram_user_id,
            telegram_access_hash=telegram_access_hash,
        )

        db.add(telegram_account)

        link_token.used_at = datetime.now(timezone.utc)

        await db.commit()

    await send_telegram_message(
        telegram_user_id=telegram_user_id,
        telegram_access_hash=telegram_access_hash,
        message=(
            "✅ Your Telegram account has been connected "
            "to API Watchdog successfully."
        ),
    )

    return True