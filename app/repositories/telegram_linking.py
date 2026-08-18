from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.telegram_linking import telegram_link_token
from sqlalchemy import func
from datetime import datetime,timezone

async def get_active_telegram_link_tokens(
    db: AsyncSession,
    token_hash
):
    result = await db.execute(
            select(telegram_link_token).where(
                telegram_link_token.token == token_hash,
                telegram_link_token.used_at.is_(None),
                telegram_link_token.expires_at > datetime.now(timezone.utc),
            )
        )

    return result