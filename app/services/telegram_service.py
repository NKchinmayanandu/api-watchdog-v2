
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user import User
import secrets
from app.repositories.telegram import add_telegram_token_link
from app.core.security import hash_telegram_link_token
from app.core.config import settings
async def connect_telegram_user(
    db: AsyncSession,
    current_user: User,
):
    token = await generate_telegram_link_token()

    hashed_token = hash_telegram_link_token(
        telegram_token=token
    )

    await add_telegram_token_link(
        user_id=current_user.id,
        db=db,
        token=hashed_token,
    )

    return {
    "telegram_url": f"https://t.me/{settings.telegram_username}",
    "telegram_token": token
    }

async def generate_telegram_link_token() -> str:
    return secrets.token_urlsafe(32)