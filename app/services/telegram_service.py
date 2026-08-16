
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user import User
import secrets
from app.repositories.telegram import add_telegram_token_link
from app.core.security import hash_telegram_link_token
async def connect_telegram_user(db:AsyncSession,
                           current_user:User):
    token = await generate_telegram_link_token()
    hashed_token = hash_telegram_link_token(telegram_token=token)
    await add_telegram_token_link(user_id=current_user.id,db=db,token=hashed_token)
    telegram_url = create_link(raw_token=token)
    return {
    "telegram_url": telegram_url
    }

async def create_link(raw_token:str):

    telegram_url = f"https://t.me/{BOT_USERNAME}?start={raw_token}"
    return telegram_url


async def generate_telegram_link_token() -> str:
    return secrets.token_urlsafe(32)