from sqlalchemy.ext.asyncio import AsyncSession
from app.models.telegram_linking import telegram_link_token
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from app.models.telegram_accounts import TelegramAccount
async def add_telegram_token_link(
    user_id: int,
    token: str,
    db: AsyncSession,
):
    now = datetime.now(timezone.utc)
    telegram_link = telegram_link_token(
        user_id=user_id,
        token=token,
        expires_at=now + timedelta(minutes=10),
    )
    db.add(telegram_link)
    await db.commit()
    return 
async def get_telegram_account(user_id:int,db:AsyncSession):
    account = await db.execute(select(TelegramAccount).where
                              (TelegramAccount.user_id==user_id))
    return account