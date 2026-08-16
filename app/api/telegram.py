from fastapi import APIRouter,Depends
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.dependencies import get_current_user
from app.models.user import User
from app.services.telegram_service import connect_telegram_user
router = APIRouter(prefix="/telegram",tags=["Telegram"])

@router.post("/link")
async def connect_telegram(db:AsyncSession=Depends(get_db),
                           current_user:User=Depends(get_current_user)):
    return await connect_telegram_user(db=db,current_user=current_user)