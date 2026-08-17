from app.db.base import Base
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import ForeignKey,DateTime,func
from datetime import datetime
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.models.user import User

class TelegramAccount(Base):
    __tablename__ = "telegram_accounts"

    id: Mapped[int] = mapped_column(primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
    )

    telegram_user_id: Mapped[int] = mapped_column(
        unique=True,
        nullable=False,
        index=True,
    )

    telegram_access_hash: Mapped[int] = mapped_column(
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )