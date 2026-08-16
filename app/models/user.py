from datetime import datetime

from sqlalchemy import BigInteger, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.models.endpoint import Endpoint
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    endpoints : Mapped[list["Endpoint"]] = relationship(
        back_populates="owner",
        cascade="all, delete-orphan",
    )
    telegram_chat_id: Mapped[int | None] = mapped_column(
    BigInteger,
    nullable=True,
    unique=True,
    )