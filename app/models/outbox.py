from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(primary_key=True)

    owner_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )

    endpoint_id: Mapped[int] = mapped_column(
        ForeignKey("endpoints.id", ondelete="CASCADE"),
        index=True,
    )
    
    telegram_random_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    event_type: Mapped[str] = mapped_column(String(50))
    current_status: Mapped[str] = mapped_column(String(20))

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now()
    )

    claimed_at: Mapped[datetime | None] = mapped_column(
        nullable=True
    )

    claimed_by: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    processed_at: Mapped[datetime | None] = mapped_column(
        nullable=True
    )

    endpoint_url: Mapped[str] = mapped_column(
    String(2048),
    nullable=False,
    )

    latency_ms : Mapped[int] = mapped_column(nullable=False)