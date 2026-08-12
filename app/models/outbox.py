from datetime import datetime

from sqlalchemy import ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(primary_key=True)

    endpoint_id: Mapped[int] = mapped_column(
        ForeignKey("endpoints.id", ondelete="CASCADE"),
        index=True,
    )

    event_type: Mapped[str] = mapped_column(String(50))

    current_status: Mapped[str] = mapped_column(String(20))

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now()
    )

    processed_at: Mapped[datetime | None] = mapped_column(
        nullable=True
    )