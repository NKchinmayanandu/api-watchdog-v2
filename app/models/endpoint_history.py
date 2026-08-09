from enum import Enum as PyEnum

from sqlalchemy import Enum as SQLEnum
from sqlalchemy import DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from app.db.base import Base
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.models.endpoint import Endpoint
class EndpointStatus(str, PyEnum):
    UP = "UP"
    DOWN = "DOWN"


class EndpointStatusHistory(Base):
    __tablename__ = "endpoint_status_history"

    id: Mapped[int] = mapped_column(primary_key=True)

    endpoint_id: Mapped[int] = mapped_column(
        ForeignKey("endpoints.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    status: Mapped[EndpointStatus] = mapped_column(
        SQLEnum(EndpointStatus),
        nullable=False,
    )

    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    endpoint: Mapped["Endpoint"] = relationship(
        back_populates="status_history"
    )