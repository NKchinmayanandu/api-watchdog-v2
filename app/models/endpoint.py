from datetime import datetime

from sqlalchemy import String, func, ForeignKey,UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.models.user import User
from sqlalchemy import Integer,Float
class Endpoint(Base):

    __tablename__ = "endpoints"

    id : Mapped[int] = mapped_column(primary_key=True)
    owner_id : Mapped[int] = mapped_column(ForeignKey("users.id",ondelete="CASCADE"),index=True,)
    name: Mapped[str] = mapped_column(String(100))
    url : Mapped[str] = mapped_column(String(2048))

    created_at : Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at : Mapped[datetime] = mapped_column(
        server_default=func.now(),
        onupdate=func.now(),
    )
    current_status: Mapped[str | None] = mapped_column(String(20))
    last_checked_at: Mapped[datetime | None] = mapped_column()
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latency: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner : Mapped["User"] = relationship(
        back_populates="endpoints"
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "name",
            name="uq_name_endpoint_user",
        ),
    )