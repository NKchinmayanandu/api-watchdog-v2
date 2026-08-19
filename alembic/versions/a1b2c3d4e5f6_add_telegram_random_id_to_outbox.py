"""add telegram_random_id and owner_id to outbox_events

Revision ID: a1b2c3d4e5f6
Revises: 9925191a829f
Create Date: 2026-08-20 01:08:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "9925191a829f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add telegram_random_id (BIGINT) and owner_id (FK) to outbox_events."""
    op.add_column(
        "outbox_events",
        sa.Column(
            "telegram_random_id",
            sa.BigInteger(),
            nullable=False,
            server_default="0",   # temporary default so existing rows don't break
        ),
    )
    # Remove the server default after backfilling — new rows must supply the value.
    op.alter_column("outbox_events", "telegram_random_id", server_default=None)

    op.add_column(
        "outbox_events",
        sa.Column("owner_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_outbox_events_owner_id",
        "outbox_events",
        "users",
        ["owner_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_outbox_events_owner_id",
        "outbox_events",
        ["owner_id"],
        unique=False,
    )


def downgrade() -> None:
    """Remove telegram_random_id and owner_id from outbox_events."""
    op.drop_index("ix_outbox_events_owner_id", table_name="outbox_events")
    op.drop_constraint(
        "fk_outbox_events_owner_id", "outbox_events", type_="foreignkey"
    )
    op.drop_column("outbox_events", "owner_id")
    op.drop_column("outbox_events", "telegram_random_id")
