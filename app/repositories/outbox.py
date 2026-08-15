from app.models.outbox import OutboxEvent
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime,timezone,timedelta
from sqlalchemy import select,update


async def claim_outbox_events(
    db: AsyncSession,
    worker_name: str,
    limit: int = 10,
):
    now = datetime.now(timezone.utc)
    lease_timeout = timedelta(seconds=30)

    result = await db.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.processed_at.is_(None),
            (
                OutboxEvent.claimed_at.is_(None)
                | (
                    OutboxEvent.claimed_at
                    < now - lease_timeout
                )
            ),
        )
        .order_by(OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    events = list(result.scalars().all())

    for event in events:
        event.claimed_at = now
        event.claimed_by = worker_name

    await db.commit()

    return events

async def mark_processed(
    db: AsyncSession,
    event_ids: list[int],
):
    await db.execute(
        update(OutboxEvent)
        .where(
            OutboxEvent.id.in_(event_ids),
            OutboxEvent.processed_at.is_(None),
        )
        .values(
            processed_at=datetime.now(timezone.utc)
        )
    )

    await db.commit()
