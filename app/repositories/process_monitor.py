from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.endpoint import get_endpoint_by_id
from app.models.endpoint_history import EndpointStatusHistory
from datetime import datetime,timezone
from app.models.outbox import OutboxEvent
async def persist_monitor_result(
    db: AsyncSession,
    endpoint_id: int,
    current_status: str,
    status_code: int,
    latency_ms: float,
    status_changed: bool,
    owner_id: int,
    url: str
):
    endpoint = await get_endpoint_by_id(
        db=db,
        endpoint_id=endpoint_id,
        owner_id=owner_id
    )

    if not endpoint:
        return None

    endpoint.current_status = current_status
    endpoint.latency = latency_ms
    endpoint.status_code = status_code

    if status_changed:
        history = EndpointStatusHistory(
            endpoint_id=endpoint.id,
            status=current_status,
            occurred_at=datetime.now(timezone.utc),
        )
        db.add(history)

        outbox_event = OutboxEvent(
            endpoint_id=endpoint.id,
            event_type="endpoint_status_changed",
            current_status=current_status,
           endpoint_url=url,
           latency_ms=latency_ms
        )
        db.add(outbox_event)

    await db.commit()

    return endpoint