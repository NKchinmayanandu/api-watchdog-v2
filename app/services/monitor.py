from app.repositories.process_monitor import persist_monitor_result
from app.redis import cache,streams,pubsub
import httpx
import time 
from app.db.session import AsyncSessionLocal
from app.repositories.endpoint import get_endpoint_by_id
from app.redis import scheduler
import logging

async def process_job(job):
    message_id, endpoint_id = job

    try:
        endpoint_cache = await cache.get_endpoint_cache(endpoint_id)

        if not endpoint_cache:
            return

        url = endpoint_cache["url"]
        previous_status = endpoint_cache.get("current_status")

        async with httpx.AsyncClient() as client:
            start = time.perf_counter()
            response = await client.get(url)
            latency_ms = (time.perf_counter() - start) * 1000

        status_code = response.status_code
        current_status = (
            "UP" if 200 <= status_code < 400 else "DOWN"
        )

        status_changed = (
            previous_status is not None
            and previous_status != current_status
        )

        await cache.update_endpoint_cache(
            endpoint_id=endpoint_id,
            mapping={
                "current_status": current_status,
                "latency_ms": latency_ms,
                "status_code": status_code,
            },
        )

        async with AsyncSessionLocal() as db:
            endpoint = await persist_monitor_result(
                db=db,
                endpoint_id=endpoint_id,
                current_status=current_status,
                status_code=status_code,
                latency_ms=latency_ms,
                status_changed=status_changed,
            )

        if status_changed:
            await pubsub.publish_endpoint_event(
                owner_id=endpoint.owner_id,
                endpoint_id=endpoint.id,
                current_status=current_status,
                status_code=status_code,
                latency_ms=latency_ms,
            )

        await scheduler.schedule_endpoint(endpoint_id)

        await streams.ack_monitor_job(message_id)

    except Exception:
        logging.exception(
            "Failed to process monitor job",
            extra={
                "message_id": message_id,
                "endpoint_id": endpoint_id,
            },
        )