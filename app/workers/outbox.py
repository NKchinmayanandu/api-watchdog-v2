import asyncio
import sys
from app.repositories import outbox
from app.redis import streams
from app.db.session import AsyncSessionLocal
from app.redis import redis_client
import asyncio
import logging
async def outbox_worker(worker_name: str):

    await redis_client.load_scripts()

    while True:

        async with AsyncSessionLocal() as db:
            events = await outbox.claim_outbox_events(
                db=db,
                worker_name=worker_name,
                limit=10,
            )

        if not events:
            await asyncio.sleep(1)
            continue

        try:
            results = await streams.publish_outbox_events(events)

        except Exception as e:
            logging.exception(f"redis xadd and lua scripts failed : {e}")
            continue

        processed_ids = []

        for event, result in zip(events, results):

            status = result[0]

            if status in ("published", "duplicate"):
                processed_ids.append(event.id)

        if processed_ids:

            async with AsyncSessionLocal() as db:
                await outbox.mark_processed(
                    db=db,
                    event_ids=processed_ids,
                )


if __name__ == "__main__":
    worker_name = sys.argv[1]
    asyncio.run(outbox_worker())