import sys
import asyncio
from app.redis import streams
import app.db.models
async def notification_worker():
    while True:
        notification_jobs = await streams.read_notification_jobs(consumer_name=consumer_name)
        if not notification_jobs:
            asyncio.sleep(1)
            continue
        tasks = [
            asyncio.create_task()
            for job in notification_jobs
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    consumer_name = sys.argv[1]
    asyncio.run(notification_worker())