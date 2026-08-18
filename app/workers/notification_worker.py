import sys
import asyncio
from app.redis import streams
from app.services.telegram_notification import process_notification
import logging
async def notification_worker():
    while True:
        notification_jobs = await streams.read_notification_jobs(consumer_name=consumer_name)
        if not notification_jobs:
            await asyncio.sleep(1)
            continue
        tasks = [
            asyncio.create_task(process_notification(job))
            for job in notification_jobs
        ]
        results = await asyncio.gather(*tasks,
                             return_exceptions=True)
        
        for job, result in zip(notification_jobs, results):
            if isinstance(result, Exception):
                logging.error(
                    "Notification job failed",
                    extra={
                        "message_id": job[0],
                        "error": str(result),
                    },
                )
if __name__ == "__main__":
    consumer_name = sys.argv[1]
    asyncio.run(notification_worker())