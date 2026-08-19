import sys
import asyncio
import logging
from app.redis import streams
from app.services.telegram_notification import process_notification
from app.telegram.client import telegram_client, start_telegram_client, stop_telegram_client
from app.telegram.listener import setup_telegram_listener

async def notification_worker(consumer_name: str):
    logging.info("Notification worker loop started...")
    while True:
        notification_jobs = await streams.read_notification_jobs(consumer_name=consumer_name)
        if not notification_jobs:
            await asyncio.sleep(1) 
            continue
            
        tasks = [
            asyncio.create_task(process_notification(job))
            for job in notification_jobs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for job, result in zip(notification_jobs, results):
            if isinstance(result, Exception):
                logging.error(
                    "Notification job failed",
                    extra={
                        "message_id": job[0],
                        "error": str(result),
                    },
                )

async def main():
    if len(sys.argv) < 2:
        print("Usage: python worker.py <consumer_name>")
        sys.exit(1)
        
    consumer_name = sys.argv[1]
    logging.info("Connecting to Telegram")
    await start_telegram_client()
    logging.info("Telegram client connected successfully!")
    setup_telegram_listener()
    try:
        await asyncio.gather(
            notification_worker(consumer_name),
            telegram_client.run_until_disconnected()
        )
    finally:
        logging.info("Disconnecting Telegram client...")
        await stop_telegram_client()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Worker stopped properly!")