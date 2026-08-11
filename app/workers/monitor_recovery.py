from app.redis import streams
import asyncio
from app.services import monitor
import sys
async def recovery_worker():
    while True:
        jobs = await streams.claim_stale_monitor_jobs(
            consumer_name=consumer_name,
            min_idle_time=30_000,
        )
        tasks = [
            asyncio.create_task(monitor.process_job(job=job))
            for job in jobs
        ]

        await asyncio.gather(*tasks)    
        await asyncio.sleep(5)

if __name__ == "__main__":
    consumer_name = sys.argv[1]
    asyncio.run(recovery_worker())