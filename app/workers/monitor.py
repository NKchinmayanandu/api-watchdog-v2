import asyncio
import sys
from app.redis import streams
from app.services import monitor

async def monitor_worker(consumer_name:str):
    while True:
        jobs = await streams.read_monitor_jobs(consumer_name=consumer_name)
        if not jobs:
            continue
        tasks = [
            asyncio.create_task(monitor.process_job(job))
            for job in jobs
        ]
        await asyncio.gather(*tasks)



if __name__ == "__main__":
    consumer_name = sys.argv[1]
    asyncio.run(monitor_worker())