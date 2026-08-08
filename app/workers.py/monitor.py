import asyncio
from app.redis import streams
from app.services import monitor

async def monitor_worker():
    while True:
        jobs = await streams.read_monitor_jobs()
        if not jobs:
            continue
        tasks = [
            asyncio.create_task(monitor.process_job(job))
            for job in jobs
        ]
        await asyncio.gather(*tasks)



if __name__ == "__main__":
    asyncio.run(monitor_worker())