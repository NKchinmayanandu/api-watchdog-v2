import asyncio
from app.redis import streams
from app.redis import cache
async def monitor_worker():
    while True:
        jobs = await streams.read_monitor_jobs()
        if not jobs:
            continue
        hset = await cache.get_endpoint_cache(endpoint_id=int(jobs))



if __name__ == "__main__":
    asyncio.run(monitor_worker())