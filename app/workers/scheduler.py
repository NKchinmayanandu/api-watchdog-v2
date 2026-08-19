import time 
from app.redis.redis_client import redis_client
from app.redis import streams,scheduler
import asyncio
async def scheduler_worker():
    while True:
    now = int(time.time())

    due_endpoints = await redis_client.zrange(
        "monitor-schedule",
        min="-inf",
        max=now,
        byscore=True
    )
    for endpoint in due_endpoints:
        endpoint_id = int(endpoint.split(":")[1])
        await streams.enqueue_monitor_jobs(endpoint_id=endpoint_id)
        await scheduler.delete_scheduled_endpoint(endpoint_id=endpoint_id)
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(scheduler_worker())