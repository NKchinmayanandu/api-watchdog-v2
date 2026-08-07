import time

from app.redis.redis_client import redis_client


async def schedule_endpoint(endpoint_id: int):
    next_check = int(time.time()) + 30

    await redis_client.zadd(
        "monitor_schedule",
        {
            f"endpoint:{endpoint_id}": next_check,
        },
    )


async def delete_scheduled_endpoint(endpoint_id: int):
    await redis_client.zrem(
        "monitor_schedule",
        f"endpoint:{endpoint_id}",
    )

