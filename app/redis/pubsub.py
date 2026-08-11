import json

from app.redis.redis_client import redis_client


async def publish_endpoint_event(
    endpoint_id: int,
    current_status: str,
    owner_id:int
):
    message = {
        "owner_id": owner_id,
        "endpoint_id": endpoint_id,
        "current_status": current_status,
    }

    await redis_client.publish(
    f"endpoint_status_changed:{owner_id}",
    json.dumps({
        "endpoint_id": endpoint_id,
        "current_status": current_status,
        }),
    )

async def subscribe_endpoint_events(owner_id: int):
    pubsub = redis_client.pubsub()

    await pubsub.subscribe(
        f"endpoint_status_changed:{owner_id}"
    )

    return pubsub