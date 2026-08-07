from app.redis.redis_client import redis_client
from app.models.endpoint import Endpoint

async def set_endpoint_cache(endpoint:Endpoint):
    await redis_client.hset(
        f"endpoint:{endpoint.id}",
        mapping={
            "id" : endpoint.id,
            "owner_id" : endpoint.owner_id,
            "name" : endpoint.name,
            "url" : endpoint.url,
        },
    )

async def delete_endpoint_cache(endpoint_id:int):
    await redis_client.delete(f"endpoint:{endpoint_id}")

async def get_endpoint_cache(endpoint_id:id):
    hset = await redis_client.hget(f"endpoint:{endpoint_id}")
    if not hset:
        return None
    return hset