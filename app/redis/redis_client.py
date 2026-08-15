from redis.asyncio import Redis
from app.core.config import settings

redis_client = Redis.from_url(
    settings.REDIS_URL,
    decode_responses=True
)

publish_outbox_sha: str | None = None

async def load_scripts():
    global publish_outbox_sha
    from app.redis.scripts import PUBLISH_OUTBOX_EVENT
    publish_outbox_sha = await redis_client.script_load(
        PUBLISH_OUTBOX_EVENT
    )