from app.redis.redis_client import redis_client

async def enqueue_monitor_jobs(endpoint_id:int):
    await redis_client.xadd(
        "monitor_jobs",
        {
            "endpoint_id":endpoint_id
        }
    )

async def read_monitor_jobs():
    jobs = await redis_client.xreadgroup(
        groupname="monitor_group",
        consumername="worker_a",
        streams={
        "monitor_jobs": ">"
        },
        count=1,
        block=5000
    )
    if not jobs:
        return None
    _, messages = jobs[0]

    message_id, fields = messages[0]

    endpoint_id = int(fields["endpoint_id"])

    return message_id,endpoint_id