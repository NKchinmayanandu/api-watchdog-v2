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
        count=10,
        block=5000
    )
    if not jobs:
        return []

    monitor_jobs = []
    for message_id,fields in jobs:
        endpoint_id = int(fields["endpoint_id"])
        monitor_jobs.append((message_id,endpoint_id))

    return message_id,endpoint_id

async def ack_monitor_job(message_id):
    await redis_client.xack(
        "monitor_jobs",
        "monitor_group",
        message_id,
    )