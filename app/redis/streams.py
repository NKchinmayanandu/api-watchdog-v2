from app.redis.redis_client import redis_client
from app.redis.scripts import PUBLISH_OUTBOX_EVENT
async def enqueue_monitor_jobs(endpoint_id:int):
    await redis_client.xadd(
        "monitor_jobs",
        {
            "endpoint_id":endpoint_id
        }
    )

async def read_monitor_jobs(consumer_name):
    jobs = await redis_client.xreadgroup(
        groupname="monitor_group",
        consumername=consumer_name,
        streams={
        "monitor_jobs": ">"
        },
        count=10,
        block=5000
    )
    if not jobs:
        return []
    _, messages = jobs[0]
    monitor_jobs = []
    for message_id,fields in messages:
        endpoint_id = int(fields["endpoint_id"])
        monitor_jobs.append((message_id,endpoint_id))

    return monitor_jobs

async def ack_monitor_job(message_id):
    await redis_client.xack(
        "monitor_jobs",
        "monitor_group",
        message_id,
    )

async def claim_stale_monitor_jobs(consumer_name:str,min_idle_time):
    jobs = await redis_client.xautoclaim(
        name="monitor_jobs",
        groupname="monitor_group",
        consumername=consumer_name,
        min_idle_time=min_idle_time,    
        start_id="0-0",
        count=10,
    )
    monitor_jobs = []
    _, messages, _ = jobs
    for message_id,fields in messages:
        endpoint_id = int(fields["endpoint_id"])
        monitor_jobs.append((message_id,endpoint_id))
    return monitor_jobs


from app.redis.redis_client import redis_client, publish_outbox_sha


async def publish_outbox_events(events):

    async with redis_client.pipeline(transaction=False) as pipe:

        for event in events:

            pipe.evalsha(
                publish_outbox_sha,
                2,
                f"outbox:notification:published:{event.id}",
                "notification_jobs",
                str(event.id),
                event.event_type,
                str(event.owner_id),
                str(event.endpoint_id),
                event.current_status,
                event.endpoint_url,
                str(event.latency_ms)
            )

        return await pipe.execute()

async def read_notification_jobs(consumer_name: str):
    jobs = await redis_client.xreadgroup(
        groupname="notification_group",
        consumername=consumer_name,
        streams={
            "notification_jobs": ">"
        },
        count=10,
        block=5000,
    )
    if not jobs:
        return []
    _, messages = jobs[0]
    notification_jobs = []
    for message_id, fields in messages:
        notification_jobs.append(
            (message_id, fields)
        )

    return notification_jobs