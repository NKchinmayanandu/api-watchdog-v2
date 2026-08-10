from app.redis.redis_client import redis_client

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