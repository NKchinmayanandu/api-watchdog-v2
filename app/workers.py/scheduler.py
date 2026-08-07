import time 
from app.redis.redis_client import redis_client
while True:
    now = int(time.time())

    due_endpoints = redis_client.zrange(
        "monitor-schedule",
        min="-inf",
        max=now,
        byscore=True
    )
    for endpoints in due_endpoints:
        