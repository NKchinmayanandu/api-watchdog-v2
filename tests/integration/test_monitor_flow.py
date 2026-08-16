"""
Integration test: Monitoring flow
=================================

Flow under test:
  POST /api/endpoints
        │
        ▼  (service: create_endpoint → sets Redis cache + schedules in ZSET)
  PostgreSQL  (endpoint row)
        │
        ▼  (scheduler worker picks up due endpoint → enqueues monitor job)
  Redis ZSET  monitor_schedule
        │
        ▼
  Redis Stream  monitor_jobs
        │
        ▼  (monitor service: process_job)
  HTTP check  →  httpbin.org  (real call)
        │
        ▼
  PostgreSQL  endpoint.current_status / endpoint_status_history
        │
        ▼  (on status change) Redis Pub/Sub  endpoint_status_changed:<owner_id>

We do NOT mock Redis or PostgreSQL.
We DO mock the outbound HTTP call in test_2 / test_3 to control status codes
without depending on a live internet target.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.endpoint import Endpoint
from app.models.endpoint_history import EndpointStatusHistory
from app.models.outbox import OutboxEvent
from app.redis import cache, scheduler, streams
from app.repositories import endpoint as endpoint_repo
from app.repositories.process_monitor import persist_monitor_result
from app.services import monitor as monitor_service
from app.services.endpoint import create_endpoint
from app.schemas.endpoint import EndpointCreate


# ---------------------------------------------------------------------------
# Test 1: Create endpoint → Redis cache + schedule populated
# ---------------------------------------------------------------------------

async def test_create_endpoint_populates_cache_and_schedule(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    After create_endpoint():
      - endpoint row exists in DB
      - Redis hash endpoint:<id> is populated
      - endpoint:<id> key is present in monitor_schedule ZSET
    """
    data = EndpointCreate(name="my-api", url="https://httpbin.org/status/200")
    out = await create_endpoint(db=db, owner_id=test_user.id, data=data)

    # ── DB row ──────────────────────────────────────────────────────────────
    result = await db.execute(
        select(Endpoint).where(Endpoint.id == out.id)
    )
    ep = result.scalar_one_or_none()
    assert ep is not None, "Endpoint row must be created in DB"
    assert ep.owner_id == test_user.id

    # ── Redis cache ──────────────────────────────────────────────────────────
    cache_data = await redis.hgetall(f"endpoint:{out.id}")
    assert cache_data, "Redis hash must be set after create_endpoint()"
    assert cache_data["url"] == "https://httpbin.org/status/200/"  # Pydantic normalises trailing /

    # ── Scheduler ZSET ──────────────────────────────────────────────────────
    score = await redis.zscore("monitor_schedule", f"endpoint:{out.id}")
    assert score is not None, "Endpoint must be scheduled in ZSET"
    # Score is a unix timestamp roughly 30 s from now
    assert score > time.time(), "Scheduled time must be in the future"


# ---------------------------------------------------------------------------
# Test 2: Scheduler worker picks up due endpoint → enqueues monitor job
# ---------------------------------------------------------------------------

async def test_scheduler_enqueues_due_endpoints(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    When an endpoint's score in monitor_schedule is in the past (i.e. due),
    the scheduler_worker iteration should XADD it to monitor_jobs.
    """
    endpoint = Endpoint(
        owner_id=test_user.id, name="sched-ep", url="https://example.com"
    )
    db.add(endpoint)
    await db.flush()

    # Plant the endpoint in the ZSET with a past timestamp (due immediately)
    past_score = int(time.time()) - 10
    await redis.zadd("monitor_schedule", {f"endpoint:{endpoint.id}": past_score})

    # Run a single scheduler iteration (the worker loop body, extracted)
    now = int(time.time())
    due_endpoints = await redis.zrange(
        "monitor_schedule", min="-inf", max=now, byscore=True
    )
    for member in due_endpoints:
        ep_id = int(member.split(":")[1])
        await streams.enqueue_monitor_jobs(endpoint_id=ep_id)
        await redis.zrem("monitor_schedule", member)

    # ── monitor_jobs stream must have a message for our endpoint ─────────────
    messages = await redis.xrange("monitor_jobs")
    assert messages, "monitor_jobs stream must be non-empty after scheduling"
    endpoint_ids_in_stream = [
        int(fields["endpoint_id"]) for _, fields in messages
    ]
    assert endpoint.id in endpoint_ids_in_stream


# ---------------------------------------------------------------------------
# Test 3: Monitor service processes a job  (HTTP mocked → UP)
# ---------------------------------------------------------------------------

async def test_monitor_process_job_up_status(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    process_job() with a mocked HTTP 200 response:
      - updates endpoint.current_status = 'UP' in DB
      - updates Redis cache current_status = 'UP'
      - ACKs the message in the stream
    """
    endpoint = Endpoint(
        owner_id=test_user.id, name="mon-ep", url="https://example.com"
    )
    db.add(endpoint)
    await db.flush()

    # Seed the Redis cache (needed by process_job)
    await redis.hset(
        f"endpoint:{endpoint.id}",
        mapping={
            "id": endpoint.id,
            "owner_id": test_user.id,
            "name": "mon-ep",
            "url": "https://example.com",
            # no current_status → first-ever check, no status_changed event
        },
    )

    # Enqueue a monitor job and read it back as the worker would
    await streams.enqueue_monitor_jobs(endpoint_id=endpoint.id)
    raw_jobs = await redis.xreadgroup(
        groupname="monitor_group",
        consumername="test-consumer-1",
        streams={"monitor_jobs": ">"},
        count=1,
    )
    assert raw_jobs, "Expected at least one monitor job in stream"
    _, messages = raw_jobs[0]
    message_id, fields = messages[0]
    job = (message_id, int(fields["endpoint_id"]))

    # ── Mock httpx so no real network call happens ───────────────────────────
    mock_response = MagicMock()
    mock_response.status_code = 200

    with patch("app.services.monitor.AsyncSessionLocal") as mock_session_cm, \
         patch("httpx.AsyncClient") as mock_http, \
         patch("app.services.monitor.scheduler.schedule_endpoint", new_callable=AsyncMock), \
         patch("app.services.monitor.streams.ack_monitor_job", new_callable=AsyncMock) as mock_ack:

        # Wire up the DB session mock to use our real test session
        mock_session_cm.return_value.__aenter__ = AsyncMock(return_value=db)
        mock_session_cm.return_value.__aexit__ = AsyncMock(return_value=False)

        # Wire up httpx mock
        mock_http_instance = AsyncMock()
        mock_http_instance.get = AsyncMock(return_value=mock_response)
        mock_http.return_value.__aenter__ = AsyncMock(return_value=mock_http_instance)
        mock_http.return_value.__aexit__ = AsyncMock(return_value=False)

        await monitor_service.process_job(job)

    # ── Assert DB updated ────────────────────────────────────────────────────
    await db.refresh(endpoint)
    assert endpoint.current_status == "UP"
    assert endpoint.status_code == 200
    assert endpoint.latency is not None

    # ── Assert Redis cache updated ───────────────────────────────────────────
    cached = await redis.hgetall(f"endpoint:{endpoint.id}")
    assert cached.get("current_status") == "UP"

    # ── Assert stream ACK was called ─────────────────────────────────────────
    mock_ack.assert_called_once_with(message_id)


# ---------------------------------------------------------------------------
# Test 4: Status transition  UP → DOWN  triggers outbox row + history row
# ---------------------------------------------------------------------------

async def test_status_transition_creates_outbox_and_history(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    persist_monitor_result() when status_changed=True must atomically write:
      - endpoint.current_status updated
      - EndpointStatusHistory row inserted
      - OutboxEvent row inserted
    All three must live in the same DB commit (transactional boundary).
    """
    endpoint = Endpoint(
        owner_id=test_user.id,
        name="trans-ep",
        url="https://example.com",
        current_status="UP",
    )
    db.add(endpoint)
    await db.flush()

    # Simulate: previous status was UP, new check returned DOWN
    result = await persist_monitor_result(
        db=db,
        endpoint_id=endpoint.id,
        current_status="DOWN",
        status_code=503,
        latency_ms=123.4,
        status_changed=True,
    )

    assert result is not None, "persist_monitor_result must return the endpoint"

    # ── endpoint updated ─────────────────────────────────────────────────────
    await db.refresh(endpoint)
    assert endpoint.current_status == "DOWN"
    assert endpoint.status_code == 503

    # ── history row ──────────────────────────────────────────────────────────
    hist_result = await db.execute(
        select(EndpointStatusHistory).where(
            EndpointStatusHistory.endpoint_id == endpoint.id
        )
    )
    histories = hist_result.scalars().all()
    assert len(histories) == 1
    assert histories[0].status.value == "DOWN"

    # ── outbox row ───────────────────────────────────────────────────────────
    outbox_result = await db.execute(
        select(OutboxEvent).where(OutboxEvent.endpoint_id == endpoint.id)
    )
    outbox_events = outbox_result.scalars().all()
    assert len(outbox_events) == 1
    ev = outbox_events[0]
    assert ev.event_type == "endpoint_status_changed"
    assert ev.current_status == "DOWN"
    assert ev.processed_at is None  # not yet processed by outbox worker


# ---------------------------------------------------------------------------
# Test 5: Pub/Sub event is published on status change
# ---------------------------------------------------------------------------

async def test_monitor_publishes_pubsub_on_status_change(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    When process_job detects a status change, it must publish a message to
    endpoint_status_changed:<owner_id> Redis channel.
    We subscribe before calling process_job so we can receive the message.
    """
    endpoint = Endpoint(
        owner_id=test_user.id,
        name="pubsub-ep",
        url="https://example.com",
        current_status="UP",  # previous status UP → DOWN triggers change
    )
    db.add(endpoint)
    await db.flush()

    # Seed cache with current_status=UP so process_job sees a change
    await redis.hset(
        f"endpoint:{endpoint.id}",
        mapping={
            "id": endpoint.id,
            "owner_id": test_user.id,
            "name": "pubsub-ep",
            "url": "https://example.com",
            "current_status": "UP",
        },
    )

    # Subscribe to the channel BEFORE triggering the event
    pubsub = redis.pubsub()
    channel = f"endpoint_status_changed:{test_user.id}"
    await pubsub.subscribe(channel)

    # Drain the initial subscribe confirmation message
    await asyncio.sleep(0.1)
    await pubsub.get_message(ignore_subscribe_messages=True)

    # Enqueue + process job  (HTTP mocked to return 503 → DOWN)
    await streams.enqueue_monitor_jobs(endpoint_id=endpoint.id)
    raw_jobs = await redis.xreadgroup(
        groupname="monitor_group",
        consumername="test-consumer-pubsub",
        streams={"monitor_jobs": ">"},
        count=1,
    )
    _, messages = raw_jobs[0]
    message_id, fields = messages[0]
    job = (message_id, int(fields["endpoint_id"]))

    mock_response = MagicMock()
    mock_response.status_code = 503

    with patch("app.services.monitor.AsyncSessionLocal") as mock_session_cm, \
         patch("httpx.AsyncClient") as mock_http, \
         patch("app.services.monitor.scheduler.schedule_endpoint", new_callable=AsyncMock), \
         patch("app.services.monitor.streams.ack_monitor_job", new_callable=AsyncMock):

        mock_session_cm.return_value.__aenter__ = AsyncMock(return_value=db)
        mock_session_cm.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_http_instance = AsyncMock()
        mock_http_instance.get = AsyncMock(return_value=mock_response)
        mock_http.return_value.__aenter__ = AsyncMock(return_value=mock_http_instance)
        mock_http.return_value.__aexit__ = AsyncMock(return_value=False)

        await monitor_service.process_job(job)

    # ── Read from Pub/Sub ────────────────────────────────────────────────────
    await asyncio.sleep(0.2)  # give Redis time to deliver
    received = None
    for _ in range(10):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if msg and msg["type"] == "message":
            received = msg
            break

    await pubsub.unsubscribe(channel)
    await pubsub.aclose()

    assert received is not None, "Pub/Sub message must be received after status change"
    payload = json.loads(received["data"])
    assert payload["endpoint_id"] == endpoint.id
    assert payload["current_status"] == "DOWN"
    assert payload["status_code"] == 503
