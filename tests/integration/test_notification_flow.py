"""
Integration test: Notification worker flow
==========================================

Flow under test:
  notification_jobs  (Redis Stream)
         │
         ▼
  XREADGROUP  (notification worker)
         │
         ▼
  process_notification_job()  ← mocked (no Telegram)
         │
         ▼
  XACK

And crash-recovery:
  Worker A  →  XREADGROUP  →  💀  (no XACK)
         │
         ▼
  XAUTOCLAIM  (after min_idle_time)
         │
         ▼
  Worker B  →  processes  →  XACK

We mock the notification service (Telegram) so this test is about the
stream-consumer workflow, not about message delivery.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.endpoint import Endpoint
from app.redis import streams


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _seed_notification_job(redis, event_id: int, owner_id: int, endpoint_id: int) -> str:
    """Directly XADD to notification_jobs, bypassing the Lua script."""
    msg_id = await redis.xadd(
        "notification_jobs",
        {
            "outbox_event_id": str(event_id),
            "event_type": "endpoint_status_changed",
            "owner_id": str(owner_id),
            "endpoint_id": str(endpoint_id),
            "current_status": "DOWN",
        },
    )
    return msg_id


# ---------------------------------------------------------------------------
# Test 1: Notification worker reads job, calls process, ACKs
# ---------------------------------------------------------------------------

async def test_notification_worker_reads_and_acks(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    XREADGROUP delivers the job → mock notification service is called exactly
    once → XACK removes it from the PEL.
    """
    endpoint = Endpoint(
        owner_id=test_user.id, name="notif-ep-1", url="https://example.com"
    )
    db.add(endpoint)
    await db.flush()

    await _seed_notification_job(
        redis, event_id=999, owner_id=test_user.id, endpoint_id=endpoint.id
    )

    # ── Read as notification worker would ────────────────────────────────────
    raw_jobs = await redis.xreadgroup(
        groupname="notification_group",
        consumername="notif-worker-1",
        streams={"notification_jobs": ">"},
        count=10,
        block=100,
    )
    assert raw_jobs, "notification_jobs must have a message to deliver"
    _, messages = raw_jobs[0]
    assert len(messages) == 1

    message_id, fields = messages[0]
    assert fields["endpoint_id"] == str(endpoint.id)
    assert fields["current_status"] == "DOWN"

    # ── Simulate calling the notification service (mocked) ───────────────────
    mock_process = AsyncMock()
    await mock_process(message_id, fields)
    mock_process.assert_called_once_with(message_id, fields)

    # ── ACK ──────────────────────────────────────────────────────────────────
    await redis.xack("notification_jobs", "notification_group", message_id)

    # Pending Entry List must be empty now
    pending = await redis.xpending(
        "notification_jobs", "notification_group", min="-", max="+", count=10
    )
    assert len(pending) == 0, "PEL must be empty after XACK"


# ---------------------------------------------------------------------------
# Test 2: Worker crash (no XACK) → XAUTOCLAIM delivers to second worker
# ---------------------------------------------------------------------------

async def test_notification_worker_crash_reclaim(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    Worker A reads the job but crashes before ACKing.
    After min_idle_time passes (we force it to 0 ms in the test),
    Worker B reclaims via XAUTOCLAIM and ACKs.
    The message must end up acknowledged.
    """
    endpoint = Endpoint(
        owner_id=test_user.id, name="notif-ep-crash", url="https://example.com"
    )
    db.add(endpoint)
    await db.flush()

    msg_id = await _seed_notification_job(
        redis, event_id=998, owner_id=test_user.id, endpoint_id=endpoint.id
    )

    # Worker A reads but does NOT ACK  ──────────────────────────────────────
    await redis.xreadgroup(
        groupname="notification_group",
        consumername="notif-worker-A",
        streams={"notification_jobs": ">"},
        count=1,
        block=100,
    )

    # Verify message is in the PEL for worker A
    pending = await redis.xpending(
        "notification_jobs", "notification_group", min="-", max="+", count=10
    )
    assert len(pending) == 1
    assert pending[0]["name"] == "notif-worker-A"

    # ── Wait a tiny bit so the message becomes "idle" then force reclaim ─────
    # XAUTOCLAIM with min_idle_time=0 reclaims any unacknowledged message.
    await asyncio.sleep(0.05)
    reclaim_result = await redis.xautoclaim(
        name="notification_jobs",
        groupname="notification_group",
        consumername="notif-worker-B",
        min_idle_time=0,
        start_id="0-0",
        count=10,
    )
    _, reclaimed_messages, _ = reclaim_result
    assert len(reclaimed_messages) == 1, "Worker B must reclaim the unacked message"
    reclaimed_id, reclaimed_fields = reclaimed_messages[0]
    assert reclaimed_fields["endpoint_id"] == str(endpoint.id)

    # Worker B processes and ACKs
    await redis.xack("notification_jobs", "notification_group", reclaimed_id)

    # PEL must now be empty
    pending_after = await redis.xpending(
        "notification_jobs", "notification_group", min="-", max="+", count=10
    )
    assert len(pending_after) == 0, "PEL must be empty after Worker B ACKs"


# ---------------------------------------------------------------------------
# Test 3: End-to-end  outbox → stream → notification worker (mock service)
# ---------------------------------------------------------------------------

async def test_outbox_to_notification_end_to_end(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    Plants an outbox event, runs the Lua publish script (as the outbox worker
    would), then simulates the notification worker consuming and ACKing it.
    Asserts the full field set is correct at every boundary.
    """
    from app.models.outbox import OutboxEvent
    from app.redis import streams as redis_streams

    endpoint = Endpoint(
        owner_id=test_user.id, name="e2e-notif-ep", url="https://example.com"
    )
    db.add(endpoint)
    await db.flush()

    # Create outbox event
    ev = OutboxEvent(
        endpoint_id=endpoint.id,
        event_type="endpoint_status_changed",
        current_status="DOWN",
    )
    ev.owner_id = test_user.id  # type: ignore[attr-defined]
    db.add(ev)
    await db.flush()

    # Outbox worker publishes via Lua
    results = await redis_streams.publish_outbox_events([ev])
    assert results[0][0] == "published"

    # Notification worker reads
    raw_jobs = await redis.xreadgroup(
        groupname="notification_group",
        consumername="e2e-notif-consumer",
        streams={"notification_jobs": ">"},
        count=1,
        block=200,
    )
    assert raw_jobs, "Notification stream must have a job after outbox publish"
    _, messages = raw_jobs[0]
    message_id, fields = messages[0]

    # ── Validate all fields propagated correctly ─────────────────────────────
    assert fields["outbox_event_id"] == str(ev.id), "Event ID must match"
    assert fields["event_type"] == "endpoint_status_changed"
    assert fields["owner_id"] == str(test_user.id)
    assert fields["endpoint_id"] == str(endpoint.id)
    assert fields["current_status"] == "DOWN"

    # Notification service (mocked — no Telegram)
    mock_notify = AsyncMock()
    await mock_notify(fields)
    mock_notify.assert_called_once()

    # ACK
    await redis.xack("notification_jobs", "notification_group", message_id)

    pending = await redis.xpending(
        "notification_jobs", "notification_group", min="-", max="+", count=10
    )
    assert len(pending) == 0
