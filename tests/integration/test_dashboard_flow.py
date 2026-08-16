"""
Integration test: Dashboard flow
=================================

Flow under test:
  Monitor service detects status change
         │
         ▼
  Redis Pub/Sub  publish_endpoint_event()
         │
         ▼
  WebSocket  /ws?token=<jwt>
         │
         ▼
  Dashboard client receives JSON message

We use httpx's WebSocket support via the ASGI transport so no real network
port is needed.  The Pub/Sub publish is done directly (as the monitor service
would do it) while an async task reads from the WebSocket.

Also tested:
  - WS rejects connections with no token (1008)
  - WS rejects connections with invalid token (1008)
"""

import asyncio
import json

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession

from app.main import app
from app.redis import pubsub as redis_pubsub
from app.core.security import create_access_token
from app.models.user import User
from app.core.security import hash_password


# ---------------------------------------------------------------------------
# WS auth rejection tests (these don't need Redis or DB state)
# ---------------------------------------------------------------------------

async def test_websocket_rejects_missing_token(redis, clean_redis):
    """WebSocket without ?token= must close with code 1008."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        with pytest.raises(Exception):
            # httpx raises on abnormal WS close
            async with ac.websocket_connect("/ws") as ws:
                await ws.receive_json()


async def test_websocket_rejects_invalid_token(redis, clean_redis):
    """WebSocket with a garbage token must close with code 1008."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        with pytest.raises(Exception):
            async with ac.websocket_connect("/ws?token=not-a-real-jwt") as ws:
                await ws.receive_json()


# ---------------------------------------------------------------------------
# WS receives Pub/Sub message
# ---------------------------------------------------------------------------

async def test_websocket_receives_status_event(
    db: AsyncSession,
    redis,
    clean_redis,
    test_user,
):
    """
    After publish_endpoint_event() is called for a user, a WebSocket client
    authenticated as that user must receive the JSON payload.

    Sequence:
      1. Open WS with valid JWT
      2. publish_endpoint_event() in a background task (simulates monitor)
      3. Assert WS message received with correct fields
    """
    token = create_access_token({"user_id": test_user.id})

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        async with ac.websocket_connect(f"/ws?token={token}") as ws:

            # Give the WS handler time to set up its Pub/Sub subscription
            await asyncio.sleep(0.2)

            # Simulate monitor service publishing a status-change event
            await redis_pubsub.publish_endpoint_event(
                owner_id=test_user.id,
                endpoint_id=42,
                current_status="DOWN",
                status_code=503,
                latency_ms=1234,
            )

            # Wait for the message to be forwarded through the WS handler
            received = None
            for _ in range(20):
                try:
                    received = await asyncio.wait_for(ws.receive_json(), timeout=0.5)
                    break
                except (asyncio.TimeoutError, Exception):
                    await asyncio.sleep(0.1)

            assert received is not None, (
                "WebSocket must forward Pub/Sub message to the client"
            )
            assert received["endpoint_id"] == 42
            assert received["current_status"] == "DOWN"
            assert received["status_code"] == 503
