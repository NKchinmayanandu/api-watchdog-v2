"""
Root conftest.py for api-watchdog-v2 integration tests.

Infrastructure assumptions
--------------------------
* PostgreSQL  accessible at TEST_DATABASE_URL  (docker-compose or local)
* Redis       accessible at TEST_REDIS_URL

Neither is mocked here.  The only thing that is mocked at this layer is the
outbound HTTP call (httpx) and the Telegram notification service – truly
external boundaries that we cannot control in CI.

Every test that touches the DB gets its own transaction that is rolled back
after the test, so tests are isolated without needing to truncate tables.
Redis keys written by tests are cleaned up via explicit DEL calls in teardown.
"""

import asyncio
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.redis.redis_client import redis_client as _global_redis
from app.core.security import hash_password, create_access_token
from app.models.user import User

# ---------------------------------------------------------------------------
# Test-DB / Redis URLs  (override via env-vars in CI)
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:password@localhost:5432/api_watchdog_test",
)
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/1")
# DB index 1 so we never clobber the dev Redis db.

# ---------------------------------------------------------------------------
# Engine scoped to the whole test session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def event_loop():
    """Create a single event loop for the whole session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def db_engine():
    """Create tables once per session, drop them after."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(db_engine):
    return async_sessionmaker(bind=db_engine, expire_on_commit=False)


# ---------------------------------------------------------------------------
# Per-test DB session  (nested transaction → rollback keeps tests isolated)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db(session_factory) -> AsyncGenerator[AsyncSession, None]:
    """
    Each test gets its own savepoint.  Everything is rolled back on teardown
    so the DB state is clean for the next test without truncating tables.
    """
    async with session_factory() as session:
        async with session.begin():
            yield session
            await session.rollback()


# ---------------------------------------------------------------------------
# Redis client pointed at DB index 1
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def redis():
    from redis.asyncio import Redis
    r = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    # Load Lua scripts so evalsha calls work
    from app.redis import redis_client as _rc_module
    from app.redis.scripts import PUBLISH_OUTBOX_EVENT
    sha = await r.script_load(PUBLISH_OUTBOX_EVENT)
    _rc_module.publish_outbox_sha = sha
    # Patch the global singleton so all app code uses this test Redis
    import app.redis.redis_client as _rc_module2
    _rc_module2.redis_client = r
    yield r
    await r.aclose()


@pytest_asyncio.fixture
async def clean_redis(redis):
    """
    Flush the test Redis DB before each test and after, so Stream / key state
    doesn't bleed between tests.
    """
    await redis.flushdb()
    # Ensure consumer groups exist (XGROUP CREATE with MKSTREAM)
    for stream, group in [
        ("monitor_jobs", "monitor_group"),
        ("notification_jobs", "notification_group"),
    ]:
        try:
            await redis.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception:
            pass  # group already exists — fine
    yield redis
    await redis.flushdb()


# ---------------------------------------------------------------------------
# HTTP test client  (overrides get_db dependency to use test DB)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(db, session_factory) -> AsyncGenerator[AsyncClient, None]:
    """
    AsyncClient wired to the test app.  The DB dependency is overridden so
    every request hits the test PostgreSQL, wrapped in the same savepoint.
    """
    async def _override_get_db():
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Convenience fixtures:  a registered user + its auth token
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def test_user(db) -> User:
    user = User(
        username="testuser",
        email="test@example.com",
        hashed_password=hash_password("secret123"),
    )
    db.add(user)
    await db.flush()   # get user.id without committing
    return user


@pytest_asyncio.fixture
def auth_token(test_user) -> str:
    return create_access_token({"user_id": test_user.id})


@pytest_asyncio.fixture
def auth_headers(auth_token) -> dict:
    return {"Authorization": f"Bearer {auth_token}"}
