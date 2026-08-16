"""
Shared helpers / sub-fixtures for integration tests.

These build on top of the root conftest.  They create real DB rows (user +
endpoint) and expose small async helper functions tests can call to drive
workers directly without spinning up separate processes.
"""

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.endpoint import Endpoint
from app.models.user import User
from app.core.security import hash_password, create_access_token


@pytest_asyncio.fixture
async def user_and_endpoint(db: AsyncSession):
    """
    Returns (user, endpoint) persisted inside the test transaction.
    The endpoint URL points to httpbin.org/status/200 – a well-known
    always-UP public echo.  Tests that need the HTTP call to succeed use
    this fixture directly; tests that mock httpx swap the fixture or patch
    the client later.
    """
    user = User(
        username="int_user",
        email="int@example.com",
        hashed_password=hash_password("pass"),
    )
    db.add(user)
    await db.flush()

    endpoint = Endpoint(
        owner_id=user.id,
        name="test-ep",
        url="https://httpbin.org/status/200",
    )
    db.add(endpoint)
    await db.flush()

    return user, endpoint


@pytest_asyncio.fixture
def token_for(user_and_endpoint):
    user, _ = user_and_endpoint
    return create_access_token({"user_id": user.id})
