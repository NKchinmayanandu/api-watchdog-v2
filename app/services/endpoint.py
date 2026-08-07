from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import AlreadyExistsError, NotFoundError
from app.models.endpoint import Endpoint
from app.repositories import endpoint as repo
from app.schemas.endpoint import EndpointCreate, EndpointOut, EndpointUpdate
from app.redis import cache, scheduler
import logging
async def create_endpoint(
    db: AsyncSession, owner_id: int, data: EndpointCreate
) -> EndpointOut:
    endpoint = await repo.create_endpoint(db, owner_id, data)
    await cache.set_endpoint_cache(endpoint=endpoint)
    await scheduler.schedule_endpoint(endpoint_id=endpoint.id)
    return EndpointOut.model_validate(endpoint)


async def get_all_endpoints(
    db: AsyncSession, owner_id: int
) -> list[EndpointOut]:
    endpoints = await repo.get_endpoints_by_owner(db, owner_id)
    return [EndpointOut.model_validate(e) for e in endpoints]


async def update_endpoint(
    db: AsyncSession, endpoint_id: int, owner_id: int, data: EndpointUpdate
) -> EndpointOut:
    endpoint = await _get_owned_or_404(db, endpoint_id, owner_id)
    updated = await repo.update_endpoint(db, endpoint, data)
    await cache.set_endpoint_cache(endpoint=updated)
    return EndpointOut.model_validate(updated)


async def delete_endpoint(
    db: AsyncSession, endpoint_id: int, owner_id: int
) -> None:
    endpoint = await _get_owned_or_404(db, endpoint_id, owner_id)
    await repo.delete_endpoint(db, endpoint)
    await cache.delete_endpoint_cache(endpoint_id=endpoint.id)
    await scheduler.delete_scheduled_endpoint(endpoint_id=endpoint)
    logging.info(
    "Endpoint deleted",
    extra={
        "endpoint_id": endpoint.id,
        "owner_id": owner_id,
        },
    )

async def _get_owned_or_404(
    db: AsyncSession, endpoint_id: int, owner_id: int
) -> Endpoint:
    endpoint = await repo.get_endpoint_by_id(db, endpoint_id, owner_id)
    if not endpoint:
        raise NotFoundError("Endpoint not found")
    return endpoint
