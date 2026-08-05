from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.endpoint import Endpoint
from app.schemas.endpoint import EndpointCreate, EndpointUpdate


async def create_endpoint(
    db: AsyncSession, owner_id: int, data: EndpointCreate
) -> Endpoint:
    endpoint = Endpoint(
        owner_id=owner_id,
        name=data.name,
        url=str(data.url),
        enabled=data.enabled,
    )
    db.add(endpoint)
    await db.commit()
    await db.refresh(endpoint)
    return endpoint


async def get_endpoints_by_owner(
    db: AsyncSession, owner_id: int
) -> list[Endpoint]:
    result = await db.execute(
        select(Endpoint).where(Endpoint.owner_id == owner_id)
    )
    return list(result.scalars().all())


async def get_endpoint_by_id(
    db: AsyncSession, endpoint_id: int, owner_id: int
) -> Endpoint | None:
    result = await db.execute(
        select(Endpoint).where(
            Endpoint.id == endpoint_id,
            Endpoint.owner_id == owner_id,
        )
    )
    return result.scalar_one_or_none()


async def update_endpoint(
    db: AsyncSession, endpoint: Endpoint, data: EndpointUpdate
) -> Endpoint:
    endpoint.name = data.name
    endpoint.url = str(data.url)
    endpoint.enabled = data.enabled
    await db.commit()
    await db.refresh(endpoint)
    return endpoint


async def delete_endpoint(db: AsyncSession, endpoint: Endpoint) -> None:
    await db.delete(endpoint)
    await db.commit()
