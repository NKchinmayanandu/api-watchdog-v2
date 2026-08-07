from datetime import datetime

from pydantic import BaseModel, HttpUrl


class EndpointCreate(BaseModel):
    name: str
    url: HttpUrl


class EndpointUpdate(BaseModel):
    name: str
    url: HttpUrl


class EndpointOut(BaseModel):
    id: int
    name: str
    url: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
