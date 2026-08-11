from contextlib import asynccontextmanager
import json

from fastapi import FastAPI, WebSocket
from app.api.dependencies import decode_access_token
from app.api.auth import router as auth_router
from app.api.endpoint import router as endpoint_router
from app.core.config import settings
from app.utils.logging import setup_logging
from app.redis import pubsub
@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    yield


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
)

app.include_router(auth_router, prefix=settings.API_PREFIX)
app.include_router(endpoint_router, prefix=settings.API_PREFIX)

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "version": settings.VERSION}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=1008)
        return

    payload = decode_access_token(token)

    if not payload:
        await websocket.close(code=1008)
        return

    user_id = payload.get("user_id")

    if user_id is None:
        await websocket.close(code=1008)
        return

    redis_pubsub = await pubsub.subscribe_endpoint_events(
        owner_id=int(user_id)
    )

    try:
        while True:
            message = await redis_pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message:
                data = json.loads(message["data"])
                await websocket.send_json(data)

    finally:
        await redis_pubsub.unsubscribe(
            f"endpoint_status_changed:{user_id}"
        )
        await redis_pubsub.close()