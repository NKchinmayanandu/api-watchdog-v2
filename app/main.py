from fastapi import FastAPI 
from app.api.auth import router as auth_router
from app.core.config import settings
from contextlib import asynccontextmanager
from app.utils.logging import setup_logging

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

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "version": settings.VERSION}