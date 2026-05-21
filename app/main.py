from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import text

from app.api.v1.notifications import router as notifications_router
from app.api.v1.subscribers import router as subscribers_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import AsyncSessionLocal
from app.messaging.rabbitmq import RabbitMQTopology
from app.schemas.notifications import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    yield


app = FastAPI(
    title="Notification Service",
    version="0.1.0",
    description="Mass notification service with priority queues, retries and idempotency.",
    lifespan=lifespan,
)

app.include_router(notifications_router, prefix="/api/v1")
app.include_router(subscribers_router, prefix="/api/v1")


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health() -> HealthResponse:
    settings = get_settings()
    postgres = "error"
    rabbitmq = "error"
    redis = "error"

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        postgres = "ok"
    except Exception:
        postgres = "error"

    try:
        client = Redis.from_url(settings.redis_url, decode_responses=True)
        await client.ping()
        await client.aclose()
        redis = "ok"
    except Exception:
        redis = "error"

    try:
        topology = RabbitMQTopology(settings)
        await topology.connect()
        await topology.close()
        rabbitmq = "ok"
    except Exception:
        rabbitmq = "error"

    overall = "ok" if {postgres, rabbitmq, redis} == {"ok"} else "degraded"
    return HealthResponse(status=overall, postgres=postgres, rabbitmq=rabbitmq, redis=redis)

