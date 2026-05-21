from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "local"
    app_debug: bool = True

    database_url: str = "postgresql+asyncpg://notifications:notifications@postgres:5432/notifications"
    redis_url: str = "redis://redis:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    rabbitmq_exchange: str = "notifications.exchange"
    rabbitmq_dlx: str = "notifications.dlx"
    rabbitmq_high_queue: str = "notifications.high"
    rabbitmq_low_queue: str = "notifications.low"
    rabbitmq_dlq: str = "notifications.dlq"

    max_retries: int = 3
    retry_backoff_seconds: str = "10,30,60"
    idempotency_ttl_seconds: int = 86_400
    idempotency_lock_ttl_seconds: int = 300
    idempotency_cleanup_interval_seconds: int = 3600
    processing_lock_ttl_seconds: int = 60
    outbox_claim_timeout_seconds: int = 300

    sms_provider_mode: Literal[
        "success",
        "slow_success",
        "temporary_error_once",
        "temporary_error_always",
        "permanent_error",
        "random",
    ] = "success"
    email_provider_mode: Literal[
        "success",
        "slow_success",
        "temporary_error_once",
        "temporary_error_always",
        "permanent_error",
        "random",
    ] = "success"

    worker_poll_interval_seconds: float = 0.25
    worker_queue_mode: Literal["high", "low", "both"] = "both"
    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_max_attempts: int = 5

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def retry_backoff_list(self) -> list[int]:
        return [int(item.strip()) for item in self.retry_backoff_seconds.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
