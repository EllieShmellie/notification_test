from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import IdempotencyKey, NotificationBatch, utcnow


class IdempotencyConflictError(Exception):
    pass


class IdempotencyRecordExists(Exception):
    def __init__(self, response_payload: dict[str, Any]) -> None:
        self.response_payload = response_payload


class IdempotencyService:
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = ttl_seconds

    async def reserve_or_get(
        self,
        session: AsyncSession,
        key: str,
        request_hash: str,
    ) -> IdempotencyKey:
        existing = await session.get(IdempotencyKey, key)
        if existing is not None and self._is_expired(existing.expires_at):
            if existing.batch_id is not None:
                batch = await session.get(NotificationBatch, existing.batch_id)
                if batch is not None:
                    batch.idempotency_key = None
            await session.delete(existing)
            await session.flush()
            existing = None

        if existing is None:
            record = IdempotencyKey(
                key=key,
                request_hash=request_hash,
                expires_at=utcnow() + timedelta(seconds=self.ttl_seconds),
            )
            session.add(record)
            return record

        if existing.request_hash != request_hash:
            raise IdempotencyConflictError

        if existing.response_payload is not None:
            raise IdempotencyRecordExists(existing.response_payload)

        return existing

    async def find(self, session: AsyncSession, key: str) -> IdempotencyKey | None:
        result = await session.execute(select(IdempotencyKey).where(IdempotencyKey.key == key))
        return result.scalar_one_or_none()

    async def cleanup_expired(self, session: AsyncSession) -> int:
        result = await session.execute(
            select(IdempotencyKey).where(IdempotencyKey.expires_at <= utcnow())
        )
        expired = list(result.scalars().all())
        for record in expired:
            if record.batch_id is not None:
                batch = await session.get(NotificationBatch, record.batch_id)
                if batch is not None:
                    batch.idempotency_key = None

        await session.execute(delete(IdempotencyKey).where(IdempotencyKey.expires_at <= utcnow()))
        return len(expired)

    def _is_expired(self, expires_at: datetime) -> bool:
        now = utcnow()
        if expires_at.tzinfo is None:
            return expires_at <= now.replace(tzinfo=None)
        return expires_at <= now
