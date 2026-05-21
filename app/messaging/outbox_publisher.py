import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_, case, or_, select

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.models import OutboxEvent, OutboxStatus, utcnow
from app.db.session import AsyncSessionLocal
from app.messaging.publisher import NotificationPublisher
from app.messaging.rabbitmq import RabbitMQTopology

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimedOutboxEvent:
    id: uuid.UUID
    event_type: str
    payload: dict


class OutboxPublisher:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.topology = RabbitMQTopology(self.settings)

    async def run_forever(self) -> None:
        await self.topology.connect()
        try:
            while True:
                published = await self.publish_pending()
                if published == 0:
                    await asyncio.sleep(self.settings.outbox_poll_interval_seconds)
        finally:
            await self.topology.close()

    async def publish_pending(self) -> int:
        publisher = NotificationPublisher(self.topology, self.settings)
        events = await self.claim_pending()

        for event in events:
            await self._publish_one(publisher, event)

        return len(events)

    async def _publish_one(
        self,
        publisher: NotificationPublisher,
        event: ClaimedOutboxEvent,
    ) -> None:
        async with AsyncSessionLocal() as session:
            db_event = await session.get(OutboxEvent, event.id)
            if db_event is None:
                return

            try:
                await publisher.publish_outbox_event(event.event_type, event.payload)
            except Exception as exc:
                self._mark_publish_failed(db_event, exc)
            else:
                self._mark_published(db_event)

            await session.commit()

    async def claim_pending(self) -> list[ClaimedOutboxEvent]:
        now = utcnow()
        claim_deadline = utcnow() - timedelta(seconds=self.settings.outbox_claim_timeout_seconds)
        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(OutboxEvent)
                    .where(
                        or_(
                            and_(
                                OutboxEvent.status == OutboxStatus.pending.value,
                                or_(
                                    OutboxEvent.available_at.is_(None),
                                    OutboxEvent.available_at <= now,
                                ),
                            ),
                            (
                                (OutboxEvent.status == OutboxStatus.processing.value)
                                & (OutboxEvent.locked_at < claim_deadline)
                            ),
                        )
                    )
                    .order_by(
                        case(
                            (
                                OutboxEvent.payload["routing_key"].as_string()
                                == "notification.high",
                                0,
                            ),
                            else_=1,
                        ),
                        OutboxEvent.created_at,
                    )
                    .limit(self.settings.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
                events = list(result.scalars().all())
                claimed_at = utcnow()
                for event in events:
                    event.status = OutboxStatus.processing.value
                    event.locked_at = claimed_at
                    event.attempts += 1

                return [
                    ClaimedOutboxEvent(
                        id=event.id,
                        event_type=event.event_type,
                        payload=dict(event.payload),
                    )
                    for event in events
                ]

    def _mark_published(self, event: OutboxEvent) -> None:
        event.status = OutboxStatus.published.value
        event.published_at = utcnow()
        event.locked_at = None
        event.last_error = None

    def _mark_publish_failed(self, event: OutboxEvent, exc: Exception) -> None:
        if event.attempts >= self.settings.outbox_max_attempts:
            event.status = OutboxStatus.failed.value
        else:
            event.status = OutboxStatus.pending.value
        event.locked_at = None
        event.last_error = str(exc)
        logger.exception(
            "failed to publish outbox event",
            extra={
                "event": "outbox_publish_failed",
                "event_id": str(event.id),
                "notification_id": str(event.aggregate_id),
            },
        )


async def main() -> None:
    configure_logging()
    await OutboxPublisher().run_forever()


if __name__ == "__main__":
    asyncio.run(main())
