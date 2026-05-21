import uuid
from datetime import timedelta

from sqlalchemy import func, select

from app.core.config import Settings
from app.db.models import (
    Notification,
    NotificationStatus,
    NotificationStatusHistory,
    OutboxEvent,
    OutboxStatus,
    utcnow,
)
from app.providers.mock import SmsMockProvider
from app.schemas.notifications import BulkNotificationRequest
from app.services.idempotency_service import IdempotencyConflictError
from app.services.notification_service import NotificationService
from app.worker.processor import NotificationProcessor, ProcessResult


def sqlite_settings(**overrides) -> Settings:
    return Settings(database_url="sqlite+aiosqlite:///:memory:", **overrides)


def sms_processor(settings: Settings, provider: SmsMockProvider) -> NotificationProcessor:
    return NotificationProcessor(settings=settings, providers={"sms": provider})


def sms_bulk_request(
    *,
    message: str = "code",
    recipient_ids: list[int] | None = None,
    notification_type: str = "transactional",
) -> BulkNotificationRequest:
    return BulkNotificationRequest(
        channel="sms",
        type=notification_type,
        message=message,
        recipient_ids=recipient_ids or [1000],
    )


async def test_bulk_request_creates_notifications_and_outbox(session_factory, fake_redis) -> None:
    settings = sqlite_settings()
    service = NotificationService(settings)

    async with session_factory() as session:
        response = await service.create_bulk(
            session=session,
            redis=fake_redis,
            request=BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code 123",
                recipient_ids=[1001, 1002, 1002],
            ),
            idempotency_key="bulk-key-1",
        )

        assert response.status == "accepted"
        assert response.notifications_created == 2

        notifications = (await session.execute(select(Notification))).scalars().all()
        outbox_events = (await session.execute(select(OutboxEvent))).scalars().all()
        history = (await session.execute(select(NotificationStatusHistory))).scalars().all()

        assert len(notifications) == 2
        assert len(outbox_events) == 2
        assert all(event.status == OutboxStatus.pending.value for event in outbox_events)
        assert all(event.payload["routing_key"] == "notification.high" for event in outbox_events)
        assert [item.status for item in history] == ["queued", "queued"]


async def test_idempotency_returns_existing_response(session_factory, fake_redis) -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:")
    service = NotificationService(settings)
    request = BulkNotificationRequest(
        channel="sms",
        type="marketing",
        message="promo",
        recipient_ids=[1, 2],
    )

    async with session_factory() as session:
        first = await service.create_bulk(session, fake_redis, request, "same-key")
        second = await service.create_bulk(session, fake_redis, request, "same-key")

        notifications = (await session.execute(select(Notification))).scalars().all()

        assert first.batch_id == second.batch_id
        assert second.status == "already_exists"
        assert len(notifications) == 2


async def test_idempotency_conflict(session_factory, fake_redis) -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:")
    service = NotificationService(settings)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="marketing",
                message="promo",
                recipient_ids=[1],
            ),
            "conflict-key",
        )

        try:
            await service.create_bulk(
                session,
                fake_redis,
                BulkNotificationRequest(
                    channel="sms",
                    type="marketing",
                    message="another promo",
                    recipient_ids=[1],
                ),
                "conflict-key",
            )
        except IdempotencyConflictError:
            pass
        else:
            raise AssertionError("Expected idempotency conflict")


async def test_expired_idempotency_key_can_be_reused(session_factory, fake_redis) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        idempotency_ttl_seconds=-1,
    )
    service = NotificationService(settings)

    async with session_factory() as session:
        first = await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="marketing",
                message="old promo",
                recipient_ids=[1],
            ),
            "expired-key",
        )
        second = await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="marketing",
                message="new promo",
                recipient_ids=[1],
            ),
            "expired-key",
        )

        assert first.batch_id != second.batch_id
        assert second.status == "accepted"


async def test_worker_delivers_notification_once(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        max_retries=3,
        retry_backoff_seconds="1,2,3",
    )
    service = NotificationService(settings)
    provider = SmsMockProvider("success")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[777],
            ),
            "worker-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack
        assert await processor.process(session, outbox.payload) == ProcessResult.ack

        notification = (await session.execute(select(Notification))).scalar_one()
        history = (
            await session.execute(
                select(NotificationStatusHistory).order_by(NotificationStatusHistory.created_at)
            )
        ).scalars().all()

        assert notification.status == NotificationStatus.delivered.value
        assert notification.attempts == 1
        assert len(provider.calls) == 1
        assert [item.status for item in history] == ["queued", "sent", "delivered"]


async def test_worker_retries_temporary_error_then_delivers(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        max_retries=3,
        retry_backoff_seconds="1,2,3",
    )
    service = NotificationService(settings)
    provider = SmsMockProvider("temporary_error_once")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[888],
            ),
            "retry-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack
        retry_outbox = (
            await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "notification.retry_scheduled"
                )
            )
        ).scalar_one()

        retry_payload = retry_outbox.payload
        assert retry_payload["retry_delay_seconds"] == 1
        assert retry_outbox.available_at is not None
        notification = (await session.execute(select(Notification))).scalar_one()
        notification.processing_lock_until = utcnow()
        await session.commit()

        assert await processor.process(session, retry_payload) == ProcessResult.ack
        notification = (await session.execute(select(Notification))).scalar_one()

        assert notification.status == NotificationStatus.delivered.value
        assert notification.attempts == 2
        assert len(provider.calls) == 2


async def test_duplicate_original_message_waits_for_retry_backoff(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        max_retries=3,
        retry_backoff_seconds="60,120,180",
    )
    service = NotificationService(settings)
    provider = SmsMockProvider("temporary_error_once")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[999],
            ),
            "duplicate-backoff-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack
        assert await processor.process(session, outbox.payload) == ProcessResult.ack
        assert await processor.process(session, outbox.payload) == ProcessResult.ack

        notification = (await session.execute(select(Notification))).scalar_one()
        retry_count = (
            await session.execute(
                select(func.count(OutboxEvent.id)).where(
                    OutboxEvent.event_type == "notification.retry_scheduled",
                )
            )
        ).scalar_one()

        assert notification.status == NotificationStatus.sent.value
        assert notification.attempts == 1
        assert len(provider.calls) == 1
        assert retry_count == 1


async def test_existing_redis_processing_lock_does_not_ack_queued_message(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:")
    service = NotificationService(settings)
    provider = SmsMockProvider("success")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[1001],
            ),
            "redis-lock-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        fake_redis.storage[f"notification:{outbox.aggregate_id}:processing"] = "stale-lock"
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack

        notification = (await session.execute(select(Notification))).scalar_one()
        assert notification.status == NotificationStatus.delivered.value
        assert len(provider.calls) == 1


async def test_active_sent_processing_lock_schedules_delayed_redelivery(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        processing_lock_ttl_seconds=60,
    )
    service = NotificationService(settings)
    provider = SmsMockProvider("success")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[1000],
            ),
            "active-lock-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        notification = (await session.execute(select(Notification))).scalar_one()
        notification.status = NotificationStatus.sent.value
        notification.attempts = 1
        notification.processing_lock_until = utcnow()
        notification.processing_lock_until = notification.processing_lock_until.replace(
            year=notification.processing_lock_until.year + 1
        )
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack
        deferred_retry = (
            await session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "notification.retry_scheduled",
                    OutboxEvent.payload["deferred_reason"].as_string()
                    == "processing_lock_active",
                )
            )
        ).scalar_one()

        assert len(provider.calls) == 0
        assert deferred_retry.payload["retry_delay_seconds"] > 0


async def test_active_sent_processing_lock_replaces_published_retry_event(
    session_factory,
    fake_redis,
) -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        processing_lock_ttl_seconds=60,
    )
    service = NotificationService(settings)
    provider = SmsMockProvider("success")
    processor = sms_processor(settings, provider)

    async with session_factory() as session:
        await service.create_bulk(
            session,
            fake_redis,
            BulkNotificationRequest(
                channel="sms",
                type="transactional",
                message="code",
                recipient_ids=[1000],
            ),
            "published-retry-key",
        )
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()
        notification = (await session.execute(select(Notification))).scalar_one()
        notification.status = NotificationStatus.sent.value
        notification.attempts = 1
        notification.processing_lock_until = utcnow() + timedelta(seconds=60)
        session.add(
            OutboxEvent(
                aggregate_type="notification",
                aggregate_id=notification.id,
                event_type="notification.retry_scheduled",
                payload={
                    **outbox.payload,
                    "deferred_reason": "processing_lock_active",
                },
                status=OutboxStatus.published.value,
                available_at=notification.processing_lock_until,
                published_at=utcnow(),
            )
        )
        await session.commit()

        assert await processor.process(session, outbox.payload) == ProcessResult.ack

        retry_count = await session.scalar(
            select(func.count(OutboxEvent.id)).where(
                OutboxEvent.event_type == "notification.retry_scheduled",
            )
        )
        assert retry_count == 2
        assert len(provider.calls) == 0


async def test_outbox_poison_event_is_marked_failed(session_factory, monkeypatch) -> None:
    import app.messaging.outbox_publisher as outbox_module

    monkeypatch.setattr(outbox_module, "AsyncSessionLocal", session_factory)
    publisher = outbox_module.OutboxPublisher()
    publisher.settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        outbox_max_attempts=1,
    )

    async with session_factory() as session:
        event = OutboxEvent(
            aggregate_type="notification",
            aggregate_id=uuid.uuid4(),
            event_type="unknown.event",
            payload={"routing_key": "notification.high"},
        )
        session.add(event)
        await session.commit()

        assert await publisher.publish_pending() == 1

        refreshed = await session.get(OutboxEvent, event.id)
        await session.refresh(refreshed)
        assert refreshed.status == OutboxStatus.failed.value
        assert "Unsupported outbox event type" in refreshed.last_error


async def test_outbox_does_not_claim_retry_before_available_at(
    session_factory,
    monkeypatch,
) -> None:
    import app.messaging.outbox_publisher as outbox_module

    monkeypatch.setattr(outbox_module, "AsyncSessionLocal", session_factory)
    publisher = outbox_module.OutboxPublisher()
    publisher.settings = Settings(database_url="sqlite+aiosqlite:///:memory:")

    async with session_factory() as session:
        event = OutboxEvent(
            aggregate_type="notification",
            aggregate_id=uuid.uuid4(),
            event_type="notification.retry_scheduled",
            payload={"routing_key": "notification.high"},
            available_at=utcnow() + timedelta(seconds=60),
        )
        session.add(event)
        await session.commit()

        assert await publisher.publish_pending() == 0

        refreshed = await session.get(OutboxEvent, event.id)
        await session.refresh(refreshed)
        assert refreshed.status == OutboxStatus.pending.value


async def test_outbox_claim_prioritizes_high_routing_key(
    session_factory,
    monkeypatch,
) -> None:
    import app.messaging.outbox_publisher as outbox_module

    monkeypatch.setattr(outbox_module, "AsyncSessionLocal", session_factory)
    publisher = outbox_module.OutboxPublisher()
    publisher.settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        outbox_batch_size=1,
    )

    async with session_factory() as session:
        old_time = utcnow() - timedelta(minutes=5)
        session.add_all(
            [
                OutboxEvent(
                    aggregate_type="notification",
                    aggregate_id=uuid.uuid4(),
                    event_type="notification.created",
                    payload={"routing_key": "notification.low"},
                    created_at=old_time,
                ),
                OutboxEvent(
                    aggregate_type="notification",
                    aggregate_id=uuid.uuid4(),
                    event_type="notification.created",
                    payload={"routing_key": "notification.low"},
                    created_at=old_time + timedelta(seconds=1),
                ),
                OutboxEvent(
                    aggregate_type="notification",
                    aggregate_id=uuid.uuid4(),
                    event_type="notification.created",
                    payload={"routing_key": "notification.high"},
                    created_at=utcnow(),
                ),
            ]
        )
        await session.commit()

        claimed = await publisher.claim_pending()

        assert len(claimed) == 1
        assert claimed[0].payload["routing_key"] == "notification.high"
