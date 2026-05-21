import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import Settings
from app.db.models import (
    IdempotencyKey,
    Notification,
    NotificationBatch,
    NotificationStatus,
    NotificationStatusHistory,
    NotificationType,
)
from app.schemas.notifications import (
    BulkNotificationRequest,
    BulkNotificationResponse,
    NotificationItem,
    NotificationListResponse,
    StatusHistoryItem,
)
from app.services.hashing import canonical_json_hash
from app.services.idempotency_service import (
    IdempotencyConflictError,
    IdempotencyRecordExists,
    IdempotencyService,
)
from app.services.outbox_service import add_outbox_event, notification_payload


class IdempotencyInProgressError(Exception):
    pass


def routing_key_for_notification_type(notification_type: str) -> str:
    if notification_type == NotificationType.transactional.value:
        return "notification.high"
    return "notification.low"


class NotificationService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.idempotency = IdempotencyService(settings.idempotency_ttl_seconds)

    async def create_bulk(
        self,
        session: AsyncSession,
        redis: Redis,
        request: BulkNotificationRequest,
        idempotency_key: str,
    ) -> BulkNotificationResponse:
        request_hash = canonical_json_hash(request.model_dump(mode="json"))
        lock_token = str(uuid.uuid4())
        lock_key = f"idem:{idempotency_key}:lock"
        lock_acquired = await redis.set(
            lock_key,
            lock_token,
            nx=True,
            ex=self.settings.idempotency_lock_ttl_seconds,
        )

        if not lock_acquired:
            raise IdempotencyInProgressError

        try:
            try:
                async with session.begin():
                    try:
                        record = await self.idempotency.reserve_or_get(
                            session=session,
                            key=idempotency_key,
                            request_hash=request_hash,
                        )
                    except IdempotencyRecordExists as exc:
                        payload = dict(exc.response_payload)
                        payload["status"] = "already_exists"
                        return BulkNotificationResponse(**payload)

                    batch_id = uuid.uuid4()
                    recipient_ids = list(dict.fromkeys(request.recipient_ids))
                    response_payload = {
                        "batch_id": str(batch_id),
                        "status": "accepted",
                        "notifications_created": len(recipient_ids),
                    }

                    batch = NotificationBatch(
                        id=batch_id,
                        channel=request.channel.value,
                        type=request.type.value,
                        message=request.message,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                    )
                    session.add(batch)
                    await session.flush()

                    for recipient_id in recipient_ids:
                        notification_id = uuid.uuid4()
                        notification = Notification(
                            id=notification_id,
                            batch_id=batch_id,
                            subscriber_id=recipient_id,
                            channel=request.channel.value,
                            type=request.type.value,
                            message=request.message,
                            status=NotificationStatus.queued.value,
                        )
                        history = NotificationStatusHistory(
                            notification_id=notification_id,
                            status=NotificationStatus.queued.value,
                        )
                        add_outbox_event(
                            session,
                            aggregate_id=notification_id,
                            event_type="notification.created",
                            payload=notification_payload(
                                event_id=uuid.uuid4(),
                                notification_id=notification_id,
                                batch_id=batch_id,
                                subscriber_id=recipient_id,
                                channel=request.channel.value,
                                notification_type=request.type.value,
                                attempt=1,
                                created_at=datetime.now(UTC),
                                routing_key=routing_key_for_notification_type(
                                    request.type.value
                                ),
                            ),
                        )
                        session.add_all([notification, history])

                    record.batch_id = batch_id
                    record.response_payload = response_payload
            except IntegrityError as exc:
                await session.rollback()
                existing = await self.idempotency.find(session, idempotency_key)
                if existing is None:
                    raise IdempotencyInProgressError from exc
                if existing.request_hash != request_hash:
                    raise IdempotencyConflictError from exc
                if existing.response_payload is None:
                    raise IdempotencyInProgressError from exc

                payload = dict(existing.response_payload)
                payload["status"] = "already_exists"
                return BulkNotificationResponse(**payload)

            return BulkNotificationResponse(**response_payload)
        finally:
            value = await redis.get(lock_key)
            if value == lock_token:
                await redis.delete(lock_key)

    async def list_for_subscriber(
        self,
        session: AsyncSession,
        subscriber_id: int,
        status: NotificationStatus | None = None,
        channel: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> NotificationListResponse:
        stmt: Select[tuple[Notification]] = (
            select(Notification)
            .options(selectinload(Notification.history))
            .where(Notification.subscriber_id == subscriber_id)
            .order_by(Notification.created_at.desc(), Notification.id.desc())
            .limit(limit)
            .offset(offset)
        )
        if status is not None:
            stmt = stmt.where(Notification.status == status.value)
        if channel is not None:
            stmt = stmt.where(Notification.channel == channel)

        result = await session.execute(stmt)
        notifications = result.scalars().unique().all()

        return NotificationListResponse(
            subscriber_id=subscriber_id,
            items=[self._to_item(notification) for notification in notifications],
            limit=limit,
            offset=offset,
        )

    async def get_notification(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
    ) -> NotificationItem | None:
        result = await session.execute(
            select(Notification)
            .options(selectinload(Notification.history))
            .where(Notification.id == notification_id)
        )
        notification = result.scalar_one_or_none()
        if notification is None:
            return None
        return self._to_item(notification)

    async def count_idempotency_records(self, session: AsyncSession) -> int:
        result = await session.execute(select(func.count(IdempotencyKey.key)))
        return int(result.scalar_one())

    def _to_item(self, notification: Notification) -> NotificationItem:
        return NotificationItem(
            notification_id=notification.id,
            batch_id=notification.batch_id,
            subscriber_id=notification.subscriber_id,
            channel=notification.channel,
            type=notification.type,
            message=notification.message,
            current_status=notification.status,
            attempts=notification.attempts,
            provider_message_id=notification.provider_message_id,
            last_error=notification.last_error,
            created_at=notification.created_at,
            updated_at=notification.updated_at,
            history=[
                StatusHistoryItem(
                    status=item.status,
                    error_message=item.error_message,
                    created_at=item.created_at,
                )
                for item in notification.history
            ],
        )
