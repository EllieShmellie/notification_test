import logging
import uuid
from datetime import datetime, timedelta
from math import ceil
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.models import (
    Notification,
    NotificationStatus,
    NotificationStatusHistory,
    OutboxEvent,
    OutboxStatus,
    utcnow,
)
from app.worker.outbox_events import NotificationOutboxEvents

logger = logging.getLogger(__name__)


class NotificationAttemptService:
    def __init__(
        self,
        settings: Settings,
        outbox_events: NotificationOutboxEvents | None = None,
    ) -> None:
        self.settings = settings
        self.outbox_events = outbox_events or NotificationOutboxEvents()

    async def reserve(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> Notification | None:
        async with session.begin():
            notification = await self._get_for_update(session, notification_id)
            if notification is None:
                logger.warning(
                    "notification not found",
                    extra={
                        "event": "notification_not_found",
                        "notification_id": str(notification_id),
                    },
                )
                return None

            if self._is_final(notification):
                logger.info(
                    "notification already finalized",
                    extra={
                        "event": "notification_already_finalized",
                        "notification_id": str(notification_id),
                    },
                )
                return None

            if self._has_active_processing_lock(notification):
                logger.info(
                    "notification is waiting for processing lock or retry backoff",
                    extra={
                        "event": "notification_deferred_redelivery_scheduled",
                        "notification_id": str(notification_id),
                    },
                )
                await self._ensure_deferred_redelivery(session, notification, payload)
                return None

            if notification.attempts >= self.settings.max_retries:
                self._drop_before_provider(session, notification)
                return None

            notification.attempts += 1
            notification.status = NotificationStatus.sent.value
            notification.processing_lock_until = utcnow() + timedelta(
                seconds=self.settings.processing_lock_ttl_seconds
            )
            session.add(
                NotificationStatusHistory(
                    notification_id=notification_id,
                    status=NotificationStatus.sent.value,
                )
            )
            return notification

    async def _get_for_update(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
    ) -> Notification | None:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id).with_for_update()
        )
        return result.scalar_one_or_none()

    def _is_final(self, notification: Notification) -> bool:
        return notification.status in {
            NotificationStatus.delivered.value,
            NotificationStatus.dropped.value,
        }

    def _has_active_processing_lock(self, notification: Notification) -> bool:
        return (
            notification.status == NotificationStatus.sent.value
            and notification.processing_lock_until is not None
            and self._is_future(notification.processing_lock_until)
        )

    def _drop_before_provider(
        self,
        session: AsyncSession,
        notification: Notification,
    ) -> None:
        notification.status = NotificationStatus.dropped.value
        notification.last_error = "Max retry attempts exceeded before provider call"
        notification.processing_lock_until = None
        session.add(
            NotificationStatusHistory(
                notification_id=notification.id,
                status=NotificationStatus.dropped.value,
                error_message=notification.last_error,
            )
        )
        self.outbox_events.add_failed(session, notification, None, notification.last_error)

    async def _ensure_deferred_redelivery(
        self,
        session: AsyncSession,
        notification: Notification,
        payload: dict[str, Any],
    ) -> None:
        existing = await session.execute(
            select(OutboxEvent.id).where(
                OutboxEvent.aggregate_id == notification.id,
                OutboxEvent.event_type == "notification.retry_scheduled",
                OutboxEvent.available_at >= notification.processing_lock_until,
                OutboxEvent.status.in_(
                    [
                        OutboxStatus.pending.value,
                        OutboxStatus.processing.value,
                    ]
                ),
            )
        )
        if existing.scalar_one_or_none() is not None:
            return

        self.outbox_events.add_retry(
            session,
            notification,
            payload,
            delay_seconds=self._seconds_until(notification.processing_lock_until),
            available_at=notification.processing_lock_until,
            deferred_reason="processing_lock_active",
        )

    def _is_future(self, value: datetime) -> bool:
        now = utcnow()
        if value.tzinfo is None:
            return value > now.replace(tzinfo=None)
        return value > now

    def _seconds_until(self, value: datetime | None) -> int:
        if value is None:
            return 1
        now = utcnow()
        if value.tzinfo is None:
            delta = value - now.replace(tzinfo=None)
        else:
            delta = value - now
        return max(1, ceil(delta.total_seconds()))
