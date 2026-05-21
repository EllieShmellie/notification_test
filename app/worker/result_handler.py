import logging
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.models import Notification, NotificationStatus, NotificationStatusHistory, utcnow
from app.providers.base import ProviderResult
from app.services.retry_policy import RetryPolicy
from app.worker.outbox_events import NotificationOutboxEvents

logger = logging.getLogger(__name__)


class ProviderResultHandler:
    def __init__(
        self,
        settings: Settings,
        outbox_events: NotificationOutboxEvents | None = None,
    ) -> None:
        self.retry_policy = RetryPolicy(settings.max_retries, settings.retry_backoff_list)
        self.outbox_events = outbox_events or NotificationOutboxEvents()

    async def apply(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
        payload: dict[str, Any],
        result: ProviderResult,
    ) -> None:
        async with session.begin():
            notification = await self._get_for_update(session, notification_id)
            if notification.status in {
                NotificationStatus.delivered.value,
                NotificationStatus.dropped.value,
            }:
                return

            if result.success and result.delivered:
                self._mark_delivered(session, notification, result)
                return

            notification.last_error = result.error_message
            notification.processing_lock_until = None

            if self._schedule_retry_if_needed(session, notification, payload, result):
                return

            self._drop(session, notification, payload, result.error_message)

    async def _get_for_update(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
    ) -> Notification:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id).with_for_update()
        )
        return result.scalar_one()

    def _mark_delivered(
        self,
        session: AsyncSession,
        notification: Notification,
        result: ProviderResult,
    ) -> None:
        notification.status = NotificationStatus.delivered.value
        notification.provider_message_id = result.provider_message_id
        notification.last_error = None
        notification.processing_lock_until = None
        session.add(
            NotificationStatusHistory(
                notification_id=notification.id,
                status=NotificationStatus.delivered.value,
            )
        )
        logger.info(
            "notification delivered",
            extra={
                "event": "notification_delivered",
                "notification_id": str(notification.id),
                "batch_id": str(notification.batch_id),
                "provider_message_id": result.provider_message_id,
            },
        )

    def _schedule_retry_if_needed(
        self,
        session: AsyncSession,
        notification: Notification,
        payload: dict[str, Any],
        result: ProviderResult,
    ) -> bool:
        if result.error_type != "temporary":
            return False

        decision = self.retry_policy.decide(notification.attempts)
        if not decision.should_retry or decision.delay_seconds is None:
            return False

        notification.processing_lock_until = utcnow() + timedelta(seconds=decision.delay_seconds)
        self.outbox_events.add_retry(
            session,
            notification,
            payload,
            delay_seconds=decision.delay_seconds,
            available_at=notification.processing_lock_until,
            error_message=result.error_message,
        )
        logger.info(
            "notification retry scheduled",
            extra={
                "event": "notification_retry_scheduled",
                "notification_id": str(notification.id),
                "attempt": notification.attempts,
            },
        )
        return True

    def _drop(
        self,
        session: AsyncSession,
        notification: Notification,
        payload: dict[str, Any],
        error_message: str | None,
    ) -> None:
        notification.status = NotificationStatus.dropped.value
        notification.processing_lock_until = None
        session.add(
            NotificationStatusHistory(
                notification_id=notification.id,
                status=NotificationStatus.dropped.value,
                error_message=error_message,
            )
        )
        logger.info(
            "notification dropped",
            extra={
                "event": "notification_dropped",
                "notification_id": str(notification.id),
                "batch_id": str(notification.batch_id),
            },
        )
        self.outbox_events.add_failed(session, notification, payload, error_message)
