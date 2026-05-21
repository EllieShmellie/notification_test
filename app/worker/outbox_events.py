import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Notification
from app.services.notification_service import routing_key_for_notification_type
from app.services.outbox_service import add_outbox_event, notification_payload


class NotificationOutboxEvents:
    def add_failed(
        self,
        session: AsyncSession,
        notification: Notification,
        source_payload: Mapping[str, Any] | None,
        error_message: str | None,
    ) -> None:
        add_outbox_event(
            session,
            aggregate_id=notification.id,
            event_type="notification.failed",
            payload=notification_payload(
                event_id=uuid.uuid4(),
                notification_id=notification.id,
                batch_id=notification.batch_id,
                subscriber_id=notification.subscriber_id,
                channel=notification.channel,
                notification_type=notification.type,
                attempt=notification.attempts,
                created_at=datetime.now(UTC),
                routing_key=self._routing_key(notification, source_payload),
                error_message=error_message,
            ),
        )

    def add_retry(
        self,
        session: AsyncSession,
        notification: Notification,
        source_payload: Mapping[str, Any],
        *,
        delay_seconds: int,
        available_at: datetime,
        error_message: str | None = None,
        deferred_reason: str | None = None,
    ) -> None:
        retry_payload = dict(source_payload)
        retry_payload["attempt"] = notification.attempts + 1
        retry_payload["routing_key"] = routing_key_for_notification_type(notification.type)
        retry_payload["retry_delay_seconds"] = delay_seconds
        if error_message is not None:
            retry_payload["error_message"] = error_message
        if deferred_reason is not None:
            retry_payload["deferred_reason"] = deferred_reason

        add_outbox_event(
            session,
            aggregate_id=notification.id,
            event_type="notification.retry_scheduled",
            payload=retry_payload,
            available_at=available_at,
        )

    def _routing_key(
        self,
        notification: Notification,
        source_payload: Mapping[str, Any] | None,
    ) -> str:
        if source_payload is not None and source_payload.get("routing_key"):
            return str(source_payload["routing_key"])
        return routing_key_for_notification_type(notification.type)
