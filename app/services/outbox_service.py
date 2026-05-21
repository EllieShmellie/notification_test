import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent


def add_outbox_event(
    session: AsyncSession,
    *,
    aggregate_id: uuid.UUID,
    event_type: str,
    payload: dict[str, Any],
    aggregate_type: str = "notification",
    available_at: datetime | None = None,
) -> OutboxEvent:
    event = OutboxEvent(
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload,
        available_at=available_at,
    )
    session.add(event)
    return event


def notification_payload(
    *,
    event_id: uuid.UUID,
    notification_id: uuid.UUID,
    batch_id: uuid.UUID,
    subscriber_id: int,
    channel: str,
    notification_type: str,
    attempt: int,
    created_at: datetime,
    routing_key: str,
    retry_delay_seconds: int | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": str(event_id),
        "notification_id": str(notification_id),
        "batch_id": str(batch_id),
        "subscriber_id": subscriber_id,
        "channel": channel,
        "type": notification_type,
        "attempt": attempt,
        "created_at": created_at.isoformat(),
        "routing_key": routing_key,
    }
    if retry_delay_seconds is not None:
        payload["retry_delay_seconds"] = retry_delay_seconds
    if error_message is not None:
        payload["error_message"] = error_message
    return payload
