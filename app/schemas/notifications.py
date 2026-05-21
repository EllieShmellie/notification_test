import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.db.models import NotificationChannel, NotificationStatus, NotificationType


class BulkNotificationRequest(BaseModel):
    channel: NotificationChannel
    type: NotificationType
    message: str = Field(min_length=1, max_length=4000)
    recipient_ids: list[int] = Field(min_length=1, max_length=10_000)


class BulkNotificationResponse(BaseModel):
    batch_id: uuid.UUID
    status: str
    notifications_created: int


class StatusHistoryItem(BaseModel):
    status: NotificationStatus
    error_message: str | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class NotificationItem(BaseModel):
    notification_id: uuid.UUID
    batch_id: uuid.UUID
    subscriber_id: int
    channel: NotificationChannel
    type: NotificationType
    message: str
    current_status: NotificationStatus
    attempts: int
    provider_message_id: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    history: list[StatusHistoryItem]


class NotificationListResponse(BaseModel):
    subscriber_id: int
    items: list[NotificationItem]
    limit: int
    offset: int


class HealthResponse(BaseModel):
    status: str
    postgres: str
    rabbitmq: str
    redis: str

