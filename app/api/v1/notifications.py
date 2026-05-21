import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_notification_service
from app.core.redis import get_redis
from app.db.session import get_session
from app.schemas.notifications import (
    BulkNotificationRequest,
    BulkNotificationResponse,
    NotificationItem,
)
from app.services.idempotency_service import IdempotencyConflictError
from app.services.notification_service import IdempotencyInProgressError, NotificationService

router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.post("/bulk", response_model=BulkNotificationResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_bulk_notifications(
    payload: BulkNotificationRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
    service: Annotated[NotificationService, Depends(get_notification_service)],
) -> BulkNotificationResponse:
    try:
        return await service.create_bulk(session, redis, payload, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key already used with different request body",
        ) from exc
    except IdempotencyInProgressError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Request with this Idempotency-Key is already in progress",
        ) from exc


@router.get("/{notification_id}", response_model=NotificationItem)
async def get_notification(
    notification_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
    service: Annotated[NotificationService, Depends(get_notification_service)],
) -> NotificationItem:
    item = await service.get_notification(session, notification_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Notification not found")
    return item
