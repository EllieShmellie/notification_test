from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_notification_service
from app.db.models import NotificationChannel, NotificationStatus
from app.db.session import get_session
from app.schemas.notifications import NotificationListResponse
from app.services.notification_service import NotificationService

router = APIRouter(prefix="/subscribers", tags=["subscribers"])


@router.get("/{subscriber_id}/notifications", response_model=NotificationListResponse)
async def list_subscriber_notifications(
    subscriber_id: int,
    status: NotificationStatus | None = None,
    channel: NotificationChannel | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    session: Annotated[AsyncSession, Depends(get_session)] = None,
    service: Annotated[NotificationService, Depends(get_notification_service)] = None,
) -> NotificationListResponse:
    return await service.list_for_subscriber(
        session=session,
        subscriber_id=subscriber_id,
        status=status,
        channel=channel.value if channel else None,
        limit=limit,
        offset=offset,
    )
