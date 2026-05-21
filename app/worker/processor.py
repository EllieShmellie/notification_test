import logging
import uuid
from enum import StrEnum
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.providers.base import NotificationProvider, ProviderResult
from app.providers.factory import build_provider
from app.worker.attempts import NotificationAttemptService
from app.worker.result_handler import ProviderResultHandler

logger = logging.getLogger(__name__)


class ProcessResult(StrEnum):
    ack = "ack"


class NotificationProcessor:
    def __init__(
        self,
        settings: Settings,
        providers: dict[str, NotificationProvider] | None = None,
        attempt_service: NotificationAttemptService | None = None,
        result_handler: ProviderResultHandler | None = None,
    ) -> None:
        self.providers = providers or {}
        self.attempt_service = attempt_service or NotificationAttemptService(settings)
        self.result_handler = result_handler or ProviderResultHandler(settings)

    async def process(self, session: AsyncSession, payload: dict[str, Any]) -> ProcessResult:
        notification_id = uuid.UUID(payload["notification_id"])
        notification = await self.attempt_service.reserve(session, notification_id, payload)
        if notification is None:
            return ProcessResult.ack

        provider_result = await self._send_to_provider(notification)
        await self.result_handler.apply(session, notification_id, payload, provider_result)
        return ProcessResult.ack

    async def _send_to_provider(self, notification: Any) -> ProviderResult:
        provider = self.providers.get(notification.channel) or build_provider(notification.channel)
        try:
            return await provider.send(
                recipient_id=notification.subscriber_id,
                message=notification.message,
                idempotency_key=str(notification.id),
            )
        except Exception as exc:
            logger.exception(
                "provider call failed unexpectedly",
                extra={
                    "event": "notification_provider_exception",
                    "notification_id": str(notification.id),
                },
            )
            return ProviderResult(
                success=False,
                delivered=False,
                error_type="temporary",
                error_message=str(exc),
            )
