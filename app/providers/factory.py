from functools import lru_cache

from app.core.config import get_settings
from app.db.models import NotificationChannel
from app.providers.base import NotificationProvider
from app.providers.mock import EmailMockProvider, SmsMockProvider


def build_provider(channel: str) -> NotificationProvider:
    settings = get_settings()
    if channel == NotificationChannel.sms.value:
        return _build_provider(channel, settings.sms_provider_mode)
    if channel == NotificationChannel.email.value:
        return _build_provider(channel, settings.email_provider_mode)
    raise ValueError(f"Unsupported notification channel: {channel}")


@lru_cache
def _build_provider(channel: str, mode: str) -> NotificationProvider:
    if channel == NotificationChannel.sms.value:
        return SmsMockProvider(mode)
    if channel == NotificationChannel.email.value:
        return EmailMockProvider(mode)
    raise ValueError(f"Unsupported notification channel: {channel}")
