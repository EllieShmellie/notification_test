from app.core.config import get_settings
from app.services.notification_service import NotificationService


def get_notification_service() -> NotificationService:
    return NotificationService(get_settings())
