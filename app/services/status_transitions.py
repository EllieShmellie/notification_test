from app.db.models import NotificationStatus

ALLOWED_TRANSITIONS: set[tuple[NotificationStatus, NotificationStatus]] = {
    (NotificationStatus.queued, NotificationStatus.sent),
    (NotificationStatus.sent, NotificationStatus.delivered),
    (NotificationStatus.queued, NotificationStatus.dropped),
    (NotificationStatus.sent, NotificationStatus.dropped),
    (NotificationStatus.delivered, NotificationStatus.delivered),
    (NotificationStatus.dropped, NotificationStatus.dropped),
}


def can_transition(current: str, target: NotificationStatus) -> bool:
    return (NotificationStatus(current), target) in ALLOWED_TRANSITIONS

