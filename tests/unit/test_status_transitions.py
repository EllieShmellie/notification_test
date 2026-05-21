from app.db.models import NotificationStatus
from app.services.status_transitions import can_transition


def test_allowed_status_transitions() -> None:
    assert can_transition("queued", NotificationStatus.sent)
    assert can_transition("sent", NotificationStatus.delivered)
    assert can_transition("sent", NotificationStatus.dropped)
    assert not can_transition("delivered", NotificationStatus.sent)

