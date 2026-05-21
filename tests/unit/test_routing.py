from app.services.notification_service import routing_key_for_notification_type


def test_routing_key_for_notification_type() -> None:
    assert routing_key_for_notification_type("transactional") == "notification.high"
    assert routing_key_for_notification_type("marketing") == "notification.low"

