from app.core.config import Settings
from app.worker.main import Worker


def test_worker_queue_mode_selects_high_queue() -> None:
    worker = Worker()
    worker.settings = Settings(
        worker_queue_mode="high",
        rabbitmq_high_queue="notifications.high.test",
        rabbitmq_low_queue="notifications.low.test",
    )

    assert worker._queue_names() == ["notifications.high.test"]


def test_worker_queue_mode_selects_low_queue() -> None:
    worker = Worker()
    worker.settings = Settings(
        worker_queue_mode="low",
        rabbitmq_high_queue="notifications.high.test",
        rabbitmq_low_queue="notifications.low.test",
    )

    assert worker._queue_names() == ["notifications.low.test"]


def test_worker_queue_mode_defaults_to_high_then_low() -> None:
    worker = Worker()
    worker.settings = Settings(
        worker_queue_mode="both",
        rabbitmq_high_queue="notifications.high.test",
        rabbitmq_low_queue="notifications.low.test",
    )

    assert worker._queue_names() == [
        "notifications.high.test",
        "notifications.low.test",
    ]
