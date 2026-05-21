import asyncio
import json
import logging
from json import JSONDecodeError

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import AsyncSessionLocal
from app.messaging.rabbitmq import RabbitMQTopology
from app.worker.processor import NotificationProcessor

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.topology = RabbitMQTopology(self.settings)

    async def run_forever(self) -> None:
        await self.topology.connect()
        processor = NotificationProcessor(self.settings)
        queue_names = self._queue_names()
        queues = [await self.topology.get_queue(queue_name) for queue_name in queue_names]
        logger.info(
            "notification worker started",
            extra={
                "event": "notification_worker_started",
                "worker_queue_mode": self.settings.worker_queue_mode,
                "queues": queue_names,
            },
        )

        try:
            while True:
                message = None
                for queue in queues:
                    message = await queue.get(fail=False, timeout=1)
                    if message is not None:
                        break

                if message is None:
                    await asyncio.sleep(self.settings.worker_poll_interval_seconds)
                    continue

                try:
                    payload = json.loads(message.body.decode("utf-8"))
                    async with AsyncSessionLocal() as session:
                        await processor.process(session, payload)
                except JSONDecodeError:
                    logger.exception(
                        "invalid notification payload",
                        extra={"event": "notification_payload_invalid"},
                    )
                    await message.reject(requeue=False)
                except Exception:
                    logger.exception(
                        "notification processing failed, message will be requeued",
                        extra={"event": "notification_processing_failed"},
                    )
                    await message.reject(requeue=True)
                else:
                    await message.ack()
        finally:
            await self.topology.close()

    def _queue_names(self) -> list[str]:
        if self.settings.worker_queue_mode == "high":
            return [self.settings.rabbitmq_high_queue]
        if self.settings.worker_queue_mode == "low":
            return [self.settings.rabbitmq_low_queue]
        return [self.settings.rabbitmq_high_queue, self.settings.rabbitmq_low_queue]


async def main() -> None:
    configure_logging()
    await Worker().run_forever()


if __name__ == "__main__":
    asyncio.run(main())
