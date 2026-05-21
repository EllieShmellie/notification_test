import json
from typing import Any

from app.core.config import Settings
from app.messaging.rabbitmq import RabbitMQTopology


class NotificationPublisher:
    def __init__(self, topology: RabbitMQTopology, settings: Settings) -> None:
        self.topology = topology
        self.settings = settings

    async def publish_notification(self, payload: dict[str, Any]) -> None:
        routing_key = payload["routing_key"]
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        await self.topology.publish(body, routing_key=routing_key)

    async def publish_outbox_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if event_type == "notification.created":
            await self.publish_notification(payload)
            return
        if event_type == "notification.retry_scheduled":
            await self.publish_notification(payload)
            return
        if event_type == "notification.failed":
            await self.publish_failed(payload)
            return
        raise ValueError(f"Unsupported outbox event type: {event_type}")

    async def publish_failed(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        await self.topology.publish(
            body,
            routing_key="notification.failed",
            exchange_name=self.settings.rabbitmq_dlx,
        )
