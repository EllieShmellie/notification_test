from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from app.core.config import Settings


@dataclass
class RabbitMQTopology:
    settings: Settings
    connection: aio_pika.RobustConnection | None = None
    channel: aio_pika.RobustChannel | None = None

    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        await self.declare()

    async def declare(self) -> None:
        assert self.channel is not None

        exchange = await self.channel.declare_exchange(
            self.settings.rabbitmq_exchange,
            ExchangeType.DIRECT,
            durable=True,
        )
        dlx = await self.channel.declare_exchange(
            self.settings.rabbitmq_dlx,
            ExchangeType.DIRECT,
            durable=True,
        )

        high_queue = await self.channel.declare_queue(
            self.settings.rabbitmq_high_queue,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-delivery-limit": 5,
                "x-dead-letter-exchange": self.settings.rabbitmq_dlx,
                "x-dead-letter-routing-key": "notification.failed",
            },
        )
        low_queue = await self.channel.declare_queue(
            self.settings.rabbitmq_low_queue,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-delivery-limit": 5,
                "x-dead-letter-exchange": self.settings.rabbitmq_dlx,
                "x-dead-letter-routing-key": "notification.failed",
            },
        )
        dlq = await self.channel.declare_queue(
            self.settings.rabbitmq_dlq,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )

        await high_queue.bind(exchange, routing_key="notification.high")
        await low_queue.bind(exchange, routing_key="notification.low")
        await dlq.bind(dlx, routing_key="notification.failed")

    async def publish(
        self,
        payload: bytes,
        routing_key: str,
        exchange_name: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        assert self.channel is not None
        exchange = await self.channel.get_exchange(exchange_name or self.settings.rabbitmq_exchange)
        message = Message(
            payload,
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
            headers=headers,
        )
        await exchange.publish(message, routing_key=routing_key)

    async def get_queue(self, queue_name: str) -> aio_pika.Queue:
        assert self.channel is not None
        return await self.channel.get_queue(queue_name)

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
