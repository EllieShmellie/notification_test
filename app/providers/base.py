from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True)
class ProviderResult:
    success: bool
    delivered: bool
    provider_message_id: str | None = None
    error_type: Literal["temporary", "permanent"] | None = None
    error_message: str | None = None


class NotificationProvider(Protocol):
    async def send(
        self,
        recipient_id: int,
        message: str,
        idempotency_key: str,
    ) -> ProviderResult:
        ...

