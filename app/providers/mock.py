import asyncio
import random
import uuid

from app.providers.base import ProviderResult


class ConfigurableMockProvider:
    def __init__(self, prefix: str, mode: str = "success") -> None:
        self.prefix = prefix
        self.mode = mode
        self.calls: list[str] = []
        self._temporary_failures: set[str] = set()

    async def send(
        self,
        recipient_id: int,
        message: str,
        idempotency_key: str,
    ) -> ProviderResult:
        self.calls.append(idempotency_key)

        if self.mode == "success":
            return self._success()

        if self.mode == "slow_success":
            await asyncio.sleep(1)
            return self._success()

        if self.mode == "temporary_error_once":
            if idempotency_key not in self._temporary_failures:
                self._temporary_failures.add(idempotency_key)
                return ProviderResult(
                    success=False,
                    delivered=False,
                    error_type="temporary",
                    error_message=f"{self.prefix} temporary error",
                )
            return self._success()

        if self.mode == "temporary_error_always":
            return ProviderResult(
                success=False,
                delivered=False,
                error_type="temporary",
                error_message=f"{self.prefix} temporary error",
            )

        if self.mode == "permanent_error":
            return ProviderResult(
                success=False,
                delivered=False,
                error_type="permanent",
                error_message=f"{self.prefix} permanent error",
            )

        if self.mode == "random" and random.random() < 0.2:
            return ProviderResult(
                success=False,
                delivered=False,
                error_type="temporary",
                error_message=f"{self.prefix} random temporary error",
            )

        return self._success()

    def _success(self) -> ProviderResult:
        return ProviderResult(
            success=True,
            delivered=True,
            provider_message_id=f"{self.prefix}-mock-{uuid.uuid4()}",
        )


class SmsMockProvider(ConfigurableMockProvider):
    def __init__(self, mode: str = "success") -> None:
        super().__init__("sms", mode)


class EmailMockProvider(ConfigurableMockProvider):
    def __init__(self, mode: str = "success") -> None:
        super().__init__("email", mode)
