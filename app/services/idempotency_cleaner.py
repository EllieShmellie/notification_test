import asyncio
import logging

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import AsyncSessionLocal
from app.services.idempotency_service import IdempotencyService

logger = logging.getLogger(__name__)


class IdempotencyCleaner:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.service = IdempotencyService(self.settings.idempotency_ttl_seconds)

    async def run_forever(self) -> None:
        while True:
            deleted = await self.cleanup_once()
            logger.info(
                "expired idempotency keys cleanup finished",
                extra={"event": "idempotency_cleanup_finished", "deleted": deleted},
            )
            await asyncio.sleep(self.settings.idempotency_cleanup_interval_seconds)

    async def cleanup_once(self) -> int:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                return await self.service.cleanup_expired(session)


async def main() -> None:
    configure_logging()
    await IdempotencyCleaner().run_forever()


if __name__ == "__main__":
    asyncio.run(main())
