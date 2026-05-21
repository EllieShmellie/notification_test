from dataclasses import dataclass


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    delay_seconds: int | None


class RetryPolicy:
    def __init__(self, max_retries: int, backoff_seconds: list[int]) -> None:
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def decide(self, attempts: int) -> RetryDecision:
        if attempts >= self.max_retries:
            return RetryDecision(should_retry=False, delay_seconds=None)

        index = max(0, min(attempts - 1, len(self.backoff_seconds) - 1))
        return RetryDecision(should_retry=True, delay_seconds=self.backoff_seconds[index])

