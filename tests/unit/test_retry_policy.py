from app.services.retry_policy import RetryPolicy


def test_retry_policy_returns_backoff_until_limit() -> None:
    policy = RetryPolicy(max_retries=3, backoff_seconds=[10, 30, 60])

    assert policy.decide(attempts=1).should_retry is True
    assert policy.decide(attempts=1).delay_seconds == 10
    assert policy.decide(attempts=2).delay_seconds == 30
    assert policy.decide(attempts=3).should_retry is False

