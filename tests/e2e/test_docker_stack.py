import os
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_E2E") != "1",
    reason="Docker Compose e2e tests are opt-in. Set RUN_E2E=1.",
)


def test_docker_stack_delivers_sms_notification() -> None:
    api_url = os.getenv("API_URL", "http://localhost:8000")

    with httpx.Client(base_url=api_url, timeout=5.0) as client:
        _wait_for_health(client)

        subscriber_id = 910001
        response = client.post(
            "/api/v1/notifications/bulk",
            headers={"Idempotency-Key": f"e2e-{uuid.uuid4()}"},
            json={
                "channel": "sms",
                "type": "transactional",
                "message": "E2E confirmation code: 123456",
                "recipient_ids": [subscriber_id],
            },
        )
        assert response.status_code == 202

        deadline = time.monotonic() + 60
        last_payload = None
        while time.monotonic() < deadline:
            status_response = client.get(f"/api/v1/subscribers/{subscriber_id}/notifications")
            status_response.raise_for_status()
            last_payload = status_response.json()
            items = last_payload["items"]
            if items and items[0]["current_status"] == "delivered":
                expected_attempts = int(os.getenv("EXPECTED_SMS_ATTEMPTS", "1"))
                history_statuses = [item["status"] for item in items[0]["history"]]

                assert history_statuses[0] == "queued"
                assert history_statuses[-1] == "delivered"
                assert history_statuses.count("sent") == expected_attempts
                assert items[0]["attempts"] == expected_attempts
                assert items[0]["provider_message_id"]
                return
            time.sleep(1)

        raise AssertionError(f"Notification was not delivered in time: {last_payload}")


def test_docker_stack_reuses_idempotency_response() -> None:
    api_url = os.getenv("API_URL", "http://localhost:8000")
    idempotency_key = f"e2e-idem-{uuid.uuid4()}"
    payload = {
        "channel": "email",
        "type": "marketing",
        "message": "E2E marketing digest",
        "recipient_ids": [920001, 920002],
    }

    with httpx.Client(base_url=api_url, timeout=5.0) as client:
        _wait_for_health(client)

        first = client.post(
            "/api/v1/notifications/bulk",
            headers={"Idempotency-Key": idempotency_key},
            json=payload,
        )
        second = client.post(
            "/api/v1/notifications/bulk",
            headers={"Idempotency-Key": idempotency_key},
            json=payload,
        )

        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json()["batch_id"] == second.json()["batch_id"]
        assert second.json()["status"] == "already_exists"


def test_docker_stack_processes_marketing_and_transactional_smoke() -> None:
    api_url = os.getenv("API_URL", "http://localhost:8000")

    with httpx.Client(base_url=api_url, timeout=5.0) as client:
        _wait_for_health(client)
        low_first_id = 930001
        low_second_id = 930002
        high_id = 930003

        marketing = client.post(
            "/api/v1/notifications/bulk",
            headers={"Idempotency-Key": f"e2e-marketing-{uuid.uuid4()}"},
            json={
                "channel": "email",
                "type": "marketing",
                "message": "E2E marketing batch",
                "recipient_ids": [low_first_id, low_second_id],
            },
        )
        transactional = client.post(
            "/api/v1/notifications/bulk",
            headers={"Idempotency-Key": f"e2e-transactional-{uuid.uuid4()}"},
            json={
                "channel": "sms",
                "type": "transactional",
                "message": "E2E urgent code: 654321",
                "recipient_ids": [high_id],
            },
        )

        assert marketing.status_code == 202
        assert transactional.status_code == 202
        assert marketing.json()["batch_id"] != transactional.json()["batch_id"]

        _wait_for_status(client, high_id, "sent")
        _wait_for_status(client, low_second_id, "sent")


def _wait_for_health(client: httpx.Client) -> None:
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = client.get("/health")
            if response.status_code == 200 and response.json()["status"] == "ok":
                return
        except Exception as exc:
            last_error = exc
        time.sleep(1)

    raise AssertionError(f"API did not become healthy: {last_error}")


def _wait_for_status(client: httpx.Client, subscriber_id: int, status: str) -> dict:
    deadline = time.monotonic() + 60
    last_payload = None
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/subscribers/{subscriber_id}/notifications")
        response.raise_for_status()
        last_payload = response.json()
        items = last_payload["items"]
        if items and any(item["status"] == status for item in items[0]["history"]):
            return items[0]
        time.sleep(1)

    raise AssertionError(f"Subscriber {subscriber_id} did not reach {status}: {last_payload}")
