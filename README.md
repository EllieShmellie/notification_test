# Notification Service

Микросервис массовой отправки SMS и Email уведомлений.

## Стек

- Python 3.12
- FastAPI
- PostgreSQL
- RabbitMQ
- Redis
- Docker Compose
- pytest

## Возможности

- массовая отправка SMS и Email;
- приоритет transactional-сообщений над marketing;
- статусы `queued`, `sent`, `delivered`, `dropped`;
- история уведомлений по подписчику;
- retry при временных ошибках провайдера;
- идемпотентность через `Idempotency-Key`;
- mock-провайдеры для SMS и Email.

## Запуск

```bash
docker-compose up --build
```

После запуска:

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- OpenAPI JSON: http://localhost:8000/openapi.json
- RabbitMQ UI: http://localhost:15672
- RabbitMQ login/password: `guest` / `guest`

## API

### Создать массовую рассылку

```bash
curl -X POST http://localhost:8000/api/v1/notifications/bulk \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{
    "channel": "sms",
    "type": "transactional",
    "message": "Ваш код подтверждения: 123456",
    "recipient_ids": [1001, 1002, 1003]
  }'
```

Пример ответа:

```json
{
  "batch_id": "1a7b9e52-7036-4bb4-90f3-18aa3f0f9aa1",
  "status": "accepted",
  "notifications_created": 3
}
```

Повторный запрос с тем же `Idempotency-Key` и тем же body вернет тот же
`batch_id`. Повторный запрос с тем же ключом, но другим body вернет
`409 Conflict`.

### Получить уведомления подписчика

```bash
curl "http://localhost:8000/api/v1/subscribers/1001/notifications?limit=50&offset=0"
```

Фильтры:

- `status=queued|sent|delivered|dropped`
- `channel=sms|email`
- `limit`
- `offset`

### Получить одно уведомление

```bash
curl http://localhost:8000/api/v1/notifications/{notification_id}
```

### Healthcheck

```bash
curl http://localhost:8000/health
```

## Тесты

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

E2E через Docker Compose:

```bash
docker compose -f docker-compose.yml -f docker-compose.test.yml up \
  --build \
  --abort-on-container-exit \
  --exit-code-from test-runner
```
