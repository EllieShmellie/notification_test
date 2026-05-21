# Notification Service

Микросервис массовой отправки SMS и Email уведомлений.

Сервис принимает bulk-запрос через HTTP API, сохраняет уведомления в PostgreSQL,
публикует задачи через RabbitMQ и обрабатывает их отдельными worker-процессами.
Внешние SMS/Email шлюзы заменены mock-провайдерами.

## Стек

- Python 3.12
- FastAPI, Pydantic v2
- SQLAlchemy 2 async, Alembic
- PostgreSQL 16
- RabbitMQ
- Redis
- Docker Compose
- pytest

## Что реализовано

- массовая отправка SMS и Email;
- статусы `queued`, `sent`, `delivered`, `dropped`;
- история статусов по подписчику;
- приоритет transactional-сообщений над marketing;
- transactional outbox для надежной публикации в RabbitMQ;
- retry при временных ошибках провайдера;
- идемпотентность HTTP-запросов через `Idempotency-Key`;
- защита от повторной обработки сообщений на уровне бизнес-логики;
- интеграционные и e2e-тесты.

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

Миграции применяются автоматически отдельным `migrate` service.

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

Доступные фильтры:

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

## Архитектура коротко

```text
Client
  -> FastAPI API
  -> PostgreSQL: batch, notifications, status history, outbox
  -> Outbox Publisher
  -> RabbitMQ: high / low / dlq
  -> Worker High / Worker Low
  -> SMS / Email Mock Provider
  -> PostgreSQL: sent / delivered / dropped
```

Transactional-сообщения публикуются в `notifications.high`, marketing-сообщения
в `notifications.low`. В Docker Compose запущены отдельные `worker-high` и
`worker-low`, поэтому критичные сообщения не ждут завершения медленных
marketing-отправок.

## Настройки mock-провайдеров

```env
SMS_PROVIDER_MODE=success
EMAIL_PROVIDER_MODE=success
```

Доступные режимы:

- `success`
- `slow_success`
- `temporary_error_once`
- `temporary_error_always`
- `permanent_error`
- `random`

## Тесты

Локально:

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

E2E-сценарий поднимает PostgreSQL, RabbitMQ, Redis, API, workers и
outbox-publisher, затем проверяет доставку уведомления, retry после временной
ошибки, идемпотентный повтор HTTP-запроса и прием transactional-сообщения рядом
с marketing batch.
