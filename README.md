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
