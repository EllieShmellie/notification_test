FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY . .

FROM base AS runtime
RUN pip install --upgrade pip \
    && pip install "."

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
