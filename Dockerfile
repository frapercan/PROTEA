# ── Stage 1: build dependencies ──────────────────────────────────────────────
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.1.0

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-root --no-interaction --no-ansi

COPY protea/ ./protea/
RUN poetry install --without dev --no-interaction --no-ansi

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY protea/ ./protea/
COPY scripts/ ./scripts/
COPY alembic/ ./alembic/
COPY alembic.ini ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Default: API server
# Override CMD to run a worker:
#   docker run protea python scripts/worker.py --queue protea.jobs
CMD ["uvicorn", "protea.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
