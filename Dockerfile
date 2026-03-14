FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Poetry and dependencies first (layer cache)
RUN pip install --no-cache-dir poetry==2.1.0

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi

# Copy source
COPY protea/ ./protea/
COPY scripts/ ./scripts/
COPY alembic/ ./alembic/
COPY alembic.ini ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# Default: API server
# Override CMD to run a worker:
#   docker run protea python scripts/worker.py --queue protea.jobs
CMD ["uvicorn", "protea.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
