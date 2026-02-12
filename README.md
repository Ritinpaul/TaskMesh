# TaskMesh

TaskMesh is a high-throughput workflow orchestration service built with FastAPI, Redis Streams, and PostgreSQL. It is designed to provide reliable async task processing with strong delivery guarantees and auditable execution history.

## Highlights

- Fast task ingestion API with idempotency-key support
- Redis Streams consumer-group queueing model
- Worker runtime with durable claim, process, and ack flow
- Exactly-once business execution semantics via idempotency ledger
- PostgreSQL-backed task, attempt, and audit persistence
- Local one-command environment using Docker Compose

## Tech Stack

- Python 3.11+
- FastAPI
- Redis Streams
- PostgreSQL
- SQLAlchemy + Alembic
- Pytest

## Architecture

1. API receives task requests and validates idempotency keys.
2. Task metadata is persisted in PostgreSQL.
3. Task events are published to a Redis Stream.
4. Workers consume via Redis consumer groups.
5. Worker execution is recorded in attempts and idempotency ledger tables.
6. Task state transitions are stored durably for replay and auditability.

## Project Structure

```text
TaskMesh/
  app/
    api/          # API routers and dependencies
    core/         # config and logging
    db/           # SQLAlchemy models and session
    queue/        # Redis client, streams, producer
    schemas/      # request/response models
    services/     # application service layer
    workers/      # worker engine and handlers
  alembic/        # DB migration environment and versions
  tests/          # API and worker tests
  docker-compose.yml
  pyproject.toml
```

## Quick Start

### 1. Run the full stack

```bash
docker compose up --build
```

### 2. Create a task

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"default","idempotency_key":"task-001","payload":{"value":42}}'
```

### 3. Check task status

```bash
curl http://localhost:8000/tasks/<task_id>
```

## Local Development

### Setup

```bash
pip install -e .[dev]
copy .env.example .env
alembic upgrade head
```

### Run API

```bash
uvicorn app.main:app --reload
```

### Run worker

```bash
python -m app.workers.main
```

### Run tests

```bash
pytest -q
```

## API Endpoints

- `POST /tasks` - submit a task
- `GET /tasks/{task_id}` - fetch task state and execution attempts
- `GET /health/live` - liveness probe
- `GET /health/ready` - readiness probe (DB + Redis)

## Reliability Notes

- Duplicate submissions with the same `idempotency_key` are deduplicated.
- Worker writes are persisted before final acknowledgement.
- Execution outcomes are stored in `idempotency_ledger` for safe reuse.

## License

MIT
