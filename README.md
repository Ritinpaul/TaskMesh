# TaskMesh

TaskMesh is a workflow orchestration service built with FastAPI, Redis Streams, and PostgreSQL. It accepts asynchronous jobs through an HTTP API, distributes work across worker consumers, and records execution state with durable idempotency controls.

## Key Features

- Task submission API with idempotency support
- Redis Streams queueing with consumer group coordination
- Worker runtime with pluggable handler registry
- PostgreSQL-backed task and attempt persistence
- Liveness and readiness health probes
- Docker Compose stack for local development and demos

## Tech Stack

- Python 3.11+
- FastAPI
- Redis Streams
- PostgreSQL
- SQLAlchemy + Alembic
- Uvicorn
- Pytest

## Architecture Overview

1. Clients submit tasks through the API with a unique idempotency key.
2. The API stores task metadata in PostgreSQL and publishes work to Redis Streams.
3. Worker processes consume stream events, execute handlers, and persist outcomes.
4. Task status and attempt history are queryable through the API.

## Quick Start (Docker)

Start PostgreSQL, Redis, API, and Worker:

```bash
docker compose up --build
```

Create a task:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"default","idempotency_key":"demo-1","payload":{"value":42}}'
```

Fetch task details:

```bash
curl http://localhost:8000/tasks/<task_id>
```

## Local Development

1. Install dependencies:

```bash
pip install -e ".[dev]"
```

2. Create environment file:

```bash
copy .env.example .env
```

3. Apply migrations:

```bash
alembic upgrade head
```

4. Start the API:

```bash
uvicorn app.main:app --reload
```

5. Start the worker:

```bash
python -m app.workers.main
```

6. Run tests:

```bash
pytest
```

## API Endpoints

- POST /tasks: submit a task (idempotent by idempotency_key)
- GET /tasks/{task_id}: retrieve task state and attempt history
- GET /health/live: liveness probe
- GET /health/ready: readiness probe (database and Redis)

## Configuration

Default environment values are provided in [.env.example](.env.example).

- DATABASE_URL: async PostgreSQL connection string
- SYNC_DATABASE_URL: sync PostgreSQL connection string for migrations/utilities
- REDIS_URL: Redis connection string
- TASK_STREAM_KEY: Redis stream key used for task events
- TASK_CONSUMER_GROUP: Redis consumer group name
- TASK_CONSUMER_NAME: worker identity
- WORKER_BLOCK_MS: stream read block timeout
- WORKER_BATCH_SIZE: max messages fetched per worker poll
- LOG_LEVEL: application log level
