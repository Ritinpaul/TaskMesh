import json

import fakeredis.aioredis
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import DeadLetterQueue, Task, TaskAttempt, TaskStatus
from app.queue.stream import ensure_consumer_group
from app.reliability.exceptions import NonRetryableTaskError
from app.workers.engine import WorkerEngine
from app.workers.handlers import HandlerRegistry


async def _create_task(session_factory: async_sessionmaker[AsyncSession], key: str) -> tuple[str, dict[str, str]]:
    async with session_factory() as session:
        task = Task(
            idempotency_key=key,
            task_type="default",
            payload={"value": 1},
            status=TaskStatus.QUEUED,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)

    fields = {
        "task_id": task.task_id,
        "idempotency_key": key,
        "task_type": "default",
        "payload": json.dumps({"value": 1}),
    }
    return task.task_id, fields


@pytest.mark.asyncio
async def test_transient_failure_retries_then_succeeds(test_session_factory: async_sessionmaker[AsyncSession]):
    settings = Settings(
        database_url="sqlite+aiosqlite:///unused.db",
        sync_database_url="sqlite:///unused.db",
        redis_url="redis://localhost:6379/0",
        task_stream_key="test:taskmesh:retries",
        task_consumer_group="test:taskmesh:workers",
        task_consumer_name="test-worker",
        worker_block_ms=5,
        worker_batch_size=10,
        max_retry_attempts=2,
        retry_base_delay_ms=1,
        retry_max_delay_ms=5,
        circuit_breaker_failure_threshold=10,
        circuit_breaker_recovery_timeout_ms=1000,
        log_level="INFO",
    )

    task_id, fields = await _create_task(test_session_factory, "retry-task-1")

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await ensure_consumer_group(redis_client, settings.task_stream_key, settings.task_consumer_group)

    first_message_id = await redis_client.xadd(settings.task_stream_key, fields)
    calls = {"count": 0}

    async def flaky_handler(payload: dict) -> dict:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("temporary dependency failure")
        return {"ok": True, "payload": payload}

    registry = HandlerRegistry()
    registry.register("default", flaky_handler)

    engine = WorkerEngine(settings, redis_client, test_session_factory, registry)
    await engine.process_message(first_message_id, fields)

    stream_entries = await redis_client.xrange(settings.task_stream_key)
    assert len(stream_entries) >= 2
    retry_message_id, retry_fields = stream_entries[-1]

    await engine.process_message(retry_message_id, retry_fields)

    async with test_session_factory() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        assert task.status == TaskStatus.SUCCEEDED

        attempts = (
            (
                await session.execute(
                    select(TaskAttempt)
                    .where(TaskAttempt.task_id == task_id)
                    .order_by(TaskAttempt.attempt_id)
                )
            )
            .scalars()
            .all()
        )
        assert len(attempts) == 2
        assert attempts[0].result_code == "retry_scheduled"
        assert attempts[1].result_code == "success"

        dlq_entries = (
            (await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.task_id == task_id)))
            .scalars()
            .all()
        )
        assert dlq_entries == []

    await redis_client.aclose()


@pytest.mark.asyncio
async def test_non_retryable_failure_routes_to_dead_letter(test_session_factory: async_sessionmaker[AsyncSession]):
    settings = Settings(
        database_url="sqlite+aiosqlite:///unused.db",
        sync_database_url="sqlite:///unused.db",
        redis_url="redis://localhost:6379/0",
        task_stream_key="test:taskmesh:dlq",
        task_consumer_group="test:taskmesh:workers",
        task_consumer_name="test-worker",
        worker_block_ms=5,
        worker_batch_size=10,
        max_retry_attempts=3,
        retry_base_delay_ms=1,
        retry_max_delay_ms=5,
        circuit_breaker_failure_threshold=10,
        circuit_breaker_recovery_timeout_ms=1000,
        log_level="INFO",
    )

    task_id, fields = await _create_task(test_session_factory, "dlq-task-1")

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await ensure_consumer_group(redis_client, settings.task_stream_key, settings.task_consumer_group)
    message_id = await redis_client.xadd(settings.task_stream_key, fields)

    async def poison_handler(_: dict) -> dict:
        raise NonRetryableTaskError("invalid payload")

    registry = HandlerRegistry()
    registry.register("default", poison_handler)

    engine = WorkerEngine(settings, redis_client, test_session_factory, registry)
    await engine.process_message(message_id, fields)

    async with test_session_factory() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        assert task.status == TaskStatus.DEAD_LETTER

        attempts = (
            (
                await session.execute(
                    select(TaskAttempt)
                    .where(TaskAttempt.task_id == task_id)
                    .order_by(TaskAttempt.attempt_id)
                )
            )
            .scalars()
            .all()
        )
        assert len(attempts) == 1
        assert attempts[0].result_code == "dead_letter"

        dlq_entries = (
            (await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.task_id == task_id)))
            .scalars()
            .all()
        )
        assert len(dlq_entries) == 1
        assert dlq_entries[0].reason == "invalid payload"

    await redis_client.aclose()
