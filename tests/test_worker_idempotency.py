import json

import fakeredis.aioredis
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import IdempotencyLedger, Task, TaskAttempt, TaskStatus
from app.queue.stream import ensure_consumer_group
from app.workers.engine import WorkerEngine
from app.workers.handlers import HandlerRegistry


@pytest.mark.asyncio
async def test_worker_executes_business_logic_once_on_duplicate_stream_messages(
    test_session_factory: async_sessionmaker[AsyncSession],
):
    settings = Settings(
        database_url="sqlite+aiosqlite:///unused.db",
        sync_database_url="sqlite:///unused.db",
        redis_url="redis://localhost:6379/0",
        task_stream_key="test:taskmesh:stream",
        task_consumer_group="test:taskmesh:workers",
        task_consumer_name="test-worker",
        worker_block_ms=5,
        worker_batch_size=10,
        log_level="INFO",
    )

    async with test_session_factory() as session:
        task = Task(
            idempotency_key="dup-worker-1",
            task_type="default",
            payload={"value": 1},
            status=TaskStatus.QUEUED,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)
        task_id = task.task_id

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await ensure_consumer_group(
        redis_client=redis_client,
        stream_key=settings.task_stream_key,
        group_name=settings.task_consumer_group,
    )

    message_fields = {
        "task_id": task_id,
        "idempotency_key": "dup-worker-1",
        "task_type": "default",
        "payload": json.dumps({"value": 1}),
    }
    first_message_id = await redis_client.xadd(settings.task_stream_key, message_fields)
    second_message_id = await redis_client.xadd(settings.task_stream_key, message_fields)

    call_counter = {"count": 0}

    async def counting_handler(payload: dict) -> dict:
        call_counter["count"] += 1
        return {"ok": True, "payload": payload, "count": call_counter["count"]}

    registry = HandlerRegistry()
    registry.register("default", counting_handler)

    worker_engine = WorkerEngine(
        settings=settings,
        redis_client=redis_client,
        session_factory=test_session_factory,
        registry=registry,
    )

    await worker_engine.process_message(first_message_id, message_fields)
    await worker_engine.process_message(second_message_id, message_fields)

    assert call_counter["count"] == 1

    async with test_session_factory() as session:
        refreshed_task = await session.get(Task, task_id)
        assert refreshed_task is not None
        assert refreshed_task.status == TaskStatus.SUCCEEDED

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
        assert attempts[0].result_code == "success"
        assert attempts[1].result_code in {"idempotent_reuse", "already_succeeded"}

        ledger = await session.get(IdempotencyLedger, "dup-worker-1")
        assert ledger is not None
        assert ledger.final_status == TaskStatus.SUCCEEDED.value

    await redis_client.aclose()
