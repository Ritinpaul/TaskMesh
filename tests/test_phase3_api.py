from sqlalchemy import select

from app.db.models import DeadLetterQueue, ReplayAudit, Task, TaskStatus
from app.queue.stream import ensure_consumer_group


async def _seed_dead_letter_task(session_factory):
    async with session_factory() as session:
        task = Task(
            idempotency_key="dead-letter-1",
            task_type="default",
            payload={"kind": "failed"},
            status=TaskStatus.DEAD_LETTER,
            error_message="poison",
            stream_id="1-0",
        )
        session.add(task)
        await session.flush()

        session.add(
            DeadLetterQueue(
                task_id=task.task_id,
                stream_id="1-0",
                reason="poison",
            )
        )
        await session.commit()
        await session.refresh(task)
        return task.task_id


def test_replay_endpoint_requeues_dead_letter_task(client, fake_producer, test_session_factory):
    import asyncio

    task_id = asyncio.run(_seed_dead_letter_task(test_session_factory))

    response = client.post(
        "/tasks/replay",
        json={"task_ids": [task_id], "requested_by": "qa-user"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted_count"] == 1
    assert payload["replayed_task_ids"] == [task_id]
    assert len(fake_producer.messages) == 1

    async def verify_state():
        async with test_session_factory() as session:
            refreshed_task = await session.get(Task, task_id)
            assert refreshed_task is not None
            assert refreshed_task.status == TaskStatus.QUEUED
            assert refreshed_task.error_message is None

            replay_audits = (
                (await session.execute(select(ReplayAudit).where(ReplayAudit.task_id == task_id)))
                .scalars()
                .all()
            )
            assert len(replay_audits) == 1
            assert replay_audits[0].requested_by == "qa-user"

            dlq_entries = (
                (await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.task_id == task_id)))
                .scalars()
                .all()
            )
            assert len(dlq_entries) == 1
            assert dlq_entries[0].replayed_at is not None

    asyncio.run(verify_state())


def test_audit_offsets_endpoint_returns_group_stats(client, fake_redis):
    import asyncio

    async def seed_stream():
        await ensure_consumer_group(
            redis_client=fake_redis,
            stream_key="taskmesh:tasks",
            group_name="taskmesh-workers",
        )
        await fake_redis.xadd("taskmesh:tasks", {"task_id": "seed-task", "payload": "{}"})

    asyncio.run(seed_stream())

    response = client.get("/audit/offsets")
    assert response.status_code == 200

    payload = response.json()
    assert payload["stream_key"] == "taskmesh:tasks"
    assert payload["stream_length"] >= 1
    assert isinstance(payload["pending_count"], int)
    assert isinstance(payload["groups"], list)
