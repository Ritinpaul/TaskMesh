from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import DeadLetterQueue, ReplayAudit, Task, TaskStatus
from app.queue.producer import StreamProducer
from app.schemas.tasks import TaskCreateRequest, TaskReplayRequest, TaskReplayResponse


class TaskNotFoundError(Exception):
    pass


class TaskService:
    def __init__(self, db_session: AsyncSession, producer: StreamProducer) -> None:
        self.db_session = db_session
        self.producer = producer

    async def submit_task(self, request: TaskCreateRequest) -> tuple[Task, bool]:
        existing_task = await self._find_by_idempotency_key(request.idempotency_key)
        if existing_task is not None:
            return existing_task, True

        task = Task(
            idempotency_key=request.idempotency_key,
            task_type=request.task_type,
            payload=request.payload,
            status=TaskStatus.QUEUED,
        )
        self.db_session.add(task)
        await self.db_session.flush()

        message = {
            "task_id": task.task_id,
            "idempotency_key": task.idempotency_key,
            "task_type": task.task_type,
            "payload": StreamProducer.serialize_payload(task.payload),
        }
        task.stream_id = await self.producer.publish_task(message)

        await self.db_session.commit()
        await self.db_session.refresh(task)

        return task, False

    async def get_task(self, task_id: str) -> Task:
        statement = select(Task).where(Task.task_id == task_id).options(selectinload(Task.attempts))
        task = (await self.db_session.execute(statement)).scalar_one_or_none()

        if task is None:
            raise TaskNotFoundError(task_id)

        return task

    async def replay_tasks(self, request: TaskReplayRequest) -> TaskReplayResponse:
        statement = select(Task).where(
            Task.task_id.in_(request.task_ids),
            Task.status == TaskStatus.DEAD_LETTER,
        )
        tasks = (await self.db_session.execute(statement)).scalars().all()

        replayed_task_ids: list[str] = []
        now = datetime.now(timezone.utc)

        for task in tasks:
            message = {
                "task_id": task.task_id,
                "idempotency_key": task.idempotency_key,
                "task_type": task.task_type,
                "payload": StreamProducer.serialize_payload(task.payload),
            }
            task.stream_id = await self.producer.publish_task(message)
            task.status = TaskStatus.QUEUED
            task.error_message = None

            dlq_statement = (
                select(DeadLetterQueue)
                .where(DeadLetterQueue.task_id == task.task_id, DeadLetterQueue.replayed_at.is_(None))
                .order_by(DeadLetterQueue.dlq_id.desc())
            )
            dlq_entry = (await self.db_session.execute(dlq_statement)).scalars().first()
            if dlq_entry is not None:
                dlq_entry.replayed_at = now

            self.db_session.add(
                ReplayAudit(
                    task_id=task.task_id,
                    requested_by=request.requested_by,
                    requested_at=now,
                    replay_status="accepted",
                )
            )
            replayed_task_ids.append(task.task_id)

        await self.db_session.commit()

        return TaskReplayResponse(
            accepted_count=len(replayed_task_ids),
            replayed_task_ids=replayed_task_ids,
        )

    async def _find_by_idempotency_key(self, idempotency_key: str) -> Task | None:
        statement = select(Task).where(Task.idempotency_key == idempotency_key)
        return (await self.db_session.execute(statement)).scalar_one_or_none()
