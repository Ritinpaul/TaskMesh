from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import Task, TaskStatus
from app.queue.producer import StreamProducer
from app.schemas.tasks import TaskCreateRequest


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

    async def _find_by_idempotency_key(self, idempotency_key: str) -> Task | None:
        statement = select(Task).where(Task.idempotency_key == idempotency_key)
        return (await self.db_session.execute(statement)).scalar_one_or_none()
