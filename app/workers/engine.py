from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import IdempotencyLedger, Task, TaskAttempt, TaskStatus
from app.workers.handlers import HandlerRegistry

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerEngine:
    def __init__(
        self,
        settings: Settings,
        redis_client: Redis,
        session_factory: async_sessionmaker[AsyncSession],
        registry: HandlerRegistry,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.session_factory = session_factory
        self.registry = registry
        self.consumer_name = f"{settings.task_consumer_name}-{os.getpid()}"

    async def run_forever(self) -> None:
        logger.info("worker started with consumer=%s", self.consumer_name)

        while True:
            try:
                messages = await self.redis_client.xreadgroup(
                    groupname=self.settings.task_consumer_group,
                    consumername=self.consumer_name,
                    streams={self.settings.task_stream_key: ">"},
                    count=self.settings.worker_batch_size,
                    block=self.settings.worker_block_ms,
                )
            except Exception:
                logger.exception("xreadgroup failed")
                continue

            if not messages:
                continue

            for _, stream_entries in messages:
                for message_id, fields in stream_entries:
                    await self.process_message(message_id=message_id, fields=fields)

    async def process_message(self, message_id: str, fields: dict[str, str]) -> None:
        task_id = fields.get("task_id")
        if not task_id:
            await self._ack(message_id)
            return

        attempt_id: int | None = None
        task_payload: dict[str, Any] = {}
        task_type = "default"

        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task is None:
                await self._ack(message_id)
                return

            task_payload = task.payload
            task_type = task.task_type

            attempt = TaskAttempt(
                task_id=task.task_id,
                worker_id=self.consumer_name,
                stream_id=message_id,
                started_at=utcnow(),
            )
            session.add(attempt)
            await session.flush()
            attempt_id = attempt.attempt_id

            ledger = await session.get(IdempotencyLedger, task.idempotency_key)
            if ledger is not None and ledger.final_status == TaskStatus.SUCCEEDED.value:
                task.status = TaskStatus.SUCCEEDED
                task.result_payload = ledger.result_payload
                task.error_message = None
                attempt.ended_at = utcnow()
                attempt.result_code = "idempotent_reuse"
                await session.commit()
                await self._ack(message_id)
                return

            if task.status == TaskStatus.SUCCEEDED:
                attempt.ended_at = utcnow()
                attempt.result_code = "already_succeeded"
                await session.commit()
                await self._ack(message_id)
                return

            task.status = TaskStatus.PROCESSING
            await session.commit()

        handler = self.registry.get(task_type)

        try:
            result = await handler(task_payload)
        except Exception as exc:
            logger.exception("task execution failed task_id=%s", task_id)
            await self._mark_failed(task_id=task_id, attempt_id=attempt_id, error=exc)
            await self._ack(message_id)
            return

        await self._mark_succeeded(task_id=task_id, attempt_id=attempt_id, result=result)
        await self._ack(message_id)

    async def _mark_succeeded(self, task_id: str, attempt_id: int | None, result: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task is None:
                return

            task.status = TaskStatus.SUCCEEDED
            task.result_payload = result
            task.error_message = None

            if attempt_id is not None:
                attempt = await session.get(TaskAttempt, attempt_id)
                if attempt is not None:
                    attempt.ended_at = utcnow()
                    attempt.result_code = "success"

            ledger = await session.get(IdempotencyLedger, task.idempotency_key)
            if ledger is None:
                ledger = IdempotencyLedger(
                    idempotency_key=task.idempotency_key,
                    task_id=task.task_id,
                    first_processed_at=utcnow(),
                    final_status=TaskStatus.SUCCEEDED.value,
                    result_payload=result,
                    execution_hash=self._hash_result(result),
                )
                session.add(ledger)
            else:
                ledger.task_id = task.task_id
                ledger.final_status = TaskStatus.SUCCEEDED.value
                ledger.result_payload = result
                ledger.execution_hash = self._hash_result(result)

            await session.commit()

    async def _mark_failed(self, task_id: str, attempt_id: int | None, error: Exception) -> None:
        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task is None:
                return

            task.status = TaskStatus.FAILED
            task.error_message = str(error)

            if attempt_id is not None:
                attempt = await session.get(TaskAttempt, attempt_id)
                if attempt is not None:
                    attempt.ended_at = utcnow()
                    attempt.result_code = "error"
                    attempt.error_type = error.__class__.__name__
                    attempt.error_message = str(error)

            ledger = await session.get(IdempotencyLedger, task.idempotency_key)
            if ledger is None:
                ledger = IdempotencyLedger(
                    idempotency_key=task.idempotency_key,
                    task_id=task.task_id,
                    first_processed_at=utcnow(),
                    final_status=TaskStatus.FAILED.value,
                    result_payload=None,
                    execution_hash=None,
                )
                session.add(ledger)
            else:
                ledger.final_status = TaskStatus.FAILED.value

            await session.commit()

    async def _ack(self, message_id: str) -> None:
        await self.redis_client.xack(
            self.settings.task_stream_key,
            self.settings.task_consumer_group,
            message_id,
        )

    @staticmethod
    def _hash_result(result: dict[str, Any]) -> str:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
