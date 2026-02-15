from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import DeadLetterQueue, IdempotencyLedger, Task, TaskAttempt, TaskStatus
from app.reliability.circuit_breaker import CircuitBreaker
from app.reliability.exceptions import CircuitOpenError, NonRetryableTaskError
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
        self._breakers: dict[str, CircuitBreaker] = {}

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
        attempt_number = 1
        task_payload: dict[str, Any] = {}
        task_type = "default"
        idempotency_key = ""

        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task is None:
                await self._ack(message_id)
                return

            task_payload = task.payload
            task_type = task.task_type
            idempotency_key = task.idempotency_key

            existing_attempts = await session.scalar(
                select(func.count(TaskAttempt.attempt_id)).where(TaskAttempt.task_id == task.task_id)
            )
            attempt_number = (existing_attempts or 0) + 1

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

        breaker = self._get_breaker(task_type)
        if not breaker.allow_request():
            await self._handle_failure(
                task_id=task_id,
                idempotency_key=idempotency_key,
                task_type=task_type,
                task_payload=task_payload,
                stream_id=message_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                error=CircuitOpenError("circuit breaker is open"),
            )
            await self._ack(message_id)
            return

        handler = self.registry.get(task_type)

        try:
            result = await handler(task_payload)
        except Exception as exc:
            logger.exception("task execution failed task_id=%s", task_id)
            breaker.record_failure()
            await self._handle_failure(
                task_id=task_id,
                idempotency_key=idempotency_key,
                task_type=task_type,
                task_payload=task_payload,
                stream_id=message_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                error=exc,
            )
            await self._ack(message_id)
            return

        breaker.record_success()
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

    async def _handle_failure(
        self,
        task_id: str,
        idempotency_key: str,
        task_type: str,
        task_payload: dict[str, Any],
        stream_id: str,
        attempt_id: int | None,
        attempt_number: int,
        error: Exception,
    ) -> None:
        retry_count = max(0, attempt_number - 1)
        is_retryable = not isinstance(error, NonRetryableTaskError)
        can_retry = is_retryable and retry_count < self.settings.max_retry_attempts

        if can_retry:
            delay_ms = min(
                self.settings.retry_base_delay_ms * (2**retry_count),
                self.settings.retry_max_delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000)

            retry_stream_id = await self.redis_client.xadd(
                self.settings.task_stream_key,
                fields={
                    "task_id": task_id,
                    "idempotency_key": idempotency_key,
                    "task_type": task_type,
                    "payload": json.dumps(task_payload, separators=(",", ":"), sort_keys=True),
                },
            )

            async with self.session_factory() as session:
                task = await session.get(Task, task_id)
                if task is None:
                    return

                task.status = TaskStatus.QUEUED
                task.stream_id = retry_stream_id
                task.error_message = str(error)

                if attempt_id is not None:
                    attempt = await session.get(TaskAttempt, attempt_id)
                    if attempt is not None:
                        attempt.ended_at = utcnow()
                        attempt.result_code = "retry_scheduled"
                        attempt.error_type = error.__class__.__name__
                        attempt.error_message = str(error)

                await session.commit()
            return

        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task is None:
                return

            task.status = TaskStatus.DEAD_LETTER
            task.error_message = str(error)

            if attempt_id is not None:
                attempt = await session.get(TaskAttempt, attempt_id)
                if attempt is not None:
                    attempt.ended_at = utcnow()
                    attempt.result_code = "dead_letter"
                    attempt.error_type = error.__class__.__name__
                    attempt.error_message = str(error)

            dlq_entry = DeadLetterQueue(
                task_id=task.task_id,
                stream_id=stream_id,
                reason=str(error),
                failed_at=utcnow(),
            )
            session.add(dlq_entry)

            ledger = await session.get(IdempotencyLedger, task.idempotency_key)
            if ledger is None:
                ledger = IdempotencyLedger(
                    idempotency_key=task.idempotency_key,
                    task_id=task.task_id,
                    first_processed_at=utcnow(),
                    final_status=TaskStatus.DEAD_LETTER.value,
                    result_payload=None,
                    execution_hash=None,
                )
                session.add(ledger)
            else:
                ledger.final_status = TaskStatus.DEAD_LETTER.value

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

    def _get_breaker(self, task_type: str) -> CircuitBreaker:
        breaker = self._breakers.get(task_type)
        if breaker is None:
            breaker = CircuitBreaker(
                failure_threshold=self.settings.circuit_breaker_failure_threshold,
                recovery_timeout_ms=self.settings.circuit_breaker_recovery_timeout_ms,
            )
            self._breakers[task_type] = breaker
        return breaker
