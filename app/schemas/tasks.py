from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.db.models import TaskStatus


class TaskCreateRequest(BaseModel):
    task_type: str = Field(min_length=1, max_length=100)
    idempotency_key: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any]


class TaskCreateResponse(BaseModel):
    task_id: str
    status: TaskStatus
    reused: bool
    stream_id: str | None


class TaskAttemptResponse(BaseModel):
    attempt_id: int
    worker_id: str
    stream_id: str
    started_at: datetime
    ended_at: datetime | None
    result_code: str | None
    error_type: str | None
    error_message: str | None


class TaskDetailResponse(BaseModel):
    task_id: str
    idempotency_key: str
    task_type: str
    status: TaskStatus
    payload: dict[str, Any]
    result_payload: dict[str, Any] | None
    error_message: str | None
    stream_id: str | None
    created_at: datetime
    updated_at: datetime
    attempts: list[TaskAttemptResponse]


class TaskReplayRequest(BaseModel):
    task_ids: list[str] = Field(min_length=1)
    requested_by: str = Field(default="system", min_length=1, max_length=120)


class TaskReplayResponse(BaseModel):
    accepted_count: int
    replayed_task_ids: list[str]


class AuditGroupOffset(BaseModel):
    group: str
    consumers: int
    pending: int
    lag: int | None = None
    last_delivered_id: str | None = None


class AuditOffsetsResponse(BaseModel):
    stream_key: str
    stream_length: int
    pending_count: int
    groups: list[AuditGroupOffset]
