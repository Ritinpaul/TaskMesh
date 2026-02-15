from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.api.dependencies import get_task_service
from app.schemas.tasks import (
    TaskReplayRequest,
    TaskReplayResponse,
    TaskAttemptResponse,
    TaskCreateRequest,
    TaskCreateResponse,
    TaskDetailResponse,
)
from app.services.task_service import TaskNotFoundError, TaskService


router = APIRouter()


@router.post("", response_model=TaskCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_task(
    request: TaskCreateRequest,
    response: Response,
    service: TaskService = Depends(get_task_service),
) -> TaskCreateResponse:
    task, reused = await service.submit_task(request)

    if reused:
        response.status_code = status.HTTP_200_OK

    return TaskCreateResponse(
        task_id=task.task_id,
        status=task.status,
        reused=reused,
        stream_id=task.stream_id,
    )


@router.get("/{task_id}", response_model=TaskDetailResponse)
async def get_task(task_id: str, service: TaskService = Depends(get_task_service)) -> TaskDetailResponse:
    try:
        task = await service.get_task(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=404, detail="task not found") from exc

    attempts = [
        TaskAttemptResponse(
            attempt_id=attempt.attempt_id,
            worker_id=attempt.worker_id,
            stream_id=attempt.stream_id,
            started_at=attempt.started_at,
            ended_at=attempt.ended_at,
            result_code=attempt.result_code,
            error_type=attempt.error_type,
            error_message=attempt.error_message,
        )
        for attempt in task.attempts
    ]

    return TaskDetailResponse(
        task_id=task.task_id,
        idempotency_key=task.idempotency_key,
        task_type=task.task_type,
        status=task.status,
        payload=task.payload,
        result_payload=task.result_payload,
        error_message=task.error_message,
        stream_id=task.stream_id,
        created_at=task.created_at,
        updated_at=task.updated_at,
        attempts=attempts,
    )


@router.post("/replay", response_model=TaskReplayResponse)
async def replay_tasks(
    request: TaskReplayRequest,
    service: TaskService = Depends(get_task_service),
) -> TaskReplayResponse:
    return await service.replay_tasks(request)
