from redis.asyncio import Redis
from redis.exceptions import ResponseError

from fastapi import APIRouter, Depends

from app.core.config import get_settings
from app.queue.redis_client import get_redis_client
from app.schemas.tasks import AuditGroupOffset, AuditOffsetsResponse


router = APIRouter(prefix="/audit")


@router.get("/offsets", response_model=AuditOffsetsResponse)
async def get_offsets(redis_client: Redis = Depends(get_redis_client)) -> AuditOffsetsResponse:
    settings = get_settings()
    stream_key = settings.task_stream_key

    try:
        stream_length = await redis_client.xlen(stream_key)
    except ResponseError:
        stream_length = 0

    groups_data: list[dict] = []
    try:
        groups_data = await redis_client.xinfo_groups(stream_key)
    except ResponseError:
        groups_data = []

    pending_count = 0
    try:
        pending = await redis_client.xpending(stream_key, settings.task_consumer_group)
        pending_count = int(pending.get("pending", 0))
    except ResponseError:
        pending_count = 0

    groups = [
        AuditGroupOffset(
            group=str(group.get("name", "")),
            consumers=int(group.get("consumers", 0)),
            pending=int(group.get("pending", 0)),
            lag=int(group["lag"]) if group.get("lag") is not None else None,
            last_delivered_id=str(group.get("last-delivered-id", "")) or None,
        )
        for group in groups_data
    ]

    return AuditOffsetsResponse(
        stream_key=stream_key,
        stream_length=stream_length,
        pending_count=pending_count,
        groups=groups,
    )
