import json
from collections.abc import Mapping

from fastapi import Depends
from redis.asyncio import Redis

from app.core.config import get_settings
from app.queue.redis_client import get_redis_client


class StreamProducer:
    def __init__(self, redis_client: Redis, stream_key: str) -> None:
        self.redis_client = redis_client
        self.stream_key = stream_key

    async def publish_task(self, message: Mapping[str, str]) -> str:
        return await self.redis_client.xadd(self.stream_key, fields=dict(message))

    @staticmethod
    def serialize_payload(payload: dict) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


async def get_stream_producer(redis_client: Redis = Depends(get_redis_client)) -> StreamProducer:
    settings = get_settings()
    return StreamProducer(redis_client=redis_client, stream_key=settings.task_stream_key)
