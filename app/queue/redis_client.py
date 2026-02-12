from redis.asyncio import Redis

from app.core.config import get_settings


_redis_client: Redis | None = None


async def get_redis_client() -> Redis:
    global _redis_client

    if _redis_client is None:
        settings = get_settings()
        _redis_client = Redis.from_url(settings.redis_url, decode_responses=True)

    return _redis_client


async def close_redis_client() -> None:
    global _redis_client

    if _redis_client is not None:
        close_method = getattr(_redis_client, "aclose", None)
        if close_method is not None:
            await close_method()
        else:
            await _redis_client.close()
        _redis_client = None
