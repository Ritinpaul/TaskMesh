from redis.asyncio import Redis
from redis.exceptions import ResponseError


async def ensure_consumer_group(redis_client: Redis, stream_key: str, group_name: str) -> None:
    try:
        await redis_client.xgroup_create(name=stream_key, groupname=group_name, id="0", mkstream=True)
    except ResponseError as exc:
        # Ignore idempotent create failures when the group already exists.
        if "BUSYGROUP" not in str(exc):
            raise
