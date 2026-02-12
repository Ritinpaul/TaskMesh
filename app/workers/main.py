import asyncio

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import SessionLocal, close_db_engine
from app.queue.redis_client import close_redis_client, get_redis_client
from app.queue.stream import ensure_consumer_group
from app.workers.engine import WorkerEngine
from app.workers.handlers import build_default_registry


async def run_worker() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    redis_client = await get_redis_client()
    await ensure_consumer_group(
        redis_client=redis_client,
        stream_key=settings.task_stream_key,
        group_name=settings.task_consumer_group,
    )

    engine = WorkerEngine(
        settings=settings,
        redis_client=redis_client,
        session_factory=SessionLocal,
        registry=build_default_registry(),
    )
    await engine.run_forever()


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(close_redis_client())
        asyncio.run(close_db_engine())


if __name__ == "__main__":
    main()
