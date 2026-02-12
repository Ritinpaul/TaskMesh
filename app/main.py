from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import close_db_engine
from app.queue.redis_client import close_redis_client, get_redis_client
from app.queue.stream import ensure_consumer_group


def create_app(enable_startup_tasks: bool = True) -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if enable_startup_tasks:
            redis_client = await get_redis_client()
            await ensure_consumer_group(
                redis_client=redis_client,
                stream_key=settings.task_stream_key,
                group_name=settings.task_consumer_group,
            )

        yield

        await close_redis_client()
        await close_db_engine()

    app = FastAPI(title="TaskMesh", version="0.1.0", lifespan=lifespan)
    app.include_router(api_router)
    return app


app = create_app()
