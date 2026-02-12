from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings


settings = get_settings()
engine = create_async_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


def build_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    test_engine = create_async_engine(database_url, pool_pre_ping=True)
    return async_sessionmaker(test_engine, expire_on_commit=False)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def close_db_engine() -> None:
    await engine.dispose()
