from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db.base import Base
from app.db.session import get_db_session
from app.main import create_app
from app.queue.producer import get_stream_producer


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, str]] = []

    async def publish_task(self, message: dict[str, str]) -> str:
        self.messages.append(message)
        return f"{len(self.messages)}-0"


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest_asyncio.fixture
async def test_session_factory(tmp_path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'taskmesh_test.db'}"
    engine = create_async_engine(database_url, future=True)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    yield session_factory

    await engine.dispose()


@pytest.fixture
def client(
    test_session_factory: async_sessionmaker[AsyncSession],
    fake_producer: FakeProducer,
) -> Iterator[TestClient]:
    app = create_app(enable_startup_tasks=False)

    async def override_db() -> AsyncIterator[AsyncSession]:
        async with test_session_factory() as session:
            yield session

    async def override_producer() -> FakeProducer:
        return fake_producer

    app.dependency_overrides[get_db_session] = override_db
    app.dependency_overrides[get_stream_producer] = override_producer

    with TestClient(app) as test_client:
        yield test_client
