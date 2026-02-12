from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.queue.producer import StreamProducer, get_stream_producer
from app.services.task_service import TaskService


async def get_task_service(
    db_session: AsyncSession = Depends(get_db_session),
    producer: StreamProducer = Depends(get_stream_producer),
) -> TaskService:
    return TaskService(db_session=db_session, producer=producer)
