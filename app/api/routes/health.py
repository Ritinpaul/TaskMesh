from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.queue.redis_client import get_redis_client


router = APIRouter(prefix="/health")


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(db_session: AsyncSession = Depends(get_db_session)) -> dict[str, str]:
    try:
        await db_session.execute(text("SELECT 1"))
        redis_client = await get_redis_client()
        await redis_client.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"dependency check failed: {exc}") from exc

    return {"status": "ready"}
