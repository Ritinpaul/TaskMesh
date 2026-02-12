from fastapi import APIRouter

from app.api.routes.health import router as health_router
from app.api.routes.tasks import router as tasks_router


api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(tasks_router, prefix="/tasks", tags=["tasks"])
