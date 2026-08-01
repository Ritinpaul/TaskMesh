from fastapi import APIRouter

from app.api.routes.audit import router as audit_router
from app.api.routes.health import router as health_router
from app.api.routes.metrics import router as metrics_router
from app.api.routes.tasks import router as tasks_router
from app.api.routes.workflows import router as workflows_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(audit_router, tags=["audit"])
api_router.include_router(tasks_router, prefix="/tasks", tags=["tasks"])
api_router.include_router(workflows_router, prefix="/workflows", tags=["workflows"])
api_router.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
