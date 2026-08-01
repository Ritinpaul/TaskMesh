from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.dag.engine import CycleDetectedError, DAGValidationError, resolve_unblocked_child_tasks
from app.services.workflow_service import create_dag_workflow, get_workflow_details

router = APIRouter()


class NodeSpec(BaseModel):
    key: str
    task_type: str = "default"
    payload: dict[str, Any] = Field(default_factory=dict)


class EdgeSpec(BaseModel):
    parent: str
    child: str


class CreateWorkflowRequest(BaseModel):
    name: str = Field(min_length=3, examples=["Data Processing Pipeline"])
    nodes: list[NodeSpec]
    edges: list[EdgeSpec] = Field(default_factory=list)


@router.post("/dag", status_code=status.HTTP_201_CREATED)
async def submit_dag_workflow(
    payload: CreateWorkflowRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    try:
        nodes_dict = [n.model_dump() for n in payload.nodes]
        edges_dict = [e.model_dump() for e in payload.edges]
        result = await create_dag_workflow(db, payload.name, nodes_dict, edges_dict)
        return result
    except CycleDetectedError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    except DAGValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )


@router.get("/{workflow_id}")
async def get_workflow_status(
    workflow_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    details = await get_workflow_details(db, workflow_id)
    if not details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow '{workflow_id}' not found",
        )
    return details


@router.post("/{workflow_id}/resolve")
async def resolve_workflow_dependencies(
    workflow_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    unblocked = await resolve_unblocked_child_tasks(db, workflow_id)
    return {
        "workflow_id": workflow_id,
        "unblocked_count": len(unblocked),
        "unblocked_tasks": [t.task_id for t in unblocked],
    }
