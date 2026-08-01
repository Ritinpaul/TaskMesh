from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dag.engine import resolve_unblocked_child_tasks, validate_and_toposort_dag
from app.db.models import Task, TaskDependency, TaskStatus, WorkflowRun, WorkflowStatus


async def create_dag_workflow(
    session: AsyncSession,
    workflow_name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, str]],
) -> dict[str, Any]:
    node_keys = [n["key"] for n in nodes]
    edge_tuples = [(e["parent"], e["child"]) for e in edges]

    # Validate graph & check for cycles
    validate_and_toposort_dag(node_keys, edge_tuples)

    workflow = WorkflowRun(
        workflow_id=str(uuid.uuid4()),
        name=workflow_name,
        status=WorkflowStatus.PENDING,
    )
    session.add(workflow)
    await session.flush()

    key_to_task_id: dict[str, str] = {}
    created_tasks: list[Task] = []

    parent_keys = {e["parent"] for e in edges}
    child_keys = {e["child"] for e in edges}
    root_keys = set(node_keys) - child_keys

    for n in nodes:
        k = n["key"]
        t_id = str(uuid.uuid4())
        key_to_task_id[k] = t_id

        is_root = k in root_keys
        status = TaskStatus.QUEUED if is_root else TaskStatus.BLOCKED

        task = Task(
            task_id=t_id,
            idempotency_key=f"wf-{workflow.workflow_id}-{k}",
            task_type=n.get("task_type", "default"),
            payload=n.get("payload", {}),
            status=status,
            workflow_id=workflow.workflow_id,
            is_blocked=not is_root,
        )
        session.add(task)
        created_tasks.append(task)

    await session.flush()

    for e in edges:
        p_id = key_to_task_id[e["parent"]]
        c_id = key_to_task_id[e["child"]]
        dep = TaskDependency(
            workflow_id=workflow.workflow_id,
            parent_task_id=p_id,
            child_task_id=c_id,
            condition="SUCCESS",
        )
        session.add(dep)

    workflow.status = WorkflowStatus.RUNNING
    await session.commit()

    return {
        "workflow_id": workflow.workflow_id,
        "name": workflow.name,
        "status": workflow.status.value,
        "task_count": len(created_tasks),
        "root_tasks": [key_to_task_id[rk] for rk in root_keys],
    }


async def get_workflow_details(session: AsyncSession, workflow_id: str) -> dict[str, Any] | None:
    wf_res = await session.execute(select(WorkflowRun).where(WorkflowRun.workflow_id == workflow_id))
    workflow = wf_res.scalar_one_or_none()
    if not workflow:
        return None

    tasks_res = await session.execute(select(Task).where(Task.workflow_id == workflow_id))
    tasks = tasks_res.scalars().all()

    deps_res = await session.execute(select(TaskDependency).where(TaskDependency.workflow_id == workflow_id))
    deps = deps_res.scalars().all()

    completed_count = sum(1 for t in tasks if t.status == TaskStatus.SUCCEEDED)
    total_count = len(tasks)
    progress_pct = round((completed_count / total_count) * 100.0, 1) if total_count > 0 else 0.0

    return {
        "workflow_id": workflow.workflow_id,
        "name": workflow.name,
        "status": workflow.status.value,
        "progress_pct": progress_pct,
        "created_at": workflow.created_at.isoformat(),
        "tasks": [
            {
                "task_id": t.task_id,
                "idempotency_key": t.idempotency_key,
                "task_type": t.task_type,
                "status": t.status.value,
                "is_blocked": t.is_blocked,
            }
            for t in tasks
        ],
        "dependencies": [
            {
                "parent_task_id": d.parent_task_id,
                "child_task_id": d.child_task_id,
                "condition": d.condition,
            }
            for d in deps
        ],
    }
