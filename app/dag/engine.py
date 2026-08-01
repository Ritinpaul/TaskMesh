from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Task, TaskDependency, TaskStatus, WorkflowRun, WorkflowStatus


class CycleDetectedError(Exception):
    """Raised when a submitted workflow graph contains a cyclic dependency loop."""
    pass


class DAGValidationError(Exception):
    """Raised when task definitions or dependency references are invalid."""
    pass


def validate_and_toposort_dag(
    nodes: list[str], edges: list[tuple[str, str]]
) -> list[str]:
    """
    Kahn's Topological Sort algorithm for DAG validation and ordering.
    nodes: list of task node identifiers.
    edges: list of (parent_id, child_id) directed dependency edges.
    Returns ordered node IDs or raises CycleDetectedError.
    """
    in_degree: dict[str, int] = {node: 0 for node in nodes}
    graph: dict[str, list[str]] = defaultdict(list)

    for parent, child in edges:
        if parent not in in_degree or child not in in_degree:
            raise DAGValidationError(f"Edge ({parent} -> {child}) references unknown node")
        graph[parent].append(child)
        in_degree[child] += 1

    queue = deque([node for node in nodes if in_degree[node] == 0])
    sorted_nodes: list[str] = []

    while queue:
        curr = queue.popleft()
        sorted_nodes.append(curr)

        for neighbor in graph[curr]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_nodes) != len(nodes):
        raise CycleDetectedError("Cyclic dependency loop detected in workflow graph")

    return sorted_nodes


async def resolve_unblocked_child_tasks(session: AsyncSession, workflow_id: str) -> list[Task]:
    """
    Evaluates blocked tasks in a workflow.
    A blocked child task becomes QUEUED & unblocked when all its parent tasks have SUCCEEDED.
    """
    blocked_tasks_res = await session.execute(
        select(Task).where(
            Task.workflow_id == workflow_id,
            Task.status == TaskStatus.BLOCKED,
        )
    )
    blocked_tasks = blocked_tasks_res.scalars().all()

    unblocked_tasks: list[Task] = []

    for task in blocked_tasks:
        # Find parent dependencies for this task
        deps_res = await session.execute(
            select(TaskDependency).where(
                TaskDependency.workflow_id == workflow_id,
                TaskDependency.child_task_id == task.task_id,
            )
        )
        dependencies = deps_res.scalars().all()

        if not dependencies:
            task.status = TaskStatus.QUEUED
            task.is_blocked = False
            unblocked_tasks.append(task)
            continue

        parent_ids = [d.parent_task_id for d in dependencies]
        parents_res = await session.execute(
            select(Task).where(Task.task_id.in_(parent_ids))
        )
        parents = parents_res.scalars().all()

        all_parents_succeeded = all(p.status == TaskStatus.SUCCEEDED for p in parents)
        any_parent_failed = any(p.status in (TaskStatus.FAILED, TaskStatus.DEAD_LETTER) for p in parents)

        if all_parents_succeeded:
            task.status = TaskStatus.QUEUED
            task.is_blocked = False
            unblocked_tasks.append(task)
        elif any_parent_failed:
            task.status = TaskStatus.FAILED
            task.error_message = "Cascading failure: Parent task failed"

    # Check overall workflow status
    all_tasks_res = await session.execute(select(Task).where(Task.workflow_id == workflow_id))
    all_tasks = all_tasks_res.scalars().all()

    wf_res = await session.execute(select(WorkflowRun).where(WorkflowRun.workflow_id == workflow_id))
    workflow = wf_res.scalar_one_or_none()

    if workflow:
        if all(t.status == TaskStatus.SUCCEEDED for t in all_tasks):
            workflow.status = WorkflowStatus.COMPLETED
        elif any(t.status in (TaskStatus.FAILED, TaskStatus.DEAD_LETTER) for t in all_tasks):
            workflow.status = WorkflowStatus.FAILED
        elif any(t.status == TaskStatus.PROCESSING for t in all_tasks):
            workflow.status = WorkflowStatus.RUNNING

    await session.commit()
    return unblocked_tasks
