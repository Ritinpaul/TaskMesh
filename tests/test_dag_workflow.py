from __future__ import annotations

import pytest
from app.dag.engine import CycleDetectedError, resolve_unblocked_child_tasks, validate_and_toposort_dag
from app.db.models import Task, TaskDependency, TaskStatus, WorkflowRun, WorkflowStatus
from app.services.workflow_service import create_dag_workflow, get_workflow_details


def test_topological_sort_and_cycle_detection() -> None:
    # 1. Valid linear DAG: A -> B -> C
    nodes = ["A", "B", "C"]
    edges = [("A", "B"), ("B", "C")]
    sorted_nodes = validate_and_toposort_dag(nodes, edges)
    assert sorted_nodes == ["A", "B", "C"]

    # 2. Parallel branching DAG: Root -> (Left, Right) -> Join
    nodes_branch = ["Root", "Left", "Right", "Join"]
    edges_branch = [
        ("Root", "Left"),
        ("Root", "Right"),
        ("Left", "Join"),
        ("Right", "Join"),
    ]
    sorted_branch = validate_and_toposort_dag(nodes_branch, edges_branch)
    assert sorted_branch[0] == "Root"
    assert sorted_branch[-1] == "Join"

    # 3. Cyclic graph: X -> Y -> Z -> X
    nodes_cycle = ["X", "Y", "Z"]
    edges_cycle = [("X", "Y"), ("Y", "Z"), ("Z", "X")]
    with pytest.raises(CycleDetectedError, match="Cyclic dependency loop"):
        validate_and_toposort_dag(nodes_cycle, edges_cycle)


@pytest.mark.asyncio
async def test_create_and_query_dag_workflow(db_session) -> None:
    nodes = [
        {"key": "fetch_data", "task_type": "fetch", "payload": {"url": "https://api.example.com"}},
        {"key": "process_a", "task_type": "transform", "payload": {"batch": 1}},
        {"key": "process_b", "task_type": "transform", "payload": {"batch": 2}},
        {"key": "merge_results", "task_type": "aggregate", "payload": {}},
    ]
    edges = [
        {"parent": "fetch_data", "child": "process_a"},
        {"parent": "fetch_data", "child": "process_b"},
        {"parent": "process_a", "child": "merge_results"},
        {"parent": "process_b", "child": "merge_results"},
    ]

    wf = await create_dag_workflow(db_session, "ETL Data Pipeline", nodes, edges)
    assert wf["status"] == "running"
    assert wf["task_count"] == 4

    details = await get_workflow_details(db_session, wf["workflow_id"])
    assert details is not None
    assert details["progress_pct"] == 0.0

    # Root task fetch_data should be QUEUED, others BLOCKED
    fetch_task = next(t for t in details["tasks"] if t["idempotency_key"].endswith("-fetch_data"))
    assert fetch_task["status"] == "queued"
    assert fetch_task["is_blocked"] is False

    merge_task = next(t for t in details["tasks"] if t["idempotency_key"].endswith("-merge_results"))
    assert merge_task["status"] == "blocked"
    assert merge_task["is_blocked"] is True


@pytest.mark.asyncio
async def test_dependency_resolution_flow(db_session) -> None:
    nodes = [
        {"key": "parent", "task_type": "step1"},
        {"key": "child", "task_type": "step2"},
    ]
    edges = [{"parent": "parent", "child": "child"}]

    wf = await create_dag_workflow(db_session, "Simple Pipeline", nodes, edges)
    details = await get_workflow_details(db_session, wf["workflow_id"])

    parent_task_id = next(t["task_id"] for t in details["tasks"] if t["idempotency_key"].endswith("-parent"))
    child_task_id = next(t["task_id"] for t in details["tasks"] if t["idempotency_key"].endswith("-child"))

    # Initially child is blocked
    child_obj = await db_session.get(Task, child_task_id)
    assert child_obj.status == TaskStatus.BLOCKED

    # Complete parent task
    parent_obj = await db_session.get(Task, parent_task_id)
    parent_obj.status = TaskStatus.SUCCEEDED
    await db_session.commit()

    # Trigger resolution
    unblocked = await resolve_unblocked_child_tasks(db_session, wf["workflow_id"])
    assert len(unblocked) == 1
    assert unblocked[0].task_id == child_task_id
    assert unblocked[0].status == TaskStatus.QUEUED
    assert unblocked[0].is_blocked is False


def test_api_submit_dag_and_cycle_error(client) -> None:
    # Valid submission via HTTP API
    valid_payload = {
        "name": "API Workflow Test",
        "nodes": [
            {"key": "step1", "task_type": "init"},
            {"key": "step2", "task_type": "process"},
        ],
        "edges": [{"parent": "step1", "child": "step2"}],
    }

    res = client.post("/workflows/dag", json=valid_payload)
    assert res.status_code == 201
    data = res.json()
    assert "workflow_id" in data

    # Cyclic submission via HTTP API
    cycle_payload = {
        "name": "Cycle Error Test",
        "nodes": [
            {"key": "taskA", "task_type": "step"},
            {"key": "taskB", "task_type": "step"},
        ],
        "edges": [
            {"parent": "taskA", "child": "taskB"},
            {"parent": "taskB", "child": "taskA"},
        ],
    }

    res_cycle = client.post("/workflows/dag", json=cycle_payload)
    assert res_cycle.status_code == 400
    assert "Cyclic dependency" in res_cycle.json()["detail"]
