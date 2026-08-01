"use client";

import type { WorkflowDetails } from "../lib/types";

interface Props {
  workflow: WorkflowDetails | null;
}

export function DAGVisualizer({ workflow }: Props) {
  if (!workflow) {
    return (
      <div className="dag-empty-state">
        <p>No active workflow selected. Submit a DAG workflow to view dependency execution topology.</p>
      </div>
    );
  }

  return (
    <div className="dag-visualizer-panel">
      <div className="dag-header">
        <div>
          <h3>{workflow.name}</h3>
          <span className="wf-id">ID: {workflow.workflow_id}</span>
        </div>
        <div className="wf-status-badge">
          <span className={`status-tag status-${workflow.status}`}>{workflow.status.toUpperCase()}</span>
          <span className="pct">{workflow.progress_pct}% Complete</span>
        </div>
      </div>

      <div className="progress-bar-bg">
        <div className="progress-bar-fill" style={{ width: `${workflow.progress_pct}%` }}></div>
      </div>

      <div className="dag-nodes-grid">
        {workflow.tasks.map((task) => {
          const keyName = task.idempotency_key.split("-").pop() || task.task_id.substring(0, 8);
          return (
            <div key={task.task_id} className={`node-card node-status-${task.status}`}>
              <div className="node-top">
                <span className="node-type">{task.task_type}</span>
                <span className={`node-pill pill-${task.status}`}>{task.status.toUpperCase()}</span>
              </div>
              <h4 className="node-title">{keyName}</h4>
              <div className="node-footer">
                <span>Blocked: {task.is_blocked ? "Yes" : "No"}</span>
              </div>
            </div>
          );
        })}
      </div>

      {workflow.dependencies.length > 0 && (
        <div className="edges-legend">
          <h4>Dependency Graph Edges ({workflow.dependencies.length})</h4>
          <div className="edges-list">
            {workflow.dependencies.map((dep, idx) => (
              <div key={idx} className="edge-item">
                <code>{dep.parent_task_id.substring(0, 8)}</code>
                <span className="arrow">&rarr;</span>
                <code>{dep.child_task_id.substring(0, 8)}</code>
                <span className="cond">({dep.condition})</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
