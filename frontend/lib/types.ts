export type WorkflowStatus = "pending" | "running" | "completed" | "failed";
export type TaskStatus = "queued" | "blocked" | "processing" | "succeeded" | "failed" | "dead_letter";

export type DAGNode = {
  task_id: string;
  idempotency_key: string;
  task_type: string;
  status: TaskStatus;
  is_blocked: boolean;
};

export type DAGEdge = {
  parent_task_id: string;
  child_task_id: string;
  condition: string;
};

export type WorkflowDetails = {
  workflow_id: string;
  name: string;
  status: WorkflowStatus;
  progress_pct: number;
  created_at: string;
  tasks: DAGNode[];
  dependencies: DAGEdge[];
};

export type MetricsSummary = {
  status: string;
  throughput_ops_per_min: number;
  p95_latency_ms: number;
  success_ratio_pct: number;
  total_tasks_tracked: number;
  status_breakdown: Record<string, number>;
  dlq_poison_depth: number;
  idempotency_ledger_entries: number;
  total_worker_attempts: number;
};
