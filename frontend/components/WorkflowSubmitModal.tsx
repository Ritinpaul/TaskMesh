"use client";

import { useState } from "react";

interface Props {
  apiBaseUrl: string;
  onWorkflowCreated: (wfId: string) => void;
}

export function WorkflowSubmitModal({ apiBaseUrl, onWorkflowCreated }: Props) {
  const [loading, setLoading] = useState(false);

  async function handleDemoSubmit() {
    setLoading(true);
    try {
      const payload = {
        name: "Enterprise ETL Data Pipeline",
        nodes: [
          { key: "fetch_sources", task_type: "fetch_api", payload: { endpoint: "/data/v1" } },
          { key: "transform_batch_1", task_type: "spark_job", payload: { batch: 101 } },
          { key: "transform_batch_2", task_type: "spark_job", payload: { batch: 102 } },
          { key: "aggregate_report", task_type: "dw_loader", payload: { target: "analytics_db" } },
        ],
        edges: [
          { parent: "fetch_sources", child: "transform_batch_1" },
          { parent: "fetch_sources", child: "transform_batch_2" },
          { parent: "transform_batch_1", child: "aggregate_report" },
          { parent: "transform_batch_2", child: "aggregate_report" },
        ],
      };

      const res = await fetch(`${apiBaseUrl}/workflows/dag`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        const data = await res.json();
        onWorkflowCreated(data.workflow_id);
      }
    } catch {
      // Stream fallback
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="submit-card">
      <div>
        <h3>Launch DAG Workflow</h3>
        <p>Trigger parallel fan-out topological DAG execution graph.</p>
      </div>
      <button className="launch-btn" onClick={handleDemoSubmit} disabled={loading}>
        {loading ? "Submitting..." : "+ Submit Demo 4-Node DAG"}
      </button>
    </div>
  );
}
