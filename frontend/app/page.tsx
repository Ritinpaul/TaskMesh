"use client";

import { useEffect, useState } from "react";
import { DAGVisualizer } from "../components/DAGVisualizer";
import { MetricsHeader } from "../components/MetricsHeader";
import { WorkflowSubmitModal } from "../components/WorkflowSubmitModal";
import type { MetricsSummary, WorkflowDetails } from "../lib/types";

export default function HomePage() {
  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

  const [metrics, setMetrics] = useState<MetricsSummary | null>(null);
  const [activeWorkflow, setActiveWorkflow] = useState<WorkflowDetails | null>(null);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string | null>(null);

  async function fetchMetrics() {
    try {
      const res = await fetch(`${apiBaseUrl}/metrics/summary`, { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        setMetrics(data);
      }
    } catch {
      // Stream fallback
    }
  }

  async function fetchWorkflow(wfId: string) {
    try {
      const res = await fetch(`${apiBaseUrl}/workflows/${wfId}`, { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        setActiveWorkflow(data);
      }
    } catch {
      // Fallback
    }
  }

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 10000);
    return () => clearInterval(interval);
  }, [apiBaseUrl]);

  useEffect(() => {
    if (activeWorkflowId) {
      fetchWorkflow(activeWorkflowId);
      const interval = setInterval(() => fetchWorkflow(activeWorkflowId), 3000);
      return () => clearInterval(interval);
    }
  }, [activeWorkflowId, apiBaseUrl]);

  return (
    <main className="page-shell">
      <header className="hero">
        <div className="hero-branding">
          <div className="hero-badge">DISTRIBUTED TASK ORCHESTRATOR</div>
          <h1>TaskMesh: High-Throughput DAG Workflow Engine</h1>
          <p>
            Built from first principles with Redis Streams consumer groups, PostgreSQL idempotency ledger deduplication,
            and topological cycle detection for parallel DAG tasks.
          </p>
        </div>
      </header>

      <MetricsHeader metrics={metrics} />

      <div className="dashboard-content">
        <div className="action-bar">
          <WorkflowSubmitModal
            apiBaseUrl={apiBaseUrl}
            onWorkflowCreated={(wfId) => {
              setActiveWorkflowId(wfId);
              fetchWorkflow(wfId);
            }}
          />
        </div>

        <section className="dag-section">
          <DAGVisualizer workflow={activeWorkflow} />
        </section>
      </div>
    </main>
  );
}
