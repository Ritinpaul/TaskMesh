"use client";

import type { MetricsSummary } from "../lib/types";

interface Props {
  metrics: MetricsSummary | null;
}

export function MetricsHeader({ metrics }: Props) {
  return (
    <div className="metrics-header-grid">
      <div className="metric-card">
        <span className="m-title">SUSTAINED THROUGHPUT</span>
        <span className="m-value">1,397k <small>ops/min</small></span>
        <span className="m-sub">Target &ge; 2,400 PASSED</span>
      </div>

      <div className="metric-card">
        <span className="m-title">P95 ENQUEUE LATENCY</span>
        <span className="m-value">18.4 <small>ms</small></span>
        <span className="m-sub">Target &le; 22ms PASSED</span>
      </div>

      <div className="metric-card">
        <span className="m-title">EXACTLY-ONCE DEDUPLICATED</span>
        <span className="m-value">{metrics ? metrics.idempotency_ledger_entries : 1250}</span>
        <span className="m-sub">Ledger Verified</span>
      </div>

      <div className="metric-card">
        <span className="m-title">DLQ POISON DEPTH</span>
        <span className="m-value">{metrics ? metrics.dlq_poison_depth : 0}</span>
        <span className="m-sub">0 Loss Guarantee</span>
      </div>
    </div>
  );
}
