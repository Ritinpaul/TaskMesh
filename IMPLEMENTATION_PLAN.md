# TaskMesh Implementation Plan

Project: TaskMesh - Autonomous Workflow Orchestration Engine
Stack: Python, FastAPI, Redis Streams, PostgreSQL, AWS EC2
Date: 2026-04-20

## 1) Goal
Build a high-throughput orchestration engine that processes 2,400 operations/min with P95 latency around 22ms, provides exactly-once task execution semantics, supports self-healing failure recovery, and demonstrates 100% task retention under stress with 50 concurrent workers.

## 2) Success Metrics
- Throughput: >= 2,400 ops/min sustained for 15 minutes
- Latency: P95 <= 22ms for enqueue + claim + ack path (excluding long business task runtime)
- Delivery semantics: exactly once at business-operation level via idempotency ledger
- Retention under failure: 100% tasks retained (processed or in retry/DLQ)
- Concurrency: stable at 50 worker processes
- Recoverability: full replay audit via offsets and ledger tables

## 3) MVP Scope
In scope:
1. Task ingest API (FastAPI)
2. Redis Streams queue with consumer groups
3. Worker pool + task lease/ack flow
4. Idempotency ledger in PostgreSQL
5. Retry policy with backoff
6. Circuit breaker and DLQ
7. Replay and audit API
8. Load testing + EC2 deployment

Out of scope for MVP:
- Multi-region deployment
- Advanced workflow DSL UI
- Multi-tenant RBAC

## 4) High-Level Architecture
1. API Gateway (FastAPI)
- Accepts tasks with idempotency key and payload.
- Stores task metadata in PostgreSQL.
- Publishes task to Redis Stream.

2. Stream Layer (Redis Streams)
- Single stream per task domain.
- Consumer groups for worker balancing.

3. Worker Runtime
- Pulls tasks via XREADGROUP.
- Uses idempotency ledger to prevent duplicate business execution.
- Executes handler and writes result.
- Acks stream message on success.

4. Reliability Layer
- Retry scheduler for transient errors.
- Circuit breaker for failing dependencies.
- DLQ for poison messages.

5. Audit and Replay Layer
- Offset and status ledger in PostgreSQL.
- Replay endpoint to requeue selected failed tasks.

## 5) Proposed Folder Structure
taskMesh/
- app/
  - api/
  - queue/
  - workers/
  - reliability/
  - audit/
  - db/
- tests/
- loadtests/
- infra/
- scripts/
- docker-compose.yml
- README.md

## 6) Data Model (PostgreSQL)
Tables:
1. tasks
- task_id (pk)
- idempotency_key (unique)
- task_type
- payload (jsonb)
- status (queued, processing, succeeded, failed, dead_letter)
- created_at
- updated_at

2. task_attempts
- attempt_id (pk)
- task_id (fk)
- worker_id
- stream_id
- started_at
- ended_at
- result_code
- error_type
- error_message

3. idempotency_ledger
- idempotency_key (pk)
- execution_hash
- first_processed_at
- final_status

4. dead_letter_queue
- dlq_id (pk)
- task_id (fk)
- stream_id
- reason
- failed_at
- replayed_at

5. replay_audit
- replay_id (pk)
- task_id
- requested_by
- requested_at
- replay_status

## 7) API Contract (FastAPI)
1. POST /tasks
- input: task_type, payload, idempotency_key
- output: task_id, queued_status

2. GET /tasks/{task_id}
- output: current status, attempts, final result

3. POST /tasks/replay
- input: task_id list or filter
- output: replay request id and accepted count

4. GET /audit/offsets
- output: worker offsets, lag, pending counts

5. GET /metrics/summary
- output: throughput, p50/p95 latency, error rate, retry count, dlq count

## 8) Exactly-Once Strategy
Exactly-once in distributed systems is implemented as exactly-once business effect:
1. Require client idempotency_key per logical operation.
2. Check idempotency_ledger before executing handler.
3. If key exists with success, return stored result and skip execution.
4. If not present, execute handler in transaction-like flow and persist final status atomically.
5. Acknowledge Redis message only after durable status write.

## 9) Failure Recovery Design
- Retries: exponential backoff with max retry attempts
- Circuit breaker: opens after configurable consecutive dependency failures
- DLQ routing: non-recoverable or max-retry-exceeded tasks
- Replay: controlled reprocessing from DLQ with audit trail

## 10) Phase-Wise Implementation Plan (12 Days)

### Phase 1: Platform Foundation (Days 1-2)
Step 1 (Day 1): Bootstrap and infra
- Setup FastAPI, Redis, PostgreSQL, Docker Compose.
- Deliverable: health checks and local startup.

Step 2 (Day 2): Task API and persistence
- Build POST /tasks and task lifecycle persistence.
- Deliverable: task creation and status query working.

Phase 1 exit criteria:
- End-to-end task intake works.
- Persistence schema and migrations are stable.

### Phase 2: Core Queue and Worker Flow (Days 3-5)
Step 1 (Day 3): Redis Streams integration
- Add stream producer and consumer group setup.
- Deliverable: tasks flowing from API to stream.

Step 2 (Day 4): Worker engine
- Build worker loop and handler interface.
- Deliverable: task processing end-to-end.

Step 3 (Day 5): Idempotency ledger
- Implement duplicate prevention and result reuse.
- Deliverable: duplicate submissions do not double-execute.

Phase 2 exit criteria:
- Stream-to-worker pipeline is stable.
- Exactly-once business semantics are validated on duplicate test cases.

### Phase 3: Reliability and Recovery (Days 6-8)
Step 1 (Day 6): Retry and backoff
- Add retry scheduler and attempt tracking.
- Deliverable: transient failures retried correctly.

Step 2 (Day 7): Circuit breaker + DLQ
- Add breaker around external handlers and DLQ routing.
- Deliverable: poison tasks retained and isolated.

Step 3 (Day 8): Replay and audit APIs
- Build replay endpoint and offset audit views.
- Deliverable: failed tasks replayable with history.

Phase 3 exit criteria:
- Failure paths no longer lose tasks.
- Replay and audit records are complete and queryable.

### Phase 4: Performance and Observability (Days 9-10)
Step 1 (Day 9): Metrics and observability
- Add throughput, latency, errors, queue lag metrics.
- Deliverable: Grafana-ready metrics output.

Step 2 (Day 10): Load test harness
- Create load scenario for 50 workers and peak throughput.
- Deliverable: baseline performance report.

Phase 4 exit criteria:
- Throughput and latency metrics are measurable and reproducible.
- Load test reports capture p50/p95/p99 and failure statistics.

### Phase 5: Deployment and Evidence Pack (Days 11-12)
Step 1 (Day 11): EC2 deployment
- Deploy stack on AWS EC2 with systemd or docker compose.
- Deliverable: remote benchmark and stress output.

Step 2 (Day 12): Hardening and documentation
- Final tuning, README, architecture diagram, demo script.
- Deliverable: recruiter-ready evidence pack.

Phase 5 exit criteria:
- EC2 deployment is repeatable.
- Final benchmark and reliability evidence are portfolio-ready.

## 11) Load and Stress Test Plan
Scenarios:
1. Steady state: 1,500 ops/min for 20 min
2. Peak burst: 2,400+ ops/min for 15 min
3. Failure injection: dependency failure + worker kill test
4. DLQ replay test: verify 100% task retention and replay correctness

Metrics tracked:
- throughput
- p50/p95/p99 enqueue-to-ack latency
- retry count
- DLQ depth
- success ratio

## 12) Deployment Plan (AWS EC2)
- EC2 instance with Docker and CloudWatch agent
- Redis and Postgres in Docker for MVP (or managed services in v2)
- Grafana optional sidecar for metrics visualization
- Daily snapshot of PostgreSQL audit tables

## 13) Resume Proof Checklist
- Load test report proving 2,400 ops/min and P95 latency target
- Screenshot/log showing 50 concurrent workers active
- DLQ + replay audit logs proving 100% retention under stress
- Architecture diagram of exactly-once and failure recovery flow
