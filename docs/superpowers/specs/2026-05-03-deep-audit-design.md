---
name: RelayBox Deep Audit Design
date: 2026-05-03
status: draft
---

# RelayBox Deep Audit Design Spec

## 1. Objective
Perform a comprehensive audit of the RelayBox codebase to identify all bugs, architectural flaws, security vulnerabilities, and testing gaps. The goal is to move the project from a functional prototype to a production-ready, highly reliable implementation of the Transactional Outbox Pattern.

## 2. Audit Framework

The audit will be conducted in three sequential phases.

### Phase 1: Reliability & Data Integrity (The "Zero-Loss" Pass)
**Focus:** Guaranteeing that no events are lost, duplicated, or stuck.

- **Relay Loop Audit**: 
    - Verify `FOR UPDATE SKIP LOCKED` in `internal/store/postgres.go` to ensure concurrent relay workers don't double-process.
    - Check for potential "stuck" events (e.g., events marked as `PROCESSING` but the worker crashes).
- **DLQ Lifecycle Audit**:
    - Trace event flow: `Main Outbox` $\to$ `DLQ` $\to$ `Retry` $\to$ `Main Outbox`.
    - Investigate the reported bug in `internal/dlq/manager.go` where `RetryBatch` soft-deletes without re-enqueuing.
- **Consumer Idempotency Audit**:
    - Audit Redis `SETNX` logic in `internal/consumer/idempotency.go`.
    - Investigate the UUID parsing failure that generates random IDs, breaking traceability.
- **Failure Mode Analysis**:
    - Simulate/Analyze failures of Kafka, Postgres, and Redis to ensure the system recovers gracefully.

### Phase 2: Architecture & Quality (The "Maintainability" Pass)
**Focus:** Improving the codebase's structure for long-term stability and testability.

- **Dependency Analysis**:
    - Identify all usages of concrete types (e.g., `*store.PostgresStore`) in `internal/poller` and `internal/consumer`.
    - Design interfaces to decouple business logic from infrastructure.
- **Test Coverage Mapping**:
    - Inventory all core functions and map them to existing tests.
    - Identify "dark areas" with 0% coverage (likely Poller, Store, and DLQ Manager).
- **Code Quality Review**:
    - Audit error handling patterns (searching for ignored errors or generic `fmt.Errorf` without wrapping).
    - Check for inconsistent naming and architectural leaks.

### Phase 3: Observability & Security (The "Production-Ready" Pass)
**Focus:** Ensuring the system is monitorable and secure.

- **Metrics Validation**:
    - Verify that Prometheus counters in `internal/metrics` are incremented exactly once per event lifecycle stage.
    - Audit lag calculation logic to ensure `relay_pending_events_count` is accurate.
- **Dashboard Audit**:
    - Validate `grafana-dashboard.json` against the metrics actually emitted by the code.
- **Security & Resource Audit**:
    - Confirm parameterized queries prevent SQL injection.
    - Check for resource leaks (DB connections, Kafka producers, Redis clients).
    - Scan for potential race conditions in the concurrent poller.

## 3. Output Format
For every issue found, a "Findings Report" will be generated with:
- **ID**: Unique identifier (e.g., `REL-001`).
- **Location**: File path and line number.
- **Severity**: `CRITICAL` (Data loss/Corruption), `MAJOR` (Bugs/Performance), `MINOR` (Debt/Style).
- **Description**: What is wrong and why it matters.
- **Proposed Fix**: High-level solution.

## 4. Success Criteria
The audit is complete when:
1. All three phases are executed.
2. A comprehensive list of findings is documented.
3. A prioritized roadmap for resolution is presented to the user.
