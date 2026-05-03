# RelayBox Deep Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Systematically identify all bugs, architectural flaws, and security risks in the RelayBox project through a three-phase deep audit.

**Architecture:** The audit is split into Reliability, Architecture, and Observability phases. Findings are recorded in a structured format (ID, Location, Severity, Description, Proposed Fix).

**Tech Stack:** Go, PostgreSQL, Kafka, Redis, Prometheus, Grafana.

---

## Audit Findings Report
All findings will be aggregated into a new file: `docs/superpowers/audit/findings_report.md`.

### Task 1: Audit Setup & Report Initialization
**Files:**
- Create: `docs/superpowers/audit/findings_report.md`

- [ ] **Step 1: Create audit directory**
  Run: `mkdir -p docs/superpowers/audit`
- [ ] **Step 2: Initialize report file**
  Create `docs/superpowers/audit/findings_report.md` with the following header:
  ```markdown
  # RelayBox Deep Audit Findings Report
  
  | ID | Location | Severity | Description | Proposed Fix |
  |---|---|---|---|---|
  ```
- [ ] **Step 3: Commit setup**
  Run: `git add docs/superpowers/audit/findings_report.md && git commit -m "audit: initialize findings report"`

---

## Phase 1: Reliability & Data Integrity (Zero-Loss Pass)

### Task 2: Relay Loop & Concurrency Audit
**Files:**
- Read: `internal/store/postgres.go`
- Read: `internal/poller/poller.go`

- [ ] **Step 1: Verify `FOR UPDATE SKIP LOCKED`**
  Read `internal/store/postgres.go` and verify that the polling query uses `FOR UPDATE SKIP LOCKED` to prevent duplicate event processing by concurrent workers.
- [ ] **Step 2: Audit "Stuck" Event State**
  Review `internal/poller/poller.go`. Check if events are marked as `PROCESSING` and if there is a timeout or cleanup mechanism for events that remain in `PROCESSING` after a crash.
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

### Task 3: DLQ Lifecycle Audit
**Files:**
- Read: `internal/dlq/manager.go`
- Read: `migrations/002_dlq.sql`

- [ ] **Step 1: Trace `RetryBatch` Logic**
  Read `internal/dlq/manager.go`. Verify if `RetryBatch` only performs a soft-delete or if it actually moves events back to the `outbox` table for reprocessing.
- [ ] **Step 2: Verify DLQ Table Schema**
  Read `migrations/002_dlq.sql` to ensure the schema supports the required retry metadata.
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

### Task 4: Consumer Idempotency Audit
**Files:**
- Read: `internal/consumer/idempotency.go`
- Read: `internal/consumer/kafka.go`

- [ ] **Step 1: Audit Redis `SETNX` Implementation**
  Review `internal/consumer/idempotency.go`. Verify that the idempotency key is unique and that the TTL is appropriate to prevent memory exhaustion in Redis.
- [ ] **Step 2: Analyze UUID Parsing Logic**
  Check how `event.ID` is handled. Verify if the fallback to random UUID generation on parsing failure breaks traceability.
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

---

## Phase 2: Architecture & Quality (Maintainability Pass)

### Task 5: Dependency & Coupling Audit
**Files:**
- Read: `internal/poller/poller.go`
- Read: `internal/consumer/kafka.go`
- Read: `internal/store/postgres.go`

- [ ] **Step 1: Identify Concrete Type Dependencies**
  Search for usages of `*store.PostgresStore` and `*publisher.KafkaPublisher` inside `internal/poller` and `internal/consumer`.
- [ ] **Step 2: Evaluate Interface Opportunities**
  Identify which concrete types should be replaced with interfaces to allow for unit testing with mocks.
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

### Task 6: Test Coverage Gap Analysis
**Files:**
- Read all files in `internal/`
- Read all files in `internal/**/*_test.go`

- [ ] **Step 1: Map Functions to Tests**
  Create a mapping of every public method in `internal/` to its corresponding test case.
- [ ] **Step 2: Identify "Dark Areas"**
  Specifically check for missing tests in:
  - `internal/poller/poller.go`
  - `internal/store/postgres.go`
  - `internal/dlq/manager.go`
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

### Task 7: General Code Quality Audit
**Files:**
- Read: All Go files in `internal/`

- [ ] **Step 1: Audit Error Handling**
  Search for `_ =` or ignored errors. Check for generic error returns that lack context (e.g., `fmt.Errorf("error")` without wrapping).
- [ ] **Step 2: Review Naming and Consistency**
  Check for inconsistent naming conventions or architectural leaks (e.g., DB logic leaking into the poller).
- [ ] **Step 3: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

---

## Phase 3: Observability & Security (Production Pass)

### Task 8: Metrics & Dashboard Audit
**Files:**
- Read: `internal/metrics/metrics.go`
- Read: `grafana-dashboard.json`

- [ ] **Step 1: Verify Metric Increments**
  Check `internal/metrics/metrics.go` and ensure counters are incremented exactly once per event state transition.
- [ ] **Step 2: Validate Lag Calculation**
  Verify the logic used to calculate `relay_pending_events_count`.
- [ ] **Step 3: Cross-Reference with Grafana**
  Check `grafana-dashboard.json` to ensure all emitted metrics are actually visualized.
- [ ] **Step 4: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

### Task 9: Security & Resource Audit
**Files:**
- Read: `internal/store/postgres.go`
- Read: `internal/store/redis.go`
- Read: `internal/publisher/kafka.go`

- [ ] **Step 1: Verify Parameterized Queries**
  Ensure all SQL queries in `internal/store/postgres.go` use placeholders (`$1`, `$2`) and no string concatenation.
- [ ] **Step 2: Audit Resource Leaks**
  Check for unclosed `sql.Rows`, `sql.Stmt`, or Kafka producer connections.
- [ ] **Step 3: Check for Race Conditions**
  Analyze concurrent access to shared state in the poller and consumer.
- [ ] **Step 4: Record Findings**
  Append any issues to `docs/superpowers/audit/findings_report.md`.

---

## Finalization

### Task 10: Final Review & Roadmap
**Files:**
- Read: `docs/superpowers/audit/findings_report.md`

- [ ] **Step 1: Prioritize Findings**
  Sort the findings in `findings_report.md` by severity (`CRITICAL` $\to$ `MAJOR` $\to$ `MINOR`).
- [ ] **Step 2: Generate Roadmap**
  Create a summary section at the top of the report suggesting the order of resolution.
- [ ] **Step 3: Final Commit**
  Run: `git add docs/superpowers/audit/findings_report.md && git commit -m "audit: finalize findings report"`
