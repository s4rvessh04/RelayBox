# Data Integrity Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the DLQ retry logic to prevent data loss and eliminate silent failures during DLQ archival.

**Architecture:** Use a database transaction to atomically move events from the DLQ back into the Outbox. Update consumer logic to surface DLQ push errors.

**Tech Stack:** Go, PostgreSQL.

---

## File Structure

- Modify: `internal/dlq/manager.go` - Implement atomic re-enqueueing in `RetryBatch`.
- Modify: `internal/consumer/idempotency.go` - Fix silent error in `dlq.Push`.
- Create: `internal/dlq/manager_test.go` - Integration tests for the DLQ retry cycle.

---

## Tasks

### Task 1: Fix Silent DLQ Push Failures
**Files:**
- Modify: `internal/consumer/idempotency.go:76-84`

- [ ] **Step 1: Replace ignored error with proper handling**
  Change line 76 from `_ = c.dlq.Push(...)` to:
  ```go
  if pushErr := c.dlq.Push(ctx, dlq.Event{...}); pushErr != nil {
      return fmt.Errorf("failed to archive event to DLQ: %w", pushErr)
  }
  ```
- [ ] **Step 2: Verify compilation**
  Run: `go build ./internal/consumer/...`
- [ ] **Step 3: Commit**
  Run: `git add internal/consumer/idempotency.go && git commit -m "fix: surface DLQ push errors to prevent silent data loss"`

### Task 2: Atomic DLQ Retry Implementation
**Files:**
- Modify: `internal/dlq/manager.go:36-72`

- [ ] **Step 1: Update `RetryBatch` to fetch full event data**
  Modify the query on line 44 to select all necessary columns:
  ```go
  rows, err := tx.Query(ctx, `
      SELECT id, original_id, aggregate_type, aggregate_id, event_type, payload 
      FROM outbox_dlq 
      WHERE deleted_at IS NULL 
      LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
  ```
- [ ] **Step 2: Implement Re-enqueueing Logic**
  Within the `rows.Next()` loop, for each event, insert a new record into `outbox_events`. 
  Query:
  ```sql
  INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, created_at, modified_at)
  VALUES ($1, $2, $3, $4, 'PENDING', $5, NOW())
  ```
  Note: Use `e.CreatedAt` (original) for `$5`. (Wait, `dlq.Event` needs `CreatedAt`. Check `internal/dlq/manager.go` struct. If missing, use `failed_at` from `outbox_dlq` table as the original creation time proxy).
- [ ] **Step 3: Maintain Atomicity**
  Ensure the `UPDATE outbox_dlq SET deleted_at = NOW()` call happens within the same transaction after all inserts succeed.
- [ ] **Step 4: Verify compilation**
  Run: `go build ./internal/dlq/...`
- [ ] **Step 5: Commit**
  Run: `git add internal/dlq/manager.go && git commit -m "fix: implement atomic DLQ to Outbox transfer in RetryBatch"`

### Task 3: Verify DLQ Retry Cycle (Integration Test)
**Files:**
- Create: `internal/dlq/manager_test.go`

- [ ] **Step 1: Implement `TestDLQRetryCycle`**
  Write a test that:
  1. Connects to a test Postgres DB.
  2. Pushes an event to the DLQ.
  3. Calls `RetryBatch`.
  4. Asserts that the event now exists in `outbox_events` with `status = 'PENDING'`.
  5. Asserts that the original event in `outbox_dlq` has `deleted_at IS NOT NULL`.
- [ ] **Step 2: Run test**
  Run: `go test -v internal/dlq/manager_test.go`
- [ ] **Step 3: Commit**
  Run: `git add internal/dlq/manager_test.go && git commit -m "test: add integration test for DLQ retry cycle"`
