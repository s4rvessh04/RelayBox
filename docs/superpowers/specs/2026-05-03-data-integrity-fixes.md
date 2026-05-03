---
name: Data Integrity Fixes Design
date: 2026-05-03
status: approved
---

# Data Integrity Fixes Design Spec

## 1. Objective
Eliminate data loss risks associated with the Dead Letter Queue (DLQ) retry mechanism and silent failures during DLQ archival.

## 2. Technical Design

### 2.1 DLQ Atomic Retry (`internal/dlq/manager.go`)
The `RetryBatch` method will be refactored to ensure events are not lost during the transition from the DLQ back to the Outbox.

**Implementation Logic:**
1. Start a PostgreSQL transaction.
2. Fetch the batch of events targeted for retry.
3. For each event in the batch:
   - Insert the payload into the `outbox` table.
   - **Preserve** the original `created_at` timestamp.
   - **Update** the `modified_at` timestamp to `NOW()`.
   - Set status to `PENDING`.
4. Mark the events as soft-deleted in the `outbox_dlq` table by setting `deleted_at = NOW()`.
5. Commit the transaction.

**Consistency Guarantee:** The transaction ensures that an event is only removed from the DLQ if it has been successfully re-enqueued in the Outbox.

### 2.2 Consumer Error Handling (`internal/consumer/idempotency.go`)
Prevent "silent drops" where an event fails processing and subsequently fails to be archived in the DLQ.

**Implementation Logic:**
- Replace `_ = m.dlq.Push(...)` with a standard Go error check.
- If `dlq.Push` returns an error, the method must return that error to the caller (the Kafka consumer).
- This ensures the Kafka offset is not committed for events that are neither processed nor archived, triggering a retry by Kafka.

## 3. Verification Plan

### 3.1 Integration Test: `TestDLQRetryCycle`
A new integration test will be added to verify the end-to-end retry flow:
1. **Setup**: Insert a test event into the `outbox_dlq` table.
2. **Action**: Execute `RetryBatch`.
3. **Verify Outbox**: Query the `outbox` table to ensure the event exists with the original creation date and updated modified date.
4. **Verify DLQ**: Query the `outbox_dlq` table to ensure the event is marked as deleted.

## 4. Success Criteria
- `RetryBatch` successfully moves events from DLQ to Outbox without loss or duplicates.
- Failures in `dlq.Push` are surfaced as errors to the consumer.
- The `TestDLQRetryCycle` passes consistently.
