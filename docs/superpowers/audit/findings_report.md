# RelayBox Deep Audit Findings Report

## Resolution Roadmap

### Phase 1: Critical Data Integrity & Loss Prevention
- **DLQ-01 (High)**: Fix `RetryBatch` to restore events to `outbox` table. (Prevents silent event loss).
- **ERR-01 (Medium)**: Handle `dlq.Push` errors. (Prevents silent data loss).

### Phase 2: Core Reliability & Correctness
- **RL-01 (High)**: Implement `PROCESSING` state and timeout cleanup for the Poller. (Prevents duplicate processing and hang-ups).
- **IDEM-01 (Medium)**: Redesign idempotency keys to be globally unique. (Prevents incorrect event skipping).

### Phase 3: Testability & Infrastructure
- **COUPL-01 (Medium)**: Decouple `Poller` from concrete Postgres/Kafka implementations using interfaces. (Enables unit testing).
- **TEST-01 (High)**: Implement comprehensive unit and integration tests for `internal/` core logic.

### Phase 4: Observability & Polish
- **IDEM-02 (Low)**: Fix UUID parsing/generation in DLQ to maintain traceability.
- **MET-01 (Low)**: Correct idempotency metrics to distinguish between "processed" and "skipped".
- **ERR-02 (Low)**: Log `Close()` errors in Kafka consumer.

---

| ID | Location | Severity | Description | Proposed Fix |

|---|---|---|---|---|
| RL-01 | `internal/poller/poller.go` | High | No `PROCESSING` state or timeout mechanism. Events stay `PENDING` until the batch completes. Crash-induced rollbacks cause immediate retries of potentially problematic events. | Introduce a `PROCESSING` status with a timestamp. Implement a cleanup worker to reset expired `PROCESSING` events back to `PENDING` after a timeout. |
| DLQ-01 | `internal/dlq/manager.go:66` | High | `RetryBatch` only performs a soft-delete in the `outbox_dlq` table. It does NOT move events back to the `outbox` table, meaning events are removed from the DLQ without being rescheduled. | Implement logic to insert the event payload back into the `outbox` table before soft-deleting from `outbox_dlq`. |
| IDEM-01 | `internal/consumer/idempotency.go` | Medium | Idempotency key depends entirely on the caller. In the current `KafkaConsumer` implementation, `msg.Key` is used for both `ID` and `IdempotencyKey`. If Kafka keys are not unique across different event types or versions, collisions may occur. | Ensure idempotency keys are derived from a combination of event type, source, and a unique sequence/ID. |
| IDEM-02 | `internal/consumer/idempotency.go:72-75` | Low | UUID parsing failure for `event.ID` leads to generation of a new random UUID. This breaks traceability between the original event and the DLQ entry, making it impossible to correlate a failed event with its original source via the DLQ ID. | Log the original `event.ID` string in the DLQ record or use a consistent hashing mechanism if UUID parsing fails, rather than random generation. |
| COUPL-01 | `internal/poller/poller.go` | Medium | `Poller` has concrete dependencies on `*store.PostgresStore` and `*publisher.KafkaPublisher`. This makes unit testing the poller logic impossible without running a full Postgres and Kafka instance. | Replace concrete types with interfaces (e.g., `OutboxStore` and `EventPublisher`) and inject these interfaces into the `Poller` struct. |
| TEST-01 | `internal/` | High | Severe lack of test coverage across the core logic. `internal/poller/poller.go`, `internal/store/postgres.go`, and `internal/dlq/manager.go` have 0% test coverage. Existing tests in `publisher` and `consumer` are only benchmarks and do not verify functional correctness. | Implement comprehensive unit and integration tests for all public methods in `internal/`, focusing first on the store, dlq manager, and poller. |

| ERR-01 | `internal/consumer/idempotency.go:76` | Medium | Error from `c.dlq.Push` is explicitly ignored (`_ = `). Failures to move a failed event to the DLQ result in silent data loss. | Handle the error from `dlq.Push` and log it as a critical failure. |
| ERR-02 | `internal/consumer/kafka.go:29` | Low | Error from `c.Close()` is ignored in the `NewKafkaConsumer` failure path. While less critical here, it's a bad practice. | Log the closure error if it occurs. |
| MET-01 | internal/consumer/idempotency.go:57 | Low | Events that are skipped due to idempotency are counted as "processed" (EventsProcessedTotal.Inc()). This inflates the success rate and masks the actual volume of duplicate events. | Separate "processed" from "skipped" metrics; only increment EventsProcessedTotal for events that actually executed business logic. |
