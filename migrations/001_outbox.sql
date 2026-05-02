--C:\
-- Create the outbox table
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(100) NOT NULL,  -- "Order", "Payment"
    aggregate_id    VARCHAR(100) NOT NULL,   -- entity ID
    event_type      VARCHAR(100) NOT NULL,   -- "OrderCreated"
    payload         JSONB        NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING', -- PENDING | SENT | FAILED
    idempotency_key  VARCHAR(200) UNIQUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ,
    retry_count     INT          NOT NULL DEFAULT 0
);

-- Partial index for the poller to find pending events efficiently
CREATE INDEX idx_outbox_pending ON outbox_events(status, created_at)
    WHERE status = 'PENDING';
