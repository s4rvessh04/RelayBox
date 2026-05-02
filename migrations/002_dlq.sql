-- Migration 002: Create DLQ
CREATE TABLE outbox_dlq (
    id UUID PRIMARY KEY,
    original_id UUID NOT NULL,
    aggregate_type TEXT,
    aggregate_id TEXT,
    event_type TEXT,
    payload JSONB,
    error_message TEXT,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dlq_original_id ON outbox_dlq(original_id);
CREATE INDEX idx_dlq_deleted_at ON outbox_dlq(deleted_at);
