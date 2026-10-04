CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    response_code INTEGER,
    error TEXT,
    duration_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX delivery_attempts_delivery_number_idx
    ON delivery_attempts (delivery_id, attempt_number);

CREATE INDEX delivery_attempts_delivery_created_idx
    ON delivery_attempts (delivery_id, created_at DESC);
