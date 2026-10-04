ALTER TABLE deliveries
    ADD COLUMN IF NOT EXISTS lease_id UUID,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

UPDATE deliveries
SET status = 'pending', updated_at = NOW()
WHERE status = 'delivering';

CREATE INDEX IF NOT EXISTS deliveries_due_idx
    ON deliveries (status, next_retry_at, lease_expires_at, created_at);
