ALTER TABLE events
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN request_fingerprint TEXT;

ALTER TABLE events
    ADD CONSTRAINT events_idempotency_key_length_check
    CHECK (idempotency_key IS NULL OR length(idempotency_key) <= 255);

CREATE UNIQUE INDEX events_project_idempotency_key_idx
    ON events (project_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
