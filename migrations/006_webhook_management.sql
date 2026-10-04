ALTER TABLE webhooks
    ADD COLUMN description TEXT,
    ADD COLUMN environment TEXT NOT NULL DEFAULT 'production',
    ADD COLUMN custom_headers JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE webhooks
    ADD CONSTRAINT webhooks_environment_check
    CHECK (length(trim(environment)) > 0);
