CREATE TABLE IF NOT EXISTS channelcore_outbox (
    id VARCHAR(256) PRIMARY KEY,
    destination VARCHAR(512) NOT NULL,
    message_key BYTEA NOT NULL DEFAULT ''::BYTEA,
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::JSONB,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    lease_owner VARCHAR(256),
    lease_until TIMESTAMPTZ,
    state SMALLINT NOT NULL DEFAULT 0 CHECK (state IN (0, 1, 2)),
    failure_reason VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    replay_count INTEGER NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    replayed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS channelcore_outbox_pending_idx
    ON channelcore_outbox (available_at, id)
    WHERE state = 0;
