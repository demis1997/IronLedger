-- IronLedger core schema: double-entry ledger, idempotency, outbox, projections.

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    policy TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE journal_entries (
    id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX journal_entries_created_at_idx ON journal_entries (created_at DESC);

CREATE TABLE postings (
    id BIGSERIAL PRIMARY KEY,
    entry_id UUID NOT NULL REFERENCES journal_entries (id) ON DELETE RESTRICT,
    account_id UUID NOT NULL REFERENCES accounts (id) ON DELETE RESTRICT,
    asset TEXT NOT NULL,
    amount_atomic TEXT NOT NULL,
    side TEXT NOT NULL
);

CREATE INDEX postings_account_id_idx ON postings (account_id, entry_id);

CREATE TABLE balances (
    account_id UUID NOT NULL REFERENCES accounts (id) ON DELETE RESTRICT,
    asset TEXT NOT NULL,
    amount_atomic TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, asset)
);

CREATE TABLE idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    leased_until TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    consumer TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE consumer_offsets (
    consumer TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer, topic, partition)
);

CREATE TABLE projection_balances (
    consumer TEXT NOT NULL,
    account_id UUID NOT NULL,
    asset TEXT NOT NULL,
    amount_atomic TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer, account_id, asset)
);
