-- Forward-only routing audit. Existing source/evidence records are unchanged.
CREATE TABLE IF NOT EXISTS routing_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE CHECK (event_id ~ '^[0-9a-f]{64}$'),
    stage TEXT NOT NULL CHECK (stage IN ('filter', 'fast')),
    outcome TEXT NOT NULL CHECK (outcome IN ('pass', 'reject')),
    input_topic TEXT NOT NULL,
    input_partition INTEGER NOT NULL CHECK (input_partition >= 0),
    input_offset BIGINT NOT NULL CHECK (input_offset >= 0),
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    source TEXT,
    source_id TEXT,
    raw_sha256 TEXT,
    reason TEXT,
    reason_status TEXT NOT NULL CHECK (reason_status IN ('recorded', 'not_recorded')),
    classifier JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    decision_sha256 TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (stage, input_topic, input_partition, input_offset),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_routing_events_original ON routing_events (source, source_id, raw_sha256);
CREATE INDEX IF NOT EXISTS idx_routing_events_stage_outcome ON routing_events (stage, outcome, id DESC);
