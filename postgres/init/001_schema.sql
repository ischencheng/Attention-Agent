CREATE TABLE IF NOT EXISTS raw_posts (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    message_key TEXT,
    source TEXT,
    author_did TEXT,
    uri TEXT,
    cid TEXT,
    operation TEXT,
    collection TEXT,
    post_text TEXT,
    event_ts TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_posts_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS raw_posts_event_ts_idx ON raw_posts (event_ts DESC);
CREATE INDEX IF NOT EXISTS raw_posts_author_did_idx ON raw_posts (author_did);
CREATE INDEX IF NOT EXISTS raw_posts_uri_idx ON raw_posts (uri);

CREATE TABLE IF NOT EXISTS enriched_posts (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    message_key TEXT,
    pipeline_version TEXT,
    source TEXT,
    author_did TEXT,
    uri TEXT,
    cid TEXT,
    langs JSONB,
    post_text TEXT,
    features JSONB,
    candidate JSONB,
    event_ts TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT enriched_posts_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS enriched_posts_event_ts_idx ON enriched_posts (event_ts DESC);
CREATE INDEX IF NOT EXISTS enriched_posts_author_did_idx ON enriched_posts (author_did);
CREATE INDEX IF NOT EXISTS enriched_posts_candidate_gin_idx ON enriched_posts USING GIN (candidate);

CREATE TABLE IF NOT EXISTS strategy_signals (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    message_key TEXT,
    signal_type TEXT NOT NULL DEFAULT 'candidate',
    pipeline_version TEXT,
    source TEXT,
    author_did TEXT,
    uri TEXT,
    cid TEXT,
    score INTEGER,
    is_candidate BOOLEAN,
    reasons JSONB,
    tickers JSONB,
    hashtags JSONB,
    keyword_hits JSONB,
    sentiment_score INTEGER,
    author_posts_5m INTEGER,
    event_ts TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT strategy_signals_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS strategy_signals_event_ts_idx ON strategy_signals (event_ts DESC);
CREATE INDEX IF NOT EXISTS strategy_signals_score_idx ON strategy_signals (score DESC);
CREATE INDEX IF NOT EXISTS strategy_signals_signal_type_idx ON strategy_signals (signal_type);
CREATE INDEX IF NOT EXISTS strategy_signals_reasons_gin_idx ON strategy_signals USING GIN (reasons);

CREATE TABLE IF NOT EXISTS llm_insights (
    id BIGSERIAL PRIMARY KEY,
    strategy_signal_id BIGINT REFERENCES strategy_signals(id) ON DELETE SET NULL,
    model_name TEXT,
    summary TEXT,
    risk_tags JSONB,
    recommendation TEXT,
    confidence NUMERIC(5, 2),
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
