CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS raw_messages (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_messages_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_raw_messages_source ON raw_messages ((payload->>'source'));
CREATE INDEX IF NOT EXISTS idx_raw_messages_source_id ON raw_messages ((payload->>'source_id'));

CREATE TABLE IF NOT EXISTS source_clean (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT source_clean_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS vector_embeddings (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    embedding_vec vector(768),
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT vector_embeddings_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS vector_embeddings_embedding_vec_hnsw
    ON vector_embeddings USING hnsw (embedding_vec vector_cosine_ops);

CREATE TABLE IF NOT EXISTS attention_tickets (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT attention_tickets_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS annotations (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT annotations_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS decision_packets (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT decision_packets_kafka_offset_uniq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS agent_state (
    agent_id TEXT PRIMARY KEY,
    state JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
