# Bluesky Golden Dataset (Raw Only)

This folder now stores raw Jetstream websocket frames only.

- Source: `wss://jetstream2.us-east.bsky.network/subscribe?compress=false`
- Dataset file: `golden_samples.json`
- Format: JSON object with `frames` array
- Current composition: 30 real captured raw frames + 20 synthetic raw frames that satisfy candidate/LLM-path conditions

Raw-data guarantee:

- Every item in `frames` is the exact websocket text frame received from Jetstream.
- No field mapping, normalization, enrichment, or schema reshaping is applied.
- No `bluesky_ingestor` output envelope is included in this file.
- Synthetic frames are still raw Jetstream-schema text frames and are not wrapped by any project-specific schema.

Use this dataset to validate `bluesky_ingestor.py` input handling from first principles.

Replay source helper:

- See `kafka-cluster/validation/replay-source/README.md` for deterministic websocket replay into ingest input.
