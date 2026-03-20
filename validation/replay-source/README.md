# Jetstream Replay Source

Lightweight websocket replay source for `bluesky_ingestor.py` input validation.

## What It Does

- Reads raw Jetstream text frames from a dataset file.
- Serves frames over websocket on `/subscribe` (or custom path).
- Replays in deterministic order for repeatable validation runs.
- Supports speed control (`messages-per-second`) and repeated sends (`repeat-each`).

## Supported Dataset Formats

- JSON object with `frames` array (recommended, matches `golden_samples.json`).
- JSON array of frames.
- JSONL (one raw JSON frame per line).

Each frame is validated as JSON text before serving.

## Quick Start (Docker Compose Service)

Use replay mode without changing the base compose file:

```bash
docker compose -f kafka-cluster/docker-compose.yaml -f kafka-cluster/docker-compose.replay.yaml up -d
```

This starts `replay-source` and overrides `bluesky-ingestor --jetstream-url` to:

- `ws://replay-source:9001/subscribe`

Stop replay mode:

```bash
docker compose -f kafka-cluster/docker-compose.yaml -f kafka-cluster/docker-compose.replay.yaml down
```

## Quick Start (Local Python)

1. Install dependency:

```bash
pip install -r kafka-cluster/validation/replay-source/requirements.txt
```

2. Run replay source:

```bash
python kafka-cluster/validation/replay-source/jetstream_replay_server.py \
  --dataset kafka-cluster/validation/golden-dataset/golden_samples.json \
  --host 0.0.0.0 \
  --port 9001 \
  --path /subscribe \
  --messages-per-second 8 \
  --loop-forever
```

3. Point ingestor to replay source:

```bash
python kafka-cluster/bluesky-demo/bluesky_ingestor.py \
  --jetstream-url ws://127.0.0.1:9001/subscribe
```

## Useful Flags

- `--messages-per-second`: replay rate (must be `> 0`).
- `--repeat-each`: repeat each frame N times (helps dedup tests).
- `--loop-forever`: infinite replay loop.
- `--max-loops`: finite loop count if not using `--loop-forever`.
- `--start-index`: start from a specific frame index.
- `--max-frames-per-loop`: cap frames per loop.

## Example: Finite, Controlled Replay

```bash
python kafka-cluster/validation/replay-source/jetstream_replay_server.py \
  --dataset kafka-cluster/validation/golden-dataset/golden_samples.json \
  --messages-per-second 5 \
  --repeat-each 2 \
  --max-loops 3 \
  --max-frames-per-loop 10
```

This sends the first 10 frames, each twice, for 3 loops in stable order.
