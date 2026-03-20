#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServerProtocol, serve


LOGGER = logging.getLogger("jetstream-replay")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay raw Jetstream websocket frames for bluesky_ingestor input."
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Path to dataset file (JSON with frames[] or JSONL).",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host.")
    parser.add_argument("--port", type=int, default=9001, help="Bind port.")
    parser.add_argument(
        "--path",
        default="/subscribe",
        help="Expected websocket path prefix (query string allowed).",
    )
    parser.add_argument(
        "--messages-per-second",
        type=float,
        default=10.0,
        help="Replay speed. Must be > 0.",
    )
    parser.add_argument(
        "--repeat-each",
        type=int,
        default=1,
        help="How many times to send each frame before moving on.",
    )
    parser.add_argument(
        "--loop-forever",
        action="store_true",
        help="Replay dataset in an infinite loop instead of one pass.",
    )
    parser.add_argument(
        "--max-loops",
        type=int,
        default=0,
        help="Loop count when not using --loop-forever. 0 means one pass.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Start replay from this frame index.",
    )
    parser.add_argument(
        "--max-frames-per-loop",
        type=int,
        default=0,
        help="Only replay first N frames per loop. 0 means all remaining frames.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def _validate_frame(raw_frame: str, index: int) -> str:
    if not isinstance(raw_frame, str):
        raise ValueError(f"frame[{index}] must be a string")
    try:
        json.loads(raw_frame)
    except json.JSONDecodeError as ex:
        raise ValueError(f"frame[{index}] is not valid JSON text frame: {ex}") from ex
    return raw_frame


def load_frames(dataset_path: Path) -> list[str]:
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset not found: {dataset_path}")

    raw_text = dataset_path.read_text(encoding="utf-8")
    suffix = dataset_path.suffix.lower()

    if suffix == ".jsonl":
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        return [_validate_frame(line, i) for i, line in enumerate(lines)]

    parsed = json.loads(raw_text)
    if isinstance(parsed, dict):
        frames_raw = parsed.get("frames")
    elif isinstance(parsed, list):
        frames_raw = parsed
    else:
        raise ValueError("dataset JSON must be an object with frames[] or an array")

    if not isinstance(frames_raw, list):
        raise ValueError("dataset JSON is missing frames[] array")

    frames: list[str] = []
    for idx, item in enumerate(frames_raw):
        if isinstance(item, str):
            frames.append(_validate_frame(item, idx))
        elif isinstance(item, (dict, list)):
            frames.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
        else:
            raise ValueError(f"frame[{idx}] must be string/object/array, got {type(item).__name__}")
    return frames


def clamp_frames(frames: list[str], start_index: int, max_frames_per_loop: int) -> list[str]:
    if start_index < 0:
        raise ValueError("--start-index must be >= 0")
    if start_index >= len(frames):
        raise ValueError(f"--start-index ({start_index}) exceeds frame count ({len(frames)})")
    sliced = frames[start_index:]
    if max_frames_per_loop > 0:
        sliced = sliced[:max_frames_per_loop]
    if not sliced:
        raise ValueError("no frames selected for replay after applying start/max options")
    return sliced


def passes_for_client(loop_forever: bool, max_loops: int) -> int | None:
    if loop_forever:
        return None
    if max_loops <= 0:
        return 1
    return max_loops


async def replay_client(
    websocket: WebSocketServerProtocol,
    request_path: str,
    *,
    expected_path: str,
    frames: list[str],
    interval_seconds: float,
    repeat_each: int,
    loop_forever: bool,
    max_loops: int,
) -> None:
    if not request_path.startswith(expected_path):
        LOGGER.warning("reject path=%s expected_prefix=%s", request_path, expected_path)
        await websocket.close(code=1008, reason=f"unexpected path: {request_path}")
        return

    passes = passes_for_client(loop_forever=loop_forever, max_loops=max_loops)
    sent_total = 0
    loop_index = 0
    client = f"{websocket.remote_address}"
    LOGGER.info("client connected addr=%s path=%s", client, request_path)

    try:
        while passes is None or loop_index < passes:
            loop_index += 1
            for frame in frames:
                for _ in range(repeat_each):
                    await websocket.send(frame)
                    sent_total += 1
                    if interval_seconds > 0:
                        await asyncio.sleep(interval_seconds)
            LOGGER.info("loop complete addr=%s loop=%s sent_total=%s", client, loop_index, sent_total)
    except ConnectionClosed:
        LOGGER.info("client disconnected addr=%s sent_total=%s", client, sent_total)
        return

    await websocket.close(code=1000, reason="replay finished")
    LOGGER.info("replay finished addr=%s loops=%s sent_total=%s", client, loop_index, sent_total)


async def run_server(args: argparse.Namespace) -> None:
    if args.messages_per_second <= 0:
        raise ValueError("--messages-per-second must be > 0")
    if args.repeat_each <= 0:
        raise ValueError("--repeat-each must be > 0")

    dataset_path = Path(args.dataset).resolve()
    loaded = load_frames(dataset_path)
    frames = clamp_frames(
        loaded,
        start_index=args.start_index,
        max_frames_per_loop=max(0, args.max_frames_per_loop),
    )
    interval_seconds = 1.0 / args.messages_per_second

    LOGGER.info(
        "loaded dataset=%s total_frames=%s selected_frames=%s mps=%.3f repeat_each=%s loop_forever=%s max_loops=%s",
        dataset_path,
        len(loaded),
        len(frames),
        args.messages_per_second,
        args.repeat_each,
        args.loop_forever,
        args.max_loops,
    )

    async def handler(ws: WebSocketServerProtocol, path: str) -> None:
        await replay_client(
            ws,
            path,
            expected_path=args.path,
            frames=frames,
            interval_seconds=interval_seconds,
            repeat_each=args.repeat_each,
            loop_forever=args.loop_forever,
            max_loops=args.max_loops,
        )

    async with serve(
        handler,
        host=args.host,
        port=args.port,
        ping_interval=30,
        ping_timeout=30,
        max_size=10 * 1024 * 1024,
    ):
        LOGGER.info("server started ws://%s:%s%s", args.host, args.port, args.path)
        await asyncio.Future()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_server(args))


if __name__ == "__main__":
    main()
