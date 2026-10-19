"""Small immutable public summaries of agent attempts; never model transcripts."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re


def safe_text(value, limit=2000):
    text = str(value or "")
    text = re.sub(r"(?i)(bearer\s+)[\w.\-]+", r"\1[redacted]", text)
    text = re.sub(r"(?i)((?:api[_-]?key|token|password|secret)\s*[=:]\s*)[^\s,;]+", r"\1[redacted]", text)
    text = re.sub(r"\b(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9]{12,})\b", "[redacted]", text)
    return text[:limit]


def write_event(directory, cycle_id, event, payload):
    """Write one start/end record exclusively and fsync before returning."""
    if event not in {"started", "completed", "failed"}:
        raise ValueError("unsupported cycle event")
    folder = Path(directory) / cycle_id
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{event}.json"
    with path.open("x") as stream:
        json.dump({"schema_version": 1, "cycle_id": cycle_id, "event": event,
                   "origin": "recorded_attempt", **payload}, stream, ensure_ascii=False, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def terminal_summary(record):
    final = record.get("final", {})
    role = record["role"]
    usage = record.get("usage", {})
    return {"role": role, "status": record["status"],
            "completed_at": record["completed_at"],
            "reviewed_input_ids": final.get("reviewed_ticket_ids" if role == "mid" else "reviewed_annotation_ids", []),
            "summary": safe_text(final.get("summary")),
            "published_count": final.get("published_count"),
            "state_verified": record.get("state_verified", False),
            "usage": {key: value for key, value in usage.items()
                      if key in {"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"}
                      and isinstance(value, (int, float))},
            "pending_count": record.get("pending_count"),
            **tool_events(record.get("output_dir"))}


def tool_events(output_dir):
    """Extract identifiers/status only; arguments, output and messages stay private."""
    result = {"tool_events": [], "tool_event_count": 0,
              "tool_timing_status": "not_recorded", "tool_detail_status": "unavailable"}
    if not output_dir:
        return result
    path = Path(output_dir) / "events.jsonl"
    if not path.is_file():
        return result
    allowed = {"db_query.py", "db_write.py", "research_sources.py", "source_context.py", "mrbrain_invest.py", "recall.py"}
    try:
        with path.open() as stream:
            for index, line in enumerate(stream):
                if index >= 2000:
                    result["tool_detail_status"] = "bounded"
                    break
                if len(line) > 2_000_000:
                    continue
                event = json.loads(line)
                if not isinstance(event, dict):
                    continue
                item = event.get("item", {})
                if not isinstance(item, dict):
                    continue
                if event.get("type") not in {"item.started", "item.completed"} or item.get("type") != "command_execution":
                    continue
                command = str(item.get("command", ""))
                names = sorted(name for name in allowed if re.search(r"(?:^|[/\s])" + re.escape(name) + r"(?:\s|$)", command))
                result["tool_event_count"] += 1
                if len(result["tool_events"]) < 200:
                    result["tool_events"].append({"event": event["type"], "item_id": item.get("id"),
                                                  "type": "command_execution", "tool_names": names,
                                                  "status": item.get("status"), "exit_code": item.get("exit_code"),
                                                  "timestamp": None})
                result["tool_detail_status"] = "recorded_metadata_only"
        if result["tool_event_count"] > len(result["tool_events"]):
            result["tool_detail_status"] = "bounded"
    except (OSError, ValueError, TypeError):
        result["tool_detail_status"] = "partially_unreadable"
    return result
