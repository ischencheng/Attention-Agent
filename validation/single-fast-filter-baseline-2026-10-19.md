# Single Fast filter: historical baseline

Issue: [#13](https://github.com/ischencheng/Attention-Agent/issues/13).

Captured from the active local runtime at **2026-10-19 06:16:50.655985 UTC**. This is a historical baseline, not post-change acceptance evidence. The accompanying JSON preserves the captured counts; subsequent reads of the live runtime can differ. No cache, Kafka offsets, original records or runtime files were changed.

The read-only capture matched gateway leaf requests with identical request fields except `role`, the same reported model, and at most 16,000 message characters. It then read the linked `run.json` files and counted `turn.completed` events without retaining event contents.

| Model | Matched leaf pairs | Filter successful invocations / completed turns | Fast successful invocations / completed turns | Filter input / output tokens | Fast input / output tokens |
| --- | ---: | ---: | ---: | ---: | ---: |
| gpt-6.1-sol | 255 | 255 / 255 | 255 / 255 | 859,968 / 11,697 | 856,816 / 14,204 |
| gpt-6-luna | 89 | 89 / 89 | 89 / 89 | 240,448 / 4,078 | 244,453 / 6,603 |

The JSON includes all reported usage counters. Cached-input tokens must not be added to input tokens. The measurements show separate model invocations for the two stages on these matched leaf requests; they do not measure dollar savings.

## Evidence boundaries

- Leaf requests can be chunks of a longer original. These counts are not distinct original-record counts.
- Only requests reaching both stages are matched. Records rejected by the old first gate are excluded. A short uncached record rejected there needed one classification when no failures occurred.
- The capture grouped by request fields and model, not by prompt version or research context. Its exact grouping and duplicate-selection behavior are recorded in the JSON.
- The SQLite selection and subsequent artifact reads were not an atomic cross-file snapshot. The timestamp was printed after those reads, while the runtime remained active.
- A cache hit retains the successful response's earlier usage. Summing routing-event usage or counting cache rows cannot establish fresh model work.
- Failed attempts may have artifacts but no cache entry. Timeout failures can lack recorded usage even if work occurred. They are outside this successful matched sample.
- An invocation artifact and completed turn are observable locally. Exact billable backend requests and internal retries are not established by those counts.
- Relay traffic can increase embeddings and later-stage work. A total cost, quality or latency comparison needs a comparable workload and measured downstream results.

## Read-only recipe for a later production capture

Use a naturally arriving, previously uncached original with a valid source identity and at most 16,000 characters of actual classifier text. Do not modify originals, clear caches, reset consumer offsets or inject a fabricated source to obtain a cache miss. A pass or rejection is valid evidence; do not force the verdict.

1. Capture the deployed gateway health/model and the set of existing `codex-runs/*/run.json` paths before the observation. Keep the full existing cache and consumer groups.
2. Query the specific durable routing row, using its database ID. Record the stage, input/output Kafka coordinates, source identity, original hash, classifier cache key, cache status, classification mode and model. For example, substitute an integer routing row ID in this read-only query:

   ```sql
   SELECT id, event_id, stage, outcome, input_topic, input_partition,
          input_offset, source, source_id, raw_sha256, classifier,
          started_at, completed_at
   FROM routing_events
   WHERE id = <ROUTING_ROW_ID>;
   ```

3. Correlate the relay's acknowledged output coordinates with Fast's input coordinates and check the same original hash. A zero-model relay must be labelled as routing policy with `classified_by_model=false`; it must not be described as a Luna judgment. The separate relay stage preserves the identity of historical filter decisions.
4. For Fast's exact `classifier.cache_key`, run the following from the repository root, replacing only the placeholder. This opens SQLite read-only, bounds artifact reads, and emits only selected metadata and hashes. It neither imports nor instantiates the gateway.

   ```sh
   python3 - '<FAST_CACHE_KEY>' <<'PY'
   import hashlib
   import json
   import re
   import sqlite3
   import sys
   from collections import Counter
   from datetime import datetime, timezone
   from pathlib import Path

   key = sys.argv[1]
   if not re.fullmatch(r"[0-9a-f]{64}", key):
       raise SystemExit("Expected the exact 64-character Fast cache key")
   runtime = (Path.cwd() / "environments/attention_judgment/runs/local-pipeline").resolve()
   database = runtime / "gateway-cache.sqlite3"
   connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
   connection.execute("PRAGMA query_only=ON")
   row = connection.execute(
       "SELECT request_json,response_json,artifact_path,created_at "
       "FROM cache WHERE kind='classify' AND key=?", (key,),
   ).fetchone()
   connection.close()
   if row is None:
       raise SystemExit("No successful cached classification for this key")
   request, response = json.loads(row[0]), json.loads(row[1])
   canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
   result = {
       "observed_at": datetime.now(timezone.utc).isoformat(),
       "cache_key": key,
       "cache_created_at": row[3],
       "role": request.get("role"),
       "message_characters": len(request.get("message", "")),
       "request_sha256": hashlib.sha256(canonical(request).encode()).hexdigest(),
       "source_identity_sha256": hashlib.sha256(canonical({
           "source": request.get("source"), "source_id": request.get("source_id")
       }).encode()).hexdigest(),
       "classification_mode": response.get("classification_mode"),
       "reported_model": response.get("model"),
       "verdict": response.get("verdict"),
   }
   if row[2] is None:
       result["artifact_status"] = "aggregate_or_non_leaf; not one model invocation"
       print(json.dumps(result, indent=2))
       raise SystemExit
   artifact = Path(row[2]).resolve()
   artifact.relative_to((runtime / "codex-runs").resolve())
   run_path, events_path = artifact / "run.json", artifact / "events.jsonl"
   if run_path.stat().st_size > 1024 * 1024 or events_path.stat().st_size > 32 * 1024 * 1024:
       raise SystemExit("Artifact exceeds the bounded metadata inspection budget")
   metadata = json.loads(run_path.read_text())
   event_types, tool_items = Counter(), 0
   with events_path.open() as stream:
       for line in stream:
           if not line.strip():
               continue
           event = json.loads(line)
           event_types[event.get("type")] += 1
           item = event.get("item")
           if isinstance(item, dict) and item.get("type") in {
               "command_execution", "mcp_tool_call", "web_search", "file_change", "tool_call"
           }:
               tool_items += 1
   allowed_usage = {"input_tokens", "cached_input_tokens", "cache_write_input_tokens",
                    "output_tokens", "reasoning_output_tokens"}
   result.update({
       "artifact_name": artifact.name,
       "run_model": metadata.get("model"),
       "run_success": metadata.get("success"),
       "returncode": metadata.get("returncode"),
       "wall_seconds": metadata.get("wall_seconds"),
       "disable_tools": metadata.get("disable_tools"),
       "completed_turns": event_types["turn.completed"],
       "failed_turns": event_types["turn.failed"],
       "tool_items": tool_items,
       "reported_usage": {k: v for k, v in (metadata.get("usage") or {}).items()
                          if k in allowed_usage and type(v) is int and v >= 0},
   })
   print(json.dumps(result, indent=2))
   PY
   ```

5. Check that the linked artifact is new relative to the pre-capture set, `run_model` is the intended Luna model, `run_success` is true, `completed_turns` is one and `tool_items` is zero. Check the original routing event's `cached=false` and short-source `classification_mode=model`; the cached response itself does not record whether a later HTTP request was a hit.
6. Inspect all new artifacts in the window, including failed ones. A successful cache link alone proves one identified successful invocation, not absence of earlier failed attempts. Concurrent unrelated traffic makes a directory-count delta insufficient. Failed artifacts lack a cache link; attribution may require local inspection of their private prompts, which must not be copied into validation artifacts.
7. Preserve evidence separately for cache replay and failures. An identical successful classification retry should create no new model artifact. A failed output/audit acknowledgement must leave the input uncommitted; test that with isolated test doubles, without publishing synthetic production records.

For short uncached records without failures, the intended acceptance budget is zero relay invocations plus one Fast invocation. Cache hits need zero fresh Fast invocations. Longer sources have a separate budget: 16,001–128,000 characters use overlapping chunks with up to nine evaluated positions and early-positive termination; above 128,000 characters use explicit full-review routing without a classifier call. A chunk aggregate with `cached=false` is not evidence of exactly one fresh invocation.

Mid and Strong use independent agent runs and can contain multiple turns. Verify their actual run metadata separately; their costs are not part of this filter/Fast baseline.
