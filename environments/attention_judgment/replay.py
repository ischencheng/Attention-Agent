#!/usr/bin/env python3
"""Run a controlled SYNTHETIC longitudinal judgment task through the real adapter.

This is an evidence/memory behavior test, not investment or price-performance
evidence. Each step starts a separate run.py process. The no-memory control
receives the same incremental input files but starts with a fresh ledger.

Example from the repository root:
  .venv/bin/python environments/attention_judgment/replay.py \
      --run-root /tmp/attention-synthetic --codex-bin /path/to/codex
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

from ledger import Ledger, digest, timestamp
from sources import observation

HERE = Path(__file__).resolve().parent
SCENARIO_ID = "synthetic-capacity-customer-correction-v1"


def scenario():
    """Fixed public facts and cutoffs; no real issuer, ticker, or stock price."""
    def release(key, published, available, text, event=None, source="fictional-company-official"):
        return observation(
            source=source, source_id=f"synthetic://{SCENARIO_ID}/{key}",
            published_at=published, available_at=available,
            event_id=event or f"{SCENARIO_ID}:{key}",
            raw={"synthetic": True, "company": "岚屿封装实验公司 / Lanyu Packaging Lab",
                 "listed": False, "title": f"[SYNTHETIC] {key}", "text": text,
                 "scope": "Fictitious semiconductor packaging and datacenter capacity; no traded security."},
        )

    initial = release("capacity-customer-commitment", "2025-06-02T12:00:00Z", "2025-06-02T12:05:00Z",
        "合成事实：岚屿封装实验公司公布，先进封装产线计划于第四季度投产，年产能计划由10万提高到18万单位。"
        "虚构客户云岬数据中心签署有约束力的最低采购合同，覆盖新增产能的70%，期限两年。"
        "投产仍需通过客户验收；公司未披露合同价格、退出条款、单位成本及融资成本。"
        "这是公司原始披露，尚无独立验证；产能计划和采购承诺不等同于实际收入或护城河。")
    duplicate = release("syndicated-capacity-story", "2025-06-03T12:00:00Z", "2025-06-03T12:02:00Z",
        "合成转载：本稿全文事实均转引岚屿6月2日公告。年产能计划由10万提高到18万单位，"
        "云岬最低采购承诺覆盖新增产能70%，期限两年。没有采访、独立核实或新增经营数据。",
        event=initial["event_id"], source="fictional-syndicated-news")
    cancellation = release("contract-cancellation", "2025-06-04T12:00:00Z", "2025-06-04T12:03:00Z",
        "合成官方更新：岚屿公告称，云岬已永久取消覆盖新增产能70%的最低采购合同，原先承诺不再有效。"
        "公司已投入预计扩产资本支出的60%，尚无替代客户承诺；产线投产验收时间仍待确定。"
        "该更新直接冲突于6月2日的客户承诺事实。未披露取消赔偿、可收回资本支出或其他客户需求。")
    correction = release("correction-temporary-delay", "2025-06-05T12:00:00Z", "2025-06-05T12:04:00Z",
        "合成官方更正：岚屿承认6月4日公告将‘暂停验收’误写成‘永久取消合同’。"
        "附双方签署的更正摘要：两年最低采购合同仍有效，覆盖新增产能70%；客户验收推迟90天，"
        "并非无条件立即采购。客户未支付新的预付款，产线尚未通过验收，60%资本支出已投入。"
        "更正解决‘永久取消’这一说法，但暂时延迟、现金回收与公司披露可靠性仍待调查。"
        "保留并明确关联此前错误公告，不应将此前相反证据从历史中删除。")
    future = release("future-customer-acceptance", "2025-06-06T12:00:00Z", "2025-06-06T12:04:00Z",
        "合成未来材料：客户终于完成验收。该材料晚于所有回放截止时间，不能用于任何一步判断。")
    delayed = release("late-delivered-financing-detail", "2025-06-05T11:00:00Z", "2025-06-07T12:00:00Z",
        "合成延迟送达材料：公司融资成本披露。虽然发布时间早于最后一步，但系统当时尚未收到，不能用于判断。")
    records = {"initial": initial, "duplicate": duplicate, "cancellation": cancellation,
               "correction": correction, "future": future, "delayed": delayed}
    steps = [
        {"name": "01-discovery", "as_of": "2025-06-02T18:00:00Z", "records": ["initial"]},
        {"name": "02-syndicated-duplicate", "as_of": "2025-06-03T18:00:00Z", "records": ["duplicate"]},
        {"name": "03-material-contradiction", "as_of": "2025-06-04T18:00:00Z", "records": ["cancellation"]},
        {"name": "04-correction", "as_of": "2025-06-05T18:00:00Z", "records": ["correction", "future", "delayed"]},
    ]
    return {"scenario_id": SCENARIO_ID, "kind": "synthetic", "records": records, "steps": steps}


def _write_once(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text() != text:
            raise ValueError(f"refusing to overwrite different replay artifact: {path}")
    else:
        with path.open("x") as stream:
            stream.write(text)


def _json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"


def ledger_path(run_root, memory, step):
    return run_root / "ledger.sqlite3" if memory == "versioned" else run_root / "steps" / step["name"] / "ledger.sqlite3"


def prepare(run_root, memory="versioned", model=None, codex_bin="codex", timeout=240):
    run_root = Path(run_root).resolve()
    case = scenario()
    manifest = {**case, "memory": memory, "requested_model": model, "codex_bin": codex_bin, "timeout": timeout,
                "input_protocol": "same incremental release files; prior ledger retained only in versioned condition",
                "grader_contract": "evidence identity, duplicate handling, revision continuity and temporal visibility; no directional or language-truth oracle"}
    for step in manifest["steps"]:
        content = "".join(json.dumps(case["records"][key], ensure_ascii=False) + "\n" for key in step["records"])
        step["input"] = f"inputs/{step['name']}.jsonl"
        step["input_sha256"] = digest(content)
        _write_once(run_root / step["input"], content)
    _write_once(run_root / "manifest.json", _json(manifest))
    return manifest


def _refs(entry):
    item = entry["payload"]["discovery"]
    return set(item["evidence_ids"] + item["counterevidence_ids"])


def grade(run_root, manifest):
    """Grade persisted evidence graphs, never generated prose sentiment."""
    run_root = Path(run_root)
    checks = []
    def check(name, passed, detail):
        checks.append({"id": name, "passed": bool(passed), "detail": detail})

    judgments_by_step = []
    all_entries = {}
    all_observations = {}
    cycles_by_step = []
    ledger_hashes = {}
    errors = []
    for step in manifest["steps"]:
        path = ledger_path(run_root, manifest["memory"], step)
        execution_path = run_root / "steps" / step["name"] / "execution.json"
        execution = json.loads(execution_path.read_text()) if execution_path.exists() else {}
        entries = []
        if path.exists():
            try:
                ledger = Ledger(path)
                try:
                    ledger_hashes[str(path.relative_to(run_root))] = ledger.verify()
                    entries = ledger.entries()
                finally:
                    ledger.close()
            except (ValueError, RuntimeError, OSError) as exc:
                errors.append(f"{step['name']}: {type(exc).__name__}: {exc}")
        else:
            errors.append(f"{step['name']}: ledger missing")
        for entry in entries:
            if entry["kind"] == "judgment":
                all_entries[entry["hash"]] = entry
            elif entry["kind"] == "observation":
                all_observations[entry["key"]] = entry["payload"]
        def from_step(entry):
            p = entry["payload"]
            return p.get("mode") == "replay" and timestamp(p["as_of"]) == timestamp(step["as_of"])
        judgments_by_step.append([e for e in entries if e["kind"] == "judgment" and from_step(e)])
        cycles = [e for e in entries if e["kind"] == "cycle" and from_step(e)]
        cycles_by_step.append(cycles)
        check(f"{step['name']}:completed", execution.get("returncode") == 0 and len(cycles) == 1,
              {"returncode": execution.get("returncode"), "completed_cycles": len(cycles)})

    check("ledger_integrity", not errors, errors)
    expected = manifest["records"]
    ids = {key: value["id"] for key, value in expected.items()}
    initial, duplicate, contradictory, corrected = judgments_by_step
    anchors = {e["payload"]["discovery"]["thesis_id"]: e for e in initial if ids["initial"] in _refs(e)}
    check("initial_material_discovery", bool(anchors), {"thesis_ids": sorted(anchors)})
    check("duplicate_does_not_create_revision", not duplicate and bool(cycles_by_step[1]),
          {"judgments_at_duplicate_step": len(duplicate)})
    addressed = {e["payload"]["discovery"]["thesis_id"]: e for e in contradictory
                 if e["payload"]["discovery"]["thesis_id"] in anchors and ids["cancellation"] in _refs(e)}
    check("contradiction_updates_existing_thesis", bool(addressed), {"thesis_ids": sorted(addressed)})
    retained = {e["payload"]["discovery"]["thesis_id"]: e for e in corrected
                if e["payload"]["discovery"]["thesis_id"] in addressed
                and {ids["cancellation"], ids["correction"]} <= _refs(e)}
    check("correction_keeps_conflicting_evidence", bool(retained), {"thesis_ids": sorted(retained)})
    linked = []
    for key, final in retained.items():
        middle, first = addressed[key], anchors[key]
        if (middle["payload"]["previous_judgment_hash"] == first["hash"]
                and final["payload"]["previous_judgment_hash"] == middle["hash"]
                and [first["payload"]["version"], middle["payload"]["version"], final["payload"]["version"]] == [1, 2, 3]):
            linked.append(key)
    check("versioned_history_survives_process_restarts", bool(linked), {"thesis_ids": sorted(linked)})
    visibility_errors = []
    source_errors = []
    event_errors = []
    for entry in all_entries.values():
        payload, refs = entry["payload"], _refs(entry)
        observed = payload["discovery"]["observed_event_ids"]
        available_events = set()
        for ref in refs:
            original = all_observations.get(ref)
            if original is None or ref not in {o["id"] for o in expected.values()}:
                source_errors.append({"judgment": entry["hash"], "ref": ref})
                continue
            available_events.add(original["event_id"])
            if (timestamp(original["available_at"]) > timestamp(payload["as_of"])
                    or timestamp(original["published_at"]) > timestamp(payload["as_of"])):
                visibility_errors.append({"judgment": entry["hash"], "ref": ref})
        if set(observed) != available_events or len(observed) != len(set(observed)):
            event_errors.append(entry["hash"])
    check("future_and_delayed_evidence_never_cited", not visibility_errors, visibility_errors)
    check("only_supplied_originals_cited", not source_errors, source_errors)
    check("source_events_deduplicated", not event_errors, event_errors)
    invented_securities = [e["hash"] for e in all_entries.values() if
                          e["payload"]["discovery"]["forecasts"] or any(
                              entity["symbol"] is not None for entity in e["payload"]["discovery"]["entities"])]
    check("no_invented_security_or_price_forecast", not invented_securities, invented_securities)
    result = {"scenario_id": manifest["scenario_id"], "kind": "synthetic", "memory": manifest["memory"],
              "passed": all(item["passed"] for item in checks),
              "checks_passed": sum(item["passed"] for item in checks), "checks_total": len(checks),
              "checks": checks, "ledger_hashes": ledger_hashes,
              "judgment_count": len(all_entries), "directional_answer_graded": False,
              "fundamental_correctness": "not graded by these structural checks",
              "limitations": ["one synthetic trajectory, not market performance or demonstrated investment skill",
                              "no-memory control intentionally lacks the state required for continuity checks",
                              "citations and version links do not prove that prose correctly explains the temporary-delay correction",
                              "model training, historical contamination resistance and generalization are not measured"]}
    _write_once(run_root / "grade.json", _json(result))
    return result


def run_replay(run_root, memory="versioned", model=None, codex_bin="codex", timeout=240):
    run_root = Path(run_root).resolve()
    manifest = prepare(run_root, memory, model, codex_bin, timeout)
    if (run_root / "started.json").exists() or (run_root / "grade.json").exists():
        raise ValueError("replay already started; choose a new run root to preserve the original attempt")
    _write_once(run_root / "started.json", _json({"started_at": datetime.now(timezone.utc).isoformat(), "python": sys.executable}))
    for step in manifest["steps"]:
        directory = run_root / "steps" / step["name"]
        directory.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, str(HERE / "run.py"), "cycle", "--ledger", str(ledger_path(run_root, memory, step)),
                   "--run-root", str(directory / "attempts"), "--input", str(run_root / step["input"]),
                   "--as-of", step["as_of"], "--codex-bin", str(codex_bin), "--timeout", str(timeout)]
        if model is not None:
            command.extend(["--model", model])
        started = datetime.now(timezone.utc).isoformat()
        try:
            completed = subprocess.run(command, cwd=HERE, capture_output=True, text=True, timeout=timeout + 30, check=False)
            stdout, stderr, code = completed.stdout, completed.stderr, completed.returncode
            error = None
        except (subprocess.TimeoutExpired, OSError) as exc:
            stdout = getattr(exc, "stdout", "") or ""
            stderr = getattr(exc, "stderr", "") or ""
            stdout = stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout
            stderr = stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr
            code, error = None, f"{type(exc).__name__}: {exc}"
        _write_once(directory / "stdout.txt", stdout)
        _write_once(directory / "stderr.txt", stderr)
        _write_once(directory / "execution.json", _json({"command": command, "started_at": started,
                    "ended_at": datetime.now(timezone.utc).isoformat(), "returncode": code, "error": error}))
    return grade(run_root, manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True, help="new output directory, or an unchanged prepare-only directory")
    parser.add_argument("--memory", choices=["versioned", "none"], default="versioned")
    parser.add_argument("--model", default=None)
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--prepare-only", action="store_true", help="write reviewable fixtures and manifest; never invoke a model")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    options = {key: getattr(args, key) for key in ("run_root", "memory", "model", "codex_bin", "timeout")}
    if args.prepare_only:
        prepare(**options)
        print(_json({"status": "prepared", "kind": "synthetic", "manifest": str(args.run_root.resolve() / "manifest.json")}))
    else:
        result = run_replay(**options)
        print(_json({"kind": "synthetic", "passed": result["passed"], "checks_passed": result["checks_passed"],
                     "checks_total": result["checks_total"], "grade": str(args.run_root.resolve() / "grade.json")}))
        return 0 if result["passed"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
