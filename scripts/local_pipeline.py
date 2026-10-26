#!/usr/bin/env python3
"""Supervise the real local pipeline using the host's existing Codex login.

Docker runs Kafka/Flink/workers/Postgres/UI. This process supervises the model
gateway, primary-source ingestor and independent Mid/Strong schedules. Runtime
state is ignored by Git. No credential files are read or copied.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))
sys.path.insert(0, str(ROOT / "scripts"))
from codex_runner import CYCLE_SCHEMA, cycle_prompt, run
from cycle_audit import safe_text, terminal_summary, tool_events, write_event
from trace_projection import publish_projection
import psycopg2
from dotenv import load_dotenv

RUNTIME = ROOT / "environments/attention_judgment/runs/local-pipeline"
PYTHON = ROOT / ".venv/bin/python"
CODEX = ROOT / "environments/attention_judgment/runs/runtime/node_modules/.bin/codex"
SERVICES = ["kafka1", "kafka2", "kafka3", "postgres", "otel-collector", "jaeger",
            "flink-jobmanager", "flink-taskmanager", "flink-job", "sink-consumer",
            "local-ml-filter", "embed-worker", "fast-agent", "twitter-ingestor",
            "backend-fastapi", "frontend-react"]


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")
    temporary.replace(path)


def environment():
    load_dotenv(ROOT / ".env", override=False)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    token_path = RUNTIME / "gateway.token"
    if not token_path.exists():
        with token_path.open("x") as stream:
            os.chmod(token_path, 0o600)
            stream.write(secrets.token_urlsafe(32))
    env = os.environ.copy()
    env.update({"ATTENTION_GATEWAY_TOKEN": token_path.read_text().strip(),
                "DB_HOST": "127.0.0.1", "DB_PORT": "5432", "DB_NAME": "bluesky",
                "DB_USER": "bluesky", "DB_PASSWORD": "bluesky",
                "KAFKA_BOOTSTRAP": "localhost:9092,localhost:9093,localhost:9094",
                "EMBEDDING_PROVIDER": "http",
                "EMBEDDING_MODEL": "sentence-transformers/all-mpnet-base-v2",
                "EMBEDDING_GATEWAY_URL": "http://127.0.0.1:8765/embed",
                "PATH": str(ROOT / ".venv/bin") + os.pathsep + env.get("PATH", ""),
                "PYTHONUNBUFFERED": "1"})
    return env


def connect():
    return psycopg2.connect(host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=os.environ.get("DB_PORT", "5432"), dbname=os.environ.get("DB_NAME", "bluesky"),
        user=os.environ.get("DB_USER", "bluesky"), password=os.environ.get("DB_PASSWORD", "bluesky"),
        connect_timeout=10)


@contextmanager
def lock(name):
    RUNTIME.mkdir(parents=True, exist_ok=True)
    with (RUNTIME / f"{name}.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def signature(role):
    table = "attention_tickets" if role == "mid" else "annotations"
    with connect() as conn, conn.cursor() as cur:
        # Each payload revision is independent work. A whole-table fingerprint
        # cannot acknowledge a bounded batch without losing its remainder.
        cur.execute(f"SELECT id, md5(payload::text) FROM {table} ORDER BY id")
        return {str(row[0]): row[1] for row in cur.fetchall()}


def read_agent_state(state_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, updated_at FROM agent_state WHERE agent_id=%s", (state_id,))
        return cur.fetchone()


def aware_utc(value, field):
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be an ISO timestamp with an explicit timezone")
    return parsed.astimezone(timezone.utc)


def thesis_records(state):
    """Read the Strong map and the legacy thesis_id list without dropping IDs."""
    if not isinstance(state, dict):
        raise ValueError("Strong agent state must be an object")
    theses = state.get("theses", {})
    if isinstance(theses, dict):
        items = theses.items()
    elif isinstance(theses, list):
        items = ((record.get("thesis_id") if isinstance(record, dict) else None, record)
                 for record in theses)
    else:
        raise ValueError("Strong theses must be an object or a list of thesis_id records")
    result = {}
    for thesis_id, thesis in items:
        if not isinstance(thesis_id, str) or not thesis_id.strip() or thesis_id in result or not isinstance(thesis, dict):
            raise ValueError("Strong theses require unique nonempty IDs and object records")
        result[thesis_id] = thesis
    return result


def due_thesis_reviews(state, cutoff):
    due = {}
    for thesis_id, thesis in thesis_records(state).items():
        if "next_review_at" not in thesis:
            continue  # Older research state did not register individual deadlines.
        deadline = thesis["next_review_at"]
        if aware_utc(deadline, f"theses[{thesis_id}].next_review_at") <= cutoff:
            due[thesis_id] = deadline
    return due


def verify_thesis_reviews(state, due, started, checked_at):
    theses = thesis_records(state)
    verified = {}
    for thesis_id, deadline in due.items():
        thesis = theses.get(thesis_id, {})
        review = thesis.get("scheduled_review")
        if not isinstance(review, dict) or review.get("due_at") != deadline:
            raise RuntimeError(f"Strong thesis {thesis_id} needs a saved scheduled_review for the captured deadline")
        reviewed_at = aware_utc(review.get("reviewed_at"), f"theses[{thesis_id}].scheduled_review.reviewed_at")
        outcome = review.get("outcome")
        if (not started <= reviewed_at <= checked_at or not isinstance(outcome, str) or not outcome.strip()):
            raise RuntimeError(f"Strong thesis {thesis_id} scheduled_review must have a fresh timestamp and a nonempty outcome")
        if aware_utc(thesis.get("next_review_at"), f"theses[{thesis_id}].next_review_at") <= checked_at:
            raise RuntimeError(f"Strong thesis {thesis_id} next_review_at must be in the future after review")
        verified[thesis_id] = {"due_at": deadline, "reviewed_at": review["reviewed_at"],
                               "outcome": safe_text(outcome), "next_review_at": thesis["next_review_at"]}
    return verified


def cycle(role, *, force=False):
    with lock(role):
        status_path = RUNTIME / f"{role}.json"
        previous = json.loads(status_path.read_text()) if status_path.exists() else {}
        attempt = {"cycle_id": f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{role}-{uuid.uuid4().hex[:8]}",
                   "started_at": now(), "role": role}
        try:
            result = _cycle_locked(role, force, previous, status_path, attempt)
            if result["status"] != "waiting_for_new_evidence":
                write_event(RUNTIME / "cycle-audit", attempt["cycle_id"], "completed",
                            {**terminal_summary(result), "daily_review": result["daily_review"],
                             "forced": force, "due_thesis_reviews": result["due_thesis_reviews"],
                             "verified_thesis_reviews": result["verified_thesis_reviews"]})
            return result
        except Exception as exc:
            write_event(RUNTIME / "cycle-audit", attempt["cycle_id"], "failed",
                        {"role": role, "status": "failed", "failed_at": now(),
                         "started_at": attempt["started_at"],
                         "error_type": type(exc).__name__, "summary": safe_text(exc),
                         "due_thesis_reviews": attempt.get("due_thesis_reviews", {}),
                         **tool_events(attempt.get("output_dir"))})
            # Preserve the last successful acknowledgments; a failed attempt
            # must remain visible and retry the same pending versions.
            save(status_path, {**previous, "role": role, "status": "failed",
                               "pending_thesis_reviews": attempt.get("due_thesis_reviews", previous.get("pending_thesis_reviews", {})),
                               "failed_at": now(), "error": f"{type(exc).__name__}: {exc}"})
            raise


def _cycle_locked(role, force, previous, status_path, attempt):
    model = (os.environ.get(f"{role.upper()}_CODEX_MODEL")
             or os.environ.get("CODEX_MODEL")
             or ("gpt-6-luna" if role == "mid" else "gpt-6.1-sol"))
    versions = signature(role)
    processed = dict(previous.get("processed_versions", {}))
    pending = sorted((int(key) for key, value in versions.items()
                      if processed.get(key) != value))
    selected = pending[:20]
    last_review = previous.get("last_review_epoch", previous.get("completed_epoch", 0))
    daily_review = role == "strong" and bool(last_review) and time.time() - last_review >= 86400
    state_id = os.environ.get(f"{role.upper()}_AGENT_ID", f"claude-{role}")
    due = {}
    if role == "strong":
        # This canonical read runs under the same role lock as the model and
        # acknowledgment. Individual deadlines are independent of the daily timer.
        before = read_agent_state(state_id)
        due = due_thesis_reviews(before[0] if before else {}, aware_utc(attempt["started_at"], "started_at"))
        # A failed save can move a DB deadline without satisfying its review
        # contract. Retain the captured obligation until this scheduler verifies it.
        unacknowledged = previous.get("pending_thesis_reviews", {})
        if not isinstance(unacknowledged, dict):
            raise ValueError("pending_thesis_reviews must be an object")
        for thesis_id, deadline in unacknowledged.items():
            if not isinstance(thesis_id, str) or not thesis_id.strip():
                raise ValueError("pending_thesis_reviews requires nonempty thesis IDs")
            aware_utc(deadline, f"pending_thesis_reviews[{thesis_id}]")
            due[thesis_id] = deadline
    attempt["due_thesis_reviews"] = due
    if not force and not pending and not daily_review and not due:
        return {"role": role, "status": "waiting_for_new_evidence", "input_count": len(versions),
                "pending_count": 0}
    workspace = RUNTIME / "agents" / role
    workspace.mkdir(parents=True, exist_ok=True)
    instructions = (ROOT / "agent/AGENTS.md").read_text()
    instructions = instructions.replace("/app/skills", str(ROOT / "skills"))
    instructions = instructions.replace("python ", str(PYTHON) + " ")
    (workspace / "AGENTS.md").write_text(instructions)
    prompt = cycle_prompt(role, state_id).replace("/app/AGENTS.md", str(workspace / "AGENTS.md"))
    table = "attention_tickets" if role == "mid" else "annotations"
    write_event(RUNTIME / "cycle-audit", attempt["cycle_id"], "started",
                {"role": role, "status": "running", "started_at": attempt["started_at"],
                 "input_table": table, "selected_input_ids": selected,
                 "selected_input_versions": {str(key): versions[str(key)] for key in selected},
                 "version_encoding": "MD5 of PostgreSQL payload::text for scheduler acknowledgment",
                 "daily_review": daily_review, "forced": force,
                 "due_thesis_reviews": due,
                 "model": model, "environment_profile": "research"})
    review_field = "reviewed_ticket_ids" if role == "mid" else "reviewed_annotation_ids"
    prompt += (f"\nUse {PYTHON} for every Python skill. Current UTC time is {now()}. "
               f"The scheduler selected these oldest pending {table} IDs: {json.dumps(selected)}. "
               f"Inspect these exact IDs rather than querying only the latest rows; return only actually "
               f"handled selected IDs in {review_field}. Leave that list empty if no IDs were selected. "
               "Other rows may be read as historical context but are not acknowledgments for this batch. "
               "Record handled IDs in durable state, and keep the remainder pending. If the saved agent "
               "state already handled a selected ID, verify the current payload version before acknowledging "
               "it without duplicate publication. An edited payload is new evidence even when its ID is old. "
               f"Daily thesis review due: {daily_review}. On a due review, inspect pending thesis review "
               "dates, invalidation conditions and evidence gaps even if no new annotations arrived; "
               "record unresolved questions and the next review date. "
               "If a source contains dated information, preserve its original date and distinguish it "
               "from today's ingestion date. A price observation is not a forecast. Register numeric "
               "probabilities only if your evidence supports one.")
    if role == "strong":
        prompt += (f"\nForced continuing research review: {force}. "
                   f"Captured due thesis IDs and exact deadlines: {json.dumps(due)}. "
                   "Daily reviews, captured thesis deadlines and an explicit forced review independently "
                   "require continuing research, even without new annotations. Read the actual originals "
                   "behind each thesis, check available supporting and contradicting evidence, invalidation "
                   "conditions and unresolved questions. Actively address the stored evidence gaps with the "
                   "available research tools. If retrieval fails, record the actual tool outcome and the "
                   "remaining gap; unchanged annotation hashes alone do not justify moving review dates. "
                   "Preserve thesis identities, questions and evidence "
                   "when evidence remains insufficient; record what was checked and what remains unresolved. "
                   "Do not turn a research review into an investment decision without adequate evidence. "
                   "For EACH captured due thesis, save scheduled_review inside that thesis with due_at equal "
                   "to its exact captured deadline, reviewed_at equal to the actual review time with an explicit "
                   f"timezone (at or after cycle start {attempt['started_at']}), and outcome describing the "
                   "originals checked, findings and unresolved gaps. Also save a future timezone-aware "
                   "next_review_at. The scheduler will read these records back before acknowledging this cycle; "
                   "a fresh row timestamp or a claim of completion alone does not acknowledge a due review.")
    output = RUNTIME / "cycles" / f"{role}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    attempt["output_dir"] = output
    started = datetime.now(timezone.utc)
    result = run(prompt, CYCLE_SCHEMA, workspace, output,
                 float(os.environ.get("CODEX_TIMEOUT_SECONDS", "900")),
                 model,
                 codex_bin=os.environ.get("CODEX_BIN", str(CODEX)), network_access=True,
                 environment_profile="research")
    row = read_agent_state(state_id)
    final = result["final"]
    state_saved = bool(row and row[1] >= started and final["state_saved"])
    if final["role"] != role or final["status"] == "blocked" or not state_saved:
        raise RuntimeError(f"{role} cycle incomplete; state_saved={state_saved}; artifacts={output}")
    verified_reviews = verify_thesis_reviews(
        row[0], due, aware_utc(attempt["started_at"], "started_at"), aware_utc(now(), "checked_at")
    ) if due else {}
    reviewed = final[review_field]
    if (any(type(value) is not int for value in reviewed) or len(set(reviewed)) != len(reviewed)
            or not set(reviewed) <= set(selected)):
        raise RuntimeError(f"{role} returned invalid or unselected reviewed IDs; artifacts={output}")
    if selected and not reviewed:
        raise RuntimeError(f"{role} did not handle any selected pending inputs; artifacts={output}")
    if role == "mid" and reviewed:
        handled = row[0].get("handled_ticket_ids") if isinstance(row[0], dict) else None
        if not isinstance(handled, list) or any(type(value) is not int or value <= 0 for value in handled):
            raise RuntimeError(f"mid saved handled_ticket_ids must be a list of positive integer IDs; artifacts={output}")
        missing = sorted(set(reviewed) - set(handled))
        if missing:
            raise RuntimeError(f"mid saved handled_ticket_ids is missing reviewed ticket IDs {missing}; artifacts={output}")
    for row_id in reviewed:
        processed[str(row_id)] = versions[str(row_id)]
    completed = time.time()
    record = {"cycle_id": attempt["cycle_id"], "role": role, "status": final["status"], "processed_versions": processed,
              "completed_at": now(), "completed_epoch": completed,
              "last_review_epoch": completed if role == "strong" and (daily_review or not last_review) else last_review,
              "selected_input_ids": selected, "pending_count": len(pending) - len(reviewed),
              "due_thesis_reviews": due, "verified_thesis_reviews": verified_reviews,
              "pending_thesis_reviews": {},
              "daily_review": daily_review, "final": final, "usage": result["usage"],
              "output_dir": str(output), "state_verified": True}
    save(status_path, record)
    return record


def bridge():
    script = ROOT / "environments/attention_judgment/pipeline.py"
    output = []
    for action in ("sync", "settle", "report"):
        result = subprocess.run([str(PYTHON), str(script), action], cwd=ROOT,
                                capture_output=True, text=True, timeout=240)
        output.append({"action": action, "returncode": result.returncode,
                       "stdout": result.stdout[-12000:], "stderr": result.stderr[-2000:]})
        if result.returncode:
            raise RuntimeError(f"Production {action} failed: {result.stderr[-1000:]}")
    production = ROOT / "environments/attention_judgment/runs/production"
    projection = publish_projection(production / "ledger.sqlite3", production / "trace-projection.sqlite3")
    save(RUNTIME / "evaluation.json", {"updated_at": now(), "results": output,
                                      "archive_projection": projection})


def daemon():
    with lock("supervisor"):
        save(RUNTIME / "supervisor.json", {"pid": os.getpid(), "started_at": now()})
        children = {}
        stop = False

        def stopping(*_):
            nonlocal stop
            stop = True

        signal.signal(signal.SIGTERM, stopping)
        signal.signal(signal.SIGINT, stopping)
        specs = {
            "gateway": [str(PYTHON), str(ROOT / "scripts/local_gateway.py"),
                        "--host", "0.0.0.0", "--port", "8765", "--codex-bin", str(CODEX),
                        "--data-dir", str(RUNTIME), "--timeout", "180", "--prewarm"],
            "rss": [str(PYTHON), str(ROOT / "scripts/ingest_primary_rss.py")],
            "observability": [str(PYTHON), str(ROOT / "scripts/observability_exporter.py")],
        }
        next_run = {"mid": 0.0, "strong": 0.0, "evaluation": 0.0}
        last_start = {}
        active = {}
        health_path = RUNTIME / "health.json"
        prior_health = json.loads(health_path.read_text()) if health_path.exists() else {}
        errors = dict(prior_health.get("errors", {}))
        try:
            while not stop:
                for name, command in specs.items():
                    if name not in children or children[name].poll() is not None:
                        if time.time() - last_start.get(name, 0) >= 30:
                            with (RUNTIME / f"{name}.log").open("a") as log:
                                children[name] = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log)
                            last_start[name] = time.time()
                for role, interval in (("mid", 1800), ("strong", 2400)):
                    if role in active:
                        if active[role].poll() is None:
                            continue
                        if active[role].returncode:
                            errors[role] = f"Cycle exit {active[role].returncode}; see {role}.log"
                        else:
                            errors.pop(role, None)
                        del active[role]
                    if time.time() >= next_run[role]:
                        with (RUNTIME / f"{role}.log").open("a") as log:
                            active[role] = subprocess.Popen([str(PYTHON), str(Path(__file__).resolve()),
                                                            "cycle", role], cwd=ROOT, stdout=log, stderr=log)
                        next_run[role] = time.time() + interval
                if time.time() >= next_run["evaluation"]:
                    try:
                        bridge()
                        errors.pop("evaluation", None)
                    except Exception as exc:
                        errors["evaluation"] = str(exc)
                    next_run["evaluation"] = time.time() + 300
                save(RUNTIME / "health.json", {"updated_at": now(), "pid": os.getpid(),
                     "children": {n: {"pid": p.pid, "running": p.poll() is None} for n, p in children.items()},
                     "active_cycles": {n: p.pid for n, p in active.items()}, "errors": errors})
                time.sleep(5)
        finally:
            for process in [*children.values(), *active.values()]:
                if process.poll() is None:
                    process.terminate()
            for process in [*children.values(), *active.values()]:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "serve", "status", "cycle", "evaluate"))
    parser.add_argument("role", nargs="?", choices=("mid", "strong", "both"), default="both")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    env = environment()
    os.environ.update(env)
    if args.action == "start":
        compose_env = env.copy()
        compose_env["EMBEDDING_GATEWAY_URL"] = "http://host.docker.internal:8765/embed"
        subprocess.run(["docker", "compose", "up", "-d", *SERVICES], cwd=ROOT, env=compose_env, check=True)
        try:
            with lock("supervisor"):
                with (RUNTIME / "supervisor.log").open("a") as log:
                    subprocess.Popen([str(PYTHON), str(Path(__file__).resolve()), "serve"],
                                     cwd=ROOT, env=env, stdout=log, stderr=log, start_new_session=True)
        except BlockingIOError:
            print("Supervisor is already running")
        print(f"Runtime status: {RUNTIME / 'health.json'}")
    elif args.action == "serve":
        daemon()
    elif args.action == "cycle":
        for role in (["mid", "strong"] if args.role == "both" else [args.role]):
            try:
                result = cycle(role, force=args.force)
            except BlockingIOError:
                result = {"role": role, "status": "already_running"}
            print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
    elif args.action == "evaluate":
        bridge()
    else:
        for name in ("health", "mid", "strong", "evaluation"):
            path = RUNTIME / f"{name}.json"
            print(json.dumps({name: json.loads(path.read_text()) if path.exists() else "not_started"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
