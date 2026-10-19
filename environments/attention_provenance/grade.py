"""External outcome grader for the real Attention Proxy code paths."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
import tempfile
import time

import pgserver
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

from cases import CASES

HERE = Path(__file__).resolve().parent
BASELINE = "ed4d938ff89645fbeb1e58a10d66509425ede379"
TABLES = {"raw_messages", "attention_tickets", "annotations", "decision_packets"}


class World:
    def __init__(self, candidate, dsn, seed):
        self.candidate, self.dsn, self.seed = candidate, dsn, seed
        self.offset = random.Random(seed).randint(1000, 1000000)
        self.trace = []

    def sql(self, query, params=()):
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall() if cursor.description else []

    def insert(self, table, payload, raw=None):
        if table not in TABLES:
            raise ValueError(table)
        self.offset += random.Random(self.offset).randint(1, 19)
        if raw is None:
            raw = payload.get("raw", payload)
        query = sql.SQL("""INSERT INTO {} (kafka_topic, kafka_partition, kafka_offset, payload, raw)
            VALUES (%s, 0, %s, %s, %s) RETURNING id""").format(sql.Identifier(table))
        return self.sql(query, (f"fixture-{table}", self.offset, Json(payload), Json(raw)))[0][0]

    def call(self, op, args=None, **options):
        request = {"op": op, "args": args or {}, "candidate": str(self.candidate),
                   "dsn": self.dsn, **options}
        env = {key: value for key, value in os.environ.items()
               if key in {"PATH", "SYSTEMROOT", "TMPDIR", "LANG"}}
        start = time.monotonic()
        try:
            process = subprocess.run([sys.executable, "-I", str(HERE / "bridge.py")],
                                     input=json.dumps(request), text=True, capture_output=True,
                                     env=env, cwd=self.candidate, timeout=20)
            if process.returncode:
                result = {"ok": False, "error": process.stderr[-4000:] or f"exit {process.returncode}"}
            else:
                result = json.loads(process.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        self.trace.append({"op": op, "args": args or {}, "options": options,
                           "result": result, "wall_seconds": round(time.monotonic() - start, 4)})
        return result

    def value(self, op, args):
        response = self.call(op, args)
        assert response["ok"], response.get("error")
        return response["result"]

    def publish(self, payload):
        response = self.value("write", {"topic": "decision_packet", "payload": payload})
        assert response["exit_code"] in {None, 0}, response
        assert len(response["published"]) == 1, "A valid decision must publish exactly once"
        message = response["published"][0]
        assert message["topic"] == "decision_packet", message
        return self.insert("decision_packets", message["payload"]), message["payload"]

    def prepare(self, payload, outage=False):
        return self.call("prepare", {"topic": "decision_packet", "payload": payload}, outage=outage)

    def api(self, path):
        return self.value("api", {"path": path})

    def update(self, params):
        return self.value("update", params)

    def reapply_migrations(self):
        migrations = sorted((self.candidate / "postgres/init").glob("002_*.sql"))
        assert migrations, "Existing databases need an additive 002_*.sql migration"
        for _ in range(2):
            for migration in migrations:
                self.sql(migration.read_text())


def grade(candidate, seed, selected=None):
    candidate = candidate.resolve()
    if not (candidate / "skills/db_write.py").is_file():
        raise ValueError("--candidate must be an Attention-Agent checkout or exported task workspace")
    results = []
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="attention-eval-") as directory:
        pg = pgserver.get_server(Path(directory) / "postgres", cleanup_mode="stop")
        try:
            for index, (name, weight, case) in enumerate(CASES):
                if selected and name not in selected:
                    continue
                database = f"attention_case_{index}"
                conn = psycopg2.connect(pg.get_uri())
                try:
                    conn.autocommit = True
                    with conn.cursor() as cursor:
                        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                finally:
                    conn.close()
                world = World(candidate, pg.get_uri(database), seed + index)
                error = None
                try:
                    # Start from the frozen deployed schema: editing only a fresh
                    # install script must not masquerade as an upgrade path.
                    schema = HERE / "baseline_schema.sql"
                    world.sql(schema.read_text())
                    for migration in sorted((candidate / "postgres/init").glob("002_*.sql")):
                        world.sql(migration.read_text())
                    # Nontrivial IDs prevent assumptions about empty-database numbering.
                    for table in sorted(TABLES):
                        world.sql("SELECT setval(%s::regclass, %s, false)",
                                  (f"{table}_id_seq", random.Random(seed + index + len(table)).randint(50, 5000)))
                    case(world)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                results.append({"name": name, "weight": weight, "passed": error is None,
                                "error": error, "trace": world.trace})
            with psycopg2.connect(pg.get_uri()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version()")
                    postgres = cursor.fetchone()[0]
        finally:
            pg.cleanup()
    total = sum(item["weight"] for item in results)
    earned = sum(item["weight"] for item in results if item["passed"])
    git = subprocess.run(["git", "-C", str(candidate), "rev-parse", "HEAD"], capture_output=True, text=True)
    digest = hashlib.sha256()
    for folder in ("skills", "backend-fastapi", "postgres/init"):
        for path in sorted((candidate / folder).rglob("*")):
            if path.is_file() and path.suffix in {".py", ".sql"}:
                digest.update(str(path.relative_to(candidate)).encode())
                digest.update(path.read_bytes())
    grader_digest = hashlib.sha256()
    for filename in ("TASK.md", "baseline_schema.sql", "cases.py", "grade.py", "bridge.py"):
        grader_digest.update(filename.encode())
        grader_digest.update((HERE / filename).read_bytes())
    return {"task": "attention-provenance-v1", "baseline_commit": BASELINE,
            "candidate_commit": git.stdout.strip() if git.returncode == 0 else None,
            "candidate_source_sha256": digest.hexdigest(),
            "grader_sha256": grader_digest.hexdigest(), "seed": seed,
            "score": round(100 * earned / total, 2) if total else 0,
            "success": bool(results) and all(item["passed"] for item in results),
            "full_suite": len(results) == len(CASES), "groups": results,
            "runtime": {"python": platform.python_version(), "postgres": postgres,
                        "wall_seconds": round(time.monotonic() - started, 3)},
            "scope": {"database": "real PostgreSQL", "api": "FastAPI TestClient, fresh process per request",
                      "kafka": "publication captured, no live broker", "model_attempt": False,
                      "security_sandbox": False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--case", action="append", choices=[case[0] for case in CASES])
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = grade(args.candidate, args.seed, args.case)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in {"groups", "runtime"}}, indent=2))
    for group in report["groups"]:
        print(f"{'PASS' if group['passed'] else 'FAIL'} {group['name']}: {group['error'] or 'ok'}")
    return 0 if report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
