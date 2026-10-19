"""Run candidate code in a fresh process; this is not a security sandbox."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys

import psycopg2
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json


def run(request):
    root = Path(request["candidate"])
    settings = parse_dsn(request["dsn"])
    for field, variable in {"host": "DB_HOST", "port": "DB_PORT", "dbname": "DB_NAME",
                            "user": "DB_USER", "password": "DB_PASSWORD"}.items():
        os.environ[variable] = settings.get(field, "5432" if field == "port" else "")
    sys.path.insert(0, str(root / "skills"))
    sys.path.insert(0, str(root / "backend-fastapi"))

    def load(relative, name):
        spec = importlib.util.spec_from_file_location(name, root / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    op, args = request["op"], request.get("args", {})
    if op in {"prepare", "write"}:
        module = load("skills/db_write.py", "candidate_write")
        original_connect = psycopg2.connect
        if request.get("outage"):
            def unavailable(*args, **kwargs):
                raise psycopg2.OperationalError("simulated unavailable database")
            psycopg2.connect = unavailable
        elif request.get("race"):
            race = request["race"]
            fired = False

            class CursorProxy:
                def __init__(self, cursor):
                    self.cursor = cursor

                def __getattr__(self, name):
                    return getattr(self.cursor, name)

                def __enter__(self):
                    self.cursor.__enter__()
                    return self

                def __exit__(self, *args):
                    return self.cursor.__exit__(*args)

                def execute(self, query, params=None):
                    nonlocal fired
                    result = self.cursor.execute(query, params)
                    text = str(query).lower()
                    if not fired and "select" in text and "from annotations" in text:
                        fired = True
                        try:
                            with original_connect(request["dsn"]) as conn:
                                with conn.cursor() as other:
                                    other.execute("SET LOCAL lock_timeout = '250ms'")
                                    other.execute("UPDATE annotations SET payload=%s WHERE id=%s",
                                                  (Json(race["annotation_payload"]), race["annotation_id"]))
                                    other.execute("UPDATE attention_tickets SET payload=%s WHERE id=%s",
                                                  (Json(race["ticket_payload"]), race["ticket_id"]))
                        except psycopg2.errors.LockNotAvailable:
                            # Explicit row locking is also a valid consistency strategy.
                            pass
                    return result

            class ConnectionProxy:
                def __init__(self, conn):
                    object.__setattr__(self, "conn", conn)

                def __getattr__(self, name):
                    return getattr(self.conn, name)

                def __setattr__(self, name, value):
                    setattr(self.conn, name, value)

                def __enter__(self):
                    self.conn.__enter__()
                    return self

                def __exit__(self, *args):
                    return self.conn.__exit__(*args)

                def cursor(self, *args, **kwargs):
                    return CursorProxy(self.conn.cursor(*args, **kwargs))

            def connect(*args, **kwargs):
                return ConnectionProxy(original_connect(*args, **kwargs))
            psycopg2.connect = connect

        if op == "prepare":
            original = json.loads(json.dumps(args["payload"]))
            value = module.prepare_payload(args["topic"], args["payload"])
            if args["payload"] != original:
                raise AssertionError("prepare_payload mutated its caller's payload")
            return value

        published = []

        def capture(topic, payload):
            published.append({"topic": topic, "payload": json.loads(json.dumps(payload))})
            return {"status": "ok", "topic": topic, "partition": 0, "offset": len(published)}

        module.produce_message = capture
        sys.argv = [str(root / "skills/db_write.py"), json.dumps(args)]
        output = io.StringIO()
        exit_code = 0
        error = None
        with contextlib.redirect_stdout(output):
            try:
                module.main()
            except SystemExit as exc:
                exit_code = exc.code
            except Exception as exc:
                exit_code = 1
                error = f"{type(exc).__name__}: {exc}"
        return {"published": published, "exit_code": exit_code,
                "stdout": output.getvalue()[:10000], "error": error}

    if op == "update":
        return load("skills/db_update.py", "candidate_update").run_update(args)
    if op == "api":
        from fastapi.testclient import TestClient
        module = load("backend-fastapi/app.py", "candidate_api")
        # No background broadcast loop: this task checks the actual HTTP route.
        with TestClient(module.app) as client:
            response = client.get(args["path"])
        return {"status_code": response.status_code, "body": response.json()}
    raise ValueError(f"Unknown operation: {op}")


if __name__ == "__main__":
    request = json.load(sys.stdin)
    logs = io.StringIO()
    with contextlib.redirect_stdout(logs):
        try:
            response = {"ok": True, "result": run(request)}
        except Exception as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    response["logs"] = logs.getvalue()[:10000]
    print(json.dumps(response, ensure_ascii=False, default=str))
