"""Append-only, hash-linked local experiment ledger (not a security boundary)."""
from __future__ import annotations
import hashlib
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamps require an explicit timezone")
    return dt.astimezone(timezone.utc)


class Ledger:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
          PRAGMA journal_mode=WAL;
          CREATE TABLE IF NOT EXISTS entries (
            sequence INTEGER PRIMARY KEY, kind TEXT NOT NULL, key TEXT NOT NULL,
            recorded_at TEXT NOT NULL, payload TEXT NOT NULL,
            previous_hash TEXT NOT NULL, hash TEXT NOT NULL UNIQUE,
            UNIQUE(kind,key));
          CREATE TRIGGER IF NOT EXISTS no_update BEFORE UPDATE ON entries
            BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
          CREATE TRIGGER IF NOT EXISTS no_delete BEFORE DELETE ON entries
            BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
        ''')
        self.verify()

    def close(self):
        self.db.close()

    def entries(self, kind=None):
        rows = self.db.execute("SELECT * FROM entries" + (" WHERE kind=?" if kind else "") + " ORDER BY sequence", (kind,) if kind else ())
        return [dict(row) | {"payload": json.loads(row["payload"])} for row in rows]

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def append(self, kind, key, payload):
        encoded = canonical(payload)
        owns_transaction = not self.db.in_transaction
        try:
            if owns_transaction:
                self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute("SELECT * FROM entries WHERE kind=? AND key=?", (kind, key)).fetchone()
            if old:
                if old["payload"] != encoded:
                    raise ValueError(f"cannot rewrite {kind}/{key}")
                if owns_transaction:
                    self.db.commit()
                return old["hash"]
            last = self.db.execute("SELECT hash FROM entries ORDER BY sequence DESC LIMIT 1").fetchone()
            previous = last["hash"] if last else "0" * 64
            at = utcnow()
            value = digest({"kind": kind, "key": key, "recorded_at": at, "payload": payload, "previous_hash": previous})
            self.db.execute("INSERT INTO entries(kind,key,recorded_at,payload,previous_hash,hash) VALUES(?,?,?,?,?,?)", (kind, key, at, encoded, previous, value))
            if owns_transaction:
                self.db.commit()
            return value
        except BaseException:
            if owns_transaction:
                self.db.rollback()
            raise

    def verify(self):
        previous = "0" * 64
        for row in self.entries():
            fields = {k: row[k] for k in ("kind", "key", "recorded_at", "payload", "previous_hash")}
            if row["previous_hash"] != previous or digest(fields) != row["hash"]:
                raise ValueError(f"ledger integrity failure at {row['sequence']}")
            previous = row["hash"]
        return previous
