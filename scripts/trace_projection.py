"""Publish a consistent standalone SQLite copy for a read-only UI mount."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time


def publish_projection(source, destination):
    """Backup committed ledger state without depending on shared WAL sidecars.

    The live ledger remains unchanged. Readers see either complete old or new
    copies; metadata is inside that same copy, so freshness matches its rows.
    """
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("the projection must not replace the live ledger")
    if not source.is_file():
        raise FileNotFoundError(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        fd, name = tempfile.mkstemp(prefix=".trace-projection-", suffix=".sqlite3", dir=destination.parent)
        os.close(fd)
        temporary = Path(name)
        try:
            deadline = time.monotonic() + 20
            def progress(_status, _remaining, _total):
                if time.monotonic() > deadline:
                    raise TimeoutError("trace projection backup exceeded 20 seconds")
            with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=5)) as live:
                with closing(sqlite3.connect(temporary)) as copy:
                    live.backup(copy, pages=256, progress=progress, sleep=0.01)
                    # A copied WAL-mode header still requires writable sidecars.
                    # Switch only the backup to a standalone rollback journal.
                    mode = copy.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                    if mode.lower() != "delete":
                        raise RuntimeError("trace projection is not standalone")
                    latest = copy.execute("SELECT COALESCE(MAX(sequence),0),MAX(recorded_at) FROM entries").fetchone()
                    metadata = {"format_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(),
                                "source_sequence": latest[0], "latest_entry_recorded_at": latest[1],
                                "publication_method": "sqlite_backup_atomic_replace"}
                    copy.execute("CREATE TABLE IF NOT EXISTS trace_projection_metadata (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
                    copy.execute("INSERT OR REPLACE INTO trace_projection_metadata(id,payload) VALUES(1,?)", (json.dumps(metadata),))
                    copy.commit()
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            return metadata
        finally:
            for path in (temporary, Path(str(temporary) + "-wal"), Path(str(temporary) + "-shm"), Path(str(temporary) + "-journal")):
                path.unlink(missing_ok=True)
