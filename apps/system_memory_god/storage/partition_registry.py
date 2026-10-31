"""Durable intent for isolated daily derived partitions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import stat
from typing import Iterator

from kernel.memory_pipeline_models import PipelineScope
from .pipeline_database import reject_directory_link


_APP_ID = 0x414C5052
_DDL = """CREATE TABLE partitions(
    partition_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('INITIALIZING','READY','INCOMPLETE')),
    evidence_required INTEGER NOT NULL CHECK(evidence_required IN (0,1)),
    ledger_required INTEGER NOT NULL CHECK(ledger_required IN (0,1)),
    vector_required INTEGER NOT NULL CHECK(vector_required IN (0,1)),
    generation INTEGER NOT NULL CHECK(generation>=0),
    raw_generation INTEGER NOT NULL CHECK(raw_generation>=0),
    updated_at TEXT NOT NULL
);"""


class PartitionRegistry:
    def __init__(self, root: Path):
        if not isinstance(root, Path) or not root.is_absolute():
            raise ValueError("absolute derived root required")
        self.root = root
        self.path = root / "daily_partition_registry.db"

    def _check_paths(self) -> bool:
        main_present = False
        side_present = False
        for suffix in ("", "-journal", "-wal", "-shm"):
            path = self.path.with_name(self.path.name + suffix)
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or getattr(info, "st_file_attributes", 0)
                    & stat.FILE_ATTRIBUTE_REPARSE_POINT):
                raise ValueError("partition registry unavailable")
            main_present |= not suffix
            side_present |= bool(suffix)
        if side_present and not main_present:
            # Another transaction can create both files after the first lstat.
            try:
                info = self.path.lstat()
            except FileNotFoundError:
                raise ValueError("partition registry unavailable") from None
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or getattr(info, "st_file_attributes", 0)
                    & stat.FILE_ATTRIBUTE_REPARSE_POINT):
                raise ValueError("partition registry unavailable")
            main_present = True
        return main_present

    @contextmanager
    def _open(self, *, write: bool) -> Iterator[sqlite3.Connection | None]:
        reject_directory_link(self.root)
        if write:
            self.root.mkdir(parents=True, exist_ok=True)
            reject_directory_link(self.root)
        present = self._check_paths()
        if not present and not write:
            yield None
            return
        mode = "rwc" if write else "ro"
        connection = sqlite3.connect(self.path.as_uri() + f"?mode={mode}",
                                     uri=True, timeout=5)
        try:
            self._check_paths()
            if write:
                connection.execute("BEGIN IMMEDIATE")
            app_id = connection.execute("PRAGMA application_id").fetchone()[0]
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if (app_id, version) == (0, 0) and write:
                if connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
                    raise ValueError("unknown partition registry")
                connection.execute(_DDL)
                connection.execute(f"PRAGMA application_id={_APP_ID}")
                connection.execute("PRAGMA user_version=2")
            elif (app_id, version) != (_APP_ID, 2):
                raise ValueError("unknown partition registry")
            columns = [row[1] for row in connection.execute(
                "PRAGMA table_info(partitions)")]
            if columns != ["partition_id", "state", "evidence_required",
                           "ledger_required", "vector_required", "generation",
                           "raw_generation", "updated_at"]:
                raise ValueError("unknown partition registry")
            yield connection
            if write:
                connection.commit()
            self._check_paths()
        except (sqlite3.Error, OSError):
            raise ValueError("partition registry unavailable") from None
        finally:
            connection.close()

    def snapshot(self, scope: PipelineScope) -> dict | None:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        with self._open(write=False) as db:
            if db is None:
                return None
            row = db.execute(
                "SELECT state,evidence_required,ledger_required,vector_required,"
                "generation,raw_generation FROM partitions WHERE partition_id=?",
                (scope.partition_id,),
            ).fetchone()
        if row is None:
            return None
        return {"state": row[0], "evidence": bool(row[1]),
                "ledger": bool(row[2]), "vector": bool(row[3]),
                "generation": row[4], "raw_generation": row[5]}

    def begin(self, scope: PipelineScope) -> int:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        partition = self.root / scope.partition_id
        reject_directory_link(partition)
        if not self._check_paths() and partition.exists() and any(partition.iterdir()):
            raise ValueError("unregistered derived partition")
        stamp = datetime.now(timezone.utc).isoformat()
        with self._open(write=True) as db:
            if db is None:
                raise ValueError("partition registry unavailable")
            row = db.execute("SELECT state,generation FROM partitions WHERE partition_id=?",
                             (scope.partition_id,)).fetchone()
            if row is not None and row[0] == "INCOMPLETE":
                raise ValueError("partition requires recovery")
            if row is None:
                reject_directory_link(partition)
                if partition.exists() and any(partition.iterdir()):
                    raise ValueError("unregistered derived partition")
                generation = 1
                db.execute("INSERT INTO partitions VALUES (?,?,?,?,?,?,?,?)",
                           (scope.partition_id, "INITIALIZING", 0, 0, 0,
                            generation, 0, stamp))
            else:
                generation = row[1] + 1
                db.execute("UPDATE partitions SET state='INITIALIZING',generation=?,"
                           "updated_at=? WHERE partition_id=?",
                           (generation, stamp, scope.partition_id))
        return generation

    def ready(self, scope: PipelineScope, *, ledger: bool, vector: bool,
              generation: int, raw_generation: int) -> None:
        if (type(scope) is not PipelineScope or type(ledger) is not bool
                or type(vector) is not bool or type(generation) is not int
                or generation < 1 or type(raw_generation) is not int
                or raw_generation < 0):
            raise ValueError("invalid partition publication")
        stamp = datetime.now(timezone.utc).isoformat()
        with self._open(write=True) as db:
            if db is None:
                raise ValueError("partition registry unavailable")
            row = db.execute("SELECT state,generation FROM partitions WHERE partition_id=?",
                             (scope.partition_id,)).fetchone()
            if row not in {("INITIALIZING", generation), ("INCOMPLETE", generation)}:
                raise ValueError("partition generation unavailable")
            db.execute("UPDATE partitions SET state='READY',evidence_required=1,"
                       "ledger_required=max(ledger_required,?),"
                       "vector_required=max(vector_required,?),"
                       "raw_generation=?,updated_at=? "
                       "WHERE partition_id=?",
                       (int(ledger), int(vector), raw_generation, stamp,
                        scope.partition_id))

    def incomplete(self, scope: PipelineScope, *, generation: int) -> None:
        if (type(scope) is not PipelineScope or type(generation) is not int
                or generation < 1):
            raise ValueError("invalid incomplete publication")
        stamp = datetime.now(timezone.utc).isoformat()
        with self._open(write=True) as db:
            if db is None:
                raise ValueError("partition registry unavailable")
            row = db.execute("SELECT state,generation FROM partitions WHERE partition_id=?",
                             (scope.partition_id,)).fetchone()
            if row not in {("INITIALIZING", generation), ("READY", generation)}:
                raise ValueError("partition generation unavailable")
            db.execute("UPDATE partitions SET state='INCOMPLETE',updated_at=? "
                       "WHERE partition_id=?", (stamp, scope.partition_id))
