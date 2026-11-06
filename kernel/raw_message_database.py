"""Private SQLite ownership, schema recognition and transaction Implementation."""

from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
import os
import shutil
import sqlite3
import hashlib
import time
from typing import Callable, Iterator

from kernel.raw_message_models import RawMessageError
from kernel.raw_message_schema import (
    APPLICATION_ID, DERIVATION_EVENT_INDEX_SQL, PARTITION_EPOCH_SQL,
    PURGE_RECEIPT_SQL, SCHEMA_SQL, SCHEMA_V1_SQL, SCHEMA_V2_SQL,
    SCHEMA_V3_SQL, SCHEMA_VERSION, SEARCH_INDEX_SQL,
)


_MAX_MIGRATION_SOURCE_BYTES = 1024 * 1024 * 1024
_MIGRATION_HEADROOM_BYTES = 64 * 1024 * 1024


def _remaining_publication_budget(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RawMessageError("publication budget exceeded")
    return remaining


def _migration_space_preflight(db: sqlite3.Connection, source: Path,
                               backup: Path) -> None:
    """Reserve room for the consistent backup and the FTS rebuild before writing."""
    try:
        logical_bytes = db.execute("PRAGMA page_count").fetchone()[0] * db.execute(
            "PRAGMA page_size").fetchone()[0]
        wal_path = source.with_name(source.name + "-wal")
        wal_bytes = wal_path.stat().st_size if wal_path.is_file() else 0
        if (logical_bytes <= 0 or logical_bytes > _MAX_MIGRATION_SOURCE_BYTES
                or wal_bytes > _MAX_MIGRATION_SOURCE_BYTES):
            raise RawMessageError("migration source too large")
        destination_probe = backup.parent
        while not destination_probe.exists():
            parent = destination_probe.parent
            if parent == destination_probe:
                raise RawMessageError("migration space unavailable")
            destination_probe = parent
        source_free = shutil.disk_usage(source.parent).free
        destination_free = shutil.disk_usage(destination_probe).free
        source_need = 3 * logical_bytes + 2 * wal_bytes + _MIGRATION_HEADROOM_BYTES
        backup_need = 2 * logical_bytes + wal_bytes + _MIGRATION_HEADROOM_BYTES
        same_volume = (os.stat(source.parent).st_dev
                       == os.stat(destination_probe).st_dev)
        if (source_free < (source_need + backup_need if same_volume else source_need)
                or destination_free < (0 if same_volume else backup_need)):
            raise RawMessageError("migration space unavailable")
    except OSError:
        raise RawMessageError("migration space unavailable") from None


def _schema_catalog(db: sqlite3.Connection) -> tuple:
    """Keep known DDL literal-sensitive, including executable schema hooks."""
    rows = db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        # SQLite reserves the literal sqlite_ prefix, not sqlite plus any character.
        "WHERE substr(lower(name),1,7) <> 'sqlite_' ORDER BY type,name"
    )
    return tuple(
        (kind, name, table, sql.strip() if sql is not None else None)
        for kind, name, table, sql in rows
    )


def _schema_signature(db: sqlite3.Connection) -> dict:
    """Recognize known table definitions and their complete structural metadata."""
    catalog = _schema_catalog(db)
    tables = [(name, sql) for kind, name, _, sql in catalog if kind == "table"]
    signature = {}
    for table, definition in tables:
        # Names come from SQLite's catalog; quote them rather than interpolating input.
        quoted = '"' + table.replace('"', '""') + '"'
        columns = tuple(tuple(r) for r in db.execute(f"PRAGMA table_xinfo({quoted})"))
        if not columns or any(len(column) != 7 for column in columns):
            raise RawMessageError("complete column metadata unavailable")
        foreign_keys = tuple(
            tuple(r) for r in db.execute(f"PRAGMA foreign_key_list({quoted})")
        )
        indexes = []
        for index in db.execute(f"PRAGMA index_list({quoted})"):
            name = '"' + index[1].replace('"', '""') + '"'
            indexes.append(
                (
                    tuple(index)[1:],
                    tuple(tuple(r) for r in db.execute(f"PRAGMA index_xinfo({name})")),
                )
            )
        # Only trim outer whitespace: SQL literals and constraint expressions matter.
        signature[table] = (
            definition.strip(),
            columns,
            foreign_keys,
            tuple(sorted(indexes)),
        )
    return {"catalog": catalog, "tables": signature}


def _statements(script: str) -> Iterator[str]:
    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            yield pending.strip()
            pending = ""
    if pending.strip():
        raise RawMessageError("incomplete raw-message schema")


@lru_cache(maxsize=4)
def _expected_schema(version: int = SCHEMA_VERSION) -> dict:
    # Lazy, memory-only reference: importing this module never opens a database.
    db = sqlite3.connect(":memory:")
    try:
        db.executescript({1: SCHEMA_V1_SQL, 2: SCHEMA_V2_SQL,
                          3: SCHEMA_V3_SQL,
                          SCHEMA_VERSION: SCHEMA_SQL}[version])
        return _schema_signature(db)
    finally:
        db.close()


def _recognize(db: sqlite3.Connection) -> bool:
    """Return False only for an empty database; v1 requires explicit migration."""
    version = db.execute("PRAGMA user_version").fetchone()[0]
    application = db.execute("PRAGMA application_id").fetchone()[0]
    if application not in {0, APPLICATION_ID}:
        raise RawMessageError("unrecognized database application")
    if version == 0:
        if application or db.execute("SELECT 1 FROM sqlite_master").fetchone():
            raise RawMessageError("unrecognized database")
        return False
    if version in {1, 2, 3}:
        if _schema_signature(db) != _expected_schema(version):
            raise RawMessageError("unrecognized database schema")
        raise RawMessageError("explicit migration required")
    if version != SCHEMA_VERSION:
        raise RawMessageError("unsupported database version")
    if _schema_signature(db) != _expected_schema(SCHEMA_VERSION):
        raise RawMessageError("unrecognized database schema")
    return True


class _RawDatabase:
    @staticmethod
    def migrate_legacy(database: str | Path, backup_destination: str | Path) -> dict:
        """Upgrade a known legacy file after a consistent, verified SQLite backup."""
        source = Path(database)
        backup = Path(backup_destination)
        if not source.is_absolute() or not backup.is_absolute() or source.is_symlink():
            raise RawMessageError("explicit migration paths required")
        source, backup = source.resolve(), backup.resolve()
        if source == backup or not source.is_file() or backup.exists():
            raise RawMessageError("migration source or backup unavailable")
        db = sqlite3.connect(source, timeout=10, isolation_level=None)
        try:
            db.execute("BEGIN IMMEDIATE")
            original_application = db.execute("PRAGMA application_id").fetchone()[0]
            source_version = db.execute("PRAGMA user_version").fetchone()[0]
            if (original_application not in {0, APPLICATION_ID}
                    or source_version not in {1, 2, 3}
                    or _schema_signature(db) != _expected_schema(source_version)):
                raise RawMessageError("unrecognized migration source")
            _migration_space_preflight(db, source, backup)
            backup.parent.mkdir(parents=True, exist_ok=True)
            with backup.open("xb"):
                pass
            reader = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=10)
            target = sqlite3.connect(backup)
            try:
                reader.backup(target)
                if (target.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                        or target.execute("PRAGMA application_id").fetchone()[0] != original_application
                        or _schema_signature(target) != _expected_schema(source_version)):
                    raise RawMessageError("migration backup invalid")
            finally:
                target.close()
                reader.close()
            if source_version == 1:
                db.execute(SEARCH_INDEX_SQL.strip().rstrip(";"))
                db.execute(PURGE_RECEIPT_SQL.strip().rstrip(";"))
                db.execute("INSERT INTO raw_search(raw_search) VALUES('rebuild')")
            if source_version in {1, 2}:
                for statement in _statements(PARTITION_EPOCH_SQL):
                    db.execute(statement)
                db.execute("INSERT INTO raw_partition_epochs(conversation_id,generation) "
                           "SELECT conversation_id,1 FROM conversations")
            db.execute(DERIVATION_EVENT_INDEX_SQL.strip().rstrip(";"))
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            if (_schema_signature(db) != _expected_schema(SCHEMA_VERSION)
                    or db.execute("PRAGMA integrity_check").fetchone()[0] != "ok"):
                raise RawMessageError("migration verification failed")
            db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()
        digest = hashlib.sha256()
        with backup.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return {"backup_path": str(backup), "backup_sha256": digest.hexdigest()}

    def __init__(self, database: str | Path):
        self.path = Path(database).resolve()
        if self.path.exists() and self.path.stat().st_size:
            try:
                with self.connection() as db:
                    db.execute("BEGIN")
                    _recognize(db)
            except sqlite3.DatabaseError:
                raise RawMessageError("invalid raw-message database") from None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection(write=True) as db:
            # Recheck after locking, including when concurrent constructors create it.
            if not _recognize(db):
                for statement in _statements(SCHEMA_SQL):
                    db.execute(statement)
                db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                db.execute(f"PRAGMA application_id={APPLICATION_ID}")
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        try:
            db.execute("PRAGMA journal_mode=WAL")
        finally:
            db.close()

    @contextmanager
    def connection(self, *, write: bool = False,
                   deadline: float | None = None,
                   _on_connection: Callable[[sqlite3.Connection], None] | None = None
                   ) -> Iterator[sqlite3.Connection]:
        remaining = _remaining_publication_budget(deadline) if deadline is not None else 10.0
        database = str(self.path) if write else self.path.as_uri() + "?mode=ro"
        db = sqlite3.connect(database, timeout=min(10.0, remaining),
                             isolation_level=None, uri=not write)
        db.row_factory = sqlite3.Row
        try:
            if _on_connection is not None:
                _on_connection(db)
            if deadline is not None:
                db.execute(f"PRAGMA busy_timeout={max(1, int(_remaining_publication_budget(deadline) * 1000))}")
                db.set_progress_handler(
                    lambda: int(time.monotonic() >= deadline), 1000)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            if deadline is not None:
                _remaining_publication_budget(deadline)
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if deadline is not None:
                _remaining_publication_budget(deadline)
            if write:
                db.commit()
        except BaseException as exc:
            if write:
                db.rollback()
            if (deadline is not None and isinstance(exc, sqlite3.DatabaseError)
                    and time.monotonic() >= deadline):
                raise RawMessageError("publication budget exceeded") from None
            raise
        finally:
            if deadline is not None:
                db.set_progress_handler(None, 0)
            db.close()

    def backup(self, destination: str | Path) -> dict:
        """Administrative full-database backup; never expose this as a user export.

        The destination must be new. Failed backups are retained as artifacts,
        not automatically deleted; the caller gets the underlying error.
        """
        destination = Path(destination).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb"):
            pass
        with self.connection() as source:
            target = sqlite3.connect(destination)
            try:
                source.backup(target)
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RawMessageError("backup integrity check failed")
            finally:
                target.close()
        digest = hashlib.sha256()
        with destination.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return {"sha256": digest.hexdigest(), "size_bytes": destination.stat().st_size}
