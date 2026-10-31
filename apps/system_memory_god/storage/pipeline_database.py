"""Strict, explicitly scoped SQLite files for new partitions only.

No legacy migration and no shared persistent connections. Each operation owns a
transaction and closes its connection, including failure paths. Caller threads
must keep this synchronous disk IO off the chat event loop.
"""

from contextlib import contextmanager
from pathlib import Path
import os
import re
import sqlite3
import stat
from typing import Callable, Iterator

from kernel.memory_pipeline_models import PipelineScope, canonical

_HEADER = """
CREATE TABLE pipeline_identity(scope TEXT NOT NULL, version INTEGER NOT NULL);
"""


def _ordinary_windows_component(component: str) -> None:
    """Only filenames with identical ordinary/extended Win32 interpretation."""
    if (not component or component in {".", ".."} or component.endswith((".", " "))
            or any(ord(char) < 32 or char in '<>:"/\\|?*' for char in component)):
        raise ValueError("unsupported pipeline path")
    # Devices remain reserved with extensions; spaces before that extension
    # do not turn a device into an ordinary file. Do not silently trim input.
    stem = component.split(".", 1)[0].rstrip(" ").upper()
    if (stem in {"CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$"}
            or re.fullmatch(r"(?:COM|LPT)[1-9¹²³]", stem)):
        raise ValueError("unsupported pipeline path")


def _windows_spelling(value: str) -> str:
    """Normalize only equivalent DOS/UNC spellings, never device namespaces."""
    value = value.replace("/", "\\")
    if value[:8].upper() == "\\\\?\\UNC\\":
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
        if not re.match(r"^[A-Za-z]:\\", value):
            raise ValueError("unsupported pipeline path")
    if value.startswith("\\\\"):
        parts = value[2:].split("\\")
        if len(parts) < 2 or any(part in {"", ".", "..", "?"} for part in parts[:2]):
            raise ValueError("unsupported pipeline path")
        tail = value[2:]
    elif re.match(r"^[A-Za-z]:\\", value):
        tail = value[3:]
    else:
        raise ValueError("unsupported pipeline path")
    # A trailing separator belongs to the root spelling, not a filename.
    for component in tail.rstrip("\\").split("\\") if tail.rstrip("\\") else ():
        _ordinary_windows_component(component)
    return value


def resolved_pipeline_path(path: Path) -> Path:
    if os.name != "nt":
        return path.resolve()
    path = Path(_windows_spelling(str(path)))
    return Path(_windows_spelling(str(path.resolve())))


def reject_directory_link(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if (not stat.S_ISDIR(info.st_mode)
            or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise ValueError("pipeline path changed")


def _catalog(db: sqlite3.Connection) -> list[tuple]:
    return db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE substr(lower(name),1,7) <> 'sqlite_' ORDER BY type,name"
    ).fetchall()


def paged_rows(db: sqlite3.Connection, query: str, parameters: tuple = (),
               *, page_size: int = 256) -> Iterator[tuple]:
    cursor = db.execute(query, parameters)
    while rows := cursor.fetchmany(page_size):
        yield from rows


class PipelineDatabase:
    def __init__(self, root: Path, scope: PipelineScope, filename: str, ddl: str, app_id: int,
                 *, version: int = 1):
        if not isinstance(root, Path) or not root.is_absolute() or type(scope) is not PipelineScope:
            raise ValueError("explicit scoped database required")
        if filename not in {"source_ledger.db", "derivation_runs.db", "derived_evidence.db",
                            "vector_index.db", "derivation_gate.db", "extraction_snapshots.db"}:
            raise ValueError("invalid pipeline database")
        if type(version) is not int or not 1 <= version <= 2**31 - 1:
            raise ValueError("invalid pipeline schema version")
        self.root = resolved_pipeline_path(root)
        self.scope = scope
        self.path = self.root / scope.partition_id / filename
        self.ddl = _HEADER + ddl
        self.app_id = app_id
        self.version = version
        with sqlite3.connect(":memory:") as template:
            template.executescript(self.ddl)
            self.catalog = _catalog(template)
        template.close()

    def _path_check(self) -> None:
        parent = self.path.parent
        if (resolved_pipeline_path(self.root) != self.root
                or resolved_pipeline_path(parent).parent != self.root):
            raise ValueError("pipeline path changed")
        reject_directory_link(parent)
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                info = self.path.with_name(self.path.name + suffix).lstat()
            except FileNotFoundError:
                continue
            # SQLite's sidefiles can write outside the partition too.
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
                raise ValueError("pipeline path changed")

    def _readonly_identity(self) -> tuple[int, int, int, int]:
        self._path_check()
        for suffix in ("-journal", "-wal", "-shm"):
            if self.path.with_name(self.path.name + suffix).exists():
                raise ValueError("pipeline sidefile unavailable for inventory")
        info = self.path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("pipeline storage unavailable")
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns

    @contextmanager
    def readonly_snapshot(self) -> Iterator[sqlite3.Connection]:
        """Recognize and scan one existing file without creating or upgrading it."""
        db = None
        try:
            before = self._readonly_identity()
            db = sqlite3.connect(self.path.as_uri() + "?mode=ro&immutable=1",
                                 uri=True, timeout=5, isolation_level=None)
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            self._recognize(db, allow_initialize=False)
            yield db
            if self._readonly_identity() != before:
                raise ValueError("pipeline storage changed during inventory")
        except (sqlite3.Error, OSError):
            raise ValueError("pipeline storage unavailable") from None
        finally:
            if db is not None:
                db.close()

    def validate_existing(self, check: Callable[[sqlite3.Connection], None] | None = None) -> None:
        with self.readonly_snapshot() as db:
            if check is not None:
                check(db)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = None
        try:
            self._path_check()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._path_check()
            db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            self._recognize(db)
            yield db
            db.commit()
        except (sqlite3.Error, OSError):
            raise ValueError("pipeline storage unavailable") from None
        finally:
            if db is not None:
                if db.in_transaction:
                    db.rollback()
                db.close()

    def _recognize(self, db: sqlite3.Connection, *, allow_initialize: bool = True) -> None:
        catalog = _catalog(db)
        app_id = db.execute("PRAGMA application_id").fetchone()[0]
        version = db.execute("PRAGMA user_version").fetchone()[0]
        has_objects = db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone() is not None
        if not has_objects and app_id == 0 and version == 0:
            if not allow_initialize:
                raise ValueError("pipeline storage unavailable")
            # executescript commits implicitly; execute static DDL individually.
            for statement in self.ddl.split(";"):
                if statement.strip():
                    db.execute(statement)
            db.execute("INSERT INTO pipeline_identity VALUES (?,?)",
                       (canonical(self.scope.key), self.version))
            db.execute(f"PRAGMA application_id={self.app_id}")
            db.execute(f"PRAGMA user_version={self.version}")
        elif catalog != self.catalog or app_id != self.app_id or version != self.version:
            raise ValueError("unknown pipeline schema")
        rows = db.execute("SELECT scope,version FROM pipeline_identity").fetchall()
        if rows != [(canonical(self.scope.key), self.version)]:
            raise ValueError("pipeline scope mismatch")
