"""Existing-file, scoped read views for isolated daily evidence and provenance."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import sqlite3
from typing import Iterator

from kernel.memory_pipeline_models import PipelineScope, canonical
from kernel.raw_message_reader import _guard, _ordinary, _same_path

from .derived_evidence import (
    _DDL as _EVIDENCE_DDL, query_evidence_ids, query_evidence_many,
    validate_evidence_ids, validate_evidence_search,
)
from .memory_ledger import _SOURCE_DDL, query_ledger_ids, validate_ledger_ids
from .pipeline_database import _HEADER, _catalog, resolved_pipeline_path


class _ExistingPipelineReadView:
    """No writable store/database instance is retained or exposed."""

    def __init__(self, root: Path, scope: PipelineScope, filename: str,
                 ddl: str, app_id: int, version: int):
        if (os.name != "nt" or not isinstance(root, Path) or not root.is_absolute()
                or type(scope) is not PipelineScope):
            raise ValueError("explicit scoped read root required")
        self._root = resolved_pipeline_path(root)
        self._parent = self._root / scope.partition_id
        self._path = self._parent / filename
        if resolved_pipeline_path(self._parent).parent != self._root:
            raise ValueError("pipeline path escaped read root")
        self._scope = canonical(scope.key)
        self._app_id = app_id
        self._version = version
        with sqlite3.connect(":memory:") as template:
            template.executescript(_HEADER + ddl)
            self._catalog = _catalog(template)

    @contextmanager
    def _open(self) -> Iterator[sqlite3.Connection]:
        try:
            _ordinary(self._root, directory=True)
            _ordinary(self._parent, directory=True)
            before = _ordinary(self._path)
            sidecars = tuple(self._path.with_name(self._path.name + suffix)
                             for suffix in ("-journal", "-wal", "-shm"))
            if any(path.exists() or path.is_symlink() for path in sidecars):
                raise ValueError("pipeline sidefile unavailable for read")
            with ExitStack() as stack:
                for path in (self._root, self._parent):
                    _guard(path, stack, directory=True)
                _guard(self._path, stack)
                db = sqlite3.connect(self._path.as_uri() + "?mode=ro", uri=True,
                                     timeout=5, isolation_level=None)
                try:
                    db.execute("PRAGMA query_only=ON")
                    db.execute("BEGIN")
                    if db.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
                        raise ValueError("pipeline journal mode unavailable")
                    listed = db.execute("PRAGMA database_list").fetchone()
                    if listed is None or not _same_path(listed[2], self._path):
                        raise ValueError("pipeline path changed")
                    if (_catalog(db) != self._catalog
                            or db.execute("PRAGMA application_id").fetchone()[0] != self._app_id
                            or db.execute("PRAGMA user_version").fetchone()[0] != self._version
                            or db.execute("SELECT scope,version FROM pipeline_identity").fetchall()
                            != [(self._scope, self._version)]):
                        raise ValueError("pipeline schema or scope mismatch")
                    yield db
                finally:
                    db.close()
                    after = _ordinary(self._path)
                    if ((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                            != (before.st_dev, before.st_ino, before.st_size,
                                before.st_mtime_ns)
                            or any(path.exists() or path.is_symlink() for path in sidecars)):
                        raise ValueError("pipeline storage changed during read")
        except (sqlite3.Error, OSError) as exc:
            raise ValueError("pipeline read unavailable") from exc

    def validate_existing(self) -> None:
        with self._open():
            pass


class DerivedEvidenceReadView(_ExistingPipelineReadView):
    def __init__(self, root: Path, scope: PipelineScope):
        super().__init__(root, scope, "derived_evidence.db", _EVIDENCE_DDL,
                         0x414C4445, 2)

    def search_ids_page(self, query: str, *, limit: int = 30,
                        after: str | None = None,
                        deadline: float | None = None) -> tuple[str, ...]:
        validate_evidence_search(query, limit, after, deadline)
        with self._open() as db:
            return query_evidence_ids(db, query, limit=limit, after=after,
                                      deadline=deadline)

    def get_many(self, memory_ids: tuple[str, ...]) -> dict[str, dict]:
        validate_evidence_ids(memory_ids)
        with self._open() as db:
            return query_evidence_many(db, memory_ids)

    def domain_ids(self, query, *, domains, limit=100, deadline=None,
                   event_start_ts=None, event_end_ts=None, timings=None):
        from .domain_queries import query_domain_ids

        with self._open() as db:
            return query_domain_ids(db, query, domains=domains, limit=limit, deadline=deadline,
                event_start_ts=event_start_ts, event_end_ts=event_end_ts, timings=timings)


class SourceLedgerReadView(_ExistingPipelineReadView):
    def __init__(self, root: Path, scope: PipelineScope):
        super().__init__(root, scope, "source_ledger.db", _SOURCE_DDL,
                         0x414C534C, 2)

    def inspect_ids(self, memory_ids: tuple[str, ...]) -> dict[str, dict]:
        validate_ledger_ids(memory_ids)
        with self._open() as db:
            return query_ledger_ids(db, memory_ids)
