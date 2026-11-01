"""Narrow reader for an existing Raw WAL database in an explicitly trusted root."""

from __future__ import annotations

from contextlib import AbstractContextManager, ExitStack, contextmanager
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
from typing import Iterator, cast

from kernel.raw_message_database import _recognize, _remaining_publication_budget
from kernel.raw_message_models import (
    RawContentChunk, RawMessageEvidence, RawMessageError, Scope, ScopeError, StoredMessage,
)
from kernel.raw_message_store import (
    query_raw_get,
    query_raw_content_chunk,
    query_raw_has_derived_results,
    query_raw_neighbors,
    query_raw_partition_state,
    query_raw_published_snapshots,
    query_raw_results,
    query_raw_search_page,
    query_raw_source_snapshots,
    _publication_deadline,
    validate_raw_search,
    raw_evidence,
)
from kernel.raw_message_validation import _id, _scope


def _same_path(first: str | Path, second: Path) -> bool:
    value = str(first)
    if value.upper().startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return os.path.normcase(os.path.abspath(value)) == os.path.normcase(str(second))


def _ordinary(path: Path, *, directory: bool = False) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise RawMessageError("existing raw database required") from None
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected(info.st_mode) or info.st_nlink != 1
            or getattr(info, "st_file_attributes", 0)
            & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise RawMessageError("raw reader path unavailable")
    return info


def _guard(path: Path, stack: ExitStack, *, directory: bool = False) -> None:
    import win32con
    import win32file

    info = _ordinary(path, directory=directory)
    flags = win32con.FILE_FLAG_BACKUP_SEMANTICS if directory else win32con.FILE_ATTRIBUTE_NORMAL
    handle = win32file.CreateFile(
        str(path), win32con.GENERIC_READ,
        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
        None, win32con.OPEN_EXISTING, flags, None,
    )
    stack.callback(handle.Close)
    identity = win32file.GetFileInformationByHandle(handle)
    if (identity[4] != info.st_dev or (identity[8] << 32 | identity[9]) != info.st_ino
            or identity[7] != 1
            or not _same_path(win32file.GetFinalPathNameByHandle(handle, 0), path)):
        raise RawMessageError("raw reader path changed")


class RawReadLifetime:
    """Keep one recognized WAL connection alive, never a recall transaction.

    The child thread owns this connection. Other request threads inspect only
    file identities; evidence still comes from independent RawReadView reads.
    """

    def __init__(self, database: str | Path, *, trusted_root: Path):
        path = Path(database)
        if (os.name != "nt" or not path.is_absolute()
                or not isinstance(trusted_root, Path) or not trusted_root.is_absolute()
                or path.parent != trusted_root):
            raise RawMessageError("explicit raw reader root required")
        self.database, self.root = path, trusted_root
        self._connection: sqlite3.Connection | None = None
        self._guards = ExitStack()
        self._owner: int | None = None
        self._identities: dict[Path, tuple[int, int]] = {}

    def start(self, view: RawReadView) -> None:
        if self._owner is not None:
            raise RawMessageError("raw reader lifetime already started")
        if (type(view) is not RawReadView or view._lifetime is not None
                or view.database != self.database or view.root != self.root):
            raise RawMessageError("explicit matching standalone read view required")
        self._owner = threading.get_ident()
        try:
            _guard(self.root, self._guards, directory=True)
            _guard(self.database, self._guards)
            with view._open():
                db = sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True,
                                     timeout=5, isolation_level=None)
                self._connection = db
                db.execute("PRAGMA query_only=ON")
                if db.execute("PRAGMA journal_mode").fetchone()[0] != "wal" or not _recognize(db):
                    raise RawMessageError("recognized raw WAL lifetime required")
                listed = db.execute("PRAGMA database_list").fetchone()
                if listed is None or not _same_path(listed[2], self.database) or db.in_transaction:
                    raise RawMessageError("raw reader lifetime path changed")
                paths = (self.root, self.database, *(self.database.with_name(
                    self.database.name + suffix) for suffix in ("-wal", "-shm")))
                for path in paths:
                    info = _ordinary(path, directory=path == self.root)
                    self._identities[path] = (info.st_dev, info.st_ino)
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self) -> None:
        if self._connection is None or not self._identities:
            raise RawMessageError("raw reader lifetime unavailable")
        for path, identity in self._identities.items():
            info = _ordinary(path, directory=path == self.root)
            if (info.st_dev, info.st_ino) != identity:
                raise RawMessageError("raw reader lifetime identity changed")

    def close(self) -> None:
        if self._owner is not None and self._owner != threading.get_ident():
            raise RawMessageError("raw reader lifetime owner mismatch")
        try:
            if self._connection is not None:
                # All request sidefile guards have ended. Root/database guards
                # do not block legitimate SQLite sidefile cleanup.
                self._connection.close()
        finally:
            self._connection = None
            self._identities.clear()
            self._guards.close()
            self._owner = None


class RawReadView:
    """Only scoped queries; no writer, migration, export, or backup API."""

    def __init__(self, database: str | Path, *, trusted_root: Path, scope: Scope,
                 lifetime: RawReadLifetime | None = None):
        path = Path(database)
        if (os.name != "nt" or not path.is_absolute()
                or not isinstance(trusted_root, Path) or not trusted_root.is_absolute()
                or path.parent != trusted_root):
            raise RawMessageError("explicit raw reader root required")
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        self.database = path
        self.root = trusted_root
        self.scope = scope
        self._lifetime = lifetime
        if lifetime is not None and (lifetime.database != path or lifetime.root != trusted_root):
            raise RawMessageError("raw reader lifetime mismatch")

    def _require_scope(self, scope: Scope) -> None:
        _scope(scope)
        if scope != self.scope:
            raise ScopeError("conversation scope mismatch")

    @contextmanager
    def _open(self, *, deadline: float | None = None) -> Iterator[sqlite3.Connection]:
        if self._lifetime is not None:
            self._lifetime.check()
        if deadline is not None:
            _remaining_publication_budget(deadline)
        _ordinary(self.root, directory=True)
        _ordinary(self.database)
        sidecars = tuple(self.database.with_name(self.database.name + suffix)
                         for suffix in ("-wal", "-shm"))
        journal = self.database.with_name(self.database.name + "-journal")
        try:
            journal.lstat()
        except FileNotFoundError:
            pass
        else:
            raise RawMessageError("unexpected raw journal")
        before = {item.name for item in self.root.iterdir()}
        if deadline is not None:
            _remaining_publication_budget(deadline)
        with ExitStack() as stack:
            _guard(self.root, stack, directory=True)
            _guard(self.database, stack)
            for path in sidecars:
                try:
                    _ordinary(path)
                except RawMessageError:
                    if path.exists() or path.is_symlink():
                        raise
                    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                    os.close(descriptor)
                _guard(path, stack)
                if deadline is not None:
                    _remaining_publication_budget(deadline)
            database = self.database.as_uri() + "?mode=ro"
            timeout = min(5.0, _remaining_publication_budget(deadline)) if deadline is not None else 5.0
            db = sqlite3.connect(database, uri=True, timeout=timeout,
                                 isolation_level=None)
            try:
                db.row_factory = sqlite3.Row
                if deadline is not None:
                    db.execute(f"PRAGMA busy_timeout={max(1, int(_remaining_publication_budget(deadline) * 1000))}")
                    db.set_progress_handler(
                        lambda: int(time.monotonic() >= deadline), 1000)
                db.execute("PRAGMA query_only=ON")
                if db.execute("PRAGMA journal_mode").fetchone()[0] != "wal":
                    raise RawMessageError("raw WAL database required")
                db.execute("BEGIN")
                if not _recognize(db):
                    raise RawMessageError("existing raw database required")
                listed = db.execute("PRAGMA database_list").fetchone()
                if listed is None or not _same_path(listed[2], self.database):
                    raise RawMessageError("raw reader path changed")
                if deadline is not None:
                    _remaining_publication_budget(deadline)
                yield db
            except sqlite3.DatabaseError:
                if deadline is not None and time.monotonic() >= deadline:
                    raise RawMessageError("publication budget exceeded") from None
                raise RawMessageError("raw reader unavailable") from None
            finally:
                if deadline is not None:
                    db.set_progress_handler(None, 0)
                db.close()
                _ordinary(self.root, directory=True)
                for path in (self.database, *sidecars):
                    _ordinary(path)
                after = {item.name for item in self.root.iterdir()}
                if after - before - {item.name for item in sidecars}:
                    raise RawMessageError("raw reader created unexpected file")
                if deadline is not None:
                    _remaining_publication_budget(deadline)
                if self._lifetime is not None:
                    self._lifetime.check()

    def get(self, scope: Scope, message_id: str, *,
            include_inactive: bool = False) -> StoredMessage | None:
        self._require_scope(scope)
        _id(message_id)
        with self._open() as db:
            return query_raw_get(db, scope, message_id,
                                 include_inactive=include_inactive)

    def source_snapshots(
        self, scope: Scope, message_ids: tuple[str, ...]
    ) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
        self._require_scope(scope)
        with self._open() as db:
            return query_raw_source_snapshots(db, scope, message_ids)

    def published_snapshots(
        self, scope: Scope, message_ids: tuple[str, ...], *,
        proofs: dict[str, tuple[tuple[str, str], ...]] | None = None,
        deadline: float | None = None,
    ) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
        self._require_scope(scope)
        page_deadline = _publication_deadline(deadline)
        with self._open(deadline=page_deadline) as db:
            return cast(dict[str, tuple[StoredMessage, tuple[dict, ...]]],
                        query_raw_published_snapshots(db, scope, message_ids,
                                                      proofs=proofs, deadline=page_deadline))

    def _db(self, *, write: bool = False) -> AbstractContextManager[sqlite3.Connection]:
        if write:
            raise RawMessageError("raw reader is read-only")
        return self._open()

    def search_page(self, scope: Scope, query: str, *, limit: int = 10,
                    start_ts: float | None = None, end_ts: float | None = None,
                    before: tuple[int, str, str] | None = None,
                    deadline: float | None = None) -> list[StoredMessage]:
        self._require_scope(scope)
        needle = validate_raw_search(scope, query, limit=limit,
                                     start_ts=start_ts, end_ts=end_ts,
                                     before=before, deadline=deadline)
        with self._open(deadline=deadline) as db:
            return cast(list[StoredMessage], query_raw_search_page(
                db, scope, needle, limit=limit, start_ts=start_ts,
                end_ts=end_ts, before=before, deadline=deadline))

    def search_evidence_page(self, scope: Scope, query: str, *, limit: int = 10,
                             start_ts=None, end_ts=None, before=None, deadline=None,
                             evidence_query: tuple[str, ...] | None = None,
                             ) -> list[RawMessageEvidence]:
        self._require_scope(scope)
        needle = validate_raw_search(scope, query, limit=limit, start_ts=start_ts,
                                     end_ts=end_ts, before=before, deadline=deadline)
        with self._open(deadline=deadline) as db:
            return cast(list[RawMessageEvidence], query_raw_search_page(
                db, scope, needle, limit=limit, start_ts=start_ts, end_ts=end_ts,
                before=before, deadline=deadline,
                evidence_query=evidence_query if evidence_query is not None else needle))

    def hint_search(self, scope: Scope, hints: tuple[str, ...], *,
                    evidence_query: tuple[str, ...], start_ts=None, end_ts=None,
                    deadline: float) -> list[RawMessageEvidence]:
        from kernel.raw_message_store import query_raw_hint_candidates

        self._require_scope(scope)
        try:
            with self._open(deadline=deadline) as db:
                return query_raw_hint_candidates(db, scope, hints, evidence_query=evidence_query,
                    start_ts=start_ts, end_ts=end_ts, deadline=deadline)
        except RawMessageError as exc:
            if str(exc) in {"publication budget exceeded", "search budget exceeded"}:
                raise TimeoutError("hint query deadline exceeded") from None
            raise

    def published_evidence_snapshots(self, scope: Scope, message_ids: tuple[str, ...], *,
                                     query: str | tuple[str, ...], proofs=None, deadline=None
                                     ) -> dict[str, tuple[RawMessageEvidence, tuple[dict, ...]]]:
        self._require_scope(scope)
        deadline = _publication_deadline(deadline)
        with self._open(deadline=deadline) as db:
            return cast(dict[str, tuple[RawMessageEvidence, tuple[dict, ...]]],
                        query_raw_published_snapshots(db, scope, message_ids, proofs=proofs,
                                                      deadline=deadline, evidence_query=query))

    def read_content_chunk(self, scope: Scope, message_id: str, *, expected_revision: int,
                           expected_content_hash: str, offset_chars: int = 0,
                           limit_chars: int = 4096, deadline=None) -> RawContentChunk:
        self._require_scope(scope)
        with self._open(deadline=deadline) as db:
            return query_raw_content_chunk(db, scope, message_id,
                expected_revision=expected_revision, expected_content_hash=expected_content_hash,
                offset_chars=offset_chars, limit_chars=limit_chars)

    def evidence_neighbors(self, scope: Scope, message_id: str, *, query: str | tuple[str, ...],
                           before: int = 2, after: int = 2, deadline=None
                           ) -> list[RawMessageEvidence]:
        return [raw_evidence(message, query) for message in self.neighbors(
            scope, message_id, before=before, after=after, deadline=deadline)]

    def search(self, scope: Scope, query: str, *, limit: int = 10,
               start_ts: float | None = None, end_ts: float | None = None,
               deadline: float | None = None) -> list[StoredMessage]:
        return self.search_page(scope, query, limit=limit, start_ts=start_ts,
                                end_ts=end_ts, deadline=deadline)

    def neighbors(self, scope: Scope, message_id: str, *, before: int = 2,
                  after: int = 2, deadline: float | None = None) -> list[StoredMessage]:
        self._require_scope(scope)
        _id(message_id)
        if any(type(value) is not int or not 0 <= value <= 10
               for value in (before, after)):
            raise RawMessageError("invalid context window")
        with self._open(deadline=deadline) as db:
            return query_raw_neighbors(db, scope, message_id,
                                       before=before, after=after)

    def results(self, scope: Scope, message_id: str, *,
                include_inactive: bool = False) -> list[dict]:
        self._require_scope(scope)
        _id(message_id)
        with self._open() as db:
            return query_raw_results(db, scope, message_id,
                                     include_inactive=include_inactive)

    def has_derived_results(self, scope: Scope) -> bool:
        self._require_scope(scope)
        with self._open() as db:
            return query_raw_has_derived_results(db, scope)

    def partition_state(self, scope: Scope) -> tuple[int, bool]:
        self._require_scope(scope)
        with self._open() as db:
            return query_raw_partition_state(db, scope)

    def conversation_registered(self) -> bool:
        """Distinguish an empty registered chat from an unknown four-part scope."""
        scope = self.scope
        with self._open() as db:
            return db.execute(
                "SELECT 1 FROM conversations WHERE agent_id=? AND user_id=? "
                "AND connector_id=? AND conversation_id=? LIMIT 1",
                (scope.agent_id, scope.user_id, scope.connector_id,
                 scope.conversation_id),
            ).fetchone() is not None
