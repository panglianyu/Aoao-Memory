"""Isolated conversation fact store.

An explicit path is required. Importing this module creates nothing and does
not wire any production channel. Calls are synchronous; async callers must
dispatch them off their event loop. Each call owns a short-lived connection.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from collections.abc import Mapping
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Callable, Generator, Iterator, cast
from dataclasses import asdict, dataclass, replace

from kernel.raw_message_models import (
    AttachmentRef,
    ConflictError,
    DerivationSpec,
    MessageDraft,
    RawContentChunk,
    RawMessageEvidence,
    RawMessageError,
    ResultAnchor,
    Scope,
    ScopeError,
    StoredMessage,
)
from kernel.memory_pipeline_models import PipelineScope, SourceRevision, identity
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS, validate_retrieval_hints

from kernel.raw_message_schema import SCHEMA_VERSION
from kernel.feishu_business_text import extract_feishu_business_text
from kernel.raw_message_database import _RawDatabase
from kernel import raw_message_outbox as _outbox
from kernel import raw_message_registry as _registry
from kernel.raw_message_validation import (
    _content,
    _draft_payload,
    _hash,
    _id,
    _limits,
    _now,
    _predicate,
    _scope,
    _specs,
    _timestamp,
)


def _message(row: sqlite3.Row) -> StoredMessage:
    values = dict(row)
    values["metadata"] = json.loads(values.pop("metadata_json"))
    values.pop("schema_version")
    values.pop("idempotency_key")
    values.pop("payload_hash")
    values["attachment_refs"] = tuple(
        AttachmentRef(**item) for item in json.loads(values.pop("attachment_refs_json"))
    )
    return StoredMessage(**values)


@dataclass(frozen=True)
class _PurgeProof:
    receipt_key: str
    revision: int
    task_ids: tuple[str, ...]
    results: tuple[tuple[str, str, str], ...]
    gate: object | None = None


_MAX_SEARCH_SCAN = 10000
_MAX_SEARCH_CANDIDATES = 5000
_MAX_SEARCH_RETURN_BYTES = 1024 * 1024
_SEARCH_VM_LIMIT = 20_000_000
_SEARCH_SECONDS_LIMIT = 3.0
_SEARCH_PROGRESS_INTERVAL = 1000


def raw_evidence(message: StoredMessage, query: str | tuple[str, ...] = "") -> RawMessageEvidence:
    """Verify the entire original before extracting an unchanged, located slice."""
    encoded = message.content.encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != message.content_hash:
        raise RawMessageError("original integrity conflict")
    fold = bytes.maketrans(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ", b"abcdefghijklmnopqrstuvwxyz")
    if isinstance(query, tuple):
        # The first member is the unchanged original query. Other members are
        # separately validated hints, never a replacement for stored evidence.
        query = next((part for part in query if part and
                      encoded.translate(fold).find(part.encode("utf-8").translate(fold)) >= 0), "")
    byte_position = encoded.translate(fold).find(query.encode("utf-8").translate(fold)) if query else 0
    # ASCII bytes cannot occur inside a UTF-8 multibyte sequence. Preserve the
    # existing ASCII-only match rule and convert back to original character IDs.
    position = len(encoded[:byte_position].decode("utf-8")) if byte_position >= 0 else -1
    start = max(0, position - 512) if position >= 0 else 0
    end = min(len(message.content), start + 2048)
    start = max(0, end - 2048)
    return RawMessageEvidence(
        message.message_id, message.agent_id, message.user_id, message.connector_id,
        message.conversation_id, message.session_id, message.ordinal, message.role,
        message.content[start:end], message.created_at, message.status,
        message.revision, message.content_hash, start != 0 or end != len(message.content),
        start, end, len(message.content), len(encoded),
    )


def query_raw_hint_candidates(db: sqlite3.Connection, scope: Scope, hints: tuple[str, ...], *,
                              evidence_query: tuple[str, ...], start_ts=None, end_ts=None,
                              deadline: float) -> list[RawMessageEvidence]:
    """One bounded scoped OR query; no FTS expression comes from an option."""
    hints = validate_retrieval_hints(hints)
    validate_raw_search(scope, "hints", limit=50, start_ts=start_ts, end_ts=end_ts,
                        before=None, deadline=deadline)
    if not hints:
        return []
    steps = 0

    def budget() -> int:
        nonlocal steps
        steps += _SEARCH_PROGRESS_INTERVAL
        return int(steps > _SEARCH_VM_LIMIT or time.monotonic() >= deadline)

    db.set_progress_handler(budget, _SEARCH_PROGRESS_INTERVAL)
    try:
        predicate = " OR ".join("instr(lower(content),?)>0" for _ in hints)
        rows = db.execute("SELECT * FROM raw_messages WHERE agent_id=? AND user_id=? "
            "AND connector_id=? AND conversation_id=? AND status IN ('active','edited') "
            "AND (" + predicate + ") ORDER BY ordinal DESC,session_id DESC,message_id DESC LIMIT 50",
            (scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id, *hints)).fetchall()
        result = []
        size = 0
        for row in rows:
            if time.monotonic() >= deadline:
                raise TimeoutError("hint query deadline exceeded")
            stamp = datetime.fromisoformat(row["created_at"]).timestamp()
            if (start_ts is not None and stamp < start_ts or end_ts is not None and stamp >= end_ts):
                continue
            evidence = raw_evidence(_message(row), evidence_query)
            size += len(json.dumps(asdict(evidence), ensure_ascii=False).encode())
            if size > _MAX_SEARCH_RETURN_BYTES:
                break
            result.append(evidence)
        return result
    finally:
        db.set_progress_handler(None, 0)


def query_raw_content_chunk(db: sqlite3.Connection, scope: Scope, message_id: str, *,
                            expected_revision: int, expected_content_hash: str,
                            offset_chars: int = 0, limit_chars: int = 4096) -> RawContentChunk:
    _scope(scope)
    _id(message_id)
    if scope.connector_id is None or scope.conversation_id is None:
        raise ScopeError("complete conversation scope required")
    if (type(expected_revision) is not int or expected_revision < 1
            or type(expected_content_hash) is not str or len(expected_content_hash) != 64
            or any(char not in "0123456789abcdef" for char in expected_content_hash)
            or type(offset_chars) is not int or offset_chars < 0
            or type(limit_chars) is not int or not 1 <= limit_chars <= 16384):
        raise RawMessageError("invalid content cursor")
    message = query_raw_get(db, scope, message_id)
    if message is None:
        raise ScopeError("original unavailable")
    if (message.revision != expected_revision or message.content_hash != expected_content_hash
            or hashlib.sha256(message.content.encode()).hexdigest() != message.content_hash):
        raise ConflictError("original version conflict")
    if offset_chars > len(message.content):
        raise RawMessageError("invalid content cursor")
    end = min(offset_chars + limit_chars, len(message.content))
    return RawContentChunk(message_id, message.revision, message.content_hash,
                           message.content[offset_chars:end], offset_chars, end,
                           len(message.content), len(message.content.encode()),
                           end == len(message.content))


def query_raw_find(db: sqlite3.Connection, scope: Scope,
                   message_id: str) -> sqlite3.Row | None:
    predicate, params = _predicate(scope)
    return db.execute(
        "SELECT * FROM raw_messages WHERE message_id=? AND " + predicate,
        [message_id, *params],
    ).fetchone()


def query_raw_get(db: sqlite3.Connection, scope: Scope, message_id: str, *,
                  include_inactive: bool = False) -> StoredMessage | None:
    row = query_raw_find(db, scope, message_id)
    if row and (include_inactive or row["status"] in {"active", "edited"}):
        return _message(row)
    return None


def query_raw_results(db: sqlite3.Connection, scope: Scope, message_id: str, *,
                      include_inactive: bool = False) -> list[dict]:
    if query_raw_find(db, scope, message_id) is None:
        return []
    query = """SELECT r.*,d.target_domain,d.revision
        FROM derivation_results r JOIN derivation_outbox d USING(task_id)
        JOIN raw_messages m USING(message_id)
        WHERE m.agent_id=? AND m.user_id=? AND m.message_id=?"""
    if not include_inactive:
        query += (" AND r.status='active' AND m.status IN ('active','edited')"
                  " AND d.revision=m.revision")
    return [dict(row) for row in db.execute(
        query + " ORDER BY d.revision,r.task_id,r.memory_id",
        (scope.agent_id, scope.user_id, message_id),
    )]


def query_raw_source_snapshots(
    db: sqlite3.Connection, scope: Scope, message_ids: tuple[str, ...]
) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
    """Read current originals and their active anchors from one Raw snapshot."""
    _scope(scope)
    if scope.connector_id is None or scope.conversation_id is None:
        raise ScopeError("complete conversation scope required")
    if type(message_ids) is not tuple or len(message_ids) > 256:
        raise RawMessageError("invalid source snapshot batch")
    unique_ids = tuple(dict.fromkeys(_id(message_id) for message_id in message_ids))
    if not db.in_transaction:
        db.execute("BEGIN")
    snapshots = {}
    for message_id in unique_ids:
        message = query_raw_get(db, scope, message_id)
        if message is not None:
            snapshots[message_id] = (
                message, tuple(query_raw_results(db, scope, message_id))
            )
    return snapshots


def query_raw_has_derived_results(db: sqlite3.Connection, scope: Scope) -> bool:
    return db.execute(
        """SELECT 1 FROM derivation_results r
        JOIN derivation_outbox d USING(task_id)
        JOIN raw_messages m USING(message_id)
        WHERE m.agent_id=? AND m.user_id=? AND m.connector_id=?
        AND m.conversation_id=? LIMIT 1""",
        (scope.agent_id, scope.user_id, scope.connector_id,
         scope.conversation_id),
    ).fetchone() is not None


def query_raw_has_succeeded_derivation(db: sqlite3.Connection, scope: Scope) -> bool:
    """Detect a completed extraction even when it legitimately emitted no anchors."""
    params = (scope.agent_id, scope.user_id, scope.connector_id,
              scope.conversation_id)
    if db.execute(
        """SELECT 1 FROM raw_messages m INDEXED BY raw_replay
        JOIN derivation_outbox d ON d.message_id=m.message_id
        WHERE m.agent_id=? AND m.user_id=? AND m.connector_id=?
        AND m.conversation_id=? AND d.status='succeeded' LIMIT 1""",
        params,
    ).fetchone() is not None:
        return True
    # Editing supersedes old tasks. The event is the durable proof that an old
    # zero-result task actually succeeded before it was superseded.
    # The verified schema supplies this unique message/revision index. Forcing
    # scoped messages first prevents a global scan of superseded tasks.
    return db.execute(
        """SELECT 1 FROM raw_messages m INDEXED BY raw_replay
        CROSS JOIN derivation_outbox d INDEXED BY sqlite_autoindex_derivation_outbox_2
        CROSS JOIN derivation_events e INDEXED BY derivation_event_task_action
        WHERE m.agent_id=? AND m.user_id=? AND m.connector_id=?
        AND m.conversation_id=? AND d.message_id=m.message_id
        AND d.status='superseded' AND e.task_id=d.task_id
        AND e.action='succeeded' LIMIT 1""",
        params,
    ).fetchone() is not None


def _publication_deadline(deadline: float | None) -> float:
    if deadline is not None and (type(deadline) not in {int, float}
                                 or not math.isfinite(deadline)):
        raise RawMessageError("invalid publication deadline")
    page_deadline = min(time.monotonic() + _SEARCH_SECONDS_LIMIT,
                        deadline if deadline is not None else math.inf)
    if time.monotonic() >= page_deadline:
        raise RawMessageError("publication budget exceeded")
    return page_deadline


def query_raw_published_snapshots(
    db: sqlite3.Connection, scope: Scope, message_ids: tuple[str, ...], *,
    proofs: dict[str, tuple[tuple[str, str], ...]] | None = None,
    deadline: float | None = None,
    evidence_query: str | tuple[str, ...] | None = None,
) -> dict[str, tuple[StoredMessage | RawMessageEvidence, tuple[dict, ...]]]:
    """Publication authority: current Raw and committed task proofs, one snapshot.

    The 500 bound covers top_k=100 and two neighbors on each side. Unlike the
    legacy source snapshot interface, anchors carry the complete extractor identity.
    """
    _scope(scope)
    if scope.connector_id is None or scope.conversation_id is None:
        raise ScopeError("complete conversation scope required")
    if type(message_ids) is not tuple or len(message_ids) > 500:
        raise RawMessageError("invalid publication snapshot batch")
    unique_ids = tuple(dict.fromkeys(_id(value) for value in message_ids))
    if proofs is not None:
        if type(proofs) is not dict or not set(proofs).issubset(unique_ids):
            raise RawMessageError("invalid publication proofs")
        count = 0
        for message_id, pairs in proofs.items():
            _id(message_id)
            if type(pairs) is not tuple:
                raise RawMessageError("invalid publication proofs")
            count += len(pairs)
            if count > 4096:
                raise RawMessageError("publication proof budget exceeded")
            for pair in pairs:
                if type(pair) is not tuple or len(pair) != 2:
                    raise RawMessageError("invalid publication proofs")
                _id(pair[0])
                _id(pair[1])
    page_deadline = _publication_deadline(deadline)
    steps = 0
    previous_busy = int(db.execute("PRAGMA busy_timeout").fetchone()[0])
    remaining_ms = max(1, math.ceil((page_deadline - time.monotonic()) * 1000))
    db.execute(f"PRAGMA busy_timeout={min(previous_busy, remaining_ms)}")

    def budget() -> int:
        nonlocal steps
        steps += _SEARCH_PROGRESS_INTERVAL
        return int(steps > _SEARCH_VM_LIMIT or time.monotonic() >= page_deadline)

    db.set_progress_handler(budget, _SEARCH_PROGRESS_INTERVAL)
    try:
        if not db.in_transaction:
            db.execute("BEGIN")
        snapshots: dict[str, tuple[StoredMessage | RawMessageEvidence, tuple[dict, ...]]] = {}
        returned_bytes = 0
        returned_proofs = 0
        for message_id in unique_ids:
            if time.monotonic() >= page_deadline:
                raise RawMessageError("publication budget exceeded")
            stored = query_raw_get(db, scope, message_id)
            if stored is None:
                continue
            message: StoredMessage | RawMessageEvidence = (
                raw_evidence(stored, evidence_query) if evidence_query is not None else stored)
            returned_bytes += len(json.dumps(asdict(message), ensure_ascii=False,
                                             default=str).encode("utf-8"))
            if returned_bytes > _MAX_SEARCH_RETURN_BYTES:
                raise RawMessageError("publication byte budget exceeded")
            selected = proofs.get(message_id, ()) if proofs is not None else None
            if selected == ():
                snapshots[message_id] = (message, ())
                continue
            statement = """SELECT r.*, d.target_domain, d.revision, d.processor,
                                  d.processor_version, d.model, d.prompt_hash
                           FROM derivation_results r JOIN derivation_outbox d USING(task_id)
                           WHERE d.message_id=? AND d.revision=?
                             AND d.status='succeeded' AND r.status='active'"""
            if selected is None:
                cursor = db.execute(statement + " LIMIT ?",
                                    (message_id, message.revision,
                                     4097 - returned_proofs))
                rows: Iterator[sqlite3.Row] = cursor
            else:
                rows = (
                    row for task_id, memory_id in selected
                    for row in db.execute(statement +
                        " AND r.task_id=? AND r.memory_id=?",
                        (message_id, message.revision, task_id, memory_id))
                )
            anchors = []
            for row in rows:
                if time.monotonic() >= page_deadline:
                    raise RawMessageError("publication budget exceeded")
                returned_proofs += 1
                if returned_proofs > 4096:
                    raise RawMessageError("publication proof budget exceeded")
                anchor = dict(row)
                returned_bytes += len(json.dumps(anchor, ensure_ascii=False,
                                                 default=str).encode("utf-8"))
                if returned_bytes > _MAX_SEARCH_RETURN_BYTES:
                    raise RawMessageError("publication byte budget exceeded")
                anchors.append(anchor)
            snapshots[message_id] = (message, tuple(anchors))
        return snapshots
    except sqlite3.OperationalError as exc:
        if (str(exc) == "interrupted" or steps > _SEARCH_VM_LIMIT
                or time.monotonic() >= page_deadline):
            raise RawMessageError("publication budget exceeded") from None
        raise
    finally:
        db.set_progress_handler(None, 0)
        db.execute(f"PRAGMA busy_timeout={previous_busy}")


def query_raw_partition_state(db: sqlite3.Connection, scope: Scope) -> tuple[int, bool]:
    if not db.in_transaction:
        db.execute("BEGIN")
    row = db.execute(
        "SELECT e.generation FROM raw_partition_epochs e "
        "JOIN conversations c USING(conversation_id) "
        "WHERE c.agent_id=? AND c.user_id=? AND c.connector_id=? "
        "AND c.conversation_id=?",
        (scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id),
    ).fetchone()
    if row is None:
        return 0, True
    pending = db.execute(
        """SELECT 1 FROM derivation_outbox d
        JOIN raw_messages m USING(message_id)
        WHERE m.agent_id=? AND m.user_id=? AND m.connector_id=?
        AND m.conversation_id=? AND m.status IN ('active','edited')
        AND d.revision=m.revision AND d.status<>'succeeded' LIMIT 1""",
        (scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id),
    ).fetchone()
    return int(row[0]), pending is None


def query_raw_neighbors(db: sqlite3.Connection, scope: Scope, message_id: str, *,
                        before: int = 2, after: int = 2) -> list[StoredMessage]:
    if not db.in_transaction:
        db.execute("BEGIN")
    row = query_raw_find(db, scope, message_id)
    if not row or row["status"] not in {"active", "edited"}:
        return []
    query = """SELECT * FROM raw_messages WHERE agent_id=? AND user_id=? AND conversation_id=?
        AND session_id=? AND status IN ('active','edited') AND ordinal"""
    params = (
        scope.agent_id, scope.user_id, row["conversation_id"],
        row["session_id"], row["ordinal"],
    )
    previous = db.execute(
        query + "<? ORDER BY ordinal DESC LIMIT ?", (*params, before),
    ).fetchall()
    following = db.execute(
        query + ">? ORDER BY ordinal LIMIT ?", (*params, after),
    ).fetchall()
    return [_message(item) for item in [*reversed(previous), row, *following]]


def validate_raw_search(scope: Scope, query: str, *, limit: int,
                        start_ts: float | None, end_ts: float | None,
                        before: tuple[int, str, str] | None,
                        deadline: float | None) -> str:
    _scope(scope)
    if scope.connector_id is None or scope.conversation_id is None:
        raise ScopeError("complete conversation scope required")
    if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
            or type(limit) is not int or not 1 <= limit <= 50):
        raise RawMessageError("invalid search request")
    if any(value is not None and (type(value) not in {int, float}
                                  or not math.isfinite(value) or value < 0)
           for value in (start_ts, end_ts)):
        raise RawMessageError("invalid search time")
    if deadline is not None and (type(deadline) not in {int, float}
                                 or not math.isfinite(deadline)):
        raise RawMessageError("invalid search deadline")
    if deadline is not None and time.monotonic() >= deadline:
        raise RawMessageError("search budget exceeded")
    if before is not None and (type(before) is not tuple or len(before) != 3
                               or type(before[0]) is not int or before[0] < 1
                               or type(before[1]) is not str
                               or type(before[2]) is not str):
        raise RawMessageError("invalid search cursor")
    return query.strip()


def query_raw_search_page(db: sqlite3.Connection, scope: Scope, needle: str, *,
                          limit: int, start_ts: float | None,
                          end_ts: float | None,
                          before: tuple[int, str, str] | None,
                          deadline: float | None,
                          evidence_query: str | tuple[str, ...] | None = None
                          ) -> list[StoredMessage] | list[RawMessageEvidence]:
    """Bounded scoped SQL shared by the writer's reader and existing-file view."""
    page_deadline = time.monotonic() + _SEARCH_SECONDS_LIMIT
    if deadline is not None:
        page_deadline = min(page_deadline, deadline)
    steps = 0

    def budget() -> int:
        nonlocal steps
        steps += _SEARCH_PROGRESS_INTERVAL
        return int(steps > _SEARCH_VM_LIMIT or time.monotonic() > page_deadline)

    db.set_progress_handler(budget, _SEARCH_PROGRESS_INTERVAL)
    try:
        scoped = (scope.agent_id, scope.user_id, scope.connector_id,
                  scope.conversation_id)
        predicate = ("agent_id=? AND user_id=? AND connector_id=? "
                     "AND conversation_id=? AND status IN ('active','edited')")
        locator_params: tuple[str, ...] = ()
        locator = ""
        if len(needle) >= 3:
            locator = " AND rowid IN (SELECT rowid FROM raw_search WHERE raw_search MATCH ?)"
            locator_params = ('"' + needle.replace('"', '""') + '"',)
            candidates = db.execute(
                "SELECT count(*) FROM (SELECT 1 FROM raw_messages WHERE "
                + predicate + locator + " LIMIT ?)",
                (*scoped, *locator_params, _MAX_SEARCH_CANDIDATES + 1),
            ).fetchone()[0]
            if candidates > _MAX_SEARCH_CANDIDATES:
                raise RawMessageError("search candidates too large")
        time_filter = ""
        time_params: list[int] = []
        if start_ts is not None:
            time_filter += " AND unixepoch(created_at)>=?"
            time_params.append(math.floor(start_ts))
        if end_ts is not None:
            time_filter += " AND unixepoch(created_at)<?"
            time_params.append(math.ceil(end_ts))
        cursor_filter = ""
        cursor_params: tuple[int, str, str] | tuple[()] = ()
        if before is not None:
            cursor_filter = " AND (ordinal,session_id,message_id)<(?,?,?)"
            cursor_params = before
        result: list = []
        return_bytes = 0
        scanned = 0
        while len(result) < limit:
            if time.monotonic() >= page_deadline:
                raise RawMessageError("search budget exceeded")
            rows = db.execute(
                "SELECT * FROM raw_messages WHERE " + predicate
                + " AND instr(lower(content),lower(?))>0"
                + locator + time_filter + cursor_filter
                + " ORDER BY ordinal DESC,session_id DESC,message_id DESC LIMIT ?",
                (*scoped, needle, *locator_params, *time_params,
                 *cursor_params, min(50, _MAX_SEARCH_SCAN - scanned)),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                if time.monotonic() >= page_deadline:
                    raise RawMessageError("search budget exceeded")
                stamp = datetime.fromisoformat(row["created_at"]).timestamp()
                if ((start_ts is not None and stamp < start_ts)
                        or (end_ts is not None and stamp >= end_ts)):
                    continue
                message = _message(row)
                item = raw_evidence(message, evidence_query) if evidence_query is not None else message
                return_bytes += (len(json.dumps(asdict(item), ensure_ascii=False).encode())
                                 if evidence_query is not None else len(message.content.encode()))
                if return_bytes > _MAX_SEARCH_RETURN_BYTES:
                    raise RawMessageError("search response too large")
                result.append(item)
                if len(result) >= limit:
                    break
            scanned += len(rows)
            if scanned >= _MAX_SEARCH_SCAN and len(result) < limit:
                raise RawMessageError("raw candidate scan budget exceeded")
            if len(rows) < 50:
                break
            last = rows[-1]
            cursor_filter = " AND (ordinal,session_id,message_id)<(?,?,?)"
            cursor_params = (last["ordinal"], last["session_id"],
                             last["message_id"])
        if time.monotonic() >= page_deadline:
            raise RawMessageError("search budget exceeded")
        return result
    except sqlite3.OperationalError as exc:
        if (str(exc) == "interrupted" or steps > _SEARCH_VM_LIMIT
                or time.monotonic() > page_deadline):
            raise RawMessageError("search budget exceeded") from None
        raise
    finally:
        db.set_progress_handler(None, 0)


class RawMessageStore:
    @staticmethod
    def migrate_database(database: str | Path, backup_destination: str | Path) -> dict:
        """Explicitly migrate a known legacy database, retaining a verified backup."""
        return _RawDatabase.migrate_legacy(database, backup_destination)

    def __init__(self, database: str | Path):
        self._database = _RawDatabase(database)
        self.database = self._database.path

    def _db(self, *, write: bool = False,
            deadline: float | None = None,
            _on_connection: Callable[[sqlite3.Connection], None] | None = None
            ) -> AbstractContextManager[sqlite3.Connection]:
        return self._database.connection(write=write, deadline=deadline, _on_connection=_on_connection)

    def register_connector(
        self,
        scope: Scope,
        connector_id: str,
        kind: str,
        account_ref: str,
        display_name: str,
    ) -> None:
        with self._db(write=True) as db:
            _registry.register_connector(
                db, scope, connector_id, kind, account_ref, display_name
            )

    def register_conversation(
        self,
        scope: Scope,
        conversation_id: str,
        connector_id: str,
        provider_conversation_id: str,
        conversation_type: str,
        display_name: str,
    ) -> None:
        with self._db(write=True) as db:
            _registry.register_conversation(
                db,
                scope,
                conversation_id,
                connector_id,
                provider_conversation_id,
                conversation_type,
                display_name,
            )

    def list_connectors(self, scope: Scope) -> list[dict]:
        with self._db() as db:
            return _registry.list_connectors(db, scope)

    def list_conversations(
        self, scope: Scope, *, connector_id: str | None = None
    ) -> list[dict]:
        with self._db() as db:
            return _registry.list_conversations(db, scope, connector_id=connector_id)

    def append(self, scope: Scope, draft: MessageDraft) -> StoredMessage:
        with self._db(write=True) as db:
            reuse_received_time = (draft.role == "user" and draft.source == "feishu"
                                   and draft.metadata.get("provider_timestamp_limited") is True)
            message, _ = self._append_record(db, scope, draft,
                                             reuse_created_at=reuse_received_time)
            return message

    def append_outbound_once(self, scope: Scope, draft: MessageDraft, *,
                             claim_outbound: bool = True) -> tuple[StoredMessage, bool]:
        """Atomically persist an assistant reply and grant one automatic send attempt."""
        if draft.role != "assistant":
            raise RawMessageError("assistant reply required")
        with self._db(write=True) as db:
            message, _ = self._append_record(db, scope, draft, reuse_created_at=True)
            return message, self._claim_outbound(db, message) if claim_outbound else False

    def _claim_outbound(self, db: sqlite3.Connection, message: StoredMessage) -> bool:
        if message.status != "active":
            raise ConflictError("outbound unavailable")
        if db.execute("SELECT 1 FROM raw_message_events WHERE message_id=? AND action='business:outbound_claimed'",
                      (message.message_id,)).fetchone():
            return False
        self._event(db, message.message_id, "business:outbound_claimed",
                    message.revision, message.status, message.status)
        return True

    def append_for_parent(self, scope: Scope, draft: MessageDraft, parent_id: str,
                          revision: int, *, outbound: bool = False,
                          claim_outbound: bool = True) -> tuple[StoredMessage, bool]:
        """Validate the source revision and append under the same write transaction."""
        with self._db(write=True) as db:
            parent = self._owned(db, scope, parent_id)
            if (parent["role"] != "user" or parent["status"] != "active"
                    or parent["revision"] != revision
                    or parent["conversation_id"] != draft.conversation_id
                    or draft.reply_to_message_id != parent_id):
                raise ConflictError("turn source unavailable")
            if outbound and draft.role != "assistant":
                raise RawMessageError("assistant reply required")
            message, inserted = self._append_record(db, scope, draft, reuse_created_at=outbound)
            fresh = self._claim_outbound(db, message) if outbound and claim_outbound else False
            return message, fresh if outbound else inserted

    def claim_input_dispatch(self, scope: Scope, message_id: str) -> bool:
        """At most one channel retry may enter the automatic business scheduler."""
        _scope(scope)
        _id(message_id)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["role"] != "user" or row["status"] != "active":
                raise ConflictError("input not dispatchable")
            exists = db.execute(
                "SELECT 1 FROM raw_message_events WHERE message_id=? AND action='business:input_claimed'",
                (message_id,),
            ).fetchone()
            if exists:
                return False
            self._event(db, message_id, "business:input_claimed",
                        row["revision"], row["status"], row["status"])
            return True

    def mark_input_dispatched(self, scope: Scope, message_id: str) -> None:
        """Report a completed in-process queue handoff without reopening the claim."""
        _scope(scope)
        _id(message_id)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if not db.execute(
                "SELECT 1 FROM raw_message_events WHERE message_id=? AND action='business:input_claimed'",
                (message_id,),
            ).fetchone():
                raise ConflictError("input dispatch not claimed")
            if not db.execute(
                "SELECT 1 FROM raw_message_events WHERE message_id=? AND action='business:input_dispatched'",
                (message_id,),
            ).fetchone():
                self._event(db, message_id, "business:input_dispatched",
                            row["revision"], row["status"], row["status"])

    def _append_record(self, db: sqlite3.Connection, scope: Scope, draft: MessageDraft,
                       *, reuse_created_at: bool = False) -> tuple[StoredMessage, bool]:
        conversation = self._conversation_for_draft(db, scope, draft)
        existing = db.execute(
            """SELECT * FROM raw_messages WHERE message_id=? OR
            (agent_id=? AND user_id=? AND connector_id=? AND idempotency_key=?)""",
            (
                draft.message_id,
                scope.agent_id,
                scope.user_id,
                conversation["connector_id"],
                draft.idempotency_key,
            ),
        ).fetchall()
        if reuse_created_at and len(existing) == 1 and existing[0]["message_id"] == draft.message_id:
            draft = replace(draft, created_at=existing[0]["created_at"])
        content_hash, metadata, attachment_refs, payload_hash = _draft_payload(scope, draft)
        if existing:
            if len(existing) != 1 or existing[0]["payload_hash"] != payload_hash:
                raise ConflictError("message identity conflict")
            return _message(existing[0]), False
        self._register_attachments(db, scope, draft.attachment_refs)
        self._insert_original(
            db,
            scope,
            draft,
            conversation,
            content_hash,
            metadata,
            attachment_refs,
            payload_hash,
        )
        db.execute(
            "INSERT INTO raw_message_revisions VALUES(?,1,?,?,NULL)",
            (draft.message_id, draft.content, content_hash),
        )
        self._event(db, draft.message_id, "created", 1, None, "active")
        _outbox.insert_tasks(db, draft.message_id, 1, draft.derivations)
        return _message(
            db.execute(
                "SELECT * FROM raw_messages WHERE message_id=?", (draft.message_id,)
            ).fetchone()
        ), True

    @staticmethod
    def _conversation_for_draft(
        db: sqlite3.Connection, scope: Scope, draft: MessageDraft
    ) -> sqlite3.Row:
        conversation = db.execute(
            "SELECT * FROM conversations WHERE conversation_id=? AND agent_id=? AND user_id=?",
            (draft.conversation_id, scope.agent_id, scope.user_id),
        ).fetchone()
        if not conversation:
            raise ScopeError("conversation unavailable in scope")
        if not _limits(
            scope, conversation["connector_id"], conversation["conversation_id"]
        ):
            raise ScopeError("conversation unavailable in scope")
        if draft.reply_to_message_id is not None:
            _id(draft.reply_to_message_id)
            if not db.execute(
                """SELECT 1 FROM raw_messages WHERE message_id=?
                AND agent_id=? AND user_id=? AND conversation_id=?""",
                (
                    draft.reply_to_message_id,
                    scope.agent_id,
                    scope.user_id,
                    draft.conversation_id,
                ),
            ).fetchone():
                raise ScopeError("reply unavailable in conversation")
        return conversation

    @staticmethod
    def _insert_original(
        db: sqlite3.Connection,
        scope: Scope,
        draft: MessageDraft,
        conversation: sqlite3.Row,
        content_hash: str,
        metadata: str,
        attachment_refs: str,
        payload_hash: str,
    ) -> None:
        ordinal = db.execute(
            "SELECT COALESCE(MAX(ordinal),0)+1 FROM raw_messages WHERE conversation_id=?",
            (draft.conversation_id,),
        ).fetchone()[0]
        db.execute(
            """INSERT INTO raw_messages(
            message_id,agent_id,user_id,connector_id,conversation_id,session_id,ordinal,
            role,content,content_type,created_at,received_at,source,sender_id,content_hash,
            reply_to_message_id,metadata_json,idempotency_key,payload_hash,attachment_refs_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                draft.message_id,
                scope.agent_id,
                scope.user_id,
                conversation["connector_id"],
                draft.conversation_id,
                draft.session_id,
                ordinal,
                draft.role,
                draft.content,
                draft.content_type,
                draft.created_at,
                _now(),
                draft.source,
                draft.sender_id,
                content_hash,
                draft.reply_to_message_id,
                metadata,
                draft.idempotency_key,
                payload_hash,
                attachment_refs,
            ),
        )
        db.execute(
            "INSERT INTO raw_search(rowid,content) "
            "SELECT rowid,content FROM raw_messages WHERE message_id=?",
            (draft.message_id,),
        )

    @staticmethod
    def _register_attachments(
        db: sqlite3.Connection, scope: Scope, refs: tuple[AttachmentRef, ...]
    ) -> None:
        for ref in refs:
            existing = db.execute(
                "SELECT * FROM attachments WHERE attachment_id=?", (ref.attachment_id,)
            ).fetchone()
            if existing:
                if (existing["agent_id"], existing["user_id"]) != (
                    scope.agent_id,
                    scope.user_id,
                ):
                    raise ScopeError("attachment unavailable in scope")
                if (
                    existing["content_type"],
                    existing["sha256"],
                    existing["size_bytes"],
                ) != (ref.content_type, ref.sha256, ref.size_bytes):
                    raise ConflictError("attachment identity conflict")
            else:
                db.execute(
                    "INSERT INTO attachments VALUES(?,?,?,?,?,?)",
                    (
                        ref.attachment_id,
                        scope.agent_id,
                        scope.user_id,
                        ref.content_type,
                        ref.sha256,
                        ref.size_bytes,
                    ),
                )

    @staticmethod
    def _event(
        db: sqlite3.Connection,
        message_id: str,
        action: str,
        revision: int,
        old_status: str | None,
        new_status: str,
    ) -> None:
        db.execute(
            """INSERT INTO raw_message_events(message_id,action,revision,old_status,new_status,occurred_at)
            VALUES(?,?,?,?,?,?)""",
            (message_id, action, revision, old_status, new_status, _now()),
        )

    @staticmethod
    def _find(
        db: sqlite3.Connection, scope: Scope, message_id: str
    ) -> sqlite3.Row | None:
        return query_raw_find(db, scope, message_id)

    @classmethod
    def _owned(
        cls, db: sqlite3.Connection, scope: Scope, message_id: str
    ) -> sqlite3.Row:
        row = cls._find(db, scope, message_id)
        if not row:
            raise ScopeError("message unavailable in scope")
        return row

    def edit(
        self,
        scope: Scope,
        message_id: str,
        content: str,
        *,
        expected_revision: int,
        edited_at: str,
    ) -> StoredMessage:
        _scope(scope)
        _id(message_id)
        content_hash = _content(content)
        _timestamp(edited_at)
        if type(expected_revision) is not int or expected_revision < 1:
            raise RawMessageError("invalid expected revision")
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["revision"] != expected_revision or row["status"] not in {
                "active",
                "edited",
            }:
                raise ConflictError("message revision conflict")
            if _timestamp(edited_at) < _timestamp(
                row["edited_at"] or row["created_at"]
            ):
                raise RawMessageError("edit precedes message version")
            revision = row["revision"] + 1
            specs = _outbox.specs_for_revision(db, message_id, row["revision"])
            _outbox.supersede(db, message_id)
            rowid = db.execute(
                "SELECT rowid FROM raw_messages WHERE message_id=?", (message_id,)
            ).fetchone()[0]
            db.execute(
                "INSERT INTO raw_search(raw_search,rowid,content) VALUES('delete',?,?)",
                (rowid, row["content"]),
            )
            db.execute(
                """UPDATE raw_messages SET content=?,content_hash=?,edited_at=?,revision=?,status='edited'
                WHERE message_id=?""",
                (content, content_hash, edited_at, revision, message_id),
            )
            db.execute(
                "INSERT INTO raw_search(rowid,content) VALUES(?,?)", (rowid, content)
            )
            db.execute(
                "INSERT INTO raw_message_revisions VALUES(?,?,?,?,?)",
                (message_id, revision, content, content_hash, edited_at),
            )
            self._event(db, message_id, "edited", revision, row["status"], "edited")
            _outbox.insert_tasks(db, message_id, revision, specs)
            return _message(self._owned(db, scope, message_id))

    def revisions(
        self, scope: Scope, message_id: str, *, include_inactive: bool = False
    ) -> list[dict]:
        _scope(scope)
        _id(message_id)
        with self._db() as db:
            if not self._find(db, scope, message_id):
                return []
            query = """SELECT r.* FROM raw_message_revisions r JOIN raw_messages m USING(message_id)
                WHERE m.message_id=? AND m.agent_id=? AND m.user_id=?"""
            if not include_inactive:
                query += " AND m.status IN ('active','edited')"
            return [
                dict(row)
                for row in db.execute(
                    query + " ORDER BY r.revision",
                    (message_id, scope.agent_id, scope.user_id),
                )
            ]

    def message_events(self, scope: Scope, message_id: str) -> list[dict]:
        _scope(scope)
        _id(message_id)
        with self._db() as db:
            if not self._find(db, scope, message_id):
                return []
            return [
                dict(row)
                for row in db.execute(
                    """SELECT e.* FROM raw_message_events e
                JOIN raw_messages m USING(message_id) WHERE m.message_id=? AND m.agent_id=?
                AND m.user_id=? ORDER BY e.event_id""",
                    (message_id, scope.agent_id, scope.user_id),
                )
            ]

    def business_input_states(self, scope: Scope) -> list[dict[str, str]]:
        """Audit original Feishu eligibility; edits do not rewrite handoff history."""
        _scope(scope)
        with self._db() as db:
            db.execute("BEGIN")
            predicate, params = _predicate(scope, "m")
            rows = db.execute(
                """SELECT m.message_id,r.content,r.content_hash,m.metadata_json,
                    m.revision,m.content AS current_content,m.content_hash AS current_hash,
                    MAX(CASE WHEN e.action='business:input_claimed' THEN 1 ELSE 0 END) claimed,
                    MAX(CASE WHEN e.action='business:input_dispatched' THEN 1 ELSE 0 END) dispatched
                    FROM raw_messages m
                    LEFT JOIN raw_message_revisions r
                    ON r.message_id=m.message_id AND r.revision=1
                    LEFT JOIN raw_message_events e
                    ON e.message_id=m.message_id WHERE """ + predicate +
                """ AND m.role='user' AND m.source='feishu'
                    AND m.status IN ('active','edited')
                    GROUP BY m.message_id ORDER BY m.conversation_id,m.ordinal""",
                params,
            )
            states = []
            # Stream bodies in the same read snapshot instead of retaining every body.
            for row in rows:
                self._check_handoff_original(row)
                metadata = json.loads(row["metadata_json"])
                if extract_feishu_business_text(
                        metadata.get("provider_message_type"), row["content"],
                        provider_limited=metadata.get("provider_limited", False),
                        provider_timestamp_limited=metadata.get("provider_timestamp_limited", False)) is None:
                    continue
                state = ("queue_dispatched" if row["dispatched"] else
                         "claimed_unknown" if row["claimed"] else "saved_unattempted")
                states.append({"message_id": row["message_id"], "state": state})
        return states

    @staticmethod
    def _check_handoff_original(row: sqlite3.Row) -> None:
        original = row["content"]
        if not isinstance(original, str) or _hash(original) != row["content_hash"]:
            raise RawMessageError("original handoff evidence unavailable")
        if row["revision"] == 1 and (
            original != row["current_content"] or row["content_hash"] != row["current_hash"]
        ):
            raise RawMessageError("original handoff evidence unavailable")

    def forget(self, scope: Scope, message_id: str) -> None:
        _scope(scope)
        _id(message_id)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["status"] == "forgotten":
                return
            if row["status"] not in {"active", "edited"}:
                raise ConflictError("message lifecycle conflict")
            db.execute(
                "UPDATE raw_messages SET status='forgotten' WHERE message_id=?",
                (message_id,),
            )
            db.execute(
                """UPDATE derivation_results SET status='forgotten' WHERE status='active' AND task_id IN
                (SELECT task_id FROM derivation_outbox WHERE message_id=?)""",
                (message_id,),
            )
            for task in db.execute(
                "SELECT task_id FROM derivation_outbox WHERE message_id=? AND status IN ('pending','leased')",
                (message_id,),
            ).fetchall():
                db.execute(
                    "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'blocked',?)",
                    (task["task_id"], time.time()),
                )
            db.execute(
                """UPDATE derivation_outbox SET status='blocked',lease_token=NULL,lease_owner=NULL,
                lease_until=NULL,updated_at=? WHERE message_id=? AND status IN ('pending','leased')""",
                (_now(), message_id),
            )
            self._event(
                db, message_id, "forgotten", row["revision"], row["status"], "forgotten"
            )

    def retract(self, scope: Scope, message_id: str) -> None:
        """Hide a withdrawn message permanently from daily recall, retaining audit."""
        _scope(scope)
        _id(message_id)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["status"] == "retracted":
                return
            if row["status"] not in {"active", "edited", "forgotten"}:
                raise ConflictError("message lifecycle conflict")
            _outbox.supersede(db, message_id)
            db.execute("UPDATE raw_messages SET status='retracted' WHERE message_id=?",
                       (message_id,))
            self._event(db, message_id, "retracted", row["revision"], row["status"],
                        "retracted")

    @staticmethod
    def _purge_key(scope: Scope, message_id: str) -> str:
        _scope(scope)
        _id(message_id)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        return _hash(json.dumps((scope.agent_id, scope.user_id, scope.connector_id,
                                 scope.conversation_id, message_id), separators=(",", ":")))

    def purge_receipt(self, scope: Scope, message_id: str) -> dict | None:
        key = self._purge_key(scope, message_id)
        with self._db() as db:
            row = db.execute("SELECT purged_at,result_count FROM purge_receipts "
                             "WHERE receipt_key=?", (key,)).fetchone()
            return ({"status": "live_stores_cleared", "purged_at": row[0],
                     "result_count": row[1]} if row else None)

    def begin_purge(self, scope: Scope, message_id: str, *, expected_revision: int) -> dict:
        """Fence recall and workers before cross-store cleanup; retry is safe."""
        self._purge_key(scope, message_id)
        if type(expected_revision) is not int or expected_revision < 1:
            raise RawMessageError("invalid expected revision")
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["revision"] != expected_revision:
                raise ConflictError("message revision conflict")
            if row["status"] not in {"active", "edited", "forgotten", "retracted", "deleting"}:
                raise ConflictError("message lifecycle conflict")
            if json.loads(row["attachment_refs_json"]):
                raise RawMessageError("attachment purge adapter required")
            if db.execute("SELECT 1 FROM raw_messages WHERE reply_to_message_id=? LIMIT 1",
                          (message_id,)).fetchone():
                raise ConflictError("reply closure must be cleared first")
            if row["status"] != "deleting":
                _outbox.supersede(db, message_id)
                db.execute("UPDATE raw_messages SET status='deleting' WHERE message_id=?",
                           (message_id,))
                self._event(db, message_id, "purge_started", row["revision"],
                            row["status"], "deleting")
            task_rows = tuple(db.execute(
                "SELECT task_id,attempts,target_domain,revision,processor,"
                "processor_version,model,prompt_hash FROM derivation_outbox "
                "WHERE message_id=? ORDER BY task_id",
                (message_id,),
            ))
            tasks = tuple(item[0] for item in task_rows)
            result_rows = tuple(db.execute(
                """SELECT r.memory_id,r.store_ref,d.target_domain,r.task_id FROM derivation_results r
                JOIN derivation_outbox d USING(task_id) WHERE d.message_id=?
                ORDER BY r.memory_id""", (message_id,),
            ))
            results = tuple((item[0], item[1], item[2]) for item in result_rows)
            return {"revision": row["revision"], "task_ids": tasks,
                    "task_domains": tuple((item[0], item[2]) for item in task_rows),
                    "task_sources": tuple((item[0], SourceRevision(
                        message_id, item[3], item[0], item[4], item[5], item[6], item[7]))
                        for item in task_rows),
                    "results": results,
                    "result_tasks": tuple((item[0], item[3]) for item in result_rows),
                    "attempted_domains": tuple(item[2] for item in task_rows
                                               if item[1] > 0)}

    def finish_purge(self, scope: Scope, message_id: str,
                     *, proof: _PurgeProof | None = None) -> dict:
        """The former public finalizer cannot attest cross-store cleanup."""
        raise ConflictError("coordinator required")

    def _finish_purge_verified(self, scope: Scope, message_id: str,
                               *, proof: _PurgeProof) -> dict:
        """Remove the live original only after external stores have been cleared."""
        _scope(scope)
        connector_id, conversation_id = scope.connector_id, scope.conversation_id
        if connector_id is None or conversation_id is None:
            raise ScopeError("complete conversation scope required")
        partition = PipelineScope(scope.user_id, scope.agent_id, connector_id,
                                  conversation_id)
        held_for = getattr(proof.gate, "held_for", None) if type(proof) is _PurgeProof else None
        if held_for is None or not held_for(partition):
            raise ConflictError("cleanup proof required")
        key = self._purge_key(scope, message_id)
        with self._db(write=True) as db:
            prior = db.execute("SELECT purged_at,result_count FROM purge_receipts "
                               "WHERE receipt_key=?", (key,)).fetchone()
            if prior:
                return {"status": "live_stores_cleared", "purged_at": prior[0],
                        "result_count": prior[1]}
            row = self._owned(db, scope, message_id)
            if row["status"] != "deleting" or json.loads(row["attachment_refs_json"]):
                raise ConflictError("purge not ready")
            tasks = tuple(item[0] for item in db.execute(
                "SELECT task_id FROM derivation_outbox WHERE message_id=? ORDER BY task_id",
                (message_id,),
            ))
            results = tuple((item[0], item[1], item[2]) for item in db.execute(
                """SELECT r.memory_id,r.store_ref,d.target_domain FROM derivation_results r
                JOIN derivation_outbox d USING(task_id) WHERE d.message_id=?
                ORDER BY r.memory_id""", (message_id,),
            ))
            if (proof.receipt_key != key or proof.revision != row["revision"]
                    or proof.task_ids != tasks or proof.results != results):
                raise ConflictError("cleanup proof mismatch")
            rowid = db.execute("SELECT rowid FROM raw_messages WHERE message_id=?",
                               (message_id,)).fetchone()[0]
            result_count = db.execute(
                """SELECT count(*) FROM derivation_results r JOIN derivation_outbox d
                USING(task_id) WHERE d.message_id=?""", (message_id,),
            ).fetchone()[0]
            db.execute("INSERT INTO raw_search(raw_search,rowid,content) VALUES('delete',?,?)",
                       (rowid, row["content"]))
            for table in ("derivation_events", "derivation_results"):
                db.execute(f"DELETE FROM {table} WHERE task_id IN "
                           "(SELECT task_id FROM derivation_outbox WHERE message_id=?)",
                           (message_id,))
            db.execute("DELETE FROM derivation_outbox WHERE message_id=?", (message_id,))
            db.execute("DELETE FROM raw_message_events WHERE message_id=?", (message_id,))
            db.execute("DELETE FROM raw_message_revisions WHERE message_id=?", (message_id,))
            db.execute("DELETE FROM raw_messages WHERE message_id=?", (message_id,))
            at = _now()
            db.execute("INSERT INTO purge_receipts VALUES(?,?,?)",
                       (key, at, result_count))
            return {"status": "live_stores_cleared", "purged_at": at,
                    "result_count": result_count}

    def restore(self, scope: Scope, message_id: str) -> None:
        _scope(scope)
        _id(message_id)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["status"] in {"active", "edited"}:
                return
            if row["status"] != "forgotten":
                raise ConflictError("message lifecycle conflict")
            previous = db.execute(
                """SELECT old_status FROM raw_message_events WHERE message_id=?
                AND action='forgotten' ORDER BY event_id DESC LIMIT 1""",
                (message_id,),
            ).fetchone()
            if not previous or previous["old_status"] not in {"active", "edited"}:
                raise ConflictError("restore history unavailable")
            db.execute(
                "UPDATE raw_messages SET status=? WHERE message_id=?",
                (previous["old_status"], message_id),
            )
            db.execute(
                """UPDATE derivation_results SET status='active' WHERE status='forgotten' AND task_id IN
                (SELECT task_id FROM derivation_outbox WHERE message_id=? AND revision=?)""",
                (message_id, row["revision"]),
            )
            for task in db.execute(
                "SELECT task_id FROM derivation_outbox WHERE message_id=? AND revision=? AND status='blocked'",
                (message_id, row["revision"]),
            ).fetchall():
                db.execute(
                    "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'unblocked',?)",
                    (task["task_id"], time.time()),
                )
            db.execute(
                """UPDATE derivation_outbox SET status='pending',updated_at=? WHERE message_id=?
                AND revision=? AND status='blocked'""",
                (_now(), message_id, row["revision"]),
            )
            self._event(
                db,
                message_id,
                "restored",
                row["revision"],
                "forgotten",
                previous["old_status"],
            )

    def enqueue_derivations(
        self, scope: Scope, message_id: str, specs: tuple[DerivationSpec, ...]
    ) -> None:
        """Explicit reprocessing; a version cannot silently change model or prompt."""
        _scope(scope)
        _id(message_id)
        _specs(specs)
        with self._db(write=True) as db:
            row = self._owned(db, scope, message_id)
            if row["status"] not in {"active", "edited"}:
                raise ConflictError("message unavailable for derivation")
            _outbox.insert_tasks(db, message_id, row["revision"], specs)

    def list_derivations(self, scope: Scope, message_id: str) -> list[dict]:
        _scope(scope)
        _id(message_id)
        with self._db() as db:
            if not self._find(db, scope, message_id):
                return []
            return [
                dict(row)
                for row in db.execute(
                    """SELECT d.* FROM derivation_outbox d
                JOIN raw_messages m USING(message_id) WHERE m.agent_id=? AND m.user_id=?
                AND m.message_id=? ORDER BY d.revision,d.task_id""",
                    (scope.agent_id, scope.user_id, message_id),
                )
            ]

    def derivation_sources(self, scope: Scope, task_ids: tuple[str, ...]
                           ) -> dict[str, tuple[SourceRevision, str]]:
        """Read an exact page of Outbox source identities in one conversation."""
        _scope(scope)
        if (scope.connector_id is None or scope.conversation_id is None
                or type(task_ids) is not tuple or not 1 <= len(task_ids) <= 256):
            raise ScopeError("complete derivation inventory scope required")
        for task_id in task_ids:
            identity(task_id)
        marks = ",".join("?" for _ in task_ids)
        with self._db() as db:
            rows = db.execute(
                "SELECT d.task_id,d.message_id,d.revision,d.processor,"
                "d.processor_version,d.target_domain,d.model,d.prompt_hash "
                "FROM derivation_outbox d JOIN raw_messages m USING(message_id) "
                "WHERE m.user_id=? AND m.agent_id=? AND m.connector_id=? "
                "AND m.conversation_id=? AND d.task_id IN (" + marks + ")",
                (scope.user_id, scope.agent_id, scope.connector_id,
                 scope.conversation_id, *task_ids),
            ).fetchall()
        return {row[0]: (SourceRevision(row[1], row[2], row[0], row[3],
                                        row[4], row[6], row[7]), row[5]) for row in rows}

    def claim(
        self,
        scope: Scope,
        worker_id: str,
        *,
        now: float | None = None,
        lease_seconds: int = 60,
    ) -> dict | None:
        with self._db(write=True) as db:
            return _outbox.claim(
                db, scope, worker_id, now=now, lease_seconds=lease_seconds
            )

    def claim_partition(self, scope: Scope, worker_id: str, *, now: float | None = None,
                        lease_seconds: int = 60, max_active: int = 2,
                        selected_attempts: Mapping[str, int] | None = None,
                        selected_sources: Mapping[str, Mapping[str, object]] | None = None) -> dict | None:
        with self._db(write=True) as db:
            return _outbox.claim_partition(db, scope, worker_id, now=now,
                                           lease_seconds=lease_seconds, max_active=max_active,
                                           selected_attempts=selected_attempts, selected_sources=selected_sources)

    def recover_selected_failed(self, rows: tuple[dict, ...], manifest_digest: str) -> None:
        """One database CAS, with the recovery authorization in the same transaction."""
        from kernel.raw_message_recovery import recover_failed_rows

        with self._db(write=True) as db:
            recover_failed_rows(db, rows, manifest_digest)

    def group_budget_origin(self, scope: Scope, task_id: str, lease_token: str) -> str:
        """Prove an untouched five-domain source, never infer old calls as zero."""
        _scope(scope)
        _id(task_id)
        _id(lease_token)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete grouped budget scope required")
        now = time.time()
        domains = {"entity", "graph", "journal", "rule", "schedule"}
        with self._db() as db:
            db.execute("BEGIN")
            current = _outbox._leased(db, scope, task_id, lease_token, now)
            if current["target_domain"] not in domains:
                return "legacy_unknown"
            rows = db.execute(
                "SELECT * FROM derivation_outbox WHERE message_id=? AND revision=? "
                "AND processor=? AND processor_version=? AND model=? AND prompt_hash=? "
                "AND target_domain IN ('entity','graph','journal','rule','schedule')",
                tuple(current[key] for key in (
                    "message_id", "revision", "processor", "processor_version", "model", "prompt_hash")),
            ).fetchall()
            if len(rows) != 5 or {row["target_domain"] for row in rows} != domains:
                return "legacy_unknown"
            for row in rows:
                pending = row["status"] == "pending" and row["attempts"] == 0
                first_lease = (row["status"] == "leased" and row["attempts"] == 1
                               and row["lease_owner"] == current["lease_owner"]
                               and row["lease_until"] is not None and row["lease_until"] > now)
                if not (pending or first_lease) or row["last_error_code"] is not None:
                    return "legacy_unknown"
                events = dict(db.execute(
                    "SELECT action,COUNT(*) FROM derivation_events WHERE task_id=? GROUP BY action",
                    (row["task_id"],)))
                expected = {"created": 1, "claimed": 1} if first_lease else {"created": 1}
                if events != expected:
                    return "legacy_unknown"
            return "fresh"

    def renew(self, scope: Scope, task_id: str, lease_token: str, *,
              now: float | None = None, lease_seconds: int = 60) -> dict:
        with self._db(write=True) as db:
            return _outbox.renew(db, scope, task_id, lease_token, now=now,
                                 lease_seconds=lease_seconds)

    def complete(
        self,
        scope: Scope,
        task_id: str,
        lease_token: str,
        results: tuple[ResultAnchor, ...],
        *,
        now: float | None = None,
    ) -> None:
        with self._db(write=True) as db:
            _outbox.complete(db, scope, task_id, lease_token, results, now=now)

    def fail(
        self,
        scope: Scope,
        task_id: str,
        lease_token: str,
        error_code: str,
        *,
        retry_after: int | None = None,
        now: float | None = None,
    ) -> None:
        with self._db(write=True) as db:
            _outbox.fail(
                db,
                scope,
                task_id,
                lease_token,
                error_code,
                retry_after=retry_after,
                now=now,
            )

    def derivation_events(self, scope: Scope, task_id: str) -> list[dict]:
        _scope(scope)
        _id(task_id)
        with self._db() as db:
            predicate, params = _predicate(scope, "m")
            return [
                dict(row)
                for row in db.execute(
                    """SELECT e.* FROM derivation_events e
                JOIN derivation_outbox d USING(task_id) JOIN raw_messages m USING(message_id)
                WHERE """
                    + predicate
                    + " AND d.task_id=? ORDER BY e.event_id",
                    [*params, task_id],
                )
            ]

    def results(
        self, scope: Scope, message_id: str, *, include_inactive: bool = False
    ) -> list[dict]:
        _scope(scope)
        _id(message_id)
        with self._db() as db:
            return query_raw_results(db, scope, message_id,
                                     include_inactive=include_inactive)

    def has_derived_results(self, scope: Scope) -> bool:
        """Whether this complete conversation ever published derived memory IDs."""
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db() as db:
            return query_raw_has_derived_results(db, scope)

    def has_succeeded_derivation(self, scope: Scope) -> bool:
        """Whether this conversation has any current or historical success."""
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db() as db:
            return query_raw_has_succeeded_derivation(db, scope)

    def partition_derivations_resolved(self, scope: Scope) -> bool:
        """All current source revisions in this conversation finished derivation."""
        return self.partition_state(scope)[1]

    def partition_state(self, scope: Scope, *, deadline: float | None = None) -> tuple[int, bool]:
        """Read the committed Raw generation and completion state atomically."""
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db(deadline=deadline) as db:
            return query_raw_partition_state(db, scope)

    def partition_publication_state(self, scope: Scope) -> tuple[int, bool, bool]:
        """Include current source failure in the same committed Raw snapshot."""
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db() as db:
            generation, resolved = query_raw_partition_state(db, scope)
            failed = db.execute(
                """SELECT 1 FROM derivation_outbox d
                JOIN raw_messages m USING(message_id)
                WHERE m.agent_id=? AND m.user_id=? AND m.connector_id=?
                AND m.conversation_id=? AND m.status IN ('active','edited')
                AND d.revision=m.revision AND d.status='failed' LIMIT 1""",
                (scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id),
            ).fetchone()
            return generation, resolved, failed is not None

    def partition_derivation_inventory(self, scope: Scope) -> tuple[dict, ...]:
        """Current-revision Outbox rows and anchors in one Raw read transaction.

        This is a cold recovery inventory, not a search or publication grant.
        An oversized partition remains unavailable until a paged audit exists.
        """
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db() as db:
            db.execute("BEGIN")
            rows = db.execute(
                "SELECT d.task_id,d.message_id,d.revision,d.processor,"
                "d.processor_version,d.target_domain,d.model,d.prompt_hash,d.status "
                "FROM derivation_outbox d JOIN raw_messages m USING(message_id) "
                "WHERE m.user_id=? AND m.agent_id=? AND m.connector_id=? "
                "AND m.conversation_id=? AND m.status IN ('active','edited') "
                "AND d.revision=m.revision ORDER BY d.task_id LIMIT 10001",
                (scope.user_id, scope.agent_id, scope.connector_id,
                 scope.conversation_id),
            ).fetchall()
            if len(rows) > 10000:
                raise ValueError("derivation recovery inventory too large")
            inventory = []
            for row in rows:
                anchors = tuple(ResultAnchor(result[0], result[1]) for result in db.execute(
                    "SELECT memory_id,store_ref FROM derivation_results "
                    "WHERE task_id=? ORDER BY memory_id", (row[0],)))
                inventory.append({"source": SourceRevision(row[1], row[2], row[0],
                    row[3], row[4], row[6], row[7]), "domain": row[5],
                    "status": row[8], "anchors": anchors})
            return tuple(inventory)

    def partition_derivation_pages(self, scope: Scope, *, page_size: int = 256
                                   ) -> Generator[tuple[dict, ...], None, None]:
        """Stream current tasks from one Raw snapshot without a total-row cap."""
        _scope(scope)
        if (scope.connector_id is None or scope.conversation_id is None
                or type(page_size) is not int or not 1 <= page_size <= 512):
            raise ScopeError("complete bounded partition scope required")
        with self._db() as db:
            db.execute("BEGIN")
            after = ""
            while True:
                rows = db.execute(
                    "SELECT d.task_id,d.message_id,d.revision,d.processor,"
                    "d.processor_version,d.target_domain,d.model,d.prompt_hash,d.status "
                    "FROM derivation_outbox d JOIN raw_messages m USING(message_id) "
                    "WHERE m.user_id=? AND m.agent_id=? AND m.connector_id=? "
                    "AND m.conversation_id=? AND m.status IN ('active','edited') "
                    "AND d.revision=m.revision AND d.task_id>? "
                    "ORDER BY d.task_id LIMIT ?",
                    (scope.user_id, scope.agent_id, scope.connector_id,
                     scope.conversation_id, after, page_size),
                ).fetchall()
                if not rows:
                    return
                page = []
                for row in rows:
                    anchors = tuple(ResultAnchor(result[0], result[1]) for result in
                                    db.execute(
                        "SELECT memory_id,store_ref FROM derivation_results "
                        "WHERE task_id=? ORDER BY memory_id", (row[0],)))
                    page.append({"source": SourceRevision(row[1], row[2], row[0],
                        row[3], row[4], row[6], row[7]), "domain": row[5],
                        "status": row[8], "anchors": anchors})
                after = rows[-1][0]
                yield tuple(page)

    def vector_anchor_pages(self, scope: Scope, *, generation: int, deadline_ns: int,
                            page_size: int = 100, _connection: sqlite3.Connection | None = None
                            ) -> Generator[tuple[dict, ...], None, None]:
        """One owner-thread transaction; unique current vector anchors, no total cap."""
        _scope(scope)
        if (scope.connector_id is None or scope.conversation_id is None
                or type(generation) is not int or generation < 0
                or type(deadline_ns) is not int or deadline_ns <= time.monotonic_ns()
                or type(page_size) is not int or not 1 <= page_size <= 100):
            raise ScopeError("invalid bounded vector page scope")
        # Page owners keep the actual connection, not only this generator, so a
        # failed context exit remains retryable after the generator has closed.
        context = (self._db(deadline=deadline_ns / 1e9) if _connection is None
                   else nullcontext(_connection))
        with context as db:
            db.set_progress_handler(lambda: int(time.monotonic_ns() >= deadline_ns), 1000)
            db.execute("PRAGMA cache_size=-128")
            db.execute("PRAGMA temp_store=FILE")
            db.execute("BEGIN")
            if query_raw_partition_state(db, scope) != (generation, True):
                raise ConflictError("vector page source changed")
            after = ""
            cursor = db.execute(
                "SELECT r.memory_id,r.store_ref,d.message_id,d.revision,d.task_id,"
                "d.processor,d.processor_version,d.model,d.prompt_hash,COUNT(*) "
                "FROM derivation_results r JOIN derivation_outbox d USING(task_id) "
                "JOIN raw_messages m USING(message_id) "
                "WHERE m.user_id=? AND m.agent_id=? AND m.connector_id=? AND m.conversation_id=? "
                "AND m.status IN ('active','edited') AND d.revision=m.revision "
                "AND d.target_domain='vector' AND d.status='succeeded' AND r.status='active' AND r.memory_id>? "
                "GROUP BY r.memory_id ORDER BY r.memory_id",
                (scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id, after),
            )
            while True:
                if time.monotonic_ns() >= deadline_ns:
                    raise TimeoutError("vector page deadline exceeded")
                # Keep one ordered cursor in the same read transaction instead
                # of re-scanning/grouping the whole corpus for every page.
                rows = cursor.fetchmany(page_size)
                if not rows:
                    return
                if any(row[9] != 1 for row in rows) or rows[0][0] <= after:
                    raise ConflictError("vector page anchor is not unique")
                page = tuple({"memory_id": row[0], "store_ref": row[1],
                              "source": SourceRevision(*row[2:9])} for row in rows)
                after = rows[-1][0]
                yield page

    def sources_for_memory(
        self, scope: Scope, memory_id: str, *, include_inactive: bool = False
    ) -> list[dict]:
        _scope(scope)
        _id(memory_id)
        with self._db() as db:
            predicate, params = _predicate(scope, "m")
            query = (
                """SELECT d.message_id,d.revision,r.status,d.target_domain
                FROM derivation_results r JOIN derivation_outbox d USING(task_id)
                JOIN raw_messages m USING(message_id)
                WHERE """
                + predicate
                + " AND r.memory_id=?"
            )
            if not include_inactive:
                query += " AND r.status='active' AND m.status IN ('active','edited') AND d.revision=m.revision"
            return [
                dict(row)
                for row in db.execute(
                    query + " ORDER BY d.message_id,d.revision", [*params, memory_id]
                )
            ]

    def get(
        self, scope: Scope, message_id: str, *, include_inactive: bool = False,
        deadline: float | None = None,
    ) -> StoredMessage | None:
        _scope(scope)
        _id(message_id)
        with self._db(deadline=deadline) as db:
            return query_raw_get(db, scope, message_id,
                                 include_inactive=include_inactive)

    def source_snapshots(
        self, scope: Scope, message_ids: tuple[str, ...]
    ) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        with self._db() as db:
            return query_raw_source_snapshots(db, scope, message_ids)

    def published_snapshots(
        self, scope: Scope, message_ids: tuple[str, ...], *,
        proofs: dict[str, tuple[tuple[str, str], ...]] | None = None,
        deadline: float | None = None,
    ) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
        page_deadline = _publication_deadline(deadline)
        with self._db(deadline=page_deadline) as db:
            return cast(dict[str, tuple[StoredMessage, tuple[dict, ...]]],
                        query_raw_published_snapshots(db, scope, message_ids,
                                                      proofs=proofs, deadline=page_deadline))

    def published_evidence_snapshots(self, scope: Scope, message_ids: tuple[str, ...], *,
                                     query: str, proofs=None, deadline=None
                                     ) -> dict[str, tuple[RawMessageEvidence, tuple[dict, ...]]]:
        page_deadline = _publication_deadline(deadline)
        with self._db(deadline=page_deadline) as db:
            return cast(dict[str, tuple[RawMessageEvidence, tuple[dict, ...]]],
                        query_raw_published_snapshots(db, scope, message_ids, proofs=proofs,
                                                      deadline=page_deadline, evidence_query=query))

    def read_content_chunk(self, scope: Scope, message_id: str, *, expected_revision: int,
                           expected_content_hash: str, offset_chars: int = 0,
                           limit_chars: int = 4096, deadline=None) -> RawContentChunk:
        with self._db(deadline=deadline) as db:
            return query_raw_content_chunk(db, scope, message_id,
                expected_revision=expected_revision, expected_content_hash=expected_content_hash,
                offset_chars=offset_chars, limit_chars=limit_chars)

    def search_evidence_page(self, scope: Scope, query: str, *, limit: int = 10,
                             start_ts=None, end_ts=None, before=None, deadline=None
                             ) -> list[RawMessageEvidence]:
        needle = validate_raw_search(scope, query, limit=limit, start_ts=start_ts,
                                     end_ts=end_ts, before=before, deadline=deadline)
        with self._db(deadline=deadline) as db:
            return cast(list[RawMessageEvidence], query_raw_search_page(
                db, scope, needle, limit=limit, start_ts=start_ts, end_ts=end_ts,
                before=before, deadline=deadline, evidence_query=needle))

    def evidence_neighbors(self, scope: Scope, message_id: str, *, query: str,
                           before: int = 2, after: int = 2, deadline=None
                           ) -> list[RawMessageEvidence]:
        return [raw_evidence(message, query) for message in self.neighbors(
            scope, message_id, before=before, after=after, deadline=deadline)]

    def replay(
        self,
        scope: Scope,
        conversation_id: str,
        *,
        session_id: str | None = None,
        after_ordinal: int = 0,
        limit: int = 100,
        include_inactive: bool = False,
    ) -> list[StoredMessage]:
        _scope(scope)
        _id(conversation_id)
        if (
            type(after_ordinal) is not int
            or after_ordinal < 0
            or type(limit) is not int
            or not 1 <= limit <= 500
        ):
            raise RawMessageError("invalid replay page")
        if session_id is not None:
            _id(session_id)
        with self._db() as db:
            predicate, scope_params = _predicate(scope)
            query = (
                "SELECT * FROM raw_messages WHERE "
                + predicate
                + " AND conversation_id=?"
            )
            params: list = [*scope_params, conversation_id]
            if session_id is not None:
                query += " AND session_id=?"
                params.append(session_id)
            query += " AND ordinal>?"
            params.append(after_ordinal)
            if not include_inactive:
                query += " AND status IN ('active','edited')"
            query += " ORDER BY ordinal LIMIT ?"
            params.append(limit)
            return [_message(row) for row in db.execute(query, params)]

    def recent(self, scope: Scope, *, limit: int = 12) -> list[StoredMessage]:
        """Latest current messages from one stable conversation, in ordinal order."""
        _scope(scope)
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete conversation scope required")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise RawMessageError("invalid recent page")
        with self._db() as db:
            rows = db.execute(
                """SELECT * FROM raw_messages WHERE agent_id=? AND user_id=?
                AND connector_id=? AND conversation_id=? AND status IN ('active','edited')
                ORDER BY ordinal DESC LIMIT ?""",
                (scope.agent_id, scope.user_id, scope.connector_id,
                 scope.conversation_id, limit),
            ).fetchall()
            return [_message(row) for row in reversed(rows)]

    def neighbors(
        self, scope: Scope, message_id: str, *, before: int = 2, after: int = 2,
        deadline: float | None = None,
    ) -> list[StoredMessage]:
        _scope(scope)
        _id(message_id)
        if any(
            type(value) is not int or not 0 <= value <= 10 for value in (before, after)
        ):
            raise RawMessageError("invalid context window")
        with self._db(deadline=deadline) as db:
            return query_raw_neighbors(db, scope, message_id,
                                       before=before, after=after)

    def search(self, scope: Scope, query: str, *, limit: int = 10,
               start_ts: float | None = None,
               end_ts: float | None = None,
               deadline: float | None = None) -> list[StoredMessage]:
        """Return current raw messages from one stable conversation only."""
        return self.search_page(scope, query, limit=limit, start_ts=start_ts,
                                end_ts=end_ts, deadline=deadline)

    def search_page(self, scope: Scope, query: str, *, limit: int = 10,
                    start_ts: float | None = None, end_ts: float | None = None,
                    before: tuple[int, str, str] | None = None,
                    deadline: float | None = None) -> list[StoredMessage]:
        """Return one bounded page ordered by a stable conversation-local cursor."""
        needle = validate_raw_search(scope, query, limit=limit,
                                     start_ts=start_ts, end_ts=end_ts,
                                     before=before, deadline=deadline)
        with self._db() as db:
            return cast(list[StoredMessage], query_raw_search_page(
                db, scope, needle, limit=limit, start_ts=start_ts,
                end_ts=end_ts, before=before, deadline=deadline))

    def export_messages(
        self,
        scope: Scope,
        conversation_id: str | None = None,
        *,
        include_inactive: bool = False,
    ) -> Iterator[dict]:
        """Sensitive export to caller; this method writes no file and logs nothing."""
        _scope(scope)
        if conversation_id is not None:
            _id(conversation_id)
        with self._db() as db:
            db.execute("BEGIN")
            predicate, params = _predicate(scope)
            query = "SELECT * FROM raw_messages WHERE " + predicate
            if conversation_id is not None:
                query += " AND conversation_id=?"
                params.append(conversation_id)
            if not include_inactive:
                query += " AND status IN ('active','edited')"
            for row in db.execute(query + " ORDER BY conversation_id,ordinal", params):
                yield asdict(_message(row))

    def reconcile(self, scope: Scope) -> dict:
        """Read-only internal consistency, not a check against the live six domains."""
        _scope(scope)
        issues = []
        with self._db() as db:
            db.execute("BEGIN")
            predicate, params = _predicate(scope)
            rows = db.execute(
                "SELECT * FROM raw_messages WHERE " + predicate, params
            ).fetchall()
            for row in rows:
                issues.extend(self._message_issues(db, scope, row))
            tasks, result_rows, derivation_issues = _outbox.reconcile(db, scope)
            issues.extend(derivation_issues)
        return {
            "schema_version": SCHEMA_VERSION,
            "messages": len(rows),
            "tasks": len(tasks),
            "results": len(result_rows),
            "issues": issues,
            "external_stores_checked": False,
        }

    @staticmethod
    def _message_issues(
        db: sqlite3.Connection, scope: Scope, row: sqlite3.Row
    ) -> list[dict]:
        mid = row["message_id"]
        codes = []
        if _hash(row["content"]) != row["content_hash"]:
            codes.append("content_hash")
        versions = db.execute(
            "SELECT * FROM raw_message_revisions WHERE message_id=? ORDER BY revision",
            (mid,),
        ).fetchall()
        if [r["revision"] for r in versions] != list(range(1, row["revision"] + 1)):
            codes.append("revision_sequence")
        if not versions or (versions[-1]["content"], versions[-1]["content_hash"]) != (
            row["content"],
            row["content_hash"],
        ):
            codes.append("latest_revision")
        if any(_hash(r["content"]) != r["content_hash"] for r in versions):
            codes.append("revision_hash")
        for ref in json.loads(row["attachment_refs_json"]):
            asset = db.execute(
                "SELECT * FROM attachments WHERE attachment_id=?",
                (ref["attachment_id"],),
            ).fetchone()
            if not asset or (
                asset["agent_id"],
                asset["user_id"],
                asset["sha256"],
                asset["size_bytes"],
                asset["content_type"],
            ) != (
                scope.agent_id,
                scope.user_id,
                ref["sha256"],
                ref["size_bytes"],
                ref["content_type"],
            ):
                codes.append("attachment_reference")
        return [{"message_id": mid, "code": code} for code in codes]

    def backup_database(self, destination: str | Path) -> dict:
        """Administrative full-database backup, not a scoped user export."""
        return self._database.backup(destination)
