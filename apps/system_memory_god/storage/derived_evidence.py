"""Immutable source-granular outputs, not a retrieval authorization authority.

Readers must join against current Raw result anchors before exposing records.
No old store, model, scheduler or automatic publication is opened here.
"""

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Callable

from kernel.challenge_diagnostics import DiagnosticError
from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical, derived_memory_id, identity
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from .pipeline_database import PipelineDatabase, paged_rows

_DDL = """
CREATE TABLE derived_evidence (
 memory_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, output_index INTEGER NOT NULL,
 domain TEXT NOT NULL, store_ref TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL,
 payload_hash TEXT NOT NULL, source_json TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(task_id,output_index));
CREATE VIRTUAL TABLE evidence_search USING fts5(
 memory_id UNINDEXED,search_text,tokenize='trigram');
"""

_FIELDS = {
    "vector": {"text"}, "entity": {"subject", "attribute", "value"},
    "graph": {"subject", "relation", "object"}, "rule": {"text", "status"},
    "journal": {"text", "occurred_at"}, "schedule": {"text", "scheduled_at"},
}


def validate_payload(domain: str, payload: dict) -> str:
    if type(domain) is not str or domain not in _FIELDS or type(payload) is not dict:
        raise DiagnosticError("domain_payload", "invalid evidence payload")
    if set(payload) != _FIELDS[domain]:
        raise DiagnosticError("domain_payload", "invalid evidence fields")
    for key, value in payload.items():
        if key in {"occurred_at", "scheduled_at"} and value is None:
            continue
        if type(value) is not str or not value.strip() or len(value) > 8192:
            raise DiagnosticError("domain_payload", "invalid evidence value")
        try:
            value.encode("utf-8")
            if key in {"occurred_at", "scheduled_at"}:
                stamp = datetime.fromisoformat(value)
                if stamp.utcoffset() is None:
                    raise ValueError()
        except (ValueError, UnicodeError):
            raise DiagnosticError("domain_payload", "invalid evidence value") from None
    if domain == "rule" and payload["status"] != "candidate":
        raise DiagnosticError("domain_payload", "rule promotion forbidden")
    return canonical(payload)


def query_evidence_ids(db: sqlite3.Connection, query: str, *, limit: int = 30,
                       after: str | None = None,
                       deadline: float | None = None) -> tuple[str, ...]:
    """Shared bounded lexical query; the caller owns connection authorization."""
    validate_evidence_search(query, limit, after, deadline)
    fields = ("text", "subject", "attribute", "value", "relation", "object")
    conditions = " OR ".join(
        f"instr(COALESCE(json_extract(payload_json,'$.{field}'),''),?)>0"
        for field in fields
    )
    if deadline is not None:
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    try:
        cursor = " AND memory_id>?" if after is not None else ""
        locator = (" AND memory_id IN (SELECT memory_id FROM evidence_search "
                   "WHERE evidence_search MATCH ?)" if len(query.strip()) >= 3 else "")
        locator_params = ((('"' + query.strip().replace('"', '""') + '"',)
                           if locator else ()))
        params = (*((query.strip(),) * len(fields)), *locator_params,
                  *((after,) if after is not None else ()), limit)
        return tuple(row[0] for row in db.execute(
            "SELECT memory_id FROM derived_evidence WHERE (" + conditions + ")"
            + locator + cursor + " ORDER BY memory_id LIMIT ?", params,
        ))
    finally:
        if deadline is not None:
            db.set_progress_handler(None, 0)


def validate_evidence_search(query: str, limit: int, after: str | None,
                             deadline: float | None) -> None:
    if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
            or type(limit) is not int or not 1 <= limit <= 100):
        raise ValueError("invalid evidence search")
    if after is not None:
        identity(after)
    if deadline is not None and (type(deadline) not in {int, float}
                                 or not time.monotonic() < deadline):
        raise ValueError("evidence search budget exceeded")


def validate_evidence_ids(memory_ids: tuple[str, ...]) -> None:
    if type(memory_ids) is not tuple or not 1 <= len(memory_ids) <= 100:
        raise ValueError("invalid evidence lookup")
    for memory_id in memory_ids:
        identity(memory_id)


def query_evidence_many(db: sqlite3.Connection,
                        memory_ids: tuple[str, ...]) -> dict[str, dict]:
    validate_evidence_ids(memory_ids)
    marks = ",".join("?" for _ in memory_ids)
    rows = db.execute("SELECT * FROM derived_evidence WHERE memory_id IN ("
                      + marks + ")", memory_ids).fetchall()
    return {row[0]: DerivedEvidenceStore._record(row) for row in rows}


class DerivedEvidenceStore:
    def __init__(self, root: Path, scope: PipelineScope):
        self._db = PipelineDatabase(root, scope, "derived_evidence.db", _DDL,
                                    0x414C4445, version=2)

    def validate_existing(self) -> None:
        self._db.validate_existing()

    def domain_ids(self, query, *, domains, limit=100, deadline=None,
                   event_start_ts=None, event_end_ts=None, timings=None):
        from .domain_queries import query_domain_ids

        with self._db.readonly_snapshot() as db:
            return query_domain_ids(db, query, domains=domains, limit=limit, deadline=deadline,
                event_start_ts=event_start_ts, event_end_ts=event_end_ts, timings=timings)

    def purge_inventory(self, raw_message_id: str,
                        validate_sources: Callable[[tuple[tuple[SourceRevision, str], ...]], None]
                        ) -> dict[str, dict]:
        """Validate every evidence row, returning only exact target results."""
        identity(raw_message_id)
        target: dict[str, dict] = {}
        with self._db.readonly_snapshot() as db:
            batch: list[tuple[SourceRevision, str]] = []
            for row in paged_rows(db, "SELECT * FROM derived_evidence ORDER BY memory_id"):
                record = self._record(row)
                source = SourceRevision(**record["source"])
                if (source.task_id != record["task_id"]
                        or record["memory_id"] != derived_memory_id(source.task_id,
                                                                      record["output_index"])
                        or record["store_ref"] != "evidence:" + record["memory_id"]
                        or canonical(asdict(source)) != row[7]
                        or validate_payload(record["domain"], record["payload"]) != row[5]):
                    raise ValueError("evidence inventory conflict")
                batch.append((source, record["domain"]))
                if len(batch) == 256:
                    validate_sources(tuple(batch))
                    batch.clear()
                if source.raw_message_id == raw_message_id:
                    target[record["memory_id"]] = record
            if batch:
                validate_sources(tuple(batch))
        return target

    def put(self, source: SourceRevision, domain: str, index: int, payload: dict) -> dict:
        with self._db.transaction() as db:
            return self._put(db, source, domain, index, payload)

    def put_batch(self, source: SourceRevision, domain: str, payloads: tuple[dict, ...]) -> list[dict]:
        if type(payloads) is not tuple or len(payloads) > 100:
            raise DiagnosticError("domain_payload", "invalid evidence batch")
        if sum(len(validate_payload(domain, value)) for value in payloads) > 2 * 1024 * 1024:
            raise DiagnosticError("domain_payload", "evidence batch too large")
        with self._db.transaction() as db:
            return [self._put(db, source, domain, index, payload)
                    for index, payload in enumerate(payloads)]

    @staticmethod
    def _put(db: sqlite3.Connection, source: SourceRevision, domain: str, index: int,
             payload: dict) -> dict:
        if type(source) is not SourceRevision:
            raise DiagnosticError("source_budget", "invalid evidence source")
        if type(index) is not int or not 0 <= index < 100:
            raise DiagnosticError("source_budget", "invalid evidence index")
        encoded = validate_payload(domain, payload)
        source_json = canonical(asdict(source))
        memory_id = derived_memory_id(source.task_id, index)
        store_ref = "evidence:" + memory_id
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        values = (memory_id, source.task_id, index, domain, store_ref, encoded, digest, source_json)
        old = db.execute("SELECT * FROM derived_evidence WHERE memory_id=?", (memory_id,)).fetchone()
        if old is not None:
            if old[:8] != values:
                raise DiagnosticError("prepare", "evidence conflict")
            return DerivedEvidenceStore._record(old)
        row = (*values, datetime.now(timezone.utc).isoformat())
        db.execute("INSERT INTO derived_evidence VALUES (?,?,?,?,?,?,?,?,?)", row)
        db.execute("INSERT INTO evidence_search(memory_id,search_text) VALUES (?,?)",
                   (memory_id, " ".join(str(value) for value in payload.values()
                                        if value is not None)))
        return DerivedEvidenceStore._record(row)

    def by_task(self, task_id: str) -> list[dict]:
        identity(task_id)
        with self._db.transaction() as db:
            return [self._record(row) for row in db.execute(
                "SELECT * FROM derived_evidence WHERE task_id=? ORDER BY output_index", (task_id,))]

    def by_tasks(self, task_ids: tuple[str, ...]) -> dict[str, list[dict]]:
        if type(task_ids) is not tuple or not 1 <= len(task_ids) <= 256:
            raise ValueError("invalid evidence task lookup")
        for task_id in task_ids:
            identity(task_id)
        marks = ",".join("?" for _ in task_ids)
        with self._db.readonly_snapshot() as db:
            rows = db.execute("SELECT * FROM derived_evidence WHERE task_id IN ("
                              + marks + ") ORDER BY task_id,output_index", task_ids)
            result: dict[str, list[dict]] = {}
            for row in rows:
                result.setdefault(row[1], []).append(self._record(row))
            return result

    def search_ids(self, query: str, *, limit: int = 30) -> tuple[str, ...]:
        """Return bounded candidate IDs only; callers must authorize every ID."""
        return self.search_ids_page(query, limit=limit)

    def search_ids_page(self, query: str, *, limit: int = 30,
                        after: str | None = None,
                        deadline: float | None = None) -> tuple[str, ...]:
        """Page lexical candidates without treating IDs as visibility grants."""
        validate_evidence_search(query, limit, after, deadline)
        with self._db.transaction() as db:
            return query_evidence_ids(db, query, limit=limit, after=after,
                                      deadline=deadline)

    def project(self, memory_ids: tuple[str, ...]) -> list[dict]:
        """Build an ephemeral view of caller-authorized IDs, never all stored records.

        This low-level method is not retrieval authorization. The orchestrator
        must obtain eligible IDs from current Raw anchors for every request.
        """
        if type(memory_ids) is not tuple or len(memory_ids) > 100:
            raise ValueError("invalid projection input")
        for key in memory_ids:
            identity(key)
        groups: dict[str, dict] = {}
        with self._db.transaction() as db:
            for key in sorted(set(memory_ids)):
                row = db.execute("SELECT * FROM derived_evidence WHERE memory_id=?", (key,)).fetchone()
                if row is None:
                    raise ValueError("projection evidence missing")
                record = self._record(row)
                group_key = canonical([record["domain"], record["payload"]])
                group = groups.setdefault(group_key, {"domain": record["domain"],
                    "payload": record["payload"], "member_ids": []})
                group["member_ids"].append(key)
        return [groups[key] for key in sorted(groups)]

    def get(self, memory_id: str) -> dict | None:
        identity(memory_id)
        with self._db.transaction() as db:
            row = db.execute("SELECT * FROM derived_evidence WHERE memory_id=?", (memory_id,)).fetchone()
        return self._record(row) if row else None

    def get_many(self, memory_ids: tuple[str, ...]) -> dict[str, dict]:
        validate_evidence_ids(memory_ids)
        with self._db.transaction() as db:
            return query_evidence_many(db, memory_ids)

    def purge_one(self, memory_id: str, raw_message_id: str) -> None:
        """Delete one source-granular result; a mismatched source is a conflict."""
        identity(memory_id)
        identity(raw_message_id)
        with self._db.transaction() as db:
            row = db.execute("SELECT source_json FROM derived_evidence WHERE memory_id=?",
                             (memory_id,)).fetchone()
            if row is None:
                return
            if json.loads(row[0])["raw_message_id"] != raw_message_id:
                raise ValueError("evidence purge source conflict")
            db.execute("DELETE FROM evidence_search WHERE memory_id=?", (memory_id,))
            db.execute("DELETE FROM derived_evidence WHERE memory_id=?", (memory_id,))

    @staticmethod
    def _record(row: tuple) -> dict:
        if hashlib.sha256(row[5].encode()).hexdigest() != row[6]:
            raise DiagnosticError("cache_integrity", "evidence integrity failure")
        return {"memory_id": row[0], "task_id": row[1], "output_index": row[2],
                "domain": row[3], "store_ref": row[4], "payload": json.loads(row[5]),
                "payload_hash": row[6], "source": json.loads(row[7]), "created_at": row[8]}
