"""Bounded exact vector pages; no approximate candidate selection or model calls."""

from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields, is_dataclass
import hashlib
import heapq
import inspect
import json
import math
import os
import sqlite3
import sys
import time
from typing import Any

import numpy as np

from kernel.memory_pipeline_models import SourceRevision, canonical, identity
from kernel.raw_message_models import Scope
from kernel.raw_message_models import RawMessageError

PAGE_ROWS = 100
METADATA_WORKING_BYTES = 2 * 1024**2
NUMERICAL_WORKING_BYTES = 40 * 1024**2
BINDING_KEYS = {"query_id", "generation", "deadline_ns"}


def binding(value: dict) -> dict:
    if (type(value) is not dict or not BINDING_KEYS <= set(value)
            or type(value["generation"]) is not int or value["generation"] < 0
            or type(value["deadline_ns"]) is not int or not 0 < value["deadline_ns"] < 2**63):
        raise ValueError("invalid page binding")
    identity(value["query_id"])
    check_deadline(value["deadline_ns"])
    return {key: value[key] for key in BINDING_KEYS}


def check_deadline(deadline_ns: int) -> None:
    if time.monotonic_ns() >= deadline_ns:
        raise TimeoutError("page deadline exceeded")


def validate_receipts(rows: list[dict]) -> None:
    """Validate one persisted write/recovery batch without retaining the corpus."""
    if type(rows) is not list or not 1 <= len(rows) <= PAGE_ROWS:
        raise ValueError("invalid page receipt batch")
    for row in rows:
        snapshot = row.get("snapshot")
        dimension = row.get("embedding_dim")
        if (type(dimension) is not int or not 1 <= dimension <= 2048 or type(snapshot) is not dict
                or set(snapshot) != {"schema", "f32", "sha256"} or type(snapshot["schema"]) is not int
                or snapshot["schema"] != 1 or type(snapshot["f32"]) is not str or len(snapshot["f32"]) > 10924):
            raise ValueError("invalid page numerical receipt")
        body = base64.b64decode(snapshot["f32"], validate=True)
        if len(body) != dimension * 4 or hashlib.sha256(body).hexdigest() != snapshot["sha256"]:
            raise ValueError("page receipt digest differs")
        vector = np.frombuffer(body, dtype="<f4")
        norm = np.linalg.norm(vector.astype(np.float64))
        if not np.isfinite(vector).all() or not np.isfinite(norm) or norm <= 0:
            raise ValueError("invalid page numerical values")


def hash_text(value) -> None:
    if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("invalid page hash")


def _metadata_bytes(*values) -> int:
    """Size one graph without retaining object IDs across transient lifetimes."""
    seen = set()
    def size(value):
        marker = id(value)
        if marker in seen:
            return 0
        seen.add(marker)
        total = sys.getsizeof(value)
        if isinstance(value, dict):
            return total + sum(size(key) + size(item) for key, item in value.items())
        if isinstance(value, (tuple, list, set)):
            return total + sum(size(item) for item in value)
        if is_dataclass(value):
            # Include the instance dictionary itself, not only field values.
            names = fields(value)
            namespace = getattr(value, "__dict__", None)
            return total + (size(namespace) if namespace is not None else
                            sum(size(getattr(value, field.name)) for field in names))
        return total
    return sum(size(value) for value in values)


def metadata_size(*values) -> int:
    """Account a proof graph and the fixed SQLite/cursor working reserve."""
    # Five bounded SQLite caches (gate, Raw, evidence, ledger, fresh Raw check)
    # plus 128KiB cursor/planner overhead. Chroma's index is separate.
    return 768 * 1024 + _metadata_bytes(*values)


class PageCursor:
    def __init__(self, request: dict):
        self.binding = binding(request)
        self.sequence, self.count, self.last_key = 0, 0, ""

    def verify(self, request: dict, *, eof: bool = False) -> None:
        if binding(request) != self.binding:
            raise ValueError("page binding changed")
        if (type(request.get("sequence")) is not int or request["sequence"] != self.sequence
                or request.get("last_key") != self.last_key):
            raise ValueError("page sequence changed")
        if eof and (type(request.get("count")) is not int or request["count"] != self.count):
            raise ValueError("page EOF count differs")

    def advance(self, ids: list[str]) -> None:
        if not 1 <= len(ids) <= PAGE_ROWS:
            raise ValueError("invalid page size")
        previous = self.last_key
        for key in ids:
            identity(key)
            if key <= previous:
                raise ValueError("page key order changed")
            previous = key
        self.last_key = previous
        self.sequence += 1
        self.count += len(ids)

    def ack(self) -> dict:
        return {**self.binding, "sequence": self.sequence, "last_key": self.last_key,
                "count": self.count, "worker_pid": os.getpid()}


class _ReverseID(str):
    def __lt__(self, other):
        return str.__gt__(self, other)


class PagedCosine:
    """One query's exact global heap; scoring uses the original un-clipped rank."""
    def __init__(self, request: dict, query, limit: int):
        self.cursor = PageCursor(request)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("invalid page TopK")
        self.limit = limit
        self.query = np.asarray(query, dtype=np.float64)
        if self.query.ndim != 1 or not 1 <= self.query.size <= 2048 or not np.isfinite(self.query).all():
            raise ValueError("invalid page query")
        self.norm = float(np.linalg.norm(self.query))
        if not math.isfinite(self.norm) or self.norm <= 0:
            raise ValueError("invalid page query")
        self.heap: list[tuple[float, _ReverseID]] = []

    def feed(self, request: dict) -> dict:
        if set(request) != BINDING_KEYS | {"sequence", "last_key", "rows"}:
            raise ValueError("invalid numeric page")
        self.cursor.verify(request)
        rows = request["rows"]
        if type(rows) is not list or not 1 <= len(rows) <= PAGE_ROWS:
            raise ValueError("invalid numeric page")
        ids, bodies = [], []
        for row in rows:
            if (type(row) is not dict or set(row) != {"memory_id", "dimension", "f32", "sha256"}
                    or type(row["dimension"]) is not int or row["dimension"] != self.query.size
                    or type(row["f32"]) is not str or len(row["f32"]) > 10924):
                raise ValueError("invalid numeric page row")
            identity(row["memory_id"])
            hash_text(row["sha256"])
            body = base64.b64decode(row["f32"], validate=True)
            if len(body) != self.query.size * 4 or hashlib.sha256(body).hexdigest() != row["sha256"]:
                raise ValueError("numeric page digest differs")
            ids.append(row["memory_id"])
            bodies.append(body)
        # Conservative allowance includes both IPC endpoints, base64/JSON copies,
        # decoded f32, float64 norm temporaries and the bounded global heap.
        if sum(map(len, bodies)) * 32 + self.limit * 2048 > NUMERICAL_WORKING_BYTES:
            raise ValueError("numeric page working set exceeded")
        matrix = np.frombuffer(b"".join(bodies), dtype="<f4").reshape(len(ids), self.query.size)
        norms = np.sqrt(np.einsum("ij,ij->i", matrix, matrix, dtype=np.float64))
        if not np.isfinite(matrix).all() or not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("invalid numeric page values")
        scores = np.einsum("ij,j->i", matrix, self.query, dtype=np.float64) / (norms * self.norm)
        self.cursor.advance(ids)
        for key, score in zip(ids, scores):
            item = (float(score), _ReverseID(key))
            if len(self.heap) < self.limit:
                heapq.heappush(self.heap, item)
            elif item[0] > self.heap[0][0] or item[0] == self.heap[0][0] and key < str(self.heap[0][1]):
                heapq.heapreplace(self.heap, item)
        check_deadline(self.cursor.binding["deadline_ns"])
        return self.cursor.ack()

    def finish(self, request: dict) -> dict:
        if set(request) != BINDING_KEYS | {"sequence", "last_key", "count"}:
            raise ValueError("invalid page EOF")
        self.cursor.verify(request, eof=True)
        hits = [(str(key), float(np.clip(score, -1, 1)))
                for score, key in sorted(self.heap, key=lambda pair: (-pair[0], str(pair[1])))]
        return {"ack": self.cursor.ack(), "hits": hits}


class QualifiedPageOwner:
    """One dedicated thread owns the scope gate, Raw transaction and cursor."""
    def __init__(self, runtime, partition, deadline_ns: int, generation: int):
        self.runtime, self.partition = runtime, partition
        self.scope = Scope(partition.agent_id, partition.user_id, partition.connector_id, partition.conversation_id)
        self.deadline_ns, self.generation = deadline_ns, generation
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="aru-page-owner")
        self.iterator = self.connection = self.gate = None
        self.evidence = self.ledger = None
        self.closed = False
        self.contexts: list[list[Any]] = []
        self.gate_release_attempted = False
        self.gate_acquired = False
        self.raw_close_unknown = False
        self.metadata_peak = 0

    async def _submit(self, method):
        future = asyncio.wrap_future(self.executor.submit(method))
        try:
            return await asyncio.shield(future)
        except (sqlite3.OperationalError, RawMessageError):
            if time.monotonic_ns() >= self.deadline_ns:
                raise TimeoutError("page database deadline exceeded") from None
            raise
        except asyncio.CancelledError:
            # The database thread keeps ownership until its actual call ends.
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    continue
            try:
                future.result()
            except BaseException:
                pass
            raise

    def _enter_context(self, factory):
        # Capture the connection immediately on creation, including exceptions
        # before context.__enter__ yields it. A consumed generator is not proof
        # that its SQLite connection actually closed.
        item = [None, None, False]
        def observed(db):
            item[1] = db
            db.execute("PRAGMA cache_size=-128")
        item[0] = factory(_on_connection=observed)
        self.contexts.append(item)
        try:
            return item[0].__enter__()
        except BaseException:
            item[2] = True
            if item[1] is None:
                self.contexts.pop()
            raise

    def _leave_context(self, *, abort=False):
        item = self.contexts[-1]
        if item[2]:
            item[1].close()
        else:
            item[2] = True
            if abort:
                item[0].__exit__(GeneratorExit, GeneratorExit(), None)
            else:
                item[0].__exit__(None, None, None)
        self.contexts.pop()

    def _partition_state(self):
        from kernel.raw_message_store import query_raw_partition_state
        db = self._enter_context(
            lambda **kw: self.runtime.raw.store._db(deadline=self.deadline_ns / 1e9, **kw))
        try:
            return query_raw_partition_state(db, self.scope)
        finally:
            # This read has no commit. Retire it without a fresh post-yield SQL
            # budget check masking the original timeout; keep db if close fails.
            self._leave_context(abort=True)

    def _open(self):
        check_deadline(self.deadline_ns)
        self.gate = self.runtime.stores.gate(self.partition)
        def observed(connection):
            self.connection = connection
            connection.execute("PRAGMA cache_size=-128")
        self.connection = self.gate._acquire(deadline=self.deadline_ns / 1e9,
                                              _on_connection=observed)
        self.gate_acquired = True
        deadline = self.deadline_ns / 1e9
        current = self._partition_state()
        if current != (self.generation, True):
            raise RuntimeError("page source unavailable")
        self.evidence = self.runtime.stores.evidence(self.partition)
        self.ledger = self.runtime.stores.ledger(self.partition)
        self.raw_db = self._enter_context(
            lambda **kw: self.runtime.raw.store._db(deadline=deadline, **kw))
        self.evidence_db = self._enter_context(
            lambda **kw: self.evidence._db.readonly_snapshot(deadline=deadline, **kw))
        self.ledger_db = self._enter_context(
            lambda **kw: self.ledger._db.readonly_snapshot(deadline=deadline, **kw))
        for db in (self.evidence_db, self.ledger_db):
            db.execute("PRAGMA cache_size=-128")
            db.execute("PRAGMA temp_store=FILE")
            db.set_progress_handler(lambda: int(time.monotonic_ns() >= self.deadline_ns), 1000)
        self.iterator = self.runtime.raw.store.vector_anchor_pages(
            self.scope, generation=self.generation, deadline_ns=self.deadline_ns,
            _connection=self.raw_db)

    async def open(self):
        await self._submit(self._open)

    def _next(self):
        check_deadline(self.deadline_ns)
        rows = next(self.iterator, None)
        if rows is None:
            if self._partition_state() != (self.generation, True):
                raise RuntimeError("page source changed")
            return None
        result = []
        ids = tuple(row["memory_id"] for row in rows)
        marks = ",".join("?" for _ in ids)
        if self.ledger_db.execute(
                "SELECT memory_id FROM memory_sources WHERE memory_id IN (" + marks + ") "
                "GROUP BY memory_id HAVING COUNT(*)<>1 OR MAX(length(CAST(source_json AS BLOB)))>24576 LIMIT 1",
                ids).fetchone() is not None:
            raise ValueError("page ledger source shape exceeded")
        if self.ledger_db.execute(
                "SELECT 1 FROM memory_items WHERE memory_id IN (" + marks + ") "
                "AND length(CAST(item_json AS BLOB))>32768 LIMIT 1", ids).fetchone() is not None:
            raise ValueError("page ledger item shape exceeded")
        ledger = self._ledger_page(ids, rows)
        wanted = {row["memory_id"]: row for row in rows}
        fixed_bytes = metadata_size(rows, ids, ledger, wanted, marks)
        retained_bytes = 0
        measured = fixed_bytes + sys.getsizeof(result)
        self.metadata_peak = max(self.metadata_peak, measured)
        if measured > METADATA_WORKING_BYTES:
            raise ValueError("page proof working set exceeded")
        records = self.evidence_db.execute("SELECT * FROM derived_evidence WHERE memory_id IN ("
                                           + marks + ") AND length(CAST(payload_json AS BLOB))<=131072 "
                                           "AND length(CAST(source_json AS BLOB))<=24576 ORDER BY memory_id", ids)
        for stored in records:
            check_deadline(self.deadline_ns)
            record = self.evidence._record(stored)
            row = wanted[record["memory_id"]]
            key, source = row["memory_id"], row["source"]
            if (record is None or record["domain"] != "vector" or record["source"] != {
                    field: getattr(source, field) for field in SourceRevision.__dataclass_fields__}
                    or record["store_ref"] != row["store_ref"]
                    or ledger.get(key) != {"has_item": True, "sources": (source,)}):
                raise ValueError("page source proof differs")
            text = record["payload"]["text"]
            body = text.encode("utf-8")
            if len(text) > 8192 or len(body) > 32768:
                raise ValueError("page text exceeds source contract")
            proof = {"memory_id": key, "payload_hash": record["payload_hash"],
                     "text_hash": hashlib.sha256(body).hexdigest(), "text_bytes": len(body)}
            result.append(proof)
            # Each retained entry is counted once when appended. Sharing with
            # the fixed graph or another entry may be counted twice, never less.
            retained_bytes += _metadata_bytes(proof)
            measured = (fixed_bytes + sys.getsizeof(result) + retained_bytes
                        + _metadata_bytes(stored, record, body))
            self.metadata_peak = max(self.metadata_peak, measured)
            if measured > METADATA_WORKING_BYTES:
                raise ValueError("page proof working set exceeded")
            del stored, record, body, text, proof
        if tuple(row["memory_id"] for row in result) != ids:
            raise ValueError("page evidence missing")
        encoded_text = canonical(result)
        encoded_body = encoded_text.encode("utf-8")
        measured = (fixed_bytes + sys.getsizeof(result) + retained_bytes
                    + _metadata_bytes(encoded_text, encoded_body))
        self.metadata_peak = max(self.metadata_peak, measured)
        if (measured > METADATA_WORKING_BYTES
                or len(encoded_body) * 8 > METADATA_WORKING_BYTES):
            raise ValueError("page metadata working set exceeded")
        return result

    def _ledger_page(self, ids, raw_rows):
        """Preserve ledger validation while never materializing encoded rows in bulk."""
        from system_memory_god.models import MemoryItem
        marks = ",".join("?" for _ in ids)
        items: set[str] = set()
        sources: dict[str, tuple[SourceRevision, ...]] = {}
        fixed_bytes = metadata_size(raw_rows, ids, marks)
        retained_bytes = 0
        def measure(*transient):
            measured = (fixed_bytes + sys.getsizeof(items) + sys.getsizeof(sources)
                        + retained_bytes + _metadata_bytes(*transient))
            self.metadata_peak = max(self.metadata_peak, measured)
            if measured > METADATA_WORKING_BYTES:
                raise ValueError("page ledger proof working set exceeded")
        for stored in self.ledger_db.execute(
                "SELECT memory_id,store,store_ref,item_json FROM memory_items WHERE memory_id IN (" + marks + ")", ids):
            check_deadline(self.deadline_ns)
            key, store, store_ref, encoded = stored
            decoded = json.loads(encoded)
            item = MemoryItem(**decoded)
            measure(stored, decoded, item)
            if item.memory_id != key or item.store != store or item.store_ref != store_ref:
                raise ValueError("ledger item column conflict")
            if item.status == "active" or store == "rule" and item.status == "candidate":
                if key not in items:
                    retained_bytes += _metadata_bytes(key)
                items.add(key)
                measure(stored, decoded, item)
            del stored, decoded, item, encoded, key, store, store_ref
        for stored in self.ledger_db.execute(
                "SELECT memory_id,raw_message_id,revision,source_json FROM memory_sources WHERE memory_id IN ("
                + marks + ")", ids):
            check_deadline(self.deadline_ns)
            key, raw_id, revision, encoded = stored
            decoded = json.loads(encoded)
            source = SourceRevision(**decoded)
            if source.raw_message_id != raw_id or source.revision != revision or key in sources:
                raise ValueError("ledger source column conflict")
            entry = (source,)
            sources[key] = entry
            retained_bytes += _metadata_bytes(key, entry)
            measure(stored, decoded, source)
            del stored, decoded, source, encoded, entry, key, raw_id, revision
        result = {key: {"has_item": key in items, "sources": sources.get(key, ())}
                  for key in ids if key in items or key in sources}
        # Only this final transition traverses the completed page graph. Do not
        # repeat it for every source or retain transient encoded SQL rows.
        measured = metadata_size(raw_rows, ids, marks, items, sources, result)
        self.metadata_peak = max(self.metadata_peak, measured)
        if measured > METADATA_WORKING_BYTES:
            raise ValueError("page ledger proof working set exceeded")
        return result

    async def next_page(self):
        return await self._submit(self._next)

    def _close(self):
        if self.raw_close_unknown:
            raise RuntimeError("Raw page close ownership unknown")
        if self.iterator is not None:
            try:
                self.iterator.close()
            except BaseException:
                if inspect.isgenerator(self.iterator) and inspect.getgeneratorstate(self.iterator) == inspect.GEN_CLOSED:
                    self.raw_close_unknown = True
                raise
            self.iterator = None
        while self.contexts:
            self._leave_context(abort=time.monotonic_ns() >= self.deadline_ns)
        if self.connection is not None:
            if self.gate_release_attempted or not self.gate_acquired:
                self.connection.close()
            else:
                self.gate_release_attempted = True
                self.gate._release(self.connection)
            self.connection = None

    async def close(self):
        if not self.closed:
            await self._submit(self._close)
            self.executor.shutdown(wait=True)
            self.closed = True
