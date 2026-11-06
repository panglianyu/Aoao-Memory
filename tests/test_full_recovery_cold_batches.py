"""Synthetic, bounded cold receipts; no model loads or formal storage."""

import hashlib
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.daily_vector_index import DailyVectorIndex


def receipt_index(tmp_path, *, dimension=1024, text="plain source", count=100, fault=None):
    root = tmp_path / "vectors"
    root.mkdir()
    with sqlite3.connect(root / "chroma.sqlite3") as db:
        db.execute("CREATE TABLE collections(name TEXT,dimension INTEGER)")
        db.execute("INSERT INTO collections VALUES('daily_evidence',?)", (dimension,))
    records = [{"memory_id": f"memory-{i:04}", "payload": {"text": text},
                "payload_hash": hashlib.sha256(str(i).encode()).hexdigest()} for i in range(count)]
    by_id = {row["memory_id"]: row for row in records}
    calls = []

    def get(*, ids, include):
        calls.append(tuple(ids))
        keys = list(reversed(ids))
        if fault == "missing":
            keys.pop()
        elif fault == "duplicate":
            keys[-1] = keys[0]
        elif fault == "extra":
            keys.append("unexpected-id")
        return {"ids": keys, "documents": [text] * len(keys),
                "metadatas": [{"payload_hash": by_id.get(key, records[0])["payload_hash"]} for key in keys],
                "embeddings": [[1.0] + [0.0] * (dimension - 1) for _ in keys]}

    index = DailyVectorIndex.__new__(DailyVectorIndex)
    index._lock = threading.RLock()
    index.stores = SimpleNamespace(partition=lambda scope: tmp_path)
    index._collection = lambda scope: SimpleNamespace(get=get)
    return index, records, calls


@pytest.mark.asyncio
async def test_cold_receipts_batch_and_reorder_exact_membership(tmp_path):
    index, records, calls = receipt_index(tmp_path)
    rows = await index.receipts(PipelineScope("u", "a", "c", "s"), records, snapshot=True)
    assert [row["memory_id"] for row in rows] == [row["memory_id"] for row in records]
    assert len(calls) == 1
    assert all(row["embedding_dim"] == 1024 and len(row["snapshot"]["sha256"]) == 64 for row in rows)


@pytest.mark.asyncio
async def test_cold_receipts_split_long_utf8_and_high_dimensions(tmp_path):
    index, records, calls = receipt_index(tmp_path, dimension=65536, text="文" * 8000, count=10)
    rows = await index.receipts(PipelineScope("u", "a", "c", "s"), records, snapshot=True)
    assert len(rows) == 10 and len(calls) > 1
    assert all(len(batch) * (24000 * 8 + 2048) <= 512 * 1024 for batch in calls)
    assert all(len(batch) * 65536 * 64 <= 40 * 1024**2 for batch in calls)
    assert all(row["snapshot"] == {"schema": 1, "unavailable": "vector_capacity_exceeded"} for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "duplicate", "extra"])
async def test_cold_receipts_reject_wrong_batch_membership(tmp_path, fault):
    index, records, _ = receipt_index(tmp_path, count=3, fault=fault)
    with pytest.raises(RuntimeError, match="vector receipt"):
        await index.receipts(PipelineScope("u", "a", "c", "s"), records, snapshot=True)


def test_target_queue_keeps_exact_applied_boundary_and_scope():
    from system_memory_god.storage.daily_vector_index import _check_target_queue
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE max_seq_id(segment_id TEXT,seq_id BLOB)")
        db.execute("CREATE TABLE embeddings_queue(seq_id INTEGER PRIMARY KEY,id TEXT)")
        db.execute("INSERT INTO max_seq_id VALUES('segment',?)", ((10).to_bytes(8, "big"),))
        db.executemany("INSERT INTO embeddings_queue VALUES(?,?)", [(9,"older"),(10,"boundary"),(11,"pending")])
        _check_target_queue(db, "segment", ("older", "boundary", "absent"))
        with pytest.raises(RuntimeError, match="unapplied"):
            _check_target_queue(db, "segment", ("pending",))
        # With no applied watermark, every matching queued target is pending.
        with pytest.raises(RuntimeError, match="unapplied"):
            _check_target_queue(db, "unknown-segment", ("older",))


def test_local_recovery_audit_reuses_only_unchanged_inventory(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_index import VectorRecoveryAudit
    root = tmp_path / "vectors"
    root.mkdir()
    with sqlite3.connect(root / "chroma.sqlite3") as db:
        db.execute("CREATE TABLE max_seq_id(segment_id TEXT,seq_id BLOB)")
        db.execute("CREATE TABLE embeddings_queue(seq_id INTEGER PRIMARY KEY,id TEXT)")
        db.execute("INSERT INTO max_seq_id VALUES('segment',?)", ((10).to_bytes(8, "big"),))
    (tmp_path / "vector_index.db").write_bytes(b"synthetic-identity")
    calls = []

    def inventory(stores, scope, targets):
        calls.append((scope, targets))
        return "segment"

    monkeypatch.setattr(DailyVectorIndex, "_validate_existing_inventory", inventory)
    stores = SimpleNamespace(partition=lambda scope: tmp_path)
    scope = PipelineScope("u", "a", "c", "s")
    audit = VectorRecoveryAudit(stores, scope)
    audit.validate(("one",))
    audit.validate(("two",))
    assert len(calls) == 1
    (tmp_path / "vector_index.db").write_bytes(b"changed-synthetic-identity")
    audit.validate(("three",))
    assert len(calls) == 2
    VectorRecoveryAudit(stores, scope).validate(("four",))
    assert len(calls) == 3, "a new recovery cannot borrow the prior inventory"


@pytest.mark.asyncio
@pytest.mark.parametrize("dimension", [None, "invalid", 0, 65537])
async def test_invalid_dimension_rejected_before_any_embedding_fetch(tmp_path, dimension):
    index, records, calls = receipt_index(tmp_path, dimension=dimension, count=1)
    with pytest.raises(ValueError, match="dimension"):
        await index.receipts(PipelineScope("u", "a", "c", "s"), records, snapshot=True)
    assert calls == []


def test_queue_watermark_above_sqlite_integer_range_keeps_old_max_semantics():
    from system_memory_god.storage.daily_vector_index import _check_target_queue
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE max_seq_id(segment_id TEXT,seq_id BLOB)")
        db.execute("CREATE TABLE embeddings_queue(seq_id INTEGER PRIMARY KEY,id TEXT)")
        db.execute("INSERT INTO max_seq_id VALUES('segment',?)", ((2**64-1).to_bytes(8, "big"),))
        db.execute("INSERT INTO embeddings_queue VALUES(?,?)", (2**63-1, "target"))
        _check_target_queue(db, "segment", ("target",))


@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
def test_cached_recovery_rejects_new_authority_sidefile_before_queue(tmp_path, monkeypatch, suffix):
    from contextlib import closing
    from system_memory_god.storage.daily_vector_index import VectorRecoveryAudit, _file_fingerprint
    root = tmp_path / "vectors"
    root.mkdir()
    with closing(sqlite3.connect(root / "chroma.sqlite3")) as db:
        with db:
            db.execute("CREATE TABLE max_seq_id(segment_id TEXT,seq_id BLOB)")
            db.execute("CREATE TABLE embeddings_queue(seq_id INTEGER PRIMARY KEY,id TEXT)")
    authority = tmp_path / "vector_index.db"
    authority.write_bytes(b"synthetic-identity")
    monkeypatch.setattr(DailyVectorIndex, "_validate_existing_inventory", lambda *args: "segment")
    audit = VectorRecoveryAudit(SimpleNamespace(partition=lambda scope: tmp_path), PipelineScope("u", "a", "c", "s"))
    audit.validate(("one",))
    before = _file_fingerprint(authority)
    authority.with_name(authority.name + suffix).write_bytes(b"new-sidefile")
    assert _file_fingerprint(authority) == before
    with pytest.raises(ValueError, match="authority"):
        audit.validate(("two",))
