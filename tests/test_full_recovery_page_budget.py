"""Linear proof accounting over real temporary SQLite rows, without models."""

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import sqlite3
import time

import pytest

from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical
from system_memory_god.models import MemoryItem
from system_memory_god.storage import daily_vector_pages as pages
from system_memory_god.storage.derived_evidence import DerivedEvidenceStore


@contextmanager
def page_fixture(count=100, *, large=False):
    db = sqlite3.connect(":memory:")
    owner = pages.QualifiedPageOwner(None, PipelineScope("u", "a", "c", "s"),
                                    time.monotonic_ns() + 60_000_000_000, 0)
    try:
        db.execute("CREATE TABLE memory_items(memory_id,store,store_ref,item_json)")
        db.execute("CREATE TABLE memory_sources(memory_id,raw_message_id,revision,source_json)")
        db.execute("CREATE TABLE derived_evidence(memory_id,task_id,output_index,domain,store_ref,"
                   "payload_json,payload_hash,source_json,created_at)")
        rows = []
        text = "\U00020000" * 8192 if large else "synthetic source"
        payload = canonical({"text": text})
        digest = hashlib.sha256(payload.encode()).hexdigest()
        suffix = "字" * 248 if large else ""
        for index in range(count):
            key = f"m{index:03}"
            source = SourceRevision("raw" + suffix, 1, f"t{index:03}" + suffix,
                                    "p" + suffix, "v" + suffix, "model" + suffix, "a" * 64)
            encoded = canonical(asdict(source))
            item = MemoryItem(memory_id=key, store="vector", store_ref="ref-" + key,
                              content_preview="\U00020000" * 800 if large else "preview")
            db.execute("INSERT INTO memory_items VALUES(?,?,?,?)",
                       (key, "vector", item.store_ref, canonical(asdict(item))))
            db.execute("INSERT INTO memory_sources VALUES(?,?,?,?)",
                       (key, source.raw_message_id, source.revision, encoded))
            db.execute("INSERT INTO derived_evidence VALUES(?,?,?,?,?,?,?,?,?)",
                       (key, source.task_id, 0, "vector", item.store_ref, payload, digest, encoded, "synthetic"))
            rows.append({"memory_id": key, "store_ref": item.store_ref, "source": source})
        owner.ledger_db = owner.evidence_db = db
        owner.evidence = DerivedEvidenceStore
        owner.iterator = iter((tuple(rows),))
        yield owner, rows
    finally:
        db.close()
        owner.executor.shutdown(wait=True)


def test_page_proof_walks_are_linear_not_one_full_graph_per_record(monkeypatch):
    visits = 0
    original = pages.fields

    def observed(value):
        nonlocal visits
        if isinstance(value, SourceRevision):
            visits += 1
        return original(value)

    with page_fixture() as (owner, _):
        monkeypatch.setattr(pages, "fields", observed)
        assert len(owner._next()) == 100
        assert visits <= 1000, "fixed page sources must not be traversed for every row"


def test_long_unicode_source_payload_and_transients_fit_without_bulk_decode():
    with page_fixture(100, large=True) as (owner, _):
        result = owner._next()
        assert len(result) == 100
        assert all(row["text_bytes"] == 32768 for row in result)
        assert 768 * 1024 < owner.metadata_peak <= 2 * 1024**2


def test_metadata_limit_is_exact_at_observed_conservative_bound(monkeypatch):
    with page_fixture(7, large=True) as (owner, _):
        owner._next()
        maximum = owner.metadata_peak
    monkeypatch.setattr(pages, "METADATA_WORKING_BYTES", maximum)
    with page_fixture(7, large=True) as (owner, _):
        assert len(owner._next()) == 7
        assert owner.metadata_peak == maximum
    monkeypatch.setattr(pages, "METADATA_WORKING_BYTES", maximum - 1)
    with page_fixture(7, large=True) as (owner, _):
        with pytest.raises(ValueError, match="working set"):
            owner._next()


def test_incremental_ledger_peak_dominates_complete_retained_graph():
    with page_fixture(100, large=True) as (owner, rows):
        ids = tuple(row["memory_id"] for row in rows)
        ledger = owner._ledger_page(ids, rows)
        assert owner.metadata_peak >= pages.metadata_size(rows, ids, ledger)
