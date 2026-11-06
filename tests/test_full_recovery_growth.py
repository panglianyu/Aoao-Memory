"""Real persisted growth, synthetic 1024D only; fixture build is not Add throughput."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import datetime
import gc
import json
import multiprocessing
import os
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
import threading
import time

import numpy as np
import psutil
import pytest

from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical
from kernel.raw_message_models import MessageDraft, ResultAnchor, Scope
from kernel.raw_message_store import RawMessageStore
from kernel import raw_message_outbox
from system_memory_god.grouped_extractor import memory_specs
from system_memory_god.memory_factory import create_memory_service
from system_memory_god.models import MemoryItem
from system_memory_god.storage.daily_vector_worker import _synthetic_embed_1024
from system_memory_god.storage.derivation_runs import _snapshot
from system_memory_god.storage.scoped_stores import ScopedStores
from tests.test_raw_message_service import profile

STAMP = "2026-11-05T00:00:00+00:00"
QUERY = "synthetic growth complete-tail query"
LIMIT = 10


class VectorExtractor:
    async def extract(self, domain, message, source, max_output_tokens):
        assert domain == "vector"
        return ({"text": message.content},)


def settings(root):
    return replace(profile(root / "raw"), derived_root=root / "derived",
                   derivations=memory_specs("raw-vector-v1"))


def service(root):
    return create_memory_service(settings(root), mode="raw-vector-v1", extractor=VectorExtractor(),
                                 embedding="synthetic-1024", semantic_mode="paged_exact")


def draft(scope, index):
    key = f"growth-message-{index:06}"
    return MessageDraft(key, scope.conversation_id, "synthetic-growth-session", "user",
                        f"ordinary synthetic growth source {index}", "text", STAMP, "web", scope.user_id,
                        idempotency_key=key, derivations=memory_specs("raw-vector-v1"))


class ResourceSample:
    """Observe this pytest process and its own descendants, never unrelated processes."""
    def __init__(self):
        self.stop_event = threading.Event()
        self.peak_rss = 0
        self.minimum_available = psutil.virtual_memory().available
        self.failure = None
        self.thread = threading.Thread(target=self._run, name="growth-rss", daemon=True)

    def _sample(self):
        parent = psutil.Process(os.getpid())
        processes = [parent, *parent.children(recursive=True)]
        total = 0
        for process in processes:
            try:
                total += process.memory_info().rss
            except psutil.NoSuchProcess:
                pass
        self.peak_rss = max(self.peak_rss, total)
        self.minimum_available = min(self.minimum_available, psutil.virtual_memory().available)

    def _run(self):
        while not self.stop_event.wait(.05):
            try:
                self._sample()
            except Exception as error:
                self.failure = type(error).__name__
                return

    def check(self):
        self._sample()
        assert self.failure is None, "resource observation unavailable"
        assert self.peak_rss <= 6 * 1024**3, "candidate process tree crossed 6GiB sample stop line"
        assert self.minimum_available >= 2 * 1024**3, "host available memory crossed 2GiB stop line"

    def __enter__(self):
        self.check()
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join()


def _bulk_build(root_text, scope_values, count):
    """A separate child owns fixture-only Chroma handles and exits before cold startup."""
    root = Path(root_text)
    scope = Scope(*scope_values)
    partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
    stores = ScopedStores(root / "derived")
    raw = RawMessageStore(settings(root).database)
    evidence, ledger, runs = stores.evidence(partition), stores.ledger(partition), stores.runs(partition)
    registry = stores.partition_registry()
    registry.begin(partition)
    with ExitStack() as stack:
        raw_db = stack.enter_context(raw._db(write=True))
        evidence_db = stack.enter_context(evidence._db.transaction())
        ledger_db = stack.enter_context(ledger._db.transaction())
        runs_db = stack.enter_context(runs._db.transaction())
        for index in range(1, count):
            original = draft(scope, index)
            stored, created = raw._append_record(raw_db, scope, original)
            assert created and stored.revision == 1
            task = raw_db.execute("SELECT * FROM derivation_outbox WHERE message_id=?",
                                  (original.message_id,)).fetchone()
            assert task is not None and task["target_domain"] == "vector"
            token = f"synthetic-growth-lease-{index}"
            raw_db.execute("UPDATE derivation_outbox SET status='leased',attempts=1,lease_owner=?,"
                           "lease_token=?,lease_until=? WHERE task_id=?",
                           ("synthetic-growth-builder", token, time.time() + 600, task["task_id"]))
            raw_db.execute("INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'claimed',?)",
                           (task["task_id"], time.time()))
            source = SourceRevision(stored.message_id, 1, task["task_id"], task["processor"],
                                    task["processor_version"], task["model"], task["prompt_hash"])
            payload = {"text": stored.content}
            record = evidence._put(evidence_db, source, "vector", 0, payload)
            encoded = canonical(payload)
            frozen = _snapshot(source, "vector", (encoded,), "")
            runs_db.execute("INSERT INTO derivation_runs VALUES(?,?,?,?,?)",
                            (source.task_id, frozen, 1, token, "ledger_written"))
            item = MemoryItem(memory_id=record["memory_id"], store="vector", store_ref=record["store_ref"],
                              actor="user", created_at=datetime.fromisoformat(STAMP).timestamp(), status="active")
            ledger_db.execute("INSERT INTO memory_items VALUES(?,?,?,?)",
                              (item.memory_id, item.store, item.store_ref, canonical(asdict(item))))
            ledger_db.execute("INSERT INTO memory_sources VALUES(?,?,?,?,?)",
                              (item.memory_id, source.raw_message_id, 1, canonical(asdict(source)), STAMP))
            ledger_db.execute("INSERT INTO memory_events VALUES(?,'created',?)", (item.memory_id, STAMP))
            raw_message_outbox.complete(raw_db, scope, source.task_id, token,
                                       (ResultAnchor(item.memory_id, item.store_ref),))
    state = raw.partition_state(scope)
    assert state[1] is True
    # Keep INITIALIZING: the real cold runtime must audit all persisted output,
    # receipts and Raw anchors and itself call registry.ready; do not fake current.
    from chromadb import PersistentClient
    from chromadb.config import Settings

    client = PersistentClient(path=str(stores.partition(partition) / "vectors"),
                              settings=Settings(anonymized_telemetry=False))
    collection = client.get_collection("daily_evidence", embedding_function=None)
    query = np.asarray(_synthetic_embed_1024([QUERY])[0], dtype=np.float32)
    with evidence._db.readonly_snapshot() as db:
        tail = db.execute("SELECT MAX(memory_id) FROM derived_evidence").fetchone()[0]
        assert db.execute("SELECT COUNT(*) FROM derived_evidence").fetchone()[0] == count
        matrix = np.lib.format.open_memmap(root / "reference.npy", mode="w+", dtype=np.float32,
                                          shape=(count, 1024))
        ids, documents, metadatas, vectors = [], [], [], []
        offset = 0
        for memory_id, body, digest in db.execute(
                "SELECT memory_id,payload_json,payload_hash FROM derived_evidence ORDER BY memory_id"):
            vector = query.copy()
            if memory_id != tail:
                vector[0] *= -float(1 + offset % 97)
            matrix[offset] = vector
            offset += 1
            ids.append(memory_id)
            documents.append(json.loads(body)["text"])
            metadatas.append({"payload_hash": digest})
            vectors.append(vector.tolist())
            if len(ids) == 128:
                collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=vectors)
                ids, documents, metadatas, vectors = [], [], [], []
        if ids:
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=vectors)
        assert offset == count and collection.count() == count
        matrix.flush()
        del matrix
    assert registry.snapshot(partition)["state"] == "INITIALIZING"
    # The child exits naturally, which retires every in-process Chroma thread,
    # connection and system cache before the parent starts actual cold workers.


async def _build_in_child(root, scope, count, resources, ownership):
    process = multiprocessing.get_context("spawn").Process(
        target=_bulk_build, args=(str(root), (scope.agent_id, scope.user_id,
                                             scope.connector_id, scope.conversation_id), count),
        name="aru-synthetic-growth-build")
    process.start()
    ownership["retired"] = False
    began = time.monotonic()
    try:
        while process.is_alive():
            resources.check()
            if time.monotonic() - began > 600:
                raise TimeoutError("synthetic fixture builder exceeded 600 seconds")
            await asyncio.sleep(.1)
        process.join()
        assert process.exitcode == 0, "synthetic corpus build failed"
    finally:
        if process.is_alive():
            # This exact builder has no service/worker children: the normal
            # mother service was completely closed before it was started.
            process.terminate()
            await asyncio.to_thread(process.join, 10)
        if process.is_alive():
            raise RuntimeError("synthetic builder retirement remains unknown")
        ownership["retired"] = True
        process.close()


def _reference(root):
    query = np.asarray(_synthetic_embed_1024([QUERY])[0], dtype=np.float64)
    matrix = np.load(root / "reference.npy", mmap_mode="r")
    try:
        norms = np.sqrt(np.einsum("ij,ij->i", matrix, matrix, dtype=np.float64))
        scores = np.einsum("ij,j->i", matrix, query, dtype=np.float64) / (norms * np.linalg.norm(query))
        return scores
    finally:
        matrix._mmap.close()


async def _exercise(count, record_property):
    # The runner owns this short TEMP root and counts its complete size.
    parent = Path(gettempdir())
    directory = TemporaryDirectory(prefix="g-", dir=parent)
    directory._finalizer.detach()  # Retain failed/unknown ownership; no GC deletion.
    root = Path(directory.name)
    report = {"rows": count, "dimension": 1024, "fixture_build_is_add_throughput": False}
    active = None
    safe_cleanup = False
    builder = {"retired": True}
    with ResourceSample() as resources:
        try:
            began = time.monotonic()
            active = service(root)
            await active.start()
            scope = active.scopes[0]
            await active.add(scope, draft(scope, 0))
            initial = await active.wait_until_published(scope, deadline=time.monotonic() + 60)
            assert initial["status"] == "current", initial
            await active.close()
            active = None
            await _build_in_child(root, scope, count, resources, builder)
            report["fixture_build_seconds"] = time.monotonic() - began
            resources.check()
            scores = await asyncio.to_thread(_reference, root)
            # IDs come from the same persisted, audited metadata order as the
            # input matrix, while the reference scorer is the full matrix path.
            stores = ScopedStores(root / "derived")
            partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
            with stores.evidence(partition)._db.readonly_snapshot() as db:
                keys = [row[0] for row in db.execute("SELECT memory_id FROM derived_evidence ORDER BY memory_id")]
            expected = sorted(zip(keys, map(float, scores)), key=lambda item: (-item[1], item[0]))[:LIMIT]
            expected = [(key, float(np.clip(score, -1, 1))) for key, score in expected]
            assert expected[0][0] == keys[-1], "fixture's best hit must require the final keyset page"
            del keys, scores
            gc.collect()
            began = time.monotonic()
            active = service(root)
            await active.start()
            cold = await active.wait_until_published(scope, deadline=time.monotonic() + 120)
            report["cold_publication_seconds"] = time.monotonic() - began
            assert cold["status"] == "current", cold
            reader = active.runtime.snapshot_reader
            for label in ("cold_search_seconds", "warm_search_seconds"):
                began = time.monotonic()
                hits = await reader.search_cosine_hits(partition, QUERY, limit=LIMIT, purpose="interactive")
                report[label] = time.monotonic() - began
                assert [row[0] for row in hits] == [row[0] for row in expected]
                assert np.allclose([row[1] for row in hits], [row[1] for row in expected], rtol=0, atol=1e-12)
                assert report[label] < 2.0, "exact complete query exceeded its original interactive deadline"
                diagnostic = dict(reader._paged_diagnostic)
                assert diagnostic["complete"] is True and diagnostic["rows"] == count
                assert diagnostic["pages"] == (count + 99) // 100
                assert diagnostic["metadata_peak"] <= 2 * 1024**2
                report["page_metadata_peak"] = max(report.get("page_metadata_peak", 0), diagnostic["metadata_peak"])
            assert active.runtime.vector._snapshot_rows == {}
            assert not active.runtime.vector._admission_owners
            assert not reader._transport._admission_owners
            began = time.monotonic()
            await active.add(scope, draft(scope, count))
            appended = await active.wait_until_published(scope, deadline=time.monotonic() + 60)
            report["one_synthetic_add_seconds"] = time.monotonic() - began
            assert appended["status"] == "current", appended
            assert active.raw.store.partition_state(scope)[1] is True
            report["fixture_bytes"] = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
            assert report["fixture_bytes"] <= 4 * 1024**3
            resources.check()
        finally:
            if active is not None:
                await active.close()
            safe_cleanup = builder["retired"]
            report.update(peak_process_tree_rss=resources.peak_rss,
                          minimum_available_memory=resources.minimum_available)
            record_property("synthetic_growth", json.dumps(report, sort_keys=True))
            if safe_cleanup:
                directory.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1000, 5002, 12000, 40000])
async def test_persisted_1024d_growth_complete_exact_query(count, record_property):
    await _exercise(count, record_property)


@pytest.mark.asyncio
async def test_small_growth_fixture_roundtrip(record_property):
    await _exercise(12, record_property)
