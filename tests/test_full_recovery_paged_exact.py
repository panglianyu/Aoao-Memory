"""Regression seams for bounded page ownership and deadline handoff."""

import asyncio
import time

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector


@pytest.mark.parametrize("layer", ["raw", "pipeline", "gate"])
def test_page_metadata_sql_uses_original_deadline(tmp_path, monkeypatch, layer):
    import sqlite3
    from dataclasses import replace
    import kernel.raw_message_store as raw_module
    from tests.test_raw_message_store import opened

    def slow_sql(db, *args, **kwargs):
        return db.execute("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000) "
                          "SELECT sum(x) FROM n").fetchone()
    scope = PipelineScope("u", "a", "c", "s")
    stores = ScopedStores(tmp_path / "derived")
    if layer == "raw":
        store, raw_scope = opened(tmp_path)
        raw_scope = replace(raw_scope, connector_id="wx-main", conversation_id="group-1")
        monkeypatch.setattr(raw_module, "query_raw_partition_state", slow_sql)
        def call(deadline):
            return store.partition_state(raw_scope, deadline=deadline)
    elif layer == "pipeline":
        database = stores.ledger(scope)._db
        with database.transaction():
            pass
        monkeypatch.setattr(database, "_recognize", slow_sql)
        def call(deadline):
            with database.readonly_snapshot(deadline=deadline):
                pytest.fail("deadline did not stop recognition SQL")
    else:
        gate = stores.gate(scope)
        monkeypatch.setattr(gate._database, "_recognize", slow_sql)
        def call(deadline):
            connection = gate._acquire(deadline=deadline)
            gate._release(connection)
            pytest.fail("deadline did not stop gate recognition SQL")
    began = time.monotonic()
    with pytest.raises((TimeoutError, sqlite3.Error, raw_module.RawMessageError)):
        call(began + 0.02)
    assert time.monotonic() - began < 0.2


@pytest.mark.asyncio
async def test_closed_raw_generator_cannot_claim_retirement_after_close_failure():
    from types import SimpleNamespace
    from system_memory_god.storage.daily_vector_pages import QualifiedPageOwner
    events = []
    def raw_rows():
        try:
            yield ()
        finally:
            raise OSError("controlled underlying Raw close failure")
    owner = QualifiedPageOwner(None, PipelineScope("u", "a", "c", "s"),
                               time.monotonic_ns() + 1_000_000_000, 0)
    owner.iterator = raw_rows()
    next(owner.iterator)
    owner.connection = object()
    owner.gate = SimpleNamespace(_release=lambda connection: events.append("released"))
    try:
        with pytest.raises(OSError):
            await owner.close()
        with pytest.raises(RuntimeError, match="Raw.*unknown"):
            await owner.close()
        assert not owner.closed and events == [] and owner.connection is not None
    finally:
        # The controlled fake has no real SQLite owner; do not leave a test thread.
        owner.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_gate_timeout_does_not_retire_unstarted_busy_writer(monkeypatch):
    from types import SimpleNamespace
    import system_memory_god.storage.daily_vector_pages as pages
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot
    scope = PipelineScope("u", "a", "c", "s")
    events = []
    class Transport:
        worker_pid = 123
        def __init__(self, name):
            self.name = name
        async def acquire_session_owner(self, deadline):
            return self.name
        def release_session_owner(self, owner):
            events.append(("release", owner))
        async def _invoke(self, operation, scope, **kwargs):
            page = kwargs["page"]
            return {key: page[key] for key in pages.BINDING_KEYS} | {
                "sequence": 0, "last_key": "", "count": 0, "worker_pid": self.worker_pid}
        async def retire_worker(self):
            events.append(("retire", self.name))
            if self.name == "writer":
                raise RuntimeError("unrelated writer still pending")
    class SourceOwner:
        def __init__(self, *args):
            pass
        async def open(self):
            raise TimeoutError("controlled gate wait deadline")
        async def close(self):
            events.append(("close", "source"))
    monkeypatch.setattr(pages, "QualifiedPageOwner", SourceOwner)
    facade = object.__new__(SupervisedDailyVectorSnapshot)
    facade.paged_runtime = SimpleNamespace(vector=Transport("writer"))
    facade._transport = Transport("reader")
    facade._lifecycle = asyncio.Lock()
    facade._ready = {scope: "0"}
    facade._failed_page_owners = []
    facade.available = lambda scope: True
    with pytest.raises(TimeoutError, match="gate wait"):
        await facade._search_paged(scope, "query", 1, time.monotonic() + 1)
    assert ("retire", "writer") not in events
    assert ("retire", "reader") in events
    assert not facade._failed_page_owners


@pytest.mark.asyncio
async def test_raw_page_connection_is_retried_after_real_context_close_failure(tmp_path, monkeypatch):
    import sqlite3
    from dataclasses import replace
    from types import SimpleNamespace
    import kernel.raw_message_database as raw_database
    from tests.test_raw_message_store import opened
    from system_memory_god.storage.daily_vector_pages import QualifiedPageOwner
    store, scope = opened(tmp_path)
    scope = replace(scope, connector_id="wx-main", conversation_id="group-1")
    partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
    stores = ScopedStores(tmp_path / "derived")
    for target in (stores.evidence(partition), stores.ledger(partition)):
        with target._db.transaction():
            pass
    connections = []
    original_connect = sqlite3.connect
    class Connection(sqlite3.Connection):
        close_calls = 0
        def close(self):
            self.close_calls += 1
            if len(connections) > 1 and self is connections[1] and self.close_calls == 1:
                raise OSError("controlled real Raw connection close failure")
            super().close()
    def connect(database, *args, **kwargs):
        if "raw_messages.db" in str(database):
            kwargs["factory"] = Connection
            value = original_connect(database, *args, **kwargs)
            connections.append(value)
            return value
        return original_connect(database, *args, **kwargs)
    monkeypatch.setattr(raw_database.sqlite3, "connect", connect)
    owner = QualifiedPageOwner(SimpleNamespace(raw=SimpleNamespace(store=store), stores=stores),
                               partition, time.monotonic_ns() + 2_000_000_000, 0)
    try:
        await owner.open()
        assert await owner.next_page() is None
        with pytest.raises(OSError, match="real Raw"):
            await owner.close()
        assert not owner.closed and owner.connection is not None
        assert owner.contexts[-1][1] is connections[1]
        await owner.close()
        assert owner.closed and owner.connection is None and connections[1].close_calls == 2
    finally:
        if not owner.closed:
            await owner.close()


def test_page_ledger_streams_encoded_sources_and_counts_them_in_budget():
    import sqlite3
    from dataclasses import asdict
    from kernel.memory_pipeline_models import SourceRevision, canonical
    from system_memory_god.storage.daily_vector_pages import QualifiedPageOwner
    owner = QualifiedPageOwner(None, PipelineScope("u", "a", "c", "s"),
                               time.monotonic_ns() + 2_000_000_000, 0)
    class NoBulkCursor:
        def __init__(self, cursor):
            self.cursor = cursor
        def __iter__(self):
            return iter(self.cursor)
        def fetchall(self):
            pytest.fail("page ledger must not retain all encoded sources")
    class Database:
        def __init__(self, db):
            self.db = db
        def execute(self, *args):
            return NoBulkCursor(self.db.execute(*args))
    db = sqlite3.connect(":memory:")
    try:
        db.execute("CREATE TABLE memory_items(memory_id,store,store_ref,item_json)")
        db.execute("CREATE TABLE memory_sources(memory_id,raw_message_id,revision,source_json)")
        ids, rows = [], []
        for index in range(100):
            key = f"m{index:03}"
            # Legal large Unicode identities exercise encoded and parsed proof
            # objects; no source string is replaced by a short display ID.
            source = SourceRevision("a" + "字" * 255, 1, f"t{index}" + "字" * 250,
                                    "p" + "字" * 255, "v" + "字" * 255, "m" + "字" * 255, "a" * 64)
            encoded = canonical(asdict(source))
            db.execute("INSERT INTO memory_sources VALUES(?,?,?,?)",
                       (key, source.raw_message_id, source.revision, encoded))
            ids.append(key)
            rows.append({"memory_id": key, "source": source})
        owner.ledger_db = Database(db)
        result = owner._ledger_page(tuple(ids), rows)
        assert len(result) == 100
        assert 768 * 1024 < owner.metadata_peak <= 2 * 1024**2
    finally:
        db.close()
        owner.executor.shutdown(wait=True)


def test_paged_scores_include_tail_and_reject_missing_sequence():
    import base64
    import hashlib
    import numpy as np
    from system_memory_god.storage.daily_vector_pages import PagedCosine

    binding = {"query_id": "q1", "generation": 7, "deadline_ns": time.monotonic_ns() + 10_000_000_000}
    scorer = PagedCosine(binding, [1.0, 0.0], 2)
    for sequence, start in enumerate(range(0, 12000, 100)):
        rows = []
        for index in range(start, start + 100):
            body = np.asarray([1.0, 0.0] if index == 11999 else [0.0, 1.0], dtype="<f4").tobytes()
            rows.append({"memory_id": f"m{index:05}", "f32": base64.b64encode(body).decode(),
                         "sha256": hashlib.sha256(body).hexdigest(), "dimension": 2})
        scorer.feed({**binding, "sequence": sequence, "last_key": "" if not start else f"m{start-1:05}", "rows": rows})
    result = scorer.finish({**binding, "sequence": 120, "last_key": "m11999", "count": 12000})
    assert result["hits"] == [("m11999", 1.0), ("m00000", 0.0)]
    bad = PagedCosine(binding, [1.0, 0.0], 2)
    with pytest.raises(ValueError, match="sequence"):
        bad.feed({**binding, "sequence": 1, "last_key": "", "rows": rows})


@pytest.mark.parametrize("values", [[0.0, 0.0], [float("nan"), 1.0]])
def test_paged_write_receipt_rejects_non_searchable_numbers(values):
    import base64
    import hashlib
    import struct
    from system_memory_god.storage.daily_vector_pages import validate_receipts
    body = struct.pack("<2f", *values)
    receipt = {"embedding_dim": 2, "snapshot": {"schema": 1,
               "f32": base64.b64encode(body).decode(), "sha256": hashlib.sha256(body).hexdigest()}}
    with pytest.raises(ValueError, match="numerical values"):
        validate_receipts([receipt])


@pytest.mark.asyncio
async def test_fence_drains_accepted_work_and_holds_new_admission(tmp_path, monkeypatch):
    transport = SupervisedDailyVector(ScopedStores(tmp_path), embedding="synthetic")
    scope = PipelineScope("u", "a", "c", "s")
    transport._pending = 1
    acquire = asyncio.create_task(transport.acquire_deadline_fence(time.monotonic_ns() + 2_000_000_000))
    await asyncio.sleep(0)
    assert not acquire.done()
    transport._pending = 0
    token = await acquire
    calls = []
    monkeypatch.setattr(transport, "_call", lambda *args, **kwargs: calls.append(args[0]) or True)
    blocked = asyncio.create_task(transport._invoke("contains", scope, ids=("id",)))
    await asyncio.sleep(0)
    assert calls == []
    target = time.monotonic_ns() + 1_000_000_000
    token.commit_deadline(target)
    assert token.deadline_ns == target
    token.release()
    assert await blocked is True
    assert calls == ["contains"]


@pytest.mark.asyncio
async def test_cancelled_fence_withdraws_admission_without_retiring_work(tmp_path):
    transport = SupervisedDailyVector(ScopedStores(tmp_path), embedding="synthetic")
    transport._pending = 1
    acquire = asyncio.create_task(transport.acquire_deadline_fence(time.monotonic_ns() + 2_000_000_000))
    await asyncio.sleep(0)
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    assert transport._deadline_fence is None
    assert transport._pending == 1


@pytest.mark.asyncio
async def test_unknown_handoff_stays_closed_after_fence_release(tmp_path):
    transport = SupervisedDailyVector(ScopedStores(tmp_path), embedding="synthetic")
    token = await transport.acquire_deadline_fence(time.monotonic_ns() + 1_000_000_000)
    token.release(failed=True)
    assert not transport._closed
    with pytest.raises(RuntimeError, match="handoff"):
        await transport._invoke("contains", PipelineScope("u", "a", "c", "s"), ids=("id",))


@pytest.mark.asyncio
async def test_admitted_page_continuation_drains_through_a_handoff_fence(tmp_path, monkeypatch):
    transport = SupervisedDailyVector(ScopedStores(tmp_path), embedding="synthetic")
    owner = await transport.acquire_session_owner(time.monotonic() + 2)
    fence_task = asyncio.create_task(transport.acquire_deadline_fence(time.monotonic_ns() + 2_000_000_000))
    await asyncio.sleep(0)
    assert not fence_task.done()
    monkeypatch.setattr(transport, "_call", lambda *args, **kwargs: True)
    assert await transport._invoke("contains", PipelineScope("u", "a", "c", "s"),
                                   ids=("id",), _admission_owner=owner) is True
    transport.release_session_owner(owner)
    fence = await fence_task
    fence.release()


@pytest.mark.asyncio
async def test_owner_close_failure_keeps_gate_and_can_finish_same_owner():
    from types import SimpleNamespace
    from system_memory_god.storage.daily_vector_pages import QualifiedPageOwner
    owner = QualifiedPageOwner(None, PipelineScope("u", "a", "c", "s"),
                               time.monotonic_ns() + 1_000_000_000, 0)
    events = []
    class Cursor:
        attempts = 0
        def close(self):
            self.attempts += 1
            if self.attempts == 1:
                raise OSError("controlled cursor close failure")
            events.append("cursor_closed")
    owner.iterator = Cursor()
    owner.connection = object()
    owner.gate_acquired = True
    owner.gate = SimpleNamespace(_release=lambda connection: events.append("gate_released"))
    with pytest.raises(OSError):
        await owner.close()
    assert events == [] and not owner.closed and owner.connection is not None
    await owner.close()
    assert events == ["cursor_closed", "gate_released"] and owner.closed


@pytest.mark.asyncio
async def test_paged_query_lifecycle_wait_uses_original_deadline():
    from types import SimpleNamespace
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot
    facade = object.__new__(SupervisedDailyVectorSnapshot)
    facade.paged_runtime = SimpleNamespace(vector=object())
    facade._transport = object()
    facade._lifecycle = asyncio.Lock()
    await facade._lifecycle.acquire()
    try:
        with pytest.raises(TimeoutError, match="page lifecycle"):
            await asyncio.wait_for(facade._search_paged(PipelineScope("u", "a", "c", "s"),
                "query", 1, time.monotonic() + 0.02), timeout=0.2)
        assert facade._lifecycle.locked()
    finally:
        facade._lifecycle.release()


@pytest.mark.asyncio
async def test_real_raw_chroma_and_two_process_page_query(tmp_path):
    from dataclasses import replace
    from kernel.raw_message_models import MessageDraft
    from system_memory_god.derivation import daily_specs
    from system_memory_god.memory_factory import create_memory_service
    from tests.test_daily_memory_early_acceptance import SyntheticExtractor
    from tests.test_raw_message_service import profile

    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived", derivations=daily_specs("synthetic"))
    service = create_memory_service(settings, mode="legacy-daily-v1", extractor=SyntheticExtractor(),
                                    embedding="synthetic", semantic_mode="paged_exact")
    try:
        await service.start()
        scope = service.scopes[0]
        await service.add(scope, MessageDraft("m1", scope.conversation_id, "s1", "user", "hello",
                         "text", "2026-11-05T00:00:00+00:00", "web", scope.user_id, idempotency_key="m1"))
        state = await service.wait_until_published(scope, deadline=time.monotonic() + 30)
        assert state["status"] == "current", state
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        result = await service.runtime.snapshot_reader.search_cosine_hits(
            partition, "acceptance-semantic-key", limit=2, purpose="interactive")
        assert len(result) == 1 and result[0][1] > 0.999
        assert service.runtime.vector._snapshot_rows == {}
        assert service.runtime.vector._admission_owners == set()
        assert service.runtime.snapshot_reader._transport._admission_owners == set()
    finally:
        await service.close()
