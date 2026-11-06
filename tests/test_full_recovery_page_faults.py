"""Faults on real temporary Raw/Chroma and writer/reader processes; no cloud IO."""

import asyncio
from contextlib import asynccontextmanager, closing
from pathlib import Path
import sqlite3
import tempfile
import time

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.daily_vector_pages import QualifiedPageOwner
from tests.test_full_recovery_growth import draft, service


@asynccontextmanager
async def published():
    # Short paths keep Windows Chroma names within its platform path limit.
    directory = tempfile.TemporaryDirectory(prefix="f-", dir=tempfile.gettempdir())
    directory._finalizer.detach()
    root = Path(directory.name).resolve()
    assert root.parent == Path(tempfile.gettempdir()).resolve() and root.name.startswith("f-")
    memory = service(root)
    try:
        await memory.start()
        scope = memory.scopes[0]
        await memory.add(scope, draft(scope, 0))
        state = await memory.wait_until_published(scope, deadline=time.monotonic() + 30)
        assert state["status"] == "current", state
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        yield memory, scope, partition
    finally:
        await memory.close()
        # Unknown process/SQLite ownership preserves the fixture for diagnosis.
        directory.cleanup()


def pause_first_page(monkeypatch):
    reached, release = asyncio.Event(), asyncio.Event()
    owners = []
    original = QualifiedPageOwner.next_page

    async def next_page(owner):
        rows = await original(owner)
        if rows is not None and owner not in owners:
            owners.append(owner)
            reached.set()
            await release.wait()
        return rows

    monkeypatch.setattr(QualifiedPageOwner, "next_page", next_page)
    return reached, release, owners


def gate_is_held(memory, partition):
    path = memory.runtime.stores.gate(partition)._database.path
    with closing(sqlite3.connect(path, timeout=.01, isolation_level=None)) as db:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            db.execute("BEGIN IMMEDIATE")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["edit", "insert", "purge_intent"])
async def test_changed_source_never_returns_partial_paged_result(monkeypatch, mutation):
    async with published() as (memory, scope, partition):
        reached, release, owners = pause_first_page(monkeypatch)
        reader = memory.runtime.snapshot_reader
        query = asyncio.create_task(reader.search_cosine_hits(
            partition, "ordinary source", limit=1, purpose="interactive"))
        try:
            await asyncio.wait_for(reached.wait(), 1.5)
            await asyncio.to_thread(gate_is_held, memory, partition)
            if mutation == "edit":
                await memory.edit(scope, "growth-message-000000", "updated ordinary source",
                                  expected_revision=1, edited_at="2026-11-05T00:00:01+00:00")
            elif mutation == "insert":
                await memory.add(scope, draft(scope, 1))
            else:
                # This is the real first step of RawMessageRetrieval.purge.
                # Retain the tombstone and original data; no blind deletion in a test hook.
                await asyncio.to_thread(memory.raw.store.begin_purge, scope,
                                        "growth-message-000000", expected_revision=1)
            release.set()
            with pytest.raises((RuntimeError, ValueError)):
                await query
            assert owners[0].closed and owners[0].connection is None
            assert not reader._failed_page_owners
            assert not reader._paged_diagnostic.get("complete", False)
            with memory.raw.store._db() as db:
                assert db.execute("SELECT COUNT(*) FROM raw_messages").fetchone()[0] == (2 if mutation == "insert" else 1)
                assert db.execute("SELECT COUNT(*) FROM raw_message_revisions").fetchone()[0] >= 1
        finally:
            release.set()
            if not query.done():
                query.cancel()
            await asyncio.gather(query, return_exceptions=True)


@pytest.mark.asyncio
async def test_actual_reader_process_failure_retires_page_owner(monkeypatch):
    async with published() as (memory, scope, partition):
        reader = memory.runtime.snapshot_reader
        transport = reader._transport
        original = transport._invoke
        killed = []

        async def invoke(operation, *args, **kwargs):
            if operation == "page_score" and not killed:
                process = transport._process
                assert process is not None and process.is_alive()
                killed.append(process.pid)
                process.terminate()
                await asyncio.to_thread(process.join, 2)
                assert not process.is_alive()
            return await original(operation, *args, **kwargs)

        monkeypatch.setattr(transport, "_invoke", invoke)
        with pytest.raises((RuntimeError, OSError, TimeoutError)):
            await reader.search_cosine_hits(partition, "ordinary source", limit=1, purpose="interactive")
        assert len(killed) == 1 and reader.worker_pid is None
        assert not reader._failed_page_owners and not reader.available(partition)
        assert not memory.runtime.vector._admission_owners and not transport._admission_owners
        assert memory.raw.store.get(scope, "growth-message-000000") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancelled_waiter", "deadline"])
async def test_real_page_owner_keeps_gate_until_query_settles(monkeypatch, failure):
    async with published() as (memory, scope, partition):
        reached, release, owners = pause_first_page(monkeypatch)
        reader = memory.runtime.snapshot_reader
        deadline = time.monotonic() + (1 if failure == "deadline" else 2)
        query = asyncio.create_task(reader.search_cosine_hits(
            partition, "ordinary source", limit=1, purpose="interactive", deadline=deadline))
        try:
            await asyncio.wait_for(reached.wait(), .8)
            if failure == "cancelled_waiter":
                query.cancel()
                await asyncio.sleep(0)
                assert not query.done()
            else:
                await asyncio.sleep(max(0, deadline - time.monotonic()) + .01)
            assert not owners[0].closed
            await asyncio.to_thread(gate_is_held, memory, partition)
            release.set()
            with pytest.raises(asyncio.CancelledError if failure == "cancelled_waiter" else TimeoutError):
                await query
            assert owners[0].closed and owners[0].connection is None
            assert not reader._failed_page_owners
            assert not memory.runtime.vector._admission_owners and not reader._transport._admission_owners
            assert memory.raw.store.get(scope, "growth-message-000000") is not None
        finally:
            release.set()
            if not query.done():
                query.cancel()
            await asyncio.gather(query, return_exceptions=True)


@pytest.mark.asyncio
async def test_gate_recognition_and_close_failure_retains_actual_connection(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from system_memory_god.storage.derivation_gate import DailyDerivationGate
    from system_memory_god.storage.scoped_stores import ScopedStores

    partition = PipelineScope("u", "a", "c", "s")
    gate = DailyDerivationGate(tmp_path, partition)
    connections = []
    connect = sqlite3.connect

    class Connection(sqlite3.Connection):
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise OSError("controlled gate close failure")
            super().close()

    def observed_connect(*args, **kwargs):
        kwargs["factory"] = Connection
        db = connect(*args, **kwargs)
        connections.append(db)
        return db

    def rejected(*args, **kwargs):
        raise ValueError("controlled gate recognition failure")

    monkeypatch.setattr(sqlite3, "connect", observed_connect)
    monkeypatch.setattr(gate._database, "_recognize", rejected)
    stores = ScopedStores(tmp_path)
    monkeypatch.setattr(stores, "gate", lambda requested: gate)
    owner = QualifiedPageOwner(SimpleNamespace(stores=stores), partition,
                               time.monotonic_ns() + 2_000_000_000, 0)
    try:
        with pytest.raises(OSError, match="gate close"):
            await owner.open()
        assert owner.connection is connections[0] and not owner.closed
        assert connections[0].in_transaction and not owner.gate_acquired
        await owner.close()
        assert owner.closed and owner.connection is None and connections[0].close_calls == 2
    finally:
        if not owner.closed:
            await owner.close()
