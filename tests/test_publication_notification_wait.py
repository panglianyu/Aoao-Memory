"""Publication notifications prompt fresh proofs and never authorize success."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
import time

import pytest

from kernel.app_bus import AppBus
from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_profile import ConversationBinding
from kernel.raw_message_service import RawMessageService
from kernel.raw_message_worker import RawMessageWorker
from system_memory_god.daily_runtime import DailyMemoryRuntime
from system_memory_god.memory_service import MemoryService
from tests.test_daily_memory_early_acceptance import SyntheticExtractor, daily, ingress
from tests.test_daily_memory_runtime import publication_until
from tests.test_memory_terminal_publication import source_with_task
from tests.test_raw_message_service import profile


def partition_for(scope):
    return PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)


@asynccontextmanager
async def isolated(tmp_path):
    settings = profile(tmp_path / "raw")
    connector = replace(settings.connectors[0], conversations=(
        *settings.connectors[0].conversations,
        ConversationBinding("second", "second", "web", "Synthetic")))
    settings = replace(settings, connectors=(connector,), derived_root=tmp_path / "derived")
    raw = RawMessageService(settings)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: settings.connectors[0].agent_id)
    runtime = DailyMemoryRuntime(raw, settings, bus, extractor=SyntheticExtractor(),
                                 embedding="synthetic", semantic_mode="disabled")
    service = MemoryService(runtime, profile_id="memory-full-v1")
    try:
        await raw.initialize_profile()
        yield service, runtime, raw.configured_scopes()
    finally:
        # These tests can substitute RAM-only reader metadata, never a process.
        runtime.snapshot_reader = runtime.snapshot_cache = None
        await runtime.close()


def pending_state():
    return {"status": "preparing", "reason": "publication_pending", "terminal": False}


@pytest.mark.asyncio
async def test_quiet_wait_avoids_fifty_millisecond_polling(tmp_path, monkeypatch):
    async with isolated(tmp_path) as (service, runtime, scopes):
        calls = []

        async def pending(scope, *, deadline):
            calls.append(time.monotonic())
            return pending_state()

        monkeypatch.setattr(service, "status", pending)
        result = await service.wait_until_published(scopes[0], deadline=time.monotonic() + .22)
        assert result["reason"] == "publication_deadline"
        assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["current", "disabled", "stopped", "terminal"])
async def test_entry_always_checks_fresh_status_once(tmp_path, monkeypatch, kind):
    async with isolated(tmp_path) as (service, runtime, scopes):
        calls = []

        async def status(scope, *, deadline):
            calls.append(scope)
            return {"status": "unavailable" if kind == "terminal" else kind,
                    "terminal": kind == "terminal", "reason": "synthetic"}

        monkeypatch.setattr(service, "status", status)
        result = await asyncio.wait_for(service.wait_until_published(
            scopes[0], deadline=time.monotonic() + 60), .5)
        assert result["reason"] == "synthetic" and calls == [scopes[0]]


@pytest.mark.asyncio
async def test_change_during_status_is_not_lost_before_wait_registration(tmp_path, monkeypatch):
    async with isolated(tmp_path) as (service, runtime, scopes):
        partition = partition_for(scopes[0])
        calls = []

        async def status(scope, *, deadline):
            calls.append(scope)
            if len(calls) == 1:
                runtime.signal_publication_change(partition, "synthetic")
                return pending_state()
            return {"status": "current", "terminal": False}

        async def forbidden_sleep(*args, **kwargs):
            raise AssertionError("a prior change must trigger an immediate fresh query")

        monkeypatch.setattr(service, "status", status)
        with monkeypatch.context() as wait_patch:
            wait_patch.setattr(asyncio, "sleep", forbidden_sleep)
            result = await service.wait_until_published(scopes[0], deadline=time.monotonic() + 2)
        assert result["status"] == "current" and len(calls) == 2


@pytest.mark.asyncio
async def test_broadcast_scope_isolation_and_waiter_cancellation(tmp_path):
    async with isolated(tmp_path) as (service, runtime, scopes):
        first, other = map(partition_for, scopes)
        revision = runtime.publication_revision(first)
        deadline = time.monotonic() + 5
        waits = [asyncio.create_task(runtime.wait_publication_change(
            first, revision, deadline=deadline)) for _ in range(3)]
        unrelated = asyncio.create_task(runtime.wait_publication_change(
            other, runtime.publication_revision(other), deadline=deadline))
        try:
            await asyncio.sleep(0)
            waits[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await waits[0]
            runtime.signal_publication_change(first, "synthetic")
            await asyncio.wait_for(asyncio.gather(*waits[1:]), .5)
            assert not unrelated.done()
            assert runtime.publication_revision(first) == revision + 1
            assert runtime.publication_revision(other) == 0
            assert runtime._semantic_dirty == set()
            assert runtime._semantic_attempts == {}
            assert runtime._semantic_failed == set()
        finally:
            unrelated.cancel()
            await asyncio.gather(*waits, unrelated, return_exceptions=True)


@pytest.mark.asyncio
async def test_unconfigured_scope_cannot_allocate_notifications(tmp_path):
    async with isolated(tmp_path) as (service, runtime, scopes):
        unknown = replace(partition_for(scopes[0]), conversation_id="unknown")
        before = len(runtime._publication_changes)
        with pytest.raises(ValueError, match="unknown semantic scope"):
            runtime.signal_publication_change(unknown, "synthetic")
        with pytest.raises(ValueError, match="unknown semantic scope"):
            runtime.publication_revision(unknown)
        assert len(runtime._publication_changes) == before == len(scopes)


@pytest.mark.asyncio
@pytest.mark.parametrize("duration", [-1, .03])
async def test_short_and_expired_deadline_preserve_original_limit(tmp_path, monkeypatch, duration):
    async with isolated(tmp_path) as (service, runtime, scopes):
        calls = []

        async def pending(scope, *, deadline):
            calls.append(scope)
            return pending_state()

        monkeypatch.setattr(service, "status", pending)
        start = time.monotonic()
        result = await asyncio.wait_for(service.wait_until_published(
            scopes[0], deadline=start + duration), .5)
        assert result["reason"] == "publication_deadline"
        assert len(calls) == (1 if duration < 0 else 2)


@pytest.mark.asyncio
async def test_silent_external_change_is_found_by_one_second_fallback(tmp_path, monkeypatch):
    async with isolated(tmp_path) as (service, runtime, scopes):
        ready, queried = asyncio.Event(), asyncio.Event()
        calls = []

        async def status(scope, *, deadline):
            calls.append(time.monotonic())
            queried.set()
            return {"status": "current", "terminal": False} if ready.is_set() else pending_state()

        monkeypatch.setattr(service, "status", status)
        task = asyncio.create_task(service.wait_until_published(
            scopes[0], deadline=time.monotonic() + 4))
        try:
            await queried.wait()
            ready.set()
            result = await asyncio.wait_for(task, 1.5)
            assert result["status"] == "current" and len(calls) == 2
            assert calls[1] - calls[0] <= 1.4
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_signal_without_commit_cannot_authorize_current(tmp_path, monkeypatch):
    async with isolated(tmp_path) as (service, runtime, scopes):
        partition = partition_for(scopes[0])
        entered = asyncio.Event()
        calls = []

        async def pending(scope, *, deadline):
            calls.append(scope)
            entered.set()
            return pending_state()

        monkeypatch.setattr(service, "status", pending)
        task = asyncio.create_task(service.wait_until_published(
            scopes[0], deadline=time.monotonic() + .15))
        await entered.wait()
        runtime.signal_publication_change(partition, "commit_unknown")
        assert (await task)["reason"] == "publication_deadline"
        assert len(calls) == 3


class FailingBus:
    async def derive_raw_message(self, *args):
        raise ValueError("synthetic processor failure")


@pytest.mark.asyncio
@pytest.mark.parametrize("over_attempts", [False, True])
async def test_worker_notifies_only_after_real_failure_commit(tmp_path, over_attempts):
    store, scope = source_with_task(tmp_path)
    observations, diagnosed, successes = [], [], []
    worker = RawMessageWorker(store, FailingBus(), (scope,), max_attempts=1,
        on_failure=lambda: diagnosed.append(store.partition_publication_state(scope)[2]),
        on_settled=lambda *args: successes.append(args))
    worker.on_state_change = lambda *args: observations.append(
        (args, store.partition_publication_state(scope)[2]))
    task = store.claim_partition(scope, worker.worker_id, lease_seconds=60, max_active=2)
    assert task is not None
    if over_attempts:
        task = {**task, "attempts": 2}
    try:
        assert await worker._execute(scope, task) == 0
        assert observations == [((partition_for(scope), task["task_id"]), True)]
        assert diagnosed == ([] if over_attempts else [False])
        assert successes == []
        assert store.list_derivations(scope, "msg-1")[0]["status"] == "failed"
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_failed_failure_commit_never_sends_committed_notification(tmp_path, monkeypatch):
    store, scope = source_with_task(tmp_path)
    notifications = []
    worker = RawMessageWorker(store, FailingBus(), (scope,), max_attempts=1)
    worker.on_state_change = lambda *args: notifications.append(args)

    def cannot_commit(*args, **kwargs):
        raise OSError("synthetic commit failure")

    monkeypatch.setattr(store, "fail", cannot_commit)
    with pytest.raises(OSError, match="synthetic commit failure"):
        await worker.run_once()
    assert notifications == []
    assert not store.partition_publication_state(scope)[2]
    with pytest.raises(RuntimeError, match="drain failed"):
        await worker.close()


@pytest.mark.asyncio
async def test_notification_callback_error_cannot_reclassify_committed_failure(tmp_path):
    store, scope = source_with_task(tmp_path)
    worker = RawMessageWorker(store, FailingBus(), (scope,), max_attempts=1)

    def broken_observer(*args):
        raise ValueError("synthetic observer failure")

    worker.on_state_change = broken_observer
    try:
        assert await worker.run_once() == 0
        assert store.partition_publication_state(scope)[2]
        assert worker.notification_error == "state_notification_failed"
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("obstacle", ["generation", "resolved", "failed", "pid", "ready", "dirty", "lifecycle"])
async def test_notification_keeps_all_fresh_publication_guards(tmp_path, monkeypatch, obstacle):
    async with isolated(tmp_path) as (service, runtime, scopes):
        partition = partition_for(scopes[0])
        runtime._state = "ready"
        runtime.semantic_mode = "snapshot"
        reader = SimpleNamespace(worker_pid=41, _ready={partition: "generation"},
                                 _lifecycle=asyncio.Lock(), available=lambda p: True)
        runtime.snapshot_reader = reader
        runtime.snapshot_cache = SimpleNamespace(_current={partition: "generation"}, scopes=(partition,))
        runtime._semantic_publications[partition] = (0, "generation", 41)
        original = runtime.raw.store.partition_publication_state
        calls = []

        def state(scope):
            calls.append(scope)
            generation, resolved, failed = original(scope)
            return (generation + (obstacle == "generation"),
                    resolved and obstacle != "resolved", failed or obstacle == "failed")

        monkeypatch.setattr(runtime.raw.store, "partition_publication_state", state)
        if obstacle == "pid":
            reader.worker_pid = 42
        elif obstacle == "ready":
            reader._ready.clear()
        elif obstacle == "dirty":
            runtime._semantic_dirty.add(partition)
        elif obstacle == "lifecycle":
            await reader._lifecycle.acquire()
        runtime.signal_publication_change(partition, "synthetic")
        result = await service.status(scopes[0], deadline=time.monotonic() + 2)
        assert result["status"] != "current" and calls == [scopes[0]]
        if obstacle == "failed":
            assert result["terminal"]


@pytest.mark.asyncio
async def test_stop_broadcasts_and_releases_notifications(tmp_path, monkeypatch):
    async with isolated(tmp_path) as (service, runtime, scopes):
        queried = asyncio.Event()

        async def status(scope, *, deadline):
            queried.set()
            return ({"status": "stopped", "terminal": False}
                    if runtime._state in {"stopping", "stopped"} else pending_state())

        monkeypatch.setattr(service, "status", status)
        task = asyncio.create_task(service.wait_until_published(
            scopes[0], deadline=time.monotonic() + 5))
        await queried.wait()
        await runtime.close()
        assert (await asyncio.wait_for(task, .5))["status"] == "stopped"
        assert runtime._publication_changes == {}
        assert (await service.wait_until_published(
            scopes[0], deadline=time.monotonic() + 5))["status"] == "stopped"


@pytest.mark.asyncio
async def test_real_publisher_and_worker_bindings_notify_new_add(tmp_path, monkeypatch):
    async with daily(tmp_path) as (raw, bus, runtime):
        scope = raw.configured_scopes()[0]
        partition = partition_for(scope)
        await publication_until(runtime, partition, lambda state: state["status"] == "current")
        assert runtime.raw.worker.on_state_change == runtime.signal_publication_change
        service = MemoryService(runtime, profile_id="memory-full-v1")
        before = runtime.publication_revision(partition)
        scope, message_id = await ingress(raw, bus, "synthetic notified publication")
        result = await service.wait_until_published(scope, deadline=time.monotonic() + 20)
        assert result["status"] == "current"
        assert runtime.publication_revision(partition) > before
        assert all(row["status"] == "succeeded" for row in raw.store.list_derivations(scope, message_id))


@pytest.mark.asyncio
async def test_terminal_early_return_and_publisher_exit_broadcast(tmp_path):
    async with isolated(tmp_path) as (service, runtime, scopes):
        partition = partition_for(scopes[0])
        runtime.snapshot_cache = SimpleNamespace(scopes=(partition,))
        runtime._semantic_attempts[partition] = 3
        before = runtime.publication_revision(partition)
        runtime._notify_semantic(partition, "attempts_exhausted")
        assert runtime.publication_revision(partition) > before
        assert not runtime._semantic_dirty

        async def fail():
            raise RuntimeError("synthetic publisher failure")

        task = asyncio.create_task(fail())
        await asyncio.gather(task, return_exceptions=True)
        before = runtime.publication_revision(partition)
        runtime._semantic_finished(task)
        assert runtime.publication_revision(partition) > before
        assert partition in runtime._semantic_failed
