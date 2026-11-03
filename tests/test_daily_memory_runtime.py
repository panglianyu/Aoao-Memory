"""Explicit daily runtime owns the actual captured Raw store and all processes."""

import asyncio
import os
from dataclasses import replace
from datetime import datetime, timezone
from contextlib import contextmanager

import pytest

from kernel.app_bus import AppBus
from kernel.message import Message
from kernel.raw_message_models import DerivationSpec, Scope
from kernel.raw_message_service import RawMessageService
from tests.test_raw_message_service import profile


@contextmanager
def snapshot_write_fault(monkeypatch, *, failures=3, fatal=False, partition=None):
    """Fault the real cache's file open; source/worker/publication still execute."""
    from pathlib import Path

    original = Path.open
    observed = []

    def open_file(path, *args, **kwargs):
        if (path.name == "vectors.f32" and path.parent.name.startswith("staging-")
                and any(part.startswith("aoao-daily-semantic-") for part in path.parts)
                and (partition is None or path.parent.parent.name == partition)
                and len(observed) < failures):
            observed.append(path)
            if fatal:
                raise MemoryError("injected cache open failure")
            raise OSError("injected cache open failure")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(Path, "open", open_file)
        yield observed


async def publication_until(runtime, partition, accepts, *, timeout=20):
    import time

    deadline = time.monotonic() + timeout
    while True:
        state = await runtime.semantic_publication_state(partition, deadline=deadline)
        if accepts(state):
            return state
        assert time.monotonic() < deadline, state
        await asyncio.sleep(.01)


@pytest.mark.asyncio
async def test_first_publication_failure_is_recoverable_not_terminal(tmp_path, monkeypatch):
    from kernel.memory_pipeline_models import PipelineScope
    from tests.test_daily_memory_early_acceptance import daily

    with snapshot_write_fault(monkeypatch, failures=1) as calls:
        async with daily(tmp_path) as (raw, bus, runtime):
            scope = raw.configured_scopes()[0]
            partition = PipelineScope(scope.user_id, scope.agent_id,
                                      scope.connector_id, scope.conversation_id)

            def current(state):
                assert state["terminal"] is False
                return state["status"] == "current"

            assert (await publication_until(runtime, partition, current))["reason"] == "none"
            assert len(calls) == 1


@pytest.mark.asyncio
async def test_unexpected_publisher_cancellation_is_terminal_only_while_running(tmp_path):
    import time
    from kernel.memory_pipeline_models import PipelineScope
    from tests.test_daily_memory_early_acceptance import daily

    async with daily(tmp_path) as (raw, bus, runtime):
        scope = raw.configured_scopes()[0]
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        await publication_until(runtime, partition, lambda state: state["status"] == "current")
        publisher = next(task for task in asyncio.all_tasks()
                         if getattr(getattr(task.get_coro(), "cr_code", None), "co_name", None)
                         == "_publish_semantic")
        publisher.cancel()
        await asyncio.gather(publisher, return_exceptions=True)
        state = await runtime.semantic_publication_state(partition, deadline=time.monotonic() + 1)
        assert state["terminal"] is True and state["reason"] == "semantic_publisher_failed"
    stopped = await runtime.semantic_publication_state(partition, deadline=time.monotonic() + 1)
    assert stopped["status"] == "stopped" and stopped["terminal"] is False


@pytest.mark.asyncio
async def test_exhausted_publication_in_one_partition_does_not_terminalize_another(tmp_path, monkeypatch):
    from kernel.memory_pipeline_models import PipelineScope
    from kernel.raw_message_profile import ConversationBinding
    from tests.test_daily_memory_early_acceptance import daily

    settings = profile(tmp_path / "raw")
    connector = replace(settings.connectors[0], conversations=(
        *settings.connectors[0].conversations,
        ConversationBinding("second_console", "second-chat", "web", "Second")))
    settings = replace(settings, connectors=(connector,), derived_root=tmp_path / "derived")
    async with daily(tmp_path, settings=settings) as (raw, bus, runtime):
        partitions = tuple(PipelineScope(scope.user_id, scope.agent_id,
                           scope.connector_id, scope.conversation_id) for scope in raw.configured_scopes())
        # Recovery is still queued when the context returns; fail only the
        # first partition's real file opens, never the other's publication.
        with snapshot_write_fault(monkeypatch, partition=partitions[0].partition_id) as calls:
            failed = await publication_until(runtime, partitions[0], lambda state: state["terminal"])
            assert failed["reason"] == "publication_attempts_exhausted"
            other = await publication_until(runtime, partitions[1], lambda state: state["status"] == "current")
            assert other["terminal"] is False and len(calls) == 3


@pytest.mark.asyncio
async def test_capacity_publication_terminal_is_real_and_preserves_originals(tmp_path):
    from kernel.memory_pipeline_models import PipelineScope
    from tests.test_daily_memory_early_acceptance import daily, ingress

    async with daily(tmp_path) as (raw, bus, runtime):
        scope = raw.configured_scopes()[0]
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        await publication_until(runtime, partition, lambda state: state["status"] == "current")
        lease = runtime.snapshot_cache.acquire(partition)
        try:
            scope, first = await ingress(raw, bus, "capacity-original-first")
            await publication_until(runtime, partition, lambda state: state["status"] == "current")
            scope, second = await ingress(raw, bus, "capacity-original-second")
            terminal = await publication_until(runtime, partition, lambda state: state["terminal"])
            assert terminal["reason"] == "publication_attempts_exhausted"
            assert raw.store.get(scope, first).content == "capacity-original-first"
            assert raw.store.get(scope, second).content == "capacity-original-second"
            assert all(task["status"] == "succeeded" for task in raw.store.list_derivations(scope, second))
        finally:
            runtime.snapshot_cache.release(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("repeat", range(3))
async def test_committed_memory_waits_for_its_snapshot_publication(tmp_path, monkeypatch, repeat):
    """A prepared old reader is not proof that newly committed sources are searchable."""
    import time
    from datetime import timedelta
    from kernel import raw_message_outbox as outbox
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.derivation import daily_specs
    from tests.test_daily_memory_early_acceptance import daily, ingress

    specs = tuple(sorted(daily_specs("synthetic-acceptance"),
                         key=lambda spec: spec.target_domain == "vector"))
    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                       derivations=specs)
    tick = datetime.now(timezone.utc)

    def insertion_time():
        nonlocal tick
        tick += timedelta(microseconds=1)
        return tick.isoformat()

    release_vector, committed, release_notice = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async with daily(tmp_path, settings=settings) as (raw, bus, runtime):
        original = bus.derive_raw_message

        async def controlled(partition, task, message, guard, complete):
            if task["target_domain"] != "vector":
                return await original(partition, task, message, guard, complete)
            await release_vector.wait()
            result = await original(partition, task, message, guard, complete)
            committed.set()
            await release_notice.wait()
            return result

        monkeypatch.setattr(bus, "derive_raw_message", controlled)
        deadline = time.monotonic() + 20
        try:
            with monkeypatch.context() as patch:
                patch.setattr(outbox, "_now", insertion_time)
                scope, message_id = await ingress(raw, bus, "synthetic-publication-fence")
            partition = PipelineScope(scope.user_id, scope.agent_id,
                                      scope.connector_id, scope.conversation_id)
            while True:
                tasks = raw.store.list_derivations(scope, message_id)
                if (sum(task["status"] == "succeeded" for task in tasks) == 5
                        and runtime.health()["semantic_state"] == "ready"):
                    break
                assert time.monotonic() < deadline, runtime.health()
                await asyncio.sleep(.01)
            release_vector.set()
            await asyncio.wait_for(committed.wait(), max(.001, deadline - time.monotonic()))
            expected = {row["memory_id"] for row in raw.store.results(scope, message_id)
                        if row["target_domain"] == "vector"}
            assert len(expected) == 1
            assert all(task["status"] == "succeeded"
                       for task in raw.store.list_derivations(scope, message_id))
            assert runtime.health()["semantic_state"] == "ready"
            assert await runtime.snapshot_reader.search_cosine_hits(
                partition, "acceptance-semantic-key") == ()
            before = await runtime.semantic_publication_state(partition, deadline=deadline)
            assert before["status"] == "preparing"
            assert before["raw_generation"] > before["published_raw_generation"]
            release_notice.set()
            while True:
                current = await runtime.semantic_publication_state(partition, deadline=deadline)
                if current["status"] == "current":
                    break
                assert time.monotonic() < deadline, current
                await asyncio.sleep(.01)
            assert current["raw_generation"] == current["published_raw_generation"]
            hits = await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key")
            assert {key for key, _ in hits} == expected
        finally:
            release_vector.set()
            release_notice.set()


@pytest.mark.asyncio
async def test_empty_capture_also_checks_source_generation(tmp_path, monkeypatch):
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from system_memory_god.derivation import _RecoveryGenerationChanged, daily_specs

    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                       derivations=daily_specs("synthetic-acceptance"))
    raw = RawMessageService(settings)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, settings, bus, extractor=object(),
                                embedding="synthetic", semantic_mode="snapshot")
    try:
        await raw.initialize_profile()
        saved = await raw.capture(Message.user_input("synthetic-before", "web",
                                  chat_name="web_console"), "test-agent")
        scope = raw.configured_scopes()[0]
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        original = runtime.vector.snapshot_vectors

        def capture_then_edit(partition):
            captured = original(partition)
            assert captured == {}
            raw.store.edit(scope, saved.get("origin_message_id"), "synthetic-after",
                           expected_revision=1, edited_at=datetime.now(timezone.utc).isoformat())
            return captured

        monkeypatch.setattr(runtime.vector, "snapshot_vectors", capture_then_edit)
        with pytest.raises(_RecoveryGenerationChanged):
            runtime._verified_snapshot_rows(partition)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_edit_during_snapshot_warm_cannot_sign_current_publication(tmp_path, monkeypatch):
    import time
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.storage.daily_vector_snapshot import load_snapshot
    from tests.test_daily_memory_early_acceptance import daily, ingress

    entered, release = asyncio.Event(), asyncio.Event()
    async with daily(tmp_path) as (raw, bus, runtime):
        original = runtime.snapshot_reader.warm_existing
        held = False

        async def warm(partition):
            nonlocal held
            await original(partition)
            generation = runtime.snapshot_reader._ready[partition]
            if not held and load_snapshot(runtime.snapshot_cache.root, partition,
                                         generation, embedding="synthetic").ids:
                held = True
                entered.set()
                await release.wait()

        monkeypatch.setattr(runtime.snapshot_reader, "warm_existing", warm)
        deadline = time.monotonic() + 20
        try:
            scope, message_id = await ingress(raw, bus, "synthetic-edit-during-warm")
            partition = PipelineScope(scope.user_id, scope.agent_id,
                                      scope.connector_id, scope.conversation_id)
            await asyncio.wait_for(entered.wait(), max(.001, deadline - time.monotonic()))
            raw.store.edit(scope, message_id, "synthetic-new-revision", expected_revision=1,
                           edited_at=datetime.now(timezone.utc).isoformat())
            assert (await runtime.semantic_publication_state(partition, deadline=deadline))["status"] != "current"
            release.set()
            while True:
                state = await runtime.semantic_publication_state(partition, deadline=deadline)
                if state["status"] == "current":
                    break
                assert time.monotonic() < deadline, state
                await asyncio.sleep(.01)
            expected = {row["memory_id"] for row in raw.store.results(scope, message_id)
                        if row["revision"] == 2 and row["target_domain"] == "vector"}
            hits = await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key")
            assert {key for key, _ in hits} == expected and expected
        finally:
            release.set()


@pytest.mark.asyncio
async def test_current_empty_publication_is_legal_and_recovery_resigns_it(tmp_path):
    import time
    from kernel.memory_pipeline_models import PipelineScope
    from tests.test_daily_memory_early_acceptance import daily, ingress

    class EmptyExtractor:
        async def extract(self, *args):
            return ()

    async with daily(tmp_path, extractor=EmptyExtractor()) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "synthetic-no-derived-vectors")
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        deadline = time.monotonic() + 20

        async def current():
            while True:
                state = await runtime.semantic_publication_state(partition, deadline=deadline)
                if state["status"] == "current":
                    return state
                assert time.monotonic() < deadline, state
                await asyncio.sleep(.01)

        before = await current()
        assert raw.store.results(scope, message_id) == []
        assert await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key") == ()
        old_pid = runtime.snapshot_reader.worker_pid
        await runtime.snapshot_reader.retire_worker()
        assert (await runtime.semantic_publication_state(partition, deadline=deadline))["status"] == "unavailable"
        await runtime.snapshot_reader.warm_existing(partition)
        assert runtime.snapshot_reader.worker_pid != old_pid
        assert (await runtime.semantic_publication_state(partition, deadline=deadline))["status"] == "preparing"
        assert (await current())["raw_generation"] == before["raw_generation"]
        for field in ("user_id", "agent_id", "connector_id", "conversation_id"):
            wrong = replace(partition, **{field: "unconfigured-test-scope"})
            with pytest.raises(ValueError, match="unknown semantic scope"):
                await runtime.semantic_publication_state(wrong, deadline=deadline)
    assert (await runtime.semantic_publication_state(partition, deadline=deadline))["status"] == "stopped"
    assert not runtime._semantic_publications


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_publication_metadata_remains_owned_after_deadline_or_cancel(tmp_path, monkeypatch, cancel):
    import threading
    import time
    from kernel.memory_pipeline_models import PipelineScope
    from tests.test_daily_memory_early_acceptance import daily

    async with daily(tmp_path) as (raw, bus, runtime):
        scope = raw.configured_scopes()[0]
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        deadline = time.monotonic() + 20
        while (await runtime.semantic_publication_state(partition, deadline=deadline))["status"] != "current":
            assert time.monotonic() < deadline
            await asyncio.sleep(.01)
        original = raw.store.partition_publication_state
        entered, release = threading.Event(), threading.Event()

        def blocked(scope):
            entered.set()
            release.wait()
            return original(scope)

        monkeypatch.setattr(raw.store, "partition_publication_state", blocked)
        request = asyncio.create_task(runtime.semantic_publication_state(
            partition, deadline=time.monotonic() + (1 if cancel else .05)))
        try:
            while not entered.is_set():
                assert time.monotonic() < deadline
                await asyncio.sleep(.001)
            if cancel:
                request.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await request
            else:
                state = await request
                assert state["status"] == "unavailable" and state["terminal"] is False
            assert len(runtime._semantic_metadata) == 1
        finally:
            release.set()
            await asyncio.gather(request, return_exceptions=True)
        await runtime.close()
        assert not runtime._semantic_metadata


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["none", "unverified", "registry", "publication"])
async def test_startup_diagnostic_preserves_actual_phase_without_private_source(tmp_path, monkeypatch, fault):
    import json
    import sqlite3
    import time
    from contextlib import nullcontext
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from Tools import probe_daily_memory_recall_diag as diag

    config = replace(profile(tmp_path / "private-chat-sentinel"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    scope = raw.configured_scopes()[0]
    partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=object(), embedding="synthetic",
                                semantic_mode="snapshot", semantic_diagnostics=True)
    bus.attach_daily_runtime(runtime)
    if fault == "unverified":
        directory = runtime.stores.partition(partition)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "private-chat-sentinel").touch()
    if fault == "registry":
        config.derived_root.mkdir(exist_ok=True)
        with sqlite3.connect(config.derived_root / "daily_partition_registry.db") as db:
            db.execute("CREATE TABLE private_chat_sentinel(value TEXT)")
    observed = None
    try:
        with snapshot_write_fault(monkeypatch) if fault == "publication" else nullcontext():
            await runtime.start()
            assert await diag.wait_ready(runtime, time.monotonic() + 20, initial=True) is (fault == "none")
            observed = runtime.startup_diagnostic()
            entry = observed["scopes"][0]
            assert entry["slot"] == 0
            assert entry["stage"] == ({"none": "ready", "unverified": "partition_recovery",
                "registry": "partition_recovery", "publication": "snapshot_publication"}[fault])
            assert entry["error_code"] == ({"none": "none", "unverified": "recovery_unverified",
                "registry": "recovery_failed", "publication": "publication_failed"}[fault])
            assert entry["error_type"] == ({"none": "none", "unverified": "none",
                "registry": "ValueError", "publication": "OSError"}[fault])
            assert "private-chat-sentinel" not in json.dumps(observed)
            entry["stage"] = "modified"
            assert runtime.startup_diagnostic()["scopes"][0]["stage"] != "modified"
    finally:
        await runtime.close()
    assert runtime.startup_diagnostic()["scopes"][0]["error_code"] == observed["scopes"][0]["error_code"]


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_snapshot_diagnostics_are_explicit_opt_in(tmp_path, enabled):
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=object(), embedding="synthetic",
                                semantic_mode="snapshot", semantic_diagnostics=enabled)
    try:
        assert runtime.snapshot_reader.diagnostics is enabled
        assert runtime.snapshot_reader.diagnostic() == {}
        assert runtime.startup_diagnostic() == ({
            "stage": "created", "error_code": "none", "error_type": "none", "scopes": [],
            "writer": {"stage": "not_started", "error_code": "none", "error_type": "none", "elapsed_ms": 0.0},
            "reader": {"stage": "not_started", "error_code": "none", "error_type": "none", "elapsed_ms": 0.0},
        } if enabled else {})
        assert runtime.retrieval.request_timeout == .3
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("value, mode", [(True, "disabled"), (1, "snapshot"),
                                       (None, "snapshot"), ("true", "snapshot")])
async def test_invalid_diagnostics_fail_before_runtime_resources(tmp_path, value, mode):
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    try:
        with pytest.raises(ValueError, match="semantic diagnostics"):
            DailyMemoryRuntime(raw, config, bus, extractor=object(), embedding="synthetic",
                               semantic_mode=mode, semantic_diagnostics=value)
        assert not config.derived_root.exists()
    finally:
        await raw.close()

@pytest.mark.asyncio
async def test_real_model_isolated_capture_derivation_recall_and_restart(tmp_path):
    if os.environ.get("AOAO_DAILY_REAL_API_TEST") != "1":
        pytest.skip("explicit bounded model probe only")

    import time
    from openai import AsyncOpenAI
    from kernel.config_loader import load_api_config
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from system_memory_god.derivation import StructuredDailyExtractor, daily_specs

    api = load_api_config()
    if not api.get("api_key") or not api.get("base_url") or not api.get("model_name"):
        pytest.skip("no explicitly configured model endpoint")

    class BoundedExtractor(StructuredDailyExtractor):
        calls = 0

        async def extract(self, domain, message, source, max_output_tokens):
            if self.calls >= 20:
                raise RuntimeError("isolated extraction call budget exhausted")
            self.calls += 1
            return await super().extract(domain, message, source,
                                         min(max_output_tokens, 512))

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=daily_specs(api["model_name"]))
    client = AsyncOpenAI(base_url=api["base_url"], api_key=api["api_key"],
                         timeout=15, max_retries=0)
    extractor = BoundedExtractor(client, api["model_name"])
    message_id = None
    try:
        for iteration in range(2):
            raw = RawMessageService(config)
            bus = AppBus(str(tmp_path / f"apps-{iteration}"))
            bus.attach_raw_messages(raw, lambda: "test-agent")
            runtime = DailyMemoryRuntime(raw, config, bus, extractor=extractor,
                                         embedding="bge-m3")
            bus.attach_daily_runtime(runtime)
            try:
                await runtime.start()
                if iteration == 0:
                    saved = await bus.accept_input(Message.user_input(
                        "【合成测试数据】虚构角色甲最喜欢红茶。", "web",
                        chat_name="web_console"))
                    message_id = saved.get("origin_message_id")
                scope = raw.configured_scopes()[0]
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    tasks = raw.store.list_derivations(scope, message_id)
                    if len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks):
                        break
                    if extractor.calls >= 20:
                        break
                    await asyncio.sleep(0.25)
                assert len(tasks) == 6, (len(tasks), extractor.calls)
                assert all(task["status"] == "succeeded" for task in tasks), (
                    [task["status"] for task in tasks], extractor.calls)
                assert raw.store.has_derived_results(scope)
                result = await runtime.retrieval.search_with_status(
                    scope, "红茶", memory_scope="portrait")
                assert message_id in {item["source_message_id"] for item in result.items}
            finally:
                await runtime.close()
        assert 1 <= extractor.calls <= 20
    finally:
        await client.close()


def test_runtime_health_reports_reader_recovery_attempts(tmp_path):
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=object(), embedding="synthetic")
    assert runtime.health()["reader_recovery_attempts"] == 0


@pytest.mark.asyncio
async def test_raw_reader_is_ready_while_existing_vector_owner_warms(
    tmp_path, monkeypatch
):
    import asyncio
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=object(),
                                 embedding="synthetic")
    bus.attach_daily_runtime(runtime)
    await raw.initialize_profile()
    saved = await raw.capture(Message.user_input(
        "合成红茶原文", "web", chat_name="web_console"), "test-agent")
    scope = raw.configured_scopes()[0]
    warming = asyncio.Event()
    release = asyncio.Event()

    class ExistingPartition:
        def snapshot(self, partition):
            return {"vector": True}

    async def recovered(*args, **kwargs):
        return True

    async def slow_warm(*args, **kwargs):
        warming.set()
        await release.wait()

    monkeypatch.setattr(runtime.stores, "partition_registry",
                        lambda: ExistingPartition())
    monkeypatch.setattr(runtime.derivation, "recover_partition", recovered)
    monkeypatch.setattr(runtime.vector, "warm_existing", slow_warm)
    startup = asyncio.create_task(runtime.start())
    try:
        await asyncio.wait_for(warming.wait(), 5)
        assert startup.done(), "Raw-ready startup must not wait for vector warm"
        assert raw.worker is not None
        assert scope in raw.worker.blocked_scopes
        assert runtime.retrieval.worker_pid is not None
        result = await runtime.retrieval.search_with_status(scope, "红茶",
                                                             memory_scope="raw")
        assert saved.get("origin_message_id") in {
            item["source_message_id"] for item in result.items}
    finally:
        await asyncio.wait_for(startup, 5)
        await runtime.close()
        assert not runtime._recovery_tasks
        assert runtime.health()["derived_state"] == "stopped"
        assert scope in raw.worker.blocked_scopes


@pytest.mark.asyncio
@pytest.mark.parametrize("embedding", ["bge-m3", "synthetic"])
async def test_runtime_keeps_raw_online_without_borrowed_vector_read(tmp_path, embedding):
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            return ()

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding=embedding)
    bus.attach_daily_runtime(runtime)
    try:
        await runtime.start()
        assert runtime.health()["online_vector"] == "disabled_pending_isolation"
        saved = await bus.accept_input(Message.user_input(
            "隔离对话原文红茶", "web", chat_name="web_console"))
        scope = raw.configured_scopes()[0]
        found = await runtime.retrieval.search_with_status(scope, "红茶")
        assert saved.get("origin_message_id") in {
            item["source_message_id"] for item in found.items}, (
                found.status, found.reason_codes, runtime.health())
        assert runtime.health()["reason_codes"].count("vector_online_disabled") == 1
        assert runtime.retrieval.vector_index is None
    finally:
        await runtime.close()


def test_runtime_rejects_more_conversations_than_vector_capacity(tmp_path):
    from kernel.raw_message_profile import ConversationBinding
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    original = profile(tmp_path / "raw")
    conversations = tuple(
        ConversationBinding(f"web_{number}", f"remote_{number}", "web",
                            f"聊天{number}") for number in range(9))
    connector = replace(original.connectors[0], conversations=conversations)
    config = replace(original, connectors=(connector,), derived_root=tmp_path / "derived")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    with pytest.raises(ValueError, match="vector scope capacity"):
        DailyMemoryRuntime(raw, config, bus, extractor=object(), embedding="synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("produce_result", [True, False])
@pytest.mark.parametrize("edit_history", [False, True])
async def test_missing_derived_partition_with_raw_results_pauses_pending_work(
    tmp_path, produce_result, edit_history
):
    from kernel.raw_message_profile import ConversationBinding
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            if produce_result:
                return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)
            return ()

    original = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "original_derived",
                       derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    first_raw = RawMessageService(original)
    first_bus = AppBus(str(tmp_path / "apps_first"))
    first_bus.attach_raw_messages(first_raw, lambda: "test-agent")
    first_runtime = DailyMemoryRuntime(first_raw, original, first_bus,
                                       extractor=Extractor(), embedding="synthetic")
    first_bus.attach_daily_runtime(first_runtime)
    try:
        await first_runtime.start()
        first = await first_bus.accept_input(Message.user_input(
            "旧原文红茶", "web", chat_name="web_console"))
        scope = first_raw.configured_scopes()[0]
        first_id = first.get("origin_message_id")
        deadline = asyncio.get_running_loop().time() + 8
        while first_raw.store.list_derivations(scope, first_id)[0]["status"] != "succeeded":
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        assert first_raw.store.has_derived_results(scope) is produce_result
    finally:
        await first_runtime.close()

    connector = replace(original.connectors[0], conversations=(
        *original.connectors[0].conversations,
        ConversationBinding("web_second", "local-chat-2", "web", "第二对话"),
    ))
    missing = replace(original, derived_root=tmp_path / "missing_derived",
                      connectors=(connector,))
    pending_raw = RawMessageService(missing)
    await pending_raw.initialize_profile()
    if edit_history:
        pending_raw.store.edit(
            scope, first_id, "修订原文红茶", expected_revision=1,
            edited_at=datetime.now(timezone.utc).isoformat(),
        )
    pending = await pending_raw.capture(Message.user_input(
        "待派生原文绿茶", "web", chat_name="web_console"), "test-agent")
    pending_id = pending.get("origin_message_id")
    assert pending_raw.store.list_derivations(scope, pending_id)[0]["status"] == "pending"
    await pending_raw.close()

    restarted_raw = RawMessageService(missing)
    restarted_bus = AppBus(str(tmp_path / "apps_restarted"))
    restarted_bus.attach_raw_messages(restarted_raw, lambda: "test-agent")
    restarted_runtime = DailyMemoryRuntime(restarted_raw, missing, restarted_bus,
                                           extractor=Extractor(), embedding="synthetic")
    restarted_bus.attach_daily_runtime(restarted_runtime)
    try:
        await restarted_runtime.start()
        healthy_scope = restarted_raw.configured_scopes()[1]
        healthy = await restarted_bus.accept_input(Message.user_input(
            "健康分区原文绿茶", "web", chat_name="web_second"))
        healthy_id = healthy.get("origin_message_id")
        deadline = asyncio.get_running_loop().time() + 8
        while restarted_raw.store.list_derivations(healthy_scope, healthy_id)[0]["status"] != "succeeded":
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        assert restarted_runtime.health()["state"] == "degraded"
        assert restarted_raw.store.get(scope, first_id).content == (
            "修订原文红茶" if edit_history else "旧原文红茶")
        assert restarted_raw.store.has_derived_results(scope) is produce_result
        assert restarted_raw.store.list_derivations(scope, pending_id)[0]["status"] == "pending"
        assert await restarted_raw.worker.run_once() == 0
        result = await restarted_runtime.retrieval.search_with_status(scope, "绿茶")
        assert result.status == ("degraded" if produce_result else "ready")
        assert pending_id in {item["source_message_id"] for item in result.items}
    finally:
        await restarted_runtime.close()


@pytest.mark.asyncio
async def test_corrupt_derived_registry_allows_only_verified_raw_and_pending_work(tmp_path):
    import sqlite3
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            raise AssertionError("corrupt partition must not derive")

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    config.derived_root.mkdir()
    registry_path = config.derived_root / "daily_partition_registry.db"
    with sqlite3.connect(registry_path) as db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding="synthetic")
    bus.attach_daily_runtime(runtime)
    try:
        await runtime.start()
        saved = await bus.accept_input(Message.user_input("原文红茶", "web", chat_name="web_console"))
        scope = raw.configured_scopes()[0]
        message_id = saved.get("origin_message_id")
        assert raw.store.get(scope, message_id).content == "原文红茶"
        assert [task["status"] for task in raw.store.list_derivations(scope, message_id)] == ["pending"]
        result = await runtime.retrieval.search_with_status(scope, "红茶")
        assert result.status == "degraded"
        assert [item["source_message_id"] for item in result.items] == [message_id]
        assert await raw.worker.run_once() == 0
        assert runtime.health()["state"] == "degraded"
        with sqlite3.connect(registry_path) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 0
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_unusable_partition_pauses_only_its_derivation(tmp_path):
    from kernel.memory_pipeline_models import PipelineScope
    from kernel.raw_message_profile import ConversationBinding
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from system_memory_god.storage.scoped_stores import ScopedStores

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)

    base = profile(tmp_path / "raw")
    connector = replace(base.connectors[0], conversations=(
        *base.connectors[0].conversations,
        ConversationBinding("web_second", "local-chat-2", "web", "第二对话"),
    ))
    config = replace(base, connectors=(connector,), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    raw = RawMessageService(config)
    await raw.initialize_profile()
    bad, good = raw.configured_scopes()
    stores = ScopedStores(config.derived_root)
    bad_partition = PipelineScope(bad.user_id, bad.agent_id,
                                  bad.connector_id, bad.conversation_id)
    stores.partition(bad_partition).mkdir(parents=True)
    (stores.partition(bad_partition) / "foreign.db").write_bytes(b"foreign")
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding="synthetic")
    bus.attach_daily_runtime(runtime)
    try:
        await runtime.start()
        bad_saved = await bus.accept_input(Message.user_input(
            "坏分区原文红茶", "web", chat_name="web_console"))
        good_saved = await bus.accept_input(Message.user_input(
            "好分区原文红茶", "web", chat_name="web_second"))
        assert runtime.health()["state"] == "degraded"
        deadline = asyncio.get_running_loop().time() + 8
        while raw.store.list_derivations(good, good_saved.get("origin_message_id"))[0]["status"] != "succeeded":
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        assert raw.store.list_derivations(bad, bad_saved.get("origin_message_id"))[0]["status"] == "pending"
        found = await runtime.retrieval.search_with_status(good, "红茶", memory_scope="portrait")
        assert [item["source_message_id"] for item in found.items] == [
            good_saved.get("origin_message_id")]
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_runtime_explicitly_warms_existing_vector_owner(tmp_path):
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            return ({"text": "红茶语义事实"},)

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "vector", "fake", "a" * 64),))
    scope = None
    for iteration in range(2):
        raw = RawMessageService(config)
        bus = AppBus(str(tmp_path / f"apps_{iteration}"))
        bus.attach_raw_messages(raw, lambda: "test-agent")
        runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding="synthetic")
        bus.attach_daily_runtime(runtime)
        try:
            await runtime.start()
            if iteration == 0:
                saved = await bus.accept_input(Message.user_input(
                    "我想喝红茶", "web", chat_name="web_console"))
                scope = raw.configured_scopes()[0]
                message_id = saved.get("origin_message_id")
                deadline = asyncio.get_running_loop().time() + 8
                while raw.store.list_derivations(scope, message_id)[0]["status"] != "succeeded":
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.05)
            else:
                deadline = asyncio.get_running_loop().time() + 8
                while runtime.health()["derived_state"] == "recovering":
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.01)
                assert runtime.health()["derived_state"] == "ready"
                pid = runtime.vector.worker_pid
                assert pid is not None
                await runtime.vector.reader().warm_existing(PipelineScope(
                    scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id))
                assert runtime.vector.worker_pid == pid
                result = await runtime.retrieval.search_with_status(scope, "红茶", memory_scope="auto")
                assert message_id in {item["source_message_id"] for item in result.items}
        finally:
            await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("force_query_failure", [False, True])
async def test_runtime_capture_publish_query_restart(tmp_path, force_query_failure):
    import json
    import threading
    import time
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    message_id = None
    for iteration in range(2):
        started = time.monotonic()
        stages = []
        claimed = threading.Event()
        completed = threading.Event()

        def mark(name):
            stages.append((name, round((time.monotonic() - started) * 1000)))

        raw = RawMessageService(config)
        bus = AppBus(str(tmp_path / "apps"))
        bus.attach_raw_messages(raw, lambda: "test-agent")
        runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding="synthetic")
        bus.attach_daily_runtime(runtime)
        prepare = runtime.retrieval.prepare
        warm = runtime.vector.warm_existing
        claim = raw.store.claim_partition
        complete = raw.store.complete

        def trace_claim(*args, **kwargs):
            mark("claim_begin")
            try:
                task = claim(*args, **kwargs)
            finally:
                mark("claim_end")
            if task is not None:
                mark("task_leased")
                claimed.set()
            return task

        def trace_complete(*args, **kwargs):
            mark("complete_begin")
            try:
                result = complete(*args, **kwargs)
                mark("complete_committed")
                completed.set()
                return result
            finally:
                mark("complete_end")

        async def trace_prepare(scope):
            mark("prepare_begin")
            try:
                return await prepare(scope)
            finally:
                mark("prepare_end")

        async def trace_warm(scope):
            mark("vector_warm_begin")
            try:
                return await warm(scope)
            finally:
                mark("vector_warm_end")

        runtime.retrieval.prepare = trace_prepare
        runtime.vector.warm_existing = trace_warm
        raw.store.claim_partition = trace_claim
        raw.store.complete = trace_complete
        observations = []
        failure = None
        close_failure = None
        health_before_close = None
        try:
            await runtime.start()
            mark("runtime_started")
            if iteration == 0:
                saved = await bus.accept_input(Message.user_input("我想喝茶", "web", chat_name="web_console"))
                mark("raw_captured")
                message_id = saved.get("origin_message_id")
                scope = Scope("test-agent", "test-owner", "web-test", saved.get("conversation_id"))
            if force_query_failure:
                async def fail_query(*args, **kwargs):
                    raise RuntimeError("injected query failure")

                runtime.retrieval.search_with_status = fail_query
            deadline = asyncio.get_running_loop().time() + 8
            while True:
                query_started = time.monotonic()
                result = await runtime.retrieval.search_with_status(scope, "红茶", memory_scope="portrait")
                task_status = raw.store.list_derivations(scope, message_id)[0]["status"]
                health = runtime.health()
                current = (result.status, result.reason_codes, len(result.items),
                           task_status, health["reader_pid"], health["vector_pid"],
                           health["reader_recovery_attempts"],
                           health["vector_recovery_attempts"])
                observations.append((round((time.monotonic() - started) * 1000), current,
                                     round((time.monotonic() - query_started) * 1000)))
                if result.items:
                    mark("published_query")
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("publication deadline exceeded")
                if completed.is_set():
                    await asyncio.sleep(0.05)
                else:
                    await asyncio.to_thread(completed.wait, 0.05)
            assert result.items[0]["source_message_id"] == message_id
            assert runtime.retrieval.raw is bus.raw_messages.store
            while runtime.health()["derived_state"] == "recovering":
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
            assert runtime.health()["state"] == "ready"
        except Exception as exc:
            failure = exc
        finally:
            health_before_close = runtime.health()
            mark("close_begin")
            try:
                await runtime.close()
            except Exception as exc:
                close_failure = exc
            finally:
                mark("close_end")
        if failure is not None or close_failure is not None:
            health_after_close = runtime.health()
            report = tmp_path / f"on06-iteration-{iteration}.json"
            report.write_text(json.dumps({
                "iteration": iteration,
                "stages_ms": stages,
                "observations_ms": observations,
                "claimed": claimed.is_set(),
                "completed": completed.is_set(),
                "health_before_close": {key: health_before_close[key] for key in (
                    "state", "reason_codes", "reader_pid", "vector_pid",
                    "reader_recovery_attempts", "vector_recovering",
                    "vector_recovery_attempts")},
                "health_after_close": {key: health_after_close[key] for key in (
                    "state", "reason_codes", "reader_pid", "vector_pid",
                    "reader_recovery_attempts", "vector_recovering",
                    "vector_recovery_attempts")},
                "failure_type": type(failure).__name__ if failure else None,
                "close_failure_type": type(close_failure).__name__ if close_failure else None,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            if force_query_failure:
                saved_report = json.loads(report.read_text(encoding="utf-8"))
                assert saved_report["failure_type"] == "RuntimeError"
                assert saved_report["health_before_close"]["reader_pid"] is not None
                assert saved_report["health_after_close"]["reader_pid"] is None
                assert saved_report["health_before_close"]["reader_recovery_attempts"] == 0
                return
            pytest.fail(f"ON-06 iteration {iteration} failed; report: {report}", pytrace=False)
        assert runtime.health()["state"] == "stopped"
        assert runtime.retrieval.worker_pid is None and runtime.vector.worker_pid is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_channel", [False, True])
async def test_boot_factory_receives_actual_store_before_channels(tmp_path, monkeypatch, fail_channel):
    from types import SimpleNamespace
    from kernel import agent_runtime, data_layout, message_pool, platform_hub
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from tests.test_memory_daily_online import TeaExtractor

    events, runtimes = [], []
    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    monkeypatch.setattr(data_layout, "migrate_once", lambda: None)
    monkeypatch.setattr(AppBus, "scan_apps", lambda self: None)
    monkeypatch.setattr(agent_runtime, "Brain", lambda bus: SimpleNamespace(_active_agent_id="test-agent"))
    monkeypatch.setattr(agent_runtime, "get_bios_list", lambda: ["synthetic-channel"])
    monkeypatch.setattr(agent_runtime, "get_bios_flag", lambda *a, **kw: False)
    monkeypatch.setattr(message_pool, "MessagePool", lambda path: None)

    class Hub:
        def __init__(self, path):
            pass

        async def load(self):
            pass

    monkeypatch.setattr(platform_hub, "PlatformHub", Hub)

    async def channels(bus, apps):
        events.append("channels")
        health = bus.daily_memory_health()
        assert health["raw_state"] == "ready"
        assert health["state"] in {"ready", "degraded"}
        if fail_channel:
            raise RuntimeError("synthetic channel startup failure")

    monkeypatch.setattr(agent_runtime, "connect_bios_apps", channels)

    def factory(raw, profile, bus):
        events.append("factory")
        assert raw is bus.raw_messages and raw.profile is profile
        runtime = DailyMemoryRuntime(raw, profile, bus, extractor=TeaExtractor(), embedding="synthetic")
        runtimes.append(runtime)
        return runtime

    if fail_channel:
        with pytest.raises(RuntimeError, match="synthetic channel"):
            await agent_runtime.boot(raw_profile=config, daily_runtime_factory=factory)
        assert runtimes[0].health()["state"] == "stopped"
        assert runtimes[0].retrieval.worker_pid is None
        return

    bus, _ = await agent_runtime.boot(raw_profile=config, daily_runtime_factory=factory)
    try:
        assert events == ["factory", "channels"]
        with pytest.raises(ValueError):
            bus.attach_daily_runtime(bus._daily_runtime)
    finally:
        await agent_runtime.shutdown(bus)
    assert bus.daily_memory_health()["state"] == "stopped"


@pytest.mark.asyncio
async def test_boot_mixed_factory_arguments_fail_before_any_side_effect():
    from kernel.agent_runtime import boot

    with pytest.raises(ValueError, match="factory"):
        await boot(daily_runtime_factory=lambda *a: None, daily_derivation=object())


@pytest.mark.asyncio
async def test_fifty_continuous_messages_survive_restart_failure_and_edit(tmp_path):
    import hashlib
    from datetime import datetime, timezone
    from system_memory_god.daily_runtime import DailyMemoryRuntime

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            if message.content == "在线原文-7":
                raise ValueError("synthetic failure")
            await asyncio.sleep(0.005)
            return ({"subject": "用户", "attribute": "记录", "value": message.content},)

    config = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                     derivations=(DerivationSpec("online", "v1", "entity", "fake", "a" * 64),))
    saved_ids = []
    for offset in (0, 25):
        raw = RawMessageService(config)
        bus = AppBus(str(tmp_path / "apps"))
        bus.attach_raw_messages(raw, lambda: "test-agent")
        runtime = DailyMemoryRuntime(raw, config, bus, extractor=Extractor(), embedding="synthetic")
        bus.attach_daily_runtime(runtime)
        try:
            await runtime.start()
            for n in range(offset, offset + 25):
                msg = await bus.accept_input(Message.user_input(f"在线原文-{n}", "web", chat_name="web_console"))
                saved_ids.append(msg.get("origin_message_id"))
                scope = Scope("test-agent", "test-owner", "web-test", msg.get("conversation_id"))
                if n == 20:
                    raw.store.edit(scope, saved_ids[0], "修订后的原文", expected_revision=1,
                                   edited_at=datetime.now(timezone.utc).isoformat())
                # Foreground query is allowed to report recovery explicitly, not
                # fabricate a successful empty result when the child is unavailable.
                outcome = await runtime.retrieval.search_with_status(scope, f"在线原文-{n}")
                if outcome.status == "ready":
                    assert saved_ids[-1] in {row["source_message_id"] for row in outcome.items}
            expected = offset + 25
            deadline = asyncio.get_running_loop().time() + 20
            while True:
                statuses = [task["status"] for mid in saved_ids
                            for task in raw.store.list_derivations(scope, mid)
                            if task["revision"] == raw.store.get(scope, mid).revision]
                if statuses.count("succeeded") == expected - 1 and statuses.count("failed") == 1:
                    break
                assert asyncio.get_running_loop().time() < deadline, runtime.health()
                await asyncio.sleep(0.1)
            replay = raw.store.replay(scope, scope.conversation_id, limit=100)
            assert len(replay) == expected
            assert len({row.message_id for row in replay}) == expected
            assert all(hashlib.sha256(row.content.encode()).hexdigest() == row.content_hash
                       for row in replay)
            assert [row.ordinal for row in replay] == sorted(row.ordinal for row in replay)
            assert raw.store.get(scope, saved_ids[0]).content == "修订后的原文"
            assert raw.store.get(scope, saved_ids[7]).content == "在线原文-7"
        finally:
            await runtime.close()
