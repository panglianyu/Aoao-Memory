"""Twelve early cases on real isolated daily stores, with synthetic model boundaries.

No production profile, cloud client, real embedding model, or generated hit list.
AppBus is the usual ingress; explicit history uses public MessageDraft/append.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
import warnings

import pytest

from kernel.app_bus import AppBus
from kernel.memory_pipeline_models import PipelineScope
from kernel.message import Message
from kernel.raw_message_models import MessageDraft, RawMessageError, Scope
from kernel.raw_message_service import ChannelOriginal, RawMessageService
from system_memory_god.daily_runtime import DailyMemoryRuntime
from system_memory_god.derivation import daily_specs
from tests.test_raw_message_service import profile

_PUBLICATION_OBSERVATION_ROOT = Path(__file__).resolve().parents[1] / (
    "runtime/upgrade_validation/memory-daily-recall-diagnosis-20261009/validation/v06")


def _selected_observation_root():
    selected = os.environ.get("AOAO_EARLY_CASES_DIAGNOSTIC_ROOT")
    if not selected:
        return None
    try:
        candidate = Path(selected)
        if (candidate.resolve() == _PUBLICATION_OBSERVATION_ROOT
                and not candidate.is_symlink() and candidate.is_dir()):
            return _PUBLICATION_OBSERVATION_ROOT
    except (OSError, RuntimeError, ValueError):
        pass
    return None


class SyntheticExtractor:
    """Only the extraction boundary is synthetic; persistence/retrieval are real."""

    def __init__(self, facts=None, vectors=None):
        self.facts = facts or {}
        self.vectors = vectors or {}

    async def extract(self, domain, message, source, max_output_tokens):
        if domain == "entity" and message.content in self.facts:
            return (dict(self.facts[message.content]),)
        if domain == "vector":
            return ({"text": self.vectors.get(message.content, "acceptance-semantic-key")},)
        return ()


@pytest.fixture
def publication_observation_root(tmp_path, monkeypatch):
    root = tmp_path / "publication-observation"
    root.mkdir()
    monkeypatch.setattr(sys.modules[__name__], "_PUBLICATION_OBSERVATION_ROOT", root, raising=False)
    monkeypatch.setenv("AOAO_EARLY_CASES_DIAGNOSTIC_ROOT", str(root))
    return root


def test_observation_selection_is_current_only_and_default_disabled(tmp_path, monkeypatch, publication_observation_root):
    observation = recall_observation()
    assert observation is not None and observation.root == publication_observation_root
    monkeypatch.delenv("AOAO_EARLY_CASES_DIAGNOSTIC_ROOT")
    assert recall_observation() is None
    for name in ("memory-daily-early-cases-r3-20261008", "validation/v04"):
        obsolete = tmp_path / name
        obsolete.mkdir(parents=True)
        monkeypatch.setenv("AOAO_EARLY_CASES_DIAGNOSTIC_ROOT", str(obsolete))
        assert recall_observation() is None
        with pytest.raises(ValueError):
            PublicationObservation()
        assert list(obsolete.iterdir()) == []


class PublicationObservation:
    """Opt-in test-only observation; original calls/results/errors stay intact."""

    def __init__(self):
        expected = _selected_observation_root()
        if expected is None:
            raise ValueError("explicit current diagnostic root required")
        self.v03 = True
        self.root = expected
        self.started = time.monotonic()
        self.batch_started = self.started
        self.events = []
        self.errors = 0
        self.truncated = False
        self.lock = threading.Lock()
        self.originals = []
        self.artifact_unavailable = False
        self.output_unavailable = False
        self.event_bytes = 0
        self.artifact_path = None

    def record(self, stage, **fields):
        # Observation faults must not change a production call's behavior.
        try:
            with self.lock:
                event = {"stage": stage,
                    "elapsed": round(time.monotonic() - self.started, 6), **fields}
                size = len(json.dumps(event, ensure_ascii=True, allow_nan=False).encode())
                if (len(self.events) >= (200 if self.v03 else 2000)
                        or self.v03 and self.event_bytes + size > 60 * 1024):
                    self.truncated = True
                    return
                self.event_bytes += size
                self.events.append(event)
        except Exception:
            self.errors += 1

    def state(self, runtime, partition):
        scope = Scope(partition.agent_id, partition.user_id,
                      partition.connector_id, partition.conversation_id)
        try:
            state = {"generation": list(runtime.raw.store.partition_state(scope)),
                    "attempts": runtime._semantic_attempts.get(partition, 0),
                    "dirty": partition in runtime._semantic_dirty}
            if self.v03:
                reader = runtime.snapshot_reader
                state.update(scope_hash=hashlib.sha256(partition.partition_id.encode()).hexdigest(),
                    reader_pid=reader.worker_pid, available=reader.available(partition),
                    lifecycle_locked=reader._lifecycle.locked(),
                    recovery_pending=reader._recovery is not None and not reader._recovery.done(),
                    reader_generation=reader._ready.get(partition),
                    cache_generation=runtime.snapshot_cache._current.get(partition))
            return state
        except Exception:
            self.errors += 1
            return {}

    @staticmethod
    def error_type(exc):
        name = type(exc).__name__
        return name if name in {"ValueError", "RuntimeError", "OSError", "TimeoutError",
                               "SnapshotCapacityError", "CancelledError",
                               "_RecoveryGenerationChanged"} else "OtherError"

    def install(self, runtime):
        def sync_observer(owner, attribute, stage):
            original = getattr(owner, attribute)

            def observed(partition, *args, **kwargs):
                self.record(stage + ":start", **self.state(runtime, partition))
                try:
                    result = original(partition, *args, **kwargs)
                    self.record(stage + ":end", **self.state(runtime, partition))
                    return result
                except BaseException as exc:
                    self.record(stage + ":error", error_type=self.error_type(exc),
                                **self.state(runtime, partition))
                    raise

            self.originals.append((owner, attribute, original))
            setattr(owner, attribute, observed)

        if not self.v03:
            sync_observer(runtime, "_notify_semantic", "notify")
        sync_observer(runtime, "_verified_snapshot_rows", "verified")
        sync_observer(runtime.snapshot_cache, "publish", "publish")
        original = runtime.snapshot_reader.warm_existing

        async def observed_warm(partition):
            self.record("warm:start", **self.state(runtime, partition))
            try:
                result = await original(partition)
                self.record("warm:end", **self.state(runtime, partition))
                return result
            except BaseException as exc:
                self.record("warm:error", error_type=self.error_type(exc),
                            **self.state(runtime, partition))
                raise

        self.originals.append((runtime.snapshot_reader, "warm_existing", original))
        runtime.snapshot_reader.warm_existing = observed_warm
        if self.v03:
            original_query = runtime.snapshot_reader.search_cosine_hits

            async def observed_query(partition, query, **kwargs):
                from Tools.probe_daily_memory_recall_diag import (
                    finite_diagnostic, reader_identity)

                self.record("query:start", budget_ms=150, **self.state(runtime, partition))
                previous, reader_before = None, None
                try:
                    previous = runtime.snapshot_reader.diagnostic()
                    reader_before = reader_identity(runtime)
                except Exception:
                    self.errors += 1
                def diagnostic_after():
                    try:
                        if previous is None:
                            return {"association": "inconclusive", "values": None}
                        return finite_diagnostic(runtime.snapshot_reader.diagnostic(), previous,
                            reader_before=reader_before, reader_after=reader_identity(runtime))
                    except Exception:
                        self.errors += 1
                        return {"association": "inconclusive", "values": None}
                began = time.perf_counter()
                try:
                    result = await original_query(partition, query, **kwargs)
                except BaseException as exc:
                    elapsed = (time.perf_counter() - began) * 1000
                    self.record("query:error", query_elapsed_ms=elapsed,
                        diagnostic=diagnostic_after(), observer_error=previous is None,
                        error_type=self.error_type(exc), **self.state(runtime, partition))
                    raise
                elapsed = (time.perf_counter() - began) * 1000
                try:
                    diagnostic = diagnostic_after()
                    self.record("query:end", query_elapsed_ms=elapsed, candidate_count=len(result),
                        diagnostic=diagnostic, observer_error=previous is None, **self.state(runtime, partition))
                except Exception:
                    self.errors += 1
                return result

            self.originals.append((runtime.snapshot_reader, "search_cosine_hits", original_query))
            runtime.snapshot_reader.search_cosine_hits = observed_query

    def mark_batch(self):
        self.batch_started = time.monotonic()
        self.record("batch:start")

    async def after_deadline(self, runtime, scope, message_id):
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        self.record("acceptance:failed", message_hash=hashlib.sha256(
            message_id.encode()).hexdigest(), **self.state(runtime, partition))
        while time.monotonic() < self.batch_started + 80:
            health = runtime.health()
            self.record("observation:health", semantic=health["semantic_state"],
                        reason=health["semantic_reason"], derived=health["derived_state"],
                        reader_pid=health["semantic_reader_pid"], writer_pid=health["vector_pid"],
                        **self.state(runtime, partition))
            if health["semantic_state"] == "ready":
                break
            await asyncio.sleep(.5)

    def finish(self, raw, scope, *, closed):
        for owner, attribute, original in reversed(self.originals):
            setattr(owner, attribute, original)
        self.record("observation:closed" if closed else "observation:close_failed")
        # Public replay/task APIs, explicitly after normal runtime.close().
        try:
            messages = raw.store.replay(scope, scope.conversation_id)
            states = {}
            for message in messages:
                for task in raw.store.list_derivations(scope, message.message_id):
                    states[task["status"]] = states.get(task["status"], 0) + 1
            output = {"prefix": "[DEBUG-EARLY-V06]",
                      "schema": 3 if self.v03 else 1,
                      "acceptance_deadline_seconds": 20,
                      "observation_limit_seconds": 0 if self.v03 else 80,
                      "observer_errors": self.errors, "truncated": self.truncated,
                      "close_succeeded": closed,
                      "after_close_attempt": {"messages": len(messages), "task_states": states},
                      "events": self.events}
            body = json.dumps(output, ensure_ascii=True, allow_nan=False,
                              separators=(",", ":") if self.v03 else None)
            if self.v03 and len(body.encode()) > 64 * 1024:
                raise ValueError("bounded diagnostic artifact required")
            self.artifact_path = self.root / f"publication-trace-{time.time_ns()}.json"
            self.artifact_path.write_text(body, encoding="utf-8")
        except Exception:
            self.artifact_unavailable = True
            try:
                sys.stderr.write("[DEBUG-EARLY-R3] diagnostic artifact unavailable\n")
            except Exception:
                self.output_unavailable = True


def recall_observation():
    return PublicationObservation() if _selected_observation_root() is not None else None


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [False, True])
async def test_query_observer_failure_does_not_replace_real_query(tmp_path, monkeypatch, prepared, publication_observation_root):
    observation = PublicationObservation()
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "虚构观察器异常边界")
        await settled(raw, runtime, scope, message_id)
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        observation.install(runtime)
        def unavailable():
            raise OSError("synthetic observation IO failure")
        monkeypatch.setattr(runtime.snapshot_reader, "diagnostic", unavailable)
        try:
            if prepared:
                hits = await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key")
                expected = {row["memory_id"] for row in raw.store.results(scope, message_id) if row["target_domain"] == "vector"}
                assert {identity for identity, _ in hits} == expected and hits
            else:
                async with runtime.snapshot_reader._lifecycle:
                    with pytest.raises(RuntimeError, match="snapshot reader not prepared"):
                        await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key")
            assert observation.errors > 0
        finally:
            for owner, attribute, original in reversed(observation.originals):
                setattr(owner, attribute, original)


@pytest.mark.asyncio
async def test_query_observer_failure_preserves_original_cancellation(tmp_path, monkeypatch, publication_observation_root):
    observation = PublicationObservation()
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "虚构观察器取消边界")
        await settled(raw, runtime, scope, message_id)
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        entered, release = asyncio.Event(), asyncio.Event()
        original = runtime.snapshot_reader._transport._invoke
        async def transport_boundary(operation, *args, **kwargs):
            if operation == "search_cosine_hits":
                entered.set()
                await release.wait()
            return await original(operation, *args, **kwargs)
        def unavailable():
            raise OSError("synthetic observation IO failure")
        observation.install(runtime)
        monkeypatch.setattr(runtime.snapshot_reader._transport, "_invoke", transport_boundary)
        monkeypatch.setattr(runtime.snapshot_reader, "diagnostic", unavailable)
        task = asyncio.create_task(runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key"))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
            errors = [event for event in observation.events if event["stage"] == "query:error"]
            assert errors[-1]["error_type"] == "CancelledError"
            assert errors[-1]["diagnostic"]["association"] == "inconclusive"
        finally:
            release.set()
            for owner, attribute, saved in reversed(observation.originals):
                setattr(owner, attribute, saved)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_failure", [False, True])
async def test_diagnostic_io_failure_retains_original_result(tmp_path, monkeypatch, output_failure, publication_observation_root):
    """Optional filesystem/output failures do not replace an original assertion."""
    observation = PublicationObservation()
    raw = RawMessageService(profile(tmp_path))
    await raw.initialize_profile()
    try:
        def denied_write(*args, **kwargs):
            raise OSError("synthetic diagnostic filesystem unavailable")

        class DeniedOutput:
            def write(self, text):
                raise OSError("synthetic diagnostic output unavailable")

            def flush(self):
                return None

        with pytest.raises(AssertionError, match="original acceptance failure"):
            with monkeypatch.context() as patch:
                patch.setattr(Path, "write_text", denied_write)
                if output_failure:
                    patch.setattr(sys, "stderr", DeniedOutput())
                with warnings.catch_warnings():
                    warnings.simplefilter("error")
                    try:
                        raise AssertionError("original acceptance failure")
                    finally:
                        observation.finish(raw, raw.configured_scopes()[0], closed=True)
        assert observation.artifact_unavailable
        assert observation.output_unavailable is output_failure
    finally:
        await raw.close()


@pytest.mark.asyncio
async def test_v03_observation_has_fixed_root_and_bounded_events(tmp_path, monkeypatch, publication_observation_root):
    observation = PublicationObservation()
    raw = RawMessageService(profile(tmp_path))
    await raw.initialize_profile()
    try:
        for number in range(250):
            observation.record("query:start", number=number)
        observation.finish(raw, raw.configured_scopes()[0], closed=True)
        assert observation.artifact_path.parent == publication_observation_root
        body = observation.artifact_path.read_bytes()
        result = json.loads(body)
        assert len(body) <= 64 * 1024
        assert len(result["events"]) <= 200 and result["truncated"] is True
        assert result["schema"] == 3
    finally:
        await raw.close()


def _cleanup_observation_property(snapshot):
    """Accept only the finite local observer schema before adding JUnit metadata."""
    from system_memory_god.storage.supervised_daily_vector import _CLEANUP_STAGES, _CLEANUP_ERROR_TYPES

    if (type(snapshot) is not dict or set(snapshot) != {
            "schema", "attempt", "frames", "dropped", "complete"}
            or type(snapshot["schema"]) is not int or snapshot["schema"] != 1
            or any(type(snapshot[name]) is not int or not 0 <= snapshot[name] <= 1_000_000
                   for name in ("attempt", "dropped"))
            or type(snapshot["complete"]) is not bool
            or type(snapshot["frames"]) is not list or len(snapshot["frames"]) > 32):
        return None
    errors = {kind.__name__ for kind in _CLEANUP_ERROR_TYPES} | {"OtherError"}
    for frame in snapshot["frames"]:
        if (type(frame) is not dict or not {"stage", "elapsed_ms"} <= set(frame)
                or set(frame) - {"stage", "elapsed_ms", "process_alive", "error_type"}
                or type(frame["stage"]) is not str or frame["stage"] not in _CLEANUP_STAGES
                or type(frame["elapsed_ms"]) not in {int, float}
                or not math.isfinite(frame["elapsed_ms"]) or not 0 <= frame["elapsed_ms"] <= 999_999_999.999
                or "process_alive" in frame and type(frame["process_alive"]) is not bool
                or "error_type" in frame and (type(frame["error_type"]) is not str
                                              or frame["error_type"] not in errors)):
            return None
    encoded = json.dumps(snapshot, separators=(",", ":"))
    return encoded if len(encoded.encode("utf-8")) <= 8192 else None


@asynccontextmanager
async def daily(tmp_path, *, settings=None, extractor=None, observation=None,
                cleanup_observation=None):
    config = settings or replace(
        profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
        derivations=daily_specs("synthetic-acceptance"),
    )
    raw = RawMessageService(config)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: config.connectors[0].agent_id)
    runtime = DailyMemoryRuntime(
        raw, config, bus, extractor=extractor or SyntheticExtractor(),
        embedding="synthetic", semantic_mode="snapshot",
        semantic_diagnostics=(cleanup_observation is not None
                              or observation is not None and observation.v03),
    )
    cleanup_transport = (runtime.snapshot_reader._transport
                         if cleanup_observation is not None else None)
    bus.attach_daily_runtime(runtime)
    if observation is not None:
        observation.install(runtime)
    try:
        await runtime.start()
        yield raw, bus, runtime
    finally:
        closed = False
        try:
            await runtime.close()
            closed = True
        finally:
            if cleanup_transport is not None:
                try:
                    snapshot = cleanup_transport._cleanup_diagnostic_snapshot()
                    encoded = _cleanup_observation_property(snapshot)
                    if encoded is not None:
                        cleanup_observation.append(("vector_cleanup", encoded))
                except Exception:
                    # Observation must not replace the original close outcome.
                    pass
            if observation is not None:
                observation.finish(raw, raw.configured_scopes()[0], closed=closed)


async def settled(raw, runtime, scope, message_id, *, revision=1, deadline=None):
    deadline = time.monotonic() + 20 if deadline is None else deadline
    while time.monotonic() < deadline:
        tasks = [task for task in raw.store.list_derivations(scope, message_id)
                 if task["revision"] == revision]
        if (len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks)
                and runtime.health()["semantic_state"] == "ready"):
            partition = PipelineScope(scope.user_id, scope.agent_id,
                                      scope.connector_id, scope.conversation_id)
            publication = await runtime.semantic_publication_state(partition, deadline=deadline)
            if publication["status"] == "current":
                return tasks
        await asyncio.sleep(.05)
    health = runtime.health()
    states = [(task["target_domain"], task["status"], task["last_error_code"])
              for task in raw.store.list_derivations(scope, message_id)
              if task["revision"] == revision]
    raise AssertionError(
        f"six-domain completion/snapshot publication deadline exceeded: "
        f"jobs={states}, semantic={health['semantic_state']}, "
        f"reason={health['semantic_reason']}, derived={health['derived_state']}")


def provenance(raw, runtime, scope, message_id, *, revision=1):
    original = raw.store.get(scope, message_id)
    assert original is not None
    assert original.revision == revision
    assert original.status == ("active" if revision == 1 else "edited")
    assert (original.user_id, original.agent_id, original.connector_id,
            original.conversation_id) == (
                scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
    partition = PipelineScope(scope.user_id, scope.agent_id,
                              scope.connector_id, scope.conversation_id)
    results = raw.store.results(scope, message_id)
    assert results
    assert len({item["memory_id"] for item in results}) == len(results)
    for item in results:
        sources = runtime.stores.ledger(partition).sources_of(item["memory_id"])
        assert len(sources) == 1
        assert sources[0].raw_message_id == message_id
        assert sources[0].revision == revision
        record = runtime.stores.evidence(partition).get(item["memory_id"])
        assert record is not None
        assert record["store_ref"] == item["store_ref"]
        assert record["source"] == asdict(sources[0])
        assert record["domain"] == item["target_domain"]
        assert raw.store.derivation_sources(scope, (sources[0].task_id,)) == {
            sources[0].task_id: (sources[0], item["target_domain"])}
    return original, results


async def ingress(raw, bus, content, *, chat="web_console", source="web", **metadata):
    saved = await bus.accept_input(Message.user_input(
        content, source, chat_name=chat, **metadata))
    message_id = saved.get("origin_message_id")
    assert message_id
    scopes = [scope for scope in raw.configured_scopes()
              if raw.store.get(scope, message_id) is not None]
    assert len(scopes) == 1
    return scopes[0], message_id


def hit_proof(raw, scope, hits, expected_ids, *, revision=1):
    assert hits
    assert {item["source_message_id"] for item in hits} == set(expected_ids)
    for item in hits:
        original = raw.store.get(scope, item["source_message_id"])
        assert original is not None
        assert original.revision == item["revision"] == revision
        assert item["source_content"] == original.content
        assert item["source_time"] == original.created_at
        assert original.status in {"active", "edited"}


def source_inventory(raw, saved):
    """Test-admin content/task inventory, not a caller authorization interface."""
    inventory = []
    for scope, message_id in saved:
        row = raw.store.get(scope, message_id)
        assert row is not None
        payload = {"message": asdict(row), "tasks": raw.store.list_derivations(scope, message_id),
                   "results": raw.store.results(scope, message_id)}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
        inventory.append((scope, raw.store.partition_state(scope),
                          tuple(item.message_id for item in raw.store.replay(scope, scope.conversation_id)),
                          message_id, digest))
    return tuple(inventory)


@pytest.mark.asyncio
async def test_time_update_1(tmp_path):
    """Two explicit Raw preferences retain exact entity sources and latest evidence."""
    old_text = "虚构用户以前喜欢喝红茶"
    current_text = "虚构用户现在不喝红茶，只喝咖啡"
    extractor = SyntheticExtractor({
        old_text: {"subject": "虚构用户", "attribute": "饮品偏好", "value": "红茶"},
        current_text: {"subject": "虚构用户", "attribute": "饮品偏好", "value": "咖啡"},
    })
    async with daily(tmp_path, extractor=extractor) as (raw, bus, runtime):
        scope, old_id = await ingress(raw, bus, old_text)
        await settled(raw, runtime, scope, old_id)
        _, current_id = await ingress(raw, bus, current_text)
        await settled(raw, runtime, scope, current_id)
        old, _ = provenance(raw, runtime, scope, old_id)
        current, _ = provenance(raw, runtime, scope, current_id)
        assert old.created_at < current.created_at
        assert (old.content, current.content) == (old_text, current_text)
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        for message_id, value in ((old_id, "红茶"), (current_id, "咖啡")):
            entities = [runtime.stores.evidence(partition).get(item["memory_id"])
                        for item in raw.store.results(scope, message_id)
                        if item["target_domain"] == "entity"]
            assert len(entities) == 1
            assert entities[0]["payload"] == {
                "subject": "虚构用户", "attribute": "饮品偏好", "value": value}
            assert entities[0]["source"]["raw_message_id"] == message_id
        outcome = await runtime.retrieval.search_with_status(scope, "红茶", top_k=1)
        assert outcome.status == "ready" and not outcome.reason_codes
        hits = outcome.items
        hit_proof(raw, scope, hits, {current_id})
        with pytest.raises(AssertionError):
            hit_proof(raw, scope, hits, {old_id})
        with pytest.raises(AssertionError):
            hit_proof(raw, scope, hits, {current_id}, revision=2)
        # Source-level fusion may label this vector while retaining exact Raw proof.
        assert hits[0]["source_content"] == current.content


@pytest.mark.asyncio
async def test_numeric_snapshot_contribution(tmp_path):
    """A real numerical candidate survives fusion with no lexical candidate."""
    content = "虚构原文仅用于来源核对"
    vector_text = "vector-fixture-key"
    query = "numerical-only-probe"
    extractor = SyntheticExtractor(vectors={content: vector_text})
    observation = recall_observation()
    async with daily(tmp_path, extractor=extractor, observation=observation) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, content)
        await settled(raw, runtime, scope, message_id)
        _, anchors = provenance(raw, runtime, scope, message_id)
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        assert raw.store.search(scope, query) == []
        assert runtime.stores.evidence(partition).search_ids(query) == ()
        with pytest.raises(AssertionError):
            assert runtime.stores.evidence(partition).search_ids(vector_text) == ()
        expected = {item["memory_id"] for item in anchors if item["target_domain"] == "vector"}
        candidates = await runtime.snapshot_reader.search_cosine_hits(partition, query)
        assert {memory_id for memory_id, _ in candidates} == expected
        assert all(math.isfinite(score) and -1 <= score <= 1 for _, score in candidates)
        outcome = await runtime.retrieval.search_with_status(scope, query)
        assert outcome.status == "ready" and not outcome.reason_codes
        hit_proof(raw, scope, outcome.items, {message_id})
        assert {item["memory_id"] for item in outcome.items} == expected


@pytest.mark.asyncio
async def test_publication_recovers_after_three_source_changes(tmp_path, monkeypatch):
    """Real append inside capture must not permanently fence a live publisher."""
    from system_memory_god.derivation import _RecoveryGenerationChanged

    async with daily(tmp_path) as (raw, bus, runtime):
        scope, seed_id = await ingress(raw, bus, "虚构核验窗口种子")
        await settled(raw, runtime, scope, seed_id)
        capture = runtime.vector.snapshot_vectors
        changed = []
        verification = runtime._verified_snapshot_rows
        conflicts = []

        def verified(partition):
            try:
                return verification(partition)
            except _RecoveryGenerationChanged:
                conflicts.append(partition)
                raise

        def capture_with_append(partition):
            rows = capture(partition)
            if rows and len(changed) < 3:
                index = len(changed)
                before = raw.store.partition_state(scope)
                saved = append_history(raw, scope, f"window-change-{index}",
                    f"虚构核验窗口写入{index}", f"2026-10-08T10:00:0{index}+08:00", "window")
                after = raw.store.partition_state(scope)
                changed.append((saved.message_id, before, after))
            return rows

        monkeypatch.setattr(runtime.vector, "snapshot_vectors", capture_with_append)
        monkeypatch.setattr(runtime, "_verified_snapshot_rows", verified)
        _, tick_id = await ingress(raw, bus, "虚构核验窗口触发")
        await settled(raw, runtime, scope, tick_id)
        assert len(changed) == 3 and all(before != after for _, before, after in changed)
        assert len(conflicts) >= 3
        expected = {seed_id, tick_id, *(message_id for message_id, _, _ in changed)}
        for message_id in expected:
            await settled(raw, runtime, scope, message_id)
            provenance(raw, runtime, scope, message_id)
        result = await runtime.retrieval.search_with_status(scope, "acceptance-semantic-key", top_k=100)
        assert result.status == "ready" and not result.reason_codes
        hit_proof(raw, scope, result.items, expected)


@pytest.mark.asyncio
async def test_hard_filesystem_budget_survives_interleaved_source_conflicts(tmp_path, monkeypatch):
    """Cache write-boundary faults and moving Raw proofs keep separate budgets."""
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, seed_id = await ingress(raw, bus, "虚构硬故障前种子")
        await settled(raw, runtime, scope, seed_id)
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        lease = runtime.snapshot_cache.acquire(partition)
        generation = lease.generation
        runtime.snapshot_cache.release(lease)
        capture, open_file = runtime.vector.snapshot_vectors, Path.open
        hard_errors, changes = [], []

        def open_with_fault(path, mode="r", *args, **kwargs):
            if (mode == "xb" and path.name == "vectors.f32"
                    and path.parent.parent == runtime.snapshot_cache.root / partition.partition_id
                    and path.parent.name.startswith("staging-") and len(path.parent.name) == 40
                    and all(char in "0123456789abcdef" for char in path.parent.name[8:])
                    and len(hard_errors) < 3):
                hard_errors.append(time.monotonic())
                raise OSError("synthetic snapshot filesystem denied")
            return open_file(path, mode, *args, **kwargs)

        def capture_with_conflict(selected):
            rows = capture(selected)
            if rows and len(hard_errors) == 1 and len(changes) < 3:
                index = len(changes)
                changes.append(append_history(raw, scope, f"mixed-source-{index}",
                    f"虚构交错来源{index}", f"2026-10-08T11:00:0{index}+08:00", "mixed").message_id)
            return rows

        monkeypatch.setattr(Path, "open", open_with_fault)
        monkeypatch.setattr(runtime.vector, "snapshot_vectors", capture_with_conflict)
        _, tick_id = await ingress(raw, bus, "虚构硬故障触发")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if (len(hard_errors) == 3 and runtime._semantic_attempts.get(partition) == 3
                    and runtime.health()["semantic_state"] == "degraded"
                    and runtime.health()["semantic_reason"] == "semantic_publication_incomplete"):
                break
            await asyncio.sleep(.05)
        assert len(changes) == len(hard_errors) == 3
        assert runtime._semantic_attempts.get(partition) == 3
        assert runtime.health()["semantic_state"] == "degraded"
        assert runtime.health()["semantic_reason"] == "semantic_publication_incomplete"
        # Restoring the filesystem does not clear a real hard-fault fence.
        _, later_id = await ingress(raw, bus, "虚构硬故障后保存仍可靠")
        message_ids = {seed_id, tick_id, later_id, *changes}
        while time.monotonic() < deadline:
            if all(len(raw.store.list_derivations(scope, key)) == 6 and
                   all(task["status"] == "succeeded" for task in raw.store.list_derivations(scope, key))
                   for key in message_ids):
                break
            await asyncio.sleep(.05)
        for key in message_ids:
            assert raw.store.get(scope, key) is not None
            tasks = raw.store.list_derivations(scope, key)
            assert len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks)
            provenance(raw, runtime, scope, key)
        await asyncio.sleep(.1)
        assert len(hard_errors) == 3 and runtime._semantic_attempts.get(partition) == 3
        lease = runtime.snapshot_cache.acquire(partition)
        assert lease.generation == generation
        runtime.snapshot_cache.release(lease)
        assert runtime.health()["semantic_state"] == "degraded"


@pytest.mark.asyncio
async def test_real_snapshot_capacity_fault_remains_fenced(tmp_path, monkeypatch):
    """A real bounded capacity check preserves Raw without publishing a subset."""
    from system_memory_god.storage import daily_vector_snapshot as snapshot_module

    async with daily(tmp_path) as (raw, bus, runtime):
        seed_deadline = time.monotonic() + 20
        scope, seed_id = await ingress(raw, bus, "虚构容量种子")
        await settled(raw, runtime, scope, seed_id, deadline=seed_deadline)
        _, anchors = provenance(raw, runtime, scope, seed_id)
        expected = {item["memory_id"] for item in anchors if item["target_domain"] == "vector"}
        assert len(expected) == 1
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        cached_ids, candidate_ids = set(), set()
        while time.monotonic() < seed_deadline:
            if runtime.health()["semantic_state"] == "ready":
                lease = runtime.snapshot_cache.acquire(partition)
                try:
                    data = snapshot_module.load_snapshot(runtime.snapshot_cache.root, partition,
                                                         lease.generation, embedding="synthetic")
                    cached_ids = set(data.ids)
                    original_generation = lease.generation
                finally:
                    runtime.snapshot_cache.release(lease)
                if cached_ids == expected:
                    candidates = await runtime.snapshot_reader.search_cosine_hits(
                        partition, "acceptance-semantic-key")
                    candidate_ids = {key for key, _ in candidates}
                    if candidate_ids == expected:
                        break
            await asyncio.sleep(.05)
        assert time.monotonic() < seed_deadline
        assert cached_ids == candidate_ids == expected
        # Lower the actual validator limit; all input rows are real receipts.
        monkeypatch.setattr(snapshot_module, "MAX_ROWS", 1)
        _, later_id = await ingress(raw, bus, "虚构容量第二条仍须保存")
        deadline = time.monotonic() + 20
        while runtime.health()["semantic_reason"] != "vector_capacity_exceeded" and time.monotonic() < deadline:
            await asyncio.sleep(.05)
        assert runtime.health()["semantic_state"] == "degraded"
        assert runtime.health()["semantic_reason"] == "vector_capacity_exceeded"
        assert runtime._semantic_attempts.get(partition) == 3
        for message_id in (seed_id, later_id):
            tasks = raw.store.list_derivations(scope, message_id)
            assert len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks)
            provenance(raw, runtime, scope, message_id)
        lease = runtime.snapshot_cache.acquire(partition)
        try:
            assert lease.generation == original_generation
            data = snapshot_module.load_snapshot(runtime.snapshot_cache.root, partition,
                                                 lease.generation, embedding="synthetic")
            assert set(data.ids) == expected
        finally:
            runtime.snapshot_cache.release(lease)
        candidates = await runtime.snapshot_reader.search_cosine_hits(partition, "acceptance-semantic-key")
        assert len(candidates) == 1
        assert {key for key, _ in candidates} == expected
        provenance(raw, runtime, scope, seed_id)


@pytest.mark.asyncio
async def test_changing_scope_does_not_starve_another_scope(tmp_path, monkeypatch):
    """B publishes actual new evidence while A still needs fresh Raw proofs."""
    from kernel.raw_message_profile import ConversationBinding

    base = profile(tmp_path / "raw")
    settings = replace(base, derived_root=tmp_path / "derived",
        derivations=daily_specs("synthetic-acceptance"), connectors=(replace(
            base.connectors[0], conversations=(
                ConversationBinding("busy", "busy-provider", "web", "虚构忙会话"),
                ConversationBinding("other", "other-provider", "web", "虚构独立会话"))),))
    observation = recall_observation()
    async with daily(tmp_path, settings=settings, observation=observation) as (raw, bus, runtime):
        busy, busy_seed = await ingress(raw, bus, "虚构忙会话种子", chat="busy")
        other, other_seed = await ingress(raw, bus, "虚构独立会话种子", chat="other")
        await settled(raw, runtime, busy, busy_seed)
        await settled(raw, runtime, other, other_seed)
        busy_partition = PipelineScope(busy.user_id, busy.agent_id,
                                       busy.connector_id, busy.conversation_id)
        other_partition = PipelineScope(other.user_id, other.agent_id,
                                        other.connector_id, other.conversation_id)
        capture = runtime.vector.snapshot_vectors
        changes = []

        def capture_with_busy_source(partition):
            rows = capture(partition)
            if partition == busy_partition and rows and len(changes) < 6:
                index = len(changes)
                changes.append(append_history(raw, busy, f"busy-change-{index}",
                    f"虚构忙会话持续写入{index}", f"2026-10-08T12:00:0{index}+08:00", "busy").message_id)
            return rows

        monkeypatch.setattr(runtime.vector, "snapshot_vectors", capture_with_busy_source)
        _, busy_tick = await ingress(raw, bus, "虚构忙会话启动变化", chat="busy")
        deadline = time.monotonic() + 20
        while not changes and time.monotonic() < deadline:
            await asyncio.sleep(.05)
        assert changes
        _, other_tick = await ingress(raw, bus, "虚构独立会话新证据", chat="other")
        while time.monotonic() < deadline:
            tasks = raw.store.list_derivations(other, other_tick)
            if (len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks)
                    and runtime.snapshot_reader.available(other_partition)
                    and not runtime.snapshot_reader._lifecycle.locked()):
                expected = {row["memory_id"] for message_id in (other_seed, other_tick)
                            for row in raw.store.results(other, message_id)
                            if row["target_domain"] == "vector"}
                hits = await runtime.snapshot_reader.search_cosine_hits(other_partition, "acceptance-semantic-key")
                if {memory_id for memory_id, _ in hits} == expected:
                    break
            await asyncio.sleep(.05)
        else:
            raise AssertionError("independent scope did not publish actual new vector evidence")
        assert 1 <= len(changes) < 6
        assert runtime.health()["semantic_state"] == "preparing"
        result = await runtime.retrieval.search_with_status(other, "acceptance-semantic-key", top_k=100)
        assert result.status == "ready" and not result.reason_codes
        hit_proof(raw, other, result.items, {other_seed, other_tick})
        for message_id in (other_seed, other_tick):
            provenance(raw, runtime, other, message_id)
        for message_id in (busy_seed, busy_tick):
            await settled(raw, runtime, busy, message_id)
        assert len(changes) == 6
        for message_id in changes:
            await settled(raw, runtime, busy, message_id)
            provenance(raw, runtime, busy, message_id)
        result = await runtime.retrieval.search_with_status(busy, "acceptance-semantic-key", top_k=100)
        hit_proof(raw, busy, result.items, {busy_seed, busy_tick, *changes})
        assert result.status == "ready" and not result.reason_codes


@pytest.mark.asyncio
async def test_close_drains_inflight_verification_before_releasing_owners(tmp_path, monkeypatch):
    """Normal close waits for the actual capture thread; no owner is force-killed."""
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, seed_id = await ingress(raw, bus, "虚构关闭核验种子")
        await settled(raw, runtime, scope, seed_id)
        capture = runtime.vector.snapshot_vectors
        entered, release = threading.Event(), threading.Event()
        raw_closed = asyncio.Event()
        close_raw = raw.close
        closing = None

        async def observed_raw_close():
            result = await close_raw()
            raw_closed.set()
            return result

        def blocked_capture(partition):
            rows = capture(partition)
            entered.set()
            assert release.wait(6), "synthetic verification release not received"
            return rows

        monkeypatch.setattr(runtime.vector, "snapshot_vectors", blocked_capture)
        monkeypatch.setattr(raw, "close", observed_raw_close)
        try:
            _, message_id = await ingress(raw, bus, "虚构关闭期间原文不丢")
            assert await asyncio.to_thread(entered.wait, 3)
            closing = asyncio.create_task(runtime.close())
            # Python 3.10 has no Task.cancelling(). Observe real Raw closure;
            # _close_parts then cancels the publisher before awaiting its drain.
            await asyncio.wait_for(raw_closed.wait(), 3)
            await asyncio.sleep(.01)
            assert not runtime._semantic_task.done()
            assert not closing.done()
            assert runtime.health()["derived_state"] == "stopping"
            release.set()
            await asyncio.wait_for(asyncio.shield(closing), 8)
            assert runtime.health()["state"] == "stopped"
            assert runtime.raw.settlement_status != "unsettled"
            assert runtime.vector.worker_pid is None
            assert runtime.snapshot_reader.worker_pid is None
            assert raw.store.get(scope, message_id).content == "虚构关闭期间原文不丢"
        finally:
            release.set()
            if closing is not None:
                await asyncio.gather(closing)


@pytest.mark.asyncio
async def test_privacy_1(tmp_path, request):
    """A private source cannot leak through a configured group or foreign owner."""
    from kernel.raw_message_profile import ConnectorBinding, ConversationBinding

    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
        derivations=daily_specs("synthetic-acceptance"), connectors=(ConnectorBinding(
            "test-agent", "web", "web-test", "synthetic-account", (
                ConversationBinding("private", "private-id", "dm", "虚构私聊"),
                ConversationBinding("group", "group-id", "group", "虚构群聊"))),))
    async with daily(tmp_path, settings=settings,
                     cleanup_observation=request.node.user_properties) as (raw, bus, runtime):
        private, message_id = await ingress(raw, bus, "虚构隐私暗号是琥珀橙", chat="private")
        await settled(raw, runtime, private, message_id)
        group = next(scope for scope in raw.configured_scopes() if scope != private)
        hits = await runtime.retrieval.search(private, "acceptance-semantic-key")
        hit_proof(raw, private, hits, {message_id})
        provenance(raw, runtime, private, message_id)
        for forbidden in (group, replace(private, user_id="foreign-owner")):
            assert raw.store.get(forbidden, message_id) is None
            assert await runtime.retrieval.search(forbidden, "琥珀橙") == []
            assert await runtime.retrieval.search(forbidden, "acceptance-semantic-key") == []
            with pytest.raises(AssertionError):
                provenance(raw, runtime, forbidden, message_id)


@pytest.mark.asyncio
async def test_forgetting_1(tmp_path):
    """Forget removes current proof; restore retains exactly the same derivations."""
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "虚构航班偏好是靠窗")
        tasks = await settled(raw, runtime, scope, message_id)
        _, before = provenance(raw, runtime, scope, message_id)
        raw.store.forget(scope, message_id)
        assert raw.store.get(scope, message_id) is None
        assert raw.store.results(scope, message_id) == []
        assert await runtime.retrieval.search(scope, "acceptance-semantic-key") == []
        with pytest.raises(AssertionError):
            provenance(raw, runtime, scope, message_id)
        raw.store.restore(scope, message_id)
        _, after = provenance(raw, runtime, scope, message_id)
        assert after == before
        assert raw.store.list_derivations(scope, message_id) == tasks
        hit_proof(raw, scope, await runtime.retrieval.search(
            scope, "acceptance-semantic-key"), {message_id})


def append_history(raw, scope, message_id, content, timestamp, session):
    """Approved public storage path for explicit history, not ordinary chat ingress."""
    return raw.store.append(scope, MessageDraft(
        message_id=message_id, conversation_id=scope.conversation_id,
        session_id=session, role="user", content=content, content_type="text",
        created_at=timestamp, source="web", sender_id="synthetic-owner",
        derivations=raw.profile.derivations,
    ))


@pytest.mark.asyncio
async def test_refusal_1(tmp_path):
    """Similar evidence is not an unsupported fact or a final-model refusal."""
    content = "虚构角色喜欢蓝色"
    extractor = SyntheticExtractor({content: {
        "subject": "虚构角色", "attribute": "颜色偏好", "value": "蓝色"}})
    async with daily(tmp_path, extractor=extractor) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, content)
        await settled(raw, runtime, scope, message_id)
        _, results = provenance(raw, runtime, scope, message_id)
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        records = runtime.stores.evidence(partition).get_many(
            tuple(item["memory_id"] for item in results))
        facts = [record["payload"] for record in records.values() if record["domain"] == "entity"]
        assert facts == [{"subject": "虚构角色", "attribute": "颜色偏好", "value": "蓝色"}]
        unknown_hits = await runtime.retrieval.search(scope, "生日")
        assert all("生日" not in item["source_content"] for item in unknown_hits)
        assert all(fact["attribute"] != "生日" for fact in facts)
        similar = await runtime.retrieval.search(scope, "acceptance-semantic-key")
        hit_proof(raw, scope, similar, {message_id})
        with pytest.raises(AssertionError):
            assert any(fact["attribute"] == "生日" for fact in facts)


@pytest.mark.asyncio
async def test_multi_1(tmp_path):
    """Three fields across three real sessions; latest source, not LLM composition."""
    # Identical vectors within a field exercise chronology, not synthetic hash semantics.
    extractor = SyntheticExtractor(vectors={
        f"{field}记忆值是{field}{version}": f"{field}记忆值"
        for field in ("A", "B", "C") for version in (1, 2, 3)})
    observation = (PublicationObservation()
                   if os.environ.get("AOAO_EARLY_CASES_DIAGNOSTICS") == "1" else None)
    async with daily(tmp_path, extractor=extractor, observation=observation) as (raw, _, runtime):
        scope = raw.configured_scopes()[0]
        originals = []
        if observation is not None:
            observation.mark_batch()
        for version in (1, 2, 3):
            for field in ("A", "B", "C"):
                originals.append(append_history(
                    raw, scope, f"history-{field}-{version}", f"{field}记忆值是{field}{version}",
                    f"2026-09-{version + 20:02d}T12:00:00+08:00", f"session-{version}"))
        for original in originals:
            try:
                await settled(raw, runtime, scope, original.message_id)
            except AssertionError:
                if observation is not None:
                    await observation.after_deadline(runtime, scope, original.message_id)
                raise
            provenance(raw, runtime, scope, original.message_id)
        replay = raw.store.replay(scope, scope.conversation_id)
        assert [message.message_id for message in replay] == [m.message_id for m in originals]
        assert [message.ordinal for message in replay] == list(range(1, 10))
        for version in (1, 2, 3):
            session = raw.store.replay(scope, scope.conversation_id, session_id=f"session-{version}")
            assert [message.message_id for message in session] == [
                f"history-{field}-{version}" for field in ("A", "B", "C")]
        for field in ("A", "B", "C"):
            hits = await runtime.retrieval.search(scope, f"{field}记忆值", top_k=1)
            hit_proof(raw, scope, hits, {f"history-{field}-3"})
            with pytest.raises(AssertionError):
                hit_proof(raw, scope, hits, {f"history-{field}-1"})


@pytest.mark.asyncio
async def test_time_range_1(tmp_path):
    """Timezone-aware [start,end) filter, including exact lower/upper boundaries."""
    times = {
        "before": "2026-09-23T23:59:59+08:00",
        "lower": "2026-09-24T00:00:00+08:00",
        "inside": "2026-09-23T17:00:00+00:00",
        "upper": "2026-09-24T02:00:00+08:00",
    }
    async with daily(tmp_path) as (raw, _, runtime):
        scope = raw.configured_scopes()[0]
        for message_id, timestamp in times.items():
            append_history(raw, scope, message_id, f"虚构时间窗记录 {message_id}", timestamp, "window-session")
        for message_id in times:
            await settled(raw, runtime, scope, message_id)
        start = datetime.fromisoformat(times["lower"]).timestamp()
        end = datetime.fromisoformat(times["upper"]).timestamp()
        hits = await runtime.retrieval.search(scope, "虚构时间窗记录", top_k=100,
                                               start_ts=start, end_ts=end)
        hit_proof(raw, scope, hits, {"lower", "inside"})
        with pytest.raises(AssertionError):
            hit_proof(raw, scope, hits, {"lower", "inside", "upper"})
        for item in hits:
            assert start <= datetime.fromisoformat(item["source_time"]).timestamp() < end
            provenance(raw, runtime, scope, item["source_message_id"])


@pytest.mark.asyncio
async def test_supersede_1(tmp_path):
    """Public edit supersedes revision one and survives a full isolated restart."""
    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                       derivations=daily_specs("synthetic-acceptance"))
    async with daily(tmp_path, settings=settings) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "虚构出行方式是汽车")
        await settled(raw, runtime, scope, message_id)
        _, old_results = provenance(raw, runtime, scope, message_id)
        raw.store.edit(scope, message_id, "虚构出行方式改成高铁", expected_revision=1,
                       edited_at=datetime.now(timezone.utc).isoformat())
        await settled(raw, runtime, scope, message_id, revision=2)
        current, new_results = provenance(raw, runtime, scope, message_id, revision=2)
        assert current.content == "虚构出行方式改成高铁"
        assert {item["memory_id"] for item in old_results}.isdisjoint(
            item["memory_id"] for item in new_results)
        assert all(item["revision"] == 2 for item in new_results)
        with pytest.raises(AssertionError):
            provenance(raw, runtime, scope, message_id, revision=1)
    async with daily(tmp_path, settings=settings) as (raw, _, runtime):
        await settled(raw, runtime, scope, message_id, revision=2)
        current, restored_results = provenance(raw, runtime, scope, message_id, revision=2)
        assert current.content == "虚构出行方式改成高铁"
        assert restored_results == new_results
        hits = await runtime.retrieval.search(scope, "acceptance-semantic-key")
        hit_proof(raw, scope, hits, {message_id}, revision=2)


@pytest.mark.asyncio
async def test_conflict_1(tmp_path):
    """Contradictory original statements stay distinct; no fabricated resolution."""
    first = "虚构饮品偏好：只喝咖啡"
    second = "虚构饮品偏好：从不喝咖啡"
    extractor = SyntheticExtractor({
        first: {"subject": "虚构角色", "attribute": "饮品偏好", "value": "只喝咖啡"},
        second: {"subject": "虚构角色", "attribute": "饮品偏好", "value": "从不喝咖啡"}})
    async with daily(tmp_path, extractor=extractor) as (raw, bus, runtime):
        scope, first_id = await ingress(raw, bus, first)
        _, second_id = await ingress(raw, bus, second)
        for message_id in (first_id, second_id):
            await settled(raw, runtime, scope, message_id)
            provenance(raw, runtime, scope, message_id)
        original_one = raw.store.get(scope, first_id)
        original_two = raw.store.get(scope, second_id)
        assert original_one.content == first and original_two.content == second
        assert original_one.created_at < original_two.created_at
        hits = await runtime.retrieval.search(scope, "饮品偏好", top_k=100, neighbors=1)
        hit_proof(raw, scope, hits, {first_id, second_id})
        assert {item["source_content"] for item in hits} == {first, second}
        with pytest.raises(AssertionError):
            hit_proof(raw, scope, hits, {second_id})
        replay = raw.store.replay(scope, scope.conversation_id)
        assert [(item.message_id, item.content) for item in replay] == [(first_id, first), (second_id, second)]


@pytest.mark.asyncio
async def test_third_1(tmp_path):
    """A real channel-original envelope retains sender, not the scope owner as author."""
    from kernel.raw_message_profile import ConnectorBinding, ConversationBinding
    from kernel.raw_message_service import ChannelOriginal

    original_text = '{"text":"虚构朋友说他养了一只猫"}'
    extractor = SyntheticExtractor({original_text: {
        "subject": "fictional-friend", "attribute": "宠物", "value": "猫"}})
    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
        derivations=daily_specs("synthetic-acceptance"), connectors=(ConnectorBinding(
            "test-agent", "feishu", "feishu-test", "synthetic-account", (
                ConversationBinding("fictional-group", "fictional-group", "group", "虚构群"),)),))
    async with daily(tmp_path, settings=settings, extractor=extractor) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "模型信封不是原文", source="feishu",
            chat="fictional-group", raw_original=ChannelOriginal(
                content=original_text, sender_id="fictional-friend",
                provider_message_id="fictional-provider-message",
                provider_conversation_id="fictional-group", created_at="2026-09-24T15:00:00+08:00"))
        await settled(raw, runtime, scope, message_id)
        original, results = provenance(raw, runtime, scope, message_id)
        assert original.content == original_text
        assert original.sender_id == "fictional-friend" != original.user_id
        assert original.source == "feishu"
        assert original.metadata["provider_message_id"] == "fictional-provider-message"
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        facts = [runtime.stores.evidence(partition).get(item["memory_id"])["payload"]
                 for item in results if item["target_domain"] == "entity"]
        assert facts == [{"subject": "fictional-friend", "attribute": "宠物", "value": "猫"}]
        with pytest.raises(AssertionError):
            assert original.sender_id == original.user_id
        hit_proof(raw, scope, await runtime.retrieval.search(scope, "虚构朋友"), {message_id})


@pytest.mark.asyncio
async def test_reflection_1(tmp_path):
    """Outbox failure/restart/retry; explicitly not the legacy ReflectionDaemon."""
    content = "虚构故障恢复记录"
    fact = {content: {"subject": "虚构角色", "attribute": "恢复记录", "value": "原文保留"}}

    class FailingVectorBoundary(SyntheticExtractor):
        async def extract(self, domain, message, source, max_output_tokens):
            if domain == "vector":
                raise OSError("synthetic extraction boundary failure")
            return await super().extract(domain, message, source, max_output_tokens)

    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived",
                       derivations=daily_specs("synthetic-acceptance"))
    async with daily(tmp_path, settings=settings, extractor=FailingVectorBoundary(fact)) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, content)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            tasks = raw.store.list_derivations(scope, message_id)
            failed = [task for task in tasks if task["target_domain"] == "vector"
                      and task["last_error_code"] == "processor_error"]
            others = [task for task in tasks if task["target_domain"] != "vector"]
            if failed and len(others) == 5 and all(task["status"] == "succeeded" for task in others):
                break
            await asyncio.sleep(.02)
        else:
            raise AssertionError("partial success and persisted extraction failure not observed")
        assert failed[0]["status"] == "pending"
        assert 1 <= failed[0]["attempts"] < 3
        original, partial = provenance(raw, runtime, scope, message_id)
        assert original.content == content
        with pytest.raises(AssertionError):
            assert all(task["status"] == "succeeded" for task in tasks)
    async with daily(tmp_path, settings=settings, extractor=SyntheticExtractor(fact)) as (raw, _, runtime):
        finished = await settled(raw, runtime, scope, message_id)
        assert {task["task_id"] for task in finished} == {task["task_id"] for task in tasks}
        vector_task = next(task for task in finished if task["target_domain"] == "vector")
        assert vector_task["attempts"] > failed[0]["attempts"]
        original, results = provenance(raw, runtime, scope, message_id)
        assert original.content == content
        assert {item["memory_id"] for item in partial}.issubset(item["memory_id"] for item in results)
        assert len(results) == 2
        assert raw.worker is not None
        assert await raw.worker.run_once() == 0
        assert raw.store.results(scope, message_id) == results
        hit_proof(raw, scope, await runtime.retrieval.search(
            scope, "acceptance-semantic-key"), {message_id})


@pytest.mark.asyncio
async def test_agent_1(tmp_path):
    """Identical messages in two chats and four individually changed scope fields."""
    from kernel.raw_message_profile import ConversationBinding

    base = profile(tmp_path / "raw")
    settings = replace(base, derived_root=tmp_path / "derived",
        derivations=daily_specs("synthetic-acceptance"), connectors=(replace(
            base.connectors[0], conversations=(
                ConversationBinding("one", "one-id", "web", "同名聊天"),
                ConversationBinding("two", "two-id", "web", "同名聊天"))),))
    async with daily(tmp_path, settings=settings) as (raw, bus, runtime):
        saved = [await ingress(raw, bus, "完全相同的虚构消息", chat=chat) for chat in ("one", "two")]
        assert saved[0][1] != saved[1][1]
        for scope, message_id in saved:
            await settled(raw, runtime, scope, message_id)
        for scope, message_id in saved:
            provenance(raw, runtime, scope, message_id)
            hit_proof(raw, scope, await runtime.retrieval.search(
                scope, "acceptance-semantic-key"), {message_id})
            for field in ("user_id", "agent_id", "connector_id", "conversation_id"):
                forbidden = replace(scope, **{field: "foreign"})
                assert raw.store.get(forbidden, message_id) is None
                assert raw.store.results(forbidden, message_id) == []
                assert await runtime.retrieval.search(forbidden, "acceptance-semantic-key") == []
                with pytest.raises(AssertionError):
                    provenance(raw, runtime, forbidden, message_id)


@pytest.mark.asyncio
async def test_populated_identity_groups_are_bilaterally_isolated(tmp_path):
    """Legitimate full identities share a real DB, not five empty databases."""
    from kernel.raw_message_profile import ConnectorBinding, ConversationBinding

    groups = (("U1", "A1", "C1", "K1", "P1"),
              ("U2", "A1", "C2", "K1", "P1"),
              ("U1", "A2", "C3", "K1", "P1"),
              ("U1", "A1", "C4", "K4", "P1"),
              ("U1", "A1", "C1", "K1", "P2"))
    configurations = [replace(profile(tmp_path / "raw"), owner_id=owner,
        derived_root=tmp_path / "derived", derivations=daily_specs("synthetic-acceptance"),
        connectors=(ConnectorBinding(agent, "web", connector, account, (
            ConversationBinding("web_console", provider, "web", "同名虚构会话"),)),))
        for owner, agent, connector, account, provider in groups]
    content = "相同的虚构隔离证据"
    extractor = SyntheticExtractor({content: {
        "subject": "虚构角色", "attribute": "隔离证据", "value": "同文不同来源"}})
    saved = []
    for config in configurations:
        async with daily(tmp_path, settings=config, extractor=extractor) as (raw, bus, runtime):
            scope, message_id = await ingress(raw, bus, content)
            await settled(raw, runtime, scope, message_id)
            provenance(raw, runtime, scope, message_id)
            saved.append((scope, message_id))
    assert len({scope for scope, _ in saved}) == len({key for _, key in saved}) == 5
    for config, (scope, message_id) in zip(configurations, saved, strict=True):
        async with daily(tmp_path, settings=config, extractor=extractor) as (raw, bus, runtime):
            assert raw.configured_scopes() == (scope,)
            # Test-admin proof of populated fixtures, not an HTTP authorization claim.
            assert all(raw.store.get(other_scope, other_id) is not None
                       for other_scope, other_id in saved)
            await settled(raw, runtime, scope, message_id)
            provenance(raw, runtime, scope, message_id)
            result = await runtime.retrieval.search_with_status(scope, "acceptance-semantic-key", top_k=100)
            assert result.status == "ready" and not result.reason_codes
            hit_proof(raw, scope, result.items, {message_id})
            assert raw.store.replay(scope, scope.conversation_id)[0].content == content
            for other_scope, other_id in saved:
                if other_scope != scope:
                    assert raw.store.get(scope, other_id) is None
                    assert raw.store.results(scope, other_id) == []
                    with pytest.raises(AssertionError):
                        provenance(raw, runtime, scope, other_id)
            empty = await runtime.retrieval.search_with_status(
                scope, "acceptance-semantic-key", start_ts=4102444800)
            assert empty.status == "ready" and not empty.items and not empty.reason_codes
            rejected = (
                (Message.user_input("虚构拒绝Agent", "web", chat_name="web_console"), "unknown-agent"),
                (Message.user_input("虚构拒绝会话", "web", chat_name="unknown-chat"), scope.agent_id),
                (Message.user_input("虚构冲突provider", "web", chat_name="web_console",
                    raw_original=ChannelOriginal(content="虚构冲突原文",
                        provider_message_id="synthetic-rejected",
                        provider_conversation_id="unconfigured-provider")), scope.agent_id),
            )
            for rejected_message, agent_id in rejected:
                before = source_inventory(raw, saved)
                with pytest.raises(RawMessageError):
                    await bus.prepare_input(rejected_message, agent_id=agent_id)
                assert source_inventory(raw, saved) == before
                for retained_scope, retained_id in saved:
                    tasks = raw.store.list_derivations(retained_scope, retained_id)
                    assert len(tasks) == 6 and all(task["status"] == "succeeded" for task in tasks)
                provenance(raw, runtime, scope, message_id)


@pytest.mark.asyncio
async def test_internal_scope_status_does_not_stand_in_for_ingress_authorization(tmp_path):
    """A ready empty internal query is not a grant to an external identity."""
    async with daily(tmp_path) as (raw, bus, runtime):
        scope, message_id = await ingress(raw, bus, "虚构Scope语义证据")
        await settled(raw, runtime, scope, message_id)
        before = source_inventory(raw, ((scope, message_id),))
        unknown = await runtime.retrieval.search_with_status(
            replace(scope, agent_id="unknown-agent"), "acceptance-semantic-key")
        assert unknown.status == "ready" and not unknown.items and not unknown.reason_codes
        with pytest.raises(ValueError, match="complete read scope required"):
            await runtime.retrieval.search_with_status(
                replace(scope, connector_id=None), "acceptance-semantic-key")
        with pytest.raises(ValueError, match="complete read scope required"):
            await runtime.retrieval.search_with_status(
                replace(scope, conversation_id=None), "acceptance-semantic-key")
        with pytest.raises(RawMessageError):
            await bus.prepare_input(
                Message.user_input("虚构未授权输入", "web", chat_name="web_console"),
                agent_id="unknown-agent")
        assert source_inventory(raw, ((scope, message_id),)) == before
        provenance(raw, runtime, scope, message_id)


@pytest.mark.asyncio
async def test_single_1(tmp_path):
    """Lossless AppBus saved receipt, six jobs, and real Raw/Ledger reconciliation."""
    content = '虚构验收：原文\n第二行  空格\t"引号"与 café。'
    async with daily(tmp_path) as (raw, bus, runtime):
        saved = await bus.accept_input(Message.user_input(content, "web", chat_name="web_console"))
        message_id = saved.get("origin_message_id")
        scope = raw.configured_scopes()[0]
        assert message_id
        assert raw.store.get(scope, message_id).content == content
        tasks = await settled(raw, runtime, scope, message_id)
        assert {task["target_domain"] for task in tasks} == {
            "vector", "entity", "graph", "rule", "journal", "schedule"}
        with pytest.raises(AssertionError):
            provenance(raw, runtime, scope, "deliberately-wrong-source")
        original, results = provenance(raw, runtime, scope, message_id)
        assert original.content == content
        assert len(results) == 1
        hits = await runtime.retrieval.search(scope, "acceptance-semantic-key")
        assert message_id in {item["source_message_id"] for item in hits}
        assert runtime.health()["reader_pid"] != runtime.health()["vector_pid"]
