"""Online publication contracts, using isolated real Raw/Evidence/Ledger stores."""

import asyncio
from contextlib import contextmanager
from functools import wraps
import json
import pytest
import sqlite3
import threading
import time

from kernel.raw_message_models import RawMessageError

from kernel.app_bus import AppBus
from kernel.raw_message_models import DerivationSpec, Scope
from kernel.raw_message_retrieval import RawMessageRetrieval
from kernel.raw_message_worker import RawMessageWorker
from system_memory_god.derivation import DailyDerivation
from system_memory_god.storage.scoped_stores import ScopedStores
from tests.test_raw_message_store import draft, opened


class _ParallelObservation:
    """Test-only, bounded scalar timeline; never retain message/task/error objects."""

    _modules = {"apps/system_memory_god/derivation.py": "derivation",
                "apps/system_memory_god/storage/partition_registry.py": "partition_registry",
                "apps/system_memory_god/storage/daily_vector_index.py": "daily_vector_index",
                "apps/system_memory_god/storage/supervised_daily_vector.py": "supervised_daily_vector",
                "kernel/raw_message_worker.py": "raw_message_worker",
                "kernel/app_bus.py": "app_bus"}
    _phases = {"derive", "process", "extract", "close", "registry.open", "registry.begin",
               "runs.open", "runs.get"} | {
                   prefix + name for prefix in ("sql.", "dispatch.")
                   for name in ("claim_partition", "renew", "get", "complete", "partition_state", "fail")}

    def __init__(self, capacity=128):
        if type(capacity) is not int or not 1 <= capacity <= 128:
            raise ValueError("invalid observation capacity")
        self.capacity = capacity
        self._started = time.monotonic()
        self._lock = threading.Lock()
        self._events = []
        self._dropped = 0

    def record(self, phase, slot, stage, *, error=None):
        row = {"phase": phase if type(phase) is str and phase in self._phases else "other",
               "slot": slot if type(slot) is int and slot in {0, 1, 2} else 0,
               "stage": stage if type(stage) is str and stage in {
                   "start", "done", "cancelled", "timeout", "error", "accepted", "empty",
                   "entered", "released"} else "other",
               "at_ms": round(min(999999999., max(0., (time.monotonic() - self._started) * 1000)), 3)}
        if error is not None:
            chain, current = [], error
            for _ in range(2):
                if current is None:
                    break
                stops, trace = [], current.__traceback__
                for _ in range(64):
                    if trace is None:
                        break
                    filename = trace.tb_frame.f_code.co_filename.replace("\\", "/")
                    module = next((name for suffix, name in self._modules.items()
                                   if filename.endswith("/" + suffix)), None)
                    if module is not None and 0 < trace.tb_lineno <= 1000000:
                        stops.append({"module": module, "line": trace.tb_lineno})
                        stops = stops[-2:]
                    trace = trace.tb_next
                chain.append({"type": type(current).__name__ if type(current) in {
                    ValueError, RuntimeError, OSError, TimeoutError, MemoryError,
                    RawMessageError, asyncio.CancelledError} else "OtherError", "stops": stops})
                current = current.__cause__ or (None if current.__suppress_context__ else current.__context__)
            row["errors"] = chain
        with self._lock:
            if len(self._events) < self.capacity:
                self._events.append(row)
            else:
                self._dropped = min(1000000, self._dropped + 1)

    def _attempt_record(self, *args, **kwargs):
        try:
            self.record(*args, **kwargs)
        except BaseException:
            # Test observations must not replace a business result or exception.
            pass

    def snapshot(self):
        with self._lock:
            return {"events": list(self._events), "dropped": self._dropped}

    @staticmethod
    def slot(args):
        scope = args[0] if args else None
        return {"group-1": 1, "group-2": 2}.get(getattr(scope, "conversation_id", None), 0)

    @contextmanager
    def span(self, phase, slot):
        self._attempt_record(phase, slot, "start")
        try:
            yield
        except BaseException as error:
            stage = ("cancelled" if isinstance(error, asyncio.CancelledError) else
                     "timeout" if isinstance(error, TimeoutError) else "error")
            self._attempt_record(phase, slot, stage, error=error)
            raise
        else:
            self._attempt_record(phase, slot, "done")

    def sync(self, phase, operation, *, slot=None):
        @wraps(operation)
        def observed(*args, **kwargs):
            selected = self.slot(args) if slot is None else slot
            with self.span(phase, selected):
                result = operation(*args, **kwargs)
                if phase == "sql.claim_partition":
                    self._attempt_record(phase, selected, "accepted" if result is not None else "empty")
                return result
        return observed

    def asynchronous(self, phase, operation, *, slot=None):
        @wraps(operation)
        async def observed(*args, **kwargs):
            selected = self.slot(args) if slot is None else slot
            with self.span(phase, selected):
                return await operation(*args, **kwargs)
        return observed


def _observe_parallel_io(monkeypatch, worker, bus, stores, observation):
    for name in ("claim_partition", "renew", "get", "complete", "partition_state", "fail"):
        monkeypatch.setattr(worker.store, name,
                            observation.sync("sql." + name, getattr(worker.store, name)))
    original_io = worker._raw_io

    async def raw_io(operation, scope, *args, **kwargs):
        with observation.span("dispatch." + operation.__name__, observation.slot((scope,))):
            return await original_io(operation, scope, *args, **kwargs)

    monkeypatch.setattr(worker, "_raw_io", raw_io)
    monkeypatch.setattr(bus, "derive_raw_message",
                        observation.asynchronous("derive", bus.derive_raw_message))
    registry_factory, runs_factory = stores.partition_registry, stores.runs

    def registry():
        with observation.span("registry.open", 0):
            original = registry_factory()
            monkeypatch.setattr(original, "begin",
                                observation.sync("registry.begin", original.begin))
            return original

    def runs(scope):
        selected = observation.slot((scope,))
        with observation.span("runs.open", selected):
            original = runs_factory(scope)
            monkeypatch.setattr(original, "get",
                                observation.sync("runs.get", original.get, slot=selected))
            return original

    monkeypatch.setattr(stores, "partition_registry", registry)
    monkeypatch.setattr(stores, "runs", runs)


def _parallel_checkpoint(worker, run, active, peak, entered, release):
    known_errors = {"", "timeout", "invalid_output", "processor_error", "unavailable",
                    "active", "draining", "raw_io", "draining_failed"}
    return {"entered": entered.is_set(), "release": release.is_set(),
            "active_slots": len(active), "peak": peak, "run_done": run.done(),
            "run_cancelled": run.cancelled(), "raw_writes": len(worker._raw_writes),
            "worker_error": worker.last_error if worker.last_error in known_errors else "other",
            "settlement": worker.settlement_status}


async def _finish_parallel_observation(worker, run, observation, checkpoint, before):
    completed = None
    try:
        completed = await run
        return completed
    finally:
        try:
            with observation.span("close", 0):
                await worker.close()
        finally:
            print("R7_CONCURRENCY_TRACE " + json.dumps({
                "schema": 1, "wait_seconds": 2, "before_release": before,
                "after_close": checkpoint(), "completed": completed, **observation.snapshot(),
            }, separators=(",", ":")))


class TeaExtractor:
    async def extract(self, domain, message, source, max_output_tokens):
        return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)


def test_parallel_observation_keeps_finite_events():
    observation = _ParallelObservation(capacity=2)
    for _ in range(5):
        observation.record("sql.get", 1, "start")
    snapshot = observation.snapshot()
    assert len(snapshot["events"]) == 2
    assert snapshot["dropped"] == 3
    assert all(row["phase"] == "sql.get" and row["slot"] == 1
               for row in snapshot["events"])


def test_parallel_observation_preserves_error_without_private_text():
    import json

    observation = _ParallelObservation()
    failure = ValueError("synthetic private text")

    def operation():
        raise failure

    with pytest.raises(ValueError) as caught:
        observation.sync("sql.get", operation, slot=1)()
    assert caught.value is failure
    snapshot = observation.snapshot()
    assert [row["stage"] for row in snapshot["events"]] == ["start", "error"]
    assert "synthetic private text" not in json.dumps(snapshot)


@pytest.mark.parametrize("capacity", [0, 129])
def test_parallel_observation_rejects_unbounded_configuration(capacity):
    with pytest.raises(ValueError, match="^invalid observation capacity$"):
        _ParallelObservation(capacity=capacity)


@pytest.mark.asyncio
async def test_parallel_observer_fault_cannot_replace_result_or_original_error(monkeypatch):
    observation = _ParallelObservation()
    failure = ValueError("private-original-sentinel")

    def broken(*args, **kwargs):
        raise RuntimeError("private-observer-sentinel")

    def success():
        return failure

    async def failed():
        raise failure

    monkeypatch.setattr(observation, "record", broken)
    assert observation.sync("sql.get", success)() is failure
    with pytest.raises(ValueError) as caught:
        await observation.asynchronous("derive", failed)()
    assert caught.value is failure


def test_parallel_error_stops_are_whitelisted_bounded_and_text_free():
    observation = _ParallelObservation()
    namespace = {}
    exec(compile("def operation():\n try:\n  raise OSError('private-source-key')\n"
                 " except OSError as cause:\n  raise ValueError('private-body') from cause\n",
                 "C:/private-location/apps/system_memory_god/derivation.py", "exec"), namespace)
    for _ in range(100):
        with pytest.raises(ValueError):
            observation.sync("process", namespace["operation"])()
    snapshot = observation.snapshot()
    assert len(snapshot["events"]) == 128 and snapshot["dropped"] == 72
    errors = next(row["errors"] for row in snapshot["events"] if "errors" in row)
    assert [error["type"] for error in errors] == ["ValueError", "OSError"]
    assert all(error["stops"] and len(error["stops"]) <= 2 for error in errors)
    assert all(stop["module"] == "derivation" and type(stop["line"]) is int
               for error in errors for stop in error["stops"])
    encoded = json.dumps(snapshot).encode()
    assert len(encoded) <= 65536 and b"private" not in encoded and b"C:" not in encoded
    observation.record("private-phase", "private-scope", "private-stage")
    assert b"private" not in json.dumps(observation.snapshot()).encode()


@pytest.mark.asyncio
async def test_parallel_instance_observer_restores_after_repeated_failure(monkeypatch):
    from types import SimpleNamespace

    failure = ValueError("private-message")

    async def process():
        raise failure

    instance, untouched = SimpleNamespace(process=process), SimpleNamespace(process=process)
    for _ in range(2):
        observation = _ParallelObservation()
        with monkeypatch.context() as boundary:
            boundary.setattr(instance, "process", observation.asynchronous("process", instance.process))
            with pytest.raises(ValueError) as caught:
                await instance.process()
            assert caught.value is failure and untouched.process is process
        assert instance.process is process
        assert [row["stage"] for row in observation.snapshot()["events"]] == ["start", "error"]


def test_publication_proofs_are_selective_and_bounded(tmp_path):
    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    raw.append(owner, draft("dense", content="红茶原文"))
    stamp = "2026-09-18T14:00:00+08:00"
    with sqlite3.connect(raw.database) as db:
        db.executemany(
            """INSERT INTO derivation_outbox
               (task_id,message_id,revision,processor,processor_version,
                target_domain,model,prompt_hash,status,created_at,updated_at)
               VALUES (?,?,1,?,?,'entity','fake',?,'succeeded',?,?)""",
            ((f"task-{i}", "dense", "extract", f"v{i}", "a" * 64, stamp, stamp)
             for i in range(4097)),
        )
        db.executemany(
            "INSERT INTO derivation_results(task_id,memory_id,store_ref) VALUES (?,?,?)",
            ((f"task-{i}", f"memory-{i}", f"entity:{i}") for i in range(4097)),
        )
    assert raw.published_snapshots(scope, ("dense",), proofs={})["dense"][1] == ()
    selected = raw.published_snapshots(
        scope, ("dense",), proofs={"dense": (("task-4096", "memory-4096"),)}
    )
    assert [anchor["memory_id"] for anchor in selected["dense"][1]] == ["memory-4096"]
    with pytest.raises(RawMessageError, match="budget"):
        raw.published_snapshots(scope, ("dense",))


def test_publication_snapshot_limits_messages_bytes_proofs_and_deadline(tmp_path):
    import time
    from kernel.raw_message_reader import RawReadView

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    raw.append(owner, draft("large", content="x" * (1024 * 1024)))
    reader = RawReadView(raw.database, trusted_root=raw.database.parent, scope=scope)
    ids = tuple(f"missing-{index}" for index in range(500))
    assert reader.published_snapshots(scope, ids, proofs={}) == {}
    with pytest.raises(RawMessageError, match="batch"):
        reader.published_snapshots(scope, (*ids, "large"), proofs={})
    with pytest.raises(RawMessageError, match="byte budget"):
        reader.published_snapshots(scope, ("large",), proofs={})
    with pytest.raises(RawMessageError, match="proof budget"):
        reader.published_snapshots(
            scope, ("large",),
            proofs={"large": (("some-task", "some-memory"),) * 4097})
    with pytest.raises(RawMessageError, match="budget"):
        reader.published_snapshots(scope, (), proofs={}, deadline=time.monotonic() - 1)
    assert raw.published_snapshots(
        Scope("ailu", "other", "wx-main", "group-1"), ("large",), proofs={}) == {}


@pytest.mark.parametrize("reader_kind", ["borrowed", "owner"])
def test_publication_deadline_covers_locked_database_open(tmp_path, reader_kind):
    import time
    from kernel.raw_message_reader import RawReadView

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    raw.append(owner, draft("locked", content="隔离红茶原文"))
    target = (RawReadView(raw.database, trusted_root=raw.database.parent, scope=scope)
              if reader_kind == "borrowed" else raw)
    blocker = sqlite3.connect(raw.database, isolation_level=None)
    try:
        blocker.execute("PRAGMA locking_mode=EXCLUSIVE")
        blocker.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        with pytest.raises(RawMessageError, match="budget"):
            target.published_snapshots(scope, ("locked",), proofs={}, deadline=started + 0.1)
        assert time.monotonic() - started < 0.8
    finally:
        blocker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_state", ["pending", "leased", "failed"])
async def test_unrelated_work_does_not_hide_raw_or_published_evidence(tmp_path, pending_state):
    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    spec = DerivationSpec("extract", "v1", "entity", "fake", "a" * 64)
    stores = ScopedStores(tmp_path / "derived")
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_daily_derivation(DailyDerivation(stores, TeaExtractor()))
    raw.append(owner, draft("published", content="我喝茶", derivations=(spec,)))
    assert await RawMessageWorker(raw, bus, (scope,)).run_once() == 1
    raw.append(owner, draft("new-work", content="新原文", derivations=(spec,)))
    if pending_state != "pending":
        task = raw.claim(scope, "blocked-worker", now=100)
        if pending_state == "failed":
            raw.fail(scope, task["task_id"], task["lease_token"], "processor_error", now=101)
    recall = RawMessageRetrieval(raw, stores)
    evidence = await recall.search(scope, "红茶", memory_scope="portrait")
    assert [(row["domain"], row["source_message_id"]) for row in evidence] == [
        ("entity", "published")]
    originals = await recall.search(scope, "新原文")
    assert [(row["domain"], row["source_message_id"]) for row in originals] == [
        ("raw", "new-work")]


@pytest.mark.asyncio
async def test_unavailable_derived_store_preserves_raw_with_explicit_status(tmp_path):
    from kernel.memory_pipeline_models import PipelineScope

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    stores = ScopedStores(tmp_path / "derived")
    raw.append(owner, draft("original", content="原文红茶"))
    partition = PipelineScope("owner", "ailu", "wx-main", "group-1")
    registry = stores.partition_registry()
    generation = registry.begin(partition)
    registry.incomplete(partition, generation=generation)
    recall = RawMessageRetrieval(raw, stores)
    outcome = await recall.search_with_status(scope, "红茶")
    assert outcome.status == "degraded"
    assert outcome.reason_codes == ("derived_unavailable",)
    assert [item["message_id"] for item in outcome.items] == ["original"]
    raw.forget(scope, "original")
    assert (await recall.search_with_status(scope, "红茶")).items == []


@pytest.mark.asyncio
async def test_shared_owner_prepares_reads_and_accepts_more_writes(tmp_path):
    from kernel.supervised_daily_retrieval import SupervisedDailyRetrieval
    from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            return ({"text": message.content},)

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    stores = ScopedStores(tmp_path / "derived")
    vector = SupervisedDailyVector(stores, embedding="synthetic")
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_daily_derivation(DailyDerivation(stores, Extractor(), vector))
    worker = RawMessageWorker(raw, bus, (scope,))
    spec = DerivationSpec("extract", "v1", "vector", "fake", "a" * 64)
    reader = None
    try:
        raw.append(owner, draft("vector-one", content="红茶", derivations=(spec,)))
        assert await worker.run_once() == 1
        pid = vector.worker_pid
        handle = vector.reader()
        assert not hasattr(handle, "close") and not hasattr(handle, "retire_worker")
        reader = SupervisedDailyRetrieval(raw, stores, handle, shared_vector_index=True)
        assert await reader.prepare(scope)
        assert await reader.search(scope, "红茶")
        raw.append(owner, draft("vector-two", content="绿茶", derivations=(spec,)))
        assert await worker.run_once() == 1
        assert await reader.search(scope, "绿茶")
        assert vector.worker_pid == pid
        await reader.close()
        assert vector.worker_pid == pid
    finally:
        if reader is not None:
            await reader.close()
        await worker.close()
        await vector.close()


@pytest.mark.asyncio
async def test_worker_overlaps_distinct_scopes_but_not_same_scope(tmp_path, monkeypatch):
    import asyncio

    entered, release = asyncio.Event(), asyncio.Event()
    active = set()
    peak = 0
    observation = _ParallelObservation()

    class Extractor:
        async def extract(self, domain, message, source, max_output_tokens):
            nonlocal peak
            assert message.conversation_id not in active
            active.add(message.conversation_id)
            peak = max(peak, len(active))
            observation.record("extract", observation.slot((message,)), "entered")
            if len(active) == 2:
                entered.set()
            await release.wait()
            active.remove(message.conversation_id)
            observation.record("extract", observation.slot((message,)), "released")
            return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)

    raw, owner = opened(tmp_path)
    raw.register_conversation(owner, "group-2", "wx-main", "remote-2", "group", "群二")
    spec = DerivationSpec("extract", "v1", "entity", "fake", "a" * 64)
    for offset in range(4):
        raw.append(owner, draft(f"parallel-{offset}", conversation_id=f"group-{offset % 2 + 1}",
                                derivations=(spec,)))
    bus = AppBus(str(tmp_path / "apps"))
    stores = ScopedStores(tmp_path / "derived")
    bus.attach_daily_derivation(DailyDerivation(stores, Extractor()))
    scopes = tuple(Scope("ailu", "owner", "wx-main", f"group-{n}") for n in (1, 2))
    worker = RawMessageWorker(raw, bus, scopes, concurrency=2)
    _observe_parallel_io(monkeypatch, worker, bus, stores, observation)
    run = asyncio.create_task(worker.run_once())

    def checkpoint():
        return _parallel_checkpoint(worker, run, active, peak, entered, release)

    try:
        await asyncio.wait_for(entered.wait(), 2)
    finally:
        before = checkpoint()
        release.set()
        completed = await _finish_parallel_observation(worker, run, observation, checkpoint, before)
    assert peak == 2
    assert completed == 2
    assert worker.settlement_status == "settled"


@pytest.mark.asyncio
async def test_first_registry_creation_keeps_distinct_scope_worker_overlap(tmp_path, monkeypatch):
    from tests.test_memory_daily_isolation import first_registry_creation

    with first_registry_creation(monkeypatch, tmp_path / "derived") as observation:
        await test_worker_overlaps_distinct_scopes_but_not_same_scope(tmp_path, monkeypatch)
        assert observation.missing.is_set() and observation.writing.is_set()


@pytest.mark.asyncio
async def test_reader_recovers_from_terminated_child_without_foreground_restart(tmp_path):
    import asyncio
    import psutil
    from kernel.supervised_daily_retrieval import SupervisedDailyRetrieval

    raw, owner = opened(tmp_path)
    raw.append(owner, draft("original", content="恢复后的红茶"))
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    reader = SupervisedDailyRetrieval(raw, ScopedStores(tmp_path / "derived"),
                                      shared_vector_index=True)
    try:
        assert await reader.prepare(scope)
        first_pid = reader.worker_pid
        psutil.Process(first_pid).terminate()
        failed = await reader.search_with_status(scope, "红茶")
        assert failed.status == "recovering" and failed.items == []
        deadline = asyncio.get_running_loop().time() + 8
        while True:
            await asyncio.sleep(0.05)
            result = await reader.search_with_status(scope, "红茶")
            if result.status == "ready":
                break
            assert asyncio.get_running_loop().time() < deadline
        assert result.items[0]["message_id"] == "original"
        assert reader.worker_pid != first_pid
    finally:
        await reader.close()
    assert (await reader.search_with_status(scope, "红茶")).status == "stopped"
    assert reader.worker_pid is None


@pytest.mark.asyncio
async def test_online_recovery_resumes_crash_before_first_frozen_output(tmp_path):
    from kernel.memory_pipeline_models import PipelineScope

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    partition = PipelineScope("owner", "ailu", "wx-main", "group-1")
    raw.append(owner, draft("pre-extraction-crash", derivations=(
        DerivationSpec("extract", "v1", "entity", "fake", "a" * 64),)))
    stores = ScopedStores(tmp_path / "derived")
    stores.partition_registry().begin(partition)
    derivation = DailyDerivation(stores, TeaExtractor())
    assert await derivation.recover_partition(partition, raw, online=True)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_daily_derivation(derivation)
    assert await RawMessageWorker(raw, bus, (scope,)).run_once() == 1
    assert await RawMessageRetrieval(raw, stores).search(scope, "红茶", memory_scope="portrait")


@pytest.mark.asyncio
async def test_busy_scope_does_not_hold_other_scopes_next_runnable_task(tmp_path):
    import asyncio

    slow_started, release, fast_second = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Extractor:
        fast_count = 0

        async def extract(self, domain, message, source, max_output_tokens):
            if message.conversation_id == "group-1":
                slow_started.set()
                await release.wait()
            else:
                self.fast_count += 1
                if self.fast_count == 2:
                    fast_second.set()
            return ({"subject": "用户", "attribute": "饮品", "value": "红茶"},)

    raw, owner = opened(tmp_path)
    raw.register_conversation(owner, "group-2", "wx-main", "remote-2", "group", "群二")
    spec = DerivationSpec("extract", "v1", "entity", "fake", "a" * 64)
    raw.append(owner, draft("slow", derivations=(spec,)))
    for n in range(2):
        raw.append(owner, draft(f"fast-{n}", conversation_id="group-2", derivations=(spec,)))
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_daily_derivation(DailyDerivation(ScopedStores(tmp_path / "derived"), Extractor()))
    scopes = tuple(Scope("ailu", "owner", "wx-main", f"group-{n}") for n in (1, 2))
    worker = RawMessageWorker(raw, bus, scopes, concurrency=2)
    worker.start()
    try:
        await asyncio.wait_for(slow_started.wait(), 3)
        await asyncio.wait_for(fast_second.wait(), 3)
    finally:
        release.set()
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["task_state", "model", "prompt_hash", "ledger_status", "store_ref"])
async def test_publication_rejects_mismatched_proof(tmp_path, corruption):
    import json
    import sqlite3
    from kernel.memory_pipeline_models import PipelineScope

    raw, owner = opened(tmp_path)
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    stores = ScopedStores(tmp_path / "derived")
    raw.append(owner, draft("proof", content="原话", derivations=(
        DerivationSpec("extract", "v1", "entity", "fake", "a" * 64),)))
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_daily_derivation(DailyDerivation(stores, TeaExtractor()))
    assert await RawMessageWorker(raw, bus, (scope,)).run_once() == 1
    recall = RawMessageRetrieval(raw, stores)
    assert await recall.search(scope, "红茶", memory_scope="portrait")
    if corruption == "ledger_status":
        partition = PipelineScope("owner", "ailu", "wx-main", "group-1")
        with sqlite3.connect(stores.partition(partition) / "source_ledger.db") as db:
            item = json.loads(db.execute("SELECT item_json FROM memory_items").fetchone()[0])
            item["status"] = "forgotten"
            db.execute("UPDATE memory_items SET item_json=?", (json.dumps(item),))
    else:
        with sqlite3.connect(raw.database) as db:
            if corruption == "task_state":
                db.execute("UPDATE derivation_outbox SET status='pending'")
            elif corruption == "store_ref":
                db.execute("UPDATE derivation_results SET store_ref='evidence:forged'")
            elif corruption == "model":
                db.execute("UPDATE derivation_outbox SET model='different-model'")
            else:
                db.execute("UPDATE derivation_outbox SET prompt_hash=?", ("b" * 64,))
    assert await recall.search(scope, "红茶", memory_scope="portrait") == []


@pytest.mark.asyncio
async def test_reader_recovery_is_bounded_and_shutdown_cancels_backoff(tmp_path, monkeypatch):
    import asyncio
    from kernel.supervised_daily_retrieval import SupervisedDailyRetrieval

    raw, owner = opened(tmp_path)
    raw.append(owner, draft("recovery-limit", content="原文"))
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    reader = SupervisedDailyRetrieval(raw, ScopedStores(tmp_path / "derived"))
    calls = 0

    async def cannot_start():
        nonlocal calls
        calls += 1
        raise RuntimeError("synthetic start failure")

    monkeypatch.setattr(reader, "start", cannot_start)
    try:
        assert (await reader.search_with_status(scope, "原文")).status == "recovering"
        deadline = asyncio.get_running_loop().time() + 10
        while (await reader.search_with_status(scope, "原文")).status == "recovering":
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        assert calls == 3
        for _ in range(5):
            assert (await reader.search_with_status(scope, "原文")).status == "unavailable"
        assert calls == 3
    finally:
        await reader.close()
    assert reader.worker_pid is None
