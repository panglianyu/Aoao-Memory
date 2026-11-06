"""Synthetic daily vector candidates stay inside a supervised process."""

import asyncio
import os
import threading

import psutil
import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector
from system_memory_god.storage.daily_vector_worker import _validate_request


def test_no_model_matrix_blocks_unmocked_bge_loading():
    if os.environ.get("AMC_RUN_BOUNDED_LIVE") == "1":
        pytest.skip("the explicit real gate has a separate scope")
    from system_memory_god.storage.daily_embedding_adapter import embed_bge_m3

    with pytest.raises(RuntimeError, match="real BGE loading is disabled"):
        embed_bge_m3(["unplanned-load-sentinel"])


def test_bge_adapter_uses_the_local_model_and_tokenizer_boundary(monkeypatch):
    from system_memory_god.storage import daily_embedding_adapter
    from tests._tokenizer_helper import CharacterTokenizer

    calls = []

    class FakeModel:
        tokenizer = CharacterTokenizer()

        def encode(self, texts, **options):
            assert options == {"normalize_embeddings": False, "convert_to_numpy": True,
                               "show_progress_bar": False, "prompt": ""}
            calls.append(tuple(texts))
            return [[1.0, 2.0] for _ in texts]

    model = FakeModel()
    monkeypatch.setattr(daily_embedding_adapter, "_local_bge_model", lambda: model)
    assert daily_embedding_adapter.embed_bge_m3(["红茶", "绿茶"]) == [[1.0, 2.0], [1.0, 2.0]]
    assert calls == [("红茶", "绿茶")]


def test_vector_owner_accepts_explicit_real_embedding_mode(tmp_path):
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                  embedding="bge-m3", request_timeout=30)
    assert index.embedding == "bge-m3"


@pytest.mark.asyncio
async def test_real_bge_vector_process_in_isolated_directory(tmp_path):
    if os.environ.get("AOAO_REAL_EMBEDDING_TEST") != "1":
        pytest.skip("explicit local model probe only")
    stores = ScopedStores(tmp_path / "derived")
    index = SupervisedDailyVector(stores, embedding="bge-m3", request_timeout=30)
    scope = PipelineScope("owner", "agent", "connector", "chat")
    try:
        await index.put(scope, [{"memory_id": "tea-memory",
                                 "payload": {"text": "用户喜欢喝红茶"},
                                 "payload_hash": "a" * 64}])
        assert await index.contains(scope, ("tea-memory",))
        assert (await index.receipts(scope, [{"memory_id": "tea-memory",
                                              "payload": {"text": "用户喜欢喝红茶"},
                                              "payload_hash": "a" * 64}]))[0]["embedding_dim"] == 1024
        assert (await index.search_cosine_hits(scope, "用户最喜欢什么茶", limit=1))[0][0] == (
            "tea-memory")
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_vector_owner_supports_eight_conversation_partitions(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        for number in range(8):
            scope = PipelineScope("owner", "agent", "connector", f"chat-{number}")
            memory_id = f"memory-{number}"
            await index.put(scope, [{"memory_id": memory_id,
                                     "payload": {"text": f"fact {number}"},
                                     "payload_hash": "a" * 64}])
            assert await index.contains(scope, (memory_id,))
        assert index.worker_pid is not None
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_writer_and_borrowed_reader_keep_original_wire_and_owner(tmp_path, monkeypatch):
    import json
    from multiprocessing.connection import _ConnectionBase

    scope = PipelineScope("owner", "agent", "connector", "wire-compatibility")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    original_send = _ConnectionBase.send_bytes
    frames = []

    def record_frame(connection, body, *args, **kwargs):
        frames.append(json.loads(body))
        return original_send(connection, body, *args, **kwargs)

    monkeypatch.setattr(_ConnectionBase, "send_bytes", record_frame)
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        pid = index.worker_pid
        await index.reader().warm_existing(scope)
        assert (await index.reader().search_cosine_hits(scope, "synthetic", limit=1))[0][0] == "one"
        assert await index.contains(scope, ("one",))
        assert index.worker_pid == pid and index.diagnostic() == {}
        search = next(frame for frame in frames if frame["op"] == "search_cosine_hits")
        assert set(search) == {"version", "id", "op", "scope", "query", "limit"}
        for frame in frames:
            _validate_request(frame)
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_borrowed_vector_warm_cannot_start_cold_owner(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        assert index.worker_pid is None
        with pytest.raises(RuntimeError, match="owner not prepared"):
            await index.reader().warm_existing(scope)
        assert index.worker_pid is None
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_borrowed_vector_search_cannot_recover_cold_or_other_scope(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    prepared = PipelineScope("owner", "agent", "connector", "prepared")
    cold = PipelineScope("owner", "agent", "connector", "cold")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        with pytest.raises(RuntimeError, match="owner not prepared"):
            await index.reader().search_cosine_hits(cold, "missing")
        await asyncio.sleep(1.2)
        assert index.worker_pid is None
        assert not index.recovery_active

        await index.put(prepared, [{"memory_id": "prepared_one",
                                    "payload": {"text": "prepared fact"},
                                    "payload_hash": "a" * 64}])
        pid = index.worker_pid
        index._process.terminate()
        index._process.join(timeout=3)
        with pytest.raises(RuntimeError, match="owner not prepared"):
            await index.reader().search_cosine_hits(cold, "missing")
        await asyncio.sleep(1.2)
        assert index.worker_pid == pid
        assert not index.recovery_active
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_borrowed_search_does_not_create_cold_scope_while_owner_is_live(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    prepared = PipelineScope("owner", "agent", "connector", "prepared")
    cold = PipelineScope("owner", "agent", "connector", "cold")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        await index.put(prepared, [{"memory_id": "prepared_one",
                                    "payload": {"text": "prepared fact"},
                                    "payload_hash": "a" * 64}])
        pid = index.worker_pid
        cold_path = stores.partition(cold)
        assert not cold_path.exists()
        with pytest.raises(RuntimeError, match="owner not prepared"):
            await index.reader().search_cosine_hits(cold, "cold fact")
        assert index.worker_pid == pid
        assert not cold_path.exists()
        assert not index.recovery_active
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_borrowed_warm_cannot_adopt_another_scope_while_owner_is_live(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    prepared = PipelineScope("owner", "agent", "connector", "prepared")
    other = PipelineScope("owner", "agent", "connector", "other")
    seed = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        await seed.put(other, [{"memory_id": "other_one",
                                "payload": {"text": "other fact"},
                                "payload_hash": "b" * 64}])
    finally:
        await seed.close()

    index = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        await index.put(prepared, [{"memory_id": "prepared_one",
                                    "payload": {"text": "prepared fact"},
                                    "payload_hash": "a" * 64}])
        pid = index.worker_pid
        with pytest.raises(RuntimeError, match="owner not prepared"):
            await index.reader().warm_existing(other)
        assert index.worker_pid == pid
        assert (await index.reader().search_cosine_hits(
            prepared, "prepared fact", limit=1))[0][0] == "prepared_one"
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_owner_write_restarts_after_dead_worker(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    first = {"memory_id": "first", "payload": {"text": "first fact"},
             "payload_hash": "a" * 64}
    second = {"memory_id": "second", "payload": {"text": "second fact"},
              "payload_hash": "b" * 64}
    try:
        await index.put(scope, [first])
        pid = index.worker_pid
        index._process.terminate()
        index._process.join(timeout=3)
        await index.put(scope, [second])
        assert index.worker_pid != pid
        assert await index.contains(scope, ("first", "second"))
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_expired_old_watchdog_cannot_retire_next_vector_operation(tmp_path, monkeypatch):
    from system_memory_god.storage import supervised_daily_vector as vector_module

    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    assert await index.contains(scope, ("missing",)) is False
    pid = index.worker_pid
    entered, release = threading.Event(), threading.Event()
    watchdogs = []

    class HeldWatchdog:
        def __init__(self, interval, callback):
            self.callback = callback

        def start(self):
            watchdogs.append(self)
            if len(watchdogs) == 2:
                entered.set()
                assert release.wait(5)

        def cancel(self):
            pass

    monkeypatch.setattr(vector_module.threading, "Timer", HeldWatchdog)
    later = None
    try:
        assert await index.contains(scope, ("missing",)) is False
        later = asyncio.create_task(index.contains(scope, ("missing",)))
        assert await asyncio.to_thread(entered.wait, 5)
        watchdogs[0].callback()
        assert index.worker_pid == pid
        release.set()
        assert await later is False
        assert index.worker_pid == pid
    finally:
        release.set()
        if later is not None:
            await asyncio.gather(later, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
async def test_borrowed_vector_recovers_published_hits_without_new_write(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    record = {"memory_id": "mem_one", "payload": {"text": "synthetic fact"},
              "payload_hash": "a" * 64}
    try:
        await index.put(scope, [record])
        old_pid = index.worker_pid
        assert old_pid is not None
        index._process.terminate()
        index._process.join(timeout=3)
        assert not psutil.pid_exists(old_pid)
        with pytest.raises(RuntimeError):
            await index.reader().search_cosine_hits(scope, "synthetic fact")
        deadline = asyncio.get_running_loop().time() + 12
        while index.worker_pid is None or index.worker_pid == old_pid:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        hits = await index.reader().search_cosine_hits(scope, "synthetic fact", limit=1)
        assert hits[0][0] == "mem_one"
        assert index.worker_pid != old_pid
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_recovery_coalesces_scopes_and_exhausts_only_failed_scope(tmp_path, monkeypatch):
    stores = ScopedStores(tmp_path / "derived")
    good = PipelineScope("owner", "agent", "connector", "good")
    bad = PipelineScope("owner", "agent", "connector", "bad")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    calls = []
    await index.put(good, [{"memory_id": "good_one", "payload": {"text": "good fact"},
                            "payload_hash": "a" * 64}])
    await index.put(bad, [{"memory_id": "bad_one", "payload": {"text": "bad fact"},
                           "payload_hash": "b" * 64}])

    async def warm(scope):
        calls.append(scope)
        if scope == bad:
            raise RuntimeError("synthetic warm failure")

    monkeypatch.setattr(index, "warm_existing", warm)
    try:
        index.request_recovery(good)
        index.request_recovery(good)
        index.request_recovery(bad)
        task = index._recovery_task
        assert task is not None
        assert index._recovery_task is task
        deadline = asyncio.get_running_loop().time() + 8
        while not index.recovery_exhausted(bad):
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.02)
        assert calls.count(good) == 1
        assert calls.count(bad) == 3
        assert not index.recovery_exhausted(good)
        assert index.recovery_attempts == 3
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_close_during_recovery_backoff_cannot_restart_owner(tmp_path, monkeypatch):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    await index.put(scope, [{"memory_id": "stored_one", "payload": {"text": "fact"},
                             "payload_hash": "a" * 64}])
    attempted = asyncio.Event()
    calls = 0

    async def fail_warm(_scope):
        nonlocal calls
        calls += 1
        attempted.set()
        raise RuntimeError("synthetic warm failure")

    monkeypatch.setattr(index, "warm_existing", fail_warm)
    index.request_recovery(scope)
    await asyncio.wait_for(attempted.wait(), 3)
    await index.close()
    await asyncio.sleep(1.1)
    assert calls == 1
    assert index.worker_pid is None
    assert not index.recovery_active


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_write_queue", [False, True])
async def test_shared_read_cancellation_drains_without_retiring_writer(tmp_path, monkeypatch, cancel_write_queue):
    import json
    import threading

    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    vector = SupervisedDailyVector(stores, embedding="synthetic")
    await vector.put(scope, [{"memory_id": "mem_first", "payload": {"text": "first"},
                              "payload_hash": "a" * 64}])
    pid = vector.worker_pid
    child = psutil.Process(pid)
    child.suspend()
    sent = threading.Event()
    connection = vector._connection
    original_send = connection.send_bytes

    def send(data):
        original_send(data)
        if json.loads(data)["op"] == ("put" if cancel_write_queue else "search_cosine_hits"):
            sent.set()

    monkeypatch.setattr(connection, "send_bytes", send)
    write = query = None
    try:
        if cancel_write_queue:
            write = asyncio.create_task(vector.put(scope, [{"memory_id": "mem_second",
                "payload": {"text": "second"}, "payload_hash": "b" * 64}]))
            assert await asyncio.to_thread(sent.wait, 3)
        query = asyncio.create_task(vector.reader().search_cosine_hits(scope, "first"))
        if not cancel_write_queue:
            assert await asyncio.to_thread(sent.wait, 3)
        else:
            await asyncio.sleep(0)
        query.cancel()
        with pytest.raises(asyncio.CancelledError):
            await query
        assert vector.worker_pid == pid and child.is_running()
        child.resume()
        if write is not None:
            await write
        found = await vector.reader().search_cosine_hits(scope, "first", limit=1)
        assert found[0][0] == "mem_first"
        assert vector.worker_pid == pid
    finally:
        if child.is_running():
            child.resume()
        for task in (query, write):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (query, write) if task is not None),
                             return_exceptions=True)
        await vector.close()


@pytest.mark.asyncio
async def test_supervised_vector_put_is_searchable_after_process_restart(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    record = {"memory_id": "mem_one", "payload": {"text": "synthetic fact"},
              "payload_hash": "a" * 64}
    first = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        await first.put(scope, [record])
        assert await first.contains(scope, ("mem_one",))
    finally:
        await first.close()

    reopened = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        hits = await reopened.search_cosine_hits(scope, "synthetic fact", limit=10)
        assert hits == (("mem_one", pytest.approx(1.0)),)
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_supervised_vector_rejects_forged_write_receipt(tmp_path, monkeypatch):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    record = {"memory_id": "mem_one", "payload": {"text": "synthetic fact"},
              "payload_hash": "a" * 64}

    async def forged(*args, **kwargs):
        return [{"memory_id": "mem_one", "payload_hash": "b" * 64,
                 "text_hash": "c" * 64, "embedding_dim": 16}]

    monkeypatch.setattr(index, "_invoke", forged)
    with pytest.raises(RuntimeError, match="receipt"):
        await index.put(scope, [record])


@pytest.mark.asyncio
async def test_timed_out_vector_query_terminates_its_worker(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(
        stores, embedding="synthetic-stall", request_timeout=2.0,
    )
    task = asyncio.create_task(index.search_cosine_hits(scope, "synthetic fact"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        pid = index.worker_pid
        assert pid is not None
        with pytest.raises(TimeoutError):
            await task
        assert not psutil.pid_exists(pid)
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_rpc_deadline_requests_same_worker_termination_before_its_first_join(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from system_memory_god.storage import supervised_daily_vector as module

    clock, events = [100.0], []

    class Process:
        pid = 24680
        alive = True

        def is_alive(self):
            return self.alive

        def terminate(self):
            events.append(("terminate", self))
            self.alive = False

        def join(self, timeout):
            events.append(("join", self))
            assert not self.alive, "deadline path joined before requesting termination"

        def close(self):
            assert not self.alive
            events.append(("close", self))

    class Connection:
        def send_bytes(self, data):
            pass

        def poll(self, timeout):
            clock[0] += 1
            return False

        def close(self):
            pass

    class HeldWatchdog:
        def __init__(self, interval, callback):
            pass

        def start(self):
            pass

        def cancel(self):
            pass

    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic", request_timeout=.15)
    scope = PipelineScope("owner", "agent", "connector", "chat")
    process = Process()
    index._process, index._connection = process, Connection()
    index._prepared_scopes.add(scope)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(module.threading, "Timer", HeldWatchdog)
    try:
        with pytest.raises(TimeoutError):
            await index.reader().search_cosine_hits(scope, "synthetic")
        assert events == [("terminate", process), ("join", process), ("close", process)]
        assert index.worker_pid is None and index._pending == 0
    finally:
        process.alive = False
        await index.close()


@pytest.mark.asyncio
async def test_cancelled_vector_query_does_not_leave_background_worker(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(
        stores, embedding="synthetic-stall", request_timeout=3.0,
    )
    task = asyncio.create_task(index.search_cosine_hits(scope, "synthetic fact"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        pid = index.worker_pid
        assert pid is not None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.3)
        assert not psutil.pid_exists(pid)
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_retire_busy_vector_owner_fails_fast_without_killing_request(tmp_path):
    import time

    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(
        stores, embedding="synthetic-stall", request_timeout=3.0,
    )
    task = asyncio.create_task(index.search_cosine_hits(scope, "synthetic fact"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        pid = index.worker_pid
        assert pid is not None
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="pending"):
            await index.retire_worker()
        assert time.monotonic() - started < 0.5
        assert index.worker_pid == pid
        assert psutil.pid_exists(pid)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await index.close()


@pytest.mark.asyncio
async def test_retire_failure_keeps_vector_pid_for_diagnosis(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic")
    await index.contains(scope, ("missing",))
    pid = index.worker_pid
    process = index._process
    assert pid is not None and process is not None
    original_terminate = process.terminate
    child = psutil.Process(pid)
    child.suspend()
    try:
        process.terminate = lambda: None
        with pytest.raises(RuntimeError, match="did not exit"):
            await index.retire_worker()
        assert index.worker_pid == pid
        assert psutil.pid_exists(pid)
    finally:
        process.terminate = original_terminate
        child.resume()
        await index.close()


@pytest.mark.asyncio
async def test_retirement_confirms_exit_after_termination_reports_permission_error(tmp_path, monkeypatch):
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    scope = PipelineScope("owner", "agent", "connector", "retirement-race")
    try:
        await index.contains(scope, ("missing",))
        process = index._process
        child = psutil.Process(index.worker_pid)
        child.suspend()
        terminate = process.terminate

        def terminated_with_error():
            terminate()
            process.join(timeout=1)
            raise PermissionError(5, "synthetic termination observation")

        monkeypatch.setattr(process, "terminate", terminated_with_error)
        try:
            await index.retire_worker()
            assert index.worker_pid is None
        finally:
            monkeypatch.setattr(process, "terminate", terminate)
            if child.is_running():
                child.resume()
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_same_derived_root_rejects_a_second_live_owner(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    first = SupervisedDailyVector(stores, embedding="synthetic")
    second = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        await first.put(scope, [{"memory_id": "mem_one",
                                 "payload": {"text": "synthetic fact"},
                                 "payload_hash": "a" * 64}])
        with pytest.raises(RuntimeError, match="owner"):
            await second.contains(scope, ("mem_one",))
        assert await first.contains(scope, ("mem_one",))
    finally:
        await second.close()
        await first.close()


@pytest.mark.asyncio
async def test_lost_write_receipt_is_reconciled_before_idempotent_retry(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    record = {"memory_id": "mem_one", "payload": {"text": "synthetic fact"},
              "payload_hash": "a" * 64}
    lost = SupervisedDailyVector(stores, embedding="synthetic-drop-ack")
    try:
        with pytest.raises(RuntimeError):
            await lost.put(scope, [record])
    finally:
        await lost.close()

    reopened = SupervisedDailyVector(stores, embedding="synthetic")
    try:
        assert await reopened.contains(scope, ("mem_one",))
        await reopened.put(scope, [record])
        with pytest.raises(ValueError):
            await reopened.put(scope, [{**record,
                                        "payload": {"text": "different fact"}}])
        assert len(await reopened.search_cosine_hits(scope, "synthetic fact")) == 1
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_wrong_response_id_retires_worker_without_returning_candidates(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic-wrong-id")
    try:
        with pytest.raises(RuntimeError, match="response"):
            await index.search_cosine_hits(scope, "synthetic fact")
        assert index.worker_pid is None
    finally:
        await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault,code", [("owner", "owner_rejected"),
    ("embedding", "embedding_rejected"), ("invalid", "invalid_ready"),
    ("exit", "process_exit"), ("deadline", "ready_deadline"),
    ("cancel", "cancelled"), ("spawn", "spawn_failed")])
async def test_startup_facts_distinguish_boundary_failures_without_private_text(tmp_path, monkeypatch, fault, code):
    import json
    from types import SimpleNamespace
    import system_memory_god.storage.supervised_daily_vector as supervised

    private = "private-chat-key-path-sentinel"
    waiting = threading.Event()

    class Connection:
        def poll(self, timeout):
            waiting.set()
            if fault in {"deadline", "cancel"}:
                threading.Event().wait(min(timeout, .01))
                return False
            return fault not in {"exit", "spawn"}

        def recv_bytes(self, limit):
            return json.dumps({"version": 1, "ready": False, "error": fault,
                               "private": private}).encode()

        def close(self):
            pass

    class Process:
        pid = 24680

        def start(self):
            if fault == "spawn":
                raise OSError(private)

        def is_alive(self):
            return fault in {"deadline", "cancel"}

        def join(self, timeout):
            pass

        def terminate(self):
            pass

        def close(self):
            pass

    process = Process()
    context = SimpleNamespace(Pipe=lambda: (Connection(), Connection()), Process=lambda **kw: process)
    monkeypatch.setattr(supervised.multiprocessing, "get_context", lambda method: context)
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic", request_timeout=.2)
    scope = PipelineScope("owner", "agent", "connector", "chat")
    # Retirement must see a dead boundary process, including cancellation.
    def terminate():
        process.is_alive = lambda: False
    process.terminate = terminate
    task = asyncio.create_task(index.search_cosine_hits(scope, private))
    try:
        if fault == "cancel":
            while not waiting.is_set():
                await asyncio.sleep(.001)
            assert index.startup_diagnostic()["stage"] == "waiting_ready"
            task.cancel()
        with pytest.raises((RuntimeError, OSError, TimeoutError, asyncio.CancelledError)):
            await task
        observed = index.startup_diagnostic()
        assert observed["stage"] == "failed" and observed["error_code"] == code
        assert private not in json.dumps(observed)
        assert index.worker_pid is None
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_malformed_startup_frame_retires_worker(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic-bad-ready")
    try:
        with pytest.raises(RuntimeError, match="startup"):
            await index.search_cosine_hits(scope, "synthetic fact")
        assert index.worker_pid is None
        startup = index.startup_diagnostic()
        assert startup["stage"] == "failed"
        assert startup["error_code"] == "invalid_ready"
        assert startup["error_type"] == "RuntimeError"
        assert type(startup["elapsed_ms"]) is float and startup["elapsed_ms"] >= 0
        startup["error_code"] = "changed"
        assert index.startup_diagnostic()["error_code"] == "invalid_ready"
    finally:
        await index.close()


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_worker_exit(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic-stall",
                                  request_timeout=3.0)
    task = asyncio.create_task(index.search_cosine_hits(scope, "synthetic fact"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        pid = index.worker_pid
        assert pid is not None
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert index.worker_pid is None
        assert not psutil.pid_exists(pid)
    finally:
        await index.close()


def test_failed_vector_termination_retains_pid_for_followup(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    index = SupervisedDailyVector(stores, embedding="synthetic")

    class StuckProcess:
        pid = 24680

        def join(self, timeout):
            return None

        def is_alive(self):
            return True

        def terminate(self):
            return None

        def close(self):
            raise AssertionError("live process must not be closed")

    index._process = StuckProcess()
    with pytest.raises(RuntimeError, match="did not exit"):
        index._stop()
    assert index.worker_pid == 24680


@pytest.mark.asyncio
async def test_cancelled_queued_query_does_not_wait_for_stalled_owner(tmp_path):
    import time

    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic-stall",
                                  request_timeout=3.0)
    first = asyncio.create_task(index.search_cosine_hits(scope, "first query"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        assert index.worker_pid is not None
        second = asyncio.create_task(index.search_cosine_hits(scope, "second query"))
        await asyncio.sleep(0.1)
        started = time.monotonic()
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert time.monotonic() - started < 0.5
    finally:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await index.close()


@pytest.mark.asyncio
async def test_supervised_vector_rejects_when_pending_capacity_is_full(tmp_path):
    stores = ScopedStores(tmp_path / "derived")
    scope = PipelineScope("owner", "agent", "connector", "chat")
    index = SupervisedDailyVector(stores, embedding="synthetic-stall",
                                  request_timeout=3.0, max_pending=1)
    first = asyncio.create_task(index.search_cosine_hits(scope, "first query"))
    try:
        for _ in range(100):
            if index.worker_pid is not None:
                break
            await asyncio.sleep(0.02)
        assert index.worker_pid is not None
        with pytest.raises(RuntimeError, match="queue full"):
            await index.search_cosine_hits(scope, "second query")
    finally:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["put", "put_snapshot", "contains", "receipts", "receipts_snapshot",
                                      "present_ids", "warm_existing", "writer_search", "borrowed_warm", "borrowed_search"])
@pytest.mark.parametrize("boundary", ["send", "recv"])
async def test_writer_deadline_with_blocked_send_and_denied_termination_keeps_pending(
        tmp_path, monkeypatch, operation, boundary):
    scope = PipelineScope("owner", "agent", "connector", "blocked-writer")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic",
                                  enable_snapshot=operation in {"put_snapshot", "receipts_snapshot"})
    entered, release = threading.Event(), threading.Event()
    process = child = terminate = call = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        index.request_timeout = .15
        process = index._process
        terminate = process.terminate
        child = psutil.Process(index.worker_pid)
        if boundary == "send":
            child.suspend()
        connection = index._connection
        boundary_name = "send_bytes" if boundary == "send" else "recv_bytes"
        original_boundary = getattr(connection, boundary_name)

        def blocked_boundary(*args, **kwargs):
            entered.set()
            assert release.wait(8), "test boundary was not released"
            return original_boundary(*args, **kwargs)

        def denied_termination():
            raise PermissionError(5, "synthetic writer owner denial")

        monkeypatch.setattr(connection, boundary_name, blocked_boundary)
        monkeypatch.setattr(process, "terminate", denied_termination)
        records = [{"memory_id": "one", "payload": {"text": "synthetic"}, "payload_hash": "a" * 64}]
        if operation in {"put", "put_snapshot"}:
            request = index.put(scope, records)
        elif operation in {"receipts", "receipts_snapshot"}:
            request = index.receipts(scope, records)
        elif operation == "contains":
            request = index.contains(scope, ("one",))
        elif operation == "present_ids":
            request = index.present_ids(scope, ("one",))
        elif operation == "warm_existing":
            request = index.warm_existing(scope)
        elif operation == "writer_search":
            request = index.search_cosine_hits(scope, "synthetic")
        elif operation == "borrowed_warm":
            request = index.reader().warm_existing(scope)
        else:
            request = index.reader().search_cosine_hits(scope, "synthetic")
        call = asyncio.create_task(request)
        assert await asyncio.to_thread(entered.wait, 1)
        done, _ = await asyncio.wait((call,), timeout=.6)
        assert call in done, "writer caller remained blocked after its deadline"
        assert isinstance(call.exception(), TimeoutError)
        assert index.worker_pid == child.pid and not connection.closed
        with pytest.raises(RuntimeError, match="pending"):
            await index.retire_worker()
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        if boundary == "send" and child is not None and child.is_running():
            child.resume()
        release.set()
        if call is not None:
            await asyncio.gather(call, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
async def test_writer_close_does_not_close_connection_under_unsettled_rpc(tmp_path, monkeypatch):
    scope = PipelineScope("owner", "agent", "connector", "closing-writer")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    entered, release = threading.Event(), threading.Event()
    process = terminate = call = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        index.request_timeout = .15
        process = index._process
        terminate = process.terminate
        connection = index._connection
        send = connection.send_bytes

        def blocked_send(data):
            entered.set()
            assert release.wait(8), "test boundary was not released"
            send(data)

        def denied_termination():
            raise PermissionError(5, "synthetic close owner denial")

        monkeypatch.setattr(connection, "send_bytes", blocked_send)
        monkeypatch.setattr(process, "terminate", denied_termination)
        call = asyncio.create_task(index.contains(scope, ("one",)))
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(TimeoutError):
            await call
        closing = asyncio.create_task(index.close())
        done, _ = await asyncio.wait((closing,), timeout=3.5)
        assert closing in done, "close did not preserve its original finite cleanup budget"
        assert isinstance(closing.exception(), RuntimeError)
        assert index.worker_pid == process.pid
        assert not connection.closed, "close invalidated an unsettled RPC's connection"
        assert index._pending == 1
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        release.set()
        if call is not None:
            await asyncio.gather(call, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
async def test_close_lifecycle_wait_is_bounded_and_preserves_owned_handles(tmp_path):
    scope = PipelineScope("owner", "agent", "connector", "lifecycle-close")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    entered, release = threading.Event(), threading.Event()
    holder = closing = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        pid, connection = index.worker_pid, index._connection

        def hold_lifecycle():
            with index._lifecycle:
                entered.set()
                assert release.wait(8), "test lifecycle boundary was not released"

        holder = asyncio.create_task(asyncio.to_thread(hold_lifecycle))
        assert await asyncio.to_thread(entered.wait, 1)
        closing = asyncio.create_task(index.close())
        done, _ = await asyncio.wait((closing,), timeout=3.5)
        assert closing in done, "close waited indefinitely on the lifecycle owner"
        assert isinstance(closing.exception(), RuntimeError)
        assert index.worker_pid == pid and not connection.closed
    finally:
        release.set()
        if holder is not None:
            await holder
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
async def test_expired_cold_call_cannot_spawn_after_lifecycle_owner_is_released(tmp_path, monkeypatch):
    from multiprocessing.process import BaseProcess

    scope = PipelineScope("owner", "agent", "connector", "expired-cold")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic", request_timeout=.15)
    entered, release = threading.Event(), threading.Event()
    starts = []
    start = BaseProcess.start

    def observed_start(process):
        starts.append(process)
        return start(process)

    def hold_lifecycle():
        with index._lifecycle:
            entered.set()
            assert release.wait(8), "test lifecycle boundary was not released"

    monkeypatch.setattr(BaseProcess, "start", observed_start)
    holder = asyncio.create_task(asyncio.to_thread(hold_lifecycle))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(TimeoutError):
            await index.contains(scope, ("one",))
        with pytest.raises(RuntimeError, match="pending"):
            await index.retire_worker()
        release.set()
        await holder
        for _ in range(200):
            if index._pending == 0:
                break
            await asyncio.sleep(.01)
        assert index._pending == 0
        assert not starts, "an already expired request started a new worker"
        assert index.worker_pid is None
    finally:
        release.set()
        await holder
        await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["close", "retire_worker"])
@pytest.mark.parametrize("cancel_dispatch", [False, True])
async def test_close_executor_dispatch_is_inside_the_original_cleanup_deadline(tmp_path, action, cancel_dispatch):
    from concurrent.futures import ThreadPoolExecutor

    scope = PipelineScope("owner", "agent", "connector", "close-dispatch")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    loop = asyncio.get_running_loop()
    entered, release = threading.Event(), threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    held = closing = previous_executor = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        pid, connection = index.worker_pid, index._connection
        previous_executor = loop._default_executor
        loop.set_default_executor(executor)

        def hold_executor():
            entered.set()
            assert release.wait(8), "test executor boundary was not released"

        held = loop.run_in_executor(None, hold_executor)
        while not entered.is_set():
            await asyncio.sleep(.001)
        closing = asyncio.create_task(getattr(index, action)())
        if cancel_dispatch:
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.gather(closing, return_exceptions=True)
            await asyncio.sleep(3.1)
        done, _ = await asyncio.wait((closing,), timeout=3.5)
        assert closing in done, "close waited indefinitely for executor dispatch"
        assert closing.cancelled() if cancel_dispatch else isinstance(closing.exception(), RuntimeError)
        assert index.worker_pid == pid and not connection.closed
        release.set()
        await held
        if index._closing_tasks:
            await asyncio.gather(*tuple(index._closing_tasks), return_exceptions=True)
        assert index.worker_pid == pid and not connection.closed, "expired dispatch performed a late cleanup"
        if cancel_dispatch:
            assert index._cleanup_failure is not None, "settled expired dispatch did not preserve its failure"
            await index.close()
            assert index.worker_pid is None
    finally:
        release.set()
        if held is not None:
            await held
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        if previous_executor is not None:
            loop.set_default_executor(previous_executor)
        try:
            await index.close()
        finally:
            if index.worker_pid is not None:
                await index.close()
            executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_old_cleanup_failure_cannot_mark_a_new_explicit_attempt(tmp_path, monkeypatch):
    import time

    scope = PipelineScope("owner", "agent", "connector", "cleanup-attempt")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    held, release_owner = threading.Event(), threading.Event()
    failure_entered, release_failure, failure_finished = threading.Event(), threading.Event(), threading.Event()
    second_entered, release_second = threading.Event(), threading.Event()
    holder = first = second = None
    intercepted = []
    response_deadlines, response_returns, action_deadlines = [], [], []
    action_guard = threading.Lock()
    active = maximum_active = 0
    main_thread = threading.current_thread()
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        record_failure = index._record_cleanup_failure
        close_response, finish_cleanup = index._close_response, index._finish_cleanup

        async def forwarded_response(deadline):
            response_deadlines.append(deadline)
            await close_response(deadline)
            response_returns.append((deadline, time.monotonic()))

        async def forwarded_cleanup(action, deadline, *, _cleanup_observation=None):
            def forwarded_action():
                nonlocal active, maximum_active
                with action_guard:
                    action_deadlines.append(deadline)
                    ordinal = len(action_deadlines)
                    active += 1
                    maximum_active = max(maximum_active, active)
                try:
                    if ordinal == 2:
                        second_entered.set()
                        assert release_second.wait(2), "test second action was not released"
                    action()
                finally:
                    with action_guard:
                        active -= 1

            await finish_cleanup(forwarded_action, deadline,
                                 _cleanup_observation=_cleanup_observation)

        def delayed_failure(*, deadline=None):
            if threading.current_thread() is not main_thread and not intercepted:
                intercepted.append(deadline)
                failure_entered.set()
                assert release_failure.wait(8), "test failure registration was not released"
                try:
                    record_failure(deadline=deadline)
                finally:
                    failure_finished.set()
            else:
                record_failure(deadline=deadline)

        def hold_lifecycle():
            with index._lifecycle:
                held.set()
                assert release_owner.wait(10), "test cleanup owner was not released"

        monkeypatch.setattr(index, "_record_cleanup_failure", delayed_failure)
        monkeypatch.setattr(index, "_close_response", forwarded_response)
        monkeypatch.setattr(index, "_finish_cleanup", forwarded_cleanup)
        holder = asyncio.create_task(asyncio.to_thread(hold_lifecycle))
        assert await asyncio.to_thread(held.wait, 1)
        first = asyncio.create_task(index.close())
        done, _ = await asyncio.wait((first,), timeout=3.5)
        assert first in done and isinstance(first.exception(), RuntimeError)
        assert await asyncio.to_thread(failure_entered.wait, 1)
        old_deadline = index._close_deadline
        old_futures = tuple(index._closing_tasks)
        assert old_futures and any(not task.done() for task in old_futures)
        second_entry = time.monotonic()
        second = asyncio.create_task(index.close())
        await asyncio.sleep(.02)
        assert not second.done() and index._close_deadline == old_deadline
        assert action_deadlines == [old_deadline] and not second_entered.is_set()
        release_failure.set()
        release_owner.set()
        assert await asyncio.to_thread(failure_finished.wait, 1)
        assert await asyncio.to_thread(second_entered.wait, 1)
        assert all(task.done() for task in old_futures), "old physical ownership must settle first"
        assert len(response_deadlines) == 2 and len(action_deadlines) == 2
        second_cap = response_deadlines[1]  # Original close computes this at entry plus three seconds.
        new_deadline = index._close_deadline
        assert old_deadline < new_deadline <= second_cap
        assert action_deadlines[1] == new_deadline and maximum_active == 1
        assert index._close_budget_failure is None
        new_state = (index._close_deadline, index._close_budget_failure, index._cleanup_failure)
        record_failure(deadline=old_deadline)
        assert (index._close_deadline, index._close_budget_failure, index._cleanup_failure) == new_state
        release_second.set()
        done, _ = await asyncio.wait((second,), timeout=max(0, second_entry + 3.5 - time.monotonic()))
        assert second in done and second.exception() is None
        assert len(response_returns) == 1 and response_returns[0][0] == second_cap
        assert response_returns[0][1] <= second_cap and time.monotonic() <= second_cap
        assert len(action_deadlines) == 2 and maximum_active == 1
    finally:
        release_failure.set()
        release_owner.set()
        release_second.set()
        if holder is not None:
            await holder
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
async def test_recovery_cannot_close_connection_owned_by_unsettled_reader(tmp_path, monkeypatch):
    scope = PipelineScope("owner", "agent", "connector", "recovery-owner")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    entered, release = threading.Event(), threading.Event()
    call = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        index.request_timeout = .15
        connection, process = index._connection, index._process
        recv = connection.recv_bytes

        def blocked_recv(*args):
            entered.set()
            assert release.wait(10), "test recovery boundary was not released"
            return recv(*args)

        monkeypatch.setattr(connection, "recv_bytes", blocked_recv)
        call = asyncio.create_task(index.reader().search_cosine_hits(scope, "synthetic"))
        assert await asyncio.to_thread(entered.wait, 1)
        process.terminate()
        await asyncio.to_thread(process.join, 1)
        with pytest.raises(TimeoutError):
            await call
        with pytest.raises(RuntimeError, match="not prepared"):
            await index.reader().search_cosine_hits(scope, "synthetic")
        await asyncio.sleep(1.2)
        assert index.recovery_attempts == 1
        assert index._pending == 1
        assert index._process is process and not connection.closed, "recovery invalidated the old RPC owner"
    finally:
        release.set()
        if call is not None:
            await asyncio.gather(call, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("borrowed", [False, True])
@pytest.mark.parametrize("boundary", ["send_bytes", "recv_bytes"])
async def test_active_cancellation_retains_the_actual_rpc_owner(tmp_path, monkeypatch, borrowed, boundary):
    scope = PipelineScope("owner", "agent", "connector", "cancel-owner")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    entered, release = threading.Event(), threading.Event()
    call = process = terminate = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        index.request_timeout = .15
        connection, process = index._connection, index._process
        terminate = process.terminate
        original = getattr(connection, boundary)

        def blocked_boundary(*args, **kwargs):
            entered.set()
            assert release.wait(10), "test cancellation boundary was not released"
            return original(*args, **kwargs)

        def denied_termination():
            raise PermissionError(5, "synthetic cancelled owner denial")

        monkeypatch.setattr(connection, boundary, blocked_boundary)
        monkeypatch.setattr(process, "terminate", denied_termination)
        owner = index.reader() if borrowed else index
        call = asyncio.create_task(owner.search_cosine_hits(scope, "synthetic"))
        assert await asyncio.to_thread(entered.wait, 1)
        call.cancel()
        done, _ = await asyncio.wait((call,), timeout=3.5)
        assert call in done and call.cancelled()
        assert index._pending == 1 and index._process is process and not connection.closed
        with pytest.raises(RuntimeError, match="pending"):
            await index.retire_worker()
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        release.set()
        if call is not None:
            await asyncio.gather(call, return_exceptions=True)
        for _ in range(400):
            if index._pending == 0:
                break
            await asyncio.sleep(.01)
        await index.close()


@pytest.mark.asyncio
async def test_pending_retirement_refusal_does_not_spend_a_future_cleanup_budget(tmp_path, monkeypatch):
    scope = PipelineScope("owner", "agent", "connector", "busy-retirement")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic")
    entered, release = threading.Event(), threading.Event()
    call = None
    try:
        await index.put(scope, [{"memory_id": "one", "payload": {"text": "synthetic"},
                                 "payload_hash": "a" * 64}])
        connection = index._connection
        recv = connection.recv_bytes

        def held_reply(*args, **kwargs):
            body = recv(*args, **kwargs)
            entered.set()
            assert release.wait(8), "test busy retirement boundary was not released"
            return body

        monkeypatch.setattr(connection, "recv_bytes", held_reply)
        call = asyncio.create_task(index.reader().search_cosine_hits(scope, "synthetic"))
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(RuntimeError, match="pending"):
            await index.retire_worker()
        await asyncio.sleep(3.1)
        release.set()
        assert await call
        await index.close()
        assert index.worker_pid is None
    finally:
        release.set()
        if call is not None:
            await asyncio.gather(call, return_exceptions=True)
        await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["put", "put_snapshot", "receipts_snapshot", "warm_existing"])
async def test_success_tail_cannot_block_event_loop_on_another_lifecycle_owner(tmp_path, monkeypatch, operation):
    import time

    scope = PipelineScope("owner", "agent", "connector", "success-tail")
    index = SupervisedDailyVector(ScopedStores(tmp_path / "derived"), embedding="synthetic",
                                  enable_snapshot=operation in {"put_snapshot", "receipts_snapshot"})
    records = [{"memory_id": "one", "payload": {"text": "synthetic"}, "payload_hash": "a" * 64}]
    entered, release = threading.Event(), threading.Event()
    holder = timer = None
    try:
        await index.put(scope, records)
        invoke = index._invoke

        def hold_lifecycle():
            with index._lifecycle:
                entered.set()
                assert release.wait(5), "test lifecycle boundary was not released"

        async def successful_then_contended(*args, **kwargs):
            nonlocal holder, timer
            result = await invoke(*args, **kwargs)
            holder = threading.Thread(target=hold_lifecycle)
            holder.start()
            assert await asyncio.to_thread(entered.wait, 1)
            timer = threading.Timer(1, release.set)
            timer.start()
            return result

        monkeypatch.setattr(index, "_invoke", successful_then_contended)
        began = time.monotonic()
        if operation in {"put", "put_snapshot"}:
            await index.put(scope, records)
        elif operation == "receipts_snapshot":
            assert await index.receipts(scope, records)
        else:
            await index.warm_existing(scope)
        assert time.monotonic() - began < .6, "successful RPC blocked the event loop on its async tail"
    finally:
        release.set()
        if timer is not None:
            timer.cancel()
        if holder is not None:
            await asyncio.to_thread(holder.join, 1)
        await index.close()


def test_vector_protocol_rejects_missing_required_record_field_with_extra_key():
    with pytest.raises(ValueError, match="invalid vector write"):
        _validate_request({
            "version": 1, "id": 1,
            "scope": ["owner", "agent", "connector", "chat"],
            "op": "put", "records": [{"payload": {"text": "fact"},
                                      "payload_hash": "a" * 64,
                                      "extra": "not an ID"}],
        })


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["lifecycle", "ipc", "termination", "join"])
async def test_cleanup_diagnostic_preserves_fixed_close_stages(tmp_path, boundary):
    import json

    index = SupervisedDailyVector(ScopedStores(tmp_path / "observed"),
                                  embedding="synthetic", process_role="snapshot", diagnostics=True)
    calls = []

    class Lock:
        def __init__(self, name):
            self.name = name

        def acquire(self, *args, **kwargs):
            calls.append(self.name + "_acquire")
            return self.name != boundary

        def release(self):
            calls.append(self.name + "_release")

    class Connection:
        def close(self):
            calls.append("connection_close")

    class Process:
        def __init__(self):
            self.alive = True

        def is_alive(self):
            calls.append("is_alive")
            return self.alive

        def join(self, *, timeout):
            calls.append("join")
            if boundary == "join":
                self.alive = False

        def terminate(self):
            calls.append("terminate")
            raise PermissionError("synthetic private termination message")

        def close(self):
            calls.append("process_close")

    index._lifecycle, index._lock = Lock("lifecycle"), Lock("ipc")
    index._connection, index._process = Connection(), Process()
    before = index.diagnostic()
    if boundary == "join":
        await index.close()
    else:
        with pytest.raises(RuntimeError) as caught:
            await index.close()
        assert str(caught.value) == ("vector lifecycle owner is busy" if boundary == "lifecycle"
                                     else "vector worker did not exit")
        if boundary == "termination":
            assert type(caught.value.__cause__) is PermissionError
    expected = {
        "lifecycle": ["lifecycle_acquire"],
        "ipc": ["lifecycle_acquire", "lifecycle_release", "ipc_acquire", "is_alive"],
        "termination": ["lifecycle_acquire", "lifecycle_release", "ipc_acquire",
                        "lifecycle_acquire", "connection_close", "join", "is_alive", "is_alive",
                        "terminate", "join", "is_alive", "lifecycle_release", "ipc_release"],
        "join": ["lifecycle_acquire", "lifecycle_release", "ipc_acquire", "lifecycle_acquire",
                 "connection_close", "join", "is_alive", "is_alive", "process_close",
                 "lifecycle_release", "ipc_release"],
    }
    assert calls == expected[boundary], "observation changed the original close call count or order"
    observed = index._cleanup_diagnostic_snapshot()
    assert set(observed) == {"schema", "attempt", "frames", "dropped", "complete"}
    assert observed["schema"] == 1 and observed["attempt"] == 1
    assert observed["complete"] is (boundary == "join")
    assert observed["dropped"] == 0 and len(observed["frames"]) <= 32
    stages = [frame["stage"] for frame in observed["frames"]]
    assert stages[:3] == ["budget", "dispatch", "dispatched"]
    assert "lifecycle_wait" in stages
    if boundary != "lifecycle":
        assert "ipc_wait" in stages
    if boundary == "termination":
        assert "termination" in stages and "termination_return" in stages
    if boundary == "join":
        assert {"join", "join_return", "stop_return", "finished"} <= set(stages)
    for frame in observed["frames"]:
        assert {"stage", "elapsed_ms"} <= set(frame)
        assert set(frame) <= {"stage", "elapsed_ms", "process_alive", "error_type"}
        assert isinstance(frame["elapsed_ms"], (int, float)) and frame["elapsed_ms"] >= 0
        if "process_alive" in frame:
            assert type(frame["process_alive"]) is bool
    assert len(json.dumps(observed).encode()) <= 8192
    assert "synthetic private termination message" not in json.dumps(observed)
    assert index.diagnostic() == before
    sealed = index._cleanup_diagnostic_snapshot()
    token = index._cleanup_diagnostic_token
    index._cleanup_diagnostic_note("join", attempt=token)
    assert index._cleanup_diagnostic_snapshot() == sealed, "completed attempts must be sealed"
    disabled = SupervisedDailyVector(ScopedStores(tmp_path / "unobserved"), embedding="synthetic")
    assert disabled._cleanup_diagnostic_snapshot() == {}


@pytest.mark.asyncio
async def test_cleanup_diagnostic_full_observer_preserves_original_error(tmp_path):
    import json

    index = SupervisedDailyVector(ScopedStores(tmp_path / "observed"),
                                  embedding="synthetic", process_role="snapshot", diagnostics=True)
    _, deadline = index._cleanup_budget()
    token = index._cleanup_diagnostic_capture(deadline)
    original = ValueError("synthetic private main error")
    calls = []

    def failed_once():
        calls.append("action")
        for _ in range(40):
            index._cleanup_diagnostic_note("join", attempt=token)
        raise original

    with pytest.raises(ValueError) as caught:
        await index._finish_cleanup(failed_once, deadline, _cleanup_observation=token)
    assert caught.value is original and calls == ["action"]
    observed = index._cleanup_diagnostic_snapshot()
    assert observed["attempt"] == 1 and len(observed["frames"]) == 32
    assert observed["dropped"] > 0 and observed["complete"] is False
    assert len(json.dumps(observed).encode()) <= 8192
    assert "synthetic private main error" not in json.dumps(observed)
    observed["frames"][0]["stage"] = "external mutation"
    assert index._cleanup_diagnostic_snapshot()["frames"][0]["stage"] == "budget"
    sealed = index._cleanup_diagnostic_snapshot()
    index._cleanup_diagnostic_note("join", attempt=token)
    assert index._cleanup_diagnostic_snapshot() == sealed
    index._cleanup_diagnostic_begin(deadline)
    replacement = index._cleanup_diagnostic_capture(deadline)
    assert replacement is not token, "equal deadlines must not identify separate attempts"
    current = index._cleanup_diagnostic_snapshot()
    index._cleanup_diagnostic_guard.acquire()
    try:
        index._cleanup_diagnostic_note("termination", attempt=token, process_alive=True)
    finally:
        index._cleanup_diagnostic_guard.release()
    assert index._cleanup_diagnostic_snapshot() == current, "old attempt must not even alter dropped"
    index._cleanup_diagnostic_note("join")
    index._stop()
    assert index._cleanup_diagnostic_snapshot() == current, "ordinary stop has no observation authority"


@pytest.mark.asyncio
async def test_cleanup_diagnostic_unavailable_observer_preserves_original_error(tmp_path, monkeypatch):
    index = SupervisedDailyVector(ScopedStores(tmp_path / "observed"),
                                  embedding="synthetic", process_role="snapshot", diagnostics=True)
    _, deadline = index._cleanup_budget()
    token = index._cleanup_diagnostic_capture(deadline)
    original = RuntimeError("synthetic private main error")
    calls = []

    class Unavailable:
        def acquire(self, *, blocking):
            assert blocking is False, "diagnostic acquisition must never wait"
            raise OSError("synthetic private observer error")

    def failed_once():
        calls.append("action")
        raise original

    index._cleanup_diagnostic_guard = Unavailable()
    with pytest.raises(RuntimeError) as caught:
        await index._finish_cleanup(failed_once, deadline, _cleanup_observation=token)
    assert caught.value is original and calls == ["action"]
    observed = index._cleanup_diagnostic_snapshot()
    assert set(observed) == {"schema", "attempt", "frames", "dropped", "complete"}
    assert observed["schema"] == 1 and observed["attempt"] == 1
    assert observed["frames"] == [] and observed["dropped"] > 0
    assert observed["complete"] is False
    for control in (KeyboardInterrupt(), SystemExit(), asyncio.CancelledError()):
        class Interrupted:
            def acquire(self, *, blocking):
                assert blocking is False
                raise control

        index._cleanup_diagnostic_guard = Interrupted()
        with pytest.raises(type(control)) as interrupted:
            index._cleanup_diagnostic_snapshot()
        assert interrupted.value is control, "observer must not swallow process-control exceptions"
    for boundary, control, secondary_failure in (("wait", asyncio.CancelledError(), False),
                              ("result", KeyboardInterrupt(), False), ("result", SystemExit(), False),
                              ("result", SystemExit(), True)):
        guard = threading.Lock()
        index._cleanup_diagnostic_guard = guard
        index._cleanup_diagnostic_begin(deadline)
        token = index._cleanup_diagnostic_capture(deadline)
        if secondary_failure:
            class FailedObservation:
                attempts = 0

                def acquire(self, *, blocking):
                    self.attempts += 1
                    if self.attempts == 2:
                        raise KeyboardInterrupt()
                    return guard.acquire(blocking=blocking)

                def release(self):
                    guard.release()

            index._cleanup_diagnostic_guard = FailedObservation()

        settled = asyncio.get_running_loop().create_future()

        def queued(coroutine):
            coroutine.close()
            return settled

        async def waiting(*args, **kwargs):
            assert tuple(args[0]) == (settled,)
            assert not settled.done()
            if boundary == "wait":
                raise control
            settled.set_exception(control)
            return {settled}, set()

        try:
            with monkeypatch.context() as patch:
                patch.setattr(asyncio, "create_task", queued)
                patch.setattr(asyncio, "wait", waiting)
                with pytest.raises(type(control)) as interrupted:
                    await index._finish_cleanup(lambda: None, deadline, _cleanup_observation=token)
            assert interrupted.value is control
            assert settled.done() is (boundary == "result")
            if boundary == "wait":
                assert settled in index._closing_tasks, "cancellation must not discard a pending owner"
            sealed = index._cleanup_diagnostic_snapshot()
            assert sealed["frames"][-1]["stage"] == ("dispatch" if secondary_failure else "failed")
            assert sealed["complete"] is False
            index._cleanup_diagnostic_note("dispatched", attempt=token)
            assert index._cleanup_diagnostic_snapshot() == sealed, "late dispatch must not reopen terminal observation"
        finally:
            if not settled.done():
                settled.set_result(None)
            await asyncio.sleep(0)
        assert settled.done() and settled not in index._closing_tasks
