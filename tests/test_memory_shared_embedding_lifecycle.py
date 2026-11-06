"""Shared inference injection preserves user-worker and command ownership."""

import asyncio
import json
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector


class FakeEmbeddingClient:
    def __init__(self):
        self.deadlines = []
        self.calls = []
        self.parent_closed = False
        self.closed = False

    def bind_deadline(self, deadline):
        self.deadlines.append(deadline)

    def __call__(self, texts):
        self.calls.append(texts)
        return [[1.0] + [0.0] * 1023]

    def close_parent_copy(self):
        self.parent_closed = True

    def close(self):
        self.closed = True


def deadline():
    return time.monotonic_ns() + 10_000_000_000


def test_shared_configuration_rejects_formal_before_creating_resources(tmp_path):
    client = FakeEmbeddingClient()
    with pytest.raises(ValueError, match="formal"):
        SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
            formal_embedding=object(), embedding_client=client, embedding_deadline_ns=deadline())
    assert not tuple(tmp_path.iterdir())


def test_shared_configuration_requires_one_bounded_deadline(tmp_path):
    for value in (None, True, 0, 2**63, time.monotonic_ns() - 1):
        with pytest.raises(ValueError):
            SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
                embedding_client=FakeEmbeddingClient(), embedding_deadline_ns=value)


def test_parent_command_carries_registered_deadline(tmp_path, monkeypatch):
    absolute = deadline()
    vector = SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
        embedding_client=FakeEmbeddingClient(), embedding_deadline_ns=absolute)
    sent = []
    connection = SimpleNamespace(
        send_bytes=lambda body: sent.append(json.loads(body)), poll=lambda timeout: True,
        recv_bytes=lambda limit: b'{"version":1,"id":1,"ok":true,"result":true}')
    vector._connection, vector._process = connection, SimpleNamespace(is_alive=lambda: True)
    monkeypatch.setattr(vector, "_acquire", lambda *args: nullcontext())
    monkeypatch.setattr(vector, "_start_locked", lambda *args: None)
    result = vector._call_transport("contains", PipelineScope("u", "a", "c", "x"),
                                   threading.Event(), ids=["m"])
    assert result is True
    assert sent[0]["embedding_deadline_ns"] == absolute


@pytest.mark.parametrize("state", ["active", "queued"])
def test_deadline_update_refuses_owned_command(tmp_path, state):
    vector = SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
        embedding_client=FakeEmbeddingClient(), embedding_deadline_ns=deadline())
    if state == "active":
        vector._active_request = object()
    else:
        vector._pending = 1
    with pytest.raises(RuntimeError):
        vector.set_embedding_deadline(deadline())


@pytest.mark.parametrize("lock_name", ["_lifecycle", "_pending_guard"])
def test_deadline_update_rejects_contended_thread_lock_without_waiting(tmp_path, lock_name):
    initial = deadline()
    vector = SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
        embedding_client=FakeEmbeddingClient(), embedding_deadline_ns=initial)
    held, release, finished = threading.Event(), threading.Event(), threading.Event()
    outcome = []

    def hold_lock():
        with getattr(vector, lock_name):
            held.set()
            release.wait(5)

    def update():
        try:
            vector.set_embedding_deadline(deadline())
            outcome.append("updated")
        except RuntimeError:
            outcome.append("rejected")
        finally:
            finished.set()

    holder = threading.Thread(target=hold_lock)
    setter = threading.Thread(target=update)
    holder.start()
    try:
        assert held.wait(1)
        setter.start()
        rejected_while_held = finished.wait(.2)
    finally:
        release.set()
        holder.join(1)
        if setter.ident is not None:
            setter.join(1)
    assert not holder.is_alive() and not setter.is_alive()
    assert rejected_while_held, "deadline setter blocked behind a worker thread"
    assert outcome == ["rejected"]
    assert vector._embedding_deadline_ns == initial
    assert vector._lifecycle.acquire(blocking=False)
    vector._lifecycle.release()
    assert vector._pending_guard.acquire(blocking=False)
    vector._pending_guard.release()


def test_writer_uses_shared_callable_and_closes_it(tmp_path, monkeypatch):
    from system_memory_god.storage import daily_vector_worker as worker

    initial, command = deadline(), deadline()
    client = FakeEmbeddingClient()
    request = {"version": 1, "id": 1, "scope": ["u", "a", "c", "x"],
               "op": "warm_existing", "embedding_deadline_ns": command}
    received = iter([json.dumps(request).encode()])
    sent = []

    def recv(limit):
        try:
            return next(received)
        except StopIteration:
            raise EOFError from None

    connection = SimpleNamespace(send_bytes=sent.append, recv_bytes=recv, close=lambda: None)
    owner_closed = []
    monkeypatch.setattr(worker, "_owner_file", lambda root: SimpleNamespace(close=lambda: owner_closed.append(1)))

    class Index:
        def __init__(self, stores, embed):
            assert embed is client

        async def warm_existing(self, scope):
            assert client.deadlines[-1] == command

    monkeypatch.setattr(worker, "DailyVectorIndex", Index)
    worker.serve(connection, str(tmp_path), "bge-m3", embedding_client=client,
                 embedding_deadline_ns=initial)
    assert client.deadlines == [initial, command]
    assert client.calls == [["阿露记忆向量预热"]]
    assert client.closed and owner_closed == [1]
    assert json.loads(sent[-1])["ok"] is True


def test_snapshot_uses_same_shared_callable_and_operation_deadline(tmp_path, monkeypatch):
    from system_memory_god.storage import daily_vector_snapshot_worker as worker

    initial, first, second = deadline(), deadline(), deadline()
    client = FakeEmbeddingClient()
    rows = [
        {"version": 1, "id": 1, "scope": ["u", "a", "c", "x"], "op": "warm_snapshot",
         "generation": "generation", "embedding_deadline_ns": first},
        {"version": 1, "id": 2, "scope": ["u", "a", "c", "x"], "op": "search_cosine_hits",
         "query": "tea", "limit": 1, "embedding_deadline_ns": second},
    ]
    received = iter(json.dumps(row).encode() for row in rows)
    sent = []

    def recv(limit):
        try:
            return next(received)
        except StopIteration:
            raise EOFError from None

    connection = SimpleNamespace(send_bytes=sent.append, recv_bytes=recv, close=lambda: None)
    data = SimpleNamespace(ids=("m",), cosine_hits=lambda vector, limit: (("m", 1.0),))
    monkeypatch.setattr(worker, "load_snapshot", lambda *args, **kwargs: data)
    worker.serve_snapshot(connection, str(tmp_path), "bge-m3", embedding_client=client,
                          embedding_deadline_ns=initial)
    assert client.deadlines == [initial, first, second]
    assert client.calls[-1] == ["tea"]
    assert client.closed and json.loads(sent[-1])["result"] == [["m", 1.0]]


def test_factory_refuses_formal_shared_before_raw_or_stores(tmp_path, monkeypatch):
    from system_memory_god import memory_factory

    settings = SimpleNamespace(formal_embedding=object(), derived_root=tmp_path)
    def denied(*args, **kwargs):
        pytest.fail("resource created before formal/shared rejection")
    monkeypatch.setattr(memory_factory, "RawMessageService", denied)
    with pytest.raises(ValueError, match="formal"):
        memory_factory.create_memory_service(settings,
            embedding_clients={"writer": FakeEmbeddingClient(), "query": FakeEmbeddingClient()},
            embedding_deadline_ns=deadline())


def test_daily_runtime_routes_roles_and_updates_both_command_contexts(tmp_path, monkeypatch):
    from system_memory_god import daily_runtime
    from system_memory_god.storage import daily_vector_snapshot, supervised_daily_vector_snapshot

    configured = []
    updates = []

    class Transport:
        def __init__(self, *args, **kwargs):
            configured.append(kwargs)
            self.client = kwargs["embedding_client"]

        def set_embedding_deadline(self, value):
            updates.append((self.client, value))

    monkeypatch.setattr(daily_runtime, "SupervisedDailyVector", Transport)
    monkeypatch.setattr(supervised_daily_vector_snapshot, "SupervisedDailyVectorSnapshot", Transport)
    monkeypatch.setattr(daily_runtime, "DailyDerivation", lambda *args: object())
    monkeypatch.setattr(daily_runtime, "SupervisedDailyRetrieval", lambda *args, **kwargs: object())
    monkeypatch.setattr(daily_vector_snapshot, "DailyVectorSnapshot", lambda *args, **kwargs: object())
    monkeypatch.setattr(daily_runtime.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    settings = SimpleNamespace(formal_embedding=None, derived_root=tmp_path, connectors=())
    raw = SimpleNamespace(profile=settings, store=object(), configured_scopes=lambda: ())
    clients = {"writer": FakeEmbeddingClient(), "query": FakeEmbeddingClient()}
    initial, following = deadline(), deadline()
    runtime = daily_runtime.DailyMemoryRuntime(raw, settings, SimpleNamespace(raw_messages=raw),
        extractor=object(), embedding="bge-m3", semantic_mode="snapshot",
        embedding_clients=clients, embedding_deadline_ns=initial)
    assert [row["embedding_client"] for row in configured] == list(clients.values())
    assert all(row["embedding_deadline_ns"] == initial for row in configured)
    runtime.set_embedding_deadline(following)
    assert updates == [(clients["writer"], following), (clients["query"], following)]


@pytest.mark.parametrize("fails", [False, True])
def test_spawn_transfers_client_once_and_closes_parent_copy(tmp_path, monkeypatch, fails):
    from system_memory_god.storage import supervised_daily_vector as supervised

    client, absolute = FakeEmbeddingClient(), deadline()
    vector = SupervisedDailyVector(ScopedStores(tmp_path), embedding="bge-m3",
        embedding_client=client, embedding_deadline_ns=absolute)
    endpoints_closed = []
    parent = SimpleNamespace(close=lambda: endpoints_closed.append("parent"), poll=lambda timeout: True,
                             recv_bytes=lambda limit: b'{"version":1,"ready":true}')
    child = SimpleNamespace(close=lambda: endpoints_closed.append("child"))
    calls = []
    class Process:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def start(self):
            if fails:
                raise OSError("synthetic spawn failure")

        def is_alive(self):
            return True

    context = SimpleNamespace(Pipe=lambda: (parent, child), Process=Process)
    monkeypatch.setattr(supervised.multiprocessing, "get_context", lambda method: context)
    if fails:
        with pytest.raises(OSError):
            vector._start_locked(time.monotonic() + 5, threading.Event())
        assert set(endpoints_closed) == {"parent", "child"}
        assert vector._process is None
        with pytest.raises(RuntimeError, match="already attempted"):
            vector._start_locked(time.monotonic() + 5, threading.Event())
    else:
        vector._start_locked(time.monotonic() + 5, threading.Event())
        assert endpoints_closed == ["child"]
    assert client.parent_closed
    assert calls[0]["kwargs"] == {"embedding_client": client, "embedding_deadline_ns": absolute}


def test_index_construction_failure_releases_client_and_user_owner(tmp_path, monkeypatch):
    from system_memory_god.storage import daily_vector_worker as worker

    client = FakeEmbeddingClient()
    closed = []
    connection = SimpleNamespace(send_bytes=lambda body: None, close=lambda: closed.append("transport"))
    monkeypatch.setattr(worker, "_owner_file", lambda root: SimpleNamespace(close=lambda: closed.append("owner")))
    def broken_index(*args):
        raise RuntimeError("synthetic index constructor failure")
    monkeypatch.setattr(worker, "DailyVectorIndex", broken_index)
    with pytest.raises(RuntimeError):
        worker.serve(connection, str(tmp_path), "bge-m3", embedding_client=client,
                     embedding_deadline_ns=deadline())
    assert client.closed and closed == ["transport", "owner"]


def fake_spawn_embedding(texts):
    """Spawnable inference double; never imports an embedding model."""
    return [[1.0] + [0.0] * 1023 for _ in texts]


@pytest.mark.asyncio
async def test_spawn_users_share_one_fake_model_and_settle_generation(tmp_path):
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner

    owner = SharedEmbeddingOwner(embedder=fake_spawn_embedding)
    generation, vector = None, None
    try:
        await owner.start(time.monotonic_ns() + 30_000_000_000)
        for number in range(2):
            absolute = time.monotonic_ns() + 30_000_000_000
            generation = await owner.new_generation(absolute)
            vector = SupervisedDailyVector(ScopedStores(tmp_path / f"user-{number}"),
                embedding="bge-m3", embedding_client=generation.clients["writer"],
                embedding_deadline_ns=absolute)
            await asyncio.to_thread(vector._start_locked, time.monotonic() + 15, threading.Event())
            assert vector.worker_pid is not None
            await generation.prepare_close()
            await vector.close()
            await generation.close()
            vector, generation = None, None
        assert owner.stats["generation_count"] == 2
        assert owner.stats["load_count"] == 1
        assert owner.stats["ipc_peak"] == 1
        assert owner.stats["ipc_inflight"] == 0
    finally:
        if generation is not None:
            await generation.prepare_close()
        if vector is not None:
            await vector.close()
        if generation is not None:
            await generation.close()
        await owner.close()
    assert owner.stats["clean_close"]
