"""Deterministic handoff and readiness contracts; no models or live service."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
import time
import traceback

import httpx
import pytest

from system_memory_god.challenge_api import create_app
from system_memory_god.challenge_runtime import ChallengeRuntime
from system_memory_god.storage import shared_embedding_owner as shared


class Parent:
    def __init__(self):
        self._state = [0] * 16
        self.slot = [0] * 15
        self.slot[shared._GENERATION] = 1
        self.slot[shared._DEADLINE] = time.monotonic_ns() + 5_000_000_000
        self._slots = [self.slot]
        self._generations = {}
        self._local_failed = False
        self.commands = []
        self.failure = None
        self.failure_sites = []
        self.command_entered = asyncio.Event()
        self.command_release = None

    def _ensure_available(self):
        if self._local_failed:
            raise RuntimeError("synthetic failed owner")

    def _mark_failed(self):
        self.failure_sites.append([(frame.name, frame.lineno) for frame in traceback.extract_stack(limit=4)])
        self._local_failed = True
        self._state[shared._FAILED] = 1

    @asynccontextmanager
    async def _parent_state(self, deadline_ns=None, *, poison_on_timeout=True):
        yield

    async def _command(self, action, number, deadline, resources, *, deadline_ns, on_attempt=None):
        if on_attempt is not None:
            on_attempt()
        self.commands.append(action)
        self.command_entered.set()
        if self.command_release is not None:
            await self.command_release.wait()
        if self.failure is not None:
            raise self.failure
        self.slot[shared._DEADLINE] = deadline


class Client:
    def __init__(self, deadline):
        self._deadline_ns = deadline

    def bind_deadline(self, deadline):
        self._deadline_ns = deadline


class Fence:
    def __init__(self, transport):
        self.transport = transport

    @property
    def deadline_ns(self):
        return self.transport.deadline_ns

    def commit_deadline(self, deadline):
        assert self.transport.fenced
        self.transport.deadline_ns = deadline

    def release(self, *, failed=False):
        self.transport.failed = failed
        self.transport.fenced = False


class Transport:
    def __init__(self, deadline, *, busy=False):
        self.deadline_ns = deadline
        self.ready = asyncio.Event()
        if not busy:
            self.ready.set()
        self.fenced = self.failed = False

    async def acquire_deadline_fence(self, deadline_ns):
        self.fenced = True
        try:
            await asyncio.wait_for(self.ready.wait(), max(0, (deadline_ns - time.monotonic_ns()) / 1e9))
            return Fence(self)
        except BaseException:
            self.fenced = False
            raise


def components(*, busy=False):
    owner = Parent()
    old = owner.slot[shared._DEADLINE]
    generation = shared.SharedEmbeddingGeneration(owner, 1, 0,
        {role: Client(old) for role in ("writer", "query")})
    writer, reader = Transport(old), Transport(old, busy=busy)
    return owner, generation, writer, reader


@pytest.mark.asyncio
async def test_runtime_uses_quiescent_handoff_without_partial_commit(tmp_path):
    owner, generation, writer, reader = components(busy=True)
    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")
    binding = object()

    async def handoff(generation, deadline_ns):
        await generation.handoff_deadline(deadline_ns, (writer, reader))

    def old_split(deadline_ns):
        writer.deadline_ns = deadline_ns
        raise RuntimeError("synthetic reader still pending")

    service = SimpleNamespace(runtime=SimpleNamespace(
        handoff_embedding_deadline=handoff, set_embedding_deadline=old_split))
    runtime._binding, runtime._service, runtime._embedding_generation = binding, service, generation
    pending = asyncio.create_task(runtime._ensure_service(binding, time.monotonic() + 1))
    try:
        await asyncio.sleep(.02)
        assert not pending.done(), "legal busy was classified as fatal"
        assert not runtime._blocked and not owner.commands
        assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE]
        reader.ready.set()
        assert await pending is service
        assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE]
        assert not writer.fenced and not reader.fenced
    finally:
        reader.ready.set()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_terminal_publication_is_not_healthy_http_200(tmp_path):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")
    runtime._service = SimpleNamespace(runtime=SimpleNamespace(
        cached_readiness=lambda: {"terminal": True, "reason": "publisher"}))
    app = create_app(runtime, token="synthetic-credential-" + "x" * 32)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local.test") as client:
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["state"] == "unavailable"


@pytest.mark.asyncio
async def test_unsent_busy_timeout_keeps_all_old_deadlines():
    owner, generation, writer, reader = components(busy=True)
    old = writer.deadline_ns
    with pytest.raises(TimeoutError):
        await generation.handoff_deadline(time.monotonic_ns() + 20_000_000, (writer, reader))
    assert not owner.commands and not owner._local_failed, owner.failure_sites
    assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE] == old
    assert not writer.fenced and not reader.fenced


@pytest.mark.asyncio
async def test_attempted_control_failure_keeps_transports_failed_closed():
    owner, generation, writer, reader = components()
    owner.failure = TimeoutError("synthetic acknowledgement unknown")
    with pytest.raises(TimeoutError):
        await generation.handoff_deadline(time.monotonic_ns() + 1_000_000_000, (writer, reader))
    assert owner._local_failed and owner.commands == ["deadline"]
    assert writer.failed and reader.failed


@pytest.mark.asyncio
async def test_cancel_before_control_releases_only_its_uncommitted_fences():
    owner, generation, writer, reader = components(busy=True)
    pending = asyncio.create_task(generation.handoff_deadline(
        time.monotonic_ns() + 1_000_000_000, (writer, reader)))
    await asyncio.sleep(.01)
    pending.cancel()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not owner.commands and not owner._local_failed
    assert not writer.fenced and not reader.fenced


@pytest.mark.asyncio
async def test_cancel_after_control_waits_for_real_ack_and_complete_commit():
    owner, generation, writer, reader = components()
    owner.command_release = asyncio.Event()
    deadline = time.monotonic_ns() + 1_000_000_000
    pending = asyncio.create_task(generation.handoff_deadline(deadline, (writer, reader)))
    await owner.command_entered.wait()
    pending.cancel()
    pending.cancel()
    await asyncio.sleep(.01)
    assert not pending.done() and writer.fenced and reader.fenced
    assert owner.slot[shared._UPDATING] == 1
    owner.command_release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE] == deadline
    assert not owner._local_failed and not writer.fenced and not reader.fenced


@pytest.mark.asyncio
async def test_second_handoff_timeout_cannot_release_first_transactions_fences():
    owner, generation, writer, reader = components(busy=True)
    pending = asyncio.create_task(generation.handoff_deadline(
        time.monotonic_ns() + 1_000_000_000, (writer, reader)))
    await asyncio.sleep(.01)
    with pytest.raises(shared.DeadlineHandoffNotCommitted):
        await generation.handoff_deadline(time.monotonic_ns() + 20_000_000, (writer, reader))
    assert writer.fenced and reader.fenced and not owner._local_failed
    reader.ready.set()
    await pending
    assert owner.commands == ["deadline"]


@pytest.mark.asyncio
async def test_partial_local_commit_never_reopens_a_failed_generation(monkeypatch):
    owner, generation, writer, reader = components()
    original = Fence.commit_deadline

    def refuse_readback(self, deadline):
        if self.transport is not reader:
            original(self, deadline)

    monkeypatch.setattr(Fence, "commit_deadline", refuse_readback)
    with pytest.raises(RuntimeError, match="readback"):
        await generation.handoff_deadline(time.monotonic_ns() + 1_000_000_000, (writer, reader))
    assert owner._local_failed and writer.failed and reader.failed


@pytest.mark.parametrize("reason", ["capacity", "derived", "publisher", "reader", "ownerunknown"])
def test_health_only_terminal_cached_state_is_unavailable(tmp_path, reason):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")
    state = {"terminal": False, "reason": "none"}
    runtime._service = SimpleNamespace(runtime=SimpleNamespace(cached_readiness=lambda: dict(state)))
    assert runtime.health()["state"] == "ready"
    state.update(terminal=True, reason=reason)
    assert runtime.health()["state"] == "unavailable"
    assert runtime.diagnostic()["readiness"] == state


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,cause", [
    ("vector_capacity_exceeded", "capacity"), ("derived_unavailable", "derived"),
    ("semantic_publisher_failed", "publisher"), ("reader_unavailable", "reader")])
async def test_publication_diagnostic_keeps_closed_original_cause(tmp_path, reason, cause):
    from system_memory_god.challenge_runtime import PublicationUnavailableError

    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")

    async def unavailable(scope, *, deadline):
        return {"status": "unavailable", "reason": reason, "terminal": True}

    service = SimpleNamespace(scopes=(object(),), wait_until_published=unavailable)
    with pytest.raises(PublicationUnavailableError):
        await runtime._published(service, time.monotonic() + 1)
    assert runtime.diagnostic()["failure"]["cause"] == cause


@pytest.mark.asyncio
async def test_complete_replay_never_skips_current_publication(tmp_path):
    from tests.test_memory_challenge_runtime import FakeFactory, add, runtime
    from system_memory_god.challenge_runtime import PublicationUnavailableError

    factory = FakeFactory()
    host = runtime(tmp_path, factory)
    try:
        await add(host)
        service = factory.services[-1]
        factory.publication_status = "unavailable"

        async def no_second_write(*args, **kwargs):
            raise AssertionError("completed replay tried to write Raw again")

        service.add = no_second_write
        with pytest.raises(PublicationUnavailableError):
            await add(host)
        assert host.diagnostic()["failure"]["cause"] == "ownerunknown"
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_local_lane_failure_preserves_owner_but_other_lane_runs():
    from system_memory_god.challenge_admission import AdmissionClosed, BoundedAdmission

    blocked = [False, False]

    async def idle(lane):
        if blocked[lane]:
            raise RuntimeError("synthetic local owner still present")

    admission = BoundedAdmission(idle, lane_count=2)
    first = admission.reserve(time.monotonic() + 1)

    async def broken(lane):
        admission.cache_enter(lane, "alice", 1)
        blocked[lane] = True
        admission.quarantine_lane(lane, RuntimeError("synthetic local failure"))
        raise RuntimeError("synthetic local operation failed")

    with pytest.raises(RuntimeError):
        await admission.submit(first, broken, affinity_key="alice")
    await asyncio.sleep(0)
    assert not admission.closing and admission.in_flight == 1
    retry = admission.reserve(time.monotonic() + 1)
    with pytest.raises(AdmissionClosed):
        await admission.submit(retry, lambda lane: asyncio.sleep(0), affinity_key="alice")
    other = admission.reserve(time.monotonic() + 1)

    async def healthy(lane):
        assert lane == 1
        return "healthy"

    assert await admission.submit(other, healthy, affinity_key="bob") == "healthy"
    assert admission.in_flight == 1 and admission.pin_count == 1
    blocked[0] = False
    admission.cache_retired(0, "alice", 1)
    await admission.close()


def test_local_runtime_block_does_not_poison_healthy_shared_owner(tmp_path):
    from tests.test_memory_challenge_lanes import host, Factory

    runtime = host(tmp_path, Factory())
    runtime.for_lane(0)._block()
    assert runtime.for_lane(0)._blocked
    assert not runtime.for_lane(1)._blocked and not runtime._failed


def test_actual_shared_owner_death_blocks_every_lane(tmp_path):
    from tests.test_memory_challenge_lanes import host, Factory

    runtime = host(tmp_path, Factory())

    def dead_process():
        raise RuntimeError("synthetic owned process has exited")

    runtime._embedding_owner._ensure_available = dead_process
    runtime.for_lane(0)._block()
    assert runtime._failed and runtime.for_lane(1)._blocked


def test_publication_cause_reaches_closed_persistent_encoder(tmp_path, monkeypatch):
    from kernel.challenge_diagnostics import _encode_record
    from system_memory_god import challenge_runtime as module

    records = []

    def capture(stage, error):
        import json
        records.append(json.loads(_encode_record(stage, error, None, None)))

    monkeypatch.setattr(module, "emit_failure", capture)
    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")
    runtime._record_failure("published", module.PublicationUnavailableError("capacity"))
    assert records[0]["publication_cause"] == "capacity"
    assert records[0]["reason"] == "unknown"


@pytest.mark.asyncio
async def test_real_transport_handoff_drains_then_releases_new_rpc(tmp_path, monkeypatch):
    import threading
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.storage.scoped_stores import ScopedStores
    from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector

    owner, generation, _, _ = components()
    writer = SupervisedDailyVector(ScopedStores(tmp_path / "writer"), embedding="synthetic")
    reader = SupervisedDailyVector(ScopedStores(tmp_path / "reader"), embedding="synthetic")
    scope = PipelineScope("user", "agent", "connector", "conversation")
    entered, retired = threading.Event(), threading.Event()
    new_calls = []

    def old_call(*args, **kwargs):
        entered.set()
        assert retired.wait(1), "synthetic owner failed to retire"
        return True

    monkeypatch.setattr(reader, "_call", old_call)
    monkeypatch.setattr(writer, "_call", lambda *args, **kwargs: new_calls.append(args[0]) or True)
    prior = asyncio.create_task(reader._invoke("contains", scope, ids=("old",)))
    pending = new = None
    try:
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(.001)
        assert entered.is_set()
        deadline = time.monotonic_ns() + 1_000_000_000
        pending = asyncio.create_task(generation.handoff_deadline(deadline, (writer, reader)))
        for _ in range(100):
            if reader._deadline_fence is not None:
                break
            await asyncio.sleep(.001)
        assert reader._deadline_fence is not None and writer._deadline_fence is not None
        new = asyncio.create_task(writer._invoke("contains", scope, ids=("new",)))
        await asyncio.sleep(.005)
        assert not owner.commands and not new_calls and not pending.done()
        retired.set()
        assert await prior is True
        await pending
        assert await new is True
        assert writer._embedding_deadline_ns == reader._embedding_deadline_ns == deadline
        assert owner.slot[shared._DEADLINE] == deadline and not owner._local_failed
        assert new_calls == ["contains"]
    finally:
        retired.set()
        await asyncio.gather(*(task for task in (prior, pending, new) if task is not None),
                             return_exceptions=True)


@pytest.mark.asyncio
async def test_terminal_publication_stays_unavailable_after_cache_retirement(tmp_path):
    from system_memory_god.challenge_runtime import PublicationUnavailableError

    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")

    async def terminal(scope, *, deadline):
        return {"status": "unavailable", "reason": "semantic_publisher_failed", "terminal": True}

    service = SimpleNamespace(scopes=(object(),), wait_until_published=terminal)
    with pytest.raises(PublicationUnavailableError):
        await runtime._published(service, time.monotonic() + 1)
    runtime._service = None
    assert runtime.health()["state"] == "unavailable"
    assert runtime.cached_readiness() == {"terminal": True, "reason": "publisher"}
    with pytest.raises(RuntimeError, match="unavailable"):
        runtime._available()


@pytest.mark.asyncio
async def test_quarantine_retires_dequeued_unstarted_ticket_through_hook():
    from system_memory_god.challenge_admission import AdmissionClosed, BoundedAdmission

    idle_entered, idle_release, hook_entered, hook_release = (asyncio.Event() for _ in range(4))

    async def idle(lane):
        idle_entered.set()
        await idle_release.wait()
        raise RuntimeError("synthetic preexisting owner is unavailable")

    async def hook():
        hook_entered.set()
        await hook_release.wait()

    admission = BoundedAdmission(idle, lane_count=2)
    ticket = admission.reserve(time.monotonic() + 1, before_release=hook)
    waiter = asyncio.create_task(admission.submit(ticket, lambda lane: asyncio.sleep(0), affinity_key="alice"))
    try:
        await idle_entered.wait()
        admission.quarantine_lane(0, RuntimeError("synthetic local failure"))
        await asyncio.sleep(0)
        assert hook_entered.is_set(), "unstarted dequeued ticket was leaked"
        assert admission.in_flight == 1 and admission.pin_count == 1
        hook_release.set()
        await admission.wait_settled(ticket)
        assert admission.in_flight == 0 and admission.pin_count == 0
        with pytest.raises(AdmissionClosed):
            await waiter
    finally:
        idle_release.set()
        hook_release.set()
        await asyncio.gather(waiter, return_exceptions=True)
        await admission.close()


@pytest.mark.parametrize("offset", [-100_000_000, 0, 100_000_000])
def test_query_embedding_can_shorten_but_never_extend_generation_deadline(offset):
    import json
    import threading

    outer = time.monotonic_ns() + 1_000_000_000
    state, global_state = [0] * 15, [0] * 15
    state[shared._GENERATION], state[shared._DEADLINE] = 1, outer

    class Connection:
        sent = None

        def send_bytes(self, body):
            self.sent = json.loads(body)

        def poll(self, remaining):
            return True

        def recv_bytes(self, maximum):
            state[shared._QUERY_ID] = 0
            global_state[shared._INFLIGHT] -= 1
            result = {key: self.sent[key] for key in (
                "schema", "owner_nonce", "generation", "role", "request_id")}
            return json.dumps({**result, "op": "result", "ok": True,
                               "vector": [1.0] * 1024}).encode()

    connection = Connection()
    client = shared.SharedEmbeddingClient(connection, state, threading.Lock(), "a" * 64,
        1, "query", outer + offset, global_state=global_state)
    if offset > 0:
        with pytest.raises(RuntimeError, match="capacity unavailable"):
            client(["synthetic query"])
        assert connection.sent is None and not state[shared._QUERY_IO]
    else:
        assert client(["synthetic query"]) == [[1.0] * 1024]
        assert connection.sent["deadline_ns"] == outer + offset
        assert state[shared._DEADLINE] == outer
        assert not state[shared._QUERY_IO] and not global_state[shared._INFLIGHT]


@pytest.mark.asyncio
async def test_unsent_accounting_lock_timeout_does_not_poison_shared_owner():
    import threading

    owner, generation, writer, reader = components()
    owner._guard = threading.Lock()
    owner._parent_state = shared.SharedEmbeddingOwner._parent_state.__get__(owner)
    owner._guard.acquire()
    old = owner.slot[shared._DEADLINE]
    try:
        with pytest.raises(shared.DeadlineHandoffNotCommitted):
            await generation.handoff_deadline(time.monotonic_ns() + 10_000_000, (writer, reader))
        assert not owner._local_failed and not owner.commands
        assert not writer.failed and not reader.failed
        assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE] == old
    finally:
        owner._guard.release()


@pytest.mark.asyncio
async def test_unsent_control_lock_timeout_is_bounded_and_keeps_old_generation():
    owner, generation, writer, reader = components()
    owner._control_lock = asyncio.Lock()
    owner._io_tasks = set()
    owner._command = shared.SharedEmbeddingOwner._command.__get__(owner)
    owner._exchange = lambda *args: owner.commands.append("sent")
    await owner._control_lock.acquire()
    old = owner.slot[shared._DEADLINE]
    pending = asyncio.create_task(generation.handoff_deadline(
        time.monotonic_ns() + 10_000_000, (writer, reader)))
    try:
        done, _ = await asyncio.wait({pending}, timeout=.1)
        assert done, "control lock exceeded the original request deadline"
        with pytest.raises(shared.DeadlineHandoffNotCommitted):
            await pending
        assert not owner.commands and not owner._local_failed
        assert not writer.failed and not reader.failed
        assert writer.deadline_ns == reader.deadline_ns == owner.slot[shared._DEADLINE] == old
        assert not owner._io_tasks and not owner.slot[shared._UPDATING]
    finally:
        owner._control_lock.release()
        await asyncio.gather(pending, return_exceptions=True)
