"""Challenge orchestration over real isolated Raw storage and a fake publisher."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from kernel.raw_message_models import ConflictError
from kernel.raw_message_service import RawMessageService
from system_memory_god.challenge_runtime import ChallengeRuntime


class FakeDeadlineRuntime:
    """Legacy orchestration fixtures supply the new handoff facade explicitly."""

    def __init__(self, observe=lambda deadline: None):
        self.observe = observe

    async def handoff_embedding_deadline(self, generation, deadline):
        await generation.set_deadline(deadline)
        self.observe(deadline)

    def cached_readiness(self):
        return {"terminal": False, "reason": "none"}


class FakeFactory:
    def __init__(self):
        self.services = []
        self.events = []
        self.publication_status = "current"
        self.publish_gate = None
        self.fail_after_append = False
        self.fail_close = False

    def __call__(self, profile, **options):
        assert options["mode"] == "memory-full-v1"
        assert options["embedding"] == "bge-m3"
        assert options["semantic_mode"] == "paged_exact"
        assert profile.derivation_input_chars == 81920
        service = FakeService(profile, self)
        self.services.append(service)
        return service


class FakeService:
    def __init__(self, profile, factory):
        self.raw = RawMessageService(profile)
        self.scopes = self.raw.configured_scopes()
        self.factory = factory
        self.started = False
        self.closed = False
        self.last_query = None
        self.publish_entered = asyncio.Event()

    async def start(self):
        assert not any(s.started and not s.closed for s in self.factory.services if s is not self)
        self.started = True
        self.factory.events.append(("start", self.scopes[0].user_id))

    async def add(self, scope, draft):
        saved = await self.raw.append(scope, draft)
        if self.factory.fail_after_append:
            self.factory.fail_after_append = False
            raise RuntimeError("fake failure after durable write")
        return SimpleNamespace(saved=True, message=saved)

    async def wait_until_published(self, scope, *, deadline):
        self.publish_entered.set()
        if self.factory.publish_gate is not None:
            await self.factory.publish_gate.wait()
        return {"status": self.factory.publication_status, "terminal": False}

    async def search(self, scope, query, **options):
        self.last_query = query
        self.last_options = options
        rows = await self.raw.replay(scope, scope.conversation_id)
        return SimpleNamespace(status="ready", items=[{
            "source_message_id": row.message_id,
            "source_content": row.content,
            "source_time": row.created_at,
        } for row in rows])


    async def close(self):
        if self.factory.fail_close:
            raise RuntimeError("fake close failure")
        await self.raw.close()
        self.closed = True
        self.factory.events.append(("close", self.scopes[0].user_id))


def runtime(tmp_path, factory):
    return ChallengeRuntime(tmp_path, client=object(), model="test-model", service_factory=factory)


class FakeEmbeddingGeneration:
    def __init__(self, owner, deadline):
        self.owner = owner
        self.clients = {"writer": object(), "query": object()}
        self.deadlines = [deadline]
        self.closing = False
        self.closed = False

    async def set_deadline(self, deadline):
        assert not self.closing
        self.deadlines.append(deadline)

    async def prepare_close(self, *, deadline_ns=None):
        self.closing = True
        self.owner.events.append("prepare_generation")

    async def close(self, *, deadline_ns=None):
        assert self.closing
        self.owner.events.append("close_generation")
        self.closed = True


class FakeEmbeddingOwner:
    def __init__(self):
        self.generations = []
        self.events = []

    async def new_generation(self, deadline):
        assert all(g.closed for g in self.generations)
        generation = FakeEmbeddingGeneration(self, deadline)
        self.generations.append(generation)
        return generation


@pytest.mark.asyncio
async def test_shared_embedding_survives_user_switch_and_closes_old_generation(tmp_path):
    factory = FakeFactory()
    original_factory = factory.__call__
    owner = FakeEmbeddingOwner()
    options_seen = []

    def injected_factory(profile, **options):
        options_seen.append(options)
        service = original_factory(profile, **options)
        service.runtime = FakeDeadlineRuntime(lambda d: owner.events.append(("D", d)))
        original_close = service.close

        async def close():
            assert owner.generations[-1].closing
            owner.events.append("close_service")
            await original_close()

        service.close = close
        return service

    service = ChallengeRuntime(tmp_path, client=object(), model="test-model",
                               service_factory=injected_factory, embedding_owner=owner)
    try:
        await add(service)
        first = owner.generations[0]
        await add(service, request="request-2")
        assert len(owner.generations) == 1 and len(first.deadlines) == 2
        await add(service, user="bob")
        assert len(owner.generations) == 2 and first.closed
        assert options_seen[0]["embedding_clients"] is first.clients
        assert type(options_seen[0]["embedding_deadline_ns"]) is int
        assert owner.events.index("prepare_generation") < owner.events.index("close_service")
        assert owner.events.index("close_service") < owner.events.index("close_generation")
    finally:
        await service.close()
    assert all(g.closed for g in owner.generations)


@pytest.mark.asyncio
async def test_shared_generation_is_retained_when_user_service_close_fails(tmp_path):
    factory = FakeFactory()
    owner = FakeEmbeddingOwner()

    def injected_factory(profile, **options):
        service = factory(profile, **options)
        service.runtime = FakeDeadlineRuntime()
        return service

    service = ChallengeRuntime(tmp_path, client=object(), model="test-model",
                               service_factory=injected_factory, embedding_owner=owner)
    await add(service)
    factory.fail_close = True
    try:
        with pytest.raises(RuntimeError, match="close failed"):
            await add(service, user="bob")
        assert len(owner.generations) == 1
        assert not owner.generations[0].closed
        assert service.health()["state"] == "unavailable"
    finally:
        factory.fail_close = False
        await service.close()
    assert owner.generations[0].closed


@pytest.mark.asyncio
async def test_shared_deadline_registration_failure_blocks_runtime(tmp_path):
    factory, owner = FakeFactory(), FakeEmbeddingOwner()

    def injected_factory(profile, **options):
        service = factory(profile, **options)
        service.runtime = FakeDeadlineRuntime()
        return service

    service = ChallengeRuntime(tmp_path, client=object(), model="test-model",
                               service_factory=injected_factory, embedding_owner=owner)
    try:
        await add(service)

        async def failed_registration(deadline):
            raise RuntimeError("fake shared owner failed")

        owner.generations[0].set_deadline = failed_registration
        with pytest.raises(RuntimeError, match="shared"):
            await add(service, request="request-2")
        assert service.health()["state"] == "unavailable"
        assert len(factory.services) == 1 and not owner.generations[0].closed
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_options_reach_scoped_service_as_hints_without_changing_original_query(tmp_path):
    from kernel.daily_memory_contracts import memory_retrieval_hints

    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    query = "original question " * 80
    try:
        await add(service)
        result = await service.search(user_id="alice", query=query, top_k=100,
                                      options=["Alice", "Carol"])
        current = factory.services[0]
        assert current.last_query == query
        assert current.last_options["retrieval_hints"] == memory_retrieval_hints(["Alice", "Carol"])
        assert result[0]["content"] == "original"
        await service.search(user_id="alice", query=query, top_k=100)
        assert "retrieval_hints" not in current.last_options
    finally:
        await service.close()


async def add(service, *, user="alice", session="first", request="request-1", text="original"):
    return await service.add(user_id=user, session_id=session, request_id=request,
                             messages=[{"role": "user", "content": text}])


@pytest.mark.asyncio
async def test_wait_idle_keeps_timed_out_publication_owner_until_settled(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    waiter = None
    try:
        await asyncio.wait_for(service.wait_idle(), timeout=1)
        await add(service)
        factory.services[0].publish_entered.clear()
        factory.publish_gate = asyncio.Event()
        service._timeout = .05
        request = asyncio.create_task(add(service, request="blocked"))
        await asyncio.wait_for(factory.services[0].publish_entered.wait(), timeout=1)
        waiter = asyncio.create_task(service.wait_idle())
        await asyncio.sleep(0)
        assert not waiter.done()
        with pytest.raises(TimeoutError):
            await request
        assert service.busy
        assert not waiter.done()
        factory.publish_gate.set()
        await asyncio.wait_for(waiter, timeout=1)
        assert not service.busy
    finally:
        if factory.publish_gate is not None:
            factory.publish_gate.set()
        if waiter is not None and not waiter.done():
            await waiter
        await service.close()


@pytest.mark.asyncio
async def test_wait_idle_survives_waiter_cancellation_without_canceling_owner(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    try:
        await add(service)
        factory.services[0].publish_entered.clear()
        factory.publish_gate = asyncio.Event()
        request = asyncio.create_task(add(service, request="canceled-waiter"))
        await asyncio.wait_for(factory.services[0].publish_entered.wait(), timeout=1)
        idle_waiter = asyncio.create_task(service.wait_idle())
        await asyncio.sleep(0)
        idle_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await idle_waiter
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert service.busy
        assert service._tasks
        factory.publish_gate.set()
        await asyncio.wait_for(service.wait_idle(), timeout=1)
        assert not service.busy
    finally:
        if factory.publish_gate is not None:
            factory.publish_gate.set()
        await service.close()


@pytest.mark.asyncio
async def test_publication_is_required_and_users_switch_only_after_close(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    try:
        assert await add(service) == {"success": True, "user_id": "alice",
                                     "session_id": "first", "request_id": "request-1"}
        await add(service, session="second", request="request-2", text="another session")
        assert [item["content"] for item in await service.search(
            user_id="alice", query="original", top_k=100)] == ["original", "another session"]
        await add(service, user="bob", text="only bob")
        assert [item["content"] for item in await service.search(
            user_id="bob", query="only", top_k=100)] == ["only bob"]
        assert [item["content"] for item in await service.search(
            user_id="alice", query="original", top_k=100)] == ["original", "another session"]
        assert [event[0] for event in factory.events] == ["start", "close", "start", "close", "start"]
        assert await service.search(user_id="unknown", query="anything", top_k=10) == []
        assert len(factory.services) == 3
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_identical_retry_and_restart_reuse_stable_message_and_timestamp(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    await add(service)
    first = (await factory.services[-1].raw.replay(
        factory.services[-1].scopes[0], factory.services[-1].scopes[0].conversation_id))[0]
    await add(service)
    with pytest.raises(ConflictError):
        await add(service, text="different content")
    await service.close()
    reopened = runtime(tmp_path, factory)
    try:
        await add(reopened)
        rows = await factory.services[-1].raw.replay(
            factory.services[-1].scopes[0], factory.services[-1].scopes[0].conversation_id)
        assert rows == [first]
        assert reopened.health() == {"state": "ready", "version": "ARU-RECOVERY-2"}
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_partial_durable_write_is_replayed_without_duplicate(tmp_path):
    factory = FakeFactory()
    factory.fail_after_append = True
    service = runtime(tmp_path, factory)
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(service)
        await add(service)
        assert len(await factory.services[-1].raw.replay(
            factory.services[-1].scopes[0], factory.services[-1].scopes[0].conversation_id)) == 1
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_failed_publication_and_recovery_never_acknowledge_or_fake_empty(tmp_path):
    factory = FakeFactory()
    factory.publication_status = "unavailable"
    service = runtime(tmp_path, factory)
    try:
        with pytest.raises(RuntimeError, match="publication unavailable"):
            await add(service)
        with pytest.raises(RuntimeError, match="publication unavailable"):
            await service.search(user_id="alice", query="original", top_k=10)
        factory.publication_status = "current"
        await add(service)
        assert (await service.search(user_id="alice", query="original", top_k=10))[0]["content"] == "original"
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_close_failure_retains_owner_and_blocks_replacement(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    await add(service)
    factory.fail_close = True
    with pytest.raises(RuntimeError):
        await add(service, user="bob")
    assert service.health()["state"] == "unavailable"
    assert len(factory.services) == 1 and not factory.services[0].closed
    with pytest.raises(RuntimeError):
        await add(service, user="charlie")
    factory.fail_close = False
    await service.close()
    assert factory.services[0].closed


@pytest.mark.asyncio
async def test_cancelled_request_settles_publication_before_user_switch(tmp_path):
    factory = FakeFactory()
    factory.publish_gate = asyncio.Event()
    service = runtime(tmp_path, factory)
    pending = asyncio.create_task(add(service))
    while not factory.services:
        await asyncio.sleep(.001)
    await factory.services[0].publish_entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, .5)
    switched = asyncio.create_task(add(service, user="bob"))
    await asyncio.sleep(.01)
    assert pending.done() and service.busy and len(factory.services) == 1
    factory.publish_gate.set()
    await switched
    await service.close()
    assert all(item.closed for item in factory.services)


@pytest.mark.asyncio
async def test_original_long_query_is_forwarded_and_invalid_batch_has_no_write(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    try:
        with pytest.raises(ValueError):
            await service.add(user_id="alice", session_id="first", request_id="bad", messages=[
                {"role": "user", "content": "would be valid"}, {"role": "system", "content": "invalid"}])
        assert not factory.services
        await add(service, user="../../foreign")
        query = "unaltered question " * 100
        await service.search(user_id="../../foreign", query=query, top_k=100)
        assert factory.services[-1].last_query == query
        assert factory.services[-1].raw.profile.database.is_relative_to(tmp_path / "users")
        with pytest.raises(ValueError):
            await service.search(user_id="../../foreign", query="q" * 32769, top_k=100)
    finally:
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["close", "factory", "initialize", "start"])
async def test_deadline_returns_while_transition_owner_retains_late_result(tmp_path, monkeypatch, stage):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    entered, release = asyncio.Event(), asyncio.Event()
    thread_entered, thread_release = threading.Event(), threading.Event()
    if stage == "close":
        await add(service)
        original = factory.services[0].close

        async def blocked_close():
            entered.set()
            await release.wait()
            await original()

        monkeypatch.setattr(factory.services[0], "close", blocked_close)
    elif stage == "factory":
        def blocked_factory(profile, **options):
            thread_entered.set()
            assert thread_release.wait(5)
            return factory(profile, **options)

        service._factory = blocked_factory
    else:
        target, name = ((RawMessageService, "initialize_profile") if stage == "initialize"
                        else (FakeService, "start"))
        original = getattr(target, name)

        async def blocked(self):
            entered.set()
            await release.wait()
            await original(self)

        monkeypatch.setattr(target, name, blocked)
    # This deadline tests a blocked transition, not filesystem startup speed.
    service._timeout = .2
    pending = asyncio.create_task(add(service, user="bob"))
    try:
        if stage == "factory":
            assert await asyncio.to_thread(thread_entered.wait, 2)
        else:
            await asyncio.wait_for(entered.wait(), 2)
        done, _ = await asyncio.wait({pending}, timeout=.5)
        assert pending in done, "request deadline must not wait for transition cleanup"
        with pytest.raises(TimeoutError):
            pending.result()
        assert service.busy and len(service._tasks) == 1
        if stage == "close":
            assert len(factory.services) == 1 and not factory.services[0].closed
        else:
            assert not factory.services or not factory.services[-1].closed
        # Windows timers may wake one clock tick early. Release the blocked
        # startup only after the absolute monotonic deadline has also passed.
        await asyncio.sleep(.03)
    finally:
        release.set()
        thread_release.set()
        await asyncio.gather(*tuple(service._tasks), return_exceptions=True)
        await asyncio.gather(pending, return_exceptions=True)
        await asyncio.sleep(0)
        try:
            assert not service.busy and service._service is None
            assert all(item.closed for item in factory.services)
            assert service.health()["state"] == "ready"
        finally:
            await service.close()
    assert all(item.closed for item in factory.services)
    if stage == "close":
        assert len(factory.services) == 1


@pytest.mark.asyncio
async def test_direct_lock_wait_expires_without_starting_an_owner(tmp_path):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    service._timeout = .03
    await service._lease.acquire()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(add(service), .5)
        await asyncio.sleep(.01)
        assert not factory.services and not service.busy
    finally:
        service._lease.release()
        await service.close()


@pytest.mark.asyncio
async def test_checkpoint_after_deadline_settles_before_retry_without_duplicate(tmp_path, monkeypatch):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    await add(service, request="seed")
    entered, release = asyncio.Event(), asyncio.Event()
    original = service._store.checkpoint

    async def blocked_checkpoint(*args):
        entered.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(service._store, "checkpoint", blocked_checkpoint)
    service._timeout = .05
    pending = asyncio.create_task(add(service, request="slow", text="durable original"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(pending, .5)
        assert service.busy
        release.set()
        await asyncio.gather(*tuple(service._tasks), return_exceptions=True)
        monkeypatch.setattr(service._store, "checkpoint", original)
        service._timeout = 2
        assert (await add(service, request="slow", text="durable original"))["success"]
        rows = await factory.services[0].raw.replay(
            factory.services[0].scopes[0], factory.services[0].scopes[0].conversation_id)
        assert [row.content for row in rows] == ["original", "durable original"]
    finally:
        release.set()
        await service.close()


@pytest.mark.asyncio
async def test_shutdown_waits_for_cancelled_request_owner(tmp_path):
    factory = FakeFactory()
    factory.publish_gate = asyncio.Event()
    service = runtime(tmp_path, factory)
    pending = asyncio.create_task(add(service))
    while not factory.services:
        await asyncio.sleep(.001)
    await factory.services[0].publish_entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, .5)
    closing = asyncio.create_task(service.close())
    await asyncio.sleep(.01)
    assert not closing.done() and not factory.services[0].closed
    factory.publish_gate.set()
    await asyncio.wait_for(closing, 2)
    assert factory.services[0].closed and service.health()["state"] == "stopped"


@pytest.mark.asyncio
async def test_close_failure_after_waiter_timeout_retains_owner_and_blocks_replacement(tmp_path, monkeypatch):
    factory = FakeFactory()
    service = runtime(tmp_path, factory)
    await add(service)
    old = factory.services[0]
    entered, release = asyncio.Event(), asyncio.Event()
    original = old.close

    async def failed_close():
        entered.set()
        await release.wait()
        raise RuntimeError("private delayed close failure")

    monkeypatch.setattr(old, "close", failed_close)
    service._timeout = .03
    pending = asyncio.create_task(add(service, user="bob"))
    try:
        await entered.wait()
        with pytest.raises(TimeoutError):
            await pending
        assert service.busy and service._service is old
        release.set()
        await asyncio.gather(*tuple(service._tasks), return_exceptions=True)
        assert service.health()["state"] == "unavailable" and service._service is old
        with pytest.raises(RuntimeError, match="unavailable"):
            await add(service, user="charlie")
        assert len(factory.services) == 1 and not old.closed
    finally:
        release.set()
        monkeypatch.setattr(old, "close", original)
        await service.close()
