"""Challenge orchestration over real isolated Raw storage and a fake publisher."""

import asyncio
from types import SimpleNamespace

import pytest

from kernel.raw_message_models import ConflictError
from kernel.raw_message_service import RawMessageService
from system_memory_god.challenge_runtime import ChallengeRuntime


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
        assert options["semantic_mode"] == "snapshot"
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


async def add(service, *, user="alice", session="first", request="request-1", text="original"):
    return await service.add(user_id=user, session_id=session, request_id=request,
                             messages=[{"role": "user", "content": text}])


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
        assert reopened.health() == {"state": "ready", "version": "AMC-ENTRY-1.0"}
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
    switched = asyncio.create_task(add(service, user="bob"))
    await asyncio.sleep(.01)
    assert not pending.done() and len(factory.services) == 1
    factory.publish_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
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
