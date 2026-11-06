"""Two real isolated Raw stores with fake derivation and embedding ownership."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from system_memory_god.challenge_api import create_app
from system_memory_god.challenge_lanes import ChallengeLanes
from tests.test_memory_challenge_runtime import FakeDeadlineRuntime, FakeFactory, FakeService


class Generation:
    def __init__(self):
        self.clients = {"writer": object(), "query": object()}

    async def set_deadline(self, deadline):
        pass

    async def prepare_close(self, *, deadline_ns=None):
        pass

    async def close(self, *, deadline_ns=None):
        pass


class Owner:
    max_generations = 2
    stats = {"failed": False, "stats_available": True}

    async def new_generation(self, deadline):
        return Generation()


class ParallelService(FakeService):
    async def start(self):
        self.started = True


class Factory(FakeFactory):
    def __call__(self, profile, **options):
        result = ParallelService(profile, self)
        result.runtime = FakeDeadlineRuntime()
        self.services.append(result)
        return result


def host(tmp_path, factory, **kwargs):
    return ChallengeLanes(tmp_path, client=object(), model="test-model",
        embedding_owner=Owner(), service_factory=factory, **kwargs)


async def add(runtime, user):
    return await runtime.add(user_id=user, session_id="session", request_id="request",
        messages=[{"role": "user", "content": "private marker " + user}])


@pytest.mark.asyncio
async def test_two_lanes_overlap_with_one_shared_journal_and_separate_evidence(tmp_path):
    factory = Factory()
    factory.publish_gate = asyncio.Event()
    phases = []
    instance = host(tmp_path, factory, observer=lambda *args: phases.append(args))
    requests = [asyncio.create_task(add(instance.for_lane(i), "user-" + str(i)))
                for i in range(2)]
    try:
        for _ in range(200):
            if len(factory.services) == 2 and all(s.publish_entered.is_set() for s in factory.services):
                break
            await asyncio.sleep(.01)
        assert len(factory.services) == 2
        assert all(s.publish_entered.is_set() for s in factory.services)
        assert all(instance.for_lane(i).busy for i in range(2))
        assert instance.for_lane(0)._store is instance.for_lane(1)._store
        assert factory.services[0].raw.profile.database != factory.services[1].raw.profile.database
        factory.publish_gate.set()
        assert all(row["success"] for row in await asyncio.gather(*requests))
        for i in range(2):
            rows = await instance.for_lane(i).search(user_id="user-" + str(i), query="marker", top_k=10)
            assert [row["content"] for row in rows] == ["private marker user-" + str(i)]
        assert phases and all(end >= start for _, _, _, start, end in phases)
    finally:
        factory.publish_gate.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await instance.close()


@pytest.mark.asyncio
async def test_close_attempts_other_lane_after_one_service_close_fails(tmp_path):
    factory = Factory()
    instance = host(tmp_path, factory)
    await asyncio.gather(*(add(instance.for_lane(i), "user-" + str(i)) for i in range(2)))
    failed = factory.services[0]
    original_close = failed.close

    async def broken_close():
        raise RuntimeError("controlled close failure")

    failed.close = broken_close
    try:
        with pytest.raises(RuntimeError):
            await instance.close()
        assert factory.services[1].closed
        assert instance.health()["state"] == "unavailable"
    finally:
        failed.close = original_close
        await instance.close()


@pytest.mark.asyncio
async def test_cache_retired_notification_follows_real_close_and_keeps_identity(tmp_path):
    factory = Factory()
    events = []
    instance = host(tmp_path, factory)
    instance.bind_admission(SimpleNamespace(
        cache_enter=lambda *args: events.append(("enter", *args)),
        cache_retired=lambda *args: events.append(("retire", *args))))
    first = instance.for_lane(0)
    await add(first, "first")
    service = factory.services[0]
    started, release = asyncio.Event(), asyncio.Event()
    original = service.close

    async def blocked_close():
        started.set()
        await release.wait()
        await original()

    service.close = blocked_close
    transition = asyncio.create_task(add(first, "second"))
    try:
        await asyncio.wait_for(started.wait(), 2)
        assert [event[0] for event in events] == ["enter"]
        release.set()
        await transition
        assert [event[0] for event in events] == ["enter", "retire", "enter"]
        assert events[0][1:] == events[1][1:]
        assert events[2][-1] > events[0][-1]
    finally:
        release.set()
        await asyncio.gather(transition, return_exceptions=True)
        await instance.close()


def test_two_lane_mode_requires_explicit_shared_model(tmp_path):
    with pytest.raises(ValueError):
        ChallengeLanes(tmp_path, client=object(), model="test-model", embedding_owner=None)


@pytest.mark.asyncio
async def test_failed_retire_cannot_report_idle_while_resources_remain(tmp_path):
    factory = Factory()
    instance = host(tmp_path, factory)
    lane = instance.for_lane(0)
    await add(lane, "first")
    generation = lane._embedding_generation
    original_prepare = generation.prepare_close

    async def expired_prepare(*, deadline_ns=None):
        raise TimeoutError("controlled expired retire")

    generation.prepare_close = expired_prepare
    try:
        with pytest.raises(RuntimeError):
            await add(lane, "second")
        with pytest.raises(RuntimeError, match="ownership"):
            await lane.wait_idle()
        assert lane.busy and lane.cache_user == "first"
        assert not factory.services[0].closed
    finally:
        generation.prepare_close = original_prepare
        await instance.close()


@pytest.mark.asyncio
async def test_runtime_and_admission_hold_ticket_for_unsettled_background_work(tmp_path, monkeypatch):
    from kernel.raw_message_service import RawMessageService

    factory = Factory()
    factory.fail_after_append = True
    instance = host(tmp_path, factory)
    token = "test-only-unsettled-token-" + "x" * 32
    app = create_app(instance, token=token)
    patch = monkeypatch.context()
    controlled = patch.__enter__()
    controlled.setattr(RawMessageService, "settlement_status", property(lambda _: "unsettled"))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://test") as client:
            response = await client.post("/add", headers={"Authorization": "Bearer " + token},
                json={"user_id": "one", "session_id": "s", "request_id": "r",
                      "messages": [{"role": "user", "content": "durable marker"}]})
        assert response.status_code == 503
        await asyncio.sleep(0)
        assert app.state.admission.in_flight == 1
        assert app.state.admission.pin_count == 1
        assert instance.for_lane(0).busy
        with pytest.raises(RuntimeError):
            await app.state.admission.close()
        assert app.state.admission.in_flight == 1
    finally:
        patch.__exit__(None, None, None)
        await instance.close()


@pytest.mark.asyncio
async def test_http_routes_users_to_distinct_lanes_and_preserves_cache_pins(tmp_path):
    factory = Factory()
    factory.publish_gate = asyncio.Event()
    instance = host(tmp_path, factory)
    token = "test-only-two-lane-token-" + "x" * 32
    app = create_app(instance, token=token)
    requests = []
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://test") as client:
            for user in ("user-0", "user-1"):
                requests.append(asyncio.create_task(client.post("/add",
                    headers={"Authorization": "Bearer " + token}, json={
                        "user_id": user, "session_id": "s", "request_id": "r",
                        "messages": [{"role": "user", "content": "private marker " + user}]})))
            for _ in range(200):
                if len(factory.services) == 2 and all(s.publish_entered.is_set() for s in factory.services):
                    break
                await asyncio.sleep(.01)
            assert len(factory.services) == 2
            assert all(s.publish_entered.is_set() for s in factory.services)
            factory.publish_gate.set()
            assert all(response.status_code == 200 for response in await asyncio.gather(*requests))
            for user in ("user-0", "user-1"):
                response = await client.post("/search", headers={"Authorization": "Bearer " + token},
                    json={"user_id": user, "query": "marker", "top_k": 10})
                assert response.status_code == 200
                assert [row["content"] for row in response.json()["data"]] == ["private marker " + user]
    finally:
        factory.publish_gate.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await app.state.admission.close()
        await instance.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("search_order", [tuple(range(16)), tuple(reversed(range(16)))])
async def test_http_full_sixteen_user_add_then_search_retiring_both_cached_users(tmp_path, search_order):
    class SwitchingService(ParallelService):
        async def start(self):
            await asyncio.sleep(0)
            await super().start()

        async def close(self):
            await asyncio.sleep(0)
            await super().close()

    class SwitchingFactory(Factory):
        def __call__(self, profile, **options):
            result = SwitchingService(profile, self)
            result.runtime = FakeDeadlineRuntime()
            self.services.append(result)
            return result

    factory = SwitchingFactory()
    instance = host(tmp_path, factory)
    token = "test-only-complete-cycle-token-" + "x" * 32
    app = create_app(instance, token=token)
    headers = {"Authorization": "Bearer " + token}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://test") as client:
            added = await asyncio.gather(*(client.post("/add", headers=headers, json={
                "user_id": f"user-{number}", "session_id": "session", "request_id": "request",
                "messages": [{"role": "user", "content": f"private marker user-{number:03d}"}],
            }) for number in range(16)))
            assert [response.status_code for response in added] == [200] * 16
            await app.state.admission.wait_empty()
            assert len(factory.services) == 16
            assert sum(not service.closed for service in factory.services) == 2
            assert app.state.admission.pin_count == 2

            searched = await asyncio.gather(*(client.post("/search", headers=headers, json={
                "user_id": f"user-{number}", "query": "marker", "top_k": 100,
            }) for number in search_order))
            assert [response.status_code for response in searched] == [200] * 16
            for number, response in zip(search_order, searched):
                assert [row["content"] for row in response.json()["data"]] == [
                    f"private marker user-{number:03d}"]
            await app.state.admission.wait_empty()
            assert not app.state.admission.closing
            assert app.state.admission.in_flight == 0 and app.state.admission.pin_count == 2
    finally:
        await app.state.admission.close()
        await instance.close()
    assert all(service.closed for service in factory.services)
