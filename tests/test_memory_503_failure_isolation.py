"""Failed requests release admission only after their real resources retire."""

import asyncio
import threading
import time
import pytest

from system_memory_god.challenge_admission import AdmissionClosed, BoundedAdmission
from system_memory_god.challenge_lanes import ChallengeLanes
from system_memory_god.challenge_runtime import ChallengeRuntime
from tests.test_memory_challenge_lanes import Factory, Owner
from tests.test_memory_challenge_runtime import FakeFactory, add


def runtime(tmp_path, factory, **options):
    return ChallengeRuntime(tmp_path, client=object(), model="test-model",
                            service_factory=factory, **options)


@pytest.mark.asyncio
async def test_local_failure_retires_service_before_reusing_lane(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    factory.fail_after_append = True
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert old.closed and instance.cache_user is None
        assert instance.health()["state"] == "ready"
        assert (await add(instance, user="next"))["success"]
        assert len(factory.services) == 2
    finally:
        await instance.close()


@pytest.mark.asyncio
async def test_failure_keeps_direct_caller_behind_cleanup_lease(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    release, entered = asyncio.Event(), asyncio.Event()
    original_close = old.close

    async def close():
        entered.set()
        await release.wait()
        await original_close()

    old.close = close
    factory.fail_after_append = True
    following = None
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.wait_for(entered.wait(), 1)
        following = asyncio.create_task(add(instance, user="next"))
        await asyncio.sleep(.01)
        assert not following.done() and instance.busy
        assert len(factory.services) == 1 and instance.cache_user == "alice"
        assert not instance._idle.is_set()
        release.set()
        assert (await asyncio.wait_for(following, 2))["success"]
        assert old.closed
    finally:
        release.set()
        if following is not None:
            await asyncio.gather(following, return_exceptions=True)
        await instance.close()


@pytest.mark.asyncio
async def test_expired_business_deadline_uses_fresh_bounded_cleanup_budget(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    release = asyncio.Event()
    factory.publish_gate = release
    old.publish_entered.clear()
    instance._timeout = .04
    request = asyncio.create_task(add(instance, request="expired"))
    try:
        await asyncio.wait_for(old.publish_entered.wait(), 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(request, .5)
        assert instance.busy and not old.closed
        await asyncio.sleep(.03)  # Windows timers can deliver the waiter one tick early.
        release.set()
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert old.closed and instance.cache_user is None
        assert instance.health()["state"] == "ready"
    finally:
        release.set()
        await instance.close()


@pytest.mark.asyncio
async def test_cleanup_timeout_retains_actual_owner_and_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr("system_memory_god.challenge_runtime.CLEANUP_TIMEOUT", .03,
                        raising=False)
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    entered, release = asyncio.Event(), asyncio.Event()
    original_close = old.close

    async def close():
        entered.set()
        await release.wait()
        await original_close()

    old.close = close
    factory.fail_after_append = True
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(.06)
        assert instance.health()["state"] == "unavailable"
        assert instance.busy and instance._service is old
        assert instance._tasks and not instance._idle.is_set()
        with pytest.raises(RuntimeError, match="unavailable"):
            await add(instance, user="next")
        release.set()
        await asyncio.gather(*tuple(instance._tasks), return_exceptions=True)
        with pytest.raises(RuntimeError, match="ownership"):
            await instance.wait_idle()
        assert instance._service is old and instance.cache_user == "alice"
    finally:
        release.set()
        old.close = original_close
        await instance.close()


@pytest.mark.asyncio
async def test_retirement_sequence_fences_raw_generation_cache_and_ticket(tmp_path):
    factory = Factory()
    events = []
    instance = runtime(tmp_path, factory, embedding_owner=Owner(),
        cache_observer=lambda event, *_: events.append("cache_" + event))
    admission = BoundedAdmission(instance.wait_idle)
    ticket = admission.reserve(time.monotonic() + 5)

    async def operation():
        await add(instance)
        old = factory.services[0]
        generation = instance._embedding_generation
        original_close = old.close

        async def close():
            await original_close()
            assert old.raw.settlement_status == "settled"
            events.append("service_close")

        async def generation_close(*, deadline_ns=None):
            assert old.closed and old.raw.settlement_status == "settled"
            assert deadline_ns > time.monotonic_ns()
            events.append("generation_close")

        old.close, generation.close = close, generation_close
        factory.fail_after_append = True
        await add(instance, request="failed")

    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await admission.submit(ticket, operation)
        await asyncio.wait_for(admission.wait_empty(), 2)
        events.append("ticket_done")
        assert events == ["cache_enter", "service_close", "generation_close",
                          "cache_retired", "ticket_done"]
        assert not admission.closing and not instance.busy
    finally:
        await admission.close()
        await instance.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepare", "service", "generation", "cache", "raw"])
async def test_cleanup_failures_remain_blocked_and_keep_identity(tmp_path, monkeypatch, stage):
    factory = Factory()
    instance = runtime(tmp_path, factory, embedding_owner=Owner())
    await add(instance)
    old, generation = factory.services[0], instance._embedding_generation
    original_close = old.close
    original_generation_close = generation.close
    original_prepare = generation.prepare_close

    async def broken(*args, **kwargs):
        raise RuntimeError("private failure")

    def broken_cache(*args):
        raise RuntimeError("private cache identity failure")

    if stage == "prepare":
        generation.prepare_close = broken
    elif stage == "service":
        old.close = broken
    elif stage == "generation":
        generation.close = broken
    elif stage == "cache":
        instance._cache_observer = broken_cache
    else:
        # A successful-looking close cannot replace evidence of Raw retirement.
        monkeypatch.setattr(type(old.raw), "settlement_status", property(lambda _: "unsettled"))
    factory.fail_after_append = True
    try:
        with pytest.raises((RuntimeError, AttributeError)):
            await add(instance, request="failed")
        await asyncio.gather(*tuple(instance._tasks), return_exceptions=True)
        assert instance.health()["state"] == "unavailable"
        assert instance.cache_user == "alice" and instance.busy
        with pytest.raises(RuntimeError, match="ownership"):
            await instance.wait_idle()
    finally:
        old.close = original_close
        generation.close = original_generation_close
        generation.prepare_close = original_prepare
        instance._cache_observer = None
        monkeypatch.undo()
        await instance.close()


@pytest.mark.asyncio
async def test_pending_raw_thread_is_retained_until_real_io_finishes(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    entered, release = threading.Event(), threading.Event()

    def late_io():
        entered.set()
        assert release.wait(5)

    writer = asyncio.create_task(asyncio.to_thread(late_io))
    old.raw._track_write(writer)
    factory.fail_after_append = True
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.sleep(.01)
        assert old.raw.settlement_status == "unsettled"
        assert instance.busy and instance.cache_user == "alice" and not old.closed
        assert instance._tasks and not writer.done()
        release.set()
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert writer.done() and old.closed and instance.cache_user is None
    finally:
        release.set()
        await instance.close()


@pytest.mark.asyncio
async def test_live_worker_daemon_must_exit_before_retirement(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    release = asyncio.Event()
    daemon = asyncio.create_task(release.wait())

    class Worker:
        @property
        def settlement_status(self):
            return "settled" if daemon.done() else "unsettled"

        async def close(self):
            await asyncio.shield(daemon)

    old.raw.worker = Worker()
    factory.fail_after_append = True
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.sleep(.01)
        assert not daemon.done() and instance.busy and not old.closed
        assert instance.health()["state"] == "ready"
        release.set()
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert daemon.done() and old.closed and instance.cache_user is None
    finally:
        release.set()
        await instance.close()


@pytest.mark.asyncio
async def test_two_lane_local_failure_does_not_poison_healthy_lane(tmp_path):
    factory = Factory()
    host = ChallengeLanes(tmp_path, client=object(), model="test-model",
                           service_factory=factory, embedding_owner=Owner())
    admission = BoundedAdmission(host.wait_idle, lane_count=2)
    host.bind_admission(admission)
    release, entered = asyncio.Event(), asyncio.Event()

    async def failed(lane):
        instance = host.for_lane(lane)
        await add(instance)
        old = factory.services[0]
        original_close = old.close

        async def close():
            entered.set()
            await release.wait()
            await original_close()

        old.close = close
        factory.fail_after_append = True
        await add(instance, request="failed")

    first = admission.reserve(time.monotonic() + 5)
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await admission.submit(first, failed, affinity_key="alice")
        await asyncio.wait_for(entered.wait(), 1)
        assert admission.in_flight == 1 and not admission.closing
        second = admission.reserve(time.monotonic() + 5)
        result = await admission.submit(second,
            lambda lane: add(host.for_lane(lane), user="bob"), affinity_key="bob")
        assert result["success"] and host.health()["state"] == "ready"
        release.set()
        await asyncio.wait_for(admission.wait_empty(), 2)
        assert not admission.closing and admission.pin_count == 1
        third = admission.reserve(time.monotonic() + 5)
        assert (await admission.submit(third,
            lambda lane: add(host.for_lane(lane), user="next"),
            affinity_key="next"))["success"]
    finally:
        release.set()
        await admission.close()
        await host.close()


@pytest.mark.asyncio
async def test_repeated_idle_waiter_cancellation_keeps_cleanup_owner(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    old = factory.services[0]
    original_close = old.close
    release = asyncio.Event()

    async def close():
        await release.wait()
        await original_close()

    old.close = close
    factory.fail_after_append = True
    try:
        with pytest.raises(RuntimeError):
            await add(instance, request="failed")
        owner = next(iter(instance._cleanup_tasks))
        for _ in range(3):
            waiter = asyncio.create_task(instance.wait_idle())
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert not owner.done() and not owner.cancelled()
            assert instance._lease.locked() and instance.busy
        release.set()
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert owner.done() and old.closed
    finally:
        release.set()
        await instance.close()


@pytest.mark.asyncio
async def test_direct_lock_wait_timeout_does_not_poison_existing_cached_owner(tmp_path):
    factory = FakeFactory()
    instance = runtime(tmp_path, factory)
    await add(instance)
    await instance._lease.acquire()
    instance._timeout = .03
    try:
        with pytest.raises(TimeoutError):
            await add(instance, user="next")
        await asyncio.sleep(.03)
        assert instance.health()["state"] == "ready" and not instance._blocked
        assert not factory.services[0].closed and len(factory.services) == 1
    finally:
        instance._lease.release()
        await instance.close()


@pytest.mark.asyncio
async def test_late_owner_failure_has_fresh_cleanup_clock_after_add_plus_sixty(tmp_path):
    factory = Factory()
    instance = runtime(tmp_path, factory, embedding_owner=Owner())
    await add(instance)
    generation = instance._embedding_generation
    original_prepare = generation.prepare_close
    cleanup_deadlines = []

    async def prepare(*, deadline_ns=None):
        cleanup_deadlines.append(deadline_ns)
        assert time.monotonic_ns() < deadline_ns <= time.monotonic_ns() + 60_000_000_000
        await original_prepare(deadline_ns=deadline_ns)

    generation.prepare_close = prepare
    old_business_deadline = time.monotonic() - 120
    try:
        await instance._lease.acquire()
        instance._start_failed_cleanup(old_business_deadline)
        await asyncio.wait_for(instance.wait_idle(), 2)
        assert cleanup_deadlines and factory.services[0].closed
        assert instance.health()["state"] == "ready"
    finally:
        await instance.close()


@pytest.mark.asyncio
async def test_cleanup_timeout_immediately_closes_both_admission_lanes(tmp_path, monkeypatch):
    monkeypatch.setattr("system_memory_god.challenge_runtime.CLEANUP_TIMEOUT", .03)
    factory = Factory()
    host = ChallengeLanes(tmp_path, client=object(), model="test-model",
                           service_factory=factory, embedding_owner=Owner())
    admission = BoundedAdmission(host.wait_idle, lane_count=2)
    host.bind_admission(admission)
    release = asyncio.Event()

    async def failed(lane):
        instance = host.for_lane(lane)
        await add(instance)
        old = factory.services[0]
        original_close = old.close

        async def close():
            await release.wait()
            await original_close()

        old.close = close
        factory.fail_after_append = True
        await add(instance, request="failed")

    first = admission.reserve(time.monotonic() + 5)
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await admission.submit(first, failed, affinity_key="alice")
        await asyncio.sleep(.06)
        assert admission.closing and host.health()["state"] == "unavailable"
        assert admission.in_flight == 1 and host.for_lane(0)._cleanup_tasks
        with pytest.raises(AdmissionClosed):
            admission.reserve(time.monotonic() + 5)
        with pytest.raises(RuntimeError, match="unavailable"):
            await add(host.for_lane(1), user="bob")
    finally:
        release.set()
        with pytest.raises(RuntimeError):
            await admission.close()
        await host.close()


@pytest.mark.asyncio
async def test_failure_callback_exception_cannot_replace_original_business_error(tmp_path):
    factory = FakeFactory()

    def broken_callback():
        raise ValueError("private observer failure")

    instance = runtime(tmp_path, factory, failure_observer=broken_callback)
    await add(instance)
    factory.fail_after_append = factory.fail_close = True
    try:
        with pytest.raises(RuntimeError, match="durable write"):
            await add(instance, request="failed")
        await asyncio.gather(*tuple(instance._tasks), return_exceptions=True)
        assert instance.health()["state"] == "unavailable"
        assert instance._service is factory.services[0] and not factory.services[0].closed
        assert instance.diagnostic()["failures"][-1]["stage"] == "failure_observer"
    finally:
        factory.fail_close = False
        await instance.close()
