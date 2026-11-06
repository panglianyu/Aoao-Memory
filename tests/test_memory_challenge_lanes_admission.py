"""Controlled two-lane admission and cache ownership races; no model or HTTP."""

import asyncio
import time

import pytest

from system_memory_god.challenge_admission import (
    AdmissionClosed, AdmissionFull, BoundedAdmission,
)


def ticket(admission, seconds=2):
    return admission.reserve(time.monotonic() + seconds)


async def ready(_lane):
    return None


@pytest.mark.asyncio
async def test_distinct_users_overlap_but_same_user_remains_fifo():
    admission = BoundedAdmission(ready, lane_count=2)
    entered = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    observed = []

    async def work(lane, user, number):
        observed.append((user, number, lane))
        entered[lane].set()
        if number == 0:
            await release.wait()
        return lane

    tasks = [asyncio.create_task(admission.submit(
        ticket(admission), lambda lane: work(lane, "a", 0), affinity_key="a")),
        asyncio.create_task(admission.submit(
        ticket(admission), lambda lane: work(lane, "b", 0), affinity_key="b")),
        asyncio.create_task(admission.submit(
        ticket(admission), lambda lane: work(lane, "a", 1), affinity_key="a"))]
    try:
        await asyncio.wait_for(asyncio.gather(*(item.wait() for item in entered)), 1)
        assert observed == [("a", 0, 0), ("b", 0, 1)]
        assert not tasks[2].done()
        release.set()
        assert await asyncio.gather(*tasks) == [0, 1, 0]
        await admission.wait_empty()
        assert admission.pin_count == 0
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await admission.close()


@pytest.mark.asyncio
async def test_cached_user_pin_survives_zero_pending_until_exact_retirement():
    admission = BoundedAdmission(ready, lane_count=2)

    async def activate(lane):
        admission.cache_enter(lane, "a", 1)
        return lane

    try:
        assert await admission.submit(ticket(admission), activate, affinity_key="a") == 0
        await admission.wait_empty()
        assert admission.pin_count == 1
        assert await admission.submit(ticket(admission), ready, affinity_key="a") is None
        assert admission.pin_count == 1
        admission.cache_retired(0, "a", 1)
        assert admission.pin_count == 0
    finally:
        await admission.close()


@pytest.mark.asyncio
async def test_retirement_with_new_pending_request_keeps_old_lane_pin():
    admission = BoundedAdmission(ready, lane_count=2)
    retiring, finish_retire = asyncio.Event(), asyncio.Event()
    seen = []

    async def activate(lane):
        admission.cache_enter(lane, "a", 1)

    await admission.submit(ticket(admission), activate, affinity_key="a")
    await admission.wait_empty()
    # Consume the round-robin tie on lane 1 so replacement enters lane 0.
    await admission.submit(ticket(admission), ready, affinity_key="b")
    await admission.wait_empty()

    async def replace(lane):
        assert lane == 0
        retiring.set()
        await finish_retire.wait()
        admission.cache_retired(lane, "a", 1)
        admission.cache_enter(lane, "c", 2)

    replacement = asyncio.create_task(admission.submit(
        ticket(admission), replace, affinity_key="c"))
    await retiring.wait()

    async def return_a(lane):
        seen.append(lane)
        admission.cache_retired(lane, "c", 2)
        admission.cache_enter(lane, "a", 3)

    returning = asyncio.create_task(admission.submit(
        ticket(admission), return_a, affinity_key="a"))
    try:
        await asyncio.sleep(0)
        finish_retire.set()
        await asyncio.gather(replacement, returning)
        await admission.wait_empty()
        assert seen == [0] and admission.pin_count == 1
        admission.cache_retired(0, "a", 3)
        assert admission.pin_count == 0
    finally:
        finish_retire.set()
        await asyncio.gather(replacement, returning, return_exceptions=True)
        await admission.close()


@pytest.mark.asyncio
async def test_stale_cache_callback_cannot_clear_new_cached_owner():
    admission = BoundedAdmission(ready, lane_count=2)

    async def activate(lane):
        admission.cache_enter(lane, "a", 1)

    await admission.submit(ticket(admission), activate, affinity_key="a")
    await admission.wait_empty()
    admission.cache_retired(0, "a", 1)
    await admission.submit(ticket(admission), ready, affinity_key="b")
    await admission.wait_empty()

    async def next_owner(lane):
        admission.cache_enter(lane, "c", 2)

    await admission.submit(ticket(admission), next_owner, affinity_key="c")
    await admission.wait_empty()
    with pytest.raises(AdmissionClosed):
        admission.cache_retired(0, "a", 1)
    assert admission.closing and admission.pin_count == 1
    admission.cache_retired(0, "c", 2)
    with pytest.raises(RuntimeError):
        await admission.close()


@pytest.mark.asyncio
async def test_settling_owner_blocks_only_its_lane_and_keeps_capacity():
    idle = [asyncio.Event(), asyncio.Event()]
    for event in idle:
        event.set()
    admission = BoundedAdmission(lambda lane: idle[lane].wait(), lane_count=2)

    async def late_owner(lane):
        idle[lane].clear()
        raise TimeoutError("late owner")

    try:
        with pytest.raises(TimeoutError):
            await admission.submit(ticket(admission), late_owner, affinity_key="a")
        assert admission.in_flight == 1

        async def second_lane(lane):
            return lane

        assert await asyncio.wait_for(admission.submit(
            ticket(admission), second_lane, affinity_key="b"), 1) == 1
        assert admission.in_flight == 1
    finally:
        idle[0].set()
        await admission.close()


@pytest.mark.asyncio
async def test_two_lane_capacity_is_global_and_includes_unassigned_bodies():
    admission = BoundedAdmission(ready, lane_count=2)
    tickets = [ticket(admission) for _ in range(32)]
    try:
        with pytest.raises(AdmissionFull):
            ticket(admission)
        assert admission.pin_count == 0 and admission.in_flight == 32
        for item in tickets:
            admission.abandon(item)
        assert admission.in_flight == 0
    finally:
        await admission.close()


@pytest.mark.asyncio
async def test_one_dispatcher_failure_still_attempts_both_lane_idle_checks():
    visits = []

    async def fail_idle(lane):
        visits.append(lane)
        if lane == 0:
            raise RuntimeError("lost owner")

    admission = BoundedAdmission(fail_idle, lane_count=2)
    first = asyncio.create_task(admission.submit(ticket(admission), ready, affinity_key="a"))
    second = asyncio.create_task(admission.submit(ticket(admission), ready, affinity_key="b"))
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(result, Exception) for result in results)
    with pytest.raises(RuntimeError):
        await admission.close()
    assert 0 in visits and 1 in visits
    assert admission.closing


@pytest.mark.asyncio
async def test_invalid_affinity_is_rejected_without_leaking_ticket():
    admission = BoundedAdmission(ready, lane_count=2)
    try:
        with pytest.raises(ValueError):
            await admission.submit(ticket(admission), ready)
        assert admission.in_flight == 0
    finally:
        await admission.close()


@pytest.mark.asyncio
async def test_global_capacity_bounds_pending_pins_plus_two_cached_users():
    admission = BoundedAdmission(ready, lane_count=2)
    release = asyncio.Event()

    async def cache_a(lane):
        admission.cache_enter(lane, "cached-a", 1)

    async def cache_b(lane):
        admission.cache_enter(lane, "cached-b", 1)

    await admission.submit(ticket(admission), cache_a, affinity_key="cached-a")
    await admission.wait_empty()
    await admission.submit(ticket(admission), cache_b, affinity_key="cached-b")
    await admission.wait_empty()

    async def blocked(_lane):
        await release.wait()

    tasks = [asyncio.create_task(admission.submit(
        ticket(admission), blocked, affinity_key=f"pending-{index}")) for index in range(32)]
    try:
        await asyncio.sleep(0)
        assert admission.in_flight == 32 and admission.pin_count == 34
        assert admission.lane_in_flight == (16, 16)
        with pytest.raises(AdmissionFull):
            ticket(admission)
        release.set()
        await asyncio.gather(*tasks)
        await admission.wait_empty()
        assert admission.pin_count == 2
        admission.cache_retired(0, "cached-a", 1)
        admission.cache_retired(1, "cached-b", 1)
        assert admission.pin_count == 0
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await admission.close()


@pytest.mark.asyncio
async def test_canceled_same_user_queue_keeps_only_real_owner_and_cache_pin():
    admission = BoundedAdmission(ready, lane_count=2)
    entered, release = asyncio.Event(), asyncio.Event()

    async def owner(lane):
        admission.cache_enter(lane, "a", 1)
        entered.set()
        await release.wait()

    first = asyncio.create_task(admission.submit(ticket(admission), owner, affinity_key="a"))
    await entered.wait()
    try:
        for _ in range(20):
            queued = asyncio.create_task(admission.submit(ticket(admission), ready, affinity_key="a"))
            await asyncio.sleep(0)
            queued.cancel()
            with pytest.raises(asyncio.CancelledError):
                await queued
            assert admission.in_flight == 1 and admission.queued == 0
            assert admission.pin_count == 1 and admission.lane_in_flight == (1, 0)
        with pytest.raises(TimeoutError):
            await admission.submit(ticket(admission, .01), ready, affinity_key="a")
        assert admission.in_flight == 1 and admission.pin_count == 1
    finally:
        release.set()
        await first
        await admission.wait_empty()
        admission.cache_retired(0, "a", 1)
        await admission.close()


@pytest.mark.asyncio
async def test_shutdown_waits_for_both_running_owners_after_callers_cancel():
    admission = BoundedAdmission(ready, lane_count=2)
    entered = [asyncio.Event(), asyncio.Event()]
    release = [asyncio.Event(), asyncio.Event()]

    async def owner(lane):
        entered[lane].set()
        await release[lane].wait()

    callers = [asyncio.create_task(admission.submit(
        ticket(admission), owner, affinity_key=f"user-{index}")) for index in range(2)]
    await asyncio.gather(*(event.wait() for event in entered))
    for caller in callers:
        caller.cancel()
    await asyncio.gather(*callers, return_exceptions=True)
    closing = asyncio.create_task(admission.close())
    try:
        await asyncio.sleep(0)
        assert admission.in_flight == 2 and not closing.done()
        release[0].set()
        await asyncio.sleep(.01)
        assert admission.in_flight == 1 and not closing.done()
        release[1].set()
        await closing
        assert admission.in_flight == 0 and admission.pin_count == 0
    finally:
        for event in release:
            event.set()
        await admission.close()
        await closing


@pytest.mark.asyncio
async def test_ticket_cannot_be_reused_across_admission_owners():
    first = BoundedAdmission(ready, lane_count=2)
    second = BoundedAdmission(ready, lane_count=2)
    reserved = ticket(first)
    try:
        with pytest.raises(AdmissionClosed):
            await second.submit(reserved, ready, affinity_key="a")
        second.abandon(reserved)
        assert first.in_flight == 1 and not reserved.future.done()
        assert second.in_flight == 0 and second.pin_count == 0
        assert await first.submit(reserved, ready, affinity_key="a") is None
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("search_order", [
    tuple(range(16)), tuple(reversed(range(16))),
    tuple(range(0, 16, 2)) + tuple(range(1, 16, 2)),
])
async def test_full_sixteen_user_write_then_read_retirement_keeps_returning_pins(search_order):
    admission = BoundedAdmission(ready, lane_count=2)
    cached = [None, None]
    tokens = [0, 0]
    retirement_started = [asyncio.Event(), asyncio.Event()]
    retirement_release = asyncio.Event()
    seen = []

    async def operation(lane, user, wave):
        previous = cached[lane]
        if previous is None or previous[0] != user:
            if previous is not None:
                if wave == "search" and not retirement_release.is_set():
                    retirement_started[lane].set()
                    await retirement_release.wait()
                await asyncio.sleep(0)
                admission.cache_retired(lane, *previous)
            tokens[lane] += 1
            admission.cache_enter(lane, user, tokens[lane])
            cached[lane] = (user, tokens[lane])
        await asyncio.sleep(0)
        seen.append((wave, user, lane))
        return user

    async def submit(user, wave):
        return await admission.submit(ticket(admission, 5),
            lambda lane: operation(lane, user, wave), affinity_key=user)

    tasks = []
    try:
        assert await asyncio.gather(*(submit(f"user-{number}", "add") for number in range(16))) == [
            f"user-{number}" for number in range(16)]
        await admission.wait_empty()
        assert admission.pin_count == 2 and admission.in_flight == 0
        prior_lanes = {user: lane for wave, user, lane in seen if wave == "add"}
        prior_caches = dict((user, lane) for lane, (user, _) in enumerate(cached))

        tasks = [asyncio.create_task(submit(f"user-{number}", "search")) for number in search_order]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in retirement_started)), 2)
        assert admission.in_flight <= 16 and admission.pin_count == 16
        # The old cached users also have Search tickets in this same wave.
        # Their pending requests must still be pinned while retirement suspends.
        for user, lane in prior_caches.items():
            assert admission._pins[user].lane_id == lane
            assert admission._pins[user].cache_token is not None
        retirement_release.set()
        assert await asyncio.gather(*tasks) == [f"user-{number}" for number in search_order]
        await admission.wait_empty()
        assert admission.in_flight == 0 and admission.pin_count == 2
        assert not admission.closing
        for wave, user, lane in seen:
            if wave == "search" and user in prior_caches:
                assert lane == prior_lanes[user]
    finally:
        retirement_release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        for lane, owner in enumerate(cached):
            if owner is not None:
                admission.cache_retired(lane, *owner)
        await admission.close()
