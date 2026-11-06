"""Bounded admission owns waiting and running work independently of HTTP callers."""

import asyncio
import time

import pytest

from system_memory_god.challenge_admission import (
    AdmissionClosed, AdmissionFull, BoundedAdmission,
)


async def ready():
    return None


@pytest.mark.asyncio
async def test_two_full_waves_are_fifo_and_capacity_is_reusable():
    admission = BoundedAdmission(ready)
    try:
        for wave in range(2):
            seen = []
            tickets = [admission.reserve(time.monotonic() + 2) for _ in range(32)]
            with pytest.raises(AdmissionFull):
                admission.reserve(time.monotonic() + 2)

            async def work(index):
                seen.append(index)
                await asyncio.sleep(0)
                return (wave, index)

            tasks = [asyncio.create_task(admission.submit(ticket, lambda i=i: work(i)))
                     for i, ticket in enumerate(tickets)]
            assert await asyncio.gather(*tasks) == [(wave, i) for i in range(32)]
            assert seen == list(range(32))
            await admission.wait_empty()
            assert admission.in_flight == 0
    finally:
        await admission.close()


@pytest.mark.asyncio
async def test_expired_queued_request_never_runs():
    entered, release = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    seen = []

    async def first_work():
        entered.set()
        await release.wait()
        seen.append("first")

    first = asyncio.create_task(admission.submit(
        admission.reserve(time.monotonic() + 2), first_work))
    await entered.wait()
    second = admission.reserve(time.monotonic() + .02)
    try:
        with pytest.raises(TimeoutError):
            await admission.submit(second, lambda: seen.append("expired"))
        assert admission.in_flight == 1
    finally:
        release.set()
        await first
        await admission.close()
    assert seen == ["first"]


@pytest.mark.asyncio
async def test_cancelled_queue_entries_do_not_accumulate_or_run():
    entered, release = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    seen = []

    async def first_work():
        entered.set()
        await release.wait()

    first = asyncio.create_task(admission.submit(
        admission.reserve(time.monotonic() + 2), first_work))
    await entered.wait()
    try:
        for _ in range(80):
            ticket = admission.reserve(time.monotonic() + 2)
            pending = asyncio.create_task(admission.submit(ticket, ready))
            await asyncio.sleep(0)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert admission.in_flight == 1
            assert admission.queued == 0
        last = asyncio.create_task(admission.submit(
            admission.reserve(time.monotonic() + 2), ready))
        await asyncio.sleep(0)
        assert not last.done()
        release.set()
        await asyncio.gather(first, last)
    finally:
        release.set()
        await admission.close()
    assert seen == []


@pytest.mark.asyncio
async def test_running_waiter_can_leave_but_capacity_waits_for_real_idle():
    idle, entered = asyncio.Event(), asyncio.Event()
    idle.set()
    admission = BoundedAdmission(idle.wait)
    seen = []

    async def owner_work():
        idle.clear()
        entered.set()
        raise TimeoutError("HTTP owner still settling")

    first = asyncio.create_task(admission.submit(
        admission.reserve(time.monotonic() + 2), owner_work))
    await entered.wait()
    with pytest.raises(TimeoutError):
        await first
    assert admission.in_flight == 1

    async def next_work():
        seen.append("next")

    second = asyncio.create_task(admission.submit(
        admission.reserve(time.monotonic() + 2), next_work))
    await asyncio.sleep(.01)
    assert not second.done() and seen == []
    idle.set()
    await second
    await admission.close()
    assert seen == ["next"] and admission.in_flight == 0


@pytest.mark.asyncio
async def test_repeated_http_cancellation_does_not_cancel_dispatcher_or_owner():
    idle, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    idle.set()
    admission = BoundedAdmission(idle.wait)

    async def work():
        idle.clear()
        entered.set()
        await release.wait()
        idle.set()
        return "finished"

    caller = asyncio.create_task(admission.submit(
        admission.reserve(time.monotonic() + 2), work))
    await entered.wait()
    caller.cancel()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert admission.in_flight == 1
    closing = asyncio.create_task(admission.close())
    await asyncio.sleep(.01)
    assert not closing.done()
    release.set()
    await closing
    assert admission.in_flight == 0


@pytest.mark.asyncio
async def test_shutdown_cancels_reading_and_rejects_late_enqueue():
    admission = BoundedAdmission(ready)
    ticket = admission.reserve(time.monotonic() + 2)
    entered = asyncio.Event()

    async def body():
        entered.set()
        await asyncio.Event().wait()

    reading = asyncio.create_task(admission.read_body(ticket, body(), 1))
    await entered.wait()
    await admission.close()
    with pytest.raises(AdmissionClosed):
        await reading
    with pytest.raises(AdmissionClosed):
        await admission.submit(ticket, ready)
    with pytest.raises(AdmissionClosed):
        admission.reserve(time.monotonic() + 2)
    assert admission.in_flight == 0


@pytest.mark.asyncio
async def test_slow_body_cannot_enqueue_after_shutdown_even_if_it_finishes():
    admission = BoundedAdmission(ready)
    ticket = admission.reserve(time.monotonic() + 2)
    await admission.close()
    with pytest.raises(AdmissionClosed):
        await admission.submit(ticket, ready)
    assert admission.in_flight == 0


@pytest.mark.asyncio
async def test_body_suppressing_cancel_stays_owned_after_repeated_http_cancel():
    admission = BoundedAdmission(ready)
    ticket = admission.reserve(time.monotonic() + 2)
    entered, saw_cancel, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    bodies = []

    async def stubborn_body():
        bodies.append(asyncio.current_task())
        entered.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                saw_cancel.set()
        return {}

    reading = asyncio.create_task(admission.read_body(ticket, stubborn_body(), 1))
    closing = None
    try:
        await entered.wait()
        reading.cancel()
        await saw_cancel.wait()
        reading.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reading
        admission.abandon(ticket)
        assert admission.in_flight == 1
        closing = asyncio.create_task(admission.close())
        await asyncio.sleep(.01)
        assert not closing.done() and admission.in_flight == 1
    finally:
        release.set()
        await asyncio.gather(*bodies, return_exceptions=True)
        await admission.close()
        if closing is not None:
            await closing
    assert admission.in_flight == 0
