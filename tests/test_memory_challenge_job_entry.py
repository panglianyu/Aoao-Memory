"""Internal job deadlines and admission settlement without external dependencies."""

import asyncio
from datetime import datetime, timezone
import time
from types import SimpleNamespace

import pytest

from kernel.raw_message_models import ConflictError
from system_memory_god.challenge_admission import AdmissionClosed, BoundedAdmission
from system_memory_god.challenge_runtime import ChallengeRuntime
from system_memory_god.challenge_store import ChallengeStore


async def ready():
    return None


@pytest.mark.asyncio
async def test_enqueue_runs_without_a_submit_waiter_and_settlement_waits_for_idle():
    idle, entered = asyncio.Event(), asyncio.Event()
    idle.set()
    admission = BoundedAdmission(idle.wait)
    ticket = admission.reserve(time.monotonic() + 2)

    async def operation():
        idle.clear()
        entered.set()
        return "accepted"

    admission.enqueue(ticket, operation)
    await entered.wait()
    assert await ticket.future == "accepted"
    waiter = asyncio.create_task(admission.wait_settled(ticket))
    try:
        await asyncio.sleep(0)
        assert not waiter.done()
        assert admission.in_flight == 1
        idle.set()
        await waiter
        assert admission.in_flight == 0
    finally:
        idle.set()
        await admission.close()


@pytest.mark.asyncio
async def test_cancelled_settlement_waiter_does_not_abandon_owned_job():
    entered, release = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    ticket = admission.reserve(time.monotonic() + 2)

    async def operation():
        entered.set()
        await release.wait()
        return "finished"

    admission.enqueue(ticket, operation)
    await entered.wait()
    waiter = asyncio.create_task(admission.wait_settled(ticket))
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert admission.in_flight == 1
    release.set()
    await admission.wait_settled(ticket)
    assert await ticket.future == "finished"
    await admission.close()


@pytest.mark.asyncio
async def test_terminal_commit_failure_blocks_next_writer_and_exposes_unsettled_ticket():
    admission = BoundedAdmission(ready)
    first = admission.reserve(time.monotonic() + 2)
    second = admission.reserve(time.monotonic() + 2)
    seen = []

    async def broken_commit():
        admission.fail_closed(OSError("synthetic terminal commit failure"))
        raise RuntimeError("terminal commit unavailable")

    async def next_writer():
        seen.append("ran")

    admission.enqueue(first, broken_commit)
    admission.enqueue(second, next_writer)
    with pytest.raises(AdmissionClosed):
        await admission.wait_settled(first)
    assert first.phase != "done" and admission.in_flight == 1
    assert seen == []
    with pytest.raises(AdmissionClosed):
        admission.reserve(time.monotonic() + 2)
    with pytest.raises(RuntimeError, match="dispatcher unavailable"):
        await admission.close()


@pytest.mark.asyncio
async def test_queued_expiry_finishes_without_running_operation():
    entered, release = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    first = admission.reserve(time.monotonic() + 2)
    second = admission.reserve(time.monotonic() + .02)
    seen = []

    async def blocking():
        entered.set()
        await release.wait()

    async def expired():
        seen.append("ran")

    admission.enqueue(first, blocking)
    admission.enqueue(second, expired)
    await entered.wait()
    await asyncio.sleep(.03)
    release.set()
    await admission.wait_settled(second)
    with pytest.raises(TimeoutError):
        await second.future
    assert seen == [] and admission.in_flight == 0
    await admission.close()


@pytest.mark.asyncio
async def test_expired_release_hook_keeps_ticket_and_blocks_next_writer_until_commit():
    entered, release_first = asyncio.Event(), asyncio.Event()
    commit_entered, release_commit = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    seen = []

    async def blocking():
        entered.set()
        await release_first.wait()

    async def commit():
        commit_entered.set()
        await release_commit.wait()

    async def forbidden():
        seen.append("expired")

    async def next_writer():
        seen.append("next")

    first = admission.reserve(time.monotonic() + 2)
    expired = admission.reserve(time.monotonic() + .02, before_release=commit)
    following = admission.reserve(time.monotonic() + 2)
    admission.enqueue(first, blocking)
    admission.enqueue(expired, forbidden)
    admission.enqueue(following, next_writer)
    try:
        await entered.wait()
        await asyncio.sleep(.03)
        release_first.set()
        await asyncio.wait_for(commit_entered.wait(), 1)
        assert admission.in_flight == 2 and expired.phase == "releasing"
        assert seen == []
        release_commit.set()
        await admission.wait_settled(following)
        assert seen == ["next"]
        with pytest.raises(TimeoutError):
            await expired.future
    finally:
        release_first.set()
        release_commit.set()
        await admission.close()


@pytest.mark.asyncio
async def test_cancelled_queue_hook_blocks_later_writer_even_after_queue_removal():
    entered, release_first = asyncio.Event(), asyncio.Event()
    commit_entered, release_commit = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    seen = []

    async def blocking():
        entered.set()
        await release_first.wait()

    async def commit():
        commit_entered.set()
        await release_commit.wait()

    async def next_writer():
        seen.append("next")

    first = admission.reserve(time.monotonic() + 2)
    cancelled = admission.reserve(time.monotonic() + 2, before_release=commit)
    following = admission.reserve(time.monotonic() + 2)
    admission.enqueue(first, blocking)
    admission.enqueue(cancelled, ready)
    admission.enqueue(following, next_writer)
    try:
        await entered.wait()
        admission.abandon(cancelled)
        await commit_entered.wait()
        release_first.set()
        await admission.wait_settled(first)
        await asyncio.sleep(0)
        assert cancelled not in admission._queues[0]
        assert seen == [] and admission.in_flight == 2
        release_commit.set()
        await admission.wait_settled(following)
        assert seen == ["next"]
    finally:
        release_first.set()
        release_commit.set()
        await admission.close()


@pytest.mark.asyncio
async def test_reading_ticket_close_waits_for_preinstalled_release_owner():
    entered, release = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    calls = 0

    async def commit():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()

    ticket = admission.reserve(time.monotonic() + 2, before_release=commit)
    closing = asyncio.create_task(admission.close())
    try:
        await entered.wait()
        assert admission.in_flight == 1 and ticket.phase == "releasing"
        assert not closing.done()
        waiter = asyncio.create_task(admission.wait_settled(ticket))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert ticket.release_task is not None and not ticket.release_task.cancelled()
        release.set()
        await closing
        await admission.wait_settled(ticket)
        assert calls == 1 and admission.in_flight == 0
    finally:
        release.set()
        await admission.close()


@pytest.mark.asyncio
async def test_expired_release_failure_retains_ticket_and_never_starts_next_writer():
    entered, release_first = asyncio.Event(), asyncio.Event()
    admission = BoundedAdmission(ready)
    seen = []

    async def blocking():
        entered.set()
        await release_first.wait()

    async def commit():
        raise OSError("synthetic durable failure")

    async def next_writer():
        seen.append("next")

    first = admission.reserve(time.monotonic() + 2)
    expired = admission.reserve(time.monotonic() + .02)
    following = admission.reserve(time.monotonic() + 2)
    admission.enqueue(first, blocking)
    admission.enqueue(expired, ready, before_release=commit)
    admission.enqueue(following, next_writer)
    try:
        await entered.wait()
        await asyncio.sleep(.03)
        release_first.set()
        with pytest.raises(AdmissionClosed):
            await admission.wait_settled(expired)
        assert expired.phase == "releasing" and expired in admission._tickets
        assert seen == [] and admission.closing
        with pytest.raises(RuntimeError, match="dispatcher unavailable"):
            await admission.close()
        assert expired in admission._tickets
    finally:
        release_first.set()


@pytest.mark.asyncio
async def test_reading_release_failure_is_visible_and_keeps_the_reservation():
    admission = BoundedAdmission(ready)

    async def commit():
        raise OSError("synthetic durable failure")

    ticket = admission.reserve(time.monotonic() + 2, before_release=commit)
    admission.abandon(ticket)
    with pytest.raises(AdmissionClosed):
        await admission.wait_settled(ticket)
    assert admission.in_flight == 1
    with pytest.raises(RuntimeError, match="dispatcher unavailable"):
        await admission.close()
    assert admission.in_flight == 1


@pytest.mark.asyncio
async def test_job_deadline_bypasses_http_budget_and_preserves_first_received_time(tmp_path):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model", add_timeout=.01)
    first_received = "2026-11-02T00:00:00+00:00"
    deadline = time.monotonic() + 1600
    calls = []

    async def capture(*args, **kwargs):
        calls.append((args, kwargs))
        await asyncio.sleep(.025)
        return {"success": True}

    runtime._add = capture
    try:
        result = await runtime.add_job(user_id="user", session_id="session", request_id="request",
            messages=[{"role": "user", "content": "synthetic"}], deadline=deadline,
            received_utc=first_received)
        assert result == {"success": True}
        assert calls[0][0][-1] == deadline
        assert calls[0][1] == {"received_utc": first_received}
        assert runtime.request_deadline() < time.monotonic() + .02
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [True, "later", float("inf"), float("nan")])
async def test_job_rejects_invalid_deadline_without_creating_owner(tmp_path, deadline):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model")
    with pytest.raises(ValueError, match="job deadline"):
        await runtime.add_job(user_id="user", session_id="session", request_id="request",
            messages=[{"role": "user", "content": "synthetic"}], deadline=deadline,
            received_utc="2026-11-02T00:00:00+00:00")
    assert not runtime._tasks
    await runtime.close()


@pytest.mark.asyncio
async def test_job_rejects_expired_or_renewed_deadline_before_raw_write(tmp_path):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model")
    arguments = dict(user_id="user", session_id="session", request_id="request",
        messages=[{"role": "user", "content": "synthetic"}],
        received_utc="2026-11-02T00:00:00+00:00")
    with pytest.raises(TimeoutError):
        await runtime.add_job(**arguments, deadline=time.monotonic() - 1)
    with pytest.raises(ValueError, match="job deadline"):
        await runtime.add_job(**arguments, deadline=time.monotonic() + 1701)
    assert not runtime._tasks and not runtime._store.database.exists()
    await runtime.close()


@pytest.mark.asyncio
async def test_store_first_accept_time_is_immutable_across_idempotent_replays(tmp_path):
    store = ChallengeStore(tmp_path, model="test-model")
    first_received = "2026-11-02T00:00:00+00:00"
    binding, first = await store.begin("user", "request", "digest", 1,
                                     received_utc=first_received)
    _, same = await store.begin("user", "request", "digest", 1,
                               received_utc=first_received)
    _, legacy = await store.begin("user", "request", "digest", 1)
    assert first == same == legacy
    assert first.received_utc == first_received
    with pytest.raises(ConflictError):
        await store.begin("user", "request", "digest", 1,
                          received_utc="2026-11-02T00:00:01+00:00")
    assert (await store.begin("user", "request", "digest", 1))[0] == binding
    _, ordinary = await store.begin("user", "ordinary", "digest", 1)
    assert datetime.fromisoformat(ordinary.received_utc).tzinfo is timezone.utc


@pytest.mark.asyncio
@pytest.mark.parametrize("received", [None, "2026-11-02T00:00:00Z", "2026-11-02T00:00:00",
    "2026-11-02T08:00:00+08:00", "2026-11-02T00:00:00.000000+00:00", "invalid"])
async def test_job_rejects_noncanonical_first_received_time(tmp_path, received):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model")
    with pytest.raises(ValueError, match="received"):
        await runtime.add_job(user_id="user", session_id="session", request_id="request",
            messages=[{"role": "user", "content": "synthetic"}],
            deadline=time.monotonic() + 1600, received_utc=received)
    assert not runtime._tasks and not runtime._store.database.exists()
    await runtime.close()


@pytest.mark.asyncio
async def test_job_passes_first_received_time_to_missing_timestamp_messages(tmp_path):
    stored = []

    class Service:
        def __init__(self, profile, **options):
            self.scopes = (SimpleNamespace(conversation_id="conversation", user_id="internal-user"),)
            self.raw = SimpleNamespace(initialize_profile=ready, settlement_status="settled")

        async def start(self):
            pass

        async def add(self, scope, draft):
            stored.append(draft)

        async def wait_until_published(self, scope, **options):
            return {"status": "current", "terminal": False}

        async def close(self):
            pass

    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model", service_factory=Service)
    first_received = "2026-11-02T00:00:00+00:00"
    try:
        result = await runtime.add_job(user_id="user", session_id="session", request_id="request",
            messages=[{"role": "user", "content": "synthetic"}],
            deadline=time.monotonic() + 1600, received_utc=first_received)
        assert result["success"] is True and stored[0].created_at == first_received
    finally:
        await runtime.close()
