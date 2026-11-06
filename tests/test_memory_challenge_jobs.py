"""Durable job owners, real SQLite/OS locks, and controlled runtime settling."""

import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest

from kernel.raw_message_models import ConflictError
from system_memory_god.challenge_admission import AdmissionClosed, AdmissionFull
from system_memory_god.challenge_job_store import ChallengeJobStore
from system_memory_god.challenge_jobs import ChallengeJobs


def lease_for(root):
    path = Path(__file__).resolve().parents[1] / "Tools" / "serve_memory_challenge.py"
    spec = importlib.util.spec_from_file_location("job_test_cli", path)
    cli = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = cli
    spec.loader.exec_module(cli)
    lease = cli._DataRootLease.acquire(root)
    cli._prepare_root(root)
    return lease


class Lane:
    def __init__(self, root, calls):
        self.root, self.model, self.calls = root, "synthetic-model", calls
        self.idle = asyncio.Event()
        self.idle.set()
        self.hold = False

    async def add_job(self, **values):
        self.calls.append((values["user_id"], values["request_id"], values["received_utc"]))
        if self.hold:
            self.idle.clear()
        return {"success": True, **{key: values[key] for key in (
            "user_id", "session_id", "request_id")}}

    async def wait_idle(self):
        await self.idle.wait()

    async def search(self, **values):
        self.calls.append(("search", values["deadline"]))
        return []


class Runtime:
    lane_count = 2

    def __init__(self, root):
        self.calls = []
        self.lanes = [Lane(root, self.calls), Lane(root, self.calls)]
        self.closed = False

    def for_lane(self, lane):
        return self.lanes[lane]

    def bind_admission(self, admission):
        self.admission = admission

    async def wait_idle(self, lane=None):
        if lane is None:
            await asyncio.gather(*(item.wait_idle() for item in self.lanes))
        else:
            await self.lanes[lane].wait_idle()

    async def close(self):
        self.closed = True

    def health(self):
        return {"state": "stopped" if self.closed else "ready", "version": "test"}


def values(user="user", request="request"):
    return {"user_id": user, "session_id": "session", "request_id": request,
            "messages": [{"role": "user", "content": "synthetic original"}]}


def assembly(tmp_path, **options):
    root = tmp_path / "runtime"
    lease = lease_for(root)
    runtime = Runtime(root)
    store = ChallengeJobStore(root / "jobs", model="synthetic-model", runtime_lease=lease)
    jobs = ChallengeJobs(runtime, store, runtime_lease=lease, **options)
    return lease, runtime, store, jobs


@pytest.mark.asyncio
async def test_duplicate_conflict_terminal_payload_and_sqlite_bounds(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path)
    try:
        first = await jobs.submit_add(**values())
        duplicate = await jobs.submit_add(**values())
        assert first.job_id == duplicate.job_id
        result = await jobs.await_terminal(first.job_id, timeout=2)
        assert result.state == "SUCCEEDED" and result.error_code is None
        assert len(runtime.calls) == 1
        with pytest.raises(ConflictError):
            await jobs.submit_add(**{**values(), "messages": [{"role": "user", "content": "changed"}]})
        with sqlite3.connect(store.database) as db:
            assert db.execute("SELECT payload FROM jobs").fetchone() == (None,)
            assert db.execute("PRAGMA page_size").fetchone()[0] == 4096
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_success_waits_for_real_idle_and_waiter_cancellation_is_private(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path)
    runtime.lanes[0].hold = True
    try:
        receipt = await jobs.submit_add(**values())
        for _ in range(100):
            if not runtime.lanes[0].idle.is_set():
                break
            await asyncio.sleep(.001)
        assert (await jobs.status(receipt.job_id)).state == "RUNNING"
        waiter = asyncio.create_task(jobs.await_terminal(receipt.job_id, timeout=2))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert jobs.admission.in_flight == 1
        runtime.lanes[0].idle.set()
        assert (await jobs.await_terminal(receipt.job_id, timeout=2)).state == "SUCCEEDED"
    finally:
        runtime.lanes[0].idle.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_accept_waiter_cancel_does_not_lose_committed_job(tmp_path, monkeypatch):
    lease, runtime, store, jobs = assembly(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.accept

    async def delayed(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "accept", delayed)
    caller = asyncio.create_task(jobs.submit_add(**values()))
    try:
        await entered.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        release.set()
        for _ in range(200):
            row = await store.find("user", "request")
            if row is not None:
                break
            await asyncio.sleep(.005)
        assert row is not None
        assert (await jobs.await_terminal(row.job_id, timeout=2)).state == "SUCCEEDED"
        assert len(runtime.calls) == 1
    finally:
        release.set()
        await jobs.close()
        lease.close()


def test_job_store_rejects_second_owner_and_wrong_or_closed_runtime_lease(tmp_path):
    root = tmp_path / "runtime"
    lease = lease_for(root)
    store = ChallengeJobStore(root / "jobs", model="synthetic-model", runtime_lease=lease)
    try:
        with pytest.raises((OSError, RuntimeError)):
            ChallengeJobStore(root / "jobs", model="synthetic-model", runtime_lease=lease)
        with pytest.raises(ValueError):
            ChallengeJobStore(root / "alternate", model="synthetic-model", runtime_lease=lease)
    finally:
        store.close()
        lease.close()
    with pytest.raises((ValueError, RuntimeError)):
        ChallengeJobStore(root / "jobs", model="synthetic-model", runtime_lease=lease)


@pytest.mark.asyncio
async def test_restart_marks_unfinished_failed_without_replaying_and_clears_payload(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path)
    try:
        import hashlib
        import json
        from datetime import datetime, timedelta, timezone

        body = values()
        body["messages"][0]["timestamp"] = None
        encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        now = datetime.now(timezone.utc)
        row, created = await store.accept(body, encoded, hashlib.sha256(encoded.encode()).hexdigest(),
            received_utc=now.isoformat(), expires_utc=(now + timedelta(seconds=1700)).isoformat())
        assert created
        store.close()
        reopened = ChallengeJobStore(store.root, model=store.model, runtime_lease=lease)
        try:
            status = await reopened.status(row.job_id)
            assert status.state == "FAILED" and status.error_code == "interrupted"
            with sqlite3.connect(reopened.database) as db:
                assert db.execute("SELECT payload FROM jobs").fetchone() == (None,)
            assert not runtime.calls
        finally:
            reopened.close()
    finally:
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_thirty_work_slots_close_queued_and_status_does_not_wait_lane(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path)
    for lane in runtime.lanes:
        lane.hold = True
    try:
        receipts = [await jobs.submit_add(**values(f"user-{i}", f"request-{i}")) for i in range(30)]
        with pytest.raises(AdmissionFull):
            await jobs.submit_add(**values("overflow", "overflow"))
        assert (await asyncio.wait_for(jobs.status(receipts[-1].job_id), .5)).state == "QUEUED"
        closing = asyncio.create_task(jobs.close())
        await asyncio.sleep(.01)
        for lane in runtime.lanes:
            lane.idle.set()
        await closing
        assert runtime.closed
        with sqlite3.connect(store.database) as db:
            assert db.execute("SELECT COUNT(*) FROM jobs WHERE state NOT IN ('SUCCEEDED','FAILED')").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM jobs WHERE payload IS NOT NULL").fetchone()[0] == 0
    finally:
        for lane in runtime.lanes:
            lane.idle.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_expired_queued_job_never_enters_runtime_and_keeps_first_utc(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path, job_timeout=.08)
    runtime.lanes[0].hold = True
    try:
        first = await jobs.submit_add(**values())
        second = await jobs.submit_add(**values(request="second"))
        await asyncio.sleep(.12)
        runtime.lanes[0].hold = False
        runtime.lanes[0].idle.set()
        done = await jobs.await_terminal(second.job_id, timeout=2)
        assert done.state == "FAILED" and done.error_code == "expired"
        assert [(user, request) for user, request, _ in runtime.calls] == [("user", "request")]
        assert runtime.calls[0][2] == first.received_utc
        assert jobs.admission.in_flight == 0
    finally:
        runtime.lanes[0].idle.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_same_user_fifo_and_other_user_uses_second_lane(tmp_path):
    lease, runtime, store, jobs = assembly(tmp_path)
    runtime.lanes[0].hold = True
    try:
        first = await jobs.submit_add(**values())
        second = await jobs.submit_add(**values(request="second"))
        other = await jobs.submit_add(**values("other", "other"))
        assert (await jobs.await_terminal(other.job_id, timeout=2)).state == "SUCCEEDED"
        assert [row[:2] for row in runtime.calls] == [("user", "request"), ("other", "other")]
        runtime.lanes[0].hold = False
        runtime.lanes[0].idle.set()
        assert (await jobs.await_terminal(first.job_id, timeout=2)).state == "SUCCEEDED"
        assert (await jobs.await_terminal(second.job_id, timeout=2)).state == "SUCCEEDED"
        assert runtime.calls[-1][:2] == ("user", "second")
    finally:
        runtime.lanes[0].idle.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_status_two_controls_and_search_keeps_one_hundred_seconds(tmp_path, monkeypatch):
    import time

    lease, runtime, store, jobs = assembly(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.status
    count = 0

    async def hold(job_id):
        nonlocal count
        count += 1
        if count == 2:
            entered.set()
        await release.wait()
        return await original(job_id)

    try:
        receipt = await jobs.submit_add(**values())
        await jobs.await_terminal(receipt.job_id, timeout=2)
        monkeypatch.setattr(store, "status", hold)
        readers = [asyncio.create_task(jobs.status(receipt.job_id)) for _ in range(2)]
        await entered.wait()
        with pytest.raises(AdmissionFull):
            await jobs.status(receipt.job_id)
        release.set()
        await asyncio.gather(*readers)
        before = time.monotonic()
        assert await jobs.search(user_id="user", query="synthetic", top_k=10) == []
        assert 99 < runtime.calls[-1][1] - before <= 100.1
    finally:
        release.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_terminal_write_failure_retains_slot_and_store_lock(tmp_path, monkeypatch):
    lease, runtime, store, jobs = assembly(tmp_path)

    async def fail(*args, **kwargs):
        raise sqlite3.OperationalError("controlled commit failure")

    monkeypatch.setattr(store, "finish", fail)
    try:
        receipt = await jobs.submit_add(**values())
        for _ in range(100):
            if jobs.health()["state"] == "unavailable":
                break
            await asyncio.sleep(.005)
        assert jobs.health()["state"] == "unavailable"
        assert jobs.admission.in_flight == 1
        assert (await jobs.status(receipt.job_id)).state != "SUCCEEDED"
        with pytest.raises(RuntimeError, match="unresolved"):
            await asyncio.wait_for(jobs.await_terminal(receipt.job_id, timeout=1710), .5)
        with pytest.raises(RuntimeError):
            await jobs.close()
        with pytest.raises((OSError, RuntimeError)):
            ChallengeJobStore(store.root, model=store.model, runtime_lease=lease)
    finally:
        store.close()
        lease.close()


@pytest.mark.asyncio
async def test_history_capacity_rejects_new_job_without_losing_accepted_result(tmp_path, monkeypatch):
    from system_memory_god import challenge_job_store as module

    lease, runtime, store, jobs = assembly(tmp_path)
    try:
        receipt = await jobs.submit_add(**values())
        assert (await jobs.await_terminal(receipt.job_id, timeout=2)).state == "SUCCEEDED"
        monkeypatch.setattr(module, "MAX_HISTORY", 1)
        with pytest.raises(RuntimeError):
            await jobs.submit_add(**values(request="overflow"))
        assert (await jobs.status(receipt.job_id)).state == "SUCCEEDED"
        assert jobs.admission.in_flight == 0
        assert (await jobs.submit_add(**values())).job_id == receipt.job_id
    finally:
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_receipt_owner_cancel_after_sql_commit_keeps_receipt_and_marks_failed(tmp_path, monkeypatch):
    import threading

    lease, runtime, store, jobs = assembly(tmp_path)
    committed, release = threading.Event(), threading.Event()
    original = store._run

    def blocked_after_commit(operation, *args):
        result = original(operation, *args)
        if operation == "accept":
            committed.set()
            assert release.wait(3)
        return result

    monkeypatch.setattr(store, "_run", blocked_after_commit)
    caller = asyncio.create_task(jobs.submit_add(**values()))
    try:
        assert await asyncio.to_thread(committed.wait, 2)
        assert len(jobs._owners) == 1
        next(iter(jobs._owners)).cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(AdmissionClosed):
            await asyncio.wait_for(caller, 2)
        record = await store.find("user", "request")
        status = await jobs.status(record.job_id)
        assert status.state == "FAILED" and status.error_code == "closed"
        assert not runtime.calls and jobs.admission.in_flight == 0
        with sqlite3.connect(store.database) as db:
            assert db.execute("SELECT payload FROM jobs").fetchone() == (None,)
    finally:
        release.set()
        await jobs.close()
        lease.close()


@pytest.mark.parametrize("commit_fails", [False, True])
@pytest.mark.asyncio
async def test_queued_expiry_terminal_commit_fences_next_writer(tmp_path, monkeypatch, commit_fails):
    lease, runtime, store, jobs = assembly(tmp_path, job_timeout=.08)
    runtime.lanes[0].hold = True
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.finish
    target = None

    async def delayed(job_id, **kwargs):
        if job_id == target:
            entered.set()
            await release.wait()
            if commit_fails:
                raise sqlite3.OperationalError("controlled queued terminal failure")
        return await original(job_id, **kwargs)

    monkeypatch.setattr(store, "finish", delayed)
    try:
        await jobs.submit_add(**values())
        second = await jobs.submit_add(**values(request="second"))
        target = second.job_id
        jobs._job_timeout = 2.0  # Only the subsequent fixture request gets this new budget.
        third = await jobs.submit_add(**values(request="third"))
        await asyncio.sleep(.12)
        runtime.lanes[0].hold = False
        runtime.lanes[0].idle.set()
        await asyncio.wait_for(entered.wait(), 2)
        assert [row[1] for row in runtime.calls] == ["request"]
        assert jobs.admission.in_flight == 2
        release.set()
        if commit_fails:
            for _ in range(100):
                if jobs.health()["state"] == "unavailable":
                    break
                await asyncio.sleep(.005)
            assert jobs.health()["state"] == "unavailable"
            assert [row[1] for row in runtime.calls] == ["request"]
            assert jobs.admission.in_flight >= 1
            with pytest.raises(RuntimeError):
                await jobs.close()
        else:
            assert (await jobs.await_terminal(second.job_id, timeout=2)).error_code == "expired"
            assert (await jobs.await_terminal(third.job_id, timeout=2)).state == "SUCCEEDED"
            assert [row[1] for row in runtime.calls] == ["request", "third"]
            await jobs.close()
    finally:
        release.set()
        runtime.lanes[0].idle.set()
        if not store._closed:
            store.close()
        lease.close()


@pytest.mark.asyncio
async def test_sqlite_returned_page_limit_is_verified(tmp_path, monkeypatch):
    from system_memory_god import challenge_job_store as module

    lease, runtime, store, jobs = assembly(tmp_path)
    try:
        receipt = await jobs.submit_add(**values())
        await jobs.await_terminal(receipt.job_id, timeout=2)
        monkeypatch.setattr(module, "MAX_PAGES", 1)
        with pytest.raises(RuntimeError, match="page limit"):
            await jobs.status(receipt.job_id)
    finally:
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_close_during_accept_keeps_reading_slot_until_durable_failure(tmp_path, monkeypatch):
    lease, runtime, store, jobs = assembly(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.accept

    async def blocked(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "accept", blocked)
    caller = asyncio.create_task(jobs.submit_add(**values()))
    try:
        await entered.wait()
        closing = asyncio.create_task(jobs.close())
        await asyncio.sleep(.02)
        assert not closing.done() and jobs.admission.in_flight == 1
        release.set()
        with pytest.raises(AdmissionClosed):
            await asyncio.wait_for(caller, 2)
        await asyncio.wait_for(closing, 2)
        with sqlite3.connect(store.database) as db:
            assert db.execute("SELECT state,error_code,payload FROM jobs").fetchone() == ("FAILED", "closed", None)
        assert not runtime.calls and runtime.closed and jobs.admission.in_flight == 0
    finally:
        release.set()
        await jobs.close()
        lease.close()


@pytest.mark.asyncio
async def test_same_user_commit_return_delay_cannot_reorder_acceptance(tmp_path, monkeypatch):
    lease, runtime, store, jobs = assembly(tmp_path)
    committed, release = asyncio.Event(), asyncio.Event()
    original = store.accept

    async def delayed(values, *args, **kwargs):
        result = await original(values, *args, **kwargs)
        if values["request_id"] == "first":
            committed.set()
            await release.wait()
        return result

    monkeypatch.setattr(store, "accept", delayed)
    first = asyncio.create_task(jobs.submit_add(**values(request="first")))
    try:
        await committed.wait()
        second = asyncio.create_task(jobs.submit_add(**values(request="second")))
        await asyncio.sleep(.03)
        assert await store.find("user", "second") is None
        assert not runtime.calls and not second.done()
        assert len(jobs._accept_tails) == 1
        release.set()
        receipts = await asyncio.gather(first, second)
        for receipt in receipts:
            assert (await jobs.await_terminal(receipt.job_id, timeout=2)).state == "SUCCEEDED"
        assert [row[1] for row in runtime.calls] == ["first", "second"]
        assert not jobs._accept_tails
    finally:
        release.set()
        await jobs.close()
        lease.close()
