"""Writer receipts and independent numerical reader process contracts."""

import base64
import hashlib
import asyncio
import math

import psutil

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector


@pytest.mark.asyncio
async def test_two_scope_retirement_resumes_after_later_lease_cleanup_failure(tmp_path, monkeypatch):
    import numpy as np
    from tests.test_daily_vector_snapshot import bound_delete_fault
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scopes = tuple(PipelineScope("owner", "agent", "web", name) for name in ("lease-a", "lease-b"))
    cache = DailyVectorSnapshot(tmp_path / "cache", scopes, embedding="synthetic")
    reader = SupervisedDailyVectorSnapshot(cache, auto_recover=False)
    try:
        for scope in scopes:
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
            await reader.warm_existing(scope)
        old_b = cache.acquire(scopes[1])
        cache.release(old_b)
        cache.publish(scopes[1], {"two": (np.ones(16, dtype="<f4") * 2).tobytes()})
        with bound_delete_fault(monkeypatch, lambda path: path.name == "manifest.json"
                                and path.parent.name == old_b.generation) as fault:
            with pytest.raises(OSError, match="injected"):
                await reader.retire_worker()
            assert reader.worker_pid is None and fault["failures"] == 1
            fault["enabled"] = False
            await reader.retire_worker()
        assert not (cache.root / scopes[1].partition_id / old_b.generation).exists()
        await reader.close()
        await reader.close()
        cache.close()
        assert not cache.root.exists()
    finally:
        try:
            await reader.close()
        finally:
            cache.close()


@pytest.mark.parametrize("budget", [True, None, 0, -.1, .251, float("nan"), float("inf")])
def test_snapshot_rejects_invalid_query_budget_before_worker(tmp_path, budget):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "invalid-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    try:
        with pytest.raises(ValueError, match="query budget"):
            SupervisedDailyVectorSnapshot(cache, query_timeout=budget)
    finally:
        cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [True, "bad", float("nan"), float("inf"), 0.0])
async def test_snapshot_rejects_invalid_or_expired_caller_before_worker(tmp_path, deadline):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "expired-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    reader = SupervisedDailyVectorSnapshot(cache)
    try:
        with pytest.raises(TimeoutError if deadline == 0.0 else ValueError):
            await reader.search_cosine_hits(scope, "synthetic", deadline=deadline)
        assert reader.worker_pid is None
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_snapshot_caller_deadline_covers_stalled_query_without_reset(tmp_path):
    import time
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "caller-deadline")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, embedding="synthetic-stall",
                                          diagnostics=True, auto_recover=False)
    try:
        await reader.warm_existing(scope)
        began = time.monotonic()
        caller_deadline = began + .08
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "synthetic", deadline=caller_deadline)
        diagnostic = reader.diagnostic()
        assert 0 < diagnostic["budget_ms"] <= (caller_deadline - began) * 1000
        assert diagnostic["timeout_source"] in {"transport_deadline", "watchdog_deadline"}
        assert diagnostic["complete"] is False
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_snapshot_does_not_regrant_budget_when_entry_clock_is_preempted(tmp_path, monkeypatch):
    import sys
    import time
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "entry-preemption")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, diagnostics=True, auto_recover=False)
    original_clock = time.monotonic
    advanced = []

    def preempted_clock():
        caller = sys._getframe(1)
        filename = caller.f_code.co_filename.replace("\\", "/")
        now = original_clock()
        if filename.endswith("/supervised_daily_vector_snapshot.py") and not advanced:
            advanced.append(True)
            return now
        if advanced and filename.endswith(("/supervised_daily_vector_snapshot.py",
                                           "/supervised_daily_vector.py")):
            return now + .11
        return now

    try:
        await reader.warm_existing(scope)
        caller_deadline = original_clock() + .08
        with monkeypatch.context() as clock_fault:
            clock_fault.setattr(time, "monotonic", preempted_clock)
            with pytest.raises(TimeoutError):
                await reader.search_cosine_hits(scope, "synthetic", deadline=caller_deadline)
        assert advanced == [True]
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("caller_budget", [None, .075], ids=["floating-effective", "short-caller"])
async def test_public_snapshot_reports_actual_effective_budget(tmp_path, monkeypatch, caller_budget):
    import json
    import time
    from types import SimpleNamespace
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage import supervised_daily_vector as transport_module
    from system_memory_god.storage import supervised_daily_vector_snapshot as facade_module
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "effective-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = facade_module.SupervisedDailyVectorSnapshot(cache, query_timeout=.15,
                                                        diagnostics=True, auto_recover=False)
    original_send = _ConnectionBase.send_bytes
    sent = []
    entry_samples = []
    base = 65536.0
    anchor = None

    def boundary_clock():
        if len(entry_samples) < 2:
            entry_samples.append(base)
            return base
        return base + (time.monotonic() - anchor)

    def record_query(connection, body, *args, **kwargs):
        frame = json.loads(body)
        if frame.get("op") == "search_cosine_hits":
            sent.append(frame["id"])
        return original_send(connection, body, *args, **kwargs)

    try:
        await reader.warm_existing(scope)
        process_id = reader.worker_pid
        caller_deadline = None if caller_budget is None else base + caller_budget
        expected_budget = ((base + .15 if caller_deadline is None else caller_deadline) - base) * 1000
        anchor = time.monotonic()
        with monkeypatch.context() as boundary:
            # Shift only the two product modules' clock domain; after the two
            # entry samples it advances with the real clock, including IPC time.
            clock = SimpleNamespace(monotonic=boundary_clock)
            boundary.setattr(facade_module, "time", clock)
            boundary.setattr(transport_module, "time", clock)
            boundary.setattr(_ConnectionBase, "send_bytes", record_query)
            assert (await reader.search_cosine_hits(scope, "synthetic", limit=1,
                                                   deadline=caller_deadline))[0][0] == "one"
        diagnostic = reader.diagnostic()
        assert entry_samples == [base, base]
        assert 0 < expected_budget < (150 if caller_budget is None else caller_budget * 1000)
        assert diagnostic["budget_ms"] == expected_budget
        assert 0 <= diagnostic["request_elapsed_ms"] <= diagnostic["budget_ms"]
        assert diagnostic["complete"] is True and diagnostic["timeout_source"] == "none"
        assert sent == [diagnostic["request_id"]] and diagnostic["request_id"] > 0
        assert reader.worker_pid == process_id and psutil.pid_exists(process_id)
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_early_watchdog_wakeup_does_not_kill_unexpired_public_query(tmp_path, monkeypatch):
    import json
    import threading
    import time
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "early-timer")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    original_timer, original_recv = threading.Timer, _ConnectionBase.recv_bytes
    wakes = []

    def early_once(interval, function, *args, **kwargs):
        wakes.append(interval)
        return original_timer(.001 if len(wakes) == 1 else interval, function, *args, **kwargs)

    def delayed_frame(connection, *args, **kwargs):
        body = original_recv(connection, *args, **kwargs)
        frame = json.loads(body)
        if type(frame.get("result")) is list:
            time.sleep(.08)
        return body

    try:
        await reader.warm_existing(scope)
        with monkeypatch.context() as patch:
            patch.setattr(threading, "Timer", early_once)
            patch.setattr(_ConnectionBase, "recv_bytes", delayed_frame)
            for _ in range(2):
                assert (await reader.search_cosine_hits(scope, "synthetic", limit=1))[0][0] == "one"
            assert reader.diagnostic()["complete"] is True
        assert wakes and reader.available(scope)
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_public_timeout_keeps_sent_request_identity_and_finite_origin(tmp_path, monkeypatch):
    import json
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "sent-timeout")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, embedding="synthetic-stall", diagnostics=True, auto_recover=False)
    original_send = _ConnectionBase.send_bytes
    sent = []

    def record_send(connection, body, *args, **kwargs):
        frame = json.loads(body)
        if frame.get("op") == "search_cosine_hits":
            sent.append(frame["id"])
        return original_send(connection, body, *args, **kwargs)

    try:
        await reader.warm_existing(scope)
        monkeypatch.setattr(_ConnectionBase, "send_bytes", record_send)
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "private-query-must-not-escape", limit=1)
        diagnostic = reader.diagnostic()
        assert sent and diagnostic["request_id"] == sent[0] > 0
        assert diagnostic["error_code"] == "parent_deadline" and diagnostic["complete"] is False
        assert diagnostic["timeout_source"] in {"transport_deadline", "watchdog_deadline"}
        assert math.isfinite(diagnostic["budget_ms"]) and 0 < diagnostic["budget_ms"] <= 150
        assert diagnostic["request_elapsed_ms"] >= 150
        assert diagnostic["embedding_ms"] is diagnostic["numerical_ms"] is None
        assert "private-query" not in str(diagnostic)
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["transport_deadline", "watchdog_deadline", "facade_postcheck"])
async def test_late_public_reply_keeps_first_timeout_origin_and_received_metrics(tmp_path, monkeypatch, origin):
    import json
    import threading
    import time
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "late-reply")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    original_recv, original_timer = _ConnectionBase.recv_bytes, threading.Timer
    loop = asyncio.get_running_loop()
    received = []

    def delayed_parent_dispatch():
        time.sleep(.2)

    def delayed_frame(connection, *args, **kwargs):
        body = original_recv(connection, *args, **kwargs)
        frame = json.loads(body)
        if type(frame.get("result")) is list:
            received.append(frame["id"])
            if origin == "facade_postcheck":
                loop.call_soon_threadsafe(delayed_parent_dispatch)
            else:
                time.sleep(.2)
        return body

    try:
        await reader.warm_existing(scope)
        with monkeypatch.context() as patch:
            patch.setattr(_ConnectionBase, "recv_bytes", delayed_frame)
            if origin == "transport_deadline":
                patch.setattr(threading, "Timer", lambda interval, fn: original_timer(interval + .5, fn))
            with pytest.raises(TimeoutError):
                await reader.search_cosine_hits(scope, "private-query", limit=1)
        diagnostic = reader.diagnostic()
        assert received and diagnostic["request_id"] == received[0]
        assert diagnostic["timeout_source"] == origin
        assert diagnostic["complete"] is False and diagnostic["error_code"] == "parent_deadline"
        assert diagnostic["embedding_ms"] >= 0 and diagnostic["numerical_ms"] >= 0
        assert diagnostic["request_elapsed_ms"] >= 150
        assert math.isfinite(diagnostic["budget_ms"]) and 0 < diagnostic["budget_ms"] <= 150
        assert not reader.available(scope) and "private-query" not in str(diagnostic)
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_queued_query_has_own_deadline_without_overwriting_running_request(tmp_path, monkeypatch):
    import json
    import threading
    import time
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "queue-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    original_recv = _ConnectionBase.recv_bytes
    entered = threading.Event()
    sent_id = []

    def stalled_first_frame(connection, *args, **kwargs):
        body = original_recv(connection, *args, **kwargs)
        frame = json.loads(body)
        if type(frame.get("result")) is list:
            sent_id.append(frame["id"])
            entered.set()
            time.sleep(.25)
        return body

    first = None
    try:
        await reader.warm_existing(scope)
        monkeypatch.setattr(_ConnectionBase, "recv_bytes", stalled_first_frame)
        first = asyncio.create_task(reader.search_cosine_hits(scope, "first-private", limit=1))
        assert await asyncio.to_thread(entered.wait, .5)
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "second-private", limit=1)
        queued = reader.diagnostic()
        assert queued["request_id"] == 0 and queued["timeout_source"] == "queue_deadline"
        assert queued["embedding_ms"] is None and queued["complete"] is False
        with pytest.raises(TimeoutError):
            await first
        settled = reader.diagnostic()
        assert settled["request_id"] == sent_id[0] and settled["timeout_source"] == "watchdog_deadline"
        assert settled["embedding_ms"] >= 0 and "private" not in str(settled)
    finally:
        if first is not None:
            await asyncio.gather(first, return_exceptions=True)
        await reader.close()
        cache.close()


async def _settled_denied_snapshot_owner(transport, process, pid, call_done,
                                         denials, bootstrap_deadline):
    """Observe one failed physical call without renewing its cleanup budget."""
    import time

    deadline = bootstrap_deadline
    while time.monotonic() < deadline:
        acquired = transport._lifecycle.acquire(blocking=False)
        if acquired:
            pending_acquired = transport._pending_guard.acquire(blocking=False)
            try:
                if pending_acquired:
                    actual_deadline = transport._termination_deadline
                    deadline_published = (
                        transport._termination_process is process
                        and type(actual_deadline) in {int, float}
                        and math.isfinite(actual_deadline))
                    if deadline_published:
                        deadline = min(deadline, actual_deadline + .250)
                    if (deadline_published and time.monotonic() < deadline and call_done.is_set()
                            and transport._pending == 0 and transport._active_request is None
                            and transport._process is process and process.pid == pid
                            and process.is_alive() and transport._termination_process is process
                            and denials and transport._termination_error is denials[-1]
                            and transport._cleanup_failure is not None
                            and transport._close_budget_failure is transport._cleanup_failure):
                        return transport._cleanup_failure, denials[-1], len(denials)
            finally:
                if pending_acquired:
                    transport._pending_guard.release()
                transport._lifecycle.release()
        await asyncio.sleep(0)
    raise AssertionError("same-owner query cleanup precondition did not settle")


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_error", [False, True])
async def test_snapshot_timeout_keeps_failed_cleanup_owned_and_close_can_retry(
        tmp_path, monkeypatch, recwarn, transport_error, record_property):
    import json
    import threading
    import time
    import numpy as np
    import psutil
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "cleanup-failure")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    process = child = terminate = None
    transport = reader._transport
    finish_cleanup = transport._finish_cleanup
    observation_guard = threading.Lock()
    context = threading.local()
    began = time.perf_counter()
    frames = []
    attempts = []
    denials = []
    call_done = threading.Event()
    seen_tasks = set()
    observation = {"complete": True, "dropped": 0}
    fields = ["attempt", "phase", "elapsed_ms", "remaining_ms", "process_state",
              "tracked", "done", "cancelled", "removed_unknown", "outcome", "old_cause",
              "diagnostic_bound", "diagnostic_last_phase", "diagnostic_complete", "diagnostic_stage_mask"]
    diagnostic_bits = ["dispatch", "dispatched", "termination", "join", "join_return", "finished", "failed"]
    error_types = {RuntimeError, ValueError, OSError, TimeoutError, PermissionError,
                   ProcessLookupError, asyncio.CancelledError}

    def outcome(error):
        return type(error).__name__ if type(error) in error_types else "OtherError"

    def note(phase, attempt=0, deadline=None, result="none", old_cause=None, tracked=(), token=None):
        try:
            current = tuple(transport._closing_tasks)
            try:
                state = "unknown" if process is None else "alive" if process.is_alive() else "exited"
            except (ValueError, OSError):
                state = "unknown"
            bound = token is not None and token is transport._cleanup_diagnostic_token
            diagnostic = transport._cleanup_diagnostic_snapshot() if bound else {}
            bound = bound and token is transport._cleanup_diagnostic_token
            if not bound:
                diagnostic = {}
            known = {"budget", "dispatch", "dispatched", "lifecycle_wait", "lifecycle_acquired",
                     "lifecycle_released", "ipc_wait", "ipc_acquired", "ipc_released",
                     "stop_lifecycle_wait", "stop_lifecycle_acquired", "stop_lifecycle_released",
                     "termination", "termination_return", "join", "join_return", "stop_return",
                     "failed", "finished"}
            stages = diagnostic.get("frames", ())
            stage = stages[-1].get("stage") if stages else None
            frame = [attempt, phase, round((time.perf_counter() - began) * 1000, 3),
                     None if deadline is None else round(max(0, deadline - time.monotonic()) * 1000, 3),
                     state, len(current), sum(task.done() for task in current),
                     sum(task.cancelled() for task in current), len(set(tracked) - set(current)),
                     result, old_cause, bound, stage if stage in known else "unknown",
                     diagnostic.get("complete") if bound else None,
                     sum(1 << index for index, label in enumerate(diagnostic_bits)
                         if any(value.get("stage") == label for value in stages)) if bound else None]
            if not observation_guard.acquire(blocking=False):
                observation["complete"] = False
                observation["dropped"] += 1
                return
            try:
                seen_tasks.update(current)
                if len(frames) >= 64 or len(json.dumps([fields, frames + [frame]],
                        allow_nan=False, separators=(",", ":")).encode()) > 3800:
                    observation["complete"] = False
                    observation["dropped"] += 1
                else:
                    frames.append(frame)
            finally:
                observation_guard.release()
        except BaseException:
            observation["complete"] = False
            observation["dropped"] += 1

    async def observed_finish(action, deadline, *, _cleanup_observation=None):
        attempt = len(attempts) + 1
        if attempt <= 4:
            attempts.append(deadline)
        else:
            observation["complete"] = False
            observation["dropped"] += 1
            return await finish_cleanup(action, deadline, _cleanup_observation=_cleanup_observation)
        retained = transport._termination_error
        tracked = tuple(transport._closing_tasks)
        note("finish_enter", attempt, deadline,
             "reused_budget" if deadline in attempts[:-1] else "new_budget",
             tracked=tracked, token=_cleanup_observation)

        def observed_action():
            previous = getattr(context, "attempt", 0)
            context.attempt = attempt
            note("action_enter", attempt, deadline, token=_cleanup_observation)
            try:
                value = action()
            except BaseException as error:
                note("action_raise", attempt, deadline, outcome(error), token=_cleanup_observation)
                raise
            else:
                note("action_return", attempt, deadline, token=_cleanup_observation)
                return value
            finally:
                context.attempt = previous

        try:
            return await finish_cleanup(observed_action, deadline,
                                        _cleanup_observation=_cleanup_observation)
        except BaseException as error:
            note("finish_raise", attempt, deadline, outcome(error),
                 error.__cause__ is retained and retained is not None, tracked, _cleanup_observation)
            raise
        finally:
            note("finish_exit", attempt, deadline, tracked=tracked, token=_cleanup_observation)

    monkeypatch.setattr(transport, "_finish_cleanup", observed_finish)
    try:
        await reader.warm_existing(scope)
        pid = reader.worker_pid
        process = reader._transport._process
        original_terminate = process.terminate

        def observed_terminate():
            attempt = getattr(context, "attempt", 0)
            note("terminate_enter", attempt)
            try:
                value = original_terminate()
            except BaseException as error:
                note("terminate_raise", attempt, result=outcome(error))
                raise
            note("terminate_return", attempt)
            return value

        monkeypatch.setattr(process, "terminate", observed_terminate)
        terminate = process.terminate
        child = psutil.Process(pid)
        child.suspend()

        def denied_termination():
            note("terminate_denied", getattr(context, "attempt", 0), result="PermissionError")
            error = PermissionError(5, "synthetic live owner denial")
            denials.append(error)
            raise error

        monkeypatch.setattr(process, "terminate", denied_termination)
        original_call = transport._call

        def completed_call(*args, **kwargs):
            try:
                return original_call(*args, **kwargs)
            finally:
                call_done.set()

        monkeypatch.setattr(transport, "_call", completed_call)
        if transport_error:
            import time

            def expired_pipe(timeout):
                time.sleep(.2)
                raise EOFError("synthetic expired transport")

            monkeypatch.setattr(reader._transport._connection, "poll", expired_pipe)
        bootstrap_deadline = time.monotonic() + .15 + 3 + .250
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "private-query", limit=1)
        _failure, previous_denial, previous_count = await _settled_denied_snapshot_owner(
            transport, process, pid, call_done, denials, bootstrap_deadline)
        assert reader.worker_pid == pid and child.is_running()
        assert reader.diagnostic()["error_type"] == "TimeoutError"
        with pytest.raises(RuntimeError, match="did not exit") as failed_close:
            await reader.close()
        assert isinstance(failed_close.value.__cause__, PermissionError)
        assert len(denials) == previous_count + 1
        assert failed_close.value.__cause__ is denials[-1]
        assert failed_close.value.__cause__ is not previous_denial
        assert reader.worker_pid == pid and child.is_running()
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        if child is not None and child.is_running():
            child.resume()
        try:
            await reader.close()
            await reader.close()
            assert reader.worker_pid is None
            cache.close()
        finally:
            try:
                if not observation_guard.acquire(blocking=False):
                    encoded = json.dumps({"schema": 1, "attempts": len(attempts),
                                          "complete": False, "dropped": len(frames)},
                                         separators=(",", ":"))
                else:
                    try:
                        if any(not task.done() for task in seen_tasks):
                            observation["complete"] = False
                        payload = {"schema": 1, "fields": fields, "frames": frames,
                                   "attempts": len(attempts), **observation,
                                   "diagnostic_stage_bits": diagnostic_bits,
                                   "task_scope": "current_set_only_removed_unknown"}
                        encoded = json.dumps(payload, allow_nan=False, separators=(",", ":"))
                    finally:
                        observation_guard.release()
                if len(encoded.encode()) > 4096:
                    encoded = json.dumps({"schema": 1, "attempts": len(attempts),
                                          "complete": False, "dropped": len(frames)},
                                         separators=(",", ":"))
                record_property("cleanup_retry_observation", encoded)
            except BaseException:
                pass
    assert not any(row.category.__name__ == "PytestUnhandledThreadExceptionWarning" for row in recwarn)


@pytest.mark.asyncio
async def test_close_lifecycle_admission_busy_retains_owner_and_later_retry_succeeds(
        tmp_path, monkeypatch):
    import threading
    import time
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "cleanup-admission")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    transport = reader._transport
    original_lifecycle = transport._lifecycle
    process = child = original_terminate = None
    denials, refused_timeouts = [], []
    call_done = threading.Event()

    class RefuseOneTimedAdmission:
        def acquire(self, *args, **kwargs):
            if "timeout" in kwargs and not refused_timeouts:
                timeout = kwargs["timeout"]
                assert type(timeout) in {int, float} and math.isfinite(timeout)
                assert 0 < timeout <= 3
                refused_timeouts.append(timeout)
                return False
            return original_lifecycle.acquire(*args, **kwargs)

        def release(self):
            return original_lifecycle.release()

        def __enter__(self):
            original_lifecycle.__enter__()
            return self

        def __exit__(self, *args):
            return original_lifecycle.__exit__(*args)

    try:
        await reader.warm_existing(scope)
        pid, process = reader.worker_pid, transport._process
        child = psutil.Process(pid)
        original_terminate = process.terminate
        child.suspend()

        def denied_termination():
            error = PermissionError(5, "synthetic live owner denial")
            denials.append(error)
            raise error

        original_call = transport._call

        def completed_call(*args, **kwargs):
            try:
                return original_call(*args, **kwargs)
            finally:
                call_done.set()

        monkeypatch.setattr(process, "terminate", denied_termination)
        monkeypatch.setattr(transport, "_call", completed_call)
        bootstrap_deadline = time.monotonic() + .15 + 3 + .250
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "private-query", limit=1)
        failure, previous_denial, previous_count = await _settled_denied_snapshot_owner(
            transport, process, pid, call_done, denials, bootstrap_deadline)
        monkeypatch.setattr(transport, "_lifecycle", RefuseOneTimedAdmission())
        try:
            with pytest.raises(RuntimeError, match="^vector lifecycle owner is busy$") as busy:
                await reader.close()
            assert busy.value.__cause__ is None
            assert len(refused_timeouts) == 1 and len(denials) == previous_count
            assert transport._process is process and reader.worker_pid == pid and process.is_alive()
            assert transport._termination_process is process
            assert transport._termination_error is previous_denial
            assert transport._cleanup_failure is failure
            assert transport._close_budget_failure is failure
        finally:
            monkeypatch.setattr(transport, "_lifecycle", original_lifecycle)
    finally:
        monkeypatch.setattr(transport, "_lifecycle", original_lifecycle)
        if process is not None:
            monkeypatch.setattr(process, "terminate", original_terminate)
        if child is not None and child.is_running():
            child.resume()
        await reader.close()
        await reader.close()
        assert reader.worker_pid is None
        cache.close()


@pytest.mark.asyncio
async def test_overlapping_failed_close_shares_one_cleanup_budget(tmp_path, monkeypatch):
    import time
    import numpy as np
    import psutil
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "concurrent-cleanup")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, auto_recover=False)
    process = child = terminate = None
    try:
        await reader.warm_existing(scope)
        process = reader._transport._process
        terminate = process.terminate
        child = psutil.Process(reader.worker_pid)
        child.suspend()

        def denied_termination():
            raise PermissionError(5, "synthetic concurrent owner denial")

        monkeypatch.setattr(process, "terminate", denied_termination)
        began = time.monotonic()
        query = asyncio.create_task(reader.search_cosine_hits(scope, "private-query", limit=1))
        await asyncio.sleep(.2)
        outcomes = await asyncio.gather(query, reader.close(), reader.close(), return_exceptions=True)
        elapsed = time.monotonic() - began
        assert isinstance(outcomes[0], TimeoutError)
        assert all(isinstance(outcome, RuntimeError) for outcome in outcomes[1:])
        assert elapsed < 4, "overlapping cleanup restarted the three-second budget"
        assert reader.worker_pid is not None
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        if child is not None and child.is_running():
            child.resume()
        await reader.close()
        cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["queued", "concurrent", "repeat"])
@pytest.mark.parametrize("iteration", range(3))
async def test_native_cleanup_fixed_nine_cases(tmp_path, monkeypatch, record_property, case, iteration):
    import json
    import threading
    import time
    import numpy as np
    from multiprocessing.process import BaseProcess
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    trace = []
    requested = threading.Event()
    original_start, original_terminate, original_close = BaseProcess.start, BaseProcess.terminate, BaseProcess.close

    def started(process):
        original_start(process)
        trace.append({"phase": "started", "generation": id(process), "pid": process.pid,
                      "monotonic": time.monotonic()})

    def terminated(process):
        row = {"phase": "termination_requested", "generation": id(process), "pid": process.pid,
               "monotonic": time.monotonic(), "error_type": "none", "winerror": None}
        trace.append(row)
        requested.set()
        try:
            original_terminate(process)
        except OSError as exc:
            row.update(error_type=type(exc).__name__, winerror=getattr(exc, "winerror", None))
            raise

    def closed(process):
        row = {"phase": "closed", "generation": id(process), "pid": process.pid,
               "exit_confirmed": process.exitcode is not None, "monotonic": time.monotonic()}
        original_close(process)
        trace.append(row)

    monkeypatch.setattr(BaseProcess, "start", started)
    monkeypatch.setattr(BaseProcess, "terminate", terminated)
    monkeypatch.setattr(BaseProcess, "close", closed)
    try:
        if case == "queued":
            await test_queued_query_has_own_deadline_without_overwriting_running_request(tmp_path, monkeypatch)
        else:
            scope = PipelineScope("owner", "agent", "web", "native-cleanup")
            cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
            reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
            try:
                await reader.warm_existing(scope)
                if case == "concurrent":
                    original_recv = _ConnectionBase.recv_bytes

                    def stalled_frame(connection, *args, **kwargs):
                        body = original_recv(connection, *args, **kwargs)
                        if type(json.loads(body).get("result")) is list:
                            time.sleep(.25)
                        return body

                    monkeypatch.setattr(_ConnectionBase, "recv_bytes", stalled_frame)
                    query = asyncio.create_task(reader.search_cosine_hits(scope, "synthetic-query", limit=1))
                    assert await asyncio.to_thread(requested.wait, 2)
                    outcomes = await asyncio.gather(query, reader.close(), reader.close(), return_exceptions=True)
                    assert isinstance(outcomes[0], TimeoutError)
                    assert outcomes[1:] == [None, None]
                await reader.close()
                await reader.close()
                assert reader.worker_pid is None
            finally:
                await reader.close()
                cache.close()
        assert any(row["phase"] == "started" for row in trace)
        assert any(row["phase"] == "closed" and row["exit_confirmed"] for row in trace)
    finally:
        record_property("native_cleanup_case", case)
        record_property("iteration", iteration)
        record_property("cleanup_trace", json.dumps(trace))


@pytest.mark.asyncio
async def test_blocked_snapshot_write_with_denied_termination_fails_bounded_and_keeps_owner(tmp_path, monkeypatch):
    import threading
    import numpy as np
    import psutil
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "blocked-cleanup")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, auto_recover=False)
    release = threading.Event()
    entered = threading.Event()
    process = child = terminate = query = None
    try:
        await reader.warm_existing(scope)
        pid = reader.worker_pid
        process = reader._transport._process
        terminate = process.terminate
        child = psutil.Process(pid)
        child.suspend()
        connection = reader._transport._connection
        send = connection.send_bytes

        def blocked_send(data):
            entered.set()
            assert release.wait(8), "test boundary was not released"
            send(data)

        def denied_termination():
            raise PermissionError(5, "synthetic blocked owner denial")

        monkeypatch.setattr(connection, "send_bytes", blocked_send)
        monkeypatch.setattr(process, "terminate", denied_termination)
        query = asyncio.create_task(reader.search_cosine_hits(scope, "synthetic-query", limit=1))
        assert await asyncio.to_thread(entered.wait, 1)
        done, _ = await asyncio.wait((query,), timeout=.6)
        assert query in done, "denied termination left caller waiting on blocked IPC"
        assert isinstance(query.exception(), TimeoutError)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(reader.close(), 3.5)
        assert not release.is_set() and child.is_running() and reader.worker_pid == pid
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        if child is not None and child.is_running():
            child.resume()
        release.set()
        if query is not None:
            await asyncio.gather(query, return_exceptions=True)
        await reader.close()
        cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["spawn", "ready"])
@pytest.mark.parametrize("outcome", ["cancel", "deadline"])
async def test_cold_snapshot_never_sends_a_generation_after_caller_abandons_it(
        tmp_path, monkeypatch, boundary, outcome):
    import json
    import threading
    import time
    from types import SimpleNamespace
    from multiprocessing.connection import _ConnectionBase
    from multiprocessing.process import BaseProcess
    import numpy as np
    from system_memory_god.storage import supervised_daily_vector as supervision
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "cold-abandoned")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, auto_recover=False)
    entered, release = threading.Event(), threading.Event()
    original_start, original_recv, original_send = BaseProcess.start, _ConnectionBase.recv_bytes, _ConnectionBase.send_bytes
    real_clock, original_wait = time.monotonic, asyncio.wait
    advance, frames, budgets = [0.0], [], []
    warm = None

    def held_start(process):
        if boundary == "spawn":
            entered.set()
            assert release.wait(10), "test spawn boundary was not released"
        return original_start(process)

    def held_ready(connection, *args, **kwargs):
        body = original_recv(connection, *args, **kwargs)
        if boundary == "ready" and json.loads(body) == {"version": 1, "ready": True}:
            entered.set()
            assert release.wait(10), "test ready boundary was not released"
        return body

    def record_send(connection, body, *args, **kwargs):
        frames.append(json.loads(body))
        return original_send(connection, body, *args, **kwargs)

    async def virtual_deadline(waiting, *, timeout=None, **kwargs):
        if outcome == "deadline" and timeout is not None and 59 < timeout <= 60:
            budgets.append(timeout)
            assert await asyncio.to_thread(entered.wait, 2)
            advance[0] = 61.0
            timeout = 0
        return await original_wait(waiting, timeout=timeout, **kwargs)

    monkeypatch.setattr(BaseProcess, "start", held_start)
    monkeypatch.setattr(_ConnectionBase, "recv_bytes", held_ready)
    monkeypatch.setattr(_ConnectionBase, "send_bytes", record_send)
    monkeypatch.setattr(supervision, "time", SimpleNamespace(monotonic=lambda: real_clock() + advance[0]))
    monkeypatch.setattr(asyncio, "wait", virtual_deadline)
    try:
        warm = asyncio.create_task(reader.warm_existing(scope))
        assert await asyncio.to_thread(entered.wait, 2)
        if outcome == "cancel":
            warm.cancel()
        done, _ = await original_wait((warm,), timeout=3.5)
        assert warm in done, "cold warm did not finish within its cancellation/deadline allowance"
        if outcome == "cancel":
            assert warm.cancelled()
        else:
            assert isinstance(warm.exception(), TimeoutError) and len(budgets) == 1
        assert reader._transport._pending == 1
        if boundary == "ready":
            assert reader.worker_pid is not None and cache._leases
        release.set()
        for _ in range(400):
            if reader._transport._pending == 0:
                break
            await asyncio.sleep(.01)
        assert reader._transport._pending == 0
        assert not any(frame.get("op") == "warm_snapshot" for frame in frames), "abandoned generation was sent late"
    finally:
        release.set()
        if warm is not None:
            await asyncio.gather(warm, return_exceptions=True)
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_snapshot_warm_preserves_its_sixty_second_deadline_and_live_lease(tmp_path, monkeypatch):
    import threading
    import time
    from types import SimpleNamespace
    import numpy as np
    from system_memory_god.storage import supervised_daily_vector as supervision
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scopes = tuple(PipelineScope("owner", "agent", "web", name) for name in ("warm-first", "warm-late"))
    cache = DailyVectorSnapshot(tmp_path / "cache", scopes, embedding="synthetic")
    reader = SupervisedDailyVectorSnapshot(cache, auto_recover=False)
    entered, release = threading.Event(), threading.Event()
    real_clock, real_timer = time.monotonic, threading.Timer
    advance, budgets = [0.0], []
    warm = process = terminate = None
    try:
        for scope in scopes:
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
        await reader.warm_existing(scopes[0])
        process = reader._transport._process
        terminate = process.terminate
        connection = reader._transport._connection
        recv = connection.recv_bytes

        def blocked_reply(*args, **kwargs):
            body = recv(*args, **kwargs)
            advance[0] = 61.0
            entered.set()
            assert release.wait(10), "test warm boundary was not released"
            return body

        def deadline_timer(interval, callback):
            budgets.append(interval)
            def fire_after_reply():
                assert entered.wait(2), "warm RPC did not reach the controlled reply"
                callback()
            return real_timer(.001, fire_after_reply)

        def denied_termination():
            raise PermissionError(5, "synthetic warm owner denial")

        monkeypatch.setattr(supervision, "time", SimpleNamespace(monotonic=lambda: real_clock() + advance[0]))
        monkeypatch.setattr(threading, "Timer", deadline_timer)
        monkeypatch.setattr(connection, "recv_bytes", blocked_reply)
        monkeypatch.setattr(process, "terminate", denied_termination)
        warm = asyncio.create_task(reader.warm_existing(scopes[1]))
        assert await asyncio.to_thread(entered.wait, 2)
        done, _ = await asyncio.wait((warm,), timeout=.6)
        assert warm in done and isinstance(warm.exception(), TimeoutError)
        assert len(budgets) == 1 and 59 < budgets[0] <= 60
        assert reader.worker_pid == process.pid and process.is_alive() and not connection.closed
        assert reader._transport._pending == 1 and len(cache._leases) == 2
        with pytest.raises(RuntimeError, match="pending"):
            await reader.retire_worker()
    finally:
        if process is not None:
            monkeypatch.setattr(process, "terminate", terminate)
        release.set()
        if warm is not None:
            await asyncio.gather(warm, return_exceptions=True)
        for _ in range(400):
            if reader._transport._pending == 0:
                break
            await asyncio.sleep(.01)
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_thread_dispatch_is_charged_to_the_public_entry_budget(tmp_path, monkeypatch):
    import json
    import time
    import numpy as np
    from multiprocessing.connection import _ConnectionBase
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "thread-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    original_thread, original_recv = asyncio.to_thread, _ConnectionBase.recv_bytes

    async def delayed_dispatch(*args, **kwargs):
        await asyncio.sleep(.12)
        return await original_thread(*args, **kwargs)

    def delayed_frame(connection, *args, **kwargs):
        body = original_recv(connection, *args, **kwargs)
        if type(json.loads(body).get("result")) is list:
            time.sleep(.06)
        return body

    try:
        await reader.warm_existing(scope)
        with monkeypatch.context() as patch:
            patch.setattr(asyncio, "to_thread", delayed_dispatch)
            patch.setattr(_ConnectionBase, "recv_bytes", delayed_frame)
            with pytest.raises(TimeoutError):
                await reader.search_cosine_hits(scope, "synthetic", limit=1)
        diagnostic = reader.diagnostic()
        assert diagnostic["queue_ms"] >= 100 and diagnostic["request_elapsed_ms"] >= 150
        assert diagnostic["complete"] is False
    finally:
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_starved_executor_query_expires_before_thread_dispatch(tmp_path):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "executor-budget")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    loop = asyncio.get_running_loop()
    await reader.warm_existing(scope)
    original_executor = loop._default_executor
    entered, released = threading.Event(), threading.Event()

    def occupied_thread():
        entered.set()
        released.wait(.5)

    pool = ThreadPoolExecutor(max_workers=1)
    pool.submit(occupied_thread)
    assert entered.wait(.5)
    release_timer = threading.Timer(.4, released.set)
    release_timer.start()
    loop.set_default_executor(pool)
    try:
        began = time.perf_counter()
        with pytest.raises(TimeoutError):
            await reader.search_cosine_hits(scope, "synthetic", limit=1)
        assert time.perf_counter() - began < .3
        diagnostic = reader.diagnostic()
        assert diagnostic["request_id"] == 0 and diagnostic["timeout_source"] == "queue_deadline"
        assert diagnostic["embedding_ms"] is None and diagnostic["complete"] is False
    finally:
        released.set()
        release_timer.cancel()
        await reader.close()
        loop._default_executor = original_executor
        pool.shutdown(wait=True)
        cache.close()


@pytest.mark.asyncio
async def test_public_pending_capacity_and_repeated_cancel_settle_without_stale_success(tmp_path):
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "pending-capacity")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, embedding="synthetic-stall", diagnostics=True, auto_recover=False)
    reads = []
    try:
        await reader.warm_existing(scope)
        reads = [asyncio.create_task(reader.search_cosine_hits(scope, "private-query", limit=1)) for _ in range(8)]
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="queue full"):
            await reader.search_cosine_hits(scope, "ninth-private", limit=1)
        assert reader.diagnostic()["error_code"] == "queue_rejected"
        assert reader.diagnostic()["request_id"] == 0
        for task in reads:
            task.cancel()
            task.cancel()
        await asyncio.gather(*reads, return_exceptions=True)
        await reader.close()
        assert reader.health()["pending"] == 0 and reader.worker_pid is None
        assert reader.diagnostic()["complete"] is False
        assert "private" not in str(reader.diagnostic())
    finally:
        await asyncio.gather(*reads, return_exceptions=True)
        await reader.close()
        cache.close()


@pytest.mark.asyncio
async def test_diagnostic_reader_keeps_hits_and_disables_probe_recovery(tmp_path):
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "diagnostics")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, diagnostics=True, auto_recover=False)
    try:
        await reader.warm_existing(scope)
        assert (await reader.search_cosine_hits(scope, "synthetic", limit=1))[0][0] == "one"
        diagnostic = reader.diagnostic()
        assert diagnostic["stage"] == "complete"
        assert diagnostic["error_code"] == "none"
        assert diagnostic["embedding_ms"] >= 0 and diagnostic["numerical_ms"] >= 0
        assert "synthetic" not in str(diagnostic)
        from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS

        with pytest.raises(ValueError):
            await reader.search_cosine_hits(scope, "x" * (MAX_MEMORY_QUERY_CHARS + 1), limit=1)
        assert reader.diagnostic()["error_code"] == "child_input_rejected"
        assert reader.health()["recovery_attempts"] == 0
    finally:
        await reader.close()
        cache.close()


@pytest.mark.parametrize("fault", ["wrong_id", "extra", "negative", "nan", "bool", "body", "parent_metadata"])
def test_diagnostic_schema_refuses_malformed_or_private_fields(fault):
    from system_memory_god.storage.daily_vector_snapshot_worker import validate_diagnostic
    value = {"request_id": 3, "stage": "complete", "error_code": "none", "error_type": "none",
             "embedding_ms": 1.0, "numerical_ms": 2.0, "complete": True}
    if fault == "wrong_id":
        value["request_id"] = 4
    elif fault == "extra":
        value["private_text"] = "must-not-escape"
    elif fault == "body":
        value["error_type"] = "private-exception-body"
    elif fault == "parent_metadata":
        value.update(timeout_source="none", request_elapsed_ms=1.0, budget_ms=150)
    else:
        value["embedding_ms"] = {"negative": -1, "nan": float("nan"), "bool": True}[fault]
    with pytest.raises(ValueError):
        validate_diagnostic(value, 3)


@pytest.mark.parametrize("failure", ["embedding", "numerical"])
def test_worker_classifies_model_and_numeric_failures_without_private_body(tmp_path, monkeypatch, failure):
    import json
    import numpy as np
    from system_memory_god.storage import daily_vector_snapshot_worker as worker
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    scope = PipelineScope("owner", "agent", "web", "diag-failure")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    requests = iter([{"version": 1, "id": 1, "scope": scope.key, "op": "warm_snapshot", "generation": generation},
                     {"version": 1, "id": 2, "scope": scope.key, "op": "search_cosine_hits", "query": "synthetic", "limit": 1}])
    frames = []
    class PipeBoundary:
        def recv_bytes(self, maximum):
            try:
                return json.dumps(next(requests)).encode()
            except StopIteration:
                raise EOFError from None
        def send_bytes(self, body):
            frames.append(json.loads(body))
        def close(self):
            pass
    def embed_failure(texts):
        if failure == "embedding":
            raise ValueError("private-body-must-not-escape")
        return [[1.0]]
    monkeypatch.setattr(worker, "_synthetic_embed", embed_failure)
    worker.serve_snapshot(PipeBoundary(), str(cache.root), "synthetic", True)
    assert frames[-1]["ok"] is False
    assert frames[-1]["diagnostic"]["error_code"] == failure + "_failed"
    assert "private-body" not in str(frames)
    cache.close()


@pytest.mark.asyncio
async def test_opt_in_writer_captures_numbers_without_changing_public_receipts(tmp_path):
    scope = PipelineScope("owner", "agent", "web", "chat")
    vector = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                   embedding="synthetic", enable_snapshot=True)
    records = [{"memory_id": "one", "payload": {"text": "synthetic"},
                "payload_hash": "a" * 64}]
    try:
        await vector.put(scope, records)
        receipt = (await vector.receipts(scope, records))[0]
        assert set(receipt) == {"memory_id", "payload_hash", "text_hash", "embedding_dim"}
        row = vector.snapshot_vectors(scope)["one"]
        assert set(row) == {"memory_id", "payload_hash", "text_hash", "embedding_dim", "snapshot"}
        body = base64.b64decode(row["snapshot"]["f32"], validate=True)
        assert hashlib.sha256(body).hexdigest() == row["snapshot"]["sha256"]
        assert len(body) == receipt["embedding_dim"] * 4
    finally:
        await vector.close()


@pytest.mark.asyncio
async def test_snapshot_reader_uses_a_distinct_process_and_never_calls_writer(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    vector = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                   embedding="synthetic", enable_snapshot=True)
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15)
    records = [{"memory_id": "one", "payload": {"text": "synthetic"}, "payload_hash": "a" * 64}]
    reader_pid = None
    try:
        await vector.put(scope, records)
        row = vector.snapshot_vectors(scope)["one"]
        cache.publish(scope, {"one": base64.b64decode(row["snapshot"]["f32"], validate=True)})
        await reader.warm_existing(scope)
        reader_pid = reader.worker_pid
        assert reader_pid is not None and reader_pid != vector.worker_pid
        assert (await reader.search_cosine_hits(scope, "synthetic", limit=1))[0][0] == "one"
        await vector.put(scope, records)
        assert await vector.contains(scope, ("one",))
    finally:
        await reader.close()
        await vector.close()
        cache.close()
    assert not psutil.pid_exists(reader_pid)


@pytest.mark.asyncio
async def test_cleanup_retry_contract_inflight_owned(tmp_path, monkeypatch):
    import sys
    import threading
    import time
    import numpy as np
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "inflight-cleanup")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    reader = SupervisedDailyVectorSnapshot(cache, diagnostics=True, auto_recover=False)
    transport = reader._transport
    loop = asyncio.get_running_loop()
    join_entered, second_entered = asyncio.Event(), asyncio.Event()
    release, held_once, join_returned = threading.Event(), threading.Event(), threading.Event()
    deadlines = []
    physical_deadlines = []
    closers = []
    owned_tasks = ()
    retry_started = None
    try:
        await reader.warm_existing(scope)
        process = transport._process
        original_join = process.join
        original_finish = transport._finish_cleanup

        def controlled_join(*args, **kwargs):
            if not held_once.is_set():
                held_once.set()
                loop.call_soon_threadsafe(join_entered.set)
                release.wait(15)
                try:
                    return original_join(*args, **kwargs)
                finally:
                    join_returned.set()
            return original_join(*args, **kwargs)

        async def observed_finish(action, deadline, *, _cleanup_observation=None):
            deadlines.append(deadline)

            def observed_action():
                physical_deadlines.append(deadline)
                return action()

            return await original_finish(observed_action, deadline,
                                         _cleanup_observation=_cleanup_observation)

        async def retry_close():
            nonlocal retry_started
            retry_started = time.monotonic()
            second_entered.set()
            return await reader.close()

        monkeypatch.setattr(process, "join", controlled_join)
        monkeypatch.setattr(transport, "_finish_cleanup", observed_finish)
        first = asyncio.create_task(reader.close())
        closers.append(first)
        await asyncio.wait_for(join_entered.wait(), 3)
        with pytest.raises(RuntimeError):
            await first
        assert not join_returned.is_set()
        owned_tasks = tuple(transport._closing_tasks)
        assert owned_tasks and any(not task.done() for task in owned_tasks)
        first_physical_deadline = deadlines[0]
        assert physical_deadlines == [first_physical_deadline]
        second = asyncio.create_task(retry_close())
        closers.append(second)
        await asyncio.wait_for(second_entered.wait(), 3)
        assert not join_returned.is_set() and any(not task.done() for task in owned_tasks)
        assert not second.done(), "explicit retry did not wait for its owned cleanup"
        assert physical_deadlines == [first_physical_deadline]
        release.set()
        await second
        assert retry_started is not None and time.monotonic() - retry_started <= 3
        assert physical_deadlines == [first_physical_deadline]
        assert reader.worker_pid is None
    finally:
        primary_active = sys.exc_info()[0] is not None
        release.set()
        cleanup_error = None
        try:
            await asyncio.gather(*closers, return_exceptions=True)
            await asyncio.gather(*(set(owned_tasks) | set(transport._closing_tasks)),
                                 return_exceptions=True)
        except BaseException as error:
            cleanup_error = error
        try:
            await reader.close()
        except BaseException as error:
            cleanup_error = cleanup_error or error
        try:
            cache.close()
        except BaseException as error:
            cleanup_error = cleanup_error or error
        if cleanup_error is not None and not primary_active:
            raise cleanup_error


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [True, False])
async def test_stalled_read_is_bounded_and_does_not_retire_or_block_writer(tmp_path, cancel):
    import time
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    vector = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                   embedding="synthetic", enable_snapshot=True)
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    reader = SupervisedDailyVectorSnapshot(cache, query_timeout=.15, embedding="synthetic-stall")
    records = [{"memory_id": "one", "payload": {"text": "synthetic"}, "payload_hash": "a" * 64}]
    try:
        await vector.put(scope, records)
        row = vector.snapshot_vectors(scope)["one"]
        cache.publish(scope, {"one": base64.b64decode(row["snapshot"]["f32"], validate=True)})
        await reader.warm_existing(scope)
        writer_pid = vector.worker_pid
        read = asyncio.create_task(reader.search_cosine_hits(scope, "synthetic"))
        await asyncio.sleep(.06)
        start = time.monotonic()
        if cancel:
            read.cancel()
            with pytest.raises(asyncio.CancelledError):
                await read
        else:
            with pytest.raises((RuntimeError, TimeoutError)):
                await read
        assert time.monotonic() - start < .3
        await vector.put(scope, [{**records[0], "memory_id": "two"}])
        assert vector.worker_pid == writer_pid
        assert await vector.contains(scope, ("two",))
    finally:
        await reader.close()
        await vector.close()
        cache.close()
