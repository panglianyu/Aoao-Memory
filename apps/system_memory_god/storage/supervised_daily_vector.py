"""Supervised owner for isolated daily vector candidates."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
from contextlib import contextmanager
from _thread import LockType
import hashlib
import json
import multiprocessing
import math
from multiprocessing.connection import Connection, PipeConnection
from multiprocessing.process import BaseProcess
import threading
import struct
import time

from kernel.memory_pipeline_models import PipelineScope, identity
from kernel.daily_memory_contracts import SNAPSHOT_QUERY_TIMEOUT, INTERACTIVE_RECALL_TIMEOUT, RecallPurpose
from .daily_vector_worker import MAX_FRAME_BYTES, serve
from .scoped_stores import ScopedStores


def _initial_diagnostic() -> dict:
    return {"request_id": 0, "stage": "queue", "error_code": "unknown",
            "error_type": "none", "embedding_ms": None, "numerical_ms": None,
            "queue_ms": None, "roundtrip_ms": None, "complete": False}


_CLEANUP_STAGES = frozenset({
    "budget", "dispatch", "dispatched", "lifecycle_wait", "lifecycle_acquired",
    "lifecycle_released", "ipc_wait", "ipc_acquired", "ipc_released",
    "stop_lifecycle_wait", "stop_lifecycle_acquired", "stop_lifecycle_released",
    "termination", "termination_return", "join", "join_return", "stop_return",
    "failed", "finished",
})
_CLEANUP_ERROR_TYPES = frozenset({RuntimeError, ValueError, OSError, TimeoutError,
                                EOFError, MemoryError, PermissionError, ProcessLookupError,
                                asyncio.CancelledError})


class _SnapshotQuery:
    """Parent-only entry deadline and finite diagnostics for one warm query."""

    def __init__(self, guard: LockType, *, budget_seconds: float = SNAPSHOT_QUERY_TIMEOUT,
                 purpose: RecallPurpose = "warm", deadline: float | None = None):
        ceiling = SNAPSHOT_QUERY_TIMEOUT if purpose == "warm" else INTERACTIVE_RECALL_TIMEOUT
        if (type(budget_seconds) not in {int, float} or not math.isfinite(budget_seconds)
                or purpose not in {"warm", "interactive"} or not 0 < budget_seconds <= ceiling):
            raise ValueError("invalid snapshot query budget")
        if deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
            raise ValueError("invalid snapshot caller deadline")
        self.began = time.monotonic()
        self.deadline = min(self.began + budget_seconds, deadline) if deadline is not None else self.began + budget_seconds
        self.guard = guard
        self.request_token: object | None = None
        self.process: BaseProcess | None = None
        self.settled = threading.Event()
        self.watchdog: threading.Timer | None = None
        self.values = {**_initial_diagnostic(), "timeout_source": "none",
                       "request_elapsed_ms": 0.0,
                       "budget_ms": min(budget_seconds, max(0.0, self.deadline - self.began)) * 1000}

    def remaining(self) -> float:
        return self.deadline - time.monotonic()

    def expire(self, source: str) -> None:
        with self.guard:
            if self.values["timeout_source"] == "none":
                self.values.update(timeout_source=source, stage="parent",
                    error_code="parent_deadline", error_type="TimeoutError", complete=False)

    def check(self, source: str) -> None:
        if self.remaining() <= 0:
            self.expire(source)
            raise TimeoutError("vector operation timed out")

    def fail(self, code: str, error_type: str = "RuntimeError") -> None:
        with self.guard:
            if self.values["error_code"] in {"unknown", "none"}:
                self.values.update(stage="parent", error_code=code,
                                   error_type=error_type, complete=False)

    def snapshot(self) -> dict:
        with self.guard:
            self.values["request_elapsed_ms"] = (time.monotonic() - self.began) * 1000
            return dict(self.values)


class SupervisedDailyVector:
    def __init__(self, stores: ScopedStores, *, embedding: str,
                 request_timeout: float = 10.0, max_pending: int = 16,
                 enable_snapshot: bool = False, process_role: str = "writer", diagnostics: bool = False,
                 formal_embedding=None):
        if formal_embedding is not None:
            from kernel.raw_message_profile import FormalEmbeddingBinding

            if embedding != "bge-m3" or type(formal_embedding) is not FormalEmbeddingBinding:
                raise ValueError("explicit formal bge-m3 binding required")
        if (type(stores) is not ScopedStores
                or embedding not in {"bge-m3", "synthetic", "synthetic-stall",
                                     "synthetic-drop-ack", "synthetic-wrong-id",
                                     "synthetic-bad-ready"}
                or type(request_timeout) not in {int, float}
                or not 0 < request_timeout <= 60
                or type(max_pending) is not int or not 1 <= max_pending <= 64
                or type(diagnostics) is not bool or diagnostics and process_role != "snapshot"
                or type(enable_snapshot) is not bool or process_role not in {"writer", "snapshot"}
                or process_role == "snapshot" and (enable_snapshot or embedding not in {
                    "bge-m3", "synthetic", "synthetic-stall"})):
            raise ValueError("invalid supervised vector configuration")
        self.stores = stores
        self.embedding = embedding
        self.request_timeout = float(request_timeout)
        self.max_pending = max_pending
        self.enable_snapshot = enable_snapshot
        self.process_role = process_role
        self.diagnostics = diagnostics
        self.formal_embedding = formal_embedding
        self._formal_attempted = False
        self._formal_state = "not_started"
        self._formal_actual = None
        self._formal_load = 0
        self._formal_encode = 0
        self._formal_unknown = False
        self._formal_facts = threading.Lock() if formal_embedding is not None else None
        self._formal_owner = None
        self._diagnostic: dict = {}
        self._diagnostic_guard = threading.Lock()
        self._cleanup_diagnostic_guard = threading.Lock() if diagnostics else None
        self._cleanup_diagnostic_attempt = 0
        self._cleanup_diagnostic_began: float | None = None
        self._cleanup_diagnostic_deadline: float | None = None
        self._cleanup_diagnostic_token: object | None = None
        self._cleanup_diagnostic_active = False
        self._cleanup_diagnostic_frames: list[dict] = []
        self._cleanup_diagnostic_dropped = 0
        self._cleanup_diagnostic_complete = False
        self._startup_began: float | None = None
        self._startup = {"stage": "not_started", "error_code": "none",
                         "error_type": "none", "elapsed_ms": 0.0}
        self._snapshot_rows: dict[PipelineScope, dict[str, dict]] = {}
        self._snapshot_failed: set[PipelineScope] = set()
        self._pending_guard = threading.Lock()
        self._pending = 0
        self._lock = threading.Lock()
        self._lifecycle = threading.RLock()
        self._connection: Connection | PipeConnection | None = None
        self._process: BaseProcess | None = None
        self._termination_process: BaseProcess | None = None
        self._termination_error: OSError | None = None
        self._termination_deadline: float | None = None
        self._cleanup_failure: object | None = None
        self._close_deadline: float | None = None
        self._close_budget_failure: object | None = None
        self._closing_tasks: set[asyncio.Task] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._request_id = 0
        self._active_request: object | None = None
        self._prepared_scopes: set[PipelineScope] = set()
        self._recovery_pending: set[PipelineScope] = set()
        self._recovery_attempts: dict[PipelineScope, int] = {}
        self._recovery_due: dict[PipelineScope, float] = {}
        self._recovery_exhausted: set[PipelineScope] = set()
        self._recovery_task: asyncio.Task | None = None
        self._recovery_wake = asyncio.Event()
        self._closed = False

    @property
    def worker_pid(self) -> int | None:
        process = self._process
        return process.pid if process is not None else None

    def formal_health(self) -> dict:
        # Health must not wait behind a slow worker startup holding the lifecycle lock.
        if self.formal_embedding is None:
            return {"state": "not_started", "actual": None, "load_calls": 0, "encode_calls": 0}
        with self._formal_facts:
            if not self._formal_attempted:
                return {"state": "not_started", "actual": None, "load_calls": 0, "encode_calls": 0}
            actual = self._formal_actual
            process = self._process
            result = {"state": "stopped" if self._closed else self._formal_state,
                      "actual": dict(actual) if actual is not None else None,
                      "load_calls": self._formal_load if actual is not None else None,
                      "encode_calls": self._formal_encode if actual is not None and not self._formal_unknown else None}
        alive = False
        if result["state"] == "ready" and process is not None:
            try:
                alive = process.is_alive()
            except (ValueError, OSError, AssertionError):
                alive = False
        with self._formal_facts:
            owner = self._formal_owner
            owner_valid = (owner is None and self._active_request is None or
                           owner is not None and owner[0] is self._active_request)
            if self._formal_unknown:
                result["encode_calls"] = None
            if result["state"] == "ready" and (not alive or self._process is not process or self._formal_unknown or not owner_valid):
                result["state"] = "failed"
        return result

    def _formal_result(self, operation, result, arguments):
        """Validate the existing business acknowledgement before committing facts."""
        if operation in {"warm_existing", "warm_snapshot"}:
            if result is not True:
                raise RuntimeError("invalid vector response")
        elif operation == "contains":
            if type(result) is not bool:
                raise RuntimeError("invalid vector response")
        elif operation == "present_ids":
            if type(result) is not list or any(type(key) is not str for key in result) or len(set(result)) != len(result) or not set(result) <= set(arguments["ids"]):
                raise RuntimeError("invalid vector response")
        elif operation == "search_cosine_hits":
            if type(result) is not list or len(result) > arguments["limit"]:
                raise RuntimeError("invalid vector response")
            seen = set()
            for row in result:
                if type(row) is not list or len(row) != 2 or type(row[0]) is not str or row[0] in seen or type(row[1]) not in {int, float} or not math.isfinite(row[1]) or not -1 <= row[1] <= 1:
                    raise RuntimeError("invalid vector response")
                identity(row[0])
                seen.add(row[0])
        elif operation in {"put", "put_snapshot", "receipts", "receipts_snapshot"}:
            records = arguments["records"]
            if type(result) is not list or len(result) != len(records):
                raise RuntimeError("invalid vector response")
            for row, record in zip(result, records):
                keys = {"memory_id", "payload_hash", "text_hash", "embedding_dim"} | ({"snapshot"} if operation.endswith("snapshot") else set())
                if (type(row) is not dict or set(row) != keys or row["memory_id"] != record["memory_id"]
                        or row["payload_hash"] != record["payload_hash"] or row["text_hash"] != hashlib.sha256(record["payload"]["text"].encode()).hexdigest()
                        or type(row["embedding_dim"]) is not int or row["embedding_dim"] != 1024):
                    raise RuntimeError("invalid vector response")
                if "snapshot" in keys:
                    snapshot = row["snapshot"]
                    if type(snapshot) is dict and snapshot == {"schema": 1, "unavailable": "vector_capacity_exceeded"} and type(snapshot["schema"]) is int:
                        continue
                    if type(snapshot) is not dict or set(snapshot) != {"schema", "f32", "sha256"} or type(snapshot["schema"]) is not int or snapshot["schema"] != 1 or type(snapshot["f32"]) is not str or len(snapshot["f32"]) > 10924:
                        raise RuntimeError("invalid vector response")
                    body = base64.b64decode(snapshot["f32"], validate=True)
                    if len(body) != 4096 or hashlib.sha256(body).hexdigest() != snapshot["sha256"] or not all(math.isfinite(value) for value in struct.unpack("<1024f", body)):
                        raise RuntimeError("invalid vector response")
        else:
            raise RuntimeError("invalid vector response")

    @contextmanager
    def _acquire(self, cancel_event: threading.Event, deadline: float,
                 query: _SnapshotQuery | None = None):
        while True:
            if cancel_event.is_set():
                raise RuntimeError("vector operation cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if query is not None:
                    query.expire("queue_deadline")
                raise TimeoutError("vector queue timed out")
            if self._lock.acquire(timeout=min(0.05, remaining)):
                break
        try:
            if query is not None:
                query.check("queue_deadline")
            yield
        finally:
            self._lock.release()

    def _start(self, deadline: float, cancel_event: threading.Event) -> None:
        with self._lifecycle:
            self._start_locked(deadline, cancel_event)

    def startup_diagnostic(self) -> dict:
        """Read finite parent-side facts without waiting on the handshake lock."""
        with self._diagnostic_guard:
            result = dict(self._startup)
            if self._startup_began is not None and result["stage"] in {"spawn", "waiting_ready"}:
                result["elapsed_ms"] = max(0.0, (time.monotonic() - self._startup_began) * 1000)
            return result

    def _startup_note(self, stage: str, code: str = "none", error: BaseException | None = None) -> None:
        with self._diagnostic_guard:
            self._startup.update(stage=stage, error_code=code,
                error_type=(type(error).__name__ if type(error) in {
                    RuntimeError, ValueError, OSError, TimeoutError, EOFError, MemoryError,
                    asyncio.CancelledError} else "OtherError") if error is not None else "none",
                elapsed_ms=max(0.0, (time.monotonic() - self._startup_began) * 1000)
                if self._startup_began is not None else 0.0)

    def _start_locked(self, deadline: float, cancel_event: threading.Event) -> None:
        if self._closed:
            raise RuntimeError("vector worker closed")
        if cancel_event.is_set():
            raise RuntimeError("vector operation cancelled")
        if time.monotonic() >= deadline:
            raise TimeoutError("vector startup timed out")
        if self._process is not None:
            return
        if self.formal_embedding is not None and self._formal_attempted:
            raise RuntimeError("formal worker startup already attempted")
        self._startup_began = time.monotonic()
        self._startup_note("spawn")
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        entry = serve
        if self.process_role == "snapshot":
            from .daily_vector_snapshot_worker import serve_snapshot
            entry = serve_snapshot
        args: tuple = (child, str(self.stores.root), self.embedding)
        if self.process_role == "snapshot" and self.diagnostics:
            args += (True,)
        process = (context.Process(target=entry, args=args, kwargs={"formal_embedding": self.formal_embedding.as_payload()})
                   if self.formal_embedding is not None else context.Process(target=entry, args=args))
        try:
            if self.formal_embedding is not None:
                with self._formal_facts:
                    self._formal_attempted = True
                    self._formal_state = "starting"
                    self._formal_load = self._formal_encode = None
            process.start()
        except BaseException as exc:
            if self.formal_embedding is not None:
                with self._formal_facts:
                    self._formal_state = "failed"
            self._startup_note("failed", "spawn_failed", exc)
            parent.close()
            child.close()
            raise
        child.close()
        self._connection = parent
        self._process = process
        self._startup_note("waiting_ready")
        failure_code = "handshake_error"
        try:
            while True:
                if cancel_event.is_set():
                    failure_code = "cancelled"
                    raise RuntimeError("vector operation cancelled")
                if parent.poll(min(0.05, max(0, deadline - time.monotonic()))):
                    failure_code = "invalid_ready"
                    frame = parent.recv_bytes(4096)
                    if self.formal_embedding is not None:
                        from .daily_embedding_adapter import _formal_json
                        ready = _formal_json(frame)
                    else:
                        ready = json.loads(frame)
                    valid = ready == {"version": 1, "ready": True} if self.formal_embedding is None else False
                    if self.formal_embedding is not None:
                        expected = {key: value for key, value in self.formal_embedding.as_payload().items()
                                    if key in {"model_id", "revision", "material_sha256", "module_policy_sha256", "loader_policy_sha256",
                                               "device", "dtype", "dimension", "normalize", "backend", "max_seq_length"}}
                        expected["warm_calls"] = 1
                        actual = ready.get("actual") if type(ready) is dict else None
                        valid = (type(ready) is dict and set(ready) == {"version", "ready", "binding_sha256", "actual"}
                            and type(ready["version"]) is int and ready["version"] == 2 and ready["ready"] is True
                            and ready["binding_sha256"] == self.formal_embedding.binding_sha256 and type(actual) is dict
                            and set(actual) == set(expected) and all(type(actual[key]) is type(value) and actual[key] == value for key, value in expected.items()))
                    if valid:
                        if cancel_event.is_set():
                            failure_code = "cancelled"
                            raise RuntimeError("vector operation cancelled")
                        if time.monotonic() >= deadline:
                            failure_code = "ready_deadline"
                            raise TimeoutError("vector startup timed out")
                        self._startup_note("ready")
                        if self.formal_embedding is not None:
                            with self._formal_facts:
                                self._formal_actual = dict(actual)
                                self._formal_state, self._formal_load = "ready", 1
                                if not self._formal_unknown:
                                    self._formal_encode = 0
                        return
                    failure_frame = (self.formal_embedding is None or type(ready) is dict and
                        set(ready) == {"version", "ready", "error"} and type(ready["version"]) is int and
                        ready["version"] == 2 and ready["ready"] is False and ready["error"] in {"owner", "embedding"})
                    if failure_frame and type(ready) is dict and ready.get("error") == "owner":
                        failure_code = "owner_rejected"
                        raise RuntimeError("vector owner unavailable")
                    if failure_frame and type(ready) is dict and ready.get("error") == "embedding":
                        failure_code = "embedding_rejected"
                    raise RuntimeError("invalid vector startup")
                if not process.is_alive():
                    failure_code = "process_exit"
                    raise RuntimeError("vector worker exited during startup")
                if time.monotonic() >= deadline:
                    failure_code = "ready_deadline"
                    raise TimeoutError("vector startup timed out")
        except Exception as exc:
            if self.formal_embedding is not None:
                with self._formal_facts:
                    self._formal_state = "failed"
            self._startup_note("failed", failure_code, exc)
            self._stop()
            raise

    def _request_termination_locked(self, process: BaseProcess, *, _cleanup_observation: object | None = None) -> None:
        if self._process is not process or self._termination_process is process or not process.is_alive():
            return
        self._termination_process = process
        self._termination_deadline = time.monotonic() + 3
        self._termination_error = None
        self._cleanup_diagnostic_note("termination", attempt=_cleanup_observation, process_alive=True)
        try:
            process.terminate()
        except OSError as exc:
            # The IPC owner still confirms exit. A timer cannot discard that
            # obligation or prevent the caller's deadline notification.
            self._termination_error = exc
        self._cleanup_diagnostic_note("termination_return", attempt=_cleanup_observation,
                                      error=self._termination_error)

    def _stop(self, *, retry_failure: object | None = None, deadline: float | None = None,
              attempt_deadline: float | None = None, _cleanup_observation: object | None = None) -> None:
        if deadline is not None and time.monotonic() >= deadline:
            self._record_cleanup_failure(deadline=attempt_deadline)
            raise self._cleanup_deadline_error() from self._termination_error
        self._cleanup_diagnostic_note("stop_lifecycle_wait", attempt=_cleanup_observation)
        acquired = (self._lifecycle.acquire() if deadline is None else
                    self._lifecycle.acquire(timeout=max(0, deadline - time.monotonic())))
        if not acquired:
            self._record_cleanup_failure(deadline=attempt_deadline)
            raise RuntimeError("vector lifecycle owner is busy")
        try:
            self._cleanup_diagnostic_note("stop_lifecycle_acquired", attempt=_cleanup_observation)
            if deadline is not None and time.monotonic() >= deadline:
                self._record_cleanup_failure(deadline=attempt_deadline)
                raise self._cleanup_deadline_error() from self._termination_error
            self._stop_locked(retry_failure=retry_failure, deadline=deadline, attempt_deadline=attempt_deadline,
                              _cleanup_observation=_cleanup_observation)
            self._cleanup_diagnostic_note("stop_return", attempt=_cleanup_observation)
        finally:
            self._lifecycle.release()
            self._cleanup_diagnostic_note("stop_lifecycle_released", attempt=_cleanup_observation)

    def _record_cleanup_failure(self, *, deadline: float | None = None) -> None:
        with self._pending_guard:
            if deadline is not None and self._close_deadline != deadline:
                return
            if self._cleanup_failure is None:
                self._cleanup_failure = object()
            self._close_budget_failure = self._cleanup_failure

    def _retry_cleanup_locked(self, retry_failure: object | None) -> None:
        if retry_failure is not None and self._cleanup_failure is retry_failure:
            self._termination_process = None
            self._termination_deadline = None
            self._termination_error = None
            self._cleanup_failure = None

    def _stop_locked(self, *, retry_failure: object | None = None, deadline: float | None = None,
                     attempt_deadline: float | None = None, _cleanup_observation: object | None = None) -> None:
        connection, process = self._connection, self._process
        self._retry_cleanup_locked(retry_failure)
        self._connection = None
        if connection is not None:
            connection.close()
        if process is not None:
            remaining = (max(0, self._termination_deadline - time.monotonic())
                         if self._termination_process is process and self._termination_deadline is not None else .25)
            if deadline is not None:
                remaining = min(remaining, max(0, deadline - time.monotonic()))
            self._cleanup_diagnostic_note("join", attempt=_cleanup_observation)
            process.join(timeout=min(.25, remaining))
            self._cleanup_diagnostic_note("join_return", attempt=_cleanup_observation)
            if process.is_alive():
                self._request_termination_locked(process, _cleanup_observation=_cleanup_observation)
                if self._termination_deadline is not None:
                    if deadline is not None:
                        self._termination_deadline = min(self._termination_deadline, deadline)
                    self._cleanup_diagnostic_note("join", attempt=_cleanup_observation)
                    process.join(timeout=max(0, self._termination_deadline - time.monotonic()))
                    self._cleanup_diagnostic_note("join_return", attempt=_cleanup_observation)
            if process.is_alive():
                self._record_cleanup_failure(deadline=attempt_deadline)
                raise RuntimeError("vector worker did not exit") from self._termination_error
            process.close()
            self._process = None
            self._termination_process = None
            self._termination_deadline = None
            self._termination_error = None
            self._cleanup_failure = None
        with self._pending_guard:
            self._close_deadline = None
            self._close_budget_failure = None

    def _call(self, operation: str, scope: PipelineScope,
              cancel_event: threading.Event, *, keep_owner: bool = False,
              allow_start: bool = True, _query: _SnapshotQuery | None = None,
              _deadline: float | None = None, _notify: Callable[[], None] | None = None, **arguments):
        if _query is not None and (type(_query) is not _SnapshotQuery or self.process_role != "snapshot"
                or operation != "search_cosine_hits" or allow_start or keep_owner):
            raise ValueError("snapshot deadline requires a prewarmed read")
        if not self.diagnostics:
            return self._call_transport(operation, scope, cancel_event,
                keep_owner=keep_owner, allow_start=allow_start, _query=_query,
                _deadline=_deadline, _notify=_notify, **arguments)
        clock = time.monotonic
        began = _query.began if _query is not None else clock()
        diagnostic = _query.values if _query is not None else _initial_diagnostic()
        try:
            return self._call_transport(operation, scope, cancel_event,
                keep_owner=keep_owner, allow_start=allow_start, _query=_query,
                _deadline=_deadline, _notify=_notify, _diagnostic=diagnostic, **arguments)
        except (RuntimeError, ValueError, OSError, EOFError, TimeoutError) as exc:
            from .daily_vector_snapshot_worker import ERROR_TYPES
            codes = {"vector operation cancelled": "parent_cancelled", "vector queue timed out": "queue_deadline",
                     "shared vector owner not prepared": "not_prepared", "vector worker exited": "child_exited",
                     "vector operation timed out": "parent_deadline", "vector startup timed out": "parent_deadline",
                     "invalid vector response": "protocol_error", "invalid snapshot response": "protocol_error",
                     "vector worker unavailable": "transport_error"}
            with self._diagnostic_guard:
                if diagnostic["error_code"] in {"unknown", "none"}:
                    diagnostic.update(stage="parent", complete=False,
                        error_code=codes.get(str(exc), "protocol_error"),
                        error_type=type(exc).__name__ if type(exc).__name__ in ERROR_TYPES else "OtherError")
            raise
        finally:
            with self._diagnostic_guard:
                diagnostic["roundtrip_ms"] = (clock() - began) * 1000
                if _query is not None:
                    diagnostic["request_elapsed_ms"] = diagnostic["roundtrip_ms"]
                self._diagnostic = dict(diagnostic)

    def diagnostic(self) -> dict:
        with self._diagnostic_guard:
            return dict(self._diagnostic) if self.diagnostics else {}

    def _cleanup_diagnostic_drop(self) -> None:
        # Saturating best-effort counter; contention never waits for this observer.
        self._cleanup_diagnostic_dropped = min(1_000_000, self._cleanup_diagnostic_dropped + 1)
        self._cleanup_diagnostic_complete = False

    def _cleanup_diagnostic_unlock(self) -> None:
        guard = self._cleanup_diagnostic_guard
        if guard is None:
            return
        try:
            guard.release()
        except Exception:
            self._cleanup_diagnostic_drop()

    def _cleanup_diagnostic_begin(self, deadline: float) -> None:
        if not self.diagnostics:
            return
        self._cleanup_diagnostic_active = False
        self._cleanup_diagnostic_token = None
        guard = self._cleanup_diagnostic_guard
        if guard is None:
            return
        acquired = False
        token = None
        try:
            acquired = guard.acquire(blocking=False)
            if not acquired:
                self._cleanup_diagnostic_drop()
                return
            self._cleanup_diagnostic_attempt = min(1_000_000, self._cleanup_diagnostic_attempt + 1)
            self._cleanup_diagnostic_began = time.monotonic()
            self._cleanup_diagnostic_deadline = deadline
            token = object()
            self._cleanup_diagnostic_token = token
            self._cleanup_diagnostic_active = True
            self._cleanup_diagnostic_frames = []
            self._cleanup_diagnostic_dropped = 0
            self._cleanup_diagnostic_complete = False
        except Exception:
            self._cleanup_diagnostic_active = False
            self._cleanup_diagnostic_drop()
        finally:
            if acquired:
                self._cleanup_diagnostic_unlock()
        self._cleanup_diagnostic_note("budget", attempt=token)

    def _cleanup_diagnostic_capture(self, deadline: float) -> object | None:
        """Opaque identity, captured before dispatch; equal deadlines are not identities."""
        if not self.diagnostics:
            return None
        guard = self._cleanup_diagnostic_guard
        if guard is None:
            return None
        acquired = False
        try:
            acquired = guard.acquire(blocking=False)
            if not acquired:
                self._cleanup_diagnostic_drop()
                return None
            if self._cleanup_diagnostic_active and deadline == self._cleanup_diagnostic_deadline:
                return self._cleanup_diagnostic_token
            return None
        except Exception:
            self._cleanup_diagnostic_drop()
            return None
        finally:
            if acquired:
                self._cleanup_diagnostic_unlock()

    def _cleanup_diagnostic_note(self, stage: str, *, attempt: object | None = None,
                                 process_alive: bool | None = None,
                                 error: BaseException | None = None) -> None:
        """Best-effort local facts; unknown liveness is omitted, never inferred."""
        if (not self.diagnostics or attempt is None or not self._cleanup_diagnostic_active
                or attempt is not self._cleanup_diagnostic_token):
            return
        guard = self._cleanup_diagnostic_guard
        if guard is None:
            return
        acquired = False
        try:
            acquired = guard.acquire(blocking=False)
            if not acquired:
                if self._cleanup_diagnostic_active and attempt is self._cleanup_diagnostic_token:
                    self._cleanup_diagnostic_drop()
                return
            if (not self._cleanup_diagnostic_active or attempt is not self._cleanup_diagnostic_token
                    or self._cleanup_diagnostic_began is None):
                return
            if stage not in _CLEANUP_STAGES or len(self._cleanup_diagnostic_frames) >= 32:
                self._cleanup_diagnostic_drop()
                if stage in {"finished", "failed"}:
                    self._cleanup_diagnostic_active = False
                return
            elapsed = (time.monotonic() - self._cleanup_diagnostic_began) * 1000
            frame = {"stage": stage, "elapsed_ms": round(min(999_999_999.999, max(0.0, elapsed)), 3)}
            if type(process_alive) is bool:
                frame["process_alive"] = process_alive
            if error is not None:
                frame["error_type"] = type(error).__name__ if type(error) in _CLEANUP_ERROR_TYPES else "OtherError"
            # Fixed fields, finite scalars and 32 frames bound the serialized snapshot below 8 KiB.
            self._cleanup_diagnostic_frames.append(frame)
            if stage == "finished":
                self._cleanup_diagnostic_complete = not self._cleanup_diagnostic_dropped
            if stage in {"finished", "failed"}:
                self._cleanup_diagnostic_active = False
        except Exception:
            self._cleanup_diagnostic_drop()
        finally:
            if acquired:
                self._cleanup_diagnostic_unlock()

    def _cleanup_diagnostic_snapshot(self) -> dict:
        """Read only finite parent-local cleanup facts without waiting on any owner."""
        if not self.diagnostics:
            return {}
        guard = self._cleanup_diagnostic_guard
        if guard is None:
            return {"schema": 1, "attempt": self._cleanup_diagnostic_attempt, "frames": [],
                    "dropped": self._cleanup_diagnostic_dropped, "complete": False}
        acquired = False
        try:
            acquired = guard.acquire(blocking=False)
            if not acquired:
                self._cleanup_diagnostic_drop()
                return {"schema": 1, "attempt": self._cleanup_diagnostic_attempt, "frames": [],
                        "dropped": self._cleanup_diagnostic_dropped, "complete": False}
            return {"schema": 1, "attempt": self._cleanup_diagnostic_attempt,
                    "frames": [dict(frame) for frame in self._cleanup_diagnostic_frames],
                    "dropped": self._cleanup_diagnostic_dropped,
                    "complete": self._cleanup_diagnostic_complete}
        except Exception:
            self._cleanup_diagnostic_drop()
            return {"schema": 1, "attempt": self._cleanup_diagnostic_attempt, "frames": [],
                    "dropped": self._cleanup_diagnostic_dropped, "complete": False}
        finally:
            if acquired:
                self._cleanup_diagnostic_unlock()

    def _start_query_watchdog(self, query: _SnapshotQuery, notify: Callable[[], None]) -> None:
        def expired() -> None:
            while not query.settled.is_set():
                remaining = query.remaining()
                if remaining <= 0:
                    break
                if query.settled.wait(remaining):
                    return
            with self._lifecycle:
                if query.settled.is_set():
                    return
                if query.request_token is None:
                    query.expire("queue_deadline")
                elif self._active_request is query.request_token and self._process is query.process:
                    query.expire("watchdog_deadline")
                    if query.process is not None:
                        self._request_termination_locked(query.process)
                else:
                    return
            notify()

        query.watchdog = threading.Timer(max(.001, query.remaining()), expired)
        query.watchdog.daemon = True
        query.watchdog.start()

    def _call_transport(self, operation: str, scope: PipelineScope,
                        cancel_event: threading.Event, *, keep_owner: bool = False,
                        allow_start: bool = True, _query: _SnapshotQuery | None = None,
                        _deadline: float | None = None, _notify: Callable[[], None] | None = None,
                        _diagnostic: dict | None = None, **arguments):
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        clock = time.monotonic
        deadline = (_query.deadline if _query is not None else _deadline if _deadline is not None else clock() + (
            60 if self.process_role == "snapshot" and operation == "warm_snapshot" else self.request_timeout))
        queued = _query.began if _query is not None else clock()
        with self._acquire(cancel_event, deadline, _query):
            if _diagnostic is not None:
                with self._diagnostic_guard:
                    _diagnostic["queue_ms"] = (clock() - queued) * 1000
            if self._closed:
                raise RuntimeError("vector worker closed")
            if cancel_event.is_set():
                raise RuntimeError("vector operation cancelled")
            if not allow_start:
                with self._pending_guard:
                    if scope not in self._prepared_scopes:
                        raise RuntimeError("shared vector owner not prepared")
            if allow_start:
                with self._lifecycle:
                    if self._process is not None and not self._process.is_alive():
                        self._stop_locked()
                    self._start_locked(deadline, cancel_event)
            connection = self._connection
            process = self._process
            if connection is None or process is None or not process.is_alive():
                raise RuntimeError("shared vector owner not prepared" if not allow_start
                                   else "vector worker unavailable")
            self._request_id += 1
            request = {"version": 1, "id": self._request_id,
                       "op": operation, "scope": scope.key, **arguments}
            data = json.dumps(request, ensure_ascii=True, separators=(",", ":"),
                              allow_nan=False).encode()
            if len(data) > MAX_FRAME_BYTES:
                raise ValueError("vector request too large")
            if _query is not None:
                _query.check("transport_deadline")
            if _diagnostic is not None:
                with self._diagnostic_guard:
                    _diagnostic["request_id"] = self._request_id
            request_token = object()
            with self._lifecycle:
                if self._closed or self._process is not process:
                    raise RuntimeError("vector worker unavailable")
                if self.formal_embedding is not None:
                    with self._formal_facts:
                        self._active_request = request_token
                        self._formal_owner = (request_token, cancel_event)
                else:
                    self._active_request = request_token
                if _query is not None:
                    _query.request_token, _query.process = request_token, process
            # This watchdog never needs the IPC lock: a blocked pipe write must
            # not make shutdown or the operation deadline unbounded.
            watchdog_settled = _query.settled if _query is not None else threading.Event()
            def expired() -> None:
                while not watchdog_settled.is_set():
                    remaining = deadline - clock()
                    if remaining <= 0:
                        break
                    if watchdog_settled.wait(remaining):
                        return
                if watchdog_settled.is_set():
                    return
                if _notify is not None:
                    _notify()
                with self._lifecycle:
                    if self._active_request is request_token and self._process is process:
                        # The IPC owner, not the timer, settles the parent's IO
                        # and confirms exit against the same termination budget.
                        self._request_termination_locked(process)

            watchdog = _query.watchdog if _query is not None else threading.Timer(max(0.001, deadline - clock()), expired)
            if watchdog is None:
                raise RuntimeError("snapshot watchdog not armed")
            if _query is None:
                watchdog.daemon = True
                watchdog.start()
            sent = False
            accepted = False
            try:
                if _query is not None:
                    _query.check("transport_deadline")
                elif clock() >= deadline:
                    raise TimeoutError("vector operation timed out")
                if cancel_event.is_set():
                    raise RuntimeError("vector operation cancelled")
                if self.formal_embedding is not None:
                    sent = True
                connection.send_bytes(data)
                sent = True
                while True:
                    if cancel_event.is_set() and not keep_owner:
                        raise RuntimeError("vector operation cancelled")
                    if _query is not None:
                        _query.check("transport_deadline")
                    if connection.poll(min(0.05, max(0, deadline - clock()))):
                        frame = connection.recv_bytes(MAX_FRAME_BYTES)
                        if self.formal_embedding is not None:
                            from .daily_embedding_adapter import _formal_json
                            try:
                                response = _formal_json(frame)
                            except (ValueError, TypeError, UnicodeError):
                                raise RuntimeError("invalid vector response") from None
                        else:
                            response = json.loads(frame)
                        if (type(response) is not dict or response.get("version") != 1
                                or response.get("id") != self._request_id):
                            raise RuntimeError("invalid vector response")
                        if type(response.get("ok")) is not bool:
                            raise RuntimeError("invalid vector response")
                        if (self.process_role == "snapshot" or self.formal_embedding is not None) and (
                                type(response.get("version")) is not int
                                or type(response.get("id")) is not int
                                or set(response) != {"version", "id", "ok", "result" if response["ok"] else "error"}
                                    | ({"diagnostic"} if self.diagnostics else set())
                                    | ({"formal"} if self.formal_embedding is not None else set())):
                            raise RuntimeError("invalid snapshot response")
                        if self.diagnostics:
                            from .daily_vector_snapshot_worker import validate_diagnostic
                            try:
                                diagnostic = validate_diagnostic(response["diagnostic"], self._request_id)
                                if diagnostic["complete"] != response["ok"]:
                                    raise ValueError("diagnostic result mismatch")
                            except (ValueError, TypeError, KeyError):
                                raise RuntimeError("invalid snapshot response") from None
                            with self._diagnostic_guard:
                                if _diagnostic is not None:
                                    if _query is not None and _diagnostic["timeout_source"] != "none":
                                        for field in ("embedding_ms", "numerical_ms"):
                                            _diagnostic[field] = diagnostic[field]
                                    else:
                                        _diagnostic.update(diagnostic)
                        if _query is not None:
                            _query.check("transport_deadline")
                        elif clock() >= deadline:
                            raise TimeoutError("vector operation timed out")
                        if not response["ok"]:
                            if response.get("error") == "ValueError":
                                raise ValueError("vector operation rejected")
                            raise RuntimeError("vector operation failed")
                        if self.formal_embedding is not None:
                            try:
                                self._formal_result(operation, response["result"], arguments)
                            except (ValueError, TypeError, KeyError):
                                raise RuntimeError("invalid vector response") from None
                            formal = response["formal"]
                            count = formal.get("encode_calls") if type(formal) is dict else None
                            if (type(formal) is not dict or set(formal) != {"encode_calls"}
                                    or type(count) is not int or count < 0):
                                raise RuntimeError("invalid vector response")
                        with self._lifecycle:
                            if self.formal_embedding is not None:
                                with self._formal_facts:
                                    valid_owner = (self._active_request is request_token and self._process is process and
                                        self._formal_owner is not None and self._formal_owner[0] is request_token and
                                        self._formal_owner[1] is cancel_event)
                                    expired = clock() >= deadline or (_query is not None and
                                        (_query.remaining() <= 0 or _query.values["timeout_source"] != "none"))
                                    if not valid_owner or self._closed or cancel_event.is_set() or expired:
                                        if sent:
                                            self._formal_unknown = True
                                            self._formal_encode = None
                                        if expired:
                                            raise TimeoutError("vector operation timed out")
                                        raise RuntimeError("vector operation cancelled" if cancel_event.is_set() else "vector worker unavailable")
                                    if self._formal_encode is not None and count < self._formal_encode:
                                        self._formal_unknown = True
                                        self._formal_encode = None
                                        raise RuntimeError("invalid vector response")
                                    if not self._formal_unknown:
                                        self._formal_encode = count
                                    accepted = True
                                    self._formal_owner = None
                                    self._active_request = None
                            else:
                                if (self._active_request is not request_token
                                        or self._process is not process):
                                    raise RuntimeError("vector worker unavailable")
                                self._active_request = None
                        return response.get("result")
                    if _query is not None:
                        _query.check("transport_deadline")
                    if not process.is_alive():
                        raise RuntimeError("vector worker exited")
                    if _query is None and clock() >= deadline:
                        raise TimeoutError("vector operation timed out")
            except TimeoutError as exc:
                # The RPC owner may see the deadline before its watchdog.
                # Do not spend the exit-observation allowance joining a child
                # that has not yet received this request's termination signal.
                with self._lifecycle:
                    if self._active_request is request_token:
                        self._request_termination_locked(process)
                try:
                    self._stop()
                except (RuntimeError, OSError) as cleanup_error:
                    raise exc from cleanup_error
                raise
            except (OSError, EOFError):
                timed_out = clock() >= deadline
                if timed_out and _query is not None:
                    _query.expire("transport_deadline")
                if timed_out:
                    with self._lifecycle:
                        if self._active_request is request_token:
                            self._request_termination_locked(process)
                try:
                    self._stop()
                except (RuntimeError, OSError) as cleanup_error:
                    if timed_out or _query is None and time.monotonic() >= deadline:
                        raise TimeoutError("vector operation timed out") from cleanup_error
                    raise RuntimeError("vector worker unavailable") from cleanup_error
                if timed_out or _query is None and time.monotonic() >= deadline:
                    raise TimeoutError("vector operation timed out") from None
                raise RuntimeError("vector worker unavailable") from None
            except (RuntimeError, json.JSONDecodeError):
                if not (keep_owner and cancel_event.is_set() and not sent):
                    self._stop()
                raise
            finally:
                with self._lifecycle:
                    if self.formal_embedding is not None:
                        with self._formal_facts:
                            owner = self._formal_owner
                            if owner is not None and owner[0] is request_token and owner[1] is cancel_event:
                                if sent and not accepted:
                                    self._formal_unknown = True
                                    self._formal_encode = None
                                self._formal_owner = None
                            if self._active_request is request_token:
                                self._active_request = None
                    elif self._active_request is request_token:
                        self._active_request = None
                    watchdog_settled.set()
                    watchdog.cancel()

    async def _invoke(self, operation: str, scope: PipelineScope, *,
                      keep_owner: bool = False, allow_start: bool = True,
                      _query: _SnapshotQuery | None = None, **arguments):
        deadline = (_query.deadline if _query is not None else time.monotonic() + (
            60 if self.process_role == "snapshot" and operation == "warm_snapshot" else self.request_timeout))
        with self._pending_guard:
            if self._closed:
                if _query is not None:
                    _query.fail("not_prepared")
                raise RuntimeError("vector worker closed")
            if self._pending >= self.max_pending:
                if _query is not None:
                    _query.fail("queue_rejected")
                raise RuntimeError("vector queue full")
            self._pending += 1
        cancel_event = threading.Event()
        deadline_signal = None
        state = {"started": False, "released": False}
        task = None

        def release() -> None:
            with self._pending_guard:
                if state["released"]:
                    return
                state["released"] = True
                self._pending -= 1
            if _query is not None:
                _query.settled.set()
                if _query.watchdog is not None:
                    _query.watchdog.cancel()

        def run_call():
            with self._pending_guard:
                if state["released"]:
                    return None
                state["started"] = True
            try:
                return self._call(operation, scope, cancel_event, keep_owner=keep_owner,
                    allow_start=allow_start, _query=_query, _deadline=deadline, _notify=notify, **arguments)
            finally:
                # Only the synchronous owner can settle a running RPC. An
                # asyncio cancellation does not stop a blocked executor thread.
                release()

        def completed(done: asyncio.Task) -> None:
            try:
                done.exception()
            except asyncio.CancelledError:
                with self._pending_guard:
                    not_started = not state["started"]
                if not_started:
                    release()

        try:
            loop = asyncio.get_running_loop()
            deadline_signal = loop.create_future()

            def notify() -> None:
                def signal() -> None:
                    if not deadline_signal.done():
                        deadline_signal.set_result(None)
                try:
                    loop.call_soon_threadsafe(signal)
                except RuntimeError:
                    # A closed loop cannot change actual RPC ownership.
                    return

            if _query is not None:
                self._start_query_watchdog(_query, notify)
            task = asyncio.create_task(asyncio.to_thread(run_call))
            task.add_done_callback(completed)
            try:
                waiting = (task, deadline_signal) if deadline_signal is not None else (task,)
                done, _ = await asyncio.wait(waiting, timeout=max(0, deadline - time.monotonic()),
                                             return_when=asyncio.FIRST_COMPLETED)
                if task in done:
                    return task.result()
                queued = _query is not None and _query.request_token is None
                if queued or not state["started"]:
                    if _query is not None:
                        _query.expire("queue_deadline")
                    if self.formal_embedding is not None:
                        with self._formal_facts:
                            cancel_event.set()
                            owner = self._formal_owner
                            if owner is not None and owner[1] is cancel_event:
                                self._formal_unknown, self._formal_encode = True, None
                    else:
                        cancel_event.set()
                    raise TimeoutError("vector queue timed out")
                # Preserve the existing initial exit-observation allowance,
                # never wait indefinitely for a synchronous pipe operation.
                until = min(deadline + .25, self._termination_deadline or deadline + .25)
                if self._termination_error is None and not task.done():
                    await asyncio.wait((task,), timeout=max(0, until - time.monotonic()))
                if task.done():
                    return task.result()
                if _query is not None:
                    _query.expire("watchdog_deadline")
                if self.formal_embedding is not None:
                    with self._formal_facts:
                        cancel_event.set()
                        owner = self._formal_owner
                        if owner is not None and owner[1] is cancel_event:
                            self._formal_unknown, self._formal_encode = True, None
                else:
                    cancel_event.set()
                raise TimeoutError("vector operation timed out") from self._termination_error
            except asyncio.CancelledError:
                if self.formal_embedding is not None:
                    with self._formal_facts:
                        cancel_event.set()
                        owner = self._formal_owner
                        if owner is not None and owner[1] is cancel_event:
                            self._formal_unknown, self._formal_encode = True, None
                else:
                    cancel_event.set()
                if keep_owner or not state["started"] or _query is not None and _query.request_token is None:
                    # Queued reads withdraw; an in-flight read drains its tagged
                    # reply under the IPC lock. It cannot retire a healthy writer.
                    raise
                cancel_until = min(deadline + 3, time.monotonic() + 3)
                while not task.done() and time.monotonic() < cancel_until:
                    try:
                        await asyncio.wait((task,), timeout=max(0, cancel_until - time.monotonic()))
                    except asyncio.CancelledError:
                        continue
                raise
        finally:
            if deadline_signal is not None:
                deadline_signal.cancel()
            if task is None:
                release()

    def reader(self) -> DailyVectorReader:
        return DailyVectorReader(self)

    @property
    def recovery_active(self) -> bool:
        return self._recovery_task is not None and not self._recovery_task.done()

    @property
    def recovery_attempts(self) -> int:
        return max(self._recovery_attempts.values(), default=0)

    def recovery_exhausted(self, scope: PipelineScope) -> bool:
        return scope in self._recovery_exhausted

    def request_recovery(self, scope: PipelineScope) -> None:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        if (self._closed or scope not in self._prepared_scopes
                or scope in self._recovery_exhausted):
            return
        self._recovery_pending.add(scope)
        self._recovery_due.setdefault(scope, asyncio.get_running_loop().time() + 1)
        self._recovery_wake.set()
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._recover_pending())
            self._recovery_task.add_done_callback(self._recovery_finished)

    def _recovery_finished(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            for scope in self._recovery_pending:
                self._recovery_exhausted.add(scope)
            self._recovery_pending.clear()
            return
        if self._closed or task is not self._recovery_task or not self._recovery_pending:
            return
        self._recovery_task = asyncio.create_task(self._recover_pending())
        self._recovery_task.add_done_callback(self._recovery_finished)

    async def _recover_pending(self) -> None:
        loop = asyncio.get_running_loop()
        while self._recovery_pending and not self._closed:
            next_due = min(self._recovery_due[scope] for scope in self._recovery_pending)
            if next_due > loop.time():
                self._recovery_wake.clear()
                try:
                    await asyncio.wait_for(self._recovery_wake.wait(),
                                           timeout=next_due - loop.time())
                except asyncio.TimeoutError:
                    pass
                continue
            for scope in tuple(self._recovery_pending):
                if self._closed:
                    return
                if self._recovery_due[scope] > loop.time():
                    continue
                attempts = self._recovery_attempts.get(scope, 0) + 1
                self._recovery_attempts[scope] = attempts
                try:
                    process = self._process
                    if process is not None and not process.is_alive():
                        await self.retire_worker()
                    await self.warm_existing(scope)
                except (RuntimeError, TimeoutError, OSError, ValueError):
                    if attempts >= 3:
                        self._recovery_pending.discard(scope)
                        self._recovery_due.pop(scope, None)
                        self._recovery_exhausted.add(scope)
                    else:
                        self._recovery_due[scope] = loop.time() + (1, 2, 4)[attempts]
                else:
                    self._recovery_pending.discard(scope)
                    self._recovery_due.pop(scope, None)
                    self._recovery_attempts.pop(scope, None)

    def retry_recovery(self, scope: PipelineScope) -> None:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        self._recovery_exhausted.discard(scope)
        self._recovery_attempts.pop(scope, None)
        self.request_recovery(scope)

    async def put(self, scope: PipelineScope, records: list[dict]) -> None:
        projected = [{key: record[key] for key in
                      ("memory_id", "payload", "payload_hash")}
                     for record in records]
        receipts = await self._invoke("put_snapshot" if self.enable_snapshot else "put",
                                      scope, records=projected)
        expected = [{"memory_id": record["memory_id"],
                     "payload_hash": record["payload_hash"],
                     "text_hash": hashlib.sha256(
                         record["payload"]["text"].encode("utf-8")).hexdigest()}
                    for record in projected]
        if (type(receipts) is not list or len(receipts) != len(expected)
                or any(type(receipt) is not dict
                       or set(receipt) != ({*required, "embedding_dim", "snapshot"}
                                          if self.enable_snapshot else {*required, "embedding_dim"})
                       or any(receipt[key] != required[key] for key in required)
                       or type(receipt["embedding_dim"]) is not int
                       or not 1 <= receipt["embedding_dim"] <= 65536
                       for receipt, required in zip(receipts, expected))):
            raise RuntimeError("vector write receipt mismatch")
        if self.enable_snapshot:
            self._capture_snapshot(scope, receipts)
        with self._pending_guard:
            if not self._closed:
                self._prepared_scopes.add(scope)

    async def contains(self, scope: PipelineScope, ids: tuple[str, ...]) -> bool:
        return await self._invoke("contains", scope, ids=ids)

    async def receipts(self, scope: PipelineScope, records: list[dict]) -> tuple[dict, ...]:
        projected = [{key: record[key] for key in
                      ("memory_id", "payload", "payload_hash")}
                     for record in records]
        rows = await self._invoke("receipts_snapshot" if self.enable_snapshot else "receipts",
                                 scope, records=projected)
        if type(rows) is not list or len(rows) != len(projected):
            raise RuntimeError("vector receipt unavailable")
        if self.enable_snapshot:
            expected = [{"memory_id": record["memory_id"], "payload_hash": record["payload_hash"],
                         "text_hash": hashlib.sha256(record["payload"]["text"].encode()).hexdigest()}
                        for record in projected]
            if any(type(row) is not dict or set(row) != {*wanted, "embedding_dim", "snapshot"}
                   or any(row[key] != wanted[key] for key in wanted)
                   or type(row["embedding_dim"]) is not int or not 1 <= row["embedding_dim"] <= 65536
                   for row, wanted in zip(rows, expected)):
                raise RuntimeError("vector receipt mismatch")
            self._capture_snapshot(scope, rows)
            return tuple({key: value for key, value in row.items() if key != "snapshot"} for row in rows)
        return tuple(rows)

    def _capture_snapshot(self, scope: PipelineScope, rows: list[dict]) -> None:
        with self._pending_guard:
            captured = self._snapshot_rows.setdefault(scope, {})
            for row in rows:
                snapshot = row["snapshot"]
                if (type(snapshot) is dict and snapshot == {
                        "schema": 1, "unavailable": "vector_capacity_exceeded"}
                        and type(snapshot["schema"]) is int):
                    self._snapshot_failed.add(scope)
                    continue
                if (type(snapshot) is not dict or set(snapshot) != {"schema", "f32", "sha256"}
                        or type(snapshot["schema"]) is not int or snapshot["schema"] != 1
                        or type(snapshot["f32"]) is not str or len(snapshot["f32"]) > 10924
                        or not 1 <= row["embedding_dim"] <= 2048):
                    raise RuntimeError("invalid numerical receipt")
                body = base64.b64decode(snapshot["f32"], validate=True)
                if (len(body) != row["embedding_dim"] * 4
                        or hashlib.sha256(body).hexdigest() != snapshot["sha256"]
                        or not all(math.isfinite(value) for value in struct.unpack(
                            "<" + "f" * row["embedding_dim"], body))):
                    raise RuntimeError("invalid numerical receipt")
                if row["memory_id"] not in captured and len(captured) >= 5000:
                    self._snapshot_failed.add(scope)
                    continue
                captured[row["memory_id"]] = row

    def snapshot_vectors(self, scope: PipelineScope) -> dict[str, dict]:
        """Already captured numbers only; never enter the writer's IPC queue."""
        with self._pending_guard:
            if not self.enable_snapshot:
                raise RuntimeError("snapshot capture disabled")
            if scope in self._snapshot_failed:
                from .daily_vector_snapshot import SnapshotCapacityError
                raise SnapshotCapacityError("vector_capacity_exceeded")
            return {key: {**row, "snapshot": dict(row["snapshot"])}
                    for key, row in self._snapshot_rows.get(scope, {}).items()}

    async def present_ids(self, scope: PipelineScope, ids: tuple[str, ...]) -> tuple[str, ...]:
        rows = await self._invoke("present_ids", scope, ids=ids)
        return tuple(rows)

    async def warm_existing(self, scope: PipelineScope, *, snapshot_generation: str | None = None) -> None:
        if self.process_role == "snapshot":
            result = await self._invoke("warm_snapshot", scope, generation=snapshot_generation)
        else:
            if snapshot_generation is not None:
                raise ValueError("writer does not accept a snapshot generation")
            result = await self._invoke("warm_existing", scope)
        if result is not True:
            raise RuntimeError("vector warm receipt unavailable")
        with self._pending_guard:
            if self._closed:
                raise RuntimeError("vector worker closed")
            self._prepared_scopes.add(scope)

    def _cleanup_budget(self, *, deadline_cap: float | None = None) -> tuple[object | None, float]:
        with self._pending_guard:
            retry_failure = self._cleanup_failure
            if (self._close_deadline is None or retry_failure is not None
                    and self._close_budget_failure is retry_failure
                    and all(task.done() for task in self._closing_tasks)):
                self._close_deadline = time.monotonic() + 3
                if deadline_cap is not None:
                    self._close_deadline = min(self._close_deadline, deadline_cap)
                self._close_budget_failure = None
                self._cleanup_diagnostic_begin(self._close_deadline)
            return retry_failure, self._close_deadline

    def _cleanup_deadline_error(self) -> RuntimeError:
        process = self._process
        return RuntimeError("vector worker did not exit" if process is not None and process.is_alive()
                            else "vector cleanup deadline exceeded")

    async def _wait_cleanup(self, deadline: float) -> None:
        # A waiter never cancels the physical thread or renews its deadline.
        while pending := tuple(task for task in self._closing_tasks if not task.done()):
            _, unfinished = await asyncio.wait(pending, timeout=max(0, deadline - time.monotonic()))
            if unfinished:
                raise self._cleanup_deadline_error() from None

    async def _finish_cleanup(self, action: Callable[[], None], deadline: float,
                              *, _cleanup_observation: object | None = None) -> None:
        # Recheck at physical publication so retirement cannot interleave a
        # second cleanup between a public handoff and this action.
        await self._wait_cleanup(deadline)

        def dispatched() -> None:
            self._cleanup_diagnostic_note("dispatched", attempt=_cleanup_observation)
            try:
                if time.monotonic() >= deadline:
                    raise self._cleanup_deadline_error() from self._termination_error
                action()
            except (RuntimeError, OSError):
                self._record_cleanup_failure(deadline=deadline)
                raise

        self._cleanup_diagnostic_note("dispatch", attempt=_cleanup_observation)
        closing = asyncio.create_task(asyncio.to_thread(dispatched))
        self._closing_tasks.add(closing)

        def finished(task: asyncio.Task) -> None:
            self._closing_tasks.discard(task)
            if not task.cancelled():
                task.exception()

        closing.add_done_callback(finished)
        try:
            done, _ = await asyncio.wait((closing,), timeout=max(0, deadline - time.monotonic()))
            if closing not in done:
                self._record_cleanup_failure(deadline=deadline)
                raise self._cleanup_deadline_error() from self._termination_error
            closing.result()
        except BaseException as error:
            try:
                self._cleanup_diagnostic_note("failed", attempt=_cleanup_observation, error=error)
            except BaseException:
                # Only secondary observation failure is suppressed here. The
                # already active primary, including process control, is re-raised.
                pass
            finally:
                if _cleanup_observation is self._cleanup_diagnostic_token:
                    self._cleanup_diagnostic_active = False
            raise
        try:
            self._cleanup_diagnostic_note("finished", attempt=_cleanup_observation)
        finally:
            if _cleanup_observation is self._cleanup_diagnostic_token:
                self._cleanup_diagnostic_active = False

    async def retire_worker(self) -> None:
        """Release this adapter's idle owner without closing it for a later retry."""
        with self._pending_guard:
            if self._pending:
                raise RuntimeError("vector owner has pending operations")
        retry_failure, deadline = self._cleanup_budget()
        observation = self._cleanup_diagnostic_capture(deadline)
        process = self._process
        def retire() -> None:
            with self._pending_guard:
                if self._pending:
                    raise RuntimeError("vector owner has pending operations")
            self._cleanup_diagnostic_note("ipc_wait", attempt=observation)
            if not self._lock.acquire(timeout=min(2, max(0, deadline - time.monotonic()))):
                raise RuntimeError("vector owner is busy")
            try:
                self._cleanup_diagnostic_note("ipc_acquired", attempt=observation)
                with self._pending_guard:
                    if self._pending:
                        raise RuntimeError("vector owner has pending operations")
                if self._process is not process:
                    raise RuntimeError("vector owner changed")
                self._stop(retry_failure=retry_failure, deadline=deadline, attempt_deadline=deadline,
                           _cleanup_observation=observation)
            finally:
                self._lock.release()
                self._cleanup_diagnostic_note("ipc_released", attempt=observation)

        await self._finish_cleanup(retire, deadline, _cleanup_observation=observation)

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *,
                                 limit: int = 30) -> tuple[tuple[str, float], ...]:
        rows = await self._invoke("search_cosine_hits", scope, query=query,
                                  limit=limit)
        return tuple((key, score) for key, score in rows)

    async def close(self) -> None:
        # Publish before yielding; concurrent callers share one response budget.
        with self._pending_guard:
            self._closed = True
        task = self._close_task
        if task is None or task.done():
            deadline = time.monotonic() + 3
            task = asyncio.create_task(self._close_response(deadline))
            self._close_task = task
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        await asyncio.shield(task)

    async def _close_response(self, deadline: float) -> None:
        await self._wait_cleanup(deadline)
        if time.monotonic() >= deadline:
            raise self._cleanup_deadline_error() from None
        with self._pending_guard:
            settled = self._pending == 0 and (
                self._recovery_task is None or self._recovery_task.done())
        if settled and self._process is None and self._connection is None:
            return
        await self._close_physical(deadline)

    async def _close_physical(self, response_deadline: float) -> None:
        retry_failure, deadline = self._cleanup_budget(deadline_cap=response_deadline)
        observation = self._cleanup_diagnostic_capture(deadline)
        attempt_deadline = deadline
        if self._recovery_task is not None and not self._recovery_task.done():
            self._recovery_task.cancel()
            await asyncio.gather(self._recovery_task, return_exceptions=True)
        def stop():
            nonlocal deadline
            self._cleanup_diagnostic_note("lifecycle_wait", attempt=observation)
            if not self._lifecycle.acquire(timeout=max(0, deadline - time.monotonic())):
                self._record_cleanup_failure(deadline=attempt_deadline)
                raise RuntimeError("vector lifecycle owner is busy")
            try:
                self._cleanup_diagnostic_note("lifecycle_acquired", attempt=observation)
                if time.monotonic() >= deadline:
                    self._record_cleanup_failure(deadline=attempt_deadline)
                    raise self._cleanup_deadline_error() from self._termination_error
                self._retry_cleanup_locked(retry_failure)
                if self._active_request is not None and self._process is not None:
                    self._request_termination_locked(self._process, _cleanup_observation=observation)
                if self._termination_deadline is not None:
                    deadline = min(deadline, self._termination_deadline)
                    self._termination_deadline = deadline
            finally:
                self._lifecycle.release()
                self._cleanup_diagnostic_note("lifecycle_released", attempt=observation)
            self._cleanup_diagnostic_note("ipc_wait", attempt=observation)
            if not self._lock.acquire(timeout=max(0, deadline - time.monotonic())):
                self._record_cleanup_failure(deadline=attempt_deadline)
                message = ("vector worker did not exit" if self._process is not None and self._process.is_alive()
                           else "vector owner has pending operations")
                raise RuntimeError(message) from self._termination_error
            try:
                self._cleanup_diagnostic_note("ipc_acquired", attempt=observation)
                self._stop(retry_failure=retry_failure, deadline=deadline, attempt_deadline=attempt_deadline,
                           _cleanup_observation=observation)
            finally:
                self._lock.release()
                self._cleanup_diagnostic_note("ipc_released", attempt=observation)
        await self._finish_cleanup(stop, deadline, _cleanup_observation=observation)


class DailyVectorReader:
    """Borrowed query capability; retirement belongs exclusively to the owner."""

    def __init__(self, owner: SupervisedDailyVector):
        self._owner = owner

    @property
    def worker_pid(self) -> int | None:
        return self._owner.worker_pid

    def recovery_exhausted(self, scope: PipelineScope) -> bool:
        return self._owner.recovery_exhausted(scope)

    async def warm_existing(self, scope: PipelineScope) -> None:
        if await self._owner._invoke("warm_existing", scope, keep_owner=True,
                                     allow_start=False) is not True:
            raise RuntimeError("vector warm receipt unavailable")

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *,
                                 limit: int = 30) -> tuple[tuple[str, float], ...]:
        process = self._owner._process
        if process is None or not process.is_alive():
            self._owner.request_recovery(scope)
            raise RuntimeError("shared vector owner not prepared")
        try:
            rows = await self._owner._invoke("search_cosine_hits", scope, query=query,
                                             limit=limit, keep_owner=True,
                                             allow_start=False)
        except (RuntimeError, TimeoutError, OSError):
            process = self._owner._process
            if process is None or not process.is_alive():
                self._owner.request_recovery(scope)
            raise
        return tuple((key, score) for key, score in rows)
