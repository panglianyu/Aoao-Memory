"""Explicit warm read-process adapter with a hard per-search parent budget."""

from __future__ import annotations

import asyncio
import json
import math
import multiprocessing
from multiprocessing.connection import Connection, PipeConnection
from multiprocessing.process import BaseProcess
from pathlib import Path
import threading
import time

from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import RawMessageError, Scope
from kernel.raw_message_store import RawMessageStore
from kernel.daily_memory_contracts import (
    DailyRecallOutcome, RecallStatus, RecallPurpose, recall_deadline,
    INTERACTIVE_RECALL_TIMEOUT,
)
from kernel.daily_memory_contracts import validate_memory_query_options, validate_retrieval_hints, MEMORY_DOMAINS
from system_memory_god.storage.scoped_stores import ScopedStores

from .daily_read_worker import MAX_READ_FRAME, MAX_READ_RESULT, READ_PROTOCOL_VERSION, serve


class ReadProcessExited(RuntimeError):
    """The read child exited before finishing the current request."""


class ReadApplicationError(RuntimeError):
    """A bounded child rejection that does not corrupt the read transport."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class _ReadControl(threading.Event):
    """Atomically distinguish cancelled admission from a dispatched wire request."""

    def __init__(self):
        super().__init__()
        self.guard = threading.Lock()
        self.dispatched = False

    def dispatch(self, deadline: float) -> None:
        with self.guard:
            if self.is_set() or time.monotonic() >= deadline:
                raise ReadApplicationError("deadline_exceeded")
            self.dispatched = True

    def cancel_admission(self) -> bool:
        with self.guard:
            self.set()
            return not self.dispatched


class SupervisedDailyRetrieval:
    def __init__(self, raw: RawMessageStore, stores: ScopedStores,
                 vector_index=None, *, request_timeout: float = 0.3,
                 owns_vector_index: bool = False, shared_vector_index: bool = False,
                 raw_only: bool = False, snapshot_vector: bool = False):
        if (type(raw) is not RawMessageStore or type(stores) is not ScopedStores
                or type(request_timeout) not in {int, float}
                or not 0 < request_timeout <= 0.3
                or type(owns_vector_index) is not bool
                or type(shared_vector_index) is not bool
                or type(raw_only) is not bool
                or type(snapshot_vector) is not bool
                or snapshot_vector and (raw_only or vector_index is None or shared_vector_index)
                or owns_vector_index and shared_vector_index
                or raw_only and (vector_index is not None or owns_vector_index
                                 or shared_vector_index)):
            raise ValueError("invalid supervised retrieval configuration")
        self.raw, self.stores, self.vector_index = raw, stores, vector_index
        self.owns_vector_index = owns_vector_index
        self.shared_vector_index = shared_vector_index
        self.raw_only = raw_only
        self.snapshot_vector = snapshot_vector
        self.request_timeout = float(request_timeout)
        self._lock = threading.Lock()
        self._stop_guard = threading.Lock()
        self._process: BaseProcess | None = None
        self._connection: Connection | PipeConnection | None = None
        self._request_id = 0
        self._closed = False
        self._read_quarantined = False
        self._vector_quarantined = False
        self._preparing = 0
        self._recovery: asyncio.Task | None = None
        self._recovery_attempts = 0
        self._cleanup_tasks: set[asyncio.Task] = set()
        self._metadata_tasks: set[asyncio.Task] = set()
        self._prepared_scopes: set[Scope] = set()
        self._last_status: RecallStatus = "unavailable"
        self._last_reasons: tuple[str, ...] = ("not_prepared",)

    def health(self) -> dict:
        status = self._last_status
        dead = self._process is not None and not self._process.is_alive()
        if self._closed:
            status = "stopped"
        elif self._preparing or self._recovery is not None and not self._recovery.done():
            status = "recovering"
        reasons = self._last_reasons
        if dead and not self._closed:
            status = "unavailable"
            reasons = ("process_exit",) + reasons
        if self.raw_only:
            reasons = tuple(dict.fromkeys((*reasons, "vector_online_disabled")))
        return {"state": status, "reason_codes": reasons,
                "recovery_attempts": self._recovery_attempts, "reader_pid": self.worker_pid}

    def _outcome(self, items: list[dict], status: RecallStatus,
                 reasons: tuple[str, ...] = (), trace: dict | None = None) -> DailyRecallOutcome:
        self._last_status, self._last_reasons = status, reasons
        return DailyRecallOutcome(items, status, reasons, trace)

    @property
    def worker_pid(self) -> int | None:
        process = self._process
        return process.pid if process is not None and process.is_alive() else None

    def _stop(self) -> None:
        with self._stop_guard:
            self._stop_locked()

    def _stop_locked(self) -> None:
        connection, process = self._connection, self._process
        deadline = time.monotonic() + .05 + 2
        if connection is not None:
            connection.close()
            self._connection = None
        if process is not None:
            process.join(timeout=max(0, min(.05, deadline - time.monotonic())))
            termination_error: OSError | None = None
            if process.is_alive():
                # CPython on Windows can wait one second inside terminate().
                if deadline - time.monotonic() < 1:
                    raise RuntimeError("read worker did not exit")
                try:
                    process.terminate()
                except OSError as exc:
                    termination_error = exc
                process.join(timeout=max(0, deadline - time.monotonic()))
            if process.is_alive() or process.exitcode is None:
                if termination_error is not None:
                    raise termination_error
                raise RuntimeError("read worker did not exit")
            process.close()
            self._process = None

    def _start(self, cancel_event: threading.Event) -> None:
        with self._lock:
            with self._stop_guard:
                if cancel_event.is_set() or self._closed:
                    raise RuntimeError("read worker startup cancelled or closed")
                if self._process is not None:
                    if self._connection is None:
                        raise RuntimeError("read worker termination unresolved")
                    return
                parent, child = multiprocessing.get_context("spawn").Pipe()
                process = multiprocessing.get_context("spawn").Process(
                    target=serve,
                    args=(child, str(Path(self.raw.database)), str(self.stores.root)),
                )
                try:
                    process.start()
                except BaseException:
                    parent.close()
                    child.close()
                    raise
                child.close()
                self._connection, self._process = parent, process
                try:
                    deadline = time.monotonic() + 5
                    while True:
                        if cancel_event.is_set():
                            raise RuntimeError("read worker startup cancelled")
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("read worker startup timed out")
                        if parent.poll(min(0.05, remaining)):
                            if json.loads(parent.recv_bytes(4096)) != {
                                    "version": READ_PROTOCOL_VERSION, "ready": True}:
                                raise RuntimeError("invalid read worker startup")
                            return
                except BaseException:
                    self._stop_locked()
                    raise

    async def start(self) -> None:
        cancel_event = threading.Event()
        task = asyncio.create_task(asyncio.to_thread(self._start, cancel_event))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancel_event.set()
            try:
                await self._await_settled(task)
            except BaseException:
                pass
            await self._await_settled(asyncio.create_task(
                asyncio.to_thread(self._stop)))
            raise

    @staticmethod
    async def _await_settled(task: asyncio.Task) -> None:
        """Wait for cleanup despite further caller cancellation."""
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        task.result()

    def _call(self, request: dict, cancel_event: threading.Event,
              deadline: float) -> list[dict] | dict:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not self._lock.acquire(timeout=remaining):
            raise ReadApplicationError("deadline_exceeded")
        try:
            if self._closed or self._process is None or self._connection is None:
                raise RuntimeError("read worker not started")
            self._request_id += 1
            request["id"] = self._request_id
            try:
                encoded = json.dumps(request, ensure_ascii=False, separators=(",", ":"),
                                     allow_nan=False).encode()
            except (TypeError, ValueError):
                raise ReadApplicationError("read_rejected") from None
            if len(encoded) > MAX_READ_FRAME or request.get("op") == "publish" and len(encoded) > MAX_READ_RESULT:
                raise ReadApplicationError("request_too_large")
            connection, process = self._connection, self._process
            try:
                if isinstance(cancel_event, _ReadControl):
                    cancel_event.dispatch(deadline)
                elif cancel_event.is_set() or time.monotonic() >= deadline:
                    raise ReadApplicationError("deadline_exceeded")
                connection.send_bytes(encoded)
                while True:
                    if cancel_event.is_set():
                        raise RuntimeError("read request cancelled")
                    if connection.poll(min(0.01, max(0, deadline - time.monotonic()))):
                        if cancel_event.is_set() or time.monotonic() >= deadline:
                            raise TimeoutError("read response arrived too late")
                        frame = connection.recv_bytes(MAX_READ_FRAME)
                        try:
                            response = json.loads(frame)
                        except (ValueError, RecursionError):
                            raise RuntimeError("invalid read worker response") from None
                        if (type(response) is not dict
                                or type(response.get("version")) is not int
                                or response["version"] != READ_PROTOCOL_VERSION
                                or type(response.get("id")) is not int
                                or response["id"] != self._request_id
                                or type(response.get("ok")) is not bool):
                            raise RuntimeError("invalid read worker response")
                        if not response["ok"]:
                            errors = {
                                "ResponseTooLarge": "result_too_large",
                                "TimeoutError": "deadline_exceeded",
                                "RawMessageError": "read_rejected",
                                "ScopeError": "read_rejected",
                                "ValueError": "read_rejected",
                            }
                            error = response.get("error")
                            if (set(response) != {"version", "id", "ok", "error"}
                                    or type(error) is not str or error not in errors):
                                raise RuntimeError("invalid read worker error")
                            raise ReadApplicationError(errors[error])
                        if set(response) != {"version", "id", "ok", "result"}:
                            raise RuntimeError("invalid read worker response")
                        result = response.get("result")
                        if request.get("op") == "content_chunk":
                            if (type(result) is not dict or set(result) != {
                                    "message_id", "revision", "content_hash", "content",
                                    "start_char", "next_char", "total_chars", "total_bytes", "eof"}
                                    or result["message_id"] != request["message_id"]
                                    or type(result["revision"]) is not int
                                    or result["revision"] != request["expected_revision"]
                                    or result["content_hash"] != request["expected_content_hash"]
                                    or type(result["content"]) is not str
                                    or any(type(result[key]) is not int or result[key] < 0 for key in (
                                        "start_char", "next_char", "total_chars", "total_bytes"))
                                    or type(result["eof"]) is not bool
                                    or result["start_char"] != request["offset_chars"]
                                    or result["next_char"] != result["start_char"] + len(result["content"])
                                    or not result["start_char"] <= result["next_char"] <= result["total_chars"]
                                    or len(result["content"]) > request["limit_chars"]
                                    or result["eof"] != (result["next_char"] == result["total_chars"])):
                                raise RuntimeError("invalid content response")
                            return result
                        if request.get("op") in {"prepare", "prepare_online"}:
                            if (type(result) is not dict or set(result) != {
                                    "ready", "vector", "raw_generation"}
                                    or type(result["ready"]) is not bool
                                    or type(result["vector"]) is not bool
                                    or type(result["raw_generation"]) is not int
                                    or result["raw_generation"] < 0):
                                raise RuntimeError("invalid read preparation")
                            return result
                        if request.get("op") == "collect":
                            if type(result) is not dict or result.get("kind") != "candidates":
                                raise RuntimeError("invalid candidate response")
                            result = {key: value for key, value in result.items() if key != "kind"}
                        if (type(result) is not dict or set(result) - {"trace"} != {
                                "items", "status", "reason_codes"}
                                or type(result["status"]) is not str
                                or result["status"] not in {"ready", "degraded", "unavailable"}
                                or type(result["items"]) is not list or len(result["items"]) > 100
                                or any(type(row) is not dict for row in result["items"])
                                or type(result["reason_codes"]) is not list
                                or any(type(reason) is not str
                                       or reason not in {"raw_unavailable", "derived_unavailable",
                                                     "result_budget_limited", "scan_budget_limited",
                                                     "derivation_pending"}
                                       for reason in result["reason_codes"])):
                            raise RuntimeError("invalid read worker result")
                        if "trace" in result:
                            trace = result["trace"]
                            query_options = request.get("query_options")
                            if (not query_options or not query_options["trace"]
                                    or type(trace) is not dict or set(trace) != {"schema", "domains"}
                                    or type(trace["schema"]) is not int or trace["schema"] != 1
                                    or type(trace["domains"]) is not dict
                                    or set(trace["domains"]) != set(MEMORY_DOMAINS)):
                                raise RuntimeError("invalid read trace")
                            for value in trace["domains"].values():
                                if (type(value) is not dict or set(value) != {"attempted", "status", "reason",
                                        "candidate_count", "selected_count", "source_ids", "processor_versions",
                                        "elapsed_ms", "limited"} or type(value["attempted"]) is not bool
                                        or type(value["limited"]) is not bool
                                        or type(value["status"]) is not str
                                        or value["status"] not in {"disabled", "ready", "empty", "failed", "pending", "not_applicable"}
                                        or type(value["reason"]) is not str or len(value["reason"]) > 64
                                        or any(type(value[field]) is not int or not 0 <= value[field] <= 100
                                               for field in ("candidate_count", "selected_count"))
                                        or type(value["elapsed_ms"]) not in {int, float}
                                        or not math.isfinite(value["elapsed_ms"]) or value["elapsed_ms"] < 0
                                        or any(type(value[field]) is not list or len(value[field]) > 100
                                            or any(type(v) is not str or len(v) > 256 for v in value[field])
                                            for field in ("source_ids", "processor_versions"))):
                                    raise RuntimeError("invalid read trace")
                        return result
                    if not process.is_alive():
                        raise ReadProcessExited("read worker exited")
                    if time.monotonic() >= deadline:
                        raise TimeoutError("read request timed out")
            except (OSError, EOFError) as exc:
                exited = not process.is_alive()
                self._stop()
                if exited:
                    raise ReadProcessExited("read worker exited") from exc
                raise RuntimeError("read worker transport failed") from exc
            except ReadApplicationError:
                raise
            except (TimeoutError, RuntimeError, json.JSONDecodeError):
                self._stop()
                raise
        finally:
            self._lock.release()

    async def prepare(self, scope: Scope) -> bool:
        self._preparing += 1
        try:
            ready = await self._prepare(scope)
            if ready:
                self._prepared_scopes.add(scope)
            return ready
        finally:
            self._preparing -= 1

    async def _prepare(self, scope: Scope) -> bool:
        """Warm one complete synthetic conversation before advertising recall readiness."""
        if (type(scope) is not Scope or scope.connector_id is None
                or scope.conversation_id is None):
            raise ValueError("complete read scope required")
        deadline = time.monotonic() + 15.0
        try:
            await self.start()
        except asyncio.CancelledError:
            await asyncio.to_thread(self._stop)
            raise
        except (RuntimeError, TimeoutError, OSError):
            return False
        request = {"version": READ_PROTOCOL_VERSION, "id": 0,
                   "op": "prepare_online" if self.shared_vector_index or self.raw_only or self.snapshot_vector else "prepare",
                   "scope": [scope.agent_id, scope.user_id,
                             scope.connector_id, scope.conversation_id]}
        cancel_event = threading.Event()
        task = asyncio.create_task(asyncio.to_thread(
            self._call, request, cancel_event, deadline))
        vector_task: asyncio.Task | None = None
        recheck_event: threading.Event | None = None
        recheck_task: asyncio.Task | None = None
        ready = False
        try:
            result = await asyncio.wait_for(asyncio.shield(task),
                                            timeout=max(0.001, deadline - time.monotonic()))
            if type(result) is not dict or result["ready"] is not True:
                return False
            if result["vector"] and not self.raw_only and not self.snapshot_vector:
                warm = getattr(self.vector_index, "warm_existing", None)
                retire = getattr(self.vector_index, "retire_worker", None)
                if (not callable(warm) or not (
                        self.shared_vector_index or self.owns_vector_index and callable(retire))):
                    return False
                partition = PipelineScope(scope.user_id, scope.agent_id,
                                          scope.connector_id, scope.conversation_id)
                vector_task = asyncio.create_task(warm(partition))
                try:
                    await asyncio.wait_for(asyncio.shield(vector_task),
                                           timeout=max(0.001, deadline - time.monotonic()))
                except (asyncio.TimeoutError, TimeoutError):
                    self._retire_vector(vector_task)
                    return False
                recheck_event = threading.Event()
                recheck_task = asyncio.create_task(asyncio.to_thread(
                    self._call, request, recheck_event, deadline))
                try:
                    current = await asyncio.wait_for(
                        asyncio.shield(recheck_task),
                        timeout=max(0.001, deadline - time.monotonic()))
                except (asyncio.TimeoutError, TimeoutError):
                    recheck_event.set()
                    self._retire_read(recheck_task)
                    return False
                if (type(current) is not dict or current["ready"] is not True
                        or (not self.shared_vector_index
                            and current["raw_generation"] != result["raw_generation"])
                        or current["vector"] is not True):
                    return False
            ready = True
            self._last_status, self._last_reasons = "ready", ()
            return True
        except asyncio.CancelledError:
            cancel_event.set()
            if not task.done():
                self._retire_read(task)
            if recheck_event is not None:
                recheck_event.set()
            if recheck_task is not None and not recheck_task.done():
                self._retire_read(recheck_task)
            if vector_task is not None and not vector_task.done():
                self._retire_vector(vector_task)
            raise
        except (asyncio.TimeoutError, TimeoutError):
            cancel_event.set()
            self._retire_read(task)
            return False
        except (RuntimeError, ValueError, OSError):
            return False
        finally:
            if not ready:
                async def cleanup() -> None:
                    try:
                        if vector_task is not None:
                            if not vector_task.done():
                                vector_task.cancel()
                            try:
                                await self._await_settled(vector_task)
                            except BaseException:
                                pass
                            retire = getattr(self.vector_index, "retire_worker", None)
                            if self.owns_vector_index and callable(retire):
                                await retire()
                                if getattr(self.vector_index, "worker_pid", None) is None:
                                    self._vector_quarantined = False
                    finally:
                        await asyncio.to_thread(self._stop)

                await self._await_settled(asyncio.create_task(cleanup()))

    async def search(self, scope: Scope, query: str, *, top_k: int = 10,
                     neighbors: int = 0, start_ts: float | None = None,
                     end_ts: float | None = None,
                     memory_scope: str = "auto", deadline: float | None = None,
                     purpose: RecallPurpose = "warm") -> list[dict]:
        return (await self.search_with_status(
            scope, query, top_k=top_k, neighbors=neighbors, start_ts=start_ts,
            end_ts=end_ts, memory_scope=memory_scope, deadline=deadline, purpose=purpose)).items

    async def read_content_chunk(self, scope: Scope, message_id: str, *,
                                  expected_revision: int, expected_content_hash: str,
                                  offset_chars: int = 0, limit_chars: int = 4096,
                                  deadline: float | None = None) -> dict:
        if (type(scope) is not Scope or scope not in self._prepared_scopes
                or self._closed or self._read_quarantined
                or self._recovery is not None and not self._recovery.done()):
            raise RawMessageError("reader unavailable")
        now = time.monotonic()
        if deadline is not None and (type(deadline) not in {int, float}
                                     or not math.isfinite(deadline)):
            raise RawMessageError("invalid content deadline")
        deadline = min(deadline, now + self.request_timeout) if deadline is not None else (
            now + self.request_timeout)
        request = {"version": READ_PROTOCOL_VERSION, "id": 0, "op": "content_chunk",
                   "scope": [scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id],
                   "message_id": message_id, "expected_revision": expected_revision,
                   "expected_content_hash": expected_content_hash, "offset_chars": offset_chars,
                   "limit_chars": limit_chars, "deadline": deadline}
        cancel_event = _ReadControl()
        task = asyncio.create_task(asyncio.to_thread(self._call, request, cancel_event, deadline))
        try:
            result = await asyncio.wait_for(asyncio.shield(task),
                                            timeout=max(0.001, deadline - time.monotonic()))
            if type(result) is not dict:
                raise RuntimeError("invalid content response")
            return result
        except ReadApplicationError as exc:
            raise RawMessageError(exc.reason) from None
        except asyncio.CancelledError:
            if not self._cancel_read(task, cancel_event):
                self._schedule_recovery()
            raise
        except (RuntimeError, OSError, ValueError, TimeoutError):
            if self._cancel_read(task, cancel_event):
                raise RawMessageError("deadline_exceeded") from None
            self._schedule_recovery()
            raise RawMessageError("reader unavailable") from None

    async def search_with_status(self, scope: Scope, query: str, *, top_k: int = 10,
                                  neighbors: int = 0, start_ts: float | None = None,
                                  end_ts: float | None = None,
                                  memory_scope: str = "auto",
                                  deadline: float | None = None,
                                  purpose: RecallPurpose = "warm", query_options=None,
                                  retrieval_hints: tuple[str, ...] = ()) -> DailyRecallOutcome:
        query_options = validate_memory_query_options(query_options)
        retrieval_hints = validate_retrieval_hints(retrieval_hints)
        if (type(scope) is not Scope or scope.connector_id is None
                or scope.conversation_id is None):
            raise ValueError("complete read scope required")
        if deadline is not None and (type(deadline) not in {int, float}
                                     or not math.isfinite(deadline)):
            raise ValueError("invalid read deadline")
        deadline = recall_deadline(purpose=purpose, deadline=deadline)
        if self._closed:
            return self._outcome([], "stopped", ("stopped",))
        if self._recovery is not None and not self._recovery.done():
            return self._outcome([], "recovering", ("reader_recovering",))
        if self._recovery_attempts >= 3 and self._read_quarantined:
            return self._outcome([], "unavailable", ("reader_unavailable",))
        if self._preparing or self._read_quarantined:
            return self._outcome([], "recovering", ("reader_preparing",))
        if self._process is not None and not self._process.is_alive():
            return self._recover_outcome("process_exit")
        if self._process is None:
            return self._recover_outcome()
        now = time.monotonic()
        limit = self.request_timeout if purpose == "warm" else INTERACTIVE_RECALL_TIMEOUT
        deadline = min(deadline, now + limit)
        if deadline <= now:
            return self._outcome([], "unavailable", ("deadline_exceeded",))
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        vector_degraded = self._vector_quarantined or self.raw_only
        baseline_scope = memory_scope
        baseline_start = start_ts
        if memory_scope.strip().lower() == "recents":
            baseline_start = max(start_ts or 0, time.time() - 7 * 86400)
        request = {"version": READ_PROTOCOL_VERSION, "id": 0,
                   "scope": [scope.agent_id, scope.user_id,
                             scope.connector_id, scope.conversation_id],
                   "query": query, "top_k": top_k, "neighbors": neighbors,
                   "start_ts": baseline_start, "end_ts": end_ts,
                   "memory_scope": baseline_scope, "vector_hits": (),
                   "deadline": deadline}
        if query_options is not None:
            request["query_options"] = query_options
        if retrieval_hints:
            request["retrieval_hints"] = retrieval_hints
        if not self.raw_only:
            request["op"] = "collect"
        cancel_event = _ReadControl()
        task = asyncio.create_task(asyncio.to_thread(
            self._call, request, cancel_event, deadline))
        vector_task: asyncio.Task | None = None
        candidate_deadline = deadline - .05
        vector_timing = {}

        async def candidates() -> tuple[tuple[str, float], ...]:
            began = time.monotonic()
            try:
                available = await self._admit_vector_metadata(partition, deadline)
                if not available:
                    return ()
                if self.snapshot_vector:
                    options = {"purpose": purpose} if purpose != "warm" else {}
                    return await self.vector_index.search_cosine_hits(
                        partition, query, limit=100, deadline=candidate_deadline, **options)
                return await self.vector_index.search_cosine_hits(partition, query, limit=100)
            finally:
                vector_timing["elapsed_ms"] = (time.monotonic() - began) * 1000

        if (not vector_degraded and self.vector_index is not None
                and (query_options is None or "vector" in query_options["enabled"])
                and memory_scope.strip().lower() in {"", "auto", "recents", "relations"}):
            vector_task = asyncio.create_task(candidates())
        try:
            result = await asyncio.wait_for(asyncio.shield(task),
                                            timeout=max(0.001, deadline - time.monotonic()))
            if type(result) is not dict:
                if vector_task is not None:
                    self._retire_vector(vector_task)
                return self._recover_outcome()
            self._recovery_attempts = 0
            hits: tuple[tuple[str, float], ...] = ()
            if vector_task is not None:
                try:
                    if vector_task.done():
                        hits = vector_task.result()
                    elif self.snapshot_vector and candidate_deadline > time.monotonic():
                        try:
                            hits = await asyncio.wait_for(asyncio.shield(vector_task),
                                timeout=candidate_deadline - time.monotonic())
                        except asyncio.TimeoutError:
                            self._retire_vector(vector_task)
                            vector_degraded = True
                    else:
                        self._retire_vector(vector_task)
                        vector_degraded = True
                except (asyncio.TimeoutError, TimeoutError, RuntimeError,
                        ValueError, OSError):
                    self._retire_vector(vector_task)
                    vector_degraded = True
            if not self.raw_only:
                collected_reasons = result["reason_codes"]
                collected_trace = result.get("trace")
                if result["status"] == "unavailable":
                    return self._outcome([], "unavailable", tuple(collected_reasons))
                cancel_event = _ReadControl()
                task = asyncio.create_task(asyncio.to_thread(
                    self._call, {**request, "op": "publish", "candidates": result["items"],
                                 "vector_hits": hits}, cancel_event, deadline))
                published = await asyncio.wait_for(asyncio.shield(task),
                    timeout=max(0.001, deadline - time.monotonic()))
                if type(published) is not dict:
                    raise RuntimeError("invalid publication response")
                result = published
                if collected_trace is not None and result.get("trace") is not None:
                    for domain, value in collected_trace["domains"].items():
                        final = result["trace"]["domains"][domain]
                        # Only operational facts survive collect. Identities,
                        # counts and versions belong to the final Raw snapshot.
                        final["attempted"] = final["attempted"] or value["attempted"]
                        final["limited"] = value["limited"] or final["limited"]
                        if value["status"] == "failed" and final["status"] not in {"disabled", "failed"}:
                            final.update(status="failed", reason=value["reason"])
                        if domain != "vector":
                            final["elapsed_ms"] = value["elapsed_ms"]
                result["reason_codes"] = list(dict.fromkeys(
                    (*collected_reasons, *result["reason_codes"])))
                if result["status"] == "ready" and result["reason_codes"]:
                    result["status"] = "degraded"
            reasons = tuple(result["reason_codes"])
            status = result["status"]
            if result.get("trace") is not None and vector_timing:
                result["trace"]["domains"]["vector"]["elapsed_ms"] = round(vector_timing["elapsed_ms"], 3)
            if vector_degraded:
                reasons += (("vector_online_disabled",) if self.raw_only
                            else ("vector_unavailable",))
                if result.get("trace") is not None:
                    vector_trace = result["trace"]["domains"]["vector"]
                    if vector_trace["status"] != "disabled":
                        vector_trace.update(attempted=vector_task is not None,
                            status="disabled" if self.raw_only else "failed",
                            reason="vector_online_disabled" if self.raw_only else "vector_unavailable")
                if status == "ready" and not self.raw_only:
                    status = "degraded"
            return self._outcome(result["items"], status, reasons, result.get("trace"))
        except asyncio.CancelledError:
            admitted = self._cancel_read(task, cancel_event)
            if vector_task is not None:
                self._retire_vector(vector_task)
            if not admitted:
                self._schedule_recovery()
            raise
        except (asyncio.TimeoutError, TimeoutError):
            admitted = self._cancel_read(task, cancel_event)
            if vector_task is not None:
                self._retire_vector(vector_task)
            return (self._outcome([], "unavailable", ("deadline_exceeded",)) if admitted
                    else self._recover_outcome("timeout"))
        except ReadProcessExited:
            if vector_task is not None:
                self._retire_vector(vector_task)
            return self._recover_outcome("process_exit")
        except ReadApplicationError as exc:
            if vector_task is not None:
                self._retire_vector(vector_task)
            return self._outcome([], "unavailable", (exc.reason,))
        except (RuntimeError, ValueError, OSError, EOFError):
            if vector_task is not None:
                self._retire_vector(vector_task)
            return self._recover_outcome("protocol_error")

    def _recover_outcome(self, reason: str | None = None) -> DailyRecallOutcome:
        self._schedule_recovery()
        recovering = self._recovery is not None and not self._recovery.done()
        reasons: tuple[str, ...] = (
            "reader_recovering" if recovering else "reader_unavailable",)
        if reason is not None:
            reasons = (reason, *reasons)
        return self._outcome([], "recovering" if recovering else "unavailable",
                             reasons)

    def _vector_partition_available(self, scope: PipelineScope) -> bool:
        path = self.stores.partition(scope)
        return (path / "vector_index.db").is_file() and (path / "vectors").is_dir()

    async def _admit_vector_metadata(self, partition: PipelineScope, deadline: float) -> bool:
        """Bound off-thread metadata admission independently of vector execution."""
        if self.snapshot_vector:
            return self.vector_index.available(partition)
        if len(self._metadata_tasks) >= 8:
            raise TimeoutError("vector metadata queue full")
        metadata_task = asyncio.create_task(asyncio.to_thread(
            self._vector_partition_available, partition))
        self._metadata_tasks.add(metadata_task)
        metadata_task.add_done_callback(self._metadata_tasks.discard)
        metadata_task.add_done_callback(self._consume_background)
        return await asyncio.wait_for(
            asyncio.shield(metadata_task),
            timeout=min(0.025, max(0.001, deadline - time.monotonic())))

    def _schedule_recovery(self) -> None:
        if (self._closed or self._recovery_attempts >= 3
                or self._recovery is not None and not self._recovery.done()):
            return

        async def recover() -> None:
            while not self._closed and self._recovery_attempts < 3:
                delay = (1, 2, 4)[self._recovery_attempts]
                self._recovery_attempts += 1
                await asyncio.sleep(delay)
                try:
                    if self._cleanup_tasks:
                        await asyncio.gather(*tuple(self._cleanup_tasks))
                    await asyncio.to_thread(self._stop)
                    if self._closed:
                        return
                    await self.start()
                    for scope in tuple(self._prepared_scopes):
                        if not await self.prepare(scope):
                            raise RuntimeError("read scope preparation unavailable")
                    self._read_quarantined = False
                    if self._prepared_scopes:
                        self._last_status, self._last_reasons = "ready", ()
                    else:
                        self._last_status, self._last_reasons = "degraded", ("not_prepared",)
                    return
                except (RuntimeError, OSError, TimeoutError):
                    continue
            if not self._closed:
                self._last_status, self._last_reasons = "unavailable", ("reader_unavailable",)

        self._recovery = asyncio.create_task(recover())
        self._recovery.add_done_callback(self._consume_background)

    @staticmethod
    def _consume_background(task: asyncio.Task) -> None:
        try:
            task.result()
        except BaseException:
            pass

    def _retire_vector(self, task: asyncio.Task) -> None:
        if task.done():
            self._consume_background(task)
            return
        self._vector_quarantined = not self.shared_vector_index
        task.cancel()
        task.add_done_callback(self._consume_background)

        def settled(_: asyncio.Task) -> None:
            if getattr(self.vector_index, "worker_pid", None) is None:
                self._vector_quarantined = False

        task.add_done_callback(settled)

    def _retire_read(self, task: asyncio.Task) -> None:
        self._read_quarantined = True
        task.add_done_callback(self._consume_background)
        cleanup = asyncio.create_task(asyncio.to_thread(self._stop))
        self._cleanup_tasks.update((task, cleanup))
        task.add_done_callback(self._cleanup_tasks.discard)
        cleanup.add_done_callback(self._cleanup_tasks.discard)
        cleanup.add_done_callback(self._consume_background)

        def settled(_: asyncio.Task) -> None:
            if task.done() and cleanup.done() and self._process is None:
                self._read_quarantined = False

        task.add_done_callback(settled)
        cleanup.add_done_callback(settled)

    def _cancel_read(self, task: asyncio.Task, control: _ReadControl) -> bool:
        # Unknown injected failures cannot certify that the transport stayed idle.
        admission = control.cancel_admission() and (
            not task.done() or not task.cancelled() and isinstance(task.exception(), ReadApplicationError))
        if admission:
            self._cleanup_tasks.add(task)
            task.add_done_callback(self._cleanup_tasks.discard)
            task.add_done_callback(self._consume_background)
        else:
            self._retire_read(task)
        return admission

    def stop_accepting(self) -> None:
        self._closed = True

    async def close(self) -> None:
        self._closed = True
        if self._recovery is not None and not self._recovery.done():
            self._recovery.cancel()
            await asyncio.gather(self._recovery, return_exceptions=True)
        def stop() -> None:
            self._closed = True
            self._stop()
        try:
            await asyncio.to_thread(stop)
            if self._cleanup_tasks:
                await asyncio.gather(*tuple(self._cleanup_tasks), return_exceptions=True)
        finally:
            if self.owns_vector_index:
                close_vector = getattr(self.vector_index, "close", None)
                if callable(close_vector):
                    await close_vector()
