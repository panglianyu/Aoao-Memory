"""Explicitly assembled background executor; Raw remains the lease authority."""

import asyncio
import uuid
from collections.abc import Mapping
from types import MappingProxyType
from typing import Callable, Protocol, TypeVar

from kernel.challenge_diagnostics import DiagnosticError, emit_failure
from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import ConflictError, Scope
from kernel.raw_message_store import RawMessageStore

_T = TypeVar("_T")


class DerivationBus(Protocol):
    async def derive_raw_message(self, scope, task, message, guard, complete): ...


class RawMessageWorker:
    def __init__(self, store: RawMessageStore, bus: DerivationBus, scopes: tuple[Scope, ...], *,
                 lease_seconds: int = 60, max_attempts: int = 3, concurrency: int = 2,
                 task_timeout: int = 120, max_input_chars: int = 32000,
                 blocked_scopes: frozenset[Scope] = frozenset(),
                 on_settled: Callable[[PipelineScope, str], None] | None = None,
                 selected_claims: Mapping[str, int] | None = None,
                 selected_sources: Mapping[str, Mapping[str, object]] | None = None,
                 on_failure: Callable[[], None] | None = None):
        for value, lower, upper in ((lease_seconds, 3, 3600), (max_attempts, 1, 10),
                                    (concurrency, 1, 16), (task_timeout, 1, 3600),
                                    (max_input_chars, 1, 128000)):
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError("invalid worker limits")
        if type(scopes) is not tuple or not scopes or len(scopes) > 100:
            raise ValueError("explicit worker scopes required")
        for scope in scopes:
            if type(scope) is not Scope or not scope.connector_id or not scope.conversation_id:
                raise ValueError("complete worker scope required")
            PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        if type(blocked_scopes) is not frozenset or not blocked_scopes.issubset(scopes):
            raise ValueError("invalid blocked worker scopes")
        if on_settled is not None and not callable(on_settled):
            raise ValueError("invalid settlement notification")
        if on_failure is not None and not callable(on_failure):
            raise ValueError("invalid failure notification")
        self.on_failure = on_failure
        if selected_claims is not None:
            if not selected_claims or len(selected_claims) > 18:
                raise ValueError("invalid selected recovery claims")
            for task_id, attempt in selected_claims.items():
                if (type(task_id) is not str or not task_id or len(task_id) > 256
                        or type(attempt) is not int or not 1 <= attempt < 2**63 - 1):
                    raise ValueError("invalid selected recovery claim")
        self.selected_claims = (MappingProxyType(dict(selected_claims))
                                if selected_claims is not None else None)
        if selected_sources is not None and (selected_claims is None or set(selected_sources) != set(selected_claims)):
            raise ValueError("invalid selected recovery sources")
        self.selected_sources = (MappingProxyType({key: MappingProxyType(dict(row))
                                 for key, row in selected_sources.items()})
                                 if selected_sources is not None else None)
        self.store, self.bus, self.scopes = store, bus, scopes
        self.blocked_scopes = blocked_scopes
        self.lease_seconds, self.max_attempts, self.concurrency = lease_seconds, max_attempts, concurrency
        self.task_timeout, self.max_input_chars = task_timeout, max_input_chars
        self.worker_id = "worker_" + uuid.uuid4().hex
        self._stop = asyncio.Event()
        self._run_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._active_runs: set[asyncio.Task] = set()
        self._draining: set[asyncio.Task] = set()
        self._raw_writes: set[asyncio.Task] = set()
        self._drain_failed = False
        self.last_error = ""
        self._next_scope = 0
        self.on_settled = on_settled
        self.notification_error = ""

    async def allow_recovered_scope(self, scope: Scope) -> None:
        """Unblock only after runtime recovery, serialized with lease admission."""
        if type(scope) is not Scope or scope not in self.scopes:
            raise ValueError("unknown worker scope")
        async with self._run_lock:
            if self._stop.is_set():
                raise RuntimeError("worker stopped")
            self.blocked_scopes = self.blocked_scopes - {scope}

    async def _raw_io(self, operation: Callable[..., _T], *args, **kwargs) -> _T:
        pending = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
        self._raw_writes.add(pending)

        def finished(done: asyncio.Task) -> None:
            try:
                error = done.exception()
                if error is not None and not isinstance(error, ConflictError):
                    self._drain_failed = True
            except asyncio.CancelledError:
                self._drain_failed = True
            finally:
                # Capture the result before discarding its only owner. A cancelled
                # waiter does not cancel the SQL thread protected by shield.
                self._raw_writes.discard(done)

        pending.add_done_callback(finished)
        return await asyncio.shield(pending)

    def _track_drain(self, task: asyncio.Task) -> None:
        self._draining.add(task)

        def finished(done: asyncio.Task) -> None:
            self._draining.discard(done)
            try:
                if done.exception() is not None:
                    self._drain_failed = True
            except asyncio.CancelledError:
                pass

        task.add_done_callback(finished)

    async def run_once(self) -> int:
        owner = asyncio.current_task()
        if owner is not None:
            self._active_runs.add(owner)
        try:
            async with self._run_lock:
                running: list[asyncio.Task] = []
                start = self._next_scope
                try:
                    for offset in range(len(self.scopes)):
                        if self._stop.is_set() or len(running) >= self.concurrency:
                            break
                        index = (start + offset) % len(self.scopes)
                        scope = self.scopes[index]
                        self._next_scope = (index + 1) % len(self.scopes)
                        if scope in self.blocked_scopes:
                            continue
                        task = await self._raw_io(
                            self.store.claim_partition, scope, self.worker_id,
                            lease_seconds=self.lease_seconds, max_active=self.concurrency,
                            **({"selected_attempts": self.selected_claims}
                               if self.selected_claims is not None else {}),
                            **({"selected_sources": self.selected_sources}
                               if self.selected_sources is not None else {}))
                        if task is not None:
                            running.append(asyncio.create_task(self._execute(scope, task)))
                    return sum(await asyncio.gather(*running))
                finally:
                    for pending in running:
                        if not pending.done():
                            pending.cancel()
                    await asyncio.gather(*running, return_exceptions=True)
        finally:
            if owner is not None:
                self._active_runs.discard(owner)

    async def _execute(self, scope: Scope, task: dict) -> int:
        if scope.connector_id is None or scope.conversation_id is None:
            raise ValueError("complete worker scope required")
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        args = (scope, task["task_id"], task["lease_token"])

        async def guard():
            await self._raw_io(self.store.renew, *args, lease_seconds=self.lease_seconds)

        async def execute():
            await guard()
            message = await asyncio.to_thread(self.store.get, scope, task["message_id"])
            if message is None or message.revision != task["revision"]:
                raise ConflictError("source unavailable")
            if len(message.content) > self.max_input_chars:
                raise DiagnosticError("source_budget", "derivation input budget exceeded")

            async def complete(results):
                await self._raw_io(self.store.complete, *args, results)
                return await self._raw_io(self.store.partition_state,
                                          scope)

            await self.bus.derive_raw_message(partition, task, message, guard, complete)

        async def renew_loop():
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                await guard()

        job = renewal = None
        attempt_limit = (self.selected_claims[task["task_id"]] + 1
                         if self.selected_claims is not None else self.max_attempts)
        try:
            if (task["attempts"] > attempt_limit
                    or self.selected_claims is not None and task["attempts"] != attempt_limit):
                await self._raw_io(self.store.fail, *args, "processor_error")
                return 0
            job = asyncio.create_task(execute())
            renewal = asyncio.create_task(renew_loop())
            done, _ = await asyncio.wait({job, renewal}, timeout=self.task_timeout,
                                         return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise TimeoutError()
            if renewal in done:
                await renewal
            await job
            self.last_error = ""
            if self.on_settled is not None:
                try:
                    self.on_settled(partition, task["task_id"])
                    self.notification_error = ""
                except Exception as error:
                    # A cache notification cannot undo committed Raw or Outbox.
                    emit_failure("published", error, domain=task.get("target_domain"), attempt=task.get("attempts"))
                    self.notification_error = "notification_failed"
            return 1
        except asyncio.CancelledError:
            # Keep the lease for expiry recovery; cancelled to_thread IO can still finish.
            raise
        except Exception as error:
            emit_failure("prepare", error, domain=task.get("target_domain"), attempt=task.get("attempts"))
            if self.on_failure is not None:
                try:
                    self.on_failure()
                except Exception:
                    self.notification_error = "failure_notification_failed"
            self.last_error = ("timeout" if isinstance(error, TimeoutError) else
                               "invalid_output" if isinstance(error, ValueError) else "processor_error")
            if not isinstance(error, ConflictError):
                try:
                    await self._raw_io(self.store.fail, *args, self.last_error,
                        retry_after=min(300, 2 ** task["attempts"])
                        if task["attempts"] < attempt_limit else None)
                except ConflictError:
                    pass
            return 0
        finally:
            for pending in (job, renewal):
                if pending is not None and not pending.done():
                    pending.cancel()
            if job is not None and not job.done():
                # The SQLite gate stays held by this task until its cancelled IO settles.
                self._track_drain(job)
            await asyncio.gather(*(p for p in (job, renewal)
                                   if p is not None and p not in self._draining),
                                 return_exceptions=True)

    def start(self) -> None:
        if self._task is not None or self._stop.is_set():
            raise ValueError("worker already started or stopped")
        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        active: dict[asyncio.Task, Scope] = {}
        stopping = asyncio.create_task(self._stop.wait())
        try:
            while not self._stop.is_set():
                for done in tuple(active):
                    if done.done():
                        del active[done]
                        try:
                            done.result()
                        except Exception:
                            self.last_error = "unavailable"
                async with self._run_lock:
                    start = self._next_scope
                    for offset in range(len(self.scopes)):
                        if self._stop.is_set() or len(active) >= self.concurrency:
                            break
                        index = (start + offset) % len(self.scopes)
                        self._next_scope = (index + 1) % len(self.scopes)
                        scope = self.scopes[index]
                        if scope in self.blocked_scopes:
                            continue
                        if scope in active.values():
                            continue
                        try:
                            task = await self._raw_io(
                                self.store.claim_partition, scope, self.worker_id,
                                lease_seconds=self.lease_seconds, max_active=self.concurrency,
                                **({"selected_attempts": self.selected_claims}
                                   if self.selected_claims is not None else {}),
                                **({"selected_sources": self.selected_sources}
                                   if self.selected_sources is not None else {}))
                            if task is not None:
                                active[asyncio.create_task(self._execute(scope, task))] = scope
                        except Exception:
                            self.last_error = "unavailable"
                # A completed slot is refilled immediately; idle/available_at
                # scans stay bounded. A slow scope cannot hold another slot idle.
                await asyncio.wait({stopping, *active}, timeout=1,
                                   return_when=asyncio.FIRST_COMPLETED)
            if active:
                await asyncio.gather(*active)
        finally:
            stopping.cancel()
            for pending in active:
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(stopping, *active, return_exceptions=True)

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=self.task_timeout + 5)
            except (TimeoutError, asyncio.TimeoutError):
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
        if self._active_runs:
            _, pending = await asyncio.wait(tuple(self._active_runs), timeout=self.task_timeout + 5)
            if pending:
                self.last_error = "active"
                raise TimeoutError("raw worker still active")
        if self._draining:
            _, pending = await asyncio.wait(tuple(self._draining), timeout=self.task_timeout + 5)
            if pending:
                self.last_error = "draining"
                raise TimeoutError("raw worker still draining")
        if self._raw_writes:
            _, pending = await asyncio.wait(tuple(self._raw_writes), timeout=self.task_timeout + 5)
            if pending:
                self.last_error = "raw_io"
                raise TimeoutError("raw worker still writing")
        if self._drain_failed:
            self.last_error = "draining_failed"
            raise RuntimeError("raw worker drain failed")

    @property
    def settlement_status(self) -> str:
        tasks = (*self._active_runs, *self._draining, *self._raw_writes)
        if (self._task is not None and not self._task.done()
                or any(not task.done() for task in tasks)):
            return "unsettled"
        return "settled_failed" if self._drain_failed else "settled"
