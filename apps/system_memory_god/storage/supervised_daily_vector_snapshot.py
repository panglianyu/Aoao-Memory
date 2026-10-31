"""Read-only facade reusing the existing bounded spawn/IPC supervision."""

from __future__ import annotations

import asyncio
import math
import time

from kernel.memory_pipeline_models import PipelineScope, identity
from kernel.daily_memory_contracts import INTERACTIVE_RECALL_TIMEOUT, RecallPurpose
from .daily_vector_snapshot import DailyVectorSnapshot, SnapshotLease
from .scoped_stores import ScopedStores
from .supervised_daily_vector import SNAPSHOT_QUERY_TIMEOUT, SupervisedDailyVector, _SnapshotQuery


class SupervisedDailyVectorSnapshot:
    def __init__(self, cache: DailyVectorSnapshot, *, embedding: str | None = None,
                 diagnostics: bool = False, auto_recover: bool = True,
                 query_timeout: float = SNAPSHOT_QUERY_TIMEOUT, formal_embedding=None):
        if formal_embedding is not None:
            from kernel.raw_message_profile import FormalEmbeddingBinding

            if type(formal_embedding) is not FormalEmbeddingBinding:
                raise ValueError("explicit formal binding required")
            if cache.binding_sha256 != formal_embedding.binding_sha256:
                raise ValueError("formal snapshot binding differs")
        if type(cache) is not DailyVectorSnapshot or type(auto_recover) is not bool or type(diagnostics) is not bool:
            raise ValueError("explicit snapshot cache required")
        if (type(query_timeout) not in {int, float} or not math.isfinite(query_timeout)
                or not 0 < query_timeout <= SNAPSHOT_QUERY_TIMEOUT):
            raise ValueError("invalid snapshot query budget")
        selected = cache.embedding if embedding is None else embedding
        if selected != cache.embedding and not (selected == "synthetic-stall" and cache.embedding == "synthetic"):
            raise ValueError("snapshot embedding differs")
        self.cache = cache
        self.auto_recover = auto_recover
        self.diagnostics = diagnostics
        self._local_diagnostic: dict = {}
        self._transport = SupervisedDailyVector(ScopedStores(cache.root), embedding=selected,
            request_timeout=query_timeout, max_pending=8, process_role="snapshot", diagnostics=diagnostics,
            **({"formal_embedding": formal_embedding} if formal_embedding is not None else {}))
        self._ready: dict[PipelineScope, str] = {}
        self._lifecycle = asyncio.Lock()
        self._closed = False
        self._pending: set[asyncio.Task] = set()
        self._leases: dict[object, SnapshotLease] = {}
        self._known_scopes: set[PipelineScope] = set()
        self._recovery: asyncio.Task | None = None
        self.recovery_attempts = 0

    @property
    def worker_pid(self) -> int | None:
        return self._transport.worker_pid

    def available(self, scope: PipelineScope) -> bool:
        return (not self._closed and scope in self._ready and self.worker_pid is not None
                and (self._recovery is None or self._recovery.done()))

    def diagnostic(self) -> dict:
        return dict(self._local_diagnostic) or self._transport.diagnostic()

    def _diagnose(self, code: str, query: _SnapshotQuery) -> None:
        if self.diagnostics:
            query.fail(code)
            self._local_diagnostic = query.snapshot()

    def _candidate_failure(self, query: _SnapshotQuery) -> None:
        self._diagnose("protocol_error", query)

    def health(self) -> dict:
        state = ("stopped" if self._closed else
                 "recovering" if self._recovery is not None and not self._recovery.done() else
                 "ready" if self._ready and self.worker_pid is not None else "unavailable")
        return {"state": state, "recovery_attempts": self.recovery_attempts,
                "pending": len(self._pending)}

    async def warm_existing(self, scope: PipelineScope) -> None:
        async with self._lifecycle:
            if self._closed:
                raise RuntimeError("snapshot reader closed")
            lease = self.cache.acquire(scope)
            try:
                await self._transport.warm_existing(scope, snapshot_generation=lease.generation)
                previous = self._leases.pop(scope, None)
                if previous is not None:
                    self.cache.release(previous)
                self._leases[scope] = lease
                self._known_scopes.add(scope)
                self._ready[scope] = lease.generation
            except BaseException:
                # The invoke boundary settles IPC/retirement before unwinding.
                # Keep a lease if a process could not be reaped; never GC under it.
                if self.worker_pid is None:
                    self.cache.release(lease)
                else:
                    self._leases[(scope, lease.token)] = lease
                raise

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *, limit: int = 30,
                                deadline: float | None = None,
                                purpose: RecallPurpose = "warm") -> tuple[tuple[str, float], ...]:
        if purpose not in {"warm", "interactive"}:
            raise ValueError("invalid snapshot recall purpose")
        if deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
            raise ValueError("invalid snapshot caller deadline")
        budget = self._transport.request_timeout if purpose == "warm" else INTERACTIVE_RECALL_TIMEOUT
        began = time.monotonic()
        absolute_deadline = min(began + budget, deadline) if deadline is not None else began + budget
        if absolute_deadline <= began:
            raise TimeoutError("snapshot caller deadline exceeded")
        # Keep the original deadline even if admission is preempted afterwards.
        query_state = _SnapshotQuery(self._transport._diagnostic_guard,
                                     budget_seconds=budget, purpose=purpose, deadline=absolute_deadline)
        try:
            query_state.check("queue_deadline")
        except TimeoutError:
            self._diagnose("parent_deadline", query_state)
            raise TimeoutError("snapshot caller deadline exceeded") from None
        if not self.available(scope) or self._lifecycle.locked():
            self._diagnose("not_prepared", query_state)
            raise RuntimeError("snapshot reader not prepared")
        if len(self._pending) >= 8:
            self._diagnose("queue_rejected", query_state)
            raise RuntimeError("snapshot query queue full")
        pending = asyncio.create_task(self._transport._invoke(
            "search_cosine_hits", scope, allow_start=False, _query=query_state, query=query, limit=limit))
        self._pending.add(pending)
        def finished(task):
            self._pending.discard(task)
            if not task.cancelled():
                task.exception()
        pending.add_done_callback(finished)
        try:
            rows = await asyncio.shield(pending)
        except asyncio.CancelledError:
            self._diagnose("parent_cancelled", query_state)
            pending.cancel()
            self._ready.clear()
            self._schedule_recovery()
            raise
        except (asyncio.TimeoutError, TimeoutError):
            self._diagnose("parent_deadline", query_state)
            pending.cancel()
            self._ready.clear()
            self._schedule_recovery()
            raise TimeoutError("snapshot query deadline exceeded") from None
        except (RuntimeError, OSError, ValueError):
            query_state.fail("unknown_query_failure")
            if self.diagnostics:
                self._local_diagnostic = query_state.snapshot()
            self._ready.clear()
            self._schedule_recovery()
            raise
        try:
            if type(rows) is not list or len(rows) > limit:
                raise RuntimeError("invalid snapshot result")
            hits = []
            for row in rows:
                if type(row) is not list or len(row) != 2:
                    raise RuntimeError("invalid snapshot hit")
                identity(row[0])
                if type(row[1]) not in {int, float} or not math.isfinite(row[1]) or not -1 <= row[1] <= 1:
                    raise RuntimeError("invalid snapshot score")
                hits.append((row[0], float(row[1])))
            if len({key for key, _ in hits}) != len(hits):
                raise RuntimeError("duplicate snapshot hit")
            query_state.check("facade_postcheck")
        except TimeoutError:
            self._diagnose("parent_deadline", query_state)
            self._ready.clear()
            self._schedule_recovery()
            raise TimeoutError("snapshot query deadline exceeded") from None
        except (RuntimeError, ValueError, TypeError):
            self._candidate_failure(query_state)
            raise
        if self.diagnostics:
            self._local_diagnostic = query_state.snapshot()
        self.recovery_attempts = 0
        return tuple(hits)

    def _schedule_recovery(self) -> None:
        if (not self.auto_recover or self._closed or self.recovery_attempts >= 3
                or self._recovery is not None and not self._recovery.done()):
            return
        async def recover():
            while not self._closed and self.recovery_attempts < 3:
                self.recovery_attempts += 1
                await asyncio.sleep((1, 2, 4)[self.recovery_attempts - 1])
                try:
                    if self._pending:
                        await asyncio.gather(*tuple(self._pending), return_exceptions=True)
                    await self.retire_worker()
                    for scope in tuple(self._known_scopes):
                        await self.warm_existing(scope)
                    return
                except (RuntimeError, OSError, ValueError, TimeoutError):
                    continue
        self._recovery = asyncio.create_task(recover())
        self._recovery.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    async def retire_worker(self) -> None:
        await self._transport.retire_worker()
        self._ready.clear()
        self._release_leases()

    def _release_leases(self) -> None:
        if self.worker_pid is not None:
            raise RuntimeError("snapshot process retirement incomplete")
        for key, lease in tuple(self._leases.items()):
            self.cache.release(lease)
            del self._leases[key]

    async def close(self) -> None:
        self._closed = True
        if self._recovery is not None and not self._recovery.done():
            self._recovery.cancel()
            await asyncio.gather(self._recovery, return_exceptions=True)
        await self._transport.close()
        if self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
        self._release_leases()
        self._ready.clear()
