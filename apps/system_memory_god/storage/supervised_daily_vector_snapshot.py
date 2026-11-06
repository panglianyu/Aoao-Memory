"""Read-only facade reusing the existing bounded spawn/IPC supervision."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import math
import time
import uuid

from kernel.memory_pipeline_models import PipelineScope, identity
from kernel.daily_memory_contracts import INTERACTIVE_RECALL_TIMEOUT, RecallPurpose
from .daily_vector_snapshot import DailyVectorSnapshot, SnapshotLease
from .scoped_stores import ScopedStores
from .supervised_daily_vector import SNAPSHOT_QUERY_TIMEOUT, SupervisedDailyVector, _SnapshotQuery


class SupervisedDailyVectorSnapshot:
    def __init__(self, cache: DailyVectorSnapshot, *, embedding: str | None = None,
                 diagnostics: bool = False, auto_recover: bool = True,
                 query_timeout: float = SNAPSHOT_QUERY_TIMEOUT, formal_embedding=None,
                 embedding_client=None, embedding_deadline_ns=None, paged_runtime=None):
        if formal_embedding is not None and embedding_client is not None:
            raise ValueError("formal and shared embedding cannot be combined")
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
        if selected != cache.embedding and not (selected in {"synthetic-stall", "synthetic-1024"} and cache.embedding == "synthetic"):
            raise ValueError("snapshot embedding differs")
        self.cache = cache
        self.paged_runtime = paged_runtime
        self.auto_recover = auto_recover
        self.diagnostics = diagnostics
        self._local_diagnostic: dict = {}
        self._transport = SupervisedDailyVector(ScopedStores(cache.root), embedding=selected,
            request_timeout=query_timeout, max_pending=8, process_role="snapshot", diagnostics=diagnostics,
            **({"formal_embedding": formal_embedding} if formal_embedding is not None else {}),
            **({"embedding_client": embedding_client, "embedding_deadline_ns": embedding_deadline_ns}
               if embedding_client is not None or embedding_deadline_ns is not None else {}))
        self._ready: dict[PipelineScope, str] = {}
        self._lifecycle = asyncio.Lock()
        self._closed = False
        self._pending: set[asyncio.Task] = set()
        self._leases: dict[object, SnapshotLease] = {}
        self._known_scopes: set[PipelineScope] = set()
        self._recovery: asyncio.Task | None = None
        self._failed_page_owners: list[tuple] = []
        self._paged_diagnostic: dict = {}
        self.recovery_attempts = 0

    @property
    def worker_pid(self) -> int | None:
        return self._transport.worker_pid

    def set_embedding_deadline(self, deadline_ns: int) -> None:
        self._transport.set_embedding_deadline(deadline_ns)

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

    async def warm_paged(self, scope: PipelineScope, generation: int) -> None:
        async with self._lifecycle:
            if self._closed:
                raise RuntimeError("snapshot reader closed")
            result = await self._transport._invoke("warm_paged", scope, page={"generation": generation})
            if result is not True:
                raise RuntimeError("paged reader preparation failed")
            with self._transport._pending_guard:
                if self._transport._closed:
                    raise RuntimeError("paged reader closed during preparation")
                self._transport._prepared_scopes.add(scope)
            self._ready[scope] = str(generation)
            self._known_scopes.add(scope)

    @asynccontextmanager
    async def _page_lifecycle(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("page lifecycle deadline exceeded")
        try:
            await asyncio.wait_for(self._lifecycle.acquire(), timeout=remaining)
        except asyncio.TimeoutError:
            raise TimeoutError("page lifecycle deadline exceeded") from None
        try:
            yield
        finally:
            self._lifecycle.release()

    async def _search_paged(self, scope, query, limit, deadline):
        from .daily_vector_pages import PageCursor, QualifiedPageOwner, check_deadline
        runtime = self.paged_runtime
        writer = runtime.vector
        reader = self._transport
        source_owner = None
        writer_owner = reader_owner = None
        reader_started = writer_started = False
        retired = False
        async with self._page_lifecycle(deadline):
            if not self.available(scope) or self._failed_page_owners:
                raise RuntimeError("paged reader not prepared")
            generation = int(self._ready[scope])
            session = {"query_id": uuid.uuid4().hex, "generation": generation,
                       "deadline_ns": int(deadline * 1e9)}
            self._paged_diagnostic = {"complete": False, "reason": "in_progress"}
            cursor = PageCursor(session)
            def checked_ack(value, expected_pid):
                expected = {**session, "sequence": cursor.sequence, "last_key": cursor.last_key,
                            "count": cursor.count, "worker_pid": expected_pid}
                if value != expected:
                    raise RuntimeError("paged worker acknowledgement differs")
            async def invoke(transport, owner, operation, page):
                check_deadline(session["deadline_ns"])
                return await transport._invoke(operation, scope, allow_start=False, _deadline_cap=deadline,
                                               _admission_owner=owner, page=page)
            try:
                # Same writer->reader order as generation deadline handoff. Once
                # admitted, page continuations drain through any later fence.
                writer_owner = await writer.acquire_session_owner(deadline)
                reader_owner = await reader.acquire_session_owner(deadline)
                reader_pid, writer_pid = reader.worker_pid, writer.worker_pid
                reader_started = True
                ready = await invoke(reader, reader_owner, "page_score_begin", {**session, "query": query, "limit": limit})
                checked_ack(ready, reader_pid)
                source_owner = QualifiedPageOwner(runtime, scope, session["deadline_ns"], generation)
                await source_owner.open()
                while True:
                    records = await source_owner.next_page()
                    if records is None:
                        break
                    if not writer_started:
                        writer_started = True
                        checked_ack(await invoke(writer, writer_owner, "page_read_begin", session), writer_pid)
                    header = {**session, "sequence": cursor.sequence, "last_key": cursor.last_key}
                    response = await invoke(writer, writer_owner, "page_read", {**header, "records": records})
                    if type(response) is not dict or set(response) != {"ack", "rows"}:
                        raise RuntimeError("invalid numeric page response")
                    rows = response["rows"]
                    if (type(rows) is not list or [row.get("memory_id") for row in rows] !=
                            [record["memory_id"] for record in records]):
                        raise RuntimeError("numeric page membership differs")
                    cursor.advance([record["memory_id"] for record in records])
                    checked_ack(response["ack"], writer_pid)
                    scored = await invoke(reader, reader_owner, "page_score", {**header, "rows": rows})
                    checked_ack(scored, reader_pid)
                eof = {**session, "sequence": cursor.sequence, "last_key": cursor.last_key, "count": cursor.count}
                if writer_started:
                    checked_ack(await invoke(writer, writer_owner, "page_read_end", eof), writer_pid)
                writer_started = False
                result = await invoke(reader, reader_owner, "page_score_end", eof)
                if type(result) is not dict or set(result) != {"ack", "hits"}:
                    raise RuntimeError("invalid paged EOF")
                checked_ack(result["ack"], reader_pid)
                reader_started = False
                reader._formal_result("search_cosine_hits", result["hits"], {"limit": limit})
                check_deadline(session["deadline_ns"])
                outcome = tuple((row[0], row[1]) for row in result["hits"])
                observed = {"complete": True, "rows": cursor.count, "pages": cursor.sequence,
                            "metadata_peak": source_owner.metadata_peak}
            finally:
                try:
                    if writer_started:
                        # A missing/late EOF cannot leave a reusable half-session.
                        await writer.retire_worker()
                    if reader_started:
                        await reader.retire_worker()
                    if writer_started or reader_started:
                        self._ready.pop(scope, None)
                    if source_owner is not None:
                        await source_owner.close()
                    if reader_owner is not None:
                        reader.release_session_owner(reader_owner)
                        reader_owner = None
                    if writer_owner is not None:
                        writer.release_session_owner(writer_owner)
                        writer_owner = None
                    retired = True
                finally:
                    if not retired:
                        self._failed_page_owners.append((source_owner, writer, writer_owner, reader_owner))
                        self._paged_diagnostic = {"complete": False, "reason": "ownerunknown"}
            check_deadline(session["deadline_ns"])
            self._paged_diagnostic = observed
            return outcome

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *, limit: int = 30,
                                deadline: float | None = None,
                                purpose: RecallPurpose = "warm") -> tuple[tuple[str, float], ...]:
        if purpose not in {"warm", "interactive"}:
            raise ValueError("invalid snapshot recall purpose")
        if self.paged_runtime is not None:
            absolute = min(time.monotonic() + (INTERACTIVE_RECALL_TIMEOUT if purpose == "interactive"
                                               else self._transport.request_timeout),
                           float("inf") if deadline is None else deadline)
            task = asyncio.create_task(self._search_paged(scope, query, limit, absolute))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        break
                if not task.cancelled():
                    task.exception()
                raise
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
        if (self._transport.embedding_client is not None
                or not self.auto_recover or self._closed or self.recovery_attempts >= 3
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
        for owner, writer, writer_token, reader_token in tuple(self._failed_page_owners):
            await writer.retire_worker()
            if owner is not None:
                await owner.close()
            if writer_token is not None:
                writer.release_session_owner(writer_token)
            if reader_token is not None:
                self._transport.release_session_owner(reader_token)
            self._failed_page_owners.remove((owner, writer, writer_token, reader_token))
        self._release_leases()
        self._ready.clear()
