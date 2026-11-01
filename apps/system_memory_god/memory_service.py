"""Host-independent memory facade; Raw/Outbox remain the write authority."""

from __future__ import annotations

import asyncio
import math
import time

from kernel.daily_memory_contracts import (
    DailyRecallOutcome, MemorySaveReceipt, MEMORY_DOMAINS, validate_memory_query_options,
    validate_retrieval_hints,
)
from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import MessageDraft, Scope, ScopeError


class MemoryService:
    """One lifecycle owner for the existing runtime, not a second daemon stack.

    Scopes are capabilities bound by trusted host configuration. External callers
    must receive a host-authorized binding, not unrestricted choice of user_id.
    """

    def __init__(self, runtime, *, profile_id: str):
        self.runtime = runtime
        self.raw, self.derivation, self.retrieval = runtime.raw, runtime.derivation, runtime.retrieval
        self.profile_id = profile_id
        self.scopes = self.raw.configured_scopes()
        if self.raw.memory_service is not None:
            raise ValueError("memory service already bound")
        self.raw.memory_service = self

    def _authorize(self, scope: Scope) -> None:
        if type(scope) is not Scope or scope not in self.scopes:
            raise ScopeError("memory scope denied")

    async def add(self, scope: Scope, original: MessageDraft) -> MemorySaveReceipt:
        self._authorize(scope)
        return MemorySaveReceipt(await self.raw.append(scope, original))

    async def replay(self, scope: Scope, **options) -> list:
        self._authorize(scope)
        return await self.raw.replay(scope, scope.conversation_id, **options)

    async def search(self, scope: Scope, query: str, **options) -> DailyRecallOutcome:
        hints = validate_retrieval_hints(options.pop("retrieval_hints", ()))
        try:
            self._authorize(scope)
        except ScopeError:
            return DailyRecallOutcome([], "unavailable", ("authority_denied",))
        trace = options.pop("trace", False)
        excluded = options.pop("exclude_domains", ())
        domains = options.pop("domains", None)
        event_start = options.pop("event_start_ts", None)
        event_end = options.pop("event_end_ts", None)
        if self.profile_id == "legacy-daily-v1" and (event_start is not None or event_end is not None):
            raise ValueError("legacy memory does not support event filters")
        if (type(excluded) not in {tuple, list} or any(domain not in MEMORY_DOMAINS for domain in excluded)
                or domains is not None and type(domains) not in {tuple, list}):
            raise ValueError("invalid memory domains")
        if self.profile_id != "legacy-daily-v1" or trace or domains is not None or excluded:
            full = self.profile_id == "memory-full-v1"
            enabled = domains if domains is not None else MEMORY_DOMAINS if full else ("raw", "vector")
            query_options = validate_memory_query_options({"full": full,
                "enabled": [domain for domain in enabled if domain not in excluded], "trace": trace,
                "event_start_ts": event_start, "event_end_ts": event_end})
            # Old LongMemEval calls keep their exact ranking/query identity.
            if full or trace or domains is not None or excluded or event_start is not None or event_end is not None:
                options["query_options"] = query_options
        if hints:
            options["retrieval_hints"] = hints
        return await self.retrieval.search_with_status(scope, query, **options)

    async def status(self, scope: Scope, *, deadline: float) -> dict:
        self._authorize(scope)
        if type(deadline) not in {int, float} or not math.isfinite(deadline):
            raise ValueError("invalid publication deadline")
        if scope.connector_id is None or scope.conversation_id is None:
            raise ScopeError("complete memory scope required")
        partition = PipelineScope(scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id)
        return await self.runtime.semantic_publication_state(partition, deadline=deadline)

    async def wait_until_published(self, scope: Scope, *, deadline: float) -> dict:
        self._authorize(scope)
        while True:
            state = await self.status(scope, deadline=deadline)
            if state["status"] in {"current", "disabled", "stopped"} or state["terminal"]:
                return state
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {**state, "status": "unavailable", "reason": "publication_deadline"}
            await asyncio.sleep(min(.05, remaining))

    async def edit(self, scope: Scope, message_id: str, content: str, *,
                   expected_revision: int, edited_at: str):
        self._authorize(scope)
        return await self._mutation(self.raw.store.edit, scope, message_id, content,
                                       expected_revision=expected_revision, edited_at=edited_at)

    async def forget(self, scope: Scope, message_id: str) -> None:
        self._authorize(scope)
        await self._mutation(self.raw.store.forget, scope, message_id)

    async def restore(self, scope: Scope, message_id: str) -> None:
        self._authorize(scope)
        await self._mutation(self.raw.store.restore, scope, message_id)

    async def _mutation(self, operation, *args, **options):
        if self.raw._closed or len(self.raw._inflight) >= 128:
            raise ValueError("memory writes unavailable")
        pending = asyncio.create_task(asyncio.to_thread(operation, *args, **options))
        self.raw._track_write(pending)
        cancelled = False
        while not pending.done():
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                cancelled = True
        result = pending.result()
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def purge(self, scope: Scope, message_id: str, *, expected_revision: int) -> dict:
        self._authorize(scope)
        # Reuse the existing fenced purge coordinator; never issue ad-hoc deletes.
        from kernel.raw_message_retrieval import RawMessageRetrieval

        coordinator = RawMessageRetrieval(self.raw.store, self.runtime.stores, self.runtime.vector)
        return await coordinator.purge(scope, message_id, expected_revision=expected_revision)

    async def start(self) -> None:
        await self.runtime.start()

    async def close(self) -> None:
        await self.runtime.close()

    def health(self) -> dict:
        return self.runtime.health()

    def startup_diagnostic(self) -> dict:
        return self.runtime.startup_diagnostic()

    @property
    def semantic_mode(self) -> str:
        return self.runtime.semantic_mode

    async def semantic_publication_state(self, partition: PipelineScope, *, deadline: float) -> dict:
        return await self.runtime.semantic_publication_state(partition, deadline=deadline)
