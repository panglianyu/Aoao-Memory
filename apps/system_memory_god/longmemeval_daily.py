"""Unlabelled raw-vector benchmark profile over the existing daily pipeline.

No LLM client, production configuration, answer labels, or migrations. A single
owned event loop preserves Runtime/Outbox tasks across the synchronous index API.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import time
from typing import Protocol

from kernel.app_bus import AppBus
from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import DerivationSpec, MessageDraft
from kernel.raw_message_profile import ConnectorBinding, ConversationBinding, RawMessageProfile
from kernel.raw_message_service import RawMessageService, stable_id
from kernel.raw_message_worker import RawMessageWorker
from .memory_factory import create_memory_service
from .longmemeval import CorpusItem, parse_longmemeval_timestamp

_CHUNK_CHARS = 8192
RAW_VECTOR_SPEC = DerivationSpec("longmemeval-raw-vector", "v1", "vector", "none",
    hashlib.sha256(b"exact original slices of 8192 characters; no LLM").hexdigest())


def retrieval_source_sha256() -> str:
    """Bind both profiles to current retrieval code, including shared dependencies."""
    root = Path(__file__).resolve().parents[2]
    files = [path for base in (root / "kernel", root / "apps" / "system_memory_god", root / "protocols")
             for path in base.rglob("*.py")]
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.relative_to(root).as_posix()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


class _RawVectorExtractor:
    async def extract(self, domain, message, source, max_output_tokens):
        if domain != "vector" or source.processor != RAW_VECTOR_SPEC.processor:
            raise ValueError("unregistered benchmark processor")
        chunks = [message.content[start:start + _CHUNK_CHARS]
                  for start in range(0, len(message.content), _CHUNK_CHARS)]
        if len(chunks) > 10:
            raise ValueError("benchmark original exceeds derivation output capacity")
        return tuple({"text": chunk} for chunk in chunks if chunk.strip())


class _BenchmarkRaw(RawMessageService):
    def start_worker(self, bus, *, blocked_scopes=frozenset(), on_settled=None):
        # Four public S-cleaned originals exceed the daily 32k default. Use the
        # existing worker's validated input limit, solely in this isolated profile.
        if (self._closed or self.worker is not None or self.profile.derived_root is None
                or bus.raw_messages is not self or bus._daily_derivation is None):
            raise ValueError("benchmark worker assembly unavailable")
        self.worker = RawMessageWorker(self.store, bus, self.configured_scopes(),
            max_input_chars=128000, blocked_scopes=blocked_scopes, on_settled=on_settled)
        self.worker.start()


class _CloseOwner(Protocol):
    def close(self) -> None: ...


class _BenchmarkCleanupError(RuntimeError):
    def __init__(self, owner: _BenchmarkCleanupOwner, *, assembly_error: BaseException | None = None):
        super().__init__("benchmark cleanup incomplete")
        self.retained_owner = owner
        self.assembly_error = assembly_error


class _BenchmarkCleanupOwner:
    """Settle the retained native owner before the adapter's loop/resources."""

    def __init__(self, adapter: LongMemEvalDailyIndex, lower: _CloseOwner | None):
        self._owners: list[_CloseOwner] = list(lower._owners) if isinstance(
            lower, _BenchmarkCleanupOwner) else ([] if lower is None else [lower])
        if not any(owner is adapter for owner in self._owners):
            self._owners.append(adapter)

    def close(self) -> None:
        for owner in tuple(self._owners):
            try:
                owner.close()
            except BaseException as error:
                lower = getattr(error, "retained_owner", None)
                if lower is not None and lower is not self and not any(
                        item is lower for item in self._owners):
                    self._owners.insert(0, lower)
                raise _BenchmarkCleanupError(self) from error
            self._owners.remove(owner)


class LongMemEvalDailyIndex:
    retrieval_mode = "daily-raw-vector-online"

    def __init__(self, case_dir: Path, *, question_id: str, embedding: str = "synthetic", close_callback=None):
        if embedding not in {"synthetic", "bge-m3"} or not isinstance(question_id, str) or not question_id:
            raise ValueError("explicit benchmark profile required")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise ValueError("synchronous benchmark requires its own event loop")
        self.root = Path(case_dir).resolve()
        if self.root.exists():
            raise ValueError("benchmark requires a fresh case directory")
        # Chroma creates a UUID segment below the hashed partition. Check the
        # native directory and longest file, not only the supplied case root.
        segment = self.root / "derived" / ("0" * 64) / "vectors" / ("0" * 36)
        directory_length = len(str(segment).encode("utf-16-le")) // 2
        file_length = len(str(segment / "index_metadata.pickle").encode("utf-16-le")) // 2
        if os.name == "nt" and (directory_length >= 248 or file_length >= 260):
            raise ValueError("benchmark downstream path too long")
        self.loop = asyncio.new_event_loop()
        self.indexed = self.skipped = 0
        self.failed = False
        self._closed = self._added = False
        self._items: dict[str, CorpusItem] = {}
        self.last_outcome: dict = {}
        self._close_callback = close_callback
        try:
            self.loop.run_until_complete(self._assemble(question_id, embedding))
        except BaseException as assembly_error:
            self.failed = True
            try:
                self.close()
            except BaseException as cleanup_error:
                owner = _BenchmarkCleanupOwner(self, getattr(cleanup_error, "retained_owner", None))
                raise _BenchmarkCleanupError(owner, assembly_error=assembly_error) from cleanup_error
            raise

    async def _assemble(self, question_id, embedding):
        owner = "lme-daily-" + hashlib.sha256(question_id.encode()).hexdigest()[:16]
        profile = RawMessageProfile(self.root / "raw" / "messages.db", owner,
            (ConnectorBinding("lme-agent", "web", "lme-web", owner,
                (ConversationBinding("benchmark", "benchmark", "web", "LongMemEval"),)),),
            (RAW_VECTOR_SPEC,), self.root / "derived")
        self.raw = _BenchmarkRaw(profile)
        self.bus = AppBus(str(self.root / "apps"))
        self.bus.attach_raw_messages(self.raw, lambda: "lme-agent")
        self.service = create_memory_service(profile, mode="raw-vector-v1", raw=self.raw, bus=self.bus,
            extractor=_RawVectorExtractor(), embedding=embedding, semantic_mode="snapshot")
        self.runtime = self.service.runtime
        self.bus.attach_daily_runtime(self.service)
        self.scope = self.raw.configured_scopes()[0]
        self.partition = PipelineScope(owner, "lme-agent", "lme-web", self.scope.conversation_id)
        self.question_id = question_id

    def add(self, corpus):
        if self._added or self._closed:
            raise ValueError("benchmark corpus already submitted or closed")
        self._added = True
        try:
            self.loop.run_until_complete(self._add(corpus))
        except BaseException:
            self.failed = True
            raise

    async def _add(self, corpus):
        await self.raw.initialize_profile()

        async def ingest():
            for original in corpus:
                item = replace(original, has_answer=False)
                reference = item.source_ref or item.corpus_id
                key = stable_id("longmemeval", self.question_id, reference)
                if key in self._items:
                    raise ValueError("duplicate benchmark source reference")
                suffix = reference.rsplit("#occ", 1)[1] if "#occ" in reference else None
                session = item.session_id + ("#occ" + suffix if suffix is not None else "")
                created_at = datetime.fromtimestamp(parse_longmemeval_timestamp(item.timestamp),
                                                     timezone.utc).isoformat()
                await self.service.add(self.scope, MessageDraft(key, self.scope.conversation_id,
                    session, "user", item.text, "text", created_at, "web", self.scope.user_id,
                    idempotency_key=key, derivations=(RAW_VECTOR_SPEC,)))
                self._items[key] = item
                self.indexed += 1

        await ingest()
        await self.service.start()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            inventory = await asyncio.to_thread(self.raw.store.partition_derivation_inventory, self.scope)
            for row in inventory:
                source = row["source"]
                if (row["status"] == "failed" and source.raw_message_id in self._items
                        and source.processor == RAW_VECTOR_SPEC.processor
                        and source.processor_version == RAW_VECTOR_SPEC.processor_version
                        and source.model == RAW_VECTOR_SPEC.model
                        and source.prompt_hash == RAW_VECTOR_SPEC.prompt_hash
                        and row["domain"] == RAW_VECTOR_SPEC.target_domain):
                    raise RuntimeError("benchmark derivation failed")
            _, resolved = await asyncio.to_thread(self.raw.store.partition_state, self.scope)
            if resolved:
                state = await self.runtime.semantic_publication_state(self.partition,
                    deadline=min(deadline, time.monotonic() + .3))
                if state["status"] == "current":
                    return
                if state["terminal"]:
                    raise RuntimeError("benchmark publication failed")
            await asyncio.sleep(.05)
        raise TimeoutError("benchmark derivation/publication deadline exceeded")

    def retrieve(self, query: str, top_k: int):
        if self._closed or not self._added or type(top_k) is not int or not 1 <= top_k <= 100:
            raise ValueError("invalid online benchmark query")
        try:
            return self.loop.run_until_complete(self._retrieve(query, top_k))
        except BaseException:
            self.failed = True
            raise

    async def _retrieve(self, query, top_k):
        began = time.monotonic()
        result = await self.service.search(self.scope, query,
            top_k=top_k, neighbors=0, purpose="interactive", deadline=began + 2)
        ranked, seen, invalid = [], set(), 0
        for evidence in result.items:
            key = evidence.get("source_message_id")
            item = self._items.get(key)
            if item is None:
                invalid += 1
                continue
            content = evidence.get("source_content")
            start = evidence.get("source_excerpt_start_char", 0)
            end = evidence.get("source_excerpt_end_char", len(item.text))
            if (type(start) is not int or type(end) is not int or not 0 <= start <= end <= len(item.text)
                    or evidence.get("revision") != 1 or content != item.text[start:end]
                    or evidence.get("source_content_hash") != hashlib.sha256(item.text.encode()).hexdigest()):
                invalid += 1
                continue
            if key not in seen:
                seen.add(key)
                ranked.append(item)
        self.last_outcome = {"status": result.status, "reason_codes": list(result.reason_codes),
            "query_ms": (time.monotonic() - began) * 1000, "source_proof_failures": invalid,
            "evidence_candidates": len(result.items), "distinct_sources": len(ranked),
            "result_budget_limited": "result_budget_limited" in result.reason_codes,
            "cloud_requests": 0, "input_policy": "user-only; raw exact; vector 8192-char slices"}
        if invalid:
            self.failed = True
            raise RuntimeError("benchmark source proof mismatch")
        return ranked

    def close(self):
        if self._closed:
            return
        try:
            runtime = getattr(self, "runtime", None)
            raw = getattr(self, "raw", None)
            if runtime is not None:
                self.loop.run_until_complete(runtime.close())
            elif raw is not None:
                self.loop.run_until_complete(raw.close())
            if asyncio.all_tasks(self.loop):
                raise RuntimeError("benchmark async settlement incomplete")
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.run_until_complete(self.loop.shutdown_default_executor())
        except BaseException:
            self.failed = True
            raise
        self.loop.close()
        self._closed = True
        if not self.failed and self._close_callback is not None:
            self._close_callback()
