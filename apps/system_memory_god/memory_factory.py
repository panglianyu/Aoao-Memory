"""Explicit memory assembly, shared by host adapters and isolated evaluations.

No Brain, channel discovery, credentials/config loading or legacy stores.
"""

from __future__ import annotations

import os
import asyncio
from kernel.raw_message_service import RawMessageService
from .daily_runtime import DailyMemoryRuntime, _shared_options
from .memory_service import MemoryService


class RawOriginalExtractor:
    def __init__(self, *, tokenizer=None):
        self._tokenizer = tokenizer

    async def extract(self, domain, message, source, max_output_tokens):
        if domain != "vector":
            raise ValueError("raw-vector profile cannot derive structured facts")
        from .storage.token_chunking import chunk_original

        chunks = await asyncio.to_thread(chunk_original, message.content, tokenizer=self._tokenizer)
        return tuple({"text": chunk} for chunk in chunks)


class _MemoryHost:
    """Only the existing worker/runtime binding contract; no application bus boot."""

    def __init__(self, raw):
        self.raw_messages = raw
        self._daily_runtime = None
        self._daily_derivation = None

    async def derive_raw_message(self, scope, task, message, guard, complete):
        if self._daily_derivation is None:
            raise RuntimeError("memory derivation not bound")
        return await self._daily_derivation.process(scope, task, message, guard, complete=complete)


def create_memory_service(profile, *, mode="memory-full-v1", extractor=None,
                          embedding="bge-m3", semantic_mode="snapshot", raw=None, bus=None,
                          semantic_diagnostics=False, client=None, model=None,
                          embedding_clients=None, embedding_deadline_ns=None) -> MemoryService:
    shared_clients = _shared_options(profile, embedding, semantic_mode,
                                     embedding_clients, embedding_deadline_ns)
    if mode not in {"raw-vector-v1", "memory-full-v1", "legacy-daily-v1"}:
        raise ValueError("unknown memory profile")
    if profile.derived_root is None:
        raise ValueError("explicit derived root required")
    if semantic_mode == "snapshot" and mode != "legacy-daily-v1":
        segment = profile.derived_root / ("0" * 64) / "vectors" / ("0" * 36)
        if os.name == "nt" and (len(str(segment).encode("utf-16-le")) // 2 >= 248
                or len(str(segment / "index_metadata.pickle").encode("utf-16-le")) // 2 >= 260):
            raise ValueError("memory profile downstream path too long")
    if mode == "raw-vector-v1" and any(spec.target_domain != "vector" for spec in profile.derivations):
        raise ValueError("raw-vector profile requires vector-only tasks")
    if extractor is None:
        if mode == "memory-full-v1" and client is not None and model is not None:
            from .grouped_extractor import GroupedDailyExtractor
            from .storage.scoped_stores import ScopedStores
            extractor = GroupedDailyExtractor(ScopedStores(profile.derived_root), client, model)
        elif mode != "raw-vector-v1":
            raise ValueError("explicit structured extraction boundary required")
        else:
            extractor = RawOriginalExtractor()
    raw = raw if raw is not None else RawMessageService(profile)
    host = bus if bus is not None else _MemoryHost(raw)
    runtime = DailyMemoryRuntime(raw, profile, host, extractor=extractor, embedding=embedding,
                                 semantic_mode=semantic_mode, semantic_diagnostics=semantic_diagnostics,
                                 **({"embedding_clients": shared_clients,
                                     "embedding_deadline_ns": embedding_deadline_ns}
                                    if shared_clients else {}))
    service = MemoryService(runtime, profile_id=mode)
    if bus is None:
        host._daily_runtime, host._daily_derivation = service, runtime.derivation
    return service
