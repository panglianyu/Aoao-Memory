"""One serialized challenge host over the existing MemoryService implementation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import time

from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from kernel.memory_pipeline_models import identity
from kernel.raw_message_models import MessageDraft
from kernel.raw_message_profile import ConnectorBinding, ConversationBinding, RawMessageProfile
from kernel.raw_message_service import stable_id
from .challenge_store import ChallengeStore, UserBinding, ordinary_directory
from .grouped_extractor import memory_specs
from .memory_factory import create_memory_service

VERSION = "AMC-ENTRY-1.0"
MAX_MESSAGES = 256
MAX_MESSAGE_CHARS = 81920


def _external_id(value: object) -> str:
    if type(value) is not str or not value.strip() or len(value) > 256 or "\0" in value:
        raise ValueError("invalid challenge identity")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ValueError("invalid challenge identity") from None
    return value


def _messages(value: object) -> list[dict]:
    if type(value) is not list or not 1 <= len(value) <= MAX_MESSAGES:
        raise ValueError("invalid challenge messages")
    result = []
    for row in value:
        if (type(row) is not dict or set(row) - {"role", "content", "timestamp"}
                or row.get("role") not in {"user", "assistant"}
                or type(row.get("content")) is not str or not row["content"].strip()
                or len(row["content"]) > MAX_MESSAGE_CHARS):
            raise ValueError("invalid challenge message")
        try:
            row["content"].encode("utf-8")
        except UnicodeError:
            raise ValueError("invalid challenge message") from None
        stamp = row.get("timestamp")
        if stamp is not None and (type(stamp) is not int or not 0 <= stamp <= 253402300799999):
            raise ValueError("invalid challenge timestamp")
        result.append({"role": row["role"], "content": row["content"], "timestamp": stamp})
    return result


class ChallengeRuntime:
    def __init__(self, root: Path, *, client, model: str, service_factory=None,
                 add_timeout: float = 1700.0):
        if not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts:
            raise ValueError("explicit absolute challenge root required")
        project = Path(__file__).resolve().parents[2]
        if (root == project or project.is_relative_to(root)
                or root.is_relative_to(project / "data") or root.is_relative_to(project / "runtime")):
            raise ValueError("challenge data must be separate from production and old evaluations")
        identity(model)
        if (client is None or type(add_timeout) not in {int, float} or not math.isfinite(add_timeout)
                or not 0 < add_timeout <= 1700):
            raise ValueError("explicit challenge model client and finite timeout required")
        self.root, self.client, self.model = root, client, model
        self._factory = service_factory or create_memory_service
        self._timeout = float(add_timeout)
        self._store = ChallengeStore(root, model=model)
        self._lease = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._service = None
        self._binding: UserBinding | None = None
        self._blocked = False
        self._closing = False
        self._closed = False
        self._close_task: asyncio.Task | None = None

    def health(self) -> dict:
        state = ("stopped" if self._closed else "unavailable" if self._blocked
                 else "closing" if self._closing else "ready")
        return {"state": state, "version": VERSION}

    def _available(self) -> None:
        if self._blocked or self._closing or self._closed:
            raise RuntimeError("challenge runtime unavailable")

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("challenge request deadline exceeded")
        return remaining

    @staticmethod
    async def _settle(task: asyncio.Task):
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        result = task.result()
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _owned(self, operation):
        task = asyncio.create_task(operation)
        self._tasks.add(task)
        try:
            return await self._settle(task)
        finally:
            self._tasks.discard(task)

    def _profile(self, binding: UserBinding) -> RawMessageProfile:
        root = self.root / "users" / binding.key
        for path in (root / "raw", root / "derived"):
            ordinary_directory(path)
            path.mkdir(parents=True, exist_ok=True)
            ordinary_directory(path)
        owner = "amc-" + binding.key
        return RawMessageProfile(root / "raw" / "messages.db", owner,
            (ConnectorBinding("amc-agent", "web", "amc-web", owner,
                (ConversationBinding("memory", "memory", "web", "Challenge memory"),)),),
            memory_specs("memory-full-v1", self.model), root / "derived",
            derivation_input_chars=MAX_MESSAGE_CHARS)

    async def _ensure_service(self, binding: UserBinding, deadline: float):
        if self._binding == binding and self._service is not None:
            return self._service
        if self._service is not None:
            try:
                await self._service.close()
            except BaseException:
                self._blocked = True
                raise RuntimeError("challenge owner close failed") from None
            self._service = None
            self._binding = None
        self._remaining(deadline)
        try:
            profile = await asyncio.to_thread(self._profile, binding)
            service = await asyncio.to_thread(self._factory, profile, mode="memory-full-v1",
                embedding="bge-m3", semantic_mode="snapshot", client=self.client, model=self.model)
            self._service, self._binding = service, binding
            self._remaining(deadline)
            await service.raw.initialize_profile()
            self._remaining(deadline)
            await asyncio.wait_for(service.start(), timeout=self._remaining(deadline))
        except BaseException:
            self._blocked = True
            raise RuntimeError("challenge owner startup failed") from None
        return service

    async def _published(self, service, deadline: float) -> None:
        self._remaining(deadline)
        state = await asyncio.wait_for(service.wait_until_published(
            service.scopes[0], deadline=deadline), timeout=self._remaining(deadline))
        if type(state) is not dict or state.get("status") != "current":
            self._remaining(deadline)
            if type(state) is dict and state.get("reason") == "publication_deadline":
                raise TimeoutError("challenge publication deadline exceeded")
            raise RuntimeError("challenge publication unavailable")
        self._remaining(deadline)

    async def add(self, *, user_id: str, session_id: str, request_id: str,
                  messages: list[dict]) -> dict:
        user_id, session_id, request_id = map(_external_id, (user_id, session_id, request_id))
        messages = _messages(messages)
        body = json.dumps({"user_id": user_id, "session_id": session_id,
            "request_id": request_id, "messages": messages}, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(body) > 2 * 1024**2:
            raise ValueError("challenge request too large")
        digest = hashlib.sha256(body).hexdigest()
        self._available()
        return await self._owned(self._add(user_id, session_id, request_id, messages, digest,
                                           time.monotonic() + self._timeout))

    async def _add(self, user_id, session_id, request_id, messages, digest, deadline):
        async with self._lease:
            self._available()
            self._remaining(deadline)
            binding, request = await self._store.begin(user_id, request_id, digest, len(messages))
            service = await self._ensure_service(binding, deadline)
            scope = service.scopes[0]
            for index in range(request.progress, len(messages)):
                self._remaining(deadline)
                row = messages[index]
                created_at = (request.received_utc if row["timestamp"] is None else
                    (datetime(1970, 1, 1, tzinfo=timezone.utc)
                     + timedelta(milliseconds=row["timestamp"])).isoformat())
                message_id = stable_id("amc-message-v1", user_id, request_id, str(index))
                await asyncio.wait_for(service.add(scope, MessageDraft(message_id, scope.conversation_id,
                    session_id, row["role"], row["content"], "text", created_at, "web",
                    scope.user_id, idempotency_key=message_id)), timeout=self._remaining(deadline))
                await self._store.checkpoint(binding, request_id, index + 1)
            await self._published(service, deadline)
            await self._store.complete(binding, request_id)
            return {"success": True, "request_id": request_id,
                    "user_id": user_id, "session_id": session_id}

    async def search(self, *, user_id: str, query: str, top_k: int) -> list[dict]:
        user_id = _external_id(user_id)
        if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(top_k) is not int or not 1 <= top_k <= 100):
            raise ValueError("invalid challenge search")
        try:
            query.encode("utf-8")
        except UnicodeError:
            raise ValueError("invalid challenge search") from None
        self._available()
        return await self._owned(self._search(user_id, query, top_k,
                                               time.monotonic() + self._timeout))

    async def _search(self, user_id, query, top_k, deadline):
        async with self._lease:
            self._available()
            self._remaining(deadline)
            binding = await self._store.lookup(user_id)
            if binding is None:
                return []
            service = await self._ensure_service(binding, deadline)
            await self._published(service, deadline)
            outcome = await service.search(service.scopes[0], query, top_k=top_k,
                neighbors=0, purpose="interactive", deadline=min(deadline, time.monotonic() + 2))
            if (outcome.status not in {"ready", "degraded"} or type(outcome.items) is not list
                    or outcome.status == "degraded" and not outcome.items):
                raise RuntimeError("challenge retrieval unavailable")
            result, seen = [], set()
            for evidence in outcome.items:
                key = evidence.get("source_message_id")
                content = evidence.get("source_content")
                if type(key) is not str or not key or type(content) is not str or not content.strip():
                    raise RuntimeError("challenge evidence invalid")
                if key in seen:
                    continue
                seen.add(key)
                item = {"id": key, "content": content}
                created_at = evidence.get("source_time")
                if type(created_at) is str and created_at:
                    item["created_at"] = created_at
                result.append(item)
                if len(result) == top_k:
                    break
            return result

    async def close(self) -> None:
        if self._closed:
            return
        self._closing = True
        if self._close_task is None or self._close_task.done():
            self._close_task = asyncio.create_task(self._close_owned())
        await self._settle(self._close_task)

    async def _close_owned(self) -> None:
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        async with self._lease:
            if self._service is not None:
                try:
                    await self._service.close()
                except BaseException:
                    self._blocked = True
                    raise RuntimeError("challenge owner close failed") from None
                self._service = None
                self._binding = None
            self._closed = True
