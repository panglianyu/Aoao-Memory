"""One serialized challenge host over the existing MemoryService implementation."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, TYPE_CHECKING

from kernel.challenge_diagnostics import DiagnosticError, emit_failure
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS, memory_retrieval_hints
from kernel.memory_pipeline_models import identity
from kernel.raw_message_models import ConflictError, MessageDraft
from kernel.raw_message_profile import ConnectorBinding, ConversationBinding, RawMessageProfile
from kernel.raw_message_service import stable_id
from .challenge_store import ChallengeStore, UserBinding, canonical_received_utc, ordinary_directory
from .grouped_extractor import memory_specs
from .memory_factory import create_memory_service
from .storage.shared_embedding_owner import DeadlineHandoffNotCommitted

if TYPE_CHECKING:
    from .storage.shared_embedding_owner import SharedEmbeddingGeneration, SharedEmbeddingOwner

VERSION = "ARU-THROUGHPUT-1"
MAX_MESSAGES = 256
MAX_MESSAGE_CHARS = 81920
CLEANUP_TIMEOUT = 60.0
PUBLICATION_CAUSES = frozenset({"capacity", "derived", "publisher", "reader", "deadline", "ownerunknown"})


class PublicationUnavailableError(RuntimeError):
    """A closed publication cause, never a model message or user identity."""

    def __init__(self, cause: str):
        self.cause_code = cause if type(cause) is str and cause in PUBLICATION_CAUSES else "ownerunknown"
        super().__init__("challenge publication unavailable")


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
                 add_timeout: float = 1700.0, embedding_owner: SharedEmbeddingOwner | None = None,
                 store: ChallengeStore | None = None, cache_observer: Callable | None = None,
                 observer: Callable | None = None, failure_observer: Callable | None = None):
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
        self._factory: Callable[..., Any] = service_factory or create_memory_service
        self._embedding_owner = embedding_owner
        self._embedding_generation: SharedEmbeddingGeneration | None = None
        self._timeout = float(add_timeout)
        if store is not None and (store.root != root or store.model != model):
            raise ValueError("shared journal must match the runtime identity")
        self._store = store if store is not None else ChallengeStore(root, model=model)
        self._cache_observer, self._observer = cache_observer, observer
        self._failure_observer = failure_observer
        self._cache_user: str | None = None
        self._cache_token = 0
        self._lease = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._cleanup_tasks: set[asyncio.Task] = set()
        self._cleanup_watchers: set[asyncio.Task] = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self._service = None
        self._binding: UserBinding | None = None
        self._blocked = False
        self._terminal_cause: str | None = None
        self._closing = False
        self._closed = False
        self._close_task: asyncio.Task | None = None
        self._failures: list[dict[str, Any]] = []

    def diagnostic(self) -> dict:
        """Internal finite diagnostics; never exception text, user IDs or data."""
        failures = deepcopy(self._failures)
        return {"state": self.health()["state"], "readiness": self.cached_readiness(),
                "failure": failures[0] if failures else None, "failures": failures}

    def _record_failure(self, stage: str, error: BaseException) -> None:
        allowed = {"cache_enter", "profile", "register_generation", "factory", "initialize",
                   "start", "bind_deadline", "prepare_close", "service_close",
                   "generation_close", "cache_retired", "published", "search",
                   "raw_settlement", "cleanup_timeout", "failure_observer"}
        if stage not in allowed:
            raise ValueError("invalid internal failure stage")
        classes = {"TimeoutError", "CancelledError", "PermissionError", "FileNotFoundError",
                   "OSError", "RuntimeError", "ValueError", "AdmissionClosed", "OperationalError",
                   "AssertionError", "KeyError", "TypeError", "BrokenPipeError", "EOFError"}
        chain: list[dict[str, Any]] = []
        current: BaseException | None = error
        seen: set[int] = set()
        project = str(Path(__file__).resolve().parents[2]).replace("\\", "/").casefold() + "/"
        while current is not None and len(chain) < 4 and id(current) not in seen:
            seen.add(id(current))
            name = type(current).__name__
            frames: list[dict[str, Any]] = []
            tb = current.__traceback__
            while tb is not None and len(frames) < 8:
                code = tb.tb_frame.f_code
                filename = code.co_filename.replace("\\", "/")
                if filename.casefold().startswith(project):
                    frames.append({"file": filename[len(project):],
                                   "function": code.co_name[:80], "line": tb.tb_lineno})
                tb = tb.tb_next
            cause: dict[str, Any] = {
                "error_type": name if name in classes else "OtherError", "frames": frames}
            if isinstance(current, OSError):
                for field in ("errno", "winerror"):
                    value = getattr(current, field, None)
                    if type(value) is int and 0 <= value <= 65535:
                        cause[field] = value
            chain.append(cause)
            current = current.__cause__ or current.__context__
        entry = {"stage": stage, "error_type": chain[0]["error_type"], "chain": chain}
        if type(error) is PublicationUnavailableError:
            entry["cause"] = error.cause_code
        elif isinstance(error, (TimeoutError, asyncio.TimeoutError)):
            entry["cause"] = "deadline"
        if len(self._failures) < 8:
            self._failures.append(entry)
        diagnostic_stage = ("retirement" if stage in {
            "prepare_close", "service_close", "generation_close", "cache_retired",
            "raw_settlement", "cleanup_timeout", "failure_observer"} else
            "published" if stage == "published" else
            "unknown" if stage == "search" else "prepare")
        try:
            evidence = error
            if "cause" in entry:
                evidence = DiagnosticError("unknown", publication_cause=entry["cause"])
                evidence = evidence.with_traceback(error.__traceback__)
            emit_failure(diagnostic_stage, evidence)
        except Exception:
            pass  # Optional evidence cannot replace the business failure or its owner.

    @contextmanager
    def _failure_stage(self, stage: str):
        try:
            yield
        except BaseException as error:
            self._record_failure(stage, error)
            raise

    def health(self) -> dict:
        state = ("stopped" if self._closed else "unavailable" if self._blocked
                 else "closing" if self._closing else "ready")
        if state == "ready" and self.cached_readiness()["terminal"]:
            state = "unavailable"
        return {"state": state, "version": VERSION}

    def cached_readiness(self) -> dict:
        """Inspect bounded RAM state only; never ask storage for freshness here."""
        if self._terminal_cause is not None:
            return {"terminal": True, "reason": self._terminal_cause}
        if self._blocked:
            return {"terminal": True, "reason": "ownerunknown"}
        backend = getattr(self._service, "runtime", None)
        observe = getattr(backend, "cached_readiness", None)
        if observe is None:
            # Host-independent fake services and an unbound fresh runtime have
            # no component cache. Production DailyMemoryRuntime supplies it.
            return {"terminal": False, "reason": "none"}
        try:
            value = observe()
        except Exception:
            return {"terminal": True, "reason": "ownerunknown"}
        if (type(value) is not dict or type(value.get("terminal")) is not bool
                or type(value.get("reason")) is not str
                or value.get("reason") not in PUBLICATION_CAUSES | {"none"}):
            return {"terminal": True, "reason": "ownerunknown"}
        return {"terminal": value["terminal"], "reason": value["reason"]}

    def _available(self) -> None:
        readiness = self.cached_readiness()
        if readiness["terminal"] and not self._blocked:
            self._terminal_cause = (readiness["reason"] if readiness["reason"] in PUBLICATION_CAUSES
                                    else "ownerunknown")
            self._block()
        if self._blocked or self._closing or self._closed:
            raise RuntimeError("challenge runtime unavailable")

    def _block(self) -> None:
        if self._blocked:
            return
        self._blocked = True
        if self._failure_observer is not None:
            try:
                self._failure_observer()
            except Exception as error:
                self._record_failure("failure_observer", error)

    @property
    def cache_user(self) -> str | None:
        return self._cache_user

    @contextmanager
    def _phase(self, stage: str, user: str):
        began = time.monotonic()
        try:
            yield
        finally:
            if self._observer is not None:
                self._observer(user, stage, began, time.monotonic())

    def _cache_event(self, event: str) -> None:
        if self._cache_observer is not None and self._cache_user is not None:
            self._cache_observer(event, self._cache_user, self._cache_token)

    @property
    def busy(self) -> bool:
        """Include owners still settling after their HTTP waiter has left."""
        return bool(self._tasks) or self._closing or self._blocked and self._owns_cache

    @property
    def _owns_cache(self) -> bool:
        return (self._cache_user is not None or self._service is not None
                or self._embedding_generation is not None)

    async def wait_idle(self) -> None:
        """Wait for operation owners, including work outliving an HTTP response."""
        await self._idle.wait()
        if self._blocked and self._owns_cache:
            raise RuntimeError("challenge ownership remains unresolved")

    def request_deadline(self, deadline: float | None = None) -> float:
        """Share one request budget with HTTP body reading and direct callers."""
        limit = time.monotonic() + self._timeout
        if deadline is not None:
            if type(deadline) not in {int, float} or not math.isfinite(deadline):
                raise ValueError("invalid challenge deadline")
            limit = min(limit, deadline)
        self._remaining(limit)
        return limit

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

    def _track_owner(self, task: asyncio.Task) -> None:
        self._idle.clear()
        self._tasks.add(task)

        def settled(done: asyncio.Task) -> None:
            self._tasks.discard(done)
            try:
                done.exception()
            except asyncio.CancelledError:
                if done in self._cleanup_tasks:
                    self._block()
            finally:
                if not self._tasks:
                    self._idle.set()

        task.add_done_callback(settled)

    async def _owned(self, operation, deadline: float):
        # No suspension is allowed between clearing idle and registering the
        # owner: the admission dispatcher must never observe a false idle gap.
        self._idle.clear()
        task = asyncio.create_task(operation)
        self._track_owner(task)
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=self._remaining(deadline))
        except asyncio.TimeoutError:
            raise TimeoutError("challenge request deadline exceeded") from None
        self._remaining(deadline)
        return result

    @asynccontextmanager
    async def _request_lease(self, deadline: float):
        await asyncio.wait_for(self._lease.acquire(), timeout=self._remaining(deadline))
        transferred = False
        try:
            self._remaining(deadline)
            yield
        except BaseException as error:
            if (not isinstance(error, (ConflictError, DeadlineHandoffNotCommitted)) and self._owns_cache
                    and not self._blocked):
                # Transfer this exact lease to cleanup before publishing the
                # request failure. Direct callers and admission see no idle gap.
                self._start_failed_cleanup(deadline)
                transferred = True
            raise
        finally:
            if not transferred:
                self._lease.release()

    @staticmethod
    def _cleanup_deadline(request_deadline: float) -> float:
        # This clock belongs only to retirement. A shielded Raw thread may
        # report its failure long after the HTTP/business clock has expired.
        return time.monotonic() + CLEANUP_TIMEOUT

    def _start_failed_cleanup(self, request_deadline: float) -> None:
        limit = self._cleanup_deadline(request_deadline)
        task = asyncio.create_task(self._retire_service(limit))
        self._cleanup_tasks.add(task)
        self._track_owner(task)

        def retired(done: asyncio.Task) -> None:
            self._cleanup_tasks.discard(done)
            self._lease.release()

        task.add_done_callback(retired)
        watcher = asyncio.create_task(self._watch_cleanup(task, limit))
        self._cleanup_watchers.add(watcher)
        watcher.add_done_callback(self._cleanup_watchers.discard)

    async def _watch_cleanup(self, task: asyncio.Task, deadline: float) -> None:
        done, _ = await asyncio.wait({task}, timeout=max(0, deadline - time.monotonic()))
        if not done:
            # Do not cancel the close coroutine: its Raw threads, workers and
            # generation still have to be owned after this cleanup budget ends.
            self._block()
            self._record_failure("cleanup_timeout", TimeoutError())

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
        self._remaining(deadline)
        if self._binding == binding and self._service is not None:
            if self._embedding_generation is not None:
                deadline_ns = int(deadline * 1_000_000_000)
                try:
                    await self._service.runtime.handoff_embedding_deadline(
                        self._embedding_generation, deadline_ns)
                except DeadlineHandoffNotCommitted as error:
                    self._record_failure("bind_deadline", error)
                    raise
                except asyncio.CancelledError:
                    # The generation handoff owner has settled before exposing
                    # cancellation. The request owner retains normal cleanup.
                    raise
                except BaseException as error:
                    self._record_failure("bind_deadline", error)
                    self._block()
                    raise RuntimeError("challenge shared embedding unavailable") from None
                self._remaining(deadline)
            return self._service
        await self._retire_service(self._cleanup_deadline(deadline))
        self._remaining(deadline)
        stage = "cache_enter"
        try:
            self._cache_token += 1
            self._cache_user = binding.user_id
            self._cache_event("enter")
            stage = "profile"
            with self._phase("factory", binding.user_id):
                profile = await asyncio.to_thread(self._profile, binding)
            self._remaining(deadline)
            shared_options = {}
            if self._embedding_owner is not None:
                stage = "register_generation"
                deadline_ns = int(deadline * 1_000_000_000)
                self._embedding_generation = await self._embedding_owner.new_generation(deadline_ns)
                shared_options = {"embedding_clients": self._embedding_generation.clients,
                                  "embedding_deadline_ns": deadline_ns}
                self._remaining(deadline)
            stage = "factory"
            with self._phase("factory", binding.user_id):
                service = await asyncio.to_thread(self._factory, profile, mode="memory-full-v1",
                    embedding="bge-m3", semantic_mode="paged_exact", client=self.client, model=self.model,
                    **shared_options)
            self._service, self._binding = service, binding
            self._remaining(deadline)
            with self._phase("start", binding.user_id):
                stage = "initialize"
                await service.raw.initialize_profile()
                self._remaining(deadline)
                stage = "start"
                await service.start()
            self._remaining(deadline)
        except TimeoutError as error:
            self._record_failure(stage, error)
            if stage in {"cache_enter", "register_generation"}:
                self._block()
            raise
        except BaseException as error:
            self._record_failure(stage, error)
            if stage in {"cache_enter", "profile", "register_generation"}:
                self._block()
            raise RuntimeError("challenge owner startup failed") from None
        return service

    async def _retire_service(self, deadline: float | None = None) -> None:
        stage = "prepare_close"
        try:
            def remaining() -> None:
                if deadline is not None:
                    self._remaining(deadline)

            remaining()
            close_options = ({"deadline_ns": int(deadline * 1_000_000_000)}
                             if deadline is not None else {})
            with self._phase("retire", self._cache_user or ""):
                if self._embedding_generation is not None:
                    await self._embedding_generation.prepare_close(**close_options)
                    remaining()
                if self._service is not None:
                    stage = "service_close"
                    await self._service.close()
                    remaining()
                    stage = "raw_settlement"
                    if self._service.raw.settlement_status != "settled":
                        raise RuntimeError("challenge Raw retirement incomplete")
                    self._service = None
                    self._binding = None
                if self._embedding_generation is not None:
                    stage = "generation_close"
                    await self._embedding_generation.close(**close_options)
                    remaining()
                    self._embedding_generation = None
                stage = "cache_retired"
                self._cache_event("retired")
                self._cache_user = None
        except BaseException as error:
            self._record_failure(stage, error)
            self._block()
            raise RuntimeError("challenge owner close failed") from None

    async def _published(self, service, deadline: float) -> None:
        with self._failure_stage("published"):
            self._remaining(deadline)
            with self._phase("published", self._cache_user or ""):
                state = await service.wait_until_published(service.scopes[0], deadline=deadline)
            if type(state) is not dict or state.get("status") != "current":
                self._remaining(deadline)
                if type(state) is dict and state.get("reason") == "publication_deadline":
                    raise TimeoutError("challenge publication deadline exceeded")
                reasons = {"vector_capacity_exceeded": "capacity", "derived_unavailable": "derived",
                           "semantic_publisher_failed": "publisher", "reader_unavailable": "reader"}
                readiness = self.cached_readiness()
                reason = state.get("reason") if type(state) is dict else None
                cause = reasons.get(reason, readiness["reason"]) if type(reason) is str else readiness["reason"]
                if readiness["terminal"] or type(state) is dict and state.get("terminal") is True:
                    self._terminal_cause = cause if cause in PUBLICATION_CAUSES else "ownerunknown"
                    self._block()
                raise PublicationUnavailableError(cause)
            self._remaining(deadline)

    async def add(self, *, user_id: str, session_id: str, request_id: str,
                  messages: list[dict], deadline: float | None = None) -> dict:
        deadline = self.request_deadline(deadline)
        return await self._add_request(user_id, session_id, request_id, messages, deadline)

    async def add_job(self, *, user_id: str, session_id: str, request_id: str,
                      messages: list[dict], deadline: float, received_utc: str) -> dict:
        """Internal durable-job entry; the caller supplies its first acceptance clock."""
        now = time.monotonic()
        if (type(deadline) not in {int, float} or not math.isfinite(deadline)
                or deadline > now + 1700):
            raise ValueError("invalid challenge job deadline")
        self._remaining(deadline)
        canonical_received_utc(received_utc)
        return await self._add_request(user_id, session_id, request_id, messages, deadline,
                                       received_utc=received_utc)

    async def _add_request(self, user_id, session_id, request_id, messages, deadline,
                           received_utc=None) -> dict:
        user_id, session_id, request_id = map(_external_id, (user_id, session_id, request_id))
        messages = _messages(messages)
        body = json.dumps({"user_id": user_id, "session_id": session_id,
            "request_id": request_id, "messages": messages}, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(body) > 2 * 1024**2:
            raise ValueError("challenge request too large")
        digest = hashlib.sha256(body).hexdigest()
        self._available()
        operation = (self._add(user_id, session_id, request_id, messages, digest, deadline)
                     if received_utc is None else
                     self._add(user_id, session_id, request_id, messages, digest, deadline,
                               received_utc=received_utc))
        return await self._owned(operation, deadline)

    async def _add(self, user_id, session_id, request_id, messages, digest, deadline,
                   received_utc=None):
        async with self._request_lease(deadline):
            self._available()
            self._remaining(deadline)
            binding, request = await self._store.begin(user_id, request_id, digest, len(messages),
                **({"received_utc": received_utc} if received_utc is not None else {}))
            service = await self._ensure_service(binding, deadline)
            scope = service.scopes[0]
            with self._phase("add", user_id):
                for index in range(request.progress, len(messages)):
                    self._remaining(deadline)
                    row = messages[index]
                    created_at = (request.received_utc if row["timestamp"] is None else
                        (datetime(1970, 1, 1, tzinfo=timezone.utc)
                         + timedelta(milliseconds=row["timestamp"])).isoformat())
                    message_id = stable_id("amc-message-v1", user_id, request_id, str(index))
                    await service.add(scope, MessageDraft(message_id, scope.conversation_id,
                        session_id, row["role"], row["content"], "text", created_at, "web",
                        scope.user_id, idempotency_key=message_id))
                    self._remaining(deadline)
                    await self._store.checkpoint(binding, request_id, index + 1)
                    self._remaining(deadline)
            await self._published(service, deadline)
            await self._store.complete(binding, request_id)
            self._remaining(deadline)
            return {"success": True, "request_id": request_id,
                    "user_id": user_id, "session_id": session_id}

    async def search(self, *, user_id: str, query: str, top_k: int,
                     options=(), deadline: float | None = None) -> list[dict]:
        deadline = self.request_deadline(deadline)
        user_id = _external_id(user_id)
        if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(top_k) is not int or not 1 <= top_k <= 100):
            raise ValueError("invalid challenge search")
        try:
            query.encode("utf-8")
        except UnicodeError:
            raise ValueError("invalid challenge search") from None
        hints = memory_retrieval_hints(options)
        self._available()
        return await self._owned(self._search(user_id, query, top_k,
                                               deadline, hints), deadline)

    async def _search(self, user_id, query, top_k, deadline, hints=()):
        async with self._request_lease(deadline):
            self._available()
            self._remaining(deadline)
            binding = await self._store.lookup(user_id)
            self._remaining(deadline)
            if binding is None:
                return []
            service = await self._ensure_service(binding, deadline)
            await self._published(service, deadline)
            query_options = {"retrieval_hints": hints} if hints else {}
            with self._failure_stage("search"), self._phase("search", user_id):
                outcome = await service.search(service.scopes[0], query, top_k=top_k,
                    neighbors=0, purpose="interactive", deadline=min(deadline, time.monotonic() + 2),
                    **query_options)
            self._remaining(deadline)
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
            await self._retire_service()
            self._closed = True
