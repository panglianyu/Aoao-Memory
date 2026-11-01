"""Private, generation-scoped access to one spawned local embedding model.

Only the trusted parent control pipe transfers Connection capabilities. Inference
connections accept bounded JSON bytes, never pickled application data.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
import json
import math
import multiprocessing
from multiprocessing.connection import _ConnectionBase, wait
from multiprocessing.process import BaseProcess
import secrets
import threading
import time


SCHEMA = "amc-shared-embedding-v1"
MAX_TEXT_BYTES = 131072
MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 65536
_INT64 = (1 << 63) - 1
_ROLES = ("writer", "query")
_ERRORS = frozenset({"expired", "closed", "protocol", "overload", "model_error"})
_REQUEST_KEYS = frozenset({"schema", "op", "owner_nonce", "generation", "role",
                           "request_id", "deadline_ns", "text"})
_RESPONSE_KEYS = frozenset({"schema", "op", "owner_nonce", "generation", "role",
                            "request_id", "ok"})
# All counters are shared by the parent, model process, and two private clients.
(_INFLIGHT, _PEAK, _LOADS, _GENERATIONS, _GENERATION, _DEADLINE, _FAILED,
 _OWNERS, _OWNER_PEAK, _CLOSING, _WRITER_ID, _QUERY_ID, _WRITER_IO,
 _QUERY_IO, _UPDATING) = range(15)
_ROLE_ID = {"writer": _WRITER_ID, "query": _QUERY_ID}
_ROLE_IO = {"writer": _WRITER_IO, "query": _QUERY_IO}


@contextmanager
def _locked(guard, deadline_ns: int | None = None):
    """Dedicated workers may wait only within a finite original/cleanup budget."""
    limit = deadline_ns if deadline_ns is not None else time.monotonic_ns() + 5_000_000_000
    remaining = max(0.0, (limit - time.monotonic_ns()) / 1_000_000_000)
    if not guard.acquire(True, remaining):
        raise RuntimeError("shared embedding accounting lock unavailable")
    try:
        yield
    finally:
        guard.release()


def _try_mark_failed(state, guard) -> None:
    if guard.acquire(False):
        try:
            state[_FAILED] = 1
        finally:
            guard.release()


def _positive_int(value) -> bool:
    return type(value) is int and 0 < value <= _INT64


def _check_deadline(deadline_ns: int) -> None:
    if not _positive_int(deadline_ns):
        raise ValueError("invalid embedding deadline")
    if time.monotonic_ns() >= deadline_ns:
        raise TimeoutError("embedding deadline expired")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate embedding field")
        result[key] = value
    return result


def _json_frame(payload: dict, maximum: int) -> bytes:
    frame = json.dumps(payload, ensure_ascii=False, allow_nan=False,
                       separators=(",", ":")).encode("utf-8")
    if len(frame) > maximum:
        raise ValueError("embedding frame too large")
    return frame


def _decode(frame: bytes, maximum: int) -> dict:
    if type(frame) is not bytes or len(frame) > maximum:
        raise ValueError("invalid embedding frame size")
    try:
        payload = json.loads(frame.decode("utf-8"), object_pairs_hook=_pairs)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError("invalid embedding JSON") from error
    if type(payload) is not dict:
        raise ValueError("invalid embedding object")
    return payload


def _identity(payload: dict, nonce: str, generation: int, role: str) -> None:
    if (payload.get("schema") != SCHEMA or payload.get("owner_nonce") != nonce
            or type(payload.get("generation")) is not int
            or payload.get("generation") != generation
            or payload.get("role") != role or not _positive_int(payload.get("request_id"))):
        raise ValueError("invalid embedding identity")


def _encode_request(nonce: str, generation: int, role: str, request_id: int,
                    deadline_ns: int, text: str) -> bytes:
    if (type(nonce) is not str or len(nonce) != 64
            or any(character not in "0123456789abcdef" for character in nonce)
            or not _positive_int(generation) or role not in _ROLES
            or not _positive_int(request_id) or not _positive_int(deadline_ns)
            or type(text) is not str or len(text.encode("utf-8")) > MAX_TEXT_BYTES):
        raise ValueError("invalid embedding request")
    return _json_frame({"schema": SCHEMA, "op": "embed", "owner_nonce": nonce,
                        "generation": generation, "role": role, "request_id": request_id,
                        "deadline_ns": deadline_ns, "text": text}, MAX_REQUEST_BYTES)


def _validate_vector(vector) -> list[float]:
    if type(vector) is not list or len(vector) != 1024:
        raise ValueError("invalid embedding vector")
    try:
        if any(type(value) not in {int, float} or not math.isfinite(value)
               for value in vector):
            raise ValueError("invalid embedding vector")
        return [float(value) for value in vector]
    except OverflowError as error:
        raise ValueError("invalid embedding vector") from error


def _decode_response(frame: bytes, nonce: str, generation: int,
                     role: str, request_id: int) -> list[float]:
    payload = _decode(frame, MAX_RESPONSE_BYTES)
    _identity(payload, nonce, generation, role)
    if (payload.get("op") != "result" or payload["request_id"] != request_id
            or type(payload.get("ok")) is not bool):
        raise ValueError("invalid embedding response identity")
    if payload["ok"]:
        if set(payload) != _RESPONSE_KEYS | {"vector"}:
            raise ValueError("invalid embedding result fields")
        return _validate_vector(payload["vector"])
    if (set(payload) != _RESPONSE_KEYS | {"error_code"}
            or type(payload["error_code"]) is not str or payload["error_code"] not in _ERRORS):
        raise ValueError("invalid embedding error fields")
    if payload["error_code"] == "expired":
        raise TimeoutError("embedding deadline expired")
    raise RuntimeError("shared embedding " + payload["error_code"])


class SharedEmbeddingClient:
    """One worker's synchronous, single-outstanding callable embedding ABI."""

    def __init__(self, connection, state, guard, nonce: str,
                 generation: int, role: str, deadline_ns: int, *, global_state=None):
        self._connection = connection
        self._state, self._guard = state, guard
        self._global = state if global_state is None else global_state
        self._nonce, self._generation, self._role = nonce, generation, role
        self._deadline_ns, self._sequence = deadline_ns, 0
        self._closed = self._unsettled = False
        self._io_lock = threading.Lock()

    def __getstate__(self):
        return {key: value for key, value in self.__dict__.items() if key != "_io_lock"}

    def __setstate__(self, values):
        self.__dict__.update(values)
        self._io_lock = threading.Lock()

    def bind_deadline(self, deadline_ns: int) -> None:
        if not _positive_int(deadline_ns):
            raise ValueError("invalid embedding deadline")
        self._deadline_ns = deadline_ns

    def close_parent_copy(self) -> None:
        self.close()

    def close(self) -> None:
        self._closed = True
        self._connection.close()

    def _wait_completion(self, request_id: int, deadline_ns: int) -> None:
        # Receiving bytes precedes the server's completion commit. Keep the role
        # reserved until that exact request's accounting is committed as well.
        while True:
            with _locked(self._guard, deadline_ns):
                if self._global[_FAILED]:
                    raise RuntimeError("shared embedding owner failed")
                if self._state[_GENERATION] != self._generation:
                    raise ValueError("embedding generation identity changed")
                outstanding = self._state[_ROLE_ID[self._role]]
                if outstanding == 0:
                    return
                if outstanding != request_id:
                    raise ValueError("embedding completion identity changed")
            _check_deadline(deadline_ns)
            time.sleep(min(0.001, max(0.0, (deadline_ns - time.monotonic_ns()) / 1_000_000_000)))

    def __call__(self, texts: list[str]) -> list[list[float]]:
        if type(texts) is not list or len(texts) != 1 or type(texts[0]) is not str:
            raise ValueError("shared embedding requires one string")
        if self._closed or self._unsettled or not self._io_lock.acquire(blocking=False):
            raise RuntimeError("shared embedding client unavailable")
        received = reserved = waiting = False
        try:
            deadline_ns = self._deadline_ns
            _check_deadline(deadline_ns)
            request_id = self._sequence + 1
            frame = _encode_request(self._nonce, self._generation, self._role,
                                    request_id, deadline_ns, texts[0])
            with _locked(self._guard, deadline_ns):
                _check_deadline(deadline_ns)
                if (self._global[_FAILED] or self._state[_CLOSING] or self._state[_UPDATING]
                        or self._state[_GENERATION] != self._generation
                        or self._state[_DEADLINE] != deadline_ns
                        or self._state[_ROLE_IO[self._role]]):
                    raise RuntimeError("shared embedding capacity unavailable")
                self._state[_ROLE_IO[self._role]] = 1
                waiting = True
            while not reserved:
                with _locked(self._guard, deadline_ns):
                    _check_deadline(deadline_ns)
                    if (self._global[_FAILED] or self._state[_CLOSING]
                            or self._state[_GENERATION] != self._generation
                            or self._state[_DEADLINE] != deadline_ns):
                        raise RuntimeError("shared embedding generation unavailable")
                    if self._global[_INFLIGHT] < 2:
                        self._state[_ROLE_ID[self._role]] = request_id
                        self._global[_INFLIGHT] += 1
                        self._global[_PEAK] = max(self._global[_PEAK], self._global[_INFLIGHT])
                        reserved = True
                        self._sequence = request_id
                if not reserved:
                    time.sleep(min(0.001, max(0.0,
                        (deadline_ns - time.monotonic_ns()) / 1_000_000_000)))
            # No expiry or I/O failure refunds a reserved slot. The model owns
            # completion accounting, and the parent owns confirmed process exit.
            _check_deadline(deadline_ns)
            self._connection.send_bytes(frame)
            remaining = (deadline_ns - time.monotonic_ns()) / 1_000_000_000
            if remaining <= 0 or not self._connection.poll(remaining):
                self._unsettled = True
                raise TimeoutError("embedding deadline expired")
            response = self._connection.recv_bytes(MAX_RESPONSE_BYTES)
            try:
                vector = _decode_response(response, self._nonce, self._generation,
                                          self._role, request_id)
            except TimeoutError:
                # An exact expired response is a completion, unlike a poll timeout.
                self._wait_completion(request_id, deadline_ns)
                received = True
                raise
            self._wait_completion(request_id, deadline_ns)
            received = True
            _check_deadline(deadline_ns)
            return [vector]
        except TimeoutError:
            if reserved and not received:
                self._unsettled = True
            raise
        except (OSError, EOFError, ValueError, RuntimeError):
            if reserved:
                _try_mark_failed(self._global, self._guard)
                self._unsettled = True
            raise
        finally:
            try:
                if received or waiting and not reserved:
                    # No data was sent for a capacity waiter; it owns no global
                    # slot. Sent calls retain their role until real completion.
                    with _locked(self._guard):
                        if self._state[_GENERATION] == self._generation:
                            self._state[_ROLE_IO[self._role]] = 0
            finally:
                self._io_lock.release()


def _ack(control, action: str, nonce: str, generation: int, error: str = ""):
    control.send((SCHEMA, action, nonce, generation, not error, error))


def _serve_shared(control, state, slots, guard, nonce: str, startup_deadline: int,
                  embedder: Callable | None) -> None:
    """The only model process; each bound pipe belongs to one fixed slot."""
    registry: dict[int, dict] = {}
    try:
        with _locked(guard, startup_deadline):
            state[_OWNERS] = 1
            state[_OWNER_PEAK] = max(state[_OWNER_PEAK], 1)
        _check_deadline(startup_deadline)
        if embedder is None:
            from .daily_embedding_adapter import embed_bge_m3

            embedder = embed_bge_m3
        with _locked(guard, startup_deadline):
            state[_LOADS] += 1
        warm = embedder(["阿露记忆向量预热"])
        if type(warm) is not list or len(warm) != 1:
            raise ValueError("invalid embedding warm result")
        _validate_vector(warm[0])
        _check_deadline(startup_deadline)
        _ack(control, "ready", nonce, 0)
        while True:
            with _locked(guard):
                if state[_FAILED]:
                    raise RuntimeError("shared embedding failed")
            connections = [connection for node in registry.values()
                           for connection in node["endpoints"].values()]
            available = wait([control, *connections], timeout=0.01)
            # Commands acknowledge short state transitions, never a full EOF wait.
            if control in available:
                command = control.recv()
                if (type(command) is not tuple or len(command) != 5
                        or command[0] not in {"register", "deadline", "prepare", "settle", "stop"}
                        or command[1] != nonce or type(command[2]) is not int):
                    raise ValueError("invalid embedding control")
                action, _, number, deadline_ns, resources = command
                if action == "register":
                    if (not _positive_int(number) or number in registry
                            or type(resources) is not tuple or len(resources) != 2):
                        raise ValueError("invalid generation registration")
                    slot, endpoints = resources
                    if (type(slot) is not int or not 0 <= slot < len(slots)
                            or any(node["slot"] == slot for node in registry.values())
                            or type(endpoints) is not tuple or len(endpoints) != 2
                            or any(type(item) is not tuple or len(item) != 2
                                   or item[0] != role or not isinstance(item[1], _ConnectionBase)
                                   for item, role in zip(endpoints, _ROLES))):
                        raise ValueError("invalid generation resources")
                    _check_deadline(deadline_ns)
                    local = slots[slot]
                    with _locked(guard, deadline_ns):
                        if (local[_GENERATION] != number or local[_DEADLINE] != deadline_ns
                                or local[_CLOSING]):
                            raise ValueError("generation reservation changed")
                    registry[number] = {"slot": slot, "state": local,
                        "endpoints": dict(endpoints), "last_ids": dict.fromkeys(_ROLES, 0),
                        "settling": False}
                    _ack(control, action, nonce, number)
                elif action == "stop":
                    if number != 0 or resources is not None or registry or state[_INFLIGHT]:
                        raise ValueError("embedding stop before all generations settled")
                    _ack(control, action, nonce, 0)
                    return
                else:
                    node = registry.get(number)
                    if node is None or resources is not None:
                        raise ValueError("invalid generation control")
                    local = node["state"]
                    if deadline_ns:
                        _check_deadline(deadline_ns)
                    if action == "deadline":
                        with _locked(guard, deadline_ns):
                            if (local[_WRITER_IO] or local[_QUERY_IO] or local[_CLOSING]
                                    or local[_WRITER_ID] or local[_QUERY_ID]):
                                raise ValueError("unsettled deadline update")
                            local[_DEADLINE] = deadline_ns
                    elif action == "prepare":
                        with _locked(guard):
                            local[_CLOSING] = 1
                    else:
                        if not local[_CLOSING] or node["settling"]:
                            raise ValueError("generation not prepared to settle")
                        node["settling"] = True
                    _ack(control, action, nonce, number)
            for number, node in list(registry.items()):
                local, endpoints = node["state"], node["endpoints"]
                for role, connection in list(endpoints.items()):
                    if connection not in available:
                        continue
                    try:
                        frame = connection.recv_bytes(MAX_REQUEST_BYTES)
                    except EOFError:
                        with _locked(guard):
                            if not local[_CLOSING] or local[_ROLE_ID[role]]:
                                raise RuntimeError("unexpected or unsettled embedding EOF")
                            local[_ROLE_IO[role]] = 0
                        connection.close()
                        del endpoints[role]
                        continue
                    payload = _decode(frame, MAX_REQUEST_BYTES)
                    _identity(payload, nonce, number, role)
                    request_id = payload["request_id"]
                    if (set(payload) != _REQUEST_KEYS or payload["op"] != "embed"
                            or not _positive_int(payload["deadline_ns"])
                            or type(payload["text"]) is not str
                            or len(payload["text"].encode("utf-8")) > MAX_TEXT_BYTES
                            or request_id <= node["last_ids"][role]):
                        raise ValueError("invalid embedding request fields")
                    with _locked(guard):
                        if (payload["deadline_ns"] != local[_DEADLINE]
                                or local[_ROLE_ID[role]] != request_id
                                or not local[_ROLE_IO[role]] or not 1 <= state[_INFLIGHT] <= 2):
                            raise ValueError("unreserved embedding request")
                    node["last_ids"][role] = request_id
                    result = {key: payload[key] for key in ("schema", "owner_nonce", "generation",
                                                           "role", "request_id")}
                    result.update(op="result", ok=False)
                    fatal = False
                    if local[_CLOSING]:
                        result["error_code"] = "closed"
                    elif time.monotonic_ns() >= payload["deadline_ns"]:
                        result["error_code"] = "expired"
                    else:
                        try:
                            values = embedder([payload["text"]])
                            if type(values) is not list or len(values) != 1:
                                raise ValueError("invalid embedding model output")
                            vector = _validate_vector(values[0])
                            if time.monotonic_ns() >= payload["deadline_ns"]:
                                result["error_code"] = "expired"
                            else:
                                result.update(ok=True, vector=vector)
                        except Exception:
                            result["error_code"] = "model_error"
                            fatal = True
                    try:
                        connection.send_bytes(_json_frame(result, MAX_RESPONSE_BYTES))
                    except (OSError, EOFError):
                        if not local[_CLOSING]:
                            raise
                    with _locked(guard):
                        if local[_ROLE_ID[role]] != request_id:
                            raise ValueError("embedding completion identity changed")
                        local[_ROLE_ID[role]] = 0
                        state[_INFLIGHT] -= 1
                    if fatal:
                        raise RuntimeError("embedding model failed")
                if node["settling"] and not endpoints:
                    with _locked(guard):
                        if (local[_WRITER_IO] or local[_QUERY_IO]
                                or local[_WRITER_ID] or local[_QUERY_ID]):
                            raise RuntimeError("embedding settlement incomplete")
                        local[_GENERATION] = 0
                    del registry[number]
    except BaseException:
        _try_mark_failed(state, guard)
    finally:
        for node in registry.values():
            for connection in node["endpoints"].values():
                connection.close()
        control.close()
        if guard.acquire(False):
            try:
                state[_OWNERS] = 0
            finally:
                guard.release()

class SharedEmbeddingGeneration:
    """One slot retained until its workers and actual inference settle."""

    def __init__(self, owner, number: int, slot: int, clients: dict[str, SharedEmbeddingClient]):
        self._owner, self.number, self.slot, self.clients = owner, number, slot, clients
        self._state = owner._slots[slot]
        self._closing = self._closed = False
        self._prepare_task: asyncio.Task | None = None
        self._close_task: asyncio.Task | None = None
        self._prepare_command_started = self._settle_command_started = False

    @staticmethod
    def _observe(completed: asyncio.Task) -> None:
        if not completed.cancelled():
            completed.exception()

    @staticmethod
    def _task_failed(task: asyncio.Task | None) -> bool:
        return task is not None and task.done() and (
            task.cancelled() or task.exception() is not None)

    def _finish_after_join(self) -> bool:
        # This is endpoint-only cleanup based on actual process exit. It does
        # not restore a failed owner or authorize any business operation.
        owner = self._owner
        if not (owner._closed and owner._clean_close and owner._process_joined):
            return False
        for client in self.clients.values():
            client.close_parent_copy()
        self._closing = self._closed = True
        owner._generations.pop(self.number, None)
        return True

    async def set_deadline(self, deadline_ns: int) -> None:
        _check_deadline(deadline_ns)
        self._owner._ensure_available()
        async with self._owner._parent_state(deadline_ns):
            state = self._state
            if (self._owner._state[_FAILED] or self._closing
                    or state[_GENERATION] != self.number
                    or state[_WRITER_IO] or state[_QUERY_IO]
                    or state[_WRITER_ID] or state[_QUERY_ID] or state[_UPDATING]):
                raise RuntimeError("embedding generation has unsettled I/O")
            state[_UPDATING] = 1
        try:
            await self._owner._command("deadline", self.number, deadline_ns, None,
                                       deadline_ns=deadline_ns)
            for client in self.clients.values():
                client.bind_deadline(deadline_ns)
        finally:
            async with self._owner._parent_state(deadline_ns):
                state[_UPDATING] = 0

    async def prepare_close(self, *, deadline_ns: int | None = None) -> None:
        if self._closed or self._finish_after_join():
            return
        limit = deadline_ns if deadline_ns is not None else time.monotonic_ns() + 30_000_000_000
        await self._prepare_close(limit, allow_unsent_retry=deadline_ns is None)

    async def _prepare_close(self, limit: int, *, allow_unsent_retry: bool) -> None:
        if (allow_unsent_retry and self._task_failed(self._prepare_task)
                and not self._prepare_command_started and not self._owner._local_failed):
            # An expired business D may fail before any command was attempted.
            # Only explicit cleanup may replace that known-unsent task.
            self._prepare_task = None
        if self._prepare_task is None:
            self._closing = True
            self._prepare_task = asyncio.create_task(self._prepare_owned(limit))
            self._prepare_task.add_done_callback(self._observe)
        await asyncio.shield(self._prepare_task)

    async def _prepare_owned(self, deadline_ns: int) -> None:
        _check_deadline(deadline_ns)
        async with self._owner._parent_state(deadline_ns):
            if self._owner._state[_FAILED] or self._owner._local_failed:
                raise RuntimeError("shared embedding owner unavailable for preparation")
            if self._state[_GENERATION] != self.number:
                raise RuntimeError("embedding generation identity changed")
            self._state[_CLOSING] = 1
        # From here an error is conservatively unknown, even if transmission
        # itself failed; a cleanup call must never blindly replay the command.
        self._prepare_command_started = True
        await self._owner._command("prepare", self.number, deadline_ns, None,
                                   deadline_ns=deadline_ns)

    async def close(self, *, deadline_ns: int | None = None) -> None:
        if self._closed or self._finish_after_join():
            return
        prepare = self._prepare_task
        prepared_or_unsent = (prepare is None or prepare.done() and (
            not self._prepare_command_started
            or not prepare.cancelled() and prepare.exception() is None))
        if (deadline_ns is None and self._task_failed(self._close_task)
                and not self._settle_command_started and prepared_or_unsent
                and not self._owner._local_failed):
            self._close_task = None
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_owned(deadline_ns))
            self._close_task.add_done_callback(self._observe)
        await asyncio.shield(self._close_task)

    async def _close_owned(self, deadline_ns: int | None) -> None:
        limit = deadline_ns if deadline_ns is not None else time.monotonic_ns() + 30_000_000_000
        await self._prepare_close(limit, allow_unsent_retry=deadline_ns is None)
        for client in self.clients.values():
            client.close_parent_copy()
        self._settle_command_started = True
        await self._owner._command("settle", self.number, limit, None, deadline_ns=limit)
        # The command only starts settlement. Never hold the control pipe while
        # waiting for this generation's EOFs; the other slot must remain usable.
        while True:
            async with self._owner._parent_state(limit):
                if self._owner._state[_FAILED]:
                    raise RuntimeError("shared embedding owner failed during settlement")
                if self._state[_GENERATION] == 0:
                    break
                if self._state[_GENERATION] != self.number:
                    raise RuntimeError("embedding generation identity changed")
            _check_deadline(limit)
            await asyncio.sleep(0.001)
        self._closed = True
        self._owner._generations.pop(self.number, None)


class SharedEmbeddingOwner:
    """One async model owner and one or two fixed generation slots."""

    def __init__(self, *, embedder: Callable | None = None, max_generations: int = 1):
        if embedder is not None and not callable(embedder):
            raise ValueError("invalid embedding callable")
        if type(max_generations) is not int or max_generations not in {1, 2}:
            raise ValueError("shared embedding supports one or two generations")
        self._max_generations = max_generations
        self._context = multiprocessing.get_context("spawn")
        # Slot zero preserves the original public-test layout. Index 15 is only
        # a global observed peak; clients constructed with legacy 15-slot arrays
        # do not access it.
        self._state = self._context.RawArray("q", 16)
        self._slots = [self._state]
        if max_generations == 2:
            self._slots.append(self._context.RawArray("q", 15))
        self._guard = self._context.RLock()
        self._nonce, self._embedder = secrets.token_hex(32), embedder
        self._process: BaseProcess | None = None
        self._control: _ConnectionBase | None = None
        self._generations: dict[int, SharedEmbeddingGeneration] = {}
        self._registration_lock = asyncio.Lock()
        self._worker_pid: int | None = None
        self._close_task: asyncio.Task | None = None
        self._local_failed = False
        self._control_lock = asyncio.Lock()
        self._io_tasks: set[asyncio.Task] = set()
        self._started = self._closing = self._closed = self._clean_close = False
        self._process_joined = False

    @property
    def max_generations(self) -> int:
        return self._max_generations

    @property
    def worker_pid(self) -> int | None:
        return self._worker_pid

    @property
    def stats(self) -> dict:
        if not self._guard.acquire(False):
            return {"load_count": None, "generation_count": None, "current_generation": None,
                    "active_generation_count": None, "active_generations": None,
                    "active_generation_peak": None,
                    "ipc_inflight": None, "ipc_peak": None, "model_owner_count": None,
                    "model_owner_peak": None, "failed": True, "clean_close": False,
                    "stats_available": False}
        try:
            if (self._process is not None and self._process.exitcode is not None
                    and not self._closing and not self._closed):
                self._state[_FAILED] = 1
            state = list(self._state)
            active = sorted(slot[_GENERATION] for slot in self._slots if slot[_GENERATION])
        finally:
            self._guard.release()
        return {"load_count": state[_LOADS], "generation_count": state[_GENERATIONS],
                "current_generation": (active[0] if len(active) == 1 else None if active else 0),
                "active_generation_count": len(active), "active_generations": active,
                "active_generation_peak": state[15],
                "ipc_inflight": state[_INFLIGHT], "ipc_peak": state[_PEAK],
                "model_owner_count": state[_OWNERS], "model_owner_peak": state[_OWNER_PEAK],
                "failed": bool(state[_FAILED]) or self._local_failed,
                "clean_close": self._clean_close, "stats_available": True}

    def _mark_failed(self) -> None:
        self._local_failed = True
        _try_mark_failed(self._state, self._guard)

    @asynccontextmanager
    async def _parent_state(self, deadline_ns: int | None = None):
        limit = deadline_ns if deadline_ns is not None else time.monotonic_ns() + 5_000_000_000
        while not self._guard.acquire(False):
            if time.monotonic_ns() >= limit:
                self._mark_failed()
                raise RuntimeError("shared embedding accounting lock unavailable")
            await asyncio.sleep(min(0.001, max(0.0, (limit - time.monotonic_ns()) / 1_000_000_000)))
        try:
            yield
        finally:
            self._guard.release()

    def _ensure_available(self) -> None:
        if (not self._started or self._closing or self._closed or self._local_failed
                or self._process is None or not self._process.is_alive()):
            raise RuntimeError("shared embedding owner unavailable")

    def _exchange(self, action, number, deadline, resources, io_deadline):
        if action != "ready":
            self._control.send((action, self._nonce, number, deadline, resources))
        remaining = (io_deadline - time.monotonic_ns()) / 1_000_000_000
        if remaining <= 0 or not self._control.poll(remaining):
            raise TimeoutError("embedding control deadline expired")
        response = self._control.recv()
        if response != (SCHEMA, action, self._nonce, number, True, ""):
            raise RuntimeError("invalid embedding control response")

    async def _command(self, action, number, deadline, resources, *, deadline_ns=None):
        io_deadline = deadline_ns or time.monotonic_ns() + 30_000_000_000

        async def owned_exchange():
            async with self._control_lock:
                try:
                    _check_deadline(io_deadline)
                    await asyncio.to_thread(self._exchange, action, number, deadline,
                                            resources, io_deadline)
                except BaseException:
                    self._mark_failed()
                    raise

        task = asyncio.create_task(owned_exchange())
        self._io_tasks.add(task)

        def settled(completed):
            self._io_tasks.discard(completed)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(settled)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            self._mark_failed()
            raise

    async def start(self, deadline_ns: int) -> None:
        _check_deadline(deadline_ns)
        if deadline_ns - time.monotonic_ns() > 120_000_000_000:
            raise ValueError("embedding startup exceeds 120 seconds")
        if self._started or self._process is not None or self._closed:
            raise RuntimeError("embedding owner cannot be restarted")
        parent, child = self._context.Pipe()
        self._control = parent
        self._process = self._context.Process(target=_serve_shared,
            args=(child, self._state, self._slots, self._guard, self._nonce,
                  deadline_ns, self._embedder), name="amc-shared-embedding")
        try:
            self._process.start()
            self._worker_pid = self._process.pid
        except BaseException:
            self._mark_failed()
            parent.close()
            if self._process.pid is None:
                self._process.close()
                self._process = None
            raise
        finally:
            child.close()
        try:
            await self._command("ready", 0, 0, None, deadline_ns=deadline_ns)
            self._started = True
        except BaseException:
            await self.close()
            raise

    async def new_generation(self, deadline_ns: int) -> SharedEmbeddingGeneration:
        _check_deadline(deadline_ns)
        self._ensure_available()
        # Serialize only slot registration, not either user's actual operation.
        async with self._registration_lock:
            _check_deadline(deadline_ns)
            self._ensure_available()
            async with self._parent_state(deadline_ns):
                free = next((index for index, slot in enumerate(self._slots)
                             if not slot[_GENERATION]
                             and not any(gen.slot == index for gen in self._generations.values())), None)
                if free is None or self._state[_FAILED]:
                    raise RuntimeError("previous embedding generation is still owned")
                local = self._slots[free]
                if local[_WRITER_IO] or local[_QUERY_IO] or local[_WRITER_ID] or local[_QUERY_ID]:
                    raise RuntimeError("previous embedding I/O is unsettled")
                number = self._state[_GENERATIONS] + 1
                if not _positive_int(number):
                    raise RuntimeError("embedding generation sequence exhausted")
                self._state[_GENERATIONS] = number
                local[_GENERATION], local[_DEADLINE] = number, deadline_ns
                local[_CLOSING], local[_UPDATING] = 0, 1
                self._state[15] = max(self._state[15],
                                     sum(bool(slot[_GENERATION]) for slot in self._slots))
            clients: dict[str, SharedEmbeddingClient] = {}
            endpoints: list[tuple[str, _ConnectionBase]] = []
            try:
                for role in _ROLES:
                    client_connection, endpoint = self._context.Pipe()
                    endpoints.append((role, endpoint))
                    clients[role] = SharedEmbeddingClient(client_connection, local, self._guard,
                        self._nonce, number, role, deadline_ns, global_state=self._state)
                generation = SharedEmbeddingGeneration(self, number, free, clients)
                # Register ownership before any cancelable control wait.
                self._generations[number] = generation
                await self._command("register", number, deadline_ns, (free, tuple(endpoints)),
                                    deadline_ns=deadline_ns)
                return generation
            except BaseException:
                self._mark_failed()
                for client in clients.values():
                    client.close_parent_copy()
                raise
            finally:
                for _, registered_endpoint in endpoints:
                    registered_endpoint.close()
                async with self._parent_state(deadline_ns):
                    local[_UPDATING] = 0

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_owned())
        await asyncio.shield(self._close_task)

    async def _close_owned(self) -> None:
        if self._closed:
            return
        self._closing = True
        process = self._process
        cleanup_error = None
        try:
            if process is not None and process.is_alive():
                async with self._parent_state():
                    failed = self._local_failed or bool(self._state[_FAILED])
                if not failed:
                    results = await asyncio.gather(
                        *(generation.close() for generation in tuple(self._generations.values())),
                        return_exceptions=True)
                    if any(isinstance(result, BaseException) for result in results):
                        raise RuntimeError("shared generations did not settle")
                    await self._command("stop", 0, 0, None)
        except BaseException as error:
            cleanup_error = error
            self._mark_failed()
        finally:
            for generation in tuple(self._generations.values()):
                for client in generation.clients.values():
                    client.close_parent_copy()
            if process is not None:
                await asyncio.to_thread(process.join, 1.0)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5.0)
                if process.is_alive():
                    self._clean_close = False
                    raise RuntimeError("shared embedding process did not settle")
                self._process_joined = True
                process.close()
                self._process = None
            if self._control is not None:
                self._control.close()
            if self._io_tasks:
                await asyncio.gather(*tuple(self._io_tasks), return_exceptions=True)
            if not self._guard.acquire(False):
                self._mark_failed()
                raise RuntimeError("shared embedding close accounting remains unknown")
            try:
                # Only the confirmed owned process join permits clearing unknown RPCs.
                self._state[_INFLIGHT] = self._state[_OWNERS] = 0
                for slot in self._slots:
                    slot[_GENERATION] = 0
                    slot[_WRITER_ID] = slot[_QUERY_ID] = 0
                    slot[_WRITER_IO] = slot[_QUERY_IO] = 0
            finally:
                self._guard.release()
            self._generations.clear()
            self._closed = self._clean_close = True
        if isinstance(cleanup_error, asyncio.CancelledError):
            raise cleanup_error
