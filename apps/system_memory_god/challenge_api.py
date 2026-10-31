"""Independent competition HTTP boundary; no host boot or model loading."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hmac
import json
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from kernel.raw_message_models import ConflictError


ENTRY_VERSION = "AMC-ENTRY-1.2"
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_MESSAGE_CHARS = 81920
MAX_MESSAGES = 256


class _RequestError(ValueError):
    def __init__(self, status: int, reason: str):
        self.status, self.reason = status, reason
        super().__init__(reason)


def _error(status: int, reason: str) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
    if status == 429:
        headers = {"Retry-After": "1"}
    return JSONResponse({"detail": {"reason": reason}}, status_code=status, headers=headers)


def _text(value: object, maximum: int) -> bool:
    if type(value) is not str or not value.strip() or len(value) > maximum:
        return False
    try:
        value.encode("utf-8")
    except UnicodeError:
        return False
    return True


def _identity(value: object) -> bool:
    return _text(value, 256) and "\x00" not in value


def _fields(value: object, required: set[str], optional: set[str] = frozenset()) -> dict:
    if type(value) is not dict or not required <= value.keys() or value.keys() - required - optional:
        raise ValueError("invalid request")
    return value


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _nonfinite(_: str) -> None:
    raise ValueError("invalid number")


async def _body(request: Request) -> dict:
    encoding = request.headers.get("content-encoding", "identity").lower()
    if encoding != "identity":
        raise _RequestError(422, "invalid_request")
    length = request.headers.get("content-length")
    if length is not None:
        try:
            size = int(length)
        except ValueError:
            raise _RequestError(422, "invalid_request") from None
        if size < 0:
            raise _RequestError(422, "invalid_request")
        if size > MAX_BODY_BYTES:
            raise _RequestError(413, "body_too_large")
    chunks = bytearray()
    async for chunk in request.stream():
        if len(chunks) + len(chunk) > MAX_BODY_BYTES:
            raise _RequestError(413, "body_too_large")
        chunks.extend(chunk)
    try:
        value = json.loads(chunks.decode("utf-8"), object_pairs_hook=_unique_pairs,
                           parse_constant=_nonfinite)
    except (ValueError, UnicodeError, RecursionError):
        raise _RequestError(422, "invalid_request") from None
    if type(value) is not dict:
        raise _RequestError(422, "invalid_request")
    return value


def _add_values(value: dict) -> dict:
    _fields(value, {"request_id", "user_id", "session_id", "messages"})
    if not all(_identity(value[key]) for key in ("request_id", "user_id", "session_id")):
        raise ValueError("invalid identity")
    messages = value["messages"]
    if type(messages) is not list or not 1 <= len(messages) <= MAX_MESSAGES:
        raise ValueError("invalid messages")
    for message in messages:
        _fields(message, {"role", "content"}, {"timestamp"})
        if (type(message["role"]) is not str or message["role"] not in {"user", "assistant"}
                or not _text(message["content"], MAX_MESSAGE_CHARS)):
            raise ValueError("invalid message")
        stamp = message.get("timestamp")
        if stamp is not None and (type(stamp) is not int or not 0 <= stamp <= 253402300799999):
            raise ValueError("invalid timestamp")
    return value


def _search_values(value: dict) -> dict:
    _fields(value, {"user_id", "query", "top_k"}, {"options"})
    if (not _identity(value["user_id"]) or not _text(value["query"], MAX_MEMORY_QUERY_CHARS)
            or type(value["top_k"]) is not int or not 1 <= value["top_k"] <= 100):
        raise ValueError("invalid search")
    if "options" in value and (type(value["options"]) is not list or len(value["options"]) > 256
                               or any(not _text(item, 32768) for item in value["options"])):
        raise ValueError("invalid options")
    return {key: value[key] for key in ("user_id", "query", "top_k", "options") if key in value}


def _add_response(result: object, values: dict) -> dict:
    if (type(result) is not dict or set(result) != {"success", "request_id", "user_id", "session_id"}
            or result["success"] is not True
            or any(result[key] != values[key] for key in ("request_id", "user_id", "session_id"))):
        raise RuntimeError("invalid memory result")
    return result


def _search_response(items: object, top_k: int) -> dict:
    if type(items) is not list or len(items) > top_k:
        raise RuntimeError("invalid memory result")
    for item in items:
        if (type(item) is not dict or not {"id", "content"} <= item.keys()
                or item.keys() - {"id", "content", "created_at"}
                or not _identity(item["id"]) or not _text(item["content"], MAX_BODY_BYTES)
                or "created_at" in item and not _text(item["created_at"], 80)):
            raise RuntimeError("invalid memory result")
    return {"data": items}


def create_app(runtime, *, token: str) -> FastAPI:
    """Expose only the authenticated memory adapter, never the host console."""
    if (type(token) is not str or not 32 <= len(token) <= 4096 or not token.isascii()
            or any(char.isspace() or ord(char) < 33 or ord(char) > 126 for char in token)):
        raise ValueError("dedicated bearer credential required")
    token_bytes = token.encode("ascii")
    admission = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(title="Aoao Memory Challenge", version=ENTRY_VERSION, docs_url=None,
                  redoc_url=None, openapi_url=None, lifespan=lifespan)

    def authorized(request: Request) -> bool:
        headers = request.headers.getlist("authorization")
        if len(headers) != 1:
            return False
        scheme, separator, credential = headers[0].partition(" ")
        return (bool(separator) and scheme.lower() == "bearer"
                and hmac.compare_digest(credential.encode("utf-8"), token_bytes))

    async def invoke(request: Request, operation: str) -> JSONResponse:
        if not authorized(request):
            return _error(401, "unauthorized")
        if admission.locked() or runtime.busy:
            return _error(429, "busy")
        async with admission:
            try:
                deadline = runtime.request_deadline()
                try:
                    body = await asyncio.wait_for(_body(request), timeout=min(30, deadline - time.monotonic()))
                except (TimeoutError, asyncio.TimeoutError):
                    return _error(408, "body_timeout")
                if operation == "add":
                    values = _add_values(body)
                    result = _add_response(await runtime.add(**values, deadline=deadline), values)
                else:
                    values = _search_values(body)
                    result = _search_response(await runtime.search(**values, deadline=deadline), values["top_k"])
                return JSONResponse(result)
            except _RequestError as error:
                return _error(error.status, error.reason)
            except ConflictError:
                return _error(409, "request_conflict")
            except (TimeoutError, asyncio.TimeoutError):
                return _error(504, "publication_deadline")
            except ValueError:
                return _error(422, "invalid_request")
            except Exception:
                return _error(503, "unavailable")

    @app.post("/add")
    async def add(request: Request):
        return await invoke(request, "add")

    @app.post("/search")
    async def search(request: Request):
        return await invoke(request, "search")

    @app.get("/health")
    async def health():
        try:
            value = runtime.health()
            state = value.get("state") if type(value) is dict else None
        except Exception:
            state = None
        if type(state) is not str or state not in {"ready", "unavailable", "closing", "stopped"}:
            state = "unavailable"
        return JSONResponse({"state": state, "version": ENTRY_VERSION},
                            status_code=200 if state == "ready" else 503)

    return app
