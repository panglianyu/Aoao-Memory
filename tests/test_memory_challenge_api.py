"""Public challenge HTTP contracts with explicitly fake memory dependencies."""

import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from kernel.raw_message_models import ConflictError
from system_memory_god.challenge_api import MAX_BODY_BYTES, create_app


TOKEN = "test-only-bearer-credential-" + "x" * 32
AUTH = {"Authorization": "Bearer " + TOKEN}
ADD = {"request_id": "request-1", "user_id": "user-1", "session_id": "session-1",
       "messages": [{"role": "user", "content": "Ada lives in Oslo.", "timestamp": 0}]}


class FakeRuntime:
    def __init__(self):
        self.calls = []
        self.error = None
        self.closed = False

    async def add(self, **values):
        self.calls.append(("add", values))
        if self.error is not None:
            raise self.error
        return {"success": True, **{key: values[key] for key in
                                   ("request_id", "user_id", "session_id")}}

    async def search(self, **values):
        self.calls.append(("search", values))
        if self.error is not None:
            raise self.error
        return [{"id": "original-1", "content": "Ada lives in Oslo."}]

    def health(self):
        return {"state": "ready", "version": "AMC-ENTRY-1.0", "secret": TOKEN}

    async def close(self):
        self.closed = True


def test_authentication_rejects_before_reading_body_or_calling_runtime():
    runtime = FakeRuntime()

    def unreadable_body():
        raise AssertionError("unauthenticated body must not be consumed")
        yield b"unused"

    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", content=unreadable_body())
        assert response.status_code == 401
        assert response.json() == {"detail": {"reason": "unauthorized"}}
        assert runtime.calls == []


def test_add_search_health_and_shutdown_follow_public_contract():
    runtime = FakeRuntime()
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH, json=ADD)
        assert response.status_code == 200
        assert response.json() == {"success": True, "request_id": "request-1",
                                   "user_id": "user-1", "session_id": "session-1"}
        query = "original question " * 40
        response = client.post("/search", headers=AUTH, json={
            "user_id": "user-1", "query": query, "top_k": 100, "options": ["A", "B"]})
        assert response.status_code == 200
        assert response.json() == {"data": [{"id": "original-1", "content": "Ada lives in Oslo."}]}
        assert runtime.calls[-1] == ("search", {"user_id": "user-1", "query": query, "top_k": 100})
        assert client.get("/health").json() == {"state": "ready", "version": "AMC-ENTRY-1.0"}
        assert client.get("/docs").status_code == 404
    assert runtime.closed


@pytest.mark.parametrize("update", [
    {"user_id": "x" * 257}, {"request_id": "bad\x00id"},
    {"messages": []}, {"messages": [{"role": "system", "content": "secret input"}]},
    {"messages": [{"role": "user", "content": "secret input", "timestamp": True}]},
    {"messages": [{"role": "user", "content": "x" * 81921}]},
    {"messages": [{"role": "user", "content": "x", "unexpected": "secret input"}]},
    {"scope": {"user_id": "foreign"}},
])
def test_invalid_add_is_rejected_without_echo_or_runtime_access(update):
    runtime = FakeRuntime()
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH, json={**ADD, **update})
        assert response.status_code == 422
        assert response.json() == {"detail": {"reason": "invalid_request"}}
        assert "secret input" not in response.text
        assert runtime.calls == []


@pytest.mark.parametrize("values", [
    {"query": "x", "user_id": "u", "top_k": True},
    {"query": "x" * 32769, "user_id": "u", "top_k": 1},
    {"query": "x", "user_id": "u", "top_k": 101},
    {"query": "x", "user_id": "u", "top_k": 1, "scope": "foreign"},
])
def test_invalid_search_never_reaches_runtime(values):
    runtime = FakeRuntime()
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        assert client.post("/search", headers=AUTH, json=values).status_code == 422
        assert runtime.calls == []


def test_streamed_body_limit_and_duplicate_json_fields_are_rejected():
    runtime = FakeRuntime()
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH,
                               content=iter([b" " * (MAX_BODY_BYTES // 2)] * 2 + [b"x"]))
        assert response.status_code == 413
        response = client.post("/add", headers=AUTH,
                               content=json.dumps(ADD)[:-1] + ',"user_id":"foreign"}')
        assert response.status_code == 422
        assert runtime.calls == []


@pytest.mark.parametrize(("error", "status", "reason"), [
    (ConflictError("private detail"), 409, "request_conflict"),
    (TimeoutError("private detail"), 504, "publication_deadline"),
    (RuntimeError("private detail"), 503, "unavailable"),
    (ValueError("private detail"), 422, "invalid_request"),
    (OSError("private detail"), 503, "unavailable"),
])
def test_runtime_failures_never_become_success_or_leak_error_text(error, status, reason):
    runtime = FakeRuntime()
    runtime.error = error
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH, json=ADD)
        assert response.status_code == status
        assert response.json() == {"detail": {"reason": reason}}
        assert "private detail" not in response.text


@pytest.mark.asyncio
async def test_second_memory_request_is_rejected_instead_of_queued():
    entered, finish = asyncio.Event(), asyncio.Event()
    runtime = FakeRuntime()
    original = runtime.add

    async def slow_add(**values):
        entered.set()
        await finish.wait()
        return await original(**values)

    runtime.add = slow_add
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, token=TOKEN)),
                                 base_url="http://test") as client:
        first = asyncio.create_task(client.post("/add", headers=AUTH, json=ADD))
        await asyncio.wait_for(entered.wait(), 2)
        try:
            response = await client.post("/search", headers=AUTH,
                                         json={"query": "Ada", "user_id": "u", "top_k": 1})
            assert response.status_code == 429
            assert response.json() == {"detail": {"reason": "busy"}}
        finally:
            finish.set()
            assert (await first).status_code == 200
