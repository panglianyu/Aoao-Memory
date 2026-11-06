"""Public challenge HTTP contracts with explicitly fake memory dependencies."""

import asyncio
import json
import time

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
        self._idle = asyncio.Event()
        self._idle.set()
        self._busy = False
        self.timeout = 2
        self.deadline = None
        self.received_deadlines = []

    def request_deadline(self):
        self.deadline = time.monotonic() + self.timeout
        return self.deadline

    @property
    def busy(self):
        return self._busy

    @busy.setter
    def busy(self, value):
        self._busy = value
        if value:
            self._idle.clear()
        else:
            self._idle.set()

    async def wait_idle(self):
        await self._idle.wait()

    async def add(self, **values):
        self.received_deadlines.append(values.pop("deadline"))
        self.calls.append(("add", values))
        if self.error is not None:
            raise self.error
        return {"success": True, **{key: values[key] for key in
                                   ("request_id", "user_id", "session_id")}}

    async def search(self, **values):
        self.received_deadlines.append(values.pop("deadline"))
        self.calls.append(("search", values))
        if self.error is not None:
            raise self.error
        return [{"id": "original-1", "content": "Ada lives in Oslo."}]

    def health(self):
        return {"state": "ready", "version": "AMC-ENTRY-1.6", "secret": TOKEN}

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
        assert runtime.calls[-1] == ("search", {"user_id": "user-1", "query": query,
                                               "top_k": 100, "options": ["A", "B"]})
        assert client.get("/health").json() == {"state": "ready", "version": "ARU-RECOVERY-2"}
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


@pytest.mark.parametrize("message", [
    {"role": "user", "content": "timestamp contract"},
    {"role": "user", "content": "timestamp contract", "timestamp": None},
    {"role": "user", "content": "timestamp contract", "timestamp": 0},
    {"role": "user", "content": "timestamp contract", "timestamp": 253402300799999},
])
def test_add_accepts_missing_null_or_integer_timestamp_without_conversion(message):
    runtime = FakeRuntime()
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH, json={**ADD, "messages": [message]})
        assert response.status_code == 200
        assert runtime.calls == [("add", {**ADD, "messages": [message]})]


@pytest.mark.parametrize("timestamp", [0.0, 1.5, True, False, -1, 253402300800000])
def test_invalid_timestamp_is_rejected_before_runtime(timestamp):
    runtime = FakeRuntime()
    message = {"role": "user", "content": "private timestamp input", "timestamp": timestamp}
    with TestClient(create_app(runtime, token=TOKEN)) as client:
        response = client.post("/add", headers=AUTH, json={**ADD, "messages": [message]})
        assert response.status_code == 422
        assert response.json() == {"detail": {"reason": "invalid_request"}}
        assert "private timestamp input" not in response.text
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
async def test_second_memory_request_waits_without_busy_rejection():
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
            second = asyncio.create_task(client.post("/search", headers=AUTH,
                json={"query": "Ada", "user_id": "u", "top_k": 1}))
            await asyncio.sleep(.01)
            assert not second.done()
        finally:
            finish.set()
            assert (await first).status_code == 200
        assert (await second).status_code == 200


@pytest.mark.asyncio
async def test_body_reading_and_runtime_receive_one_absolute_deadline():
    runtime = FakeRuntime()

    async def delayed_body():
        await asyncio.sleep(.02)
        yield json.dumps(ADD).encode()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, token=TOKEN)),
                                 base_url="http://test") as client:
        assert (await client.post("/add", headers=AUTH, content=delayed_body())).status_code == 200
    assert runtime.received_deadlines == [runtime.deadline]
    assert runtime.deadline - time.monotonic() < runtime.timeout - .01


@pytest.mark.asyncio
async def test_body_cannot_exceed_total_request_budget():
    runtime = FakeRuntime()
    runtime.timeout = .02

    async def stalled_body():
        await asyncio.Event().wait()
        yield b"unreachable"

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, token=TOKEN)),
                                 base_url="http://test") as client:
        response = await asyncio.wait_for(client.post("/add", headers=AUTH, content=stalled_body()), .5)
    assert response.status_code == 408 and not runtime.calls


@pytest.mark.asyncio
async def test_timed_out_owner_holds_execution_until_real_idle():
    runtime = FakeRuntime()

    async def timeout_add(**values):
        runtime.busy = True
        raise TimeoutError("owner still settling")

    runtime.add = timeout_add
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, token=TOKEN)),
                                 base_url="http://test") as client:
        assert (await client.post("/add", headers=AUTH, json=ADD)).status_code == 504
        values = {"query": "Ada", "user_id": "u", "top_k": 1}
        second = asyncio.create_task(client.post("/search", headers=AUTH, json=values))
        try:
            await asyncio.sleep(.01)
            assert not second.done() and not runtime.calls
        finally:
            runtime.busy = False
        assert (await second).status_code == 200


async def _until(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(poll(), 2)


@pytest.mark.asyncio
async def test_http_full_waves_keep_user_identity_and_reject_only_request_33():
    runtime = FakeRuntime()
    app = create_app(runtime, token=TOKEN)
    entered, release = asyncio.Event(), asyncio.Event()
    original = runtime.add

    async def slow_add(**values):
        entered.set()
        await release.wait()
        return await original(**values)

    runtime.add = slow_add
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as client:
            for wave in range(2):
                entered.clear()
                release.clear()
                payloads = [{**ADD, "user_id": f"user-{wave}-{i}",
                             "request_id": f"request-{wave}-{i}"} for i in range(32)]
                tasks = [asyncio.create_task(client.post("/add", headers=AUTH, json=value))
                         for value in payloads]
                try:
                    await entered.wait()
                    await _until(lambda: app.state.admission.in_flight == 32)

                    async def unreadable():
                        raise AssertionError("overload must reject before body reading")
                        yield b"unreachable"

                    response = await client.post("/add", headers=AUTH, content=unreadable())
                    assert response.status_code == 429
                    assert response.headers["retry-after"] == "1"
                finally:
                    release.set()
                    responses = await asyncio.gather(*tasks)
                assert [response.status_code for response in responses] == [200] * 32
                assert [response.json()["user_id"] for response in responses] == [
                    value["user_id"] for value in payloads]
                await app.state.admission.wait_empty()
            assert [values["user_id"] for _, values in runtime.calls] == [
                f"user-{wave}-{i}" for wave in range(2) for i in range(32)]


@pytest.mark.asyncio
async def test_shutdown_covers_slow_http_body_before_it_can_enqueue():
    runtime = FakeRuntime()
    app = create_app(runtime, token=TOKEN)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    entered = asyncio.Event()

    async def stalled_body():
        entered.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as client:
        reading = asyncio.create_task(client.post("/add", headers=AUTH, content=stalled_body()))
        await entered.wait()
        assert app.state.admission.in_flight == 1
        await lifespan.__aexit__(None, None, None)
        response = await reading
    assert response.status_code == 503
    assert runtime.closed and not runtime.calls
    assert app.state.admission.in_flight == 0


@pytest.mark.asyncio
async def test_observed_queued_disconnect_cannot_start_memory_work():
    runtime = FakeRuntime()
    app = create_app(runtime, token=TOKEN)
    entered, release = asyncio.Event(), asyncio.Event()
    original = runtime.add

    async def slow_add(**values):
        entered.set()
        await release.wait()
        return await original(**values)

    runtime.add = slow_add
    incoming, sent = asyncio.Queue(), []
    await incoming.put({"type": "http.request", "more_body": False,
                        "body": json.dumps({"query": "Ada", "user_id": "gone", "top_k": 1}).encode()})
    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "method": "POST", "scheme": "http", "path": "/search", "raw_path": b"/search",
             "query_string": b"", "root_path": "", "server": ("test", 80),
             "client": ("test", 123), "headers": [(b"authorization", AUTH["Authorization"].encode())]}

    async def send(message):
        sent.append(message)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as client:
            first = asyncio.create_task(client.post("/add", headers=AUTH, json=ADD))
            await entered.wait()
            disconnected = asyncio.create_task(app(scope, incoming.get, send))
            try:
                await _until(lambda: app.state.admission.queued == 1)
                await incoming.put({"type": "http.disconnect"})
                await disconnected
                assert app.state.admission.in_flight == 1
                assert app.state.admission.queued == 0
                assert [message["status"] for message in sent
                        if message["type"] == "http.response.start"] == [408]
            finally:
                release.set()
                assert (await first).status_code == 200
    assert [operation for operation, _ in runtime.calls] == ["add"]
