"""One explicit real-storage/Windows-worker gate with no model or network calls."""

from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import socket
import tempfile
import time
import traceback
from types import SimpleNamespace

import httpx
import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("AMC_RUN_REAL_WORKER_FAKE") != "1",
    reason="real-worker gate requires explicit AMC_RUN_REAL_WORKER_FAKE=1",
)


def _fake_1024_embedding(texts: list[str]) -> list[list[float]]:
    """Picklable Windows spawn target; never constructs a model or HTTP client."""
    return [[1.0] + [0.0] * 1023 for _ in texts]


class _EmptyStructuredClient:
    def __init__(self):
        self.chat = SimpleNamespace(completions=self)
        self.calls = 0
        self.closed = False

    async def create(self, **_kwargs):
        self.calls += 1
        await asyncio.sleep(0)
        content = json.dumps({key: [] for key in (
            "entity", "graph", "rule", "journal", "schedule")})
        return SimpleNamespace(choices=[SimpleNamespace(
            finish_reason="stop", message=SimpleNamespace(content=content))])

    async def close(self):
        self.closed = True


def _deny_network(*_args, **_kwargs):
    raise RuntimeError("network disabled for the synthetic worker gate")


@pytest.mark.asyncio
async def test_real_workers_sixteen_add_then_search(monkeypatch):
    """Retire/recreate actual runtime generations across both complete waves."""
    from system_memory_god.challenge_api import create_app
    from system_memory_god.challenge_lanes import ChallengeLanes
    from system_memory_god.memory_factory import create_memory_service
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner

    assert os.environ.get("AMC_RUN_BOUNDED_LIVE") != "1"
    for key, value in {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                       "ANONYMIZED_TELEMETRY": "False", "TOKENIZERS_PARALLELISM": "false"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(socket.socket, "connect", _deny_network)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny_network)
    monkeypatch.setattr(socket, "create_connection", _deny_network)

    root = Path(tempfile.mkdtemp(prefix="w-", dir=tempfile.gettempdir()))
    report_path = root / "worker-report.json"
    started = time.monotonic()
    deadline = started + 300.0
    client = _EmptyStructuredClient()
    embedding = SharedEmbeddingOwner(embedder=_fake_1024_embedding, max_generations=2)
    services = []
    active = [None, None]
    seen_pids: set[int] = set()
    rows = []
    report = {"schema": "amc-v16-real-workers-fake-model-v1", "passed": False,
              "budget_seconds": 300, "budget_scope": "cooperative timeout; ownership cleanup may outlive it",
              "parent_socket_guard": True, "os_network_isolation": False,
              "worker_network_policy": "offline tokenizer and explicitly fake embedding/LLM dependencies",
              "real_model_loading_allowed": False,
              "scope": "real MemoryService/DailyMemoryRuntime/Chroma/Windows workers; ASGI only",
              "rows": rows, "first_failure": None, "failure_type": None,
              "cleanup_failures": [], "source_sha256": {}, "phases": []}
    project = Path(__file__).resolve().parents[1]
    for relative in ("apps/system_memory_god/challenge_admission.py",
                     "apps/system_memory_god/challenge_runtime.py",
                     "apps/system_memory_god/challenge_lanes.py",
                     "apps/system_memory_god/daily_runtime.py",
                     "apps/system_memory_god/storage/shared_embedding_owner.py"):
        report["source_sha256"][relative] = hashlib.sha256((project / relative).read_bytes()).hexdigest()

    def write_report():
        report["elapsed_seconds"] = round(time.monotonic() - started, 6)
        report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    def factory(profile, **kwargs):
        service = create_memory_service(profile, **kwargs)
        services.append(service)
        return service

    def phase(lane, _user, stage, begin, finish):
        report["phases"].append({"lane": lane, "ordinal": active[lane], "stage": stage,
                                 "seconds": round(finish - begin, 6)})

    runtime = ChallengeLanes(root, client=client, model="synthetic-local-model",
        embedding_owner=embedding, add_timeout=100.0, service_factory=factory, observer=phase)
    token = "synthetic-worker-gate-token-only-0000000000"
    app = create_app(runtime, token=token)
    users = [f"synthetic-user-{number:02d}" for number in range(16)]
    markers = [f"WORKER16-PRIVATE-MARKER-{number:02d}" for number in range(16)]

    def diagnostics():
        return [runtime.for_lane(lane).diagnostic() for lane in range(2)]

    def safe_trace(error):
        return [{"file": str(Path(frame.filename).relative_to(project)),
                 "function": frame.name, "line": frame.lineno}
                for frame in traceback.extract_tb(error.__traceback__)
                if Path(frame.filename).is_relative_to(project)]

    def service_error_codes():
        output = []
        for service in services:
            for error in service.runtime._close_errors:
                chain, seen = [], set()
                while error is not None and id(error) not in seen and len(chain) < 4:
                    seen.add(id(error))
                    chain.append({"type": type(error).__name__,
                                  "errno": getattr(error, "errno", None),
                                  "winerror": getattr(error, "winerror", None),
                                  "frames": safe_trace(error)})
                    error = error.__cause__ or error.__context__
                output.append(chain)
        return output

    for lane_id in range(2):
        lane = runtime.for_lane(lane_id)
        for operation in ("add", "search"):
            original = getattr(lane, operation)

            async def observe(*, _original=original, _lane=lane_id,
                              _operation=operation, **kwargs):
                number = users.index(kwargs["user_id"])
                active[_lane] = number + (16 if _operation == "search" else 0)
                try:
                    return await _original(**kwargs)
                finally:
                    for service in services:
                        try:
                            health = service.health()
                        except Exception as error:
                            report.setdefault("health_observer_errors", []).append(type(error).__name__)
                            continue
                        for key in ("reader_pid", "vector_pid", "semantic_reader_pid"):
                            if type(health.get(key)) is int:
                                seen_pids.add(health[key])

            monkeypatch.setattr(lane, operation, observe)

    async def issue(http, number, operation):
        ordinal = number + (16 if operation == "search" else 0)
        begin = time.monotonic()
        row = {"ordinal": ordinal, "operation": operation, "status": None,
               "response_valid": False, "error_type": None}
        rows.append(row)
        body = ({"user_id": users[number], "session_id": f"session-{number}",
                 "request_id": f"request-{number}",
                 "messages": [{"role": "user", "content": markers[number]}]}
                if operation == "add" else
                {"user_id": users[number], "query": markers[number], "top_k": 10})
        try:
            response = await http.post("/" + operation, json=body,
                                       headers={"Authorization": "Bearer " + token})
            row["status"] = response.status_code
            payload = response.json()
            if operation == "add":
                row["response_valid"] = (payload == {"success": True,
                    "user_id": users[number], "session_id": f"session-{number}",
                    "request_id": f"request-{number}"})
            elif response.status_code == 200:
                data = payload.get("data", [])
                row["response_valid"] = (bool(data)
                    and any(item.get("content") == markers[number] for item in data)
                    and all(other not in item.get("content", "") for item in data
                            for index, other in enumerate(markers) if index != number))
        except Exception as error:
            row["error_type"] = type(error).__name__
        finally:
            row["seconds"] = round(time.monotonic() - begin, 6)
            if (row["status"] != 200 or not row["response_valid"]) and report["first_failure"] is None:
                report["first_failure"] = {"request": dict(row), "lanes": diagnostics(),
                                           "embedding": embedding.stats,
                                           "service_error_codes": service_error_codes()}
                write_report()

    async def workload():
        await embedding.start(time.monotonic_ns() + 60_000_000_000)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                base_url="http://synthetic-local-worker", timeout=110.0) as http:
            for operation in ("add", "search"):
                await asyncio.gather(*(issue(http, number, operation) for number in range(16)))
                await runtime.wait_idle()
                await app.state.admission.wait_empty()
                write_report()
                if report["first_failure"] is not None:
                    break
        report["passed"] = (len(rows) == 32 and all(
            row["status"] == 200 and row["response_valid"] for row in rows))

    try:
        write_report()
        await asyncio.wait_for(workload(), timeout=240.0)
    except Exception as error:
        report["failure_type"] = type(error).__name__
        report["failure_frames"] = safe_trace(error)
        if report["first_failure"] is None:
            report["first_failure"] = {"request": None, "lanes": diagnostics(),
                                       "embedding": embedding.stats}
    finally:
        report["before_close"] = {"lanes": diagnostics(), "embedding": embedding.stats}
        for label, close in (("admission", app.state.admission.close),
                             ("runtime", runtime.close), ("embedding", embedding.close),
                             ("client", client.close)):
            try:
                await asyncio.wait_for(close(), timeout=max(0.001, deadline - time.monotonic()))
            except Exception as error:
                report["cleanup_failures"].append({"owner": label, "type": type(error).__name__,
                                                   "frames": safe_trace(error)})
        report["after_close"] = {"lanes": diagnostics(), "embedding": embedding.stats}
        report["worker_pids_seen"] = sorted(seen_pids)
        report["direct_children_remaining"] = [child.pid for child in multiprocessing.active_children()]
        report["services_created"] = len(services)
        report["fake_llm_calls"] = client.calls
        report["fake_client_closed"] = client.closed
        write_report()
        print("worker report:", report_path)

    assert report["passed"], f"real-worker wave failed; safe diagnostics: {report_path}"
    assert not report["cleanup_failures"], f"cleanup failed; safe diagnostics: {report_path}"
    assert not report["direct_children_remaining"], f"children remain; safe diagnostics: {report_path}"
    assert report["after_close"]["embedding"]["clean_close"] is True
    assert len(seen_pids) >= 2 and len(services) >= 16
    assert client.calls == 16
