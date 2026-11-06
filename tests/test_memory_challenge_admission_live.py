"""The one enabled two-lane real gate; never a public-tunnel or Full test."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import importlib.util
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import sys
import threading
import time

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("AMC_RUN_BOUNDED_LIVE") != "1",
    reason="real-model gate requires explicit AMC_RUN_BOUNDED_LIVE=1",
)

STATE_ROOT = Path("G:/amc26/validation-1.6")
API_CONFIG = Path("G:/Aoao_OS_v0.1/config/api.yaml")
WAVES = (("add", 16), ("search", 16))
REQUEST_SECONDS = 100.0
RUNNER_SECONDS = 600.0
MIN_AVAILABLE_BYTES = 2 * 1024**3
MAX_RUNNER_RSS_BYTES = 6 * 1024**3
_PHASES = frozenset({"retire", "factory", "start", "add", "published", "search"})


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class _GateBudgetExceeded(TimeoutError):
    pass


class _SampledResourceStop(RuntimeError):
    pass


class _GateBudget:
    def __init__(self, started: float) -> None:
        self.deadline = started + RUNNER_SECONDS

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise _GateBudgetExceeded("bounded runner budget exhausted")
        return remaining


@dataclass
class _Observation:
    ordinal: int
    wave: int
    operation: str = "add"
    lane_id: int | None = None
    http_start: float = 0.0
    runtime_enter: float | None = None
    runtime_return: float | None = None
    owner_settled: float | None = None
    http_finish: float | None = None
    status: int | None = None
    response_valid: bool = False
    error_type: str | None = None
    phases: list[dict] = field(default_factory=list)

    def summary(self) -> dict:
        def elapsed(finish: float | None, start: float | None) -> float | None:
            if finish is None or start is None:
                return None
            return round(max(0.0, finish - start), 6)

        return {
            "ordinal": self.ordinal,
            "wave": self.wave,
            "operation": self.operation,
            "lane_id": self.lane_id,
            "http_status": self.status,
            "response_valid": self.response_valid,
            "error_type": self.error_type,
            "http_elapsed_seconds": elapsed(self.http_finish, self.http_start),
            "pre_runtime_seconds_including_body": elapsed(self.runtime_enter, self.http_start),
            "runtime_wait_seconds": elapsed(self.runtime_return, self.runtime_enter),
            "owner_tail_seconds": elapsed(self.owner_settled, self.runtime_return),
            "owner_elapsed_seconds": elapsed(self.owner_settled, self.runtime_enter),
            "owner_end_to_end_seconds": elapsed(self.owner_settled, self.http_start),
            "runtime_entered": self.runtime_enter is not None,
            "owner_idle_observed": self.owner_settled is not None,
            "phases": list(self.phases),
        }

    def passed(self) -> bool:
        return (
            self.status == 200 and self.response_valid and self.error_type is None
            and self.http_finish is not None
            and self.http_finish - self.http_start <= REQUEST_SECONDS
            and self.owner_settled is not None
            and self.owner_settled - self.http_start <= REQUEST_SECONDS
        )


class _Trace:
    """Observe each concrete lane's public owner, not the global idle facade."""

    def __init__(self) -> None:
        self.requests: dict[str, _Observation] = {}
        self.max_unsettled_runtime_requests = 0
        self.lane_owner_peaks = {0: 0, 1: 0}
        self.orphan_phase_count = 0
        self.cleanup_phases: list[dict] = []

    def entered(self, request_id: str, lane_id: int) -> None:
        if type(lane_id) is not int or lane_id not in self.lane_owner_peaks:
            raise ValueError("invalid observed lane")
        item = self.requests[request_id]
        item.lane_id, item.runtime_enter = lane_id, time.monotonic()
        pending = [
            value for value in self.requests.values()
            if value.runtime_enter is not None and value.owner_settled is None
        ]
        self.max_unsettled_runtime_requests = max(self.max_unsettled_runtime_requests, len(pending))
        self.lane_owner_peaks[lane_id] = max(
            self.lane_owner_peaks[lane_id], sum(value.lane_id == lane_id for value in pending),
        )

    def pending_ids(self, lane_id: int) -> tuple[str, ...]:
        return tuple(
            key for key, item in self.requests.items()
            if item.lane_id == lane_id
            and item.runtime_enter is not None and item.owner_settled is None
        )

    def settled(self, request_ids: tuple[str, ...]) -> None:
        stamp = time.monotonic()
        for key in request_ids:
            item = self.requests[key]
            if item.owner_settled is None:
                item.owner_settled = stamp

    def phase(self, lane_id: int, user_id: str, stage: str,
              started: float, finished: float) -> None:
        # The user argument is deliberately not retained or written to the report.
        if (type(lane_id) is not int or lane_id not in self.lane_owner_peaks
                or stage not in _PHASES
                or any(type(value) not in {int, float} or not math.isfinite(value)
                       for value in (started, finished))
                or finished < started):
            raise ValueError("invalid phase observation")
        pending = self.pending_ids(lane_id)
        if not pending and stage == "retire":
            if len(self.cleanup_phases) >= 8:
                raise ValueError("invalid cleanup phase observation count")
            self.cleanup_phases.append({
                "lane_id": lane_id, "stage": stage,
                "elapsed_seconds": round(finished - started, 6),
            })
            return
        if len(pending) != 1:
            self.orphan_phase_count += 1
            return
        phases = self.requests[pending[0]].phases
        if len(phases) >= 16:
            raise ValueError("invalid phase observation count")
        phases.append({"stage": stage, "elapsed_seconds": round(finished - started, 6)})

    def concurrency_passed(self) -> bool:
        return (
            self.max_unsettled_runtime_requests == 2
            and all(value <= 1 for value in self.lane_owner_peaks.values())
        )


class _MemoryObservations:
    """Sampled stop rules only; these are not OS memory isolation or hard peaks."""

    def __init__(self) -> None:
        self._sample_lock = threading.Lock()
        self.samples = 0
        self.observed_peak_tree_rss_bytes: int | None = None
        self.observed_min_machine_available_bytes: int | None = None
        self.unavailable_type: str | None = None
        self.stop_reason: str | None = None
        try:
            import psutil
        except Exception as error:
            self.psutil = None
            self.unavailable_type = type(error).__name__
            self.stop_reason = "sampling_unavailable"
        else:
            self.psutil = psutil

    def sample(self) -> None:
        # The periodic sampler and wave precheck may otherwise race updates.
        with self._sample_lock:
            self._sample_owned()

    def _sample_owned(self) -> None:
        if self.psutil is None:
            return
        try:
            process = self.psutil.Process()
            processes = [process, *process.children(recursive=True)]
            rss = 0
            for member in processes:
                try:
                    rss += member.memory_info().rss
                except self.psutil.NoSuchProcess:
                    continue
            available = self.psutil.virtual_memory().available
        except (self.psutil.Error, OSError, RuntimeError, ValueError) as error:
            self.unavailable_type = type(error).__name__
            self.stop_reason = self.stop_reason or "sampling_unavailable"
            return
        self.samples += 1
        self.observed_peak_tree_rss_bytes = max(self.observed_peak_tree_rss_bytes or 0, rss)
        previous = self.observed_min_machine_available_bytes
        self.observed_min_machine_available_bytes = (
            available if previous is None else min(previous, available)
        )
        if available < MIN_AVAILABLE_BYTES:
            self.stop_reason = self.stop_reason or "sampled_available_below_2gib"
        if rss > MAX_RUNNER_RSS_BYTES:
            self.stop_reason = self.stop_reason or "sampled_runner_rss_above_6gib"

    async def run(self, stop: asyncio.Event) -> None:
        if self.psutil is None:
            return
        while not stop.is_set():
            await asyncio.to_thread(self.sample)
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
        await asyncio.to_thread(self.sample)

    def summary(self) -> dict:
        with self._sample_lock:
            return {
                "samples": self.samples,
                "observed_peak_runner_and_descendants_rss_bytes": self.observed_peak_tree_rss_bytes,
                "observed_min_machine_available_bytes": self.observed_min_machine_available_bytes,
                "unavailable_type": self.unavailable_type,
                "stop_reason": self.stop_reason,
                "stop_limits": {
                    "minimum_machine_available_bytes": MIN_AVAILABLE_BYTES,
                    "maximum_runner_and_descendants_rss_bytes": MAX_RUNNER_RSS_BYTES,
                },
                "scope": "sampled runner/descendants; machine availability includes unrelated activity",
            }


def _load_cli():
    path = Path(__file__).resolve().parents[1] / "Tools" / "serve_memory_challenge.py"
    spec = importlib.util.spec_from_file_location("amc_v16_live_cli", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("candidate CLI unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_SHARED_COUNTERS = (
    "load_count", "generation_count", "current_generation", "ipc_inflight", "ipc_peak",
    "model_owner_count", "model_owner_peak", "clean_close", "failed",
    "active_generation_count", "active_generations", "active_generation_peak",
)


def _shared_observations(embedding_owner) -> dict | None:
    if embedding_owner is None:
        return None
    actual = embedding_owner.stats
    if type(actual) is not dict:
        raise ValueError("invalid shared observation counters")
    result = {key: actual.get(key) for key in _SHARED_COUNTERS}
    for key, value in result.items():
        if key in {"clean_close", "failed"}:
            valid = type(value) is bool
        elif key == "current_generation" and value is None:
            valid = True
        elif key == "active_generations":
            valid = (
                type(value) is list and len(value) <= 2
                and all(type(number) is int and number > 0 for number in value)
                and len(set(value)) == len(value)
            )
        else:
            valid = type(value) is int and value >= 0
        if not valid:
            raise ValueError("invalid shared observation counters")
    if len(result["active_generations"]) != result["active_generation_count"]:
        raise ValueError("invalid shared observation counters")
    return result


def _shared_passed(stats: dict | None, *, closed: bool,
                   generations: int | None = None) -> bool:
    """Owner cleanup is independent of how many workload requests were sent."""
    if type(stats) is not dict:
        return False
    generation_count = stats.get("generation_count")
    active = stats.get("active_generations")
    count = stats.get("active_generation_count")
    current = stats.get("current_generation")
    if (type(generation_count) is not int or generation_count < 0
            or generations is not None and generation_count != generations
            or type(count) is not int or not 0 <= count <= 2
            or type(active) is not list or len(active) != count
            or any(type(number) is not int or number <= 0 for number in active)
            or len(set(active)) != count):
        return False
    if current != (active[0] if count == 1 else 0 if count == 0 else None):
        return False
    expected = {
        "load_count": 1, "ipc_inflight": 0,
        "model_owner_count": 0 if closed else 1, "model_owner_peak": 1,
    }
    return (
        all(type(stats.get(key)) is int and stats[key] == value for key, value in expected.items())
        and type(stats.get("ipc_peak")) is int and 0 <= stats["ipc_peak"] <= 2
        and type(stats.get("active_generation_peak")) is int
        and 0 <= stats["active_generation_peak"] <= 2
        and stats.get("failed") is False
        and (not closed or count == 0 and stats.get("clean_close") is True)
    )


def _cleanup_passed(stats: dict | None) -> bool:
    """Confirm cleanup without requiring a successful workload or model run."""
    return (
        type(stats) is dict
        and all(type(stats.get(key)) is int and stats[key] == 0 for key in (
            "current_generation", "active_generation_count", "ipc_inflight", "model_owner_count",
        ))
        and stats.get("active_generations") == [] and stats.get("clean_close") is True
    )


def _memory_passed(summary: dict) -> bool:
    peak = summary.get("observed_peak_runner_and_descendants_rss_bytes")
    available = summary.get("observed_min_machine_available_bytes")
    return (
        type(summary.get("samples")) is int and summary["samples"] >= 2
        and type(peak) is int and 0 < peak <= MAX_RUNNER_RSS_BYTES
        and type(available) is int and available >= MIN_AVAILABLE_BYTES
        and summary.get("unavailable_type") is None
        and summary.get("stop_reason") is None
    )


def _search_valid(value: object, *, own_marker: str, all_markers: tuple[str, ...]) -> bool:
    if type(value) is not dict or set(value) != {"data"}:
        return False
    rows = value["data"]
    if type(rows) is not list or not 1 <= len(rows) <= 100:
        return False
    contents = []
    for row in rows:
        if (type(row) is not dict or not {"id", "content"} <= row.keys()
                or row.keys() - {"id", "content", "created_at"}
                or type(row["id"]) is not str or not row["id"].strip()
                or type(row["content"]) is not str or not row["content"].strip()
                or "created_at" in row and type(row["created_at"]) is not str):
            return False
        contents.append(row["content"])
    return (
        any(own_marker in content for content in contents)
        and not any(marker != own_marker and marker in content
                    for marker in all_markers for content in contents)
    )


def _observe_lane(lane, lane_id: int, trace: _Trace, request_keys: dict) -> None:
    original_add, original_search, original_idle = lane.add, lane.search, lane.wait_idle

    async def add(*, user_id, session_id, request_id, messages, deadline=None):
        trace.entered(request_id, lane_id)
        try:
            return await original_add(
                user_id=user_id, session_id=session_id, request_id=request_id,
                messages=messages, deadline=deadline,
            )
        finally:
            trace.requests[request_id].runtime_return = time.monotonic()

    async def search(*, user_id, query, top_k, options=(), deadline=None):
        request_id = request_keys[("search", user_id)]
        trace.entered(request_id, lane_id)
        try:
            return await original_search(
                user_id=user_id, query=query, top_k=top_k, options=options, deadline=deadline,
            )
        finally:
            trace.requests[request_id].runtime_return = time.monotonic()

    async def idle():
        request_ids = trace.pending_ids(lane_id)
        await original_idle()
        trace.settled(request_ids)

    lane.add, lane.search, lane.wait_idle = add, search, idle


def _write_report(path: Path, report: dict, trace: _Trace) -> None:
    report["requests"] = [item.summary() for item in trace.requests.values()]
    report["max_unsettled_runtime_requests"] = trace.max_unsettled_runtime_requests
    report["lane_owner_peaks"] = dict(trace.lane_owner_peaks)
    report["orphan_phase_count"] = trace.orphan_phase_count
    report["cleanup_phases"] = list(trace.cleanup_phases)
    # No request text, user IDs, provider settings, tokens or response bodies.
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(report, stream, ensure_ascii=True, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


@pytest.mark.asyncio
async def test_bounded_real_model_admission_wave() -> None:
    runner_started = time.monotonic()
    budget = _GateBudget(runner_started)
    import httpx
    from openai import AsyncOpenAI

    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"

    from system_memory_god.challenge_api import create_app
    from system_memory_god.challenge_lanes import ChallengeLanes
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner

    cli = _load_cli()
    root = cli._private_path(STATE_ROOT)
    if root.exists() or root.is_symlink():
        pytest.fail("live gate root already exists; refusing a second run", pytrace=False)
    root.mkdir(exist_ok=False)
    owner = cli._StartupOwner(cli._DataRootLease.acquire(root))
    try:
        cli._prepare_root(root)
    except BaseException:
        owner.release_if_empty()
        raise
    report_path = root / "live-report.json"
    report = {
        "schema": "amc-v16-bounded-live-v1",
        "started_at_utc": _utc(),
        "state": "starting",
        "waves": [{"operation": operation, "size": size} for operation, size in WAVES],
        "request_deadline_seconds": REQUEST_SECONDS,
        "runner_budget_seconds": RUNNER_SECONDS,
        "budget_exceeded": False,
        "max_sends": 32,
        "sent": 0,
        "completed_waves": [],
        "public_transport_tested": False,
        "full_capacity_proven": False,
        "scope": "real-model ASGI, 16 synthetic users/sessions, four two-message batches and long queries, two runtimes",
        "message_count": 20,
        "source_pins": {str(path): hashlib.sha256(
            (Path(__file__).resolve().parents[1] / path).read_bytes()).hexdigest() for path in (
            Path("apps/system_memory_god/challenge_runtime.py"),
            Path("apps/system_memory_god/storage/daily_snapshot_file_ops.py"),
            Path("tests/test_memory_challenge_admission_live.py"),
        )},
        "runtime_diagnostics": [],
        "timing_scope": (
            "HTTP and public per-lane owner boundaries include queue and body. "
            "Phase durations are runtime observer boundaries, not an internal API profiler. "
            "Shared-model startup is before the first HTTP and reported separately."
        ),
        "startup": {"limit_seconds": 120, "elapsed_seconds": None, "completed": False},
        "first_http_started_at_utc": None,
        "first_http_after_runner_seconds": None,
        "wave_workload_seconds": None,
        "total_runner_wall_seconds": None,
        "model_counter_scope": "managed shared owner, not an all-OS inventory or hard RAM bound",
        "shared_embedding": None,
        "shared_embedding_before_close": None,
        "failure_type": None,
        "cleanup_failure_type": None,
        "report_failure_type": None,
    }
    trace = _Trace()
    memory = _MemoryObservations()
    stop_sampling = asyncio.Event()
    await asyncio.to_thread(memory.sample)
    sampler = asyncio.create_task(memory.run(stop_sampling))
    failure = False
    workload_complete = False
    embedding_owner = None
    startup_began = workload_began = None

    async def budget_watchdog() -> None:
        await asyncio.sleep(max(0.0, budget.deadline - time.monotonic()))
        report["budget_exceeded"] = True
        report["state"] = "budget_exceeded_waiting_owned_cleanup"
        report["failure_type"] = report["failure_type"] or "_GateBudgetExceeded"
        # Accepted owners are still tracked; expiry never grants a replacement.
        try:
            _write_report(report_path, report, trace)
        except Exception as error:
            report["report_failure_type"] = type(error).__name__

    watchdog = asyncio.create_task(budget_watchdog())
    markers = tuple(f"ARUMEM16-{number:03d}-{secrets.token_hex(6)}" for number in range(16))
    user_ids = tuple(f"v16-synthetic-user-{number:03d}" for number in range(16))
    request_keys = {
        (operation, user): f"v16-{operation}-{number:03d}"
        for operation, _ in WAVES for number, user in enumerate(user_ids)
    }

    try:
        _write_report(report_path, report, trace)
        if memory.stop_reason is not None:
            raise _SampledResourceStop("sampled resource stop before model startup")
        api = cli._api_settings(API_CONFIG)
        owner.client = AsyncOpenAI(
            api_key=api["api_key"], base_url=api["base_url"], max_retries=0, timeout=60.0,
        )
        embedding_owner = SharedEmbeddingOwner(max_generations=2)
        owner.embedding_owner = embedding_owner
        startup_began = time.monotonic()
        report["startup"]["started_at_utc"] = _utc()
        _write_report(report_path, report, trace)
        startup_limit = min(120.0, budget.remaining())
        await embedding_owner.start(time.monotonic_ns() + int(startup_limit * 1_000_000_000))
        report["startup"]["elapsed_seconds"] = round(time.monotonic() - startup_began, 6)
        report["startup"]["completed"] = True
        report["shared_embedding"] = _shared_observations(embedding_owner)
        owner.runtime = ChallengeLanes(
            root, client=owner.client, model=api["model_name"], add_timeout=REQUEST_SECONDS,
            embedding_owner=embedding_owner, observer=trace.phase,
        )
        for lane_id in range(2):
            _observe_lane(owner.runtime.for_lane(lane_id), lane_id, trace, request_keys)
        bearer = secrets.token_urlsafe(32)
        app = create_app(owner.runtime, token=bearer)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://amc-bounded-live",
                timeout=REQUEST_SECONDS,
            ) as client:
                async def issue(number: int, wave: int, operation: str) -> None:
                    if memory.stop_reason is not None or report["budget_exceeded"]:
                        return
                    timeout = min(REQUEST_SECONDS, budget.remaining())
                    if report["sent"] >= 32:
                        raise RuntimeError("live gate send limit exceeded")
                    user_id = user_ids[number]
                    request_id = request_keys[(operation, user_id)]
                    session_id = f"v16-synthetic-session-{number:03d}"
                    item = _Observation(
                        number + (16 if operation == "search" else 0), wave,
                        operation=operation, http_start=time.monotonic(),
                    )
                    if report["first_http_started_at_utc"] is None:
                        report["first_http_started_at_utc"] = _utc()
                        report["first_http_after_runner_seconds"] = round(
                            item.http_start - runner_started, 6,
                        )
                    trace.requests[request_id] = item
                    if operation == "add":
                        body = {
                            "request_id": request_id, "user_id": user_id, "session_id": session_id,
                            "messages": [{
                                "role": "user", "timestamp": 0,
                                "content": f"My unique stored marker is {markers[number]}. I prefer tea.",
                            }],
                        }
                        if number % 4 == 0:
                            body["messages"].append({"role": "assistant", "timestamp": 1,
                                "content": "I will remember this preference for future conversations."})
                    else:
                        body = {
                            "user_id": user_id, "query": (
                                "Please use only evidence from my own past conversations. "
                                "I am asking about the unique marker that I explicitly asked you to remember, "
                                "rather than another person's marker or a newly invented value. "
                                "Consider the stored original message and return that evidence. "
                                "What is my unique stored marker?"),
                            "top_k": 100,
                        }
                    report["sent"] += 1
                    try:
                        response = await asyncio.wait_for(
                            client.post(
                                "/" + operation, json=body,
                                headers={"Authorization": "Bearer " + bearer},
                            ), timeout=timeout,
                        )
                        item.status = response.status_code
                        if response.status_code == 200:
                            value = response.json()
                            if operation == "add":
                                item.response_valid = value == {
                                    "success": True, "request_id": request_id,
                                    "user_id": user_id, "session_id": session_id,
                                }
                            else:
                                item.response_valid = _search_valid(
                                    value, own_marker=markers[number], all_markers=markers,
                                )
                    except Exception as error:
                        item.error_type = type(error).__name__
                    finally:
                        item.http_finish = time.monotonic()

                workload_began = time.monotonic()
                for wave, (operation, size) in enumerate(WAVES, start=1):
                    await asyncio.to_thread(memory.sample)
                    if memory.stop_reason is not None:
                        raise _SampledResourceStop("sampled resource stop before next wave")
                    # Leave the original 100 seconds intact and normal cleanup room.
                    if budget.remaining() < REQUEST_SECONDS + 60:
                        raise _GateBudgetExceeded("insufficient runner budget for a full wave")
                    report["state"] = "running"
                    report["active_wave"] = {"number": wave, "operation": operation, "size": size}
                    _write_report(report_path, report, trace)
                    await asyncio.gather(*(issue(number, wave, operation) for number in range(size)))
                    report["state"] = "settling"
                    _write_report(report_path, report, trace)
                    await owner.runtime.wait_idle()
                    completed = [item for item in trace.requests.values() if item.wave == wave]
                    report["shared_embedding"] = _shared_observations(embedding_owner)
                    report["memory"] = memory.summary()
                    wave_passed = (
                        len(completed) == size and all(item.passed() for item in completed)
                        and trace.concurrency_passed()
                        and _shared_passed(report["shared_embedding"], closed=False)
                        and report["shared_embedding"]["active_generation_peak"] == 2
                        and memory.stop_reason is None and not report["budget_exceeded"]
                    )
                    report["completed_waves"].append({
                        "number": wave, "operation": operation, "size": size,
                        "sent": len(completed), "passed": wave_passed,
                    })
                    _write_report(report_path, report, trace)
                    if not wave_passed:
                        failure = True
                        report["failure_type"] = (
                            "_SampledResourceStop" if memory.stop_reason is not None
                            else "_GateBudgetExceeded" if report["budget_exceeded"] else "WaveGateFailed"
                        )
                        break
                workload_complete = (
                    not failure and len(report["completed_waves"]) == 2 and report["sent"] == 32
                )
                report["wave_workload_seconds"] = round(time.monotonic() - workload_began, 6)
    except BaseException as error:
        failure = True
        report["failure_type"] = type(error).__name__
        if not isinstance(error, Exception):
            raise
    finally:
        report["state"] = "closing"
        if owner.runtime is not None:
            report["runtime_diagnostics"] = [owner.runtime.for_lane(lane).diagnostic()
                                             for lane in range(2)]
        if startup_began is not None and report["startup"]["elapsed_seconds"] is None:
            report["startup"]["elapsed_seconds"] = round(time.monotonic() - startup_began, 6)
        if workload_began is not None and report["wave_workload_seconds"] is None:
            report["wave_workload_seconds"] = round(time.monotonic() - workload_began, 6)
        try:
            report["shared_embedding_before_close"] = _shared_observations(embedding_owner)
            _write_report(report_path, report, trace)
        except Exception as error:
            failure = True
            report["report_failure_type"] = type(error).__name__
        try:
            await owner.close()
        except Exception as error:
            failure = True
            report["cleanup_failure_type"] = type(error).__name__
        finally:
            try:
                owner.release_if_empty()
            except Exception as error:
                failure = True
                report["cleanup_failure_type"] = type(error).__name__
            stop_sampling.set()
            try:
                await sampler
            except Exception as error:
                failure = True
                memory.unavailable_type = type(error).__name__
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)
            report["memory"] = memory.summary()
            try:
                report["shared_embedding"] = _shared_observations(embedding_owner)
            except Exception as error:
                failure = True
                report["report_failure_type"] = type(error).__name__
            report["total_runner_wall_seconds"] = round(time.monotonic() - runner_started, 6)
            report["budget_exceeded"] = (
                report["budget_exceeded"] or report["total_runner_wall_seconds"] > RUNNER_SECONDS
            )
            stats = report["shared_embedding"]
            report["gate_checks"] = {
                "startup_within_limit": (
                    report["startup"]["completed"]
                    and report["startup"]["elapsed_seconds"] <= 120
                ),
                "all_request_waves_passed": workload_complete,
                "two_lanes_observed_and_bounded": trace.concurrency_passed(),
                "two_generations_observed": (
                    type(stats) is dict and stats.get("active_generation_peak") == 2
                ),
                "owner_cleanup_confirmed": _cleanup_passed(stats),
                "single_model_and_rpc_bounds": _shared_passed(stats, closed=True),
                "memory_observations_valid_and_within_sampled_limits": _memory_passed(report["memory"]),
                "runner_within_600_seconds": not report["budget_exceeded"],
                "phase_observations_bound_to_a_request": trace.orphan_phase_count == 0,
            }
            if not all(report["gate_checks"].values()):
                failure = True
                if report["failure_type"] is None:
                    report["failure_type"] = "FinalObservationGateFailed"
            report["completed_at_utc"] = _utc()
            report["state"] = "passed" if workload_complete and not failure else "failed"
            try:
                _write_report(report_path, report, trace)
            except Exception as error:
                failure = True
                report["report_failure_type"] = type(error).__name__

    if failure or not workload_complete:
        pytest.fail("bounded real-model gate failed; see validation-1.6/live-report.json", pytrace=False)
    assert report["sent"] == 32
