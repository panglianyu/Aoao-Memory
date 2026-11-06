"""No-model regressions for live-gate observations and their failure boundaries."""

import asyncio
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


_PATH = Path(__file__).with_name("test_memory_challenge_admission_live.py")
_SPEC = importlib.util.spec_from_file_location("amc_live_sampling_subject", _PATH)
live = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = live
_SPEC.loader.exec_module(live)


class _ProbeError(Exception):
    pass


class _Gone(_ProbeError):
    pass


def _fake_psutil(*, fail=False, available=3 * 1024**3, rss=17):
    class Process:
        def children(self, *, recursive):
            assert recursive is True
            if fail:
                raise _ProbeError("private diagnostic must not be recorded")
            return [SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=23))]

        def memory_info(self):
            return SimpleNamespace(rss=rss)

    return SimpleNamespace(
        Process=Process, Error=_ProbeError, NoSuchProcess=_Gone,
        virtual_memory=lambda: SimpleNamespace(available=available),
    )


@pytest.mark.asyncio
async def test_sampling_wait_timeout_allows_repeated_observations(monkeypatch):
    monkeypatch.setitem(sys.modules, "psutil", _fake_psutil())
    observations = live._MemoryObservations()
    stop = asyncio.Event()

    async def tick(awaitable, *, timeout):
        assert timeout == 0.5
        awaitable.close()
        if observations.samples < 3:
            # Python 3.10 wait_for uses this distinct exception type.
            raise asyncio.TimeoutError
        stop.set()

    monkeypatch.setattr(live.asyncio, "wait_for", tick)
    await observations.run(stop)
    summary = observations.summary()
    assert summary["samples"] == 4
    assert summary["observed_peak_runner_and_descendants_rss_bytes"] == 40
    assert summary["observed_min_machine_available_bytes"] == 3 * 1024**3
    assert summary["unavailable_type"] is None
    assert summary["stop_reason"] is None


@pytest.mark.asyncio
async def test_sampling_failure_keeps_missing_metrics_and_only_error_type(monkeypatch):
    monkeypatch.setitem(sys.modules, "psutil", _fake_psutil(fail=True))
    observations = live._MemoryObservations()
    stop = asyncio.Event()

    async def tick(awaitable, *, timeout):
        awaitable.close()
        stop.set()

    monkeypatch.setattr(live.asyncio, "wait_for", tick)
    await observations.run(stop)
    summary = observations.summary()
    assert summary["samples"] == 0
    assert summary["observed_peak_runner_and_descendants_rss_bytes"] is None
    assert summary["observed_min_machine_available_bytes"] is None
    assert summary["unavailable_type"] == "_ProbeError"
    assert "private diagnostic" not in str(summary)


def _healthy_shared_stats():
    return {
        "load_count": 1, "generation_count": 32, "current_generation": 0,
        "ipc_inflight": 0, "ipc_peak": 2, "model_owner_count": 0,
        "model_owner_peak": 1, "clean_close": True, "failed": False,
        "active_generation_count": 0, "active_generations": [], "active_generation_peak": 2,
    }


def test_shared_gate_requires_real_cleanup_and_single_model_owner():
    stats = _healthy_shared_stats()
    assert live._shared_passed(stats, closed=True, generations=32)
    for key, value in (
        ("load_count", 2), ("generation_count", 31), ("ipc_inflight", 1),
        ("ipc_peak", 3), ("model_owner_count", 1), ("model_owner_peak", 2),
        ("clean_close", False), ("failed", True),
    ):
        changed = {**stats, key: value}
        assert not live._shared_passed(changed, closed=True, generations=32), key
    assert not live._shared_passed(None, closed=True, generations=32)


def test_memory_gate_rejects_missing_or_failed_sampling():
    summary = {
        "samples": 2, "observed_peak_runner_and_descendants_rss_bytes": 40,
        "observed_min_machine_available_bytes": 3 * 1024**3, "unavailable_type": None,
    }
    assert live._memory_passed(summary)
    for key, value in (
        ("samples", 1), ("observed_peak_runner_and_descendants_rss_bytes", None),
        ("observed_min_machine_available_bytes", None), ("unavailable_type", "TimeoutError"),
    ):
        assert not live._memory_passed({**summary, key: value}), key


def test_shared_report_whitelists_scalar_observations():
    stats = {**_healthy_shared_stats(), "private_setting": "do not serialize"}
    snapshot = live._shared_observations(SimpleNamespace(stats=stats))
    assert "private_setting" not in snapshot
    with pytest.raises(ValueError, match="invalid shared observation counters"):
        live._shared_observations(SimpleNamespace(stats={**stats, "load_count": {"secret": 1}}))


@pytest.mark.parametrize("available,rss,expected", [
    (live.MIN_AVAILABLE_BYTES - 1, 17, "sampled_available_below_2gib"),
    (live.MIN_AVAILABLE_BYTES, live.MAX_RUNNER_RSS_BYTES - 23, None),
    (live.MIN_AVAILABLE_BYTES, live.MAX_RUNNER_RSS_BYTES - 22, "sampled_runner_rss_above_6gib"),
])
def test_sampled_limits_stop_new_work_without_claiming_a_hard_bound(
        monkeypatch, available, rss, expected):
    monkeypatch.setitem(sys.modules, "psutil", _fake_psutil(available=available, rss=rss))
    observations = live._MemoryObservations()
    observations.sample()
    assert observations.stop_reason == expected
    assert "sampled" in observations.summary()["scope"]


def test_shared_report_rejects_inconsistent_or_duplicate_generations():
    stats = _healthy_shared_stats()
    for update in (
        {"active_generation_count": 1},
        {"active_generation_count": 2, "active_generations": [1, 1]},
        {"active_generation_count": 3, "active_generations": [1, 2, 3]},
    ):
        with pytest.raises(ValueError, match="invalid shared observation counters"):
            live._shared_observations(SimpleNamespace(stats={**stats, **update}))
