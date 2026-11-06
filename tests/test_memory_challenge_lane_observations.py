"""Fake observations for the single two-lane gate; no HTTP or model calls."""

import importlib.util
from pathlib import Path
import sys

import pytest


_PATH = Path(__file__).with_name("test_memory_challenge_admission_live.py")
_SPEC = importlib.util.spec_from_file_location("amc_lane_observation_subject", _PATH)
live = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = live
_SPEC.loader.exec_module(live)


def test_idle_observation_only_settles_the_matching_lane(monkeypatch):
    trace = live._Trace()
    trace.requests["add-0"] = live._Observation(0, 1, operation="add")
    trace.requests["add-1"] = live._Observation(1, 1, operation="add")
    monkeypatch.setattr(live.time, "monotonic", lambda: 1.0)
    trace.entered("add-0", 0)
    trace.entered("add-1", 1)
    assert trace.pending_ids(0) == ("add-0",)
    assert trace.pending_ids(1) == ("add-1",)
    trace.settled(trace.pending_ids(0))
    assert trace.requests["add-1"].owner_settled is None
    assert trace.max_unsettled_runtime_requests == 2
    assert trace.concurrency_passed()


def test_two_owners_in_one_lane_are_not_a_parallel_capacity_pass():
    trace = live._Trace()
    for number in range(2):
        key = str(number)
        trace.requests[key] = live._Observation(number, 1, operation="add")
        trace.entered(key, 0)
    assert trace.max_unsettled_runtime_requests == 2
    assert not trace.concurrency_passed()


def test_phase_observation_has_a_finite_shape_and_keeps_no_user_text():
    trace = live._Trace()
    trace.requests["add-0"] = live._Observation(0, 1, operation="add")
    trace.entered("add-0", 0)
    trace.phase(0, "private-user-not-recorded", "factory", 1.0, 2.0)
    summary = trace.requests["add-0"].summary()
    assert summary["phases"] == [{"stage": "factory", "elapsed_seconds": 1.0}]
    assert "private-user" not in str(summary)
    with pytest.raises(ValueError, match="invalid phase"):
        trace.phase(0, "x", "provider-secret", 1.0, 2.0)
    with pytest.raises(ValueError, match="invalid phase"):
        trace.phase(0, "x", "add", 3.0, 2.0)


def test_shutdown_retire_is_separate_from_request_phase_accounting():
    trace = live._Trace()
    trace.phase(1, "private-retired-user", "retire", 1.0, 2.0)
    assert trace.orphan_phase_count == 0
    assert trace.cleanup_phases == [{"lane_id": 1, "stage": "retire", "elapsed_seconds": 1.0}]
    assert "private-retired-user" not in str(trace.cleanup_phases)
    trace.phase(1, "private-unbound-user", "factory", 2.0, 3.0)
    assert trace.orphan_phase_count == 1


def test_search_requires_own_evidence_and_rejects_cross_user_markers():
    markers = ("marker-own", "marker-other")
    valid = {"data": [{"id": "1", "content": "Only marker-own is in this user's evidence."}]}
    assert live._search_valid(valid, own_marker=markers[0], all_markers=markers)
    assert not live._search_valid({"data": []}, own_marker=markers[0], all_markers=markers)
    assert not live._search_valid(
        {"data": [{"id": "1", "content": "marker-own and marker-other"}]},
        own_marker=markers[0], all_markers=markers,
    )
    assert not live._search_valid(
        {"data": [{"id": "1", "content": "marker-own", "private": "extra"}]},
        own_marker=markers[0], all_markers=markers,
    )


def test_cleanup_is_independent_of_unfinished_workload_generation_count():
    stats = {
        "load_count": 1, "generation_count": 17, "current_generation": 0,
        "ipc_inflight": 0, "ipc_peak": 2, "model_owner_count": 0,
        "model_owner_peak": 1, "clean_close": True, "failed": False,
        "active_generation_count": 0, "active_generations": [], "active_generation_peak": 2,
    }
    assert live._shared_passed(stats, closed=True)
    assert live._cleanup_passed(stats)
    assert live._cleanup_passed({**stats, "generation_count": 0, "ipc_peak": 0})
    assert not live._shared_passed({**stats, "active_generation_count": 1}, closed=True)
    assert not live._shared_passed({**stats, "active_generation_peak": 3}, closed=True)


def test_gate_budget_never_renews_after_a_later_operation(monkeypatch):
    monkeypatch.setattr(live.time, "monotonic", lambda: 10.0)
    budget = live._GateBudget(10.0)
    assert budget.remaining() == 600.0
    monkeypatch.setattr(live.time, "monotonic", lambda: 609.0)
    assert budget.remaining() == 1.0
    monkeypatch.setattr(live.time, "monotonic", lambda: 610.0)
    with pytest.raises(live._GateBudgetExceeded):
        budget.remaining()
