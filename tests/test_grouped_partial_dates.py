"""Temporal text remains complete; only real aware instants receive time indexes."""

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from kernel.challenge_diagnostics import DiagnosticError
from system_memory_god import grouped_extractor
from system_memory_god.storage.extraction_snapshots import validate_group


def grouped(domain, stamp, text="原始文本，保留全部事实"):
    value = {name: [] for name in ("entity", "graph", "rule", "journal", "schedule")}
    field = "occurred_at" if domain == "journal" else "scheduled_at"
    value[domain] = [{"text": text, field: stamp}]
    return value


def parse(value):
    response = SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
        message=SimpleNamespace(content=json.dumps(value)))])
    return grouped_extractor._response_outputs(response)


@pytest.mark.parametrize("domain", ["journal", "schedule"])
@pytest.mark.parametrize("stamp", ["2026-11", "2026-11-04", "2000-02-29", "0001-01", "9999-12-31"])
def test_partial_dates_retain_original_precision_in_text(domain, stamp):
    outputs = grouped(domain, stamp)
    original = deepcopy(outputs)
    value = parse(outputs)[domain][0]
    field = "occurred_at" if domain == "journal" else "scheduled_at"
    assert value == {"text": original[domain][0]["text"] + "\n" + stamp, field: None}
    assert outputs == original


@pytest.mark.parametrize("stamp", [
    "2026-02-29", "1900-02-29", "0000-01", "0000-01-01", "10000-01", "2026-13",
    "2026-00", "2026-01-00", "2026-04-31", "2026-1", "2026-1-04", "2026-11-4",
    " 2026-11", "2026-11 ", "２０２６-１１", "2026-11-04Z", "2026-11T00:00",
    "2026-11-04T00:00:00", "2026-11-04+08:00", "2026-11-04\n", "明年春天", "2026",
    "unknown", "2026-02-29T00:00:00Z", "2026-11-04T00:00Z", "2026-11-04t00:00:00Z",
    "2026-11-04T00:00:00z",
])
def test_uncertain_time_text_is_preserved_without_absolute_instant(stamp):
    for domain in ("journal", "schedule"):
        original = grouped(domain, stamp)
        parsed = parse(original)[domain][0]
        field = "occurred_at" if domain == "journal" else "scheduled_at"
        assert parsed == {"text": original[domain][0]["text"] + "\n" + stamp, field: None}


@pytest.mark.parametrize("stamp", [202611, False, [], {}, "", " \n", "\ud800", "x" * 8193])
def test_truly_invalid_timestamp_types_unicode_and_sizes_remain_rejected(stamp):
    for domain in ("journal", "schedule"):
        with pytest.raises(DiagnosticError) as caught:
            parse(grouped(domain, stamp))
        assert caught.value.safe_reason == "domain_payload"


@pytest.mark.parametrize("domain", ["journal", "schedule"])
@pytest.mark.parametrize("stamp", [None, "2026-11-04T00:00:00+08:00", "2000-02-29T12:00:00+00:00"])
def test_original_valid_timestamps_and_null_are_preserved(domain, stamp):
    outputs = grouped(domain, stamp)
    outputs["entity"] = [{"subject": "活动", "attribute": "月份", "value": "2026-11"}]
    assert parse(outputs) == outputs


@pytest.mark.parametrize("stamp", ["2000-02-29T12:00:00Z", "2000-02-29T12:00:00.123456Z"])
def test_strict_rfc3339_z_is_equivalent_utc_without_changing_text(stamp):
    for domain in ("journal", "schedule"):
        outputs = grouped(domain, stamp)
        parsed = parse(outputs)[domain][0]
        field = "occurred_at" if domain == "journal" else "scheduled_at"
        assert parsed == {"text": outputs[domain][0]["text"], field: stamp[:-1] + "+00:00"}


def test_valid_aware_time_does_not_require_text_append_headroom():
    outputs = grouped("journal", "2026-11-04T00:00:00+08:00", "中" * 8192)
    assert parse(outputs) == outputs


@pytest.mark.parametrize("stamp", ["2026-11", "2026-11-04", "2026-02-29", "明年春天"])
def test_exact_text_limit_accepts_boundary_and_refuses_overflow(stamp):
    text = "中" * (8192 - len(stamp) - 1)
    assert len(parse(grouped("journal", stamp, text))["journal"][0]["text"]) == 8192
    with pytest.raises(DiagnosticError):
        parse(grouped("journal", stamp, text + "中"))


def test_long_time_text_is_never_truncated_or_silently_dropped():
    result = parse(grouped("schedule", "x" * 8190, "中"))["schedule"][0]
    assert len(result["text"]) == 8192 and result["scheduled_at"] is None
    with pytest.raises(DiagnosticError):
        parse(grouped("schedule", "x" * 8191, "中"))


@pytest.mark.parametrize("text", [None, "", " \n", "\ud800", "x" * 8193, 1])
def test_invalid_original_text_is_not_hidden_by_normalization(text):
    with pytest.raises(DiagnosticError) as caught:
        parse(grouped("journal", "2026-11", text))
    assert caught.value.safe_reason == "domain_payload"


def test_copy_normalization_preserves_input_and_original_strict_contract():
    original = grouped("journal", "2026-11", "逐字保留\n已有日期 2026-11")
    original["schedule"] = [{"text": "计划原文", "scheduled_at": "2026-11-04"}]
    before = deepcopy(original)
    normalized = grouped_extractor._normalize_partial_dates(original)
    assert original == before and normalized is not original
    assert normalized["journal"][0] is not original["journal"][0]
    assert normalized["schedule"][0] is not original["schedule"][0]
    assert normalized["journal"][0]["text"] == before["journal"][0]["text"] + "\n2026-11"
    with pytest.raises(DiagnosticError):
        validate_group(original)
    assert validate_group(normalized) == normalized


@pytest.mark.parametrize("mutation", ["extra", "missing", "bad_other_domain", "too_many", "not_array"])
def test_normalization_does_not_hide_other_contract_failures(mutation):
    outputs = grouped("journal", "2026-11")
    if mutation == "extra":
        outputs["journal"][0]["unknown"] = "preserve and reject"
    elif mutation == "missing":
        del outputs["journal"][0]["text"]
    elif mutation == "bad_other_domain":
        outputs["entity"] = [{"subject": "a", "attribute": "b", "value": None}]
    elif mutation == "too_many":
        outputs["journal"] *= 11
    else:
        outputs["journal"] = {}
    with pytest.raises(DiagnosticError):
        parse(outputs)
