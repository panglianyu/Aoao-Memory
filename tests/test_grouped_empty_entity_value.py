"""Empty generated entity values are rejected with durable, scoped provenance."""

import asyncio
from contextlib import asynccontextmanager, closing
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kernel.challenge_diagnostics import DiagnosticError, _encode_record
from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical
from system_memory_god import grouped_extractor
from system_memory_god.storage import extraction_snapshots
from system_memory_god.storage.extraction_snapshots import ExtractionSnapshots, STRUCTURED_DOMAINS


def group():
    value = {domain: [] for domain in STRUCTURED_DOMAINS}
    value["entity"] = [{"subject": "Aru", "attribute": "project", "value": "memory"},
                       {"subject": "Aru", "attribute": "unspecified", "value": ""}]
    value["graph"] = [{"subject": "Aru", "relation": "works on", "object": "memory"}]
    return value


def response(outputs, *, ensure_ascii=True):
    return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
        message=SimpleNamespace(content=json.dumps(outputs, ensure_ascii=ensure_ascii)))])


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def test_mixed_empty_candidate_has_complete_reversible_admission_without_mutation():
    value = group()
    before = deepcopy(value)
    outputs, admission = grouped_extractor._response_admission(response(value))
    assert outputs["entity"] == [before["entity"][0]]
    assert outputs["graph"] == before["graph"]
    assert value == before
    assert admission == {"policy": "empty-entity-value/v1", "input_sha256": digest(before),
        "output_sha256": digest(outputs), "rejected": [{"index": 1,
            "candidate": before["entity"][1], "sha256": digest(before["entity"][1]),
            "reason": "empty_value"}]}


def test_strict_legacy_parser_still_rejects_same_empty_value():
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_outputs(response(group()))
    assert caught.value.safe_reason == "domain_payload"


def test_all_entity_candidates_rejected_is_not_a_successful_empty_result():
    value = group()
    value["entity"] = [value["entity"][1]]
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_admission(response(value))
    assert caught.value.safe_reason == "domain_payload"


def test_valid_response_uses_legacy_cache_format_without_admission():
    value = group()
    value["entity"] = value["entity"][:1]
    outputs, admission = grouped_extractor._response_admission(response(value))
    assert outputs == value and admission is None


@pytest.mark.parametrize("value", [None, False, 0, 1.25, [], {}, " ", "\n", "\ud800", "x" * 8193])
def test_empty_policy_does_not_accept_other_invalid_entity_values(value):
    outputs = group()
    outputs["entity"][1]["value"] = value
    with pytest.raises(DiagnosticError):
        grouped_extractor._response_admission(response(outputs))


@pytest.mark.parametrize("field,value", [
    ("subject", ""), ("subject", " \n"), ("attribute", None),
    ("attribute", "x" * 8193), ("subject", "\ud800"),
])
def test_empty_value_never_hides_another_bad_field(field, value):
    outputs = group()
    outputs["entity"][1][field] = value
    with pytest.raises(DiagnosticError):
        grouped_extractor._response_admission(response(outputs))


@pytest.mark.parametrize("mutation", ["extra", "missing", "graph", "rule", "count"])
def test_empty_entity_never_hides_group_or_other_domain_errors(mutation):
    outputs = group()
    if mutation == "extra":
        outputs["entity"][1]["extra-private-key"] = "private-value"
    elif mutation == "missing":
        del outputs["entity"][1]["attribute"]
    elif mutation == "graph":
        outputs["graph"][0]["object"] = ""
    elif mutation == "rule":
        outputs["rule"] = [{"text": "do not promote", "status": "active"}]
    else:
        outputs["entity"] = outputs["entity"][:1] + outputs["entity"][1:] * 10
    with pytest.raises(DiagnosticError):
        grouped_extractor._response_admission(response(outputs))


def test_multiple_rejections_reconstruct_original_entity_order_and_payloads():
    value = group()
    empty = value["entity"][1]
    value["entity"] = [empty, value["entity"][0], {**empty, "attribute": "other"},
                       {"subject": "Aru", "attribute": "boundary", "value": "x" * 8192}]
    before = deepcopy(value)
    outputs, admission = grouped_extractor._response_admission(response(value))
    assert [item["index"] for item in admission["rejected"]] == [0, 2]
    reconstructed = deepcopy(outputs)
    for item in admission["rejected"]:
        reconstructed["entity"].insert(item["index"], item["candidate"])
    assert reconstructed == before
    assert digest(reconstructed) == admission["input_sha256"]
    assert value == before


def test_original_response_character_limit_still_applies_before_admission():
    bad = SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
        message=SimpleNamespace(content=" " * 128001))])
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_admission(bad)
    assert caught.value.safe_reason == "content"


def test_filtering_cannot_hide_an_oversized_original_group():
    value = group()
    value["entity"] = value["entity"][:1] + [
        {"subject": "中" * 8192, "attribute": "文" * 8192, "value": ""} for _ in range(4)]
    value["graph"] = [{"subject": "甲" * 8192, "relation": "乙" * 8192,
                       "object": "丙" * 8192} for _ in range(2)]
    assert len(canonical(value)) > 512000
    candidate = response(value, ensure_ascii=False)
    assert len(candidate.choices[0].message.content) <= 128000
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_admission(candidate)
    assert caught.value.safe_reason == "group_shape"


def cache_fixture(tmp_path, *, user="alice"):
    scope = PipelineScope(user, "aru", "test", "chat")
    source = SourceRevision("raw-empty-value", 1, "task-entity", "memory-grouped", "v1",
                            "synthetic-model", grouped_extractor._GROUP_HASH)
    cache = ExtractionSnapshots(tmp_path / "pipelines", scope)
    return scope, source, cache


def cache_row(cache):
    with cache._db.readonly_snapshot() as db:
        return db.execute("SELECT * FROM extraction_snapshots").fetchone()


def receipt_paths(cache):
    parent = cache._db.path.parent
    # Test tampering uses the same Win32 spelling support as private file IO.
    if os.name == "nt" and not str(parent).startswith("\\\\?\\"):
        parent = Path("\\\\?\\" + str(parent))
    return sorted(parent.rglob("*.json"))


def assert_no_committed_cache(cache):
    if not cache._db.path.exists():
        return
    with closing(sqlite3.connect(cache._db.path.as_uri() + "?mode=ro", uri=True)) as db:
        table = db.execute("SELECT 1 FROM sqlite_master WHERE name='extraction_snapshots'").fetchone()
        if table is not None:
            assert db.execute("SELECT count(*) FROM extraction_snapshots").fetchone()[0] == 0


def admitted():
    return grouped_extractor._response_admission(response(group()))


def test_old_cache_row_digest_and_contents_remain_unchanged(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs = group()
    outputs["entity"] = outputs["entity"][:1]
    cache.put(source, "entity", "a" * 64, outputs)
    row = cache_row(cache)
    assert row[-1] == digest(row[:5])
    assert cache.get(source, "a" * 64) == outputs
    assert receipt_paths(cache) == []


def test_new_cache_requires_complete_reversible_receipt_and_shares_other_task_ids(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    row = cache_row(cache)
    assert row[-1] != digest(row[:5])
    paths = receipt_paths(cache)
    assert len(paths) == 1
    body = paths[0].read_bytes()
    assert canonical(group()["entity"][1]).encode() in body
    assert row[-1].encode() not in body
    assert cache.get(replace(source, task_id="task-graph"), "a" * 64) == outputs
    seen = []
    cache.audit(lambda batch: seen.extend(batch))
    assert seen == [(source, "entity")]
    before = row, body
    cache.put(replace(source, task_id="task-journal"), "journal", "a" * 64,
              outputs, admission=admission)
    assert (cache_row(cache), paths[0].read_bytes()) == before
    assert receipt_paths(cache) == paths


@pytest.mark.parametrize("change", ["missing", "invalid-json", "wrong-policy", "wrong-source", "oversize"])
def test_missing_or_corrupt_receipt_cannot_be_read_or_audited(tmp_path, change):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    path, = receipt_paths(cache)
    if change == "missing":
        path.unlink()
    elif change == "invalid-json":
        path.write_bytes(b"{broken")
    elif change == "oversize":
        path.write_bytes(b"x" * (1048576 + 1))
    else:
        value = json.loads(path.read_text())
        if change == "wrong-policy":
            value["policy"] = "invented-policy"
        else:
            value["source"]["raw_message_id"] = "another-source"
        path.write_text(canonical(value), encoding="ascii")
    with pytest.raises(DiagnosticError) as caught:
        cache.get(source, "a" * 64)
    assert caught.value.safe_reason == "cache_integrity"
    with pytest.raises(DiagnosticError):
        cache.audit(lambda batch: None)


@pytest.mark.parametrize("change", ["input-hash", "output-hash", "candidate-hash", "candidate", "duplicate-index"])
def test_invalid_admission_is_rejected_before_any_cache_commit(tmp_path, change):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    admission = deepcopy(admission)
    if change == "input-hash":
        admission["input_sha256"] = "0" * 64
    elif change == "output-hash":
        admission["output_sha256"] = "0" * 64
    elif change == "candidate-hash":
        admission["rejected"][0]["sha256"] = "0" * 64
    elif change == "candidate":
        admission["rejected"][0]["candidate"]["value"] = "nonempty"
    else:
        admission["rejected"].append(deepcopy(admission["rejected"][0]))
    with pytest.raises(DiagnosticError):
        cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    assert not cache.exists()
    assert receipt_paths(cache) == []


def test_new_cache_content_conflicts_fail_without_rewriting_previous_result(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    old_row = cache_row(cache)
    old_receipts = {path: path.read_bytes() for path in receipt_paths(cache)}
    with pytest.raises(DiagnosticError):
        cache.put(source, "entity", "b" * 64, outputs, admission=admission)
    assert cache_row(cache) == old_row
    assert cache.get(source, "a" * 64) == outputs
    for path, body in old_receipts.items():
        assert path.read_bytes() == body


def test_scope_receipt_cannot_be_transplanted_to_another_partition(tmp_path):
    _, source, alice = cache_fixture(tmp_path)
    _, _, bob = cache_fixture(tmp_path, user="bob")
    outputs, admission = admitted()
    alice.put(source, "entity", "a" * 64, outputs, admission=admission)
    bob.put(source, "entity", "a" * 64, outputs, admission=admission)
    alice_path, = receipt_paths(alice)
    bob_path, = receipt_paths(bob)
    bob_path.write_bytes(alice_path.read_bytes())
    with pytest.raises(DiagnosticError):
        bob.get(source, "a" * 64)
    assert alice.get(source, "a" * 64) == outputs


def test_receipt_creation_failure_never_commits_successful_cache(tmp_path, monkeypatch):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()

    def denied(*args, **kwargs):
        raise PermissionError("synthetic receipt write denied")

    monkeypatch.setattr(extraction_snapshots, "open_private_file", denied)
    with pytest.raises((DiagnosticError, ValueError)):
        cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    assert_no_committed_cache(cache)
    assert receipt_paths(cache) == []


def test_orphan_receipt_without_db_is_not_absent(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    orphan, = receipt_paths(cache)
    before = orphan.read_bytes()
    cache._db.path.unlink()
    assert not cache._db.path.exists()
    with pytest.raises(DiagnosticError) as caught:
        cache.exists()
    assert caught.value.safe_reason == "cache_integrity"
    assert not cache._db.path.exists()
    assert orphan.read_bytes() == before


def test_existing_receipt_requires_fsync_before_cache_commit(tmp_path, monkeypatch):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    real_fsync = os.fsync
    failures = []

    def fail_fsync(fd):
        info = os.fstat(fd)
        failures.append((info.st_dev, info.st_ino, info.st_size))
        raise OSError("synthetic receipt durability failure")

    monkeypatch.setattr(extraction_snapshots.os, "fsync", fail_fsync)
    with pytest.raises((DiagnosticError, ValueError)):
        cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    orphan, = receipt_paths(cache)
    before = orphan.read_bytes()
    identity = orphan.stat()
    expected = (identity.st_dev, identity.st_ino, identity.st_size)
    assert failures == [expected]
    assert_no_committed_cache(cache)

    # Byte equality alone cannot repair an interrupted durability boundary.
    with pytest.raises((DiagnosticError, ValueError)):
        cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    assert failures == [expected, expected]
    assert_no_committed_cache(cache)
    assert orphan.read_bytes() == before

    retired = []

    def successful_fsync(fd):
        info = os.fstat(fd)
        retired.append((info.st_dev, info.st_ino, info.st_size))
        return real_fsync(fd)

    monkeypatch.setattr(extraction_snapshots.os, "fsync", successful_fsync)
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    assert retired == [expected]
    assert cache.get(source, "a" * 64) == outputs
    assert orphan.read_bytes() == before


def test_purge_removes_same_source_orphan_receipts_and_preserves_other_sources(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    orphan, = receipt_paths(cache)
    with cache._db.transaction() as db:
        db.execute("DELETE FROM extraction_snapshots")
    other = replace(source, raw_message_id="another-raw", task_id="other-task")
    cache.put(other, "entity", "b" * 64, outputs, admission=admission)
    assert orphan.exists() and len(receipt_paths(cache)) == 2
    assert cache.purge(source.raw_message_id) is True
    assert not orphan.exists()
    assert len(receipt_paths(cache)) == 1
    assert cache.get(other, "b" * 64) == outputs


def test_hardlinked_receipt_is_rejected_without_modifying_external_bytes(tmp_path):
    _, source, cache = cache_fixture(tmp_path)
    outputs, admission = admitted()
    cache.put(source, "entity", "a" * 64, outputs, admission=admission)
    path, = receipt_paths(cache)
    external = tmp_path / "external.json"
    os.link(path, external)
    before = external.read_bytes()
    with pytest.raises(DiagnosticError):
        cache.get(source, "a" * 64)
    with pytest.raises(ValueError):
        cache.purge(source.raw_message_id)
    assert external.read_bytes() == before


class RealStores:
    def __init__(self, cache):
        self.cache = cache
        self.lock = asyncio.Lock()

    def extractions(self, scope):
        return self.cache

    def call_budgets(self, scope):
        from system_memory_god.storage.extraction_call_budget import ExtractionCallBudget

        return ExtractionCallBudget(self.cache._db.root, scope)

    @asynccontextmanager
    async def gate(self, scope):
        async with self.lock:
            yield


def live_inputs(scope, source):
    message = SimpleNamespace(user_id=scope.user_id, agent_id=scope.agent_id,
        connector_id=scope.connector_id, conversation_id=scope.conversation_id,
        message_id=source.raw_message_id, revision=source.revision, role="user",
        content_type="text", content="synthetic original", content_hash="a" * 64,
        created_at="2026-11-05T00:00:00+00:00")
    return message


async def guard():
    pass


async def synthetic_fresh():
    return "fresh"


guard.budget_origin = synthetic_fresh


def extractor(stores, sdk):
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    return grouped_extractor.GroupedDailyExtractor(stores, client, "synthetic-model")


@pytest.mark.asyncio
async def test_mixed_admission_is_one_model_call_and_one_durable_cache_across_domains(tmp_path):
    scope, source, cache = cache_fixture(tmp_path)
    sdk = AsyncMock(return_value=response(group()))
    worker = extractor(RealStores(cache), sdk)
    result = await asyncio.gather(*(worker.extract_guarded(domain, live_inputs(scope, source),
        replace(source, task_id="task-" + domain), 2048, guard) for domain in STRUCTURED_DOMAINS))
    expected, _ = admitted()
    assert result == [tuple(expected[domain]) for domain in STRUCTURED_DOMAINS]
    assert sdk.await_count == 1 and len(receipt_paths(cache)) == 1
    assert worker._locks == {}


@pytest.mark.asyncio
async def test_cancelled_cache_commit_retains_gate_until_external_write_retires(tmp_path, monkeypatch):
    scope, source, cache = cache_fixture(tmp_path)
    stores = RealStores(cache)
    sdk = AsyncMock(return_value=response(group()))
    worker = extractor(stores, sdk)
    started, release = threading.Event(), threading.Event()
    original = cache.put

    def delayed(*args, **kwargs):
        started.set()
        if not release.wait(5):
            raise TimeoutError("synthetic blocked write")
        return original(*args, **kwargs)

    monkeypatch.setattr(cache, "put", delayed)
    task = asyncio.create_task(worker.extract_guarded("entity", live_inputs(scope, source), source, 2048, guard))
    competitor = None
    acquired = asyncio.Event()
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)

        async def enter_same_gate():
            async with stores.gate(scope):
                acquired.set()

        competitor = asyncio.create_task(enter_same_gate())
        await asyncio.sleep(0)
        assert not task.done() and not acquired.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        if competitor is not None:
            await competitor
    assert acquired.is_set() and sdk.await_count == 1
    assert len(receipt_paths(cache)) == 1


def test_field_diagnostics_have_actual_closed_position_and_no_private_value():
    outputs = group()
    outputs["entity"][1]["attribute"] = "private-field-value-sentinel"
    outputs["entity"][1]["value"] = None
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_admission(response(outputs))
    encoded = _encode_record("prepare", caught.value, "graph", 1)
    metadata = json.loads(encoded)
    assert b"private-field-value-sentinel" not in encoded
    assert metadata["payload_issue"] == {"domain": "entity", "index": 1, "field": "value",
        "type": "NoneType", "violation": "not_string", "characters": None}
    assert metadata["domain"] == "graph"


def test_extra_model_key_name_is_never_logged():
    outputs = group()
    outputs["entity"][1]["private-key-name-sentinel"] = "private-value-sentinel"
    with pytest.raises(DiagnosticError) as caught:
        grouped_extractor._response_admission(response(outputs))
    encoded = _encode_record("prepare", caught.value, "entity", 1)
    assert b"private-key-name-sentinel" not in encoded
    assert b"private-value-sentinel" not in encoded


@pytest.mark.parametrize("field,value", [
    ("domain", "private-domain-sentinel"), ("field", "private-field-sentinel"),
    ("index", 10), ("characters", True), ("violation", "private-reason-sentinel"),
])
def test_diagnostic_issue_rejects_unknown_names_and_out_of_range_metadata(field, value):
    issue = {"domain": "entity", "index": 0, "field": "value", "type": "str",
             "violation": "empty_text", "characters": 0}
    issue[field] = value
    error = DiagnosticError("domain_payload", payload_issue=issue)
    encoded = _encode_record("prepare", error, "entity", 1)
    assert "payload_issue" not in json.loads(encoded)
    assert b"private-" not in encoded
