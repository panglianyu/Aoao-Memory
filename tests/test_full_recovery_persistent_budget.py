"""Durable same-source budgets; synthetic SQLite and synthetic SDK only."""

from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kernel.challenge_diagnostics import DiagnosticError, _encode_record
from kernel.memory_pipeline_models import PipelineScope, SourceRevision
from system_memory_god.storage.extraction_call_budget import ExtractionCallBudget, group_budget_key
from system_memory_god.grouped_extractor import GroupedDailyExtractor, _GROUP_HASH
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.extraction_snapshots import STRUCTURED_DOMAINS


def inputs():
    scope = PipelineScope("test-user", "test-agent", "test-connector", "test-conversation")
    source = SourceRevision("test-message", 1, "task-entity", "memory-grouped", "v1",
                            "synthetic-model", "a" * 64)
    return scope, source, "b" * 64


def test_same_source_domains_and_reconstruction_share_three_calls(tmp_path):
    scope, source, content = inputs()
    budget = ExtractionCallBudget(tmp_path, scope)
    first = budget.reserve(source, content, origin="fresh")
    budget.finish(first, state="failed", reason="json", finish="unknown")
    second = ExtractionCallBudget(tmp_path, scope).reserve(
        replace(source, task_id="task-graph"), content, origin="legacy_unknown")
    assert second.number == 2 and second.feedback == "json"
    budget.finish(second, state="unknown")
    third = ExtractionCallBudget(tmp_path, scope).reserve(
        replace(source, task_id="task-rule"), content, origin="fresh")
    assert third.number == 3
    budget.finish(third, state="completed")
    with pytest.raises(DiagnosticError, match="budget exhausted"):
        budget.reserve(replace(source, task_id="task-journal"), content, origin="fresh")


def test_reserved_crash_is_consumed_without_refund(tmp_path):
    scope, source, content = inputs()
    first = ExtractionCallBudget(tmp_path, scope).reserve(source, content, origin="fresh")
    reopened = ExtractionCallBudget(tmp_path, scope)
    second = reopened.reserve(source, content, origin="fresh")
    assert second.number == 2
    history = reopened.history(source, content)
    assert history[0]["state"] == "unknown"
    assert history[0]["number"] == first.number


def test_legacy_unknown_is_not_initialized_to_zero(tmp_path):
    scope, source, content = inputs()
    budget = ExtractionCallBudget(tmp_path, scope)
    with pytest.raises(DiagnosticError, match="legacy"):
        budget.reserve(source, content, origin="legacy_unknown")
    with pytest.raises(DiagnosticError, match="legacy"):
        budget.reserve(source, content, origin="fresh")
    assert budget.history(source, content) == ()


def test_terminal_history_does_not_exhaust_activity_capacity(tmp_path):
    scope, source, content = inputs()
    budget = ExtractionCallBudget(tmp_path, scope)
    for index in range(129):
        row = replace(source, raw_message_id="test-message-" + str(index))
        call = budget.reserve(row, content, origin="fresh")
        budget.finish(call, state="failed", reason="json")
    last = budget.reserve(replace(source, raw_message_id="new-message"), content, origin="fresh")
    assert last.number == 1
    assert len(budget.history(source, content)) == 0


def test_group_key_excludes_task_but_binds_all_group_identity():
    scope, source, content = inputs()
    expected = group_budget_key(scope, source, content)
    assert group_budget_key(scope, replace(source, task_id="different-domain-task"), content) == expected
    for changed in (replace(source, raw_message_id="different"), replace(source, revision=2),
                    replace(source, processor="different"), replace(source, processor_version="v2"),
                    replace(source, model="other-model"), replace(source, prompt_hash="c" * 64)):
        assert group_budget_key(scope, changed, content) != expected
    assert group_budget_key(scope, source, "c" * 64) != expected
    assert group_budget_key(replace(scope, user_id="other-user"), source, content) != expected


def test_finish_is_idempotent_but_cannot_refund_or_rewrite(tmp_path):
    scope, source, content = inputs()
    budget = ExtractionCallBudget(tmp_path, scope)
    call = budget.reserve(source, content, origin="fresh")
    budget.finish(call, state="failed", reason="json")
    budget.finish(call, state="failed", reason="json")
    with pytest.raises(DiagnosticError):
        budget.finish(call, state="completed")
    assert budget.reserve(source, content, origin="fresh").number == 2


def extraction_fixture(tmp_path, sdk, *, origin="fresh"):
    scope, source, content = inputs()
    source = replace(source, prompt_hash=_GROUP_HASH)
    message = SimpleNamespace(user_id=scope.user_id, agent_id=scope.agent_id,
        connector_id=scope.connector_id, conversation_id=scope.conversation_id,
        message_id=source.raw_message_id, revision=source.revision, content_hash=content,
        role="user", content_type="text", content="synthetic source",
        created_at="2026-11-05T00:00:00+00:00")
    stores = ScopedStores(tmp_path)
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    worker = GroupedDailyExtractor(stores, client, source.model)

    async def guard():
        return None

    async def budget_origin():
        return origin

    guard.budget_origin = budget_origin
    return worker, stores, scope, source, message, guard


def response(outputs=None):
    return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
        message=SimpleNamespace(content="{" if outputs is None else json.dumps(outputs)))])


@pytest.mark.asyncio
async def test_extractor_reconstruction_does_not_refund_sdk_calls(tmp_path):
    sdk = AsyncMock(return_value=response())
    worker, _, _, source, message, guard = extraction_fixture(tmp_path, sdk)
    with pytest.raises(DiagnosticError):
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 3
    rebuilt, _, _, _, _, new_guard = extraction_fixture(tmp_path, sdk)
    with pytest.raises(DiagnosticError):
        await rebuilt.extract_guarded("graph", message, replace(source, task_id="other-task"), 2048, new_guard)
    assert sdk.await_count == 3


@pytest.mark.asyncio
async def test_proven_fresh_source_runs_but_legacy_unknown_does_not(tmp_path):
    sdk = AsyncMock(return_value=response({domain: [] for domain in STRUCTURED_DOMAINS}))
    worker, _, _, source, message, guard = extraction_fixture(tmp_path, sdk, origin="legacy_unknown")
    with pytest.raises(DiagnosticError, match="legacy"):
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 0
    fresh, _, _, _, _, fresh_guard = extraction_fixture(tmp_path, sdk)
    new_source = replace(source, raw_message_id="genuinely-new-source")
    message.message_id = new_source.raw_message_id
    assert await fresh.extract_guarded("entity", message, new_source, 2048, fresh_guard) == ()
    assert sdk.await_count == 1


@pytest.mark.asyncio
async def test_legal_old_cache_precedes_legacy_budget_and_sdk(tmp_path):
    sdk = AsyncMock(side_effect=AssertionError("SDK must not run"))
    worker, stores, scope, source, message, guard = extraction_fixture(tmp_path, sdk, origin="legacy_unknown")
    outputs = {domain: [] for domain in STRUCTURED_DOMAINS}
    stores.extractions(scope).put(source, "entity", message.content_hash, outputs)
    assert await worker.extract_guarded("entity", message, source, 2048, guard) == ()
    assert sdk.await_count == 0
    assert not stores.call_budgets(scope)._db.path.exists()


@pytest.mark.asyncio
async def test_missing_fresh_proof_refuses_sdk(tmp_path):
    sdk = AsyncMock(side_effect=AssertionError("SDK must not run"))
    worker, _, _, source, message, guard = extraction_fixture(tmp_path, sdk)
    del guard.budget_origin
    with pytest.raises(DiagnosticError, match="legacy"):
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 0


@pytest.mark.asyncio
async def test_sdk_observes_a_committed_reservation_before_it_starts(tmp_path):
    worker, stores, scope, source, message, guard = extraction_fixture(tmp_path, None)

    async def sdk(**kwargs):
        calls = stores.call_budgets(scope).history(source, message.content_hash)
        assert calls == ({"number": 1, "state": "reserved", "reason": "unknown", "finish": "unknown"},)
        return response({domain: [] for domain in STRUCTURED_DOMAINS})

    worker.client.chat.completions.create = sdk
    assert await worker.extract_guarded("entity", message, source, 2048, guard) == ()
    assert stores.call_budgets(scope).history(source, message.content_hash)[0]["state"] == "completed"


@pytest.mark.asyncio
async def test_cache_commit_failure_and_reconstruction_do_not_refund(tmp_path, monkeypatch):
    from system_memory_god.storage.extraction_snapshots import ExtractionSnapshots

    sdk = AsyncMock(return_value=response({domain: [] for domain in STRUCTURED_DOMAINS}))

    def failed_put(*args, **kwargs):
        raise OSError("synthetic cache commit failure")

    monkeypatch.setattr(ExtractionSnapshots, "put", failed_put)
    for expected in range(1, 4):
        worker, stores, scope, source, message, guard = extraction_fixture(tmp_path, sdk)
        with pytest.raises(OSError, match="synthetic cache"):
            await worker.extract_guarded("entity", message, source, 2048, guard)
        assert sdk.await_count == expected
    worker, _, _, source, message, guard = extraction_fixture(tmp_path, sdk)
    with pytest.raises(DiagnosticError, match="budget exhausted"):
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 3
    assert [call["state"] for call in stores.call_budgets(scope).history(source, message.content_hash)] == [
        "completed", "completed", "completed"]


@pytest.mark.asyncio
async def test_reservation_failure_prevents_sdk_dispatch(tmp_path, monkeypatch):
    sdk = AsyncMock(side_effect=AssertionError("SDK must not run"))
    worker, _, _, source, message, guard = extraction_fixture(tmp_path, sdk)

    def unavailable(*args, **kwargs):
        raise OSError("synthetic ledger unavailable")

    monkeypatch.setattr(ExtractionCallBudget, "reserve", unavailable)
    with pytest.raises(OSError, match="synthetic ledger"):
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 0


def test_budget_database_cannot_escape_or_cross_scope(tmp_path):
    scope, source, content = inputs()
    budget = ExtractionCallBudget(tmp_path, scope)
    budget.reserve(source, content, origin="fresh")
    wrong = ExtractionCallBudget(tmp_path, replace(scope, user_id="another"))
    wrong._db.path = budget._db.path
    with pytest.raises(ValueError):
        wrong.reserve(source, content, origin="fresh")


@pytest.mark.parametrize("cause", ["capacity", "derived", "publisher", "reader", "deadline", "ownerunknown"])
def test_publication_cause_is_closed_and_preserved(cause):
    error = DiagnosticError("published", "synthetic private error", publication_cause=cause)
    value = json.loads(_encode_record("published", error, None, None))
    assert value["publication_cause"] == cause
    assert value["reason"] == "published"
    assert "synthetic private error" not in str(value)


@pytest.mark.parametrize("cause", ["private-free-text", {}, 1, False, []])
def test_publication_cause_unknown_is_rejected(cause):
    with pytest.raises(ValueError):
        DiagnosticError("published", publication_cause=cause)
