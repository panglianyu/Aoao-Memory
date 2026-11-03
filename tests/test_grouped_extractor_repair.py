"""Bounded same-source corrections use synthetic responses and no model calls."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kernel.challenge_diagnostics import DiagnosticError, _encode_record
from kernel.memory_pipeline_models import SourceRevision
from system_memory_god.grouped_extractor import GroupedDailyExtractor, _GROUP_HASH, _GROUP_PROMPT
from system_memory_god.storage.extraction_snapshots import STRUCTURED_DOMAINS, extraction_key


class Stores:
    def __init__(self):
        self.outputs = {}
        self.writes = 0

    def extractions(self, scope):
        parent = self

        class Cache:
            @staticmethod
            def get(source, content_hash):
                return parent.outputs.get((scope.partition_id, extraction_key(source), content_hash))

            @staticmethod
            def put(source, domain, content_hash, outputs):
                parent.writes += 1
                parent.outputs[(scope.partition_id, extraction_key(source), content_hash)] = outputs

        return Cache()

    @asynccontextmanager
    async def gate(self, scope):
        yield


def inputs(*, user="alice", message_id="raw-message", revision=1, content_hash="a" * 64):
    message = SimpleNamespace(user_id=user, agent_id="aru", connector_id="test",
        conversation_id="chat", message_id=message_id, revision=revision, role="user",
        content_type="text", content="private-original-source", content_hash=content_hash,
        created_at="2026-11-04T00:00:00+00:00")
    source = SourceRevision(message_id, revision, "task-entity", "memory-grouped", "v1",
                            "synthetic-model", _GROUP_HASH)
    return source, message


def response(content=None):
    if content is None:
        content = json.dumps({domain: [] for domain in STRUCTURED_DOMAINS})
    return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
        message=SimpleNamespace(content=content))])


def invalid_response():
    return response("{private-invalid-model-response")


async def guard():
    pass


def extractor(stores, sdk):
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    return GroupedDailyExtractor(stores, client, "synthetic-model",
                                 provider_base_url="https://api.deepseek.com")


@pytest.mark.asyncio
async def test_invalid_response_is_corrected_once_and_shared_by_all_domains():
    stores = Stores()
    sdk = AsyncMock(side_effect=[invalid_response(), response()])
    worker = extractor(stores, sdk)
    source, message = inputs()
    results = await asyncio.gather(*(worker.extract_guarded(domain, message,
        replace(source, task_id="task-" + domain), 2048, guard) for domain in STRUCTURED_DOMAINS))
    assert results == [()] * 5
    assert sdk.await_count == 2 and stores.writes == 1
    first, correction = (call.kwargs for call in sdk.await_args_list)
    assert first["messages"][0] == {"role": "system", "content": _GROUP_PROMPT}
    assert correction["messages"][:2] == first["messages"]
    assert len(correction["messages"]) == 3
    assert "private-invalid-model-response" not in json.dumps(correction)
    assert "json" in correction["messages"][-1]["content"]
    for request in (first, correction):
        assert request["max_tokens"] == 2048
        assert request["temperature"] == 0
        assert request["response_format"] == {"type": "json_object"}
        assert request["extra_body"] == {"thinking": {"type": "disabled"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    "{private-invalid-model-response", "{}",
    json.dumps({**{domain: [] for domain in STRUCTURED_DOMAINS},
        "journal": [{"text": "event", "occurred_at": 2026}]}),
], ids=["json", "group-shape", "domain-payload"])
async def test_three_invalid_calls_are_not_repeated_by_other_domains_or_raw_retries(bad):
    stores = Stores()
    sdk = AsyncMock(return_value=response(bad))
    worker = extractor(stores, sdk)
    source, message = inputs()
    for _ in range(3):
        for domain in STRUCTURED_DOMAINS:
            with pytest.raises(DiagnosticError) as caught:
                await worker.extract_guarded(domain, message,
                    replace(source, task_id="task-" + domain), 2048, guard)
            assert b"private" not in _encode_record("prepare", caught.value, domain, 3)
    assert sdk.await_count == 3
    assert stores.writes == 0
    assert "private" not in repr(worker._group_budgets)


@pytest.mark.asyncio
async def test_cancellation_consumes_a_started_call_without_permanent_failure_before_budget():
    stores, started = Stores(), asyncio.Event()
    calls = 0

    async def create(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await asyncio.Event().wait()
        return invalid_response()

    worker = extractor(stores, AsyncMock(side_effect=create))
    source, message = inputs()
    pending = asyncio.create_task(worker.extract_guarded("entity", message, source, 2048, guard))
    await started.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    with pytest.raises(DiagnosticError):
        await worker.extract_guarded("graph", message, source, 2048, guard)
    with pytest.raises(DiagnosticError):
        await worker.extract_guarded("rule", message, source, 2048, guard)
    assert calls == 3 and stores.writes == 0


@pytest.mark.asyncio
async def test_transport_errors_can_retry_but_do_not_refund_or_retain_exception_objects():
    stores = Stores()
    sdk = AsyncMock(side_effect=[RuntimeError("private-transport"),
                                RuntimeError("private-transport"), response()])
    worker = extractor(stores, sdk)
    source, message = inputs()
    for domain in ("entity", "graph"):
        with pytest.raises(RuntimeError, match="private-transport"):
            await worker.extract_guarded(domain, message, source, 2048, guard)
        assert "private" not in repr(worker._group_budgets)
    assert await worker.extract_guarded("rule", message, source, 2048, guard) == ()
    assert sdk.await_count == 3 and stores.writes == 1
    assert worker._group_budgets == {}


@pytest.mark.asyncio
async def test_transport_budget_exhaustion_is_shared_without_an_extra_request():
    stores = Stores()
    sdk = AsyncMock(side_effect=RuntimeError("private-transport"))
    worker = extractor(stores, sdk)
    source, message = inputs()
    for domain in ("entity", "graph", "rule"):
        with pytest.raises(RuntimeError):
            await worker.extract_guarded(domain, message, source, 2048, guard)
    with pytest.raises(DiagnosticError) as caught:
        await worker.extract_guarded("journal", message, source, 2048, guard)
    assert caught.value.safe_reason == "source_budget"
    assert sdk.await_count == 3 and stores.writes == 0


@pytest.mark.asyncio
async def test_guard_is_checked_before_correction_and_shared_failure():
    stores = Stores()
    sdk = AsyncMock(side_effect=[invalid_response(), response()])
    worker = extractor(stores, sdk)
    source, message = inputs()
    guards = 0

    async def revoked_before_correction():
        nonlocal guards
        guards += 1
        if guards == 3:
            raise RuntimeError("source revoked")

    with pytest.raises(RuntimeError, match="source revoked"):
        await worker.extract_guarded("entity", message, source, 2048, revoked_before_correction)
    assert sdk.await_count == 1 and stores.writes == 0
    assert await worker.extract_guarded("graph", message, source, 2048, guard) == ()
    assert sdk.await_count == 2 and stores.writes == 1

    failed = extractor(Stores(), AsyncMock(return_value=invalid_response()))
    with pytest.raises(DiagnosticError):
        await failed.extract_guarded("entity", message, source, 2048, guard)

    async def revoked():
        raise RuntimeError("source revoked")

    with pytest.raises(RuntimeError, match="source revoked"):
        await failed.extract_guarded("graph", message, source, 2048, revoked)


@pytest.mark.asyncio
async def test_guard_revocation_before_first_sdk_does_not_keep_an_unused_slot():
    stores = Stores()
    sdk = AsyncMock(return_value=response())
    worker = extractor(stores, sdk)
    source, message = inputs()
    calls = 0

    async def revoke_before_call():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("source revoked")

    with pytest.raises(RuntimeError, match="source revoked"):
        await worker.extract_guarded("entity", message, source, 2048, revoke_before_call)
    assert sdk.await_count == 0 and stores.writes == 0
    assert worker._group_budgets == {}


@pytest.mark.asyncio
async def test_successful_result_is_not_written_after_guard_revocation():
    stores = Stores()
    sdk = AsyncMock(return_value=response())
    worker = extractor(stores, sdk)
    source, message = inputs()
    calls = 0

    async def revoked_before_write():
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("source revoked")

    with pytest.raises(RuntimeError, match="source revoked"):
        await worker.extract_guarded("entity", message, source, 2048, revoked_before_write)
    assert sdk.await_count == 1 and stores.writes == 0


@pytest.mark.asyncio
async def test_scope_revision_and_content_hash_have_separate_budgets():
    stores = Stores()
    sdk = AsyncMock(return_value=invalid_response())
    worker = extractor(stores, sdk)
    cases = [inputs(), inputs(user="bob"), inputs(message_id="another-message"),
             inputs(revision=2), inputs(content_hash="b" * 64)]
    for source, message in cases:
        with pytest.raises(DiagnosticError):
            await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 15
    assert len(worker._group_budgets) == 5


@pytest.mark.asyncio
async def test_failure_slots_are_bounded_and_a_successful_cache_takes_precedence():
    stores = Stores()
    sdk = AsyncMock(return_value=invalid_response())
    worker = extractor(stores, sdk)
    for index in range(128):
        source, message = inputs(message_id=f"message-{index}", content_hash=f"{index:064x}")
        with pytest.raises(DiagnosticError):
            await worker.extract_guarded("entity", message, source, 2048, guard)
    assert sdk.await_count == 384
    assert len(worker._group_budgets) == 128
    source, message = inputs(message_id="slot-overflow")
    with pytest.raises(DiagnosticError) as caught:
        await worker.extract_guarded("entity", message, source, 2048, guard)
    assert caught.value.safe_reason == "source_budget" and sdk.await_count == 384

    first_source, first_message = inputs(message_id="message-0", content_hash="0" * 64)
    from kernel.memory_pipeline_models import PipelineScope
    scope = PipelineScope(first_message.user_id, first_message.agent_id,
                          first_message.connector_id, first_message.conversation_id)
    stores.extractions(scope).put(first_source, "entity", first_message.content_hash,
                                  {domain: [] for domain in STRUCTURED_DOMAINS})
    assert await worker.extract_guarded("entity", first_message, first_source, 2048, guard) == ()
    assert len(worker._group_budgets) == 127 and sdk.await_count == 384
