"""The original strict grouped identity gains only closed failure categories."""

from contextlib import asynccontextmanager
import hashlib
import json
from types import SimpleNamespace

import pytest

from kernel.challenge_diagnostics import DiagnosticError
from kernel.memory_pipeline_models import SourceRevision
from system_memory_god.grouped_extractor import GroupedDailyExtractor, _GROUP_HASH, _GROUP_PROMPT
from system_memory_god.storage.extraction_snapshots import STRUCTURED_DOMAINS, validate_group
from system_memory_god.storage.derived_evidence import validate_payload


class FakeStores:
    def __init__(self):
        self.cached = None
        self.writes = 0

    def extractions(self, scope):
        return self

    @asynccontextmanager
    async def gate(self, scope):
        yield

    def get(self, source, content_hash):
        return self.cached

    def put(self, source, domain, content_hash, outputs):
        self.cached = outputs
        self.writes += 1


def message_source():
    message = SimpleNamespace(user_id="alice", agent_id="ailu", connector_id="test",
        conversation_id="chat", message_id="raw-test", revision=1, role="user",
        content_type="text", content="private-prompt", created_at="2026-11-03T00:00:00+00:00",
        content_hash="1" * 64)
    source = SourceRevision(message.message_id, 1, "task-test", "memory-grouped", "v1", "test-model", _GROUP_HASH)
    return message, source


class HookedExtractor(GroupedDailyExtractor):
    def __init__(self, stores, response):
        super().__init__(stores, object(), "test-model")
        self.response = response
        self.calls = []

    async def _create_response(self, *, scope, source, **request_kwargs):
        self.calls.append((scope, source, request_kwargs))
        return self.response


async def guard():
    pass


def response(content, finish="stop"):
    return SimpleNamespace(choices=[SimpleNamespace(finish_reason=finish,
                           message=SimpleNamespace(content=content))])


@pytest.mark.parametrize("content,finish,reason", [
    ("{}", "length", "finish"), (None, "stop", "content"),
    ("x" * 128001, "stop", "content"), ("{private-answer", "stop", "json"),
    ("{}", "stop", "group_shape"),
    (json.dumps({**{d: [] for d in STRUCTURED_DOMAINS}, "rule": [{"text": "", "status": "candidate"}]}),
     "stop", "domain_payload"),
], ids=["finish", "missing-content", "oversized-content", "json", "group", "payload"])
@pytest.mark.asyncio
async def test_failure_reason_is_closed_and_no_invalid_cache_is_written(content, finish, reason):
    stores = FakeStores()
    extractor = HookedExtractor(stores, response(content, finish))
    message, source = message_source()
    with pytest.raises(DiagnosticError) as caught:
        await extractor.extract_guarded("entity", message, source, 2048, guard)
    assert caught.value.safe_reason == reason
    assert stores.writes == 0


@pytest.mark.asyncio
async def test_hook_receives_complete_scope_source_and_original_request():
    stores = FakeStores()
    outputs = {d: [] for d in STRUCTURED_DOMAINS}
    extractor = HookedExtractor(stores, response(json.dumps(outputs)))
    message, source = message_source()
    assert await extractor.extract_guarded("entity", message, source, 2048, guard) == ()
    assert await extractor.extract_guarded("journal", message, source, 2048, guard) == ()
    assert len(extractor.calls) == stores.writes == 1
    scope, received, request = extractor.calls[0]
    assert scope.key == (message.user_id, message.agent_id, message.connector_id, message.conversation_id)
    assert received is source
    assert request["messages"][0]["content"] == _GROUP_PROMPT
    assert request["max_tokens"] == 2048 and request["model"] == "test-model"
    assert hashlib.sha256(_GROUP_PROMPT.encode()).hexdigest() == _GROUP_HASH


def test_group_and_payload_validation_keep_strict_contract():
    for invalid in ({}, {d: [{}] for d in STRUCTURED_DOMAINS}, {d: [] for d in (*STRUCTURED_DOMAINS, "extra")}):
        with pytest.raises(ValueError):
            validate_group(invalid)
    with pytest.raises(DiagnosticError) as caught:
        validate_payload("journal", {"text": "private", "occurred_at": "2026-11-03"})
    assert caught.value.safe_reason == "domain_payload"


@pytest.mark.asyncio
async def test_missing_model_choice_has_closed_finish_reason():
    extractor = HookedExtractor(FakeStores(), SimpleNamespace(choices=[]))
    message, source = message_source()
    with pytest.raises(DiagnosticError) as caught:
        await extractor.extract_guarded("entity", message, source, 2048, guard)
    assert caught.value.safe_reason == "finish"
