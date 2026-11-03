"""Provider transport controls preserve strict extraction and private diagnostics."""

from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from kernel.challenge_diagnostics import DiagnosticError, _encode_record
from kernel.memory_pipeline_models import SourceRevision
from system_memory_god.grouped_extractor import (
    GroupedDailyExtractor, _GROUP_HASH, _GROUP_PROMPT, provider_request_options,
)
from system_memory_god.storage.extraction_snapshots import STRUCTURED_DOMAINS


@pytest.mark.parametrize("url", [
    "https://api.deepseek.com", "https://api.deepseek.com/",
    "https://api.deepseek.com:443/v1", "https://api.deepseek.com/v1/",
])
def test_official_origin_disables_thinking(url):
    assert provider_request_options(url) == {"extra_body": {"thinking": {"type": "disabled"}}}


@pytest.mark.parametrize("url", [
    None, 1, object(), "https://other.example/v1", "http://api.deepseek.com",
    "https://api.deepseek.com:8443", "https://api.deepseek.com:invalid",
    "https://api.deepseek.com.evil.example", "https://user@api.deepseek.com",
    "https://:password@api.deepseek.com", "https://api.deepseek.com?thinking=high",
    "https://api.deepseek.com/#fragment", "https://api.deepseek.com/beta",
    "https://api.deepseek.com/v1/extra", "\nhttps://api.deepseek.com",
    "https://api.deepseek.com\t", "https://api.deep\nseek.com",
])
def test_other_or_ambiguous_origins_do_not_gain_provider_capability(url):
    assert provider_request_options(url) == {}


class Stores:
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

    def put(self, source, domain, content_hash, output):
        self.writes += 1
        self.cached = output


def source_message():
    message = SimpleNamespace(user_id="synthetic-user", agent_id="synthetic-agent",
        connector_id="test", conversation_id="synthetic-chat", message_id="synthetic-raw",
        revision=1, role="user", content_type="text", content="private-source-text",
        created_at="2026-11-03T00:00:00+00:00", content_hash="a" * 64)
    source = SourceRevision(message.message_id, 1, "synthetic-task", "memory-grouped", "v1",
                            "synthetic-model", _GROUP_HASH)
    return source, message


async def guard():
    pass


def response(*, finish="stop", content=None):
    if content is None:
        content = json.dumps({domain: [] for domain in STRUCTURED_DOMAINS})
    return SimpleNamespace(choices=[SimpleNamespace(finish_reason=finish,
        message=SimpleNamespace(content=content, reasoning_content="private-reasoning"))])


@pytest.mark.asyncio
@pytest.mark.parametrize("url,has_option", [(None, False), ("https://other.example", False),
                                         ("https://api.deepseek.com", True)])
async def test_provider_options_reach_normal_sdk_and_recovery_hook_identically(url, has_option):
    sdk = AsyncMock(return_value=response())
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    source, message = source_message()
    normal = GroupedDailyExtractor(Stores(), client, source.model, provider_base_url=url)
    assert await normal.extract_guarded("entity", message, source, 2048, guard) == ()

    class Hooked(GroupedDailyExtractor):
        async def _create_response(self, *, scope, source, **request_kwargs):
            self.request = request_kwargs
            return response()

    hooked = Hooked(Stores(), object(), source.model, provider_base_url=url)
    assert await hooked.extract_guarded("entity", message, source, 2048, guard) == ()
    assert hooked.request == sdk.call_args.kwargs
    request = hooked.request
    assert ("extra_body" in request) is has_option
    assert request["max_tokens"] == 2048 and request["temperature"] == 0
    assert request["model"] == source.model
    assert request["response_format"] == {"type": "json_object"}
    assert request["messages"][0] == {"role": "system", "content": _GROUP_PROMPT}
    assert source.prompt_hash == _GROUP_HASH


@pytest.mark.parametrize("finish", ["length", "content_filter", "tool_calls",
                                  "insufficient_system_resource", "aborted"])
@pytest.mark.asyncio
async def test_incomplete_response_preserves_only_closed_finish_metadata(finish):
    stores = Stores()
    sdk = AsyncMock(return_value=response(finish=finish, content="private-answer"))
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    source, message = source_message()
    extractor = GroupedDailyExtractor(stores, client, source.model)
    with pytest.raises(DiagnosticError) as caught:
        await extractor.extract_guarded("entity", message, source, 2048, guard)
    encoded = _encode_record("prepare", caught.value, "entity", 5)
    assert json.loads(encoded)["finish_reason"] == finish
    assert stores.writes == 0
    assert b"private-" not in encoded


def test_unknown_or_mutated_finish_metadata_cannot_escape_closed_record():
    for finish in ("private-finish-value", object(), {"private": "value"}):
        error = DiagnosticError("finish", "private-exception", finish_reason=finish)
        assert json.loads(_encode_record("prepare", error, "entity", 5))["finish_reason"] == "unknown"
        error.safe_finish_reason = finish
        encoded = _encode_record("prepare", error, "entity", 5)
        assert json.loads(encoded)["finish_reason"] == "unknown"
        assert b"private" not in encoded


@pytest.mark.asyncio
async def test_missing_finish_attribute_still_has_closed_finish_classification():
    sdk = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace()]))
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=sdk)))
    source, message = source_message()
    with pytest.raises(DiagnosticError) as caught:
        await GroupedDailyExtractor(Stores(), client, source.model).extract_guarded(
            "entity", message, source, 2048, guard)
    assert caught.value.safe_reason == "finish"
    assert json.loads(_encode_record("prepare", caught.value, "entity", 5))["finish_reason"] == "unknown"


def test_factory_does_not_stringify_arbitrary_provider_objects(monkeypatch, tmp_path):
    from system_memory_god import memory_factory as factory
    from system_memory_god import grouped_extractor as grouped

    class Forbidden:
        def __str__(self):
            raise AssertionError("arbitrary provider URL must not be stringified")

    seen = []
    original = grouped.GroupedDailyExtractor

    def capture(*args, **kwargs):
        seen.append(kwargs["provider_base_url"])
        return original(*args, **kwargs)

    monkeypatch.setattr(grouped, "GroupedDailyExtractor", capture)
    monkeypatch.setattr(factory, "_shared_options", lambda *args: None)
    monkeypatch.setattr(factory, "DailyMemoryRuntime", lambda *args, **kwargs: SimpleNamespace(derivation=object()))
    monkeypatch.setattr(factory, "MemoryService", lambda *args, **kwargs: SimpleNamespace())
    profile = SimpleNamespace(derived_root=tmp_path)
    for origin in (None, Forbidden(), "https://api.deepseek.com", httpx.URL("https://api.deepseek.com/")):
        factory.create_memory_service(profile, semantic_mode="none", raw=object(),
                                      client=SimpleNamespace(base_url=origin), model="synthetic-model")
    assert seen == [None, None, "https://api.deepseek.com", "https://api.deepseek.com/"]
