"""Terminal Raw failures reach the existing synchronous publication barrier."""

import asyncio
from dataclasses import replace
import time

import pytest

from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import DerivationSpec
from tests.test_daily_memory_early_acceptance import SyntheticExtractor, daily, ingress
from tests.test_daily_memory_runtime import publication_until
from tests.test_raw_message_service import profile
from tests.test_raw_message_store import draft, opened


def source_with_task(tmp_path):
    store, base = opened(tmp_path)
    scope = replace(base, connector_id="wx-main", conversation_id="group-1")
    store.append(scope, draft(derivations=(
        DerivationSpec("synthetic", "v1", "entity", "test", "a" * 64),)))
    return store, scope


def fail_source(store, scope):
    task = store.claim(scope, "synthetic-worker", now=100)
    assert task is not None
    store.fail(scope, task["task_id"], task["lease_token"], "invalid_output", now=100)


@pytest.mark.parametrize("phase", ["pending", "leased", "retry", "failed", "succeeded"])
def test_raw_publication_state_distinguishes_terminal_from_retryable(tmp_path, phase):
    store, scope = source_with_task(tmp_path)
    if phase != "pending":
        task = store.claim(scope, "synthetic-worker", now=100)
        if phase in {"retry", "failed"}:
            store.fail(scope, task["task_id"], task["lease_token"], "invalid_output",
                       retry_after=1 if phase == "retry" else None, now=100)
        elif phase == "succeeded":
            store.complete(scope, task["task_id"], task["lease_token"], (), now=100)
    before = store.partition_state(scope)
    state = store.partition_publication_state(scope)
    assert type(before) is tuple and len(before) == 2
    assert state == (*before, phase == "failed")
    assert state[1] is (phase == "succeeded")
    assert store.list_derivations(scope, "msg-1")[0]["status"] == (
        "pending" if phase == "retry" else phase)


@pytest.mark.parametrize("retire", ["edit", "forget", "retract"])
def test_old_or_inactive_source_failure_is_not_terminal(tmp_path, retire):
    store, scope = source_with_task(tmp_path)
    fail_source(store, scope)
    assert store.partition_publication_state(scope)[2] is True
    if retire == "edit":
        store.edit(scope, "msg-1", "new valid source", expected_revision=1,
                   edited_at="2026-09-18T14:01:00+08:00")
    else:
        getattr(store, retire)(scope, "msg-1")
    assert store.partition_publication_state(scope)[2] is False
    assert store.list_derivations(scope, "msg-1")


def test_terminal_query_is_conversation_scoped(tmp_path):
    store, scope = source_with_task(tmp_path)
    store.register_conversation(replace(scope, conversation_id=None),
                                "other", "wx-main", "remote-other", "group", "Other")
    other = replace(scope, conversation_id="other")
    fail_source(store, scope)
    assert store.partition_publication_state(scope)[2] is True
    assert store.partition_publication_state(other)[1:] == (True, False)


@pytest.mark.parametrize("field", ["agent_id", "user_id", "connector_id", "conversation_id"])
def test_terminal_query_does_not_expose_a_different_scope(tmp_path, field):
    store, scope = source_with_task(tmp_path)
    fail_source(store, scope)
    other = replace(scope, **{field: "other-identity"})
    assert store.partition_publication_state(other) == (0, True, False)


def test_edited_current_revision_failure_is_terminal(tmp_path):
    store, scope = source_with_task(tmp_path)
    store.edit(scope, "msg-1", "current revised source", expected_revision=1,
               edited_at="2026-09-18T14:01:00+08:00")
    fail_source(store, scope)
    assert store.partition_publication_state(scope)[2] is True
    assert store.get(scope, "msg-1").revision == 2


class FailingExtractor(SyntheticExtractor):
    async def extract(self, domain, message, source, max_output_tokens):
        if domain != "vector" and message.content == "synthetic terminal source":
            raise ValueError("synthetic invalid output")
        return await super().extract(domain, message, source, max_output_tokens)


@pytest.mark.asyncio
async def test_raw_terminal_failure_stops_wait_and_preserves_other_partition(tmp_path):
    from kernel.raw_message_profile import ConversationBinding
    from system_memory_god.memory_service import MemoryService

    settings = profile(tmp_path / "raw")
    connector = replace(settings.connectors[0], conversations=(
        *settings.connectors[0].conversations,
        ConversationBinding("second_console", "second-chat", "web", "Second")))
    from system_memory_god.derivation import daily_specs
    settings = replace(settings, connectors=(connector,), derived_root=tmp_path / "derived",
                       derivations=daily_specs("synthetic-acceptance"))
    async with daily(tmp_path, settings=settings, extractor=FailingExtractor()) as (raw, bus, runtime):
        scopes = raw.configured_scopes()
        partitions = tuple(PipelineScope(s.user_id, s.agent_id, s.connector_id,
                                        s.conversation_id) for s in scopes)
        for partition in partitions:
            await publication_until(runtime, partition, lambda state: state["status"] == "current")
        raw.worker.max_attempts = 1
        scope, message_id = await ingress(raw, bus, "synthetic terminal source")
        deadline = time.monotonic() + 10
        while not any(row["status"] == "failed"
                      for row in raw.store.list_derivations(scope, message_id)):
            assert time.monotonic() < deadline
            await asyncio.sleep(.01)
        service = MemoryService(runtime, profile_id="memory-full-v1")
        state = await asyncio.wait_for(service.wait_until_published(
            scope, deadline=time.monotonic() + 60), timeout=1)
        assert set(state) == {"status", "reason", "terminal", "raw_generation",
                              "published_raw_generation", "snapshot_generation"}
        assert (state["status"], state["reason"], state["terminal"]) == (
            "unavailable", "derived_unavailable", True)
        assert raw.store.get(scope, message_id).content == "synthetic terminal source"
        assert (await runtime.semantic_publication_state(
            partitions[1], deadline=time.monotonic() + 1))["status"] == "current"


@pytest.mark.asyncio
async def test_terminal_add_fails_fast_without_ack_or_duplicate_raw(tmp_path):
    import sqlite3
    from tests.test_memory_challenge_runtime import FakeFactory, add, runtime

    factory = FakeFactory()
    failed = False

    def service_factory(profile, **options):
        service = factory(profile, **options)

        async def terminal_publication(scope, *, deadline):
            nonlocal failed
            if not failed:
                fail_source(service.raw.store, scope)
                failed = True
            assert service.raw.store.partition_publication_state(scope)[2] is True
            return {"status": "unavailable", "reason": "derived_unavailable", "terminal": True}

        service.wait_until_published = terminal_publication
        return service

    challenge = runtime(tmp_path, service_factory)
    try:
        for _ in range(2):
            with pytest.raises(RuntimeError, match="publication unavailable"):
                await asyncio.wait_for(add(challenge), timeout=1)
        raw = factory.services[0].raw
        scope = raw.configured_scopes()[0]
        assert len(await raw.replay(scope, scope.conversation_id)) == 1
        assert sum(row["status"] == "failed" for row in raw.store.list_derivations(
            scope, (await raw.replay(scope, scope.conversation_id))[0].message_id)) == 1
        with sqlite3.connect(tmp_path / "challenge.sqlite3") as db:
            assert db.execute("SELECT progress,complete FROM challenge_requests").fetchall() == [(1, 0)]
    finally:
        await challenge.close()
