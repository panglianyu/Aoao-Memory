"""First-call provenance uses real scoped Raw leases and durable history."""

import asyncio

import pytest

from kernel.raw_message_models import ConflictError, MessageDraft, Scope
from kernel.raw_message_service import RawMessageService
from system_memory_god.challenge_runtime import ChallengeRuntime
from system_memory_god.challenge_store import UserBinding


@pytest.fixture
def origin(tmp_path):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="synthetic-model")
    raw = RawMessageService(runtime._profile(UserBinding("synthetic-user", "a" * 64)))
    asyncio.run(raw.initialize_profile())
    scope = raw.configured_scopes()[0]
    asyncio.run(raw.append(scope, MessageDraft("source-one", scope.conversation_id,
        "session-one", "user", "ordinary synthetic source", "text",
        "2026-11-05T00:00:00+00:00", "web", scope.user_id)))
    tasks = []
    for _ in range(6):
        task = raw.store.claim(scope, "owner-one", lease_seconds=300)
        if task is not None and task["target_domain"] != "vector":
            tasks.append(task)
    assert len(tasks) == 5
    yield raw.store, scope, tasks
    asyncio.run(raw.close())


def test_first_live_group_has_shared_fresh_origin(origin):
    store, scope, tasks = origin
    assert {store.group_budget_origin(scope, task["task_id"], task["lease_token"])
            for task in tasks} == {"fresh"}


@pytest.mark.parametrize("state", ["retry", "failed", "succeeded", "old_owner"])
def test_prior_group_activity_is_never_refunded(origin, state):
    store, scope, tasks = origin
    old, current = tasks[:2]
    if state == "succeeded":
        store.complete(scope, old["task_id"], old["lease_token"], ())
    elif state in {"retry", "failed"}:
        store.fail(scope, old["task_id"], old["lease_token"], "processor_error",
                   **({"retry_after": 0} if state == "retry" else {}))
    else:
        with store._db(write=True) as db:
            db.execute("UPDATE derivation_outbox SET lease_owner=? WHERE task_id=?",
                       ("older-owner", old["task_id"]))
    assert store.group_budget_origin(scope, current["task_id"], current["lease_token"]) == "legacy_unknown"


def test_stale_or_other_scope_cannot_supply_first_call_proof(origin):
    store, scope, tasks = origin
    current = tasks[0]
    with pytest.raises(ConflictError):
        store.group_budget_origin(scope, current["task_id"], "wrong-token")
    other = Scope(scope.agent_id, "another-user", scope.connector_id, scope.conversation_id)
    with pytest.raises(ConflictError):
        store.group_budget_origin(other, current["task_id"], current["lease_token"])
