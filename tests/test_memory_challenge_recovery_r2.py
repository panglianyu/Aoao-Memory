"""Synthetic R1b checkpoint -> one R2 continuation, with no cloud or model."""

import asyncio
import hashlib
import json
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from kernel import raw_message_recovery as base
from kernel import raw_message_recovery_r2 as r2
from kernel.memory_pipeline_models import canonical
from kernel.raw_message_models import ConflictError
from kernel.raw_message_service import RawMessageService
from kernel.raw_message_worker import RawMessageWorker
from system_memory_god.memory_factory import create_memory_service
from system_memory_god.storage.scoped_stores import ScopedStores
from tests.test_memory_challenge_recovery import frozen as source_checkpoint_fixture  # noqa: F401
from tests.test_memory_challenge_recovery import Client, committed, connection, request, split_two_databases


def payloads():
    return {"entity": [{"subject": "synthetic", "attribute": "color", "value": str(i)} for i in range(3)],
            "graph": [{"subject": "synthetic", "relation": "knows", "object": str(i)} for i in range(3)],
            "rule": [{"text": f"synthetic preference {i}", "status": "candidate"} for i in range(3)],
            "journal": [{"text": f"synthetic event {i}", "occurred_at": None} for i in range(3)],
            "schedule": [{"text": f"synthetic plan {i}", "scheduled_at": None} for i in range(3)]}


@pytest.fixture
def checkpoint(source_checkpoint_fixture):  # noqa: F811
    frozen = source_checkpoint_fixture
    split_two_databases(frozen)
    cached = next(row for group in frozen.manifest["groups"] for row in group["rows"]
                  if row["message_id"] == "synthetic-message-0")
    cache = frozen.stores.extractions(base.scope_of(cached))
    cache.purge(cached["message_id"])
    cache.put(base.source_of(cached), cached["target_domain"], cached["content_hash"], payloads())
    old_receipt = committed(frozen)

    async def prior_success():
        rows = {row["task_id"]: row for group in frozen.manifest["groups"] for row in group["rows"]
                if row["message_id"] == "synthetic-message-0"}
        claims = {key: row["attempts"] for key, row in rows.items()}
        raw = RawMessageService(frozen.profile, selected_claims=claims, selected_sources=rows)
        extractor = base.recovery_extractor(frozen.stores, frozen.manifest["model"], SimpleNamespace(
            provider_base_url=None, create=lambda **kwargs: pytest.fail("cached old tasks must not call SDK")))
        service = create_memory_service(frozen.profile, raw=raw, extractor=extractor,
                                       embedding="synthetic", semantic_mode="disabled")
        worker = RawMessageWorker(raw.store, service.runtime.bus, (frozen.scope,),
                                  selected_claims=claims, selected_sources=rows)
        raw.worker = worker
        try:
            assert sum([await worker.run_once() for _ in range(4)]) == 3
        finally:
            await service.close()

    asyncio.run(prior_success())
    for group in frozen.manifest["groups"]:
        path = frozen.root / group["database"]
        with connection(path) as db:
            for row in group["rows"]:
                if row["message_id"] in {"synthetic-message-1", "synthetic-message-2"}:
                    db.execute("UPDATE derivation_outbox SET status='failed',attempts=4,available_at=123.0 WHERE task_id=?",
                               (row["task_id"],))
                    db.execute("INSERT INTO derivation_events(task_id,action,occurred_at,error_code) "
                               "VALUES(?,'failed',123,'invalid_output')", (row["task_id"],))
        scope = base.scope_of(group["rows"][0])
        stores = ScopedStores(path.parents[1] / "derived")
        if stores.partition_registry().snapshot(scope) is None:
            stores.partition_registry().begin(scope)
        with stores.runs(scope)._db.transaction():
            pass
    prior = frozen.root.parent / (frozen.root.name + "-prior")
    control = frozen.root.parent / (frozen.root.name + "-r2")
    prior.mkdir()
    control.mkdir()
    source = prior / "supervisor-source.py"
    source.write_text("# fixed synthetic supervisor\n")
    proof = frozen.root / ".amc-clone-proof.json"
    proof.write_text('{"schema":"amc-clone-proof-r1b"}\n')
    (prior / "manifest.json").write_text(canonical(frozen.manifest))
    old_receipt.append("call-started", key="synthetic-key-one")
    old_receipt.append("call-started", key="synthetic-key-two")
    report = {"state": "unknown", "selected": 18, "closed": True, "calls": 2, "succeeded": 3,
              "failed": 10, "additional_attempts": 13, "original_incomplete": 2, "embedding_closed": True,
              "active_generation_count": 0, "ipc_inflight": 0, "model_owner_count": 0, "elapsed_seconds": 541.344}
    old_receipt.append("unknown", report=report)
    old_receipt.close()
    (prior / "receipt.jsonl").write_bytes((frozen.root / "receipt.jsonl").read_bytes())
    (prior / "supervisor.json").write_text(canonical({"job_active": 0, "success": False, "forced": True,
        "status": "helper_unsettled", "helper": report}))
    (prior / "source-pin.json").write_text(canonical({"clone_proof_sha256": hashlib.sha256(proof.read_bytes()).hexdigest()}))
    policy = r2._Policy(frozen.root, prior, control, source)
    code = ("import os,sqlite3,sys\nconnections=[]\n"
            "for path in sys.argv[1:]:\n"
            " db=sqlite3.connect(path);connections.append(db)\n"
            " db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0')\n"
            " table,column=('challenge_identity','value') if path.endswith('challenge.sqlite3') else ('connectors','display_name')\n"
            " db.execute('BEGIN IMMEDIATE');db.execute(f\"UPDATE {table} SET {column}={column}||'x'\")\n"
            " db.execute(f'UPDATE {table} SET {column}=substr({column},1,length({column})-1)');db.commit()\n"
            "os._exit(0)\n")
    result = subprocess.run([sys.executable, "-c", code, str(frozen.root / "challenge.sqlite3"),
        *(str(path) for path in frozen.root.glob("users/*/raw/messages.db"))], capture_output=True, timeout=10)
    assert result.returncode == 0
    manifest = r2.build_manifest(frozen.root, prior, runtime_lease=None, _policy=policy)
    yield SimpleNamespace(base=frozen, root=frozen.root, prior=prior, control=control, policy=policy, manifest=manifest)
    import shutil
    shutil.rmtree(prior)
    shutil.rmtree(control)


def preflight(value, manifest=None):
    selected = manifest or value.manifest
    return r2.preflight(value.root, selected, prior_control=value.prior,
        checkpoint_sha256=selected["checkpoint"]["sha256"], runtime_lease=None, _policy=value.policy)


def authorize(value):
    receipt = r2.R2Receipt(value.control / "receipt.jsonl", value.manifest, _policy=value.policy)
    for group in value.manifest["groups"]:
        receipt.commit_group(value.root, group)
    return receipt


def test_exact_checkpoint_preserves_three_normal_proofs_and_only_selects_fifteen(checkpoint):
    assert preflight(checkpoint) == {"selected": 18, "remaining": 15, "retained": 3, "prior_calls": 2, "new_call_limit": 3}
    assert len(checkpoint.manifest["retained"]) == 3
    assert sum(len(group["rows"]) for group in checkpoint.manifest["groups"]) == 15
    assert sum(item["row"]["attempts"] == 4 for item in checkpoint.manifest["retained"]) == 3
    assert set(checkpoint.manifest["lineage"]) == {
        "manifest", "receipt", "supervisor", "clone_proof", "source_pin", "supervisor_source", "prior_calls"}


@pytest.mark.parametrize("fault", ["attempt", "lease", "pending_availability", "retained_anchor", "old_calls", "old_job", "marker"])
def test_changed_checkpoint_or_lineage_fails_before_r2_marker(checkpoint, fault):
    group = checkpoint.manifest["groups"][0]
    row = group["rows"][0]
    path = checkpoint.root / group["database"]
    if fault == "old_calls":
        with (checkpoint.prior / "receipt.jsonl").open("a") as stream:
            stream.write('{"state":"call-started"}\n')
    elif fault == "old_job":
        (checkpoint.prior / "supervisor.json").write_text('{"job_active":1}')
    elif fault == "marker":
        (checkpoint.root / ".amc-recovery-consumed").write_bytes(b"changed")
    else:
        with connection(path) as db:
            if fault == "attempt":
                db.execute("UPDATE derivation_outbox SET attempts=6 WHERE task_id=?", (row["task_id"],))
            elif fault == "lease":
                db.execute("UPDATE derivation_outbox SET lease_token='synthetic' WHERE task_id=?", (row["task_id"],))
            elif fault == "pending_availability":
                pending = next((group, row) for group in checkpoint.manifest["groups"] for row in group["rows"]
                               if row["expected_status"] == "pending")
                with connection(checkpoint.root / pending[0]["database"]) as other:
                    other.execute("UPDATE derivation_outbox SET available_at=9999999999 WHERE task_id=?", (pending[1]["task_id"],))
            else:
                retained = checkpoint.manifest["retained"][0]
                with connection(checkpoint.root / retained["database"]) as other:
                    other.execute("DELETE FROM derivation_results WHERE task_id=?", (retained["row"]["task_id"],))
    with pytest.raises((ConflictError, ValueError)):
        preflight(checkpoint)
    assert not (checkpoint.root / ".amc-recovery-r2-consumed").exists()


def test_authorization_preserves_attempts_successes_and_all_prior_history(checkpoint):
    before = r2.terminal(checkpoint.root, checkpoint.manifest)
    receipt = authorize(checkpoint)
    try:
        assert before == {"succeeded": 3, "failed": 10, "additional_attempts": 0, "original_incomplete": 2}
        assert receipt.all_committed
        for group in checkpoint.manifest["groups"]:
            with connection(checkpoint.root / group["database"]) as db:
                for row in group["rows"]:
                    assert db.execute("SELECT status,attempts,last_error_code FROM derivation_outbox WHERE task_id=?",
                        (row["task_id"],)).fetchone() == ("pending", row["attempts"], "invalid_output")
                    assert db.execute("SELECT count(*) FROM derivation_events WHERE task_id=? AND action LIKE 'operator_recovery:%'",
                        (row["task_id"],)).fetchone()[0] == 2
        with pytest.raises(OSError):
            r2.R2Receipt(checkpoint.control / "receipt.jsonl", checkpoint.manifest, _policy=checkpoint.policy)
    finally:
        receipt.close()


def test_partial_database_commit_never_allows_model_or_reauthorization(checkpoint):
    receipt = r2.R2Receipt(checkpoint.control / "receipt.jsonl", checkpoint.manifest, _policy=checkpoint.policy)
    first, second = checkpoint.manifest["groups"]
    try:
        receipt.commit_group(checkpoint.root, first)
        with connection(checkpoint.root / second["database"]) as db:
            db.execute("UPDATE derivation_outbox SET attempts=attempts+1 WHERE task_id=?", (second["rows"][0]["task_id"],))
        with pytest.raises(ConflictError):
            receipt.commit_group(checkpoint.root, second)
        assert not receipt.all_committed
        with pytest.raises(OSError):
            r2.R2Receipt(checkpoint.control / "receipt.jsonl", checkpoint.manifest, _policy=checkpoint.policy)
    finally:
        receipt.close()


def test_unselected_database_purge_is_part_of_global_preflight(checkpoint):
    from kernel.raw_message_models import Scope
    from kernel.raw_message_store import RawMessageStore

    path = checkpoint.root / "users" / ("0" * 64) / "raw/messages.db"
    store = RawMessageStore(path)
    store.register_connector(Scope("aux-agent", "aux-user"), "aux", "web", "aux", "aux")
    with connection(path) as db:
        db.execute("INSERT INTO purge_receipts VALUES('synthetic','2026-11-03T00:00:00+00:00',0)")
    with pytest.raises(ConflictError, match="purge"):
        preflight(checkpoint)


def test_availability_compare_and_set_race_rolls_back_authorization(checkpoint):
    receipt = r2.R2Receipt(checkpoint.control / "receipt.jsonl", checkpoint.manifest, _policy=checkpoint.policy)
    group = next(group for group in checkpoint.manifest["groups"]
                 if any(row["expected_status"] == "failed" for row in group["rows"]))
    row = next(row for row in group["rows"] if row["expected_status"] == "failed")
    with connection(checkpoint.root / group["database"]) as db:
        db.execute("UPDATE derivation_outbox SET available_at=124 WHERE task_id=?", (row["task_id"],))
    try:
        with pytest.raises(ConflictError):
            receipt.commit_group(checkpoint.root, group)
        assert not receipt.all_committed
        with connection(checkpoint.root / group["database"]) as db:
            assert db.execute("SELECT count(*) FROM derivation_events WHERE action LIKE 'operator_recovery:r2:%'").fetchone()[0] == 0
    finally:
        receipt.close()


@pytest.mark.asyncio
async def test_abort_rechecks_after_waiting_for_model_lock(checkpoint):
    receipt = authorize(checkpoint)
    entered, release = asyncio.Event(), asyncio.Event()

    class FailedClient(Client):
        async def create(self, **kwargs):
            self.calls += 1
            entered.set()
            await release.wait()
            raise RuntimeError("synthetic SDK failure")

    client = FailedClient()
    budget = r2.R2CallBudget(checkpoint.manifest, receipt, client, time.monotonic() + 10)
    rows = []
    seen = set()
    for group in checkpoint.manifest["groups"]:
        for row in group["rows"]:
            key = base.full_key(base.scope_of(row), base.source_of(row))
            if key not in seen:
                seen.add(key)
                rows.append(row)

    async def call(row):
        return await budget.create(scope=base.scope_of(row), source=base.source_of(row), **request(row))

    first = asyncio.create_task(call(rows[0]))
    await entered.wait()
    second = asyncio.create_task(call(rows[1]))
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    try:
        assert all(isinstance(value, Exception) for value in results)
        assert budget.terminal_event.is_set() and client.calls == budget.calls == 1
        assert sum(json.loads(line)["state"] == "call-started" for line in
                   (checkpoint.control / "receipt.jsonl").read_text().splitlines()) == 1
    finally:
        receipt.close()


@pytest.mark.asyncio
async def test_publication_abort_exits_promptly_and_close_persists_worker_failure(checkpoint):
    from Tools.recover_memory_challenge_failed import _published_or_abort

    receipt = authorize(checkpoint)
    event = asyncio.Event()
    published_entered = asyncio.Event()
    cancelled = asyncio.Event()

    class Runtime:
        async def _published(self, service, deadline):
            published_entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    waiting = asyncio.create_task(_published_or_abort(Runtime(), None, time.monotonic() + 10, event))
    await published_entered.wait()
    row = checkpoint.manifest["groups"][0]["rows"][0]
    scope = base.scope_of(row)
    from kernel.raw_message_models import Scope
    raw_scope = Scope(scope.agent_id, scope.user_id, scope.connector_id, scope.conversation_id)
    from kernel.raw_message_store import RawMessageStore

    class FailedBus:
        async def derive_raw_message(self, *args):
            raise ValueError("synthetic domain failure")

    worker = RawMessageWorker(RawMessageStore(checkpoint.root / checkpoint.manifest["groups"][0]["database"]),
        FailedBus(), (raw_scope,), selected_claims={row["task_id"]: row["attempts"]},
        selected_sources={row["task_id"]: row}, on_failure=event.set)
    try:
        assert await worker.run_once() == 0
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(waiting, 1)
        await worker.close()
        assert cancelled.is_set() and worker.settlement_status == "settled"
        with connection(worker.store.database) as db:
            assert db.execute("SELECT status,attempts FROM derivation_outbox WHERE task_id=?", (row["task_id"],)).fetchone() == ("failed", row["attempts"] + 1)
    finally:
        await worker.close()
        receipt.close()


@pytest.mark.asyncio
async def test_fifteen_normal_derivations_complete_eighteen_without_touching_prior_three(checkpoint):
    from system_memory_god.challenge_runtime import ChallengeRuntime
    from system_memory_god.challenge_store import UserBinding

    class StructuredClient(Client):
        async def create(self, **kwargs):
            assert kwargs["extra_body"] == {"thinking": {"type": "disabled"}}
            assert kwargs["max_tokens"] == 2048 and kwargs["response_format"] == {"type": "json_object"}
            self.calls += 1
            return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
                message=SimpleNamespace(content=json.dumps(payloads())))])

    receipt = authorize(checkpoint)
    client = StructuredClient()
    budget = r2.R2CallBudget(checkpoint.manifest, receipt, client, time.monotonic() + 60,
                             provider_base_url="https://api.deepseek.com")
    runtime = ChallengeRuntime(checkpoint.root, client=object(), model=checkpoint.manifest["model"])
    services = []
    try:
        total = 0
        for group in checkpoint.manifest["groups"]:
            profile = runtime._profile(UserBinding(group["user_id"], group["user_key"]))
            claims = {row["task_id"]: row["attempts"] for row in group["rows"]}
            sources = {row["task_id"]: row for row in group["rows"]}
            raw = RawMessageService(profile, selected_claims=claims, selected_sources=sources, on_failure=budget.abort)
            extractor = base.recovery_extractor(ScopedStores(profile.derived_root), checkpoint.manifest["model"], budget)
            service = create_memory_service(profile, raw=raw, extractor=extractor,
                                           embedding="synthetic", semantic_mode="disabled")
            services.append(service)
            worker = RawMessageWorker(raw.store, service.runtime.bus, raw.configured_scopes(),
                                      selected_claims=claims, selected_sources=sources, on_failure=budget.abort)
            raw.worker = worker
            total += sum([await worker.run_once() for _ in range(len(claims) + 1)])
            await service.close()
        assert total == 15 and client.calls == budget.calls == 3
        assert checkpoint.manifest["lineage"]["prior_calls"] + budget.calls == 5
        assert not budget.terminal_event.is_set()
        assert r2.terminal(checkpoint.root, checkpoint.manifest) == {
            "succeeded": 18, "failed": 0, "additional_attempts": 15, "original_incomplete": 2}
        for item in checkpoint.manifest["retained"]:
            with connection(checkpoint.root / item["database"]) as db:
                assert db.execute("SELECT status,attempts FROM derivation_outbox WHERE task_id=?",
                                  (item["row"]["task_id"],)).fetchone() == ("succeeded", 4)
    finally:
        for service in services:
            await service.close()
        receipt.close()
