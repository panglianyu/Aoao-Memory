"""Synthetic, offline one-shot recovery authorization and call-budget tests."""

import asyncio
from contextlib import closing, contextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

import pytest

from kernel import raw_message_recovery as recovery
from kernel.raw_message_models import ConflictError, MessageDraft
from kernel.raw_message_worker import RawMessageWorker
from system_memory_god.challenge_runtime import ChallengeRuntime
from system_memory_god.challenge_store import ChallengeStore
from system_memory_god.grouped_extractor import _GROUP_PROMPT
from system_memory_god.storage.extraction_snapshots import STRUCTURED_DOMAINS
from system_memory_god.storage.scoped_stores import ScopedStores


@contextmanager
def connection(path):
    with closing(sqlite3.connect(path)) as db, db:
        yield db


def test_frozen_readonly_sees_committed_wal_without_changing_db_wal_or_shm(tmp_path):
    path = tmp_path / "quiescent.db"
    code = ("import os,sqlite3,sys;db=sqlite3.connect(sys.argv[1]);"
            "db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0');"
            "db.execute('CREATE TABLE committed(value INTEGER)');"
            "db.execute('INSERT INTO committed VALUES(123)');db.commit();os._exit(0)")
    result = subprocess.run([sys.executable, "-c", code, str(path)],
                            capture_output=True, timeout=10)
    assert result.returncode == 0
    files = [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]
    before = {item.name: hashlib.sha256(item.read_bytes()).hexdigest() for item in files}
    with recovery.readonly(path, frozen=True) as db:
        assert db.execute("SELECT value FROM committed").fetchone()[0] == 123
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert {item.name: hashlib.sha256(item.read_bytes()).hexdigest() for item in files} == before


@pytest.fixture
def frozen(tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "_CALL_BUDGET_CLAIMED", False)
    short = tempfile.TemporaryDirectory(prefix="r-", dir=tempfile.gettempdir())
    root = Path(short.name)
    (root / ".amc-recovery-clone").write_bytes(b"AMC-RECOVERY-CLONE-1\n")
    journal = ChallengeStore(root, model="synthetic-model")

    async def setup():
        binding, _ = await journal.begin("synthetic-user", "request-one", "1" * 64, 2)
        await journal.checkpoint(binding, "request-one", 2)
        await journal.begin("synthetic-user", "request-two", "2" * 64, 2)
        await journal.checkpoint(binding, "request-two", 2)
        return binding

    binding = asyncio.run(setup())
    runtime = ChallengeRuntime(root, client=object(), model="synthetic-model")
    profile = runtime._profile(binding)
    from kernel.raw_message_service import RawMessageService
    raw = RawMessageService(profile)
    asyncio.run(raw.initialize_profile())
    scope = raw.configured_scopes()[0]
    store = raw.store
    for index in range(4):
        store.append(scope, MessageDraft(message_id=f"synthetic-message-{index}",
            conversation_id=scope.conversation_id, session_id="synthetic-session", role="user",
            content=f"synthetic original {index}", content_type="text", source="web", sender_id="synthetic",
            created_at="2026-11-03T00:00:00+00:00", derivations=profile.derivations))
    # This fixture represents the approved historical terminal state. Its rows
    # are synthetic and never passed to a cloud client or production service.
    with connection(profile.database) as db:
        db.execute("UPDATE derivation_outbox SET status='succeeded',attempts=1")
        db.execute("UPDATE derivation_outbox SET status='failed',attempts=3,last_error_code='invalid_output' "
                   "WHERE processor='memory-grouped' AND (message_id<>'synthetic-message-0' "
                   "OR target_domain IN ('entity','journal','schedule'))")
        db.execute("INSERT INTO derivation_events(task_id,action,occurred_at,error_code) "
                   "SELECT task_id,'failed',0,'invalid_output' FROM derivation_outbox WHERE status='failed'")
    manifest = recovery.build_manifest(root)
    row = next(row for row in manifest["groups"][0]["rows"] if row["message_id"] == "synthetic-message-0")
    stores = ScopedStores(profile.derived_root)
    stores.partition_registry().begin(recovery.scope_of(row))
    stores.extractions(recovery.scope_of(row)).put(recovery.source_of(row), "entity", row["content_hash"],
                                                 {domain: [] for domain in STRUCTURED_DOMAINS})
    yield SimpleNamespace(root=root, manifest=manifest, store=store, scope=scope,
                          stores=stores, profile=profile)
    short.cleanup()


def receipt(frozen, name="receipt.jsonl"):
    return recovery.RecoveryReceipt(frozen.root / name, frozen.manifest)


def committed(frozen):
    value = receipt(frozen)
    for group in frozen.manifest["groups"]:
        value.commit_group(frozen.root, group)
    return value


def test_preflight_readonly_and_fixed_cache_counts(frozen):
    before = frozen.profile.database.read_bytes()
    assert recovery.preflight(frozen.root, frozen.manifest) == {
        "selected": 18, "databases": 1, "cached_keys": 1, "uncached_keys": 3}
    assert frozen.profile.database.read_bytes() == before
    assert not (frozen.root / ".amc-recovery-consumed").exists()


def test_manifest_only_main_preserves_full_quiescent_wal_inventory(frozen, tmp_path, monkeypatch):
    import os
    from kernel import raw_message_clone
    from Tools import recover_memory_challenge_failed as helper

    (frozen.root / ".amc-entry-v1").write_bytes(b"AMC-ENTRY-1.0\n")
    (frozen.root / ".amc-entry.lock").write_bytes(b"")
    code = ("import os,sqlite3,sys\nconnections=[]\n"
            "for path in sys.argv[1:]:\n"
            " db=sqlite3.connect(path);connections.append(db)\n"
            " db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0')\n"
            " table,column=('challenge_identity','value') if path.endswith('challenge.sqlite3') else ('connectors','display_name')\n"
            " db.execute('BEGIN IMMEDIATE');db.execute(f\"UPDATE {table} SET {column}={column}||'x'\")\n"
            " db.execute(f'UPDATE {table} SET {column}=substr({column},1,length({column})-1)');db.commit()\n"
            "os._exit(0)\n")
    result = subprocess.run([sys.executable, "-c", code, str(frozen.root / "challenge.sqlite3"),
                             str(frozen.profile.database)], capture_output=True, timeout=10)
    assert result.returncode == 0
    assert frozen.profile.database.with_name("messages.db-wal").stat().st_size > 32
    assert frozen.profile.database.with_name("messages.db-shm").stat().st_size == 32768

    def inventory(lease=None):
        values = {}
        for path in sorted(frozen.root.rglob("*")):
            if not path.is_file():
                continue
            if path.name == ".amc-entry.lock" and lease is not None:
                with os.fdopen(os.dup(lease.descriptor), "rb") as stream:
                    stream.seek(0)
                    body = stream.read()
            else:
                body = path.read_bytes()
            values[path.relative_to(frozen.root).as_posix()] = hashlib.sha256(body).hexdigest()
        return values

    before = inventory()
    calls = []

    def proof_spy(root, *, expected_sha256, runtime_lease, manifest=None):
        assert root == frozen.root and expected_sha256 == "0" * 64
        assert inventory(runtime_lease) == before
        if manifest is not None:
            # The final proof must run after real preflight returned its counts.
            import inspect
            assert inspect.currentframe().f_back.f_locals["report"]["selected"] == 18
            assert manifest == frozen.manifest
        calls.append(manifest is not None)
        return {"prior_calls": 0}

    monkeypatch.setattr(raw_message_clone, "validate_clone_proof", proof_spy)
    output = tmp_path / "manifest-outside-data-root.json"
    assert helper.main(["--data-root", str(frozen.root), "--manifest", str(output),
                        "--clone-proof-sha256", "0" * 64, "--manifest-only"]) == 0
    assert json.loads(output.read_text()) == frozen.manifest
    assert calls == [False, True]
    assert inventory() == before
    assert not (frozen.root / ".amc-recovery-consumed").exists()


@pytest.mark.parametrize("field,value", [("attempts", 4), ("revision", 2), ("model", "wrong"),
                                          ("content_hash", "0" * 64)])
def test_manifest_conflict_rejected_globally(frozen, field, value):
    frozen.manifest["groups"][0]["rows"][0][field] = value
    with pytest.raises((ValueError, ConflictError)):
        recovery.preflight(frozen.root, frozen.manifest)
    with connection(frozen.profile.database) as db:
        assert db.execute("SELECT count(*) FROM derivation_outbox WHERE status='failed'").fetchone()[0] == 18


def test_cas_preserves_attempts_old_failures_and_bound_authorization(frozen):
    value = committed(frozen)
    try:
        assert value.all_committed
        with connection(frozen.profile.database) as db:
            assert db.execute("SELECT count(*) FROM derivation_outbox WHERE status='pending' "
                              "AND attempts=3 AND last_error_code='invalid_output'").fetchone()[0] == 18
            assert db.execute("SELECT count(*) FROM derivation_events WHERE action='failed'").fetchone()[0] == 18
            actions = db.execute("SELECT action FROM derivation_events WHERE action LIKE 'operator_recovery:%'").fetchall()
            assert len(actions) == 18
            assert {row[0] for row in actions} == {"operator_recovery:once:" + recovery.manifest_digest(frozen.manifest)}
        with pytest.raises(OSError):
            receipt(frozen, "alternate-receipt.jsonl")
    finally:
        value.close()


def test_cas_race_rolls_back_every_selected_row(frozen):
    value = receipt(frozen)
    with connection(frozen.profile.database) as db:
        db.execute("UPDATE derivation_outbox SET attempts=4 WHERE task_id=?",
                   (frozen.manifest["groups"][0]["rows"][-1]["task_id"],))
    try:
        with pytest.raises(ConflictError):
            value.commit_group(frozen.root, frozen.manifest["groups"][0])
        assert not value.all_committed
        with connection(frozen.profile.database) as db:
            assert db.execute("SELECT count(*) FROM derivation_outbox WHERE status='pending'").fetchone()[0] == 0
            assert db.execute("SELECT count(*) FROM derivation_events WHERE action LIKE 'operator_recovery:%'").fetchone()[0] == 0
    finally:
        value.close()


def test_receipt_failure_after_sql_commit_stops_and_denies_second_authorization(frozen, monkeypatch):
    value = receipt(frozen)
    original = value.append

    def failed(state, **fields):
        if state == "committed":
            raise OSError("synthetic disk failure")
        original(state, **fields)

    monkeypatch.setattr(value, "append", failed)
    try:
        with pytest.raises(OSError):
            value.commit_group(frozen.root, frozen.manifest["groups"][0])
        assert not value.all_committed
        with pytest.raises(OSError):
            receipt(frozen, "new-path.jsonl")
    finally:
        value.close()


@pytest.mark.asyncio
async def test_selected_worker_preclaim_and_exactly_one_extra_attempt(frozen):
    value = committed(frozen)
    selected = {row["task_id"]: row["attempts"] for row in frozen.manifest["groups"][0]["rows"]}
    with connection(frozen.profile.database) as db:
        # A nonselected pending task is older and would win an unfiltered claim.
        excluded = db.execute("SELECT task_id FROM derivation_outbox WHERE status='succeeded' LIMIT 1").fetchone()[0]
        db.execute("UPDATE derivation_outbox SET status='pending',attempts=0,available_at=-1 WHERE task_id=?", (excluded,))

    class Bus:
        async def derive_raw_message(self, scope, task, message, guard, complete):
            assert task["task_id"] in selected and task["attempts"] == 4
            await guard()
            await complete(())

    sources = {row["task_id"]: row for row in frozen.manifest["groups"][0]["rows"]}
    worker = RawMessageWorker(frozen.store, Bus(), (frozen.scope,), selected_claims=selected,
                              selected_sources=sources)
    try:
        assert sum([await worker.run_once() for _ in range(20)]) == 18
        with connection(frozen.profile.database) as db:
            assert db.execute("SELECT status,attempts FROM derivation_outbox WHERE task_id=?", (excluded,)).fetchone() == ("pending", 0)
        assert await worker.run_once() == 0
    finally:
        await worker.close()
        value.close()


@pytest.mark.asyncio
async def test_normal_worker_attempt_cap_unchanged(frozen):
    value = committed(frozen)

    class Bus:
        async def derive_raw_message(self, *args):
            pytest.fail("normal worker must not execute fourth attempt")

    worker = RawMessageWorker(frozen.store, Bus(), (frozen.scope,))
    try:
        assert await worker.run_once() == 0
    finally:
        await worker.close()
        value.close()


def request(row):
    return {"model": row["model"], "max_tokens": 2048, "temperature": 0,
            "response_format": {"type": "json_object"}, "messages": [
                {"role": "system", "content": _GROUP_PROMPT}, {"role": "user", "content": "synthetic"}]}


class Client:
    def __init__(self, error=None):
        self.chat = SimpleNamespace(completions=self)
        self.calls = 0
        self.error = error

    async def create(self, **kwargs):
        self.calls += 1
        if self.error:
            raise self.error
        return object()


def split_two_databases(frozen):
    from kernel.raw_message_service import stable_id

    user = "synthetic-second-user"
    key = hashlib.sha256(user.encode()).hexdigest()
    target = frozen.root / "users" / key / "raw" / "messages.db"
    target.parent.mkdir(parents=True)
    with connection(frozen.profile.database) as original, connection(target) as copied:
        original.backup(copied)
    for path, keep in ((frozen.profile.database, ("synthetic-message-0", "synthetic-message-1")),
                       (target, ("synthetic-message-2", "synthetic-message-3"))):
        with connection(path) as db:
            db.execute("DELETE FROM derivation_events WHERE task_id IN "
                       "(SELECT task_id FROM derivation_outbox WHERE message_id NOT IN (?,?))", keep)
            db.execute("DELETE FROM derivation_outbox WHERE message_id NOT IN (?,?)", keep)
            db.execute("DELETE FROM raw_messages WHERE message_id NOT IN (?,?)", keep)
            if path == target:
                owner = "amc-" + key
                conversation = stable_id("amc-agent", owner, "amc-web", "memory")
                db.execute("UPDATE raw_messages SET user_id=?,conversation_id=?", (owner, conversation))
                db.execute("UPDATE connectors SET user_id=?,account_ref=?", (owner, owner))
                db.execute("UPDATE conversations SET user_id=?,conversation_id=?", (owner, conversation))
    with connection(frozen.root / "challenge.sqlite3") as db:
        db.execute("INSERT INTO challenge_users VALUES(?,?)", (key, user))
    frozen.manifest = recovery.build_manifest(frozen.root)
    assert len(frozen.manifest["groups"]) == 2
    return target


def test_multidatabase_preflight_checks_every_group_before_any_cas(frozen):
    target = split_two_databases(frozen)
    assert recovery.preflight(frozen.root, frozen.manifest)["databases"] == 2
    with connection(target) as db:
        db.execute("UPDATE derivation_outbox SET attempts=4 WHERE status='failed'")
    with pytest.raises(ConflictError):
        recovery.preflight(frozen.root, frozen.manifest)
    with connection(frozen.profile.database) as db:
        assert db.execute("SELECT count(*) FROM derivation_outbox WHERE status='pending'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM derivation_events WHERE action LIKE 'operator_recovery:%'").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_multidatabase_partial_commit_forbids_model_and_reentry(frozen):
    split_two_databases(frozen)
    recovery.preflight(frozen.root, frozen.manifest)
    value = receipt(frozen)
    client = Client()
    budget = recovery.RecoveryCallBudget(frozen.manifest, value, client, time.monotonic() + 10)
    first, second = frozen.manifest["groups"]
    try:
        value.commit_group(frozen.root, first)
        with connection(frozen.root / second["database"]) as db:
            db.execute("UPDATE derivation_outbox SET attempts=4 WHERE status='failed'")
        with pytest.raises(ConflictError):
            value.commit_group(frozen.root, second)
        assert not value.all_committed
        row = next(row for row in first["rows"] if row["message_id"] != "synthetic-message-0")
        with pytest.raises(ValueError):
            await budget.create(scope=recovery.scope_of(row), source=recovery.source_of(row), **request(row))
        assert client.calls == 0
        with pytest.raises(OSError):
            receipt(frozen, "another.jsonl")
    finally:
        value.close()


@pytest.mark.asyncio
async def test_process_unique_three_calls_cache_excluded_scope_bound_and_ram_reused(frozen):
    value = committed(frozen)
    client = Client()
    try:
        budget = recovery.RecoveryCallBudget(frozen.manifest, value, client, time.monotonic() + 10)
        for row in frozen.manifest["groups"][0]["rows"]:
            scope, source = recovery.scope_of(row), recovery.source_of(row)
            if row["message_id"] == "synthetic-message-0":
                with pytest.raises(ValueError):
                    await budget.create(scope=scope, source=source, **request(row))
            else:
                first = await budget.create(scope=scope, source=source, **request(row))
                assert await budget.create(scope=scope, source=source, **request(row)) is first
        assert client.calls == budget.calls == 3
        with pytest.raises(RuntimeError):
            recovery.RecoveryCallBudget(frozen.manifest, value, client, time.monotonic() + 10)
        row = frozen.manifest["groups"][0]["rows"][-1]
        with pytest.raises(ValueError):
            await budget.create(scope=replace(recovery.scope_of(row), user_id="other-owner"),
                                source=recovery.source_of(row), **request(row))
        entries = [json.loads(line) for line in (frozen.root / "receipt.jsonl").read_text().splitlines()]
        assert sum(entry["state"] == "call-started" for entry in entries) == 3
    finally:
        value.close()


@pytest.mark.asyncio
async def test_exception_and_durable_marker_failure_never_repeat_cloud_call(frozen, monkeypatch):
    value = committed(frozen)
    client = Client(RuntimeError("synthetic secret text"))
    row = next(row for row in frozen.manifest["groups"][0]["rows"] if row["message_id"] != "synthetic-message-0")
    try:
        budget = recovery.RecoveryCallBudget(frozen.manifest, value, client, time.monotonic() + 10)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await budget.create(scope=recovery.scope_of(row), source=recovery.source_of(row), **request(row))
        assert client.calls == 1
        next_row = next(item for item in frozen.manifest["groups"][0]["rows"]
                        if item["message_id"] not in {row["message_id"], "synthetic-message-0"})
        monkeypatch.setattr(value, "append", lambda *args, **kwargs: (_ for _ in ()).throw(OSError()))
        with pytest.raises(OSError):
            await budget.create(scope=recovery.scope_of(next_row), source=recovery.source_of(next_row), **request(next_row))
        assert client.calls == 1
    finally:
        value.close()


def test_marker_and_hardlink_manifest_paths_rejected(frozen, tmp_path):
    from Tools.recover_memory_challenge_failed import _load, _private_write

    path = frozen.root / "manifest.json"
    _private_write(path, frozen.manifest)
    import os
    os.link(path, frozen.root / "linked.json")
    with pytest.raises(ValueError):
        _load(path)
    (frozen.root / ".amc-recovery-clone").unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        recovery.preflight(frozen.root, frozen.manifest)


@pytest.mark.asyncio
async def test_selected_recovery_uses_real_prepare_domain_ledger_ack_and_keeps_requests_incomplete(frozen):
    from system_memory_god.memory_factory import create_memory_service
    from kernel.raw_message_service import RawMessageService

    class StructuredClient(Client):
        async def create(self, **kwargs):
            self.calls += 1
            payloads = {"entity": [{"subject": "synthetic", "attribute": "color", "value": "blue"}],
                        "graph": [{"subject": "synthetic", "relation": "knows", "object": "test"}],
                        "rule": [{"text": "synthetic preference", "status": "candidate"}],
                        "journal": [{"text": "synthetic event", "occurred_at": None}],
                        "schedule": [{"text": "synthetic plan", "scheduled_at": None}]}
            return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
                message=SimpleNamespace(content=json.dumps(payloads)))])

    value = committed(frozen)
    client = StructuredClient()
    budget = recovery.RecoveryCallBudget(frozen.manifest, value, client, time.monotonic() + 60)
    extractor = recovery.recovery_extractor(frozen.stores, frozen.manifest["model"], budget)
    selected = {row["task_id"]: row["attempts"] for row in frozen.manifest["groups"][0]["rows"]}
    sources = {row["task_id"]: row for row in frozen.manifest["groups"][0]["rows"]}
    raw = RawMessageService(frozen.profile, selected_claims=selected, selected_sources=sources)
    service = create_memory_service(frozen.profile, raw=raw, extractor=extractor,
                                    embedding="synthetic", semantic_mode="disabled")
    worker = RawMessageWorker(raw.store, service.runtime.bus, (frozen.scope,), selected_claims=selected,
                              selected_sources=sources)
    raw.worker = worker
    try:
        assert sum([await worker.run_once() for _ in range(20)]) == 18
        assert client.calls == 3
        with connection(frozen.profile.database) as db:
            assert db.execute("SELECT count(*) FROM derivation_results").fetchone()[0] == 15
            assert db.execute("SELECT count(*) FROM derivation_outbox WHERE status='succeeded'").fetchone()[0] == 24
            assert db.execute("SELECT count(*) FROM derivation_events WHERE action='failed'").fetchone()[0] == 18
        scope = recovery.scope_of(frozen.manifest["groups"][0]["rows"][0])
        runs = frozen.stores.runs(scope)
        assert len(runs.get_many(tuple(selected))) == 18
        assert {run["state"] for run in runs.get_many(tuple(selected)).values()} == {"ledger_written"}
        from Tools.recover_memory_challenge_failed import _terminal
        assert _terminal(frozen.root, frozen.manifest) == {"succeeded": 18, "failed": 0,
            "additional_attempts": 18, "original_incomplete": 2}
    finally:
        await service.close()
        value.close()


@pytest.mark.asyncio
async def test_claim_rechecks_full_source_before_leasing(frozen):
    value = committed(frozen)
    row = frozen.manifest["groups"][0]["rows"][0]
    with connection(frozen.profile.database) as db:
        db.execute("UPDATE derivation_outbox SET model='changed' WHERE task_id=?", (row["task_id"],))
    selected = {row["task_id"]: row["attempts"]}
    try:
        task = frozen.store.claim_partition(frozen.scope, "synthetic-worker",
            selected_attempts=selected, selected_sources={row["task_id"]: row})
        assert task is None
        with connection(frozen.profile.database) as db:
            assert db.execute("SELECT status,attempts FROM derivation_outbox WHERE task_id=?",
                              (row["task_id"],)).fetchone() == ("pending", 3)
    finally:
        value.close()


@pytest.mark.asyncio
async def test_deadline_rejects_late_synchronous_completion_and_expired_start():
    from Tools.recover_memory_challenge_failed import _bounded

    async def blocked():
        time.sleep(.02)
        return "finished"

    with pytest.raises(TimeoutError):
        await _bounded(blocked(), time.monotonic() + .005)
    started = False

    async def expired():
        nonlocal started
        started = True

    with pytest.raises(TimeoutError):
        await _bounded(expired(), time.monotonic() - 1)
    assert started is False


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["client", "sink", "budget", "embedding", "runtime", "retained"])
async def test_constructor_failures_close_every_previously_created_owner(frozen, monkeypatch, stage):
    import openai
    from Tools import recover_memory_challenge_failed as helper
    from system_memory_god import challenge_runtime
    from system_memory_god.storage import shared_embedding_owner

    monkeypatch.setattr(helper, "_RETAINED", [])
    monkeypatch.setattr(helper, "_api_settings", lambda _: {
        "model_name": "synthetic-model", "base_url": "https://example.invalid", "api_key": "synthetic"})
    created, closed = [], []

    class Resource:
        def __init__(self, name):
            if stage == name:
                raise RuntimeError("synthetic constructor failure")
            self.name = name
            created.append(name)

        async def close(self):
            closed.append(self.name)
            if stage == "retained" and self.name == "client":
                helper._RETAINED.append(asyncio.create_task(asyncio.sleep(10)))

        @property
        def settlement_status(self):
            return "settled" if self.name in closed else "unsettled"

        @property
        def stats(self):
            return {"clean_close": self.name in closed, "active_generation_count": 0,
                    "ipc_inflight": 0, "model_owner_count": 0}

    monkeypatch.setattr(openai, "AsyncOpenAI", lambda **kwargs: Resource("client"))
    monkeypatch.setattr(helper, "DiagnosticSink", lambda *args: Resource("sink"))
    monkeypatch.setattr(shared_embedding_owner, "SharedEmbeddingOwner", lambda **kwargs: Resource("embedding"))

    def failed_runtime(*args, **kwargs):
        raise RuntimeError("synthetic runtime constructor failure")

    monkeypatch.setattr(challenge_runtime, "ChallengeRuntime", failed_runtime)
    if stage == "budget":
        monkeypatch.setattr(helper, "RecoveryCallBudget", lambda *args: (_ for _ in ()).throw(RuntimeError()))
    args = SimpleNamespace(data_root=frozen.root, diagnostics_file=frozen.root / "diagnostics.jsonl", api_config=None)
    value = receipt(frozen)
    try:
        result = await helper.execute(args, frozen.manifest, value, time.monotonic())
        assert result["state"] == ("unknown" if stage == "retained" else "failed")
        assert result["closed"] is (stage != "retained")
        assert set(created) == set(closed)
        assert result["calls"] == 0
        assert bool(helper._RETAINED) is (stage == "retained")
        assert not value.committed
        final = json.loads((frozen.root / "receipt.jsonl").read_text().splitlines()[-1])
        assert final["state"] == result["state"]
        assert final["report"] == result
        assert "elapsed_seconds" in final["report"]
    finally:
        pending = [task for task in helper._RETAINED if isinstance(task, asyncio.Task)]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        value.close()
