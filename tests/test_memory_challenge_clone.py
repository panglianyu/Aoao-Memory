"""Offline synthetic clone migration; no real challenge data or model clients."""

from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile

import pytest

from kernel import raw_message_clone as clone
from kernel.memory_pipeline_models import PipelineScope, canonical
from kernel.raw_message_service import stable_id
from system_memory_god.storage.daily_vector_index import _INDEX_DDL
from system_memory_god.storage.pipeline_database import PipelineDatabase


def write_json(path, value):
    path.write_text(canonical(value) + "\n", encoding="utf-8")


def identity(path):
    value = path.stat()
    return [str(value.st_dev), str(value.st_ino)]


def lineage(root):
    control, old = root / "old-control", root / "old-clone"
    control.mkdir()
    old.mkdir()
    (old / ".amc-recovery-consumed").write_bytes(b"consumed\n")
    manifest = {"clone": str(old), "model": "synthetic-model", "incomplete_requests": [],
                "groups": [{"database": "synthetic-db-one", "rows": [{"attempts": 3}] * 9},
                           {"database": "synthetic-db-two", "rows": [{"attempts": 3}] * 9}]}
    write_json(control / "manifest.json", manifest)
    report = {"state": "unknown", "closed": True, "calls": 0, "succeeded": 0, "failed": 0,
              "additional_attempts": 0, "embedding_closed": True, "original_incomplete": 2,
              "active_generation_count": 0, "ipc_inflight": 0, "model_owner_count": 0,
              "elapsed_seconds": 541.11}
    rows = [{"state": "authorized", "manifest": hashlib.sha256(canonical(manifest).encode()).hexdigest()}]
    for group in manifest["groups"]:
        rows.extend({"state": state, "database": group["database"]} for state in ("prepared", "committed"))
    rows.append({"state": "unknown", "report": report})
    (control / "receipt.jsonl").write_text("".join(canonical(row) + "\n" for row in rows), encoding="utf-8")
    write_json(control / "supervisor.json", {"job_active": 0, "helper_exit_code": 3,
                                           "forced": False, "success": False})
    return old, control


def add_authority(root, index, *, real=False):
    key = hashlib.sha256(f"synthetic-user-{index}".encode()).hexdigest()
    owner = "amc-" + key
    scope = PipelineScope(owner, "amc-agent", "amc-web", stable_id("amc-agent", owner, "amc-web", "memory"))
    derived = root / "users" / key / "derived"
    relative = f"users/{key}/derived/{scope.partition_id}/vector_index.db"
    if real:
        code = (
            "import sys;sys.path[:0]=[sys.argv[1],sys.argv[1]+'/apps'];"
            "from pathlib import Path;from kernel.memory_pipeline_models import PipelineScope;"
            "from system_memory_god.storage.scoped_stores import ScopedStores;"
            "from system_memory_god.storage.daily_vector_index import DailyVectorIndex;"
            "import json;scope=PipelineScope(*json.loads(sys.argv[3]));"
            "DailyVectorIndex(ScopedStores(Path(sys.argv[2])),lambda rows:[[1.,0.] for _ in rows])._collection(scope)"
        )
        result = subprocess.run([sys.executable, "-c", code, str(Path(__file__).resolve().parents[1]),
                                 str(derived), canonical(scope.key)], capture_output=True, timeout=30)
        assert result.returncode == 0, "synthetic Chroma setup failed"
    else:
        vector = derived / scope.partition_id / "vectors" / "chroma.sqlite3"
        vector.parent.mkdir(parents=True)
        with closing(sqlite3.connect(vector)) as db:
            db.execute("CREATE TABLE embeddings_queue(id TEXT)")
            db.commit()
        authority = PipelineDatabase(derived, scope, "vector_index.db", _INDEX_DDL, 0x414C5649)
        with authority.transaction() as db:
            db.execute("INSERT INTO vector_identity VALUES(?,?)", identity(vector))
    return relative, scope


def fake_validator(root, relative, scope, *, authority):
    item, actual = clone._authority(root, relative)
    assert scope == actual
    vector = item.path.parent / "vectors/chroma.sqlite3"
    assert clone._queue_ids(vector) == ()
    if authority:
        item.validate_existing(lambda db: clone._expect_identity(db, identity(vector)))


@pytest.fixture
def fixture():
    temporary = tempfile.TemporaryDirectory(prefix="c-", dir=tempfile.gettempdir())
    root = Path(temporary.name)
    original = root / "original"
    original.mkdir()
    (original / ".amc-entry-v1").write_bytes(b"AMC-ENTRY-1.0\n")
    (original / ".amc-entry.lock").write_bytes(b"\0")
    (original / "synthetic-raw.bin").write_bytes(b"unchanged original bytes")
    authorities = [add_authority(original, index) for index in range(3)]
    backup = root / "backup"
    shutil.copytree(original, backup)
    old, old_control = lineage(root)
    with clone._Inventory(original) as inventory:
        rows = inventory.rows
    policy = clone._Policy(len(rows), sum(row["bytes"] for row in rows), root / "new-clone",
                           root / "new-control", old, old_control, original=original, backup=backup)
    yield root, original, backup, policy, rows, authorities
    temporary.cleanup()


def prepare(fixture, **kwargs):
    _, original, backup, policy, _, _ = fixture
    return clone.prepare_clone(original, backup, policy.clone, policy.control,
                               _policy=policy, _validator=fake_validator, **kwargs)


def proof_hash(policy):
    return hashlib.sha256((policy.clone / clone.PROOF_NAME).read_bytes()).hexdigest()


def test_all_authorities_migrate_only_two_columns_and_proof_is_externally_bound(fixture):
    _, original, backup, policy, before, authorities = fixture
    with pytest.raises(ValueError):
        fake_validator(backup, authorities[0][0], authorities[0][1], authority=True)
    assert prepare(fixture)["authorities"] == 3
    assert clone.validate_clone_proof(policy.clone, expected_sha256=proof_hash(policy),
                                     _policy=policy, _validator=fake_validator)["prior_calls"] == 0
    with clone._Inventory(original) as source, clone._Inventory(backup) as saved:
        assert source.rows == saved.rows == before
    with clone._Inventory(policy.clone, extras=frozenset(clone._EXTRAS)) as copied:
        changed = {old["path"] for old, new in zip(before, copied.rows) if old != new}
        assert changed == {relative for relative, _ in authorities}
    pin = json.loads((policy.control / "clone-proof-pin.json").read_text())
    assert pin["proof_sha256"] == proof_hash(policy)
    assert pin["proof_path"] == (policy.clone / clone.PROOF_NAME).as_posix()
    with pytest.raises(ValueError):
        prepare(fixture)


def test_backup_hash_conflict_stops_before_any_new_output(fixture):
    _, _, backup, policy, _, _ = fixture
    (backup / "synthetic-raw.bin").write_bytes(b"changed")
    with pytest.raises(ValueError):
        prepare(fixture)
    assert not policy.clone.exists() and not policy.control.exists()


def test_equal_inventory_at_unapproved_original_paths_rejected_before_mutation(fixture):
    root, original, backup, policy, _, _ = fixture
    # The pair has identical bytes and expected counts. Only explicit trusted
    # policy injection may select alternate roots; the CLI exposes no such flag.
    forbidden = replace(policy, original=root / "approved-original", backup=root / "approved-backup")
    with pytest.raises(ValueError, match="fixed original and backup paths required"):
        clone.prepare_clone(original, backup, policy.clone, policy.control,
                            _policy=forbidden, _validator=fake_validator)
    assert not policy.clone.exists() and not policy.control.exists()


@pytest.mark.parametrize("fault", ["identity", "schema", "scope", "sidefile"])
def test_global_authority_preflight_rejects_without_any_identity_cas(fixture, fault):
    _, original, backup, policy, _, authorities = fixture
    relative, _ = authorities[-1]
    path = original / relative
    if fault == "sidefile":
        path.with_name(path.name + "-journal").write_bytes(b"foreign")
    else:
        with closing(sqlite3.connect(path)) as db:
            if fault == "identity":
                db.execute("UPDATE vector_identity SET inode='wrong'")
            elif fault == "scope":
                db.execute("UPDATE pipeline_identity SET scope='[]'")
            else:
                db.execute("CREATE TABLE extra(value TEXT)")
            db.commit()
    shutil.rmtree(backup)
    shutil.copytree(original, backup)
    with clone._Inventory(original) as current:
        policy = replace(policy, files=len(current.rows), size=sum(row["bytes"] for row in current.rows))
    bad = (*fixture[:3], policy, fixture[4], authorities)
    with pytest.raises((ValueError, sqlite3.Error)):
        prepare(bad)
    assert not (policy.clone / clone.PROOF_NAME).exists()
    for first, _ in authorities[:-1]:
        with closing(sqlite3.connect(policy.clone / first)) as db:
            assert db.execute("SELECT device,inode FROM vector_identity").fetchall() == [tuple(identity((original / first).parent / "vectors/chroma.sqlite3"))]


def test_single_link_input_rule_rejects_hardlinks(fixture):
    import os
    _, original, _, policy, _, _ = fixture
    os.link(original / "synthetic-raw.bin", original / "linked.bin")
    with pytest.raises(ValueError):
        prepare(fixture)
    assert not policy.control.exists()


def test_more_than_eight_authorities_rejected_before_identity_writes(fixture):
    _, original, backup, policy, _, authorities = fixture
    for index in range(3, 9):
        authorities.append(add_authority(original, index))
    shutil.rmtree(backup)
    shutil.copytree(original, backup)
    with clone._Inventory(original) as source:
        policy = replace(policy, files=len(source.rows), size=sum(row["bytes"] for row in source.rows),
                         original=original, backup=backup)
    updated = (*fixture[:3], policy, fixture[4], authorities)
    with pytest.raises(ValueError):
        prepare(updated)
    assert not (policy.clone / clone.PROOF_NAME).exists()


def test_copy_race_rejected_before_any_identity_writes(fixture, monkeypatch):
    policy = fixture[3]
    original_write = clone.open_private_file

    def changed(path, **kwargs):
        descriptor = original_write(path, **kwargs)
        if path == policy.clone / "synthetic-raw.bin":
            import os
            os.write(descriptor, b"unexpected-prefix")
        return descriptor

    monkeypatch.setattr(clone, "open_private_file", changed)
    with pytest.raises(ValueError):
        prepare(fixture)
    assert not (policy.clone / clone.PROOF_NAME).exists()


def test_partial_migration_never_produces_usable_proof_or_reenters(fixture, monkeypatch):
    policy = fixture[3]
    append = clone._Receipt.append
    seen = 0

    def interrupted(self, state, **fields):
        nonlocal seen
        if state == "prepared":
            seen += 1
            if seen == 2:
                raise OSError("synthetic interruption")
        return append(self, state, **fields)

    monkeypatch.setattr(clone._Receipt, "append", interrupted)
    with pytest.raises(OSError):
        prepare(fixture)
    assert not (policy.clone / clone.PROOF_NAME).exists()
    with pytest.raises(ValueError):
        prepare(fixture)
    with pytest.raises((ValueError, FileNotFoundError)):
        clone.validate_clone_proof(policy.clone, expected_sha256="0" * 64,
                                   _policy=policy, _validator=fake_validator)


@pytest.mark.parametrize("fault", ["proof", "clone", "original", "old_receipt", "old_job", "migration_receipt"])
def test_recovery_guard_rejects_changed_or_unknown_proof(fixture, fault):
    prepare(fixture)
    _, original, _, policy, _, _ = fixture
    digest = proof_hash(policy)
    if fault == "proof":
        digest = "0" * 64
    elif fault == "clone":
        (policy.clone / "synthetic-raw.bin").write_bytes(b"changed")
    elif fault == "original":
        (original / "synthetic-raw.bin").write_bytes(b"changed")
    elif fault == "old_receipt":
        with (policy.old_control / "receipt.jsonl").open("ab") as stream:
            stream.write(b'{"state":"call-started"}\n')
    elif fault == "old_job":
        write_json(policy.old_control / "supervisor.json", {"job_active": 1})
    else:
        with (policy.control / "migration-receipt.jsonl").open("ab") as stream:
            stream.write(b'{"state":"unknown"}\n')
    with pytest.raises(ValueError):
        clone.validate_clone_proof(policy.clone, expected_sha256=digest, _policy=policy, _validator=fake_validator)


def test_actual_cli_requires_proof_before_receipt_or_cas(fixture, monkeypatch):
    from Tools import recover_memory_challenge_failed as helper

    prepare(fixture)
    policy = fixture[3]
    monkeypatch.setattr(helper, "RecoveryReceipt", lambda *args: pytest.fail("must not authorize CAS"))
    with pytest.raises(ValueError):
        helper.main(["--data-root", str(policy.clone), "--manifest", str(policy.control / "manifest.json"),
                     "--clone-proof-sha256", "0" * 64, "--inspect-only"])


def test_real_chroma_copy_rejected_then_rebound_without_schema_relaxation(fixture):
    root, original, backup, policy, _, _ = fixture
    # A new synthetic root uses the actual pinned Chroma schema; the embedding
    # callback is never used because the collection contains zero records.
    original = root / "real-original"
    original.mkdir()
    (original / ".amc-entry-v1").write_bytes(b"AMC-ENTRY-1.0\n")
    (original / ".amc-entry.lock").write_bytes(b"\0")
    relative, scope = add_authority(original, 9, real=True)
    backup = root / "real-backup"
    shutil.copytree(original, backup)
    from system_memory_god.storage.scoped_stores import ScopedStores
    from system_memory_god.storage.daily_vector_index import DailyVectorIndex
    with pytest.raises(ValueError):
        DailyVectorIndex.validate_existing(ScopedStores((backup / relative).parents[1]), scope)
    with clone._Inventory(original) as source:
        policy = replace(policy, files=len(source.rows), size=sum(row["bytes"] for row in source.rows),
                         original=original, backup=backup)
    clone.prepare_clone(original, backup, policy.clone, policy.control, _policy=policy)
    DailyVectorIndex.validate_existing(ScopedStores((policy.clone / relative).parents[1]), scope)
    assert clone.validate_clone_proof(policy.clone, expected_sha256=proof_hash(policy), _policy=policy)["authorities"] == 1
