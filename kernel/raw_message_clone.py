"""Explicit, offline filesystem-identity migration of a verified challenge copy.

Only the copied vector authority rows are writable. Source and backup bytes are
pinned read-only, and successful proofs are separate from recovery permission.
"""

from __future__ import annotations

from contextlib import ExitStack, closing
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import BinaryIO, Callable

from kernel.challenge_diagnostics import (
    _pinned_directories, check_private_path, ensure_private_directory, open_private_file,
)
from kernel.memory_pipeline_models import PipelineScope, canonical
from kernel.project_workspace_io import open_bound_read_file

PROOF_NAME = ".amc-clone-proof.json"
PROOF_SCHEMA = "amc-clone-proof-r1b"
EXPECTED_FILES = 133
EXPECTED_BYTES = 62692914
CLONE_ROOT = Path("G:/amc26/recovery-1.8-503r1b")
CONTROL_ROOT = Path("G:/amc26/recovery-r1b-control")
OLD_CLONE_ROOT = Path("G:/amc26/recovery-1.8-503r1")
OLD_CONTROL_ROOT = Path("G:/amc26/recovery-r1-control")
ORIGINAL_ROOT = Path("G:/amc26/service-1.8")
BACKUP_ROOT = Path("G:/amc26/backups/recovery-r1-original-1.8")
_EXTRAS = {PROOF_NAME, ".amc-recovery-clone"}
_AUTHORITY = re.compile(r"users/([0-9a-f]{64})/derived/([0-9a-f]{64})/vector_index\.db")


@dataclass(frozen=True)
class _Policy:
    files: int = EXPECTED_FILES
    size: int = EXPECTED_BYTES
    clone: Path = CLONE_ROOT
    control: Path = CONTROL_ROOT
    old_clone: Path = OLD_CLONE_ROOT
    old_control: Path = OLD_CONTROL_ROOT
    original: Path = ORIGINAL_ROOT
    backup: Path = BACKUP_ROOT


def _pairs(rows):
    result = {}
    for key, value in rows:
        if key in result:
            raise ValueError("duplicate clone metadata field")
        result[key] = value
    return result


def _decode(body: bytes):
    def denied(_value):
        raise ValueError("invalid clone metadata number")
    return json.loads(body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=denied)


def _ordinary(root: Path) -> None:
    if not root.is_absolute() or root.resolve() != root or ".." in root.parts:
        raise ValueError("ordinary absolute clone path required")
    check_private_path(root, directory=True)


def _identity(path: Path) -> list[str]:
    info = path.lstat()
    check_private_path(path)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("ordinary clone file required")
    return [str(info.st_dev), str(info.st_ino)]


def _hash(stream, maximum: int = 64 * 1024**2) -> tuple[int, str]:
    before = os.fstat(stream.fileno())
    stream.seek(0)
    digest, size = hashlib.sha256(), 0
    while body := stream.read(64 * 1024):
        size += len(body)
        if size > maximum:
            raise ValueError("clone input too large")
        digest.update(body)
    after = os.fstat(stream.fileno())
    if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or size != before.st_size):
        raise ValueError("clone input changed")
    return size, digest.hexdigest()


class _Inventory:
    """Pin all ordinary files and directories; selected target DBs may be writable."""

    def __init__(self, root: Path, *, extras=frozenset(), writable=frozenset(), lease=None):
        self.root, self.extras, self.writable = root, extras, writable
        self.lease = lease
        self.stack = ExitStack()
        self.handles: dict[str, BinaryIO] = {}
        self.rows: list[dict] = []

    def __enter__(self):
        try:
            _ordinary(self.root)
            pending = [self.root]
            directories = 0
            while pending:
                directory = pending.pop()
                self.stack.enter_context(_pinned_directories(directory))
                directories += 1
                if directories > 256:
                    raise ValueError("clone directory bound exceeded")
                for path in sorted(directory.iterdir()):
                    info = path.lstat()
                    if getattr(info, "st_file_attributes", 0) & 1024 or path.is_symlink():
                        raise ValueError("linked clone entry denied")
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(path)
                        continue
                    relative = path.relative_to(self.root).as_posix()
                    check_private_path(path)
                    if relative in self.extras:
                        continue
                    if relative == ".amc-entry.lock" and self.lease is not None:
                        if (self.lease.root != self.root or _identity(path)
                                != [str(os.fstat(self.lease.descriptor).st_dev),
                                    str(os.fstat(self.lease.descriptor).st_ino)]):
                            raise ValueError("clone lease identity changed")
                        stream = os.fdopen(os.dup(self.lease.descriptor), "rb")
                    else:
                        stream = open_bound_read_file(self.root, path)
                    try:
                        size, digest = _hash(stream)
                        if _identity(path) != [str(os.fstat(stream.fileno()).st_dev), str(os.fstat(stream.fileno()).st_ino)]:
                            raise ValueError("clone file identity changed")
                        self.rows.append({"path": relative, "bytes": size, "sha256": digest})
                        if len(self.rows) > 256:
                            raise ValueError("clone file bound exceeded")
                        if relative not in self.writable:
                            self.handles[relative] = self.stack.enter_context(stream)
                            stream = None
                    finally:
                        if stream is not None:
                            stream.close()
            self.rows.sort(key=lambda row: row["path"])
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *_args):
        self.stack.close()

    def recheck(self) -> None:
        with _Inventory(self.root, extras=self.extras, writable=self.writable, lease=self.lease) as current:
            if current.rows != self.rows:
                raise ValueError("clone inventory changed")


def _read_pinned(stack: ExitStack, path: Path, *, maximum=256 * 1024) -> tuple[bytes, dict]:
    stack.enter_context(_pinned_directories(path.parent))
    check_private_path(path)
    stream = stack.enter_context(open_bound_read_file(path.parent, path))
    size, digest = _hash(stream, maximum)
    stream.seek(0)
    return stream.read(maximum + 1), {"path": path.as_posix(), "sha256": digest}


def _lineage(stack: ExitStack, policy: _Policy) -> tuple[dict, dict]:
    _ordinary(policy.old_clone)
    _ordinary(policy.old_control)
    bodies, files = {}, {}
    for name, path in (("manifest", policy.old_control / "manifest.json"),
                       ("receipt", policy.old_control / "receipt.jsonl"),
                       ("supervisor", policy.old_control / "supervisor.json"),
                       ("consumed", policy.old_clone / ".amc-recovery-consumed")):
        bodies[name], files[name] = _read_pinned(stack, path)
    manifest = _decode(bodies["manifest"])
    if (type(manifest) is not dict or manifest.get("clone") != str(policy.old_clone)
            or len(manifest.get("groups", ())) != 2
            or sum(len(group["rows"]) for group in manifest["groups"]) != 18
            or any(type(row["attempts"]) is not int or row["attempts"] != 3
                   for group in manifest["groups"] for row in group["rows"])):
        raise ValueError("old recovery manifest conflict")
    lines = bodies["receipt"].splitlines()
    if not 2 <= len(lines) <= 64 or any(len(line) > 4096 for line in lines):
        raise ValueError("old recovery receipt bound conflict")
    events = [_decode(line) for line in lines]
    if any(type(event) is not dict or event.get("state") == "call-started" for event in events):
        raise ValueError("old recovery call budget unavailable")
    final = events[-1].get("report")
    supervisor = _decode(bodies["supervisor"])
    if (type(final) is not dict or final.get("state") != "unknown" or final.get("closed") is not True
            or any(type(final.get(key)) is not int or final[key] != 0
                   for key in ("calls", "succeeded", "failed", "additional_attempts",
                               "active_generation_count", "ipc_inflight", "model_owner_count"))
            or final.get("embedding_closed") is not True or final.get("original_incomplete") != 2
            or type(final.get("elapsed_seconds")) not in {int, float}
            or not math.isfinite(final["elapsed_seconds"]) or not 0 < final["elapsed_seconds"] < 600
            or type(supervisor) is not dict or type(supervisor.get("job_active")) is not int
            or supervisor["job_active"] != 0 or supervisor.get("helper_exit_code") != 3
            or supervisor.get("forced") is not False or supervisor.get("success") is not False):
        raise ValueError("old recovery retirement not proven")
    expected = {group["database"] for group in manifest["groups"]}
    authorized = [event for event in events if event.get("state") == "authorized"]
    if (len(authorized) != 1 or authorized[0].get("manifest") != hashlib.sha256(canonical(manifest).encode()).hexdigest()
            or {event.get("database") for event in events if event.get("state") == "prepared"} != expected
            or {event.get("database") for event in events if event.get("state") == "committed"} != expected
            or any(event.get("state") not in {"authorized", "prepared", "committed", "unknown"} for event in events)
            or not bodies["consumed"]):
        raise ValueError("old recovery lineage conflict")
    return {"old_control_root": policy.old_control.as_posix(), "old_clone_root": policy.old_clone.as_posix(),
            **files, "calls": 0, "closed": True, "job_active": 0}, manifest


def _authority(root: Path, relative: str):
    from system_memory_god.storage.daily_vector_index import _INDEX_DDL
    from system_memory_god.storage.pipeline_database import PipelineDatabase
    from kernel.raw_message_service import stable_id

    match = _AUTHORITY.fullmatch(relative)
    if match is None:
        raise ValueError("invalid clone authority path")
    path = root / relative
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=1)) as db:
        rows = db.execute("SELECT scope,version FROM pipeline_identity").fetchall()
    if len(rows) != 1 or rows[0][1] != 1:
        raise ValueError("clone authority scope conflict")
    values = _decode(rows[0][0].encode())
    if type(values) is not list or len(values) != 4:
        raise ValueError("clone authority scope conflict")
    scope = PipelineScope(*values)
    if (canonical(scope.key) != rows[0][0] or scope.partition_id != match[2]
            or scope.user_id != "amc-" + match[1] or scope.agent_id != "amc-agent"
            or scope.connector_id != "amc-web"
            or scope.conversation_id != stable_id("amc-agent", scope.user_id, "amc-web", "memory")):
        raise ValueError("clone authority binding conflict")
    authority = PipelineDatabase(path.parents[1], scope, "vector_index.db", _INDEX_DDL, 0x414C5649)
    authority.validate_existing()
    return authority, scope


def _queue_ids(vector: Path) -> tuple[str, ...]:
    with closing(sqlite3.connect(vector.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=1)) as db:
        rows = db.execute("SELECT DISTINCT id FROM embeddings_queue ORDER BY id LIMIT 10001").fetchall()
    if len(rows) > 10000:
        raise ValueError("clone vector queue bound exceeded")
    return tuple(row[0] for row in rows)


def _validate_vector(root: Path, relative: str, scope: PipelineScope, *, authority: bool) -> None:
    from system_memory_god.storage.daily_vector_index import (
        DailyVectorIndex, _check_tree, _validate_chroma_readonly,
    )
    from system_memory_god.storage.scoped_stores import ScopedStores

    path = root / relative
    ids = _queue_ids(path.parent / "vectors/chroma.sqlite3")
    if authority:
        DailyVectorIndex.validate_existing(ScopedStores(path.parents[1]), scope, ids)
    else:
        _check_tree(path.parent / "vectors")
        _validate_chroma_readonly(path.parent / "vectors", scope, ids)


def _mkdir(path: Path, root: Path) -> None:
    relative = path.relative_to(root)
    current = root
    for name in relative.parts:
        current /= name
        ensure_private_directory(current)


def _write_json(path: Path, value: dict) -> None:
    descriptor = open_private_file(path, exclusive=True)
    with os.fdopen(descriptor, "wb") as output:
        output.write((canonical(value) + "\n").encode())
        output.flush()
        os.fsync(output.fileno())


class _Receipt:
    def __init__(self, path: Path):
        self.stream = os.fdopen(open_private_file(path, exclusive=True), "wb")

    def append(self, state: str, **fields):
        self.stream.write((canonical({"state": state, **fields}) + "\n").encode())
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self):
        self.stream.close()


def prepare_clone(original: Path, backup: Path, clone: Path, control: Path, *,
                  _policy: _Policy | None = None, _validator: Callable | None = None) -> dict:
    """CLI uses the fixed R1b policy; explicit injections are for synthetic tests."""
    policy = _policy or _Policy()
    validate = _validator or _validate_vector
    if (os.path.normcase(str(original.resolve())) != os.path.normcase(str(policy.original.resolve()))
            or os.path.normcase(str(backup.resolve())) != os.path.normcase(str(policy.backup.resolve()))):
        raise ValueError("fixed original and backup paths required")
    if clone != policy.clone or control != policy.control or original == backup:
        raise ValueError("fixed fresh clone boundary required")
    for first in (original, backup, clone, control, policy.old_clone, policy.old_control):
        if not first.is_absolute() or first.resolve() != first:
            raise ValueError("ordinary clone boundary required")
        for other in (original, backup, clone, control, policy.old_clone, policy.old_control):
            if first != other and (first.is_relative_to(other) or other.is_relative_to(first)):
                raise ValueError("disjoint clone boundaries required")
    if clone.exists() or control.exists():
        raise ValueError("fresh clone and control paths required")
    with ExitStack() as stack, _Inventory(original) as source, _Inventory(backup) as saved:
        lineage, _ = _lineage(stack, policy)
        if (source.rows != saved.rows or len(source.rows) != policy.files
                or sum(row["bytes"] for row in source.rows) != policy.size):
            raise ValueError("frozen clone inventory conflict")
        ensure_private_directory(control)
        receipt = _Receipt(control / "migration-receipt.jsonl")
        stack.callback(receipt.close)
        receipt.append("authorized", initial_inventory=source.rows, lineage=lineage)
        try:
            ensure_private_directory(clone)
            for row in source.rows:
                target = clone / row["path"]
                _mkdir(target.parent, clone)
                stream = source.handles[row["path"]]
                stream.seek(0)
                descriptor = open_private_file(target, exclusive=True)
                with os.fdopen(descriptor, "wb") as output:
                    while body := stream.read(64 * 1024):
                        output.write(body)
                    output.flush()
                    os.fsync(output.fileno())
            with _Inventory(clone) as copied:
                if copied.rows != source.rows:
                    raise ValueError("clone copy differs")
                paths = [row["path"] for row in source.rows if row["path"].endswith("/vector_index.db")]
                if not 1 <= len(paths) <= 8:
                    raise ValueError("clone authority count conflict")
                plan = []
                for relative in paths:
                    authority, scope = _authority(original, relative)
                    for root in (backup, clone):
                        other, other_scope = _authority(root, relative)
                        if other_scope != scope:
                            raise ValueError("clone authority scope differs")
                    old = _identity((original / relative).parent / "vectors/chroma.sqlite3")
                    new = _identity((clone / relative).parent / "vectors/chroma.sqlite3")
                    for root in (original, backup, clone):
                        item, _ = _authority(root, relative)
                        item.validate_existing(lambda db: _expect_identity(db, old))
                        validate(root, relative, scope, authority=root == original)
                    plan.append({"path": relative, "scope": list(scope.key), "original_identity": old,
                                 "clone_identity": new, "original_sha256": next(
                                     row["sha256"] for row in source.rows if row["path"] == relative)})
            from Tools.serve_memory_challenge import _DataRootLease
            lease = _DataRootLease.acquire(clone)
            stack.callback(lease.close)
            with _Inventory(clone, writable=frozenset(paths), lease=lease) as current:
                if current.rows != source.rows:
                    raise ValueError("clone changed before migration")
                for entry in plan:
                    relative = entry["path"]
                    authority, scope = _authority(clone, relative)
                    receipt.append("prepared", authority=entry)
                    before = _identity(authority.path)
                    with open_bound_read_file(clone, authority.path) as stream:
                        if _hash(stream)[1] != entry["original_sha256"]:
                            raise ValueError("clone authority changed before CAS")
                    with closing(sqlite3.connect(authority.path.as_uri() + "?mode=rw", uri=True,
                                                  isolation_level=None, timeout=1)) as db:
                        try:
                            db.execute("PRAGMA synchronous=FULL")
                            db.execute("BEGIN IMMEDIATE")
                            authority._recognize(db, allow_initialize=False)
                            _expect_identity(db, entry["original_identity"])
                            if (before != _identity(authority.path)
                                    or _identity(authority.path.parent / "vectors/chroma.sqlite3") != entry["clone_identity"]):
                                raise ValueError("clone migration identity changed")
                            changed = db.execute("UPDATE vector_identity SET device=?,inode=? WHERE device=? AND inode=?",
                                                 (*entry["clone_identity"], *entry["original_identity"])).rowcount
                            if changed != 1:
                                raise ValueError("clone migration CAS conflict")
                            db.commit()
                        finally:
                            if db.in_transaction:
                                db.rollback()
                    receipt.append("committed", path=relative)
                    validate(clone, relative, scope, authority=True)
            with _Inventory(clone, lease=lease) as migrated:
                final = migrated.rows
                if ([row["path"] for row in final] != [row["path"] for row in source.rows]
                        or any(old != new for old, new in zip(source.rows, final) if old["path"] not in paths)):
                    raise ValueError("clone migration changed other files")
                for entry in plan:
                    validate(clone, entry["path"], PipelineScope(*entry["scope"]), authority=True)
                    entry["clone_sha256"] = next(row["sha256"] for row in final if row["path"] == entry["path"])
            source.recheck()
            saved.recheck()
            receipt.append("validated", final_inventory=final, vector_authorities=plan)
            receipt.close()
            _, migration = _read_pinned(stack, control / "migration-receipt.jsonl", maximum=256 * 1024)
            proof = {"schema": PROOF_SCHEMA, "clone_root": clone.as_posix(), "original_root": original.as_posix(),
                     "backup_root": backup.as_posix(), "initial_inventory": source.rows, "final_inventory": final,
                     "vector_authorities": plan, "migration_receipt": migration, "lineage": lineage}
            descriptor = open_private_file(clone / ".amc-recovery-clone", exclusive=True)
            with os.fdopen(descriptor, "wb") as marker:
                marker.write(b"AMC-RECOVERY-CLONE-1\n")
                marker.flush()
                os.fsync(marker.fileno())
            _write_json(clone / PROOF_NAME, proof)
            with open_bound_read_file(clone, clone / PROOF_NAME) as stream:
                _, digest = _hash(stream, 256 * 1024)
            _write_json(control / "clone-proof-pin.json", {"schema": "amc-clone-proof-pin-r1b",
                "clone_root": clone.as_posix(), "proof_path": (clone / PROOF_NAME).as_posix(), "proof_sha256": digest})
            return {"state": "clone_prepared", "files": policy.files, "bytes": policy.size, "authorities": len(plan)}
        except BaseException:
            if not receipt.stream.closed:
                try:
                    receipt.append("unknown")
                except Exception:
                    pass
            raise


def _expect_identity(db, expected: list[str]) -> None:
    if db.execute("SELECT device,inode FROM vector_identity").fetchall() != [tuple(expected)]:
        raise ValueError("clone vector authority identity conflict")


def validate_clone_proof(root: Path, *, expected_sha256: str, manifest: dict | None = None,
                         runtime_lease=None, _policy: _Policy | None = None,
                         _validator: Callable | None = None) -> dict:
    policy = _policy or _Policy()
    validate = _validator or _validate_vector
    if root != policy.clone or not re.fullmatch("[0-9a-f]{64}", expected_sha256):
        raise ValueError("fixed clone proof required")
    with ExitStack() as stack:
        body, pinned = _read_pinned(stack, root / PROOF_NAME)
        if pinned["sha256"] != expected_sha256:
            raise ValueError("clone proof digest conflict")
        proof = _decode(body)
        if (type(proof) is not dict or set(proof) != {"schema", "clone_root", "original_root", "backup_root",
                "initial_inventory", "final_inventory", "vector_authorities", "migration_receipt", "lineage"}
                or proof["schema"] != PROOF_SCHEMA or proof["clone_root"] != root.as_posix()
                or os.path.normcase(str(Path(proof["original_root"]).resolve())) != os.path.normcase(str(policy.original.resolve()))
                or os.path.normcase(str(Path(proof["backup_root"]).resolve())) != os.path.normcase(str(policy.backup.resolve()))):
            raise ValueError("clone proof shape conflict")
        lineage, old_manifest = _lineage(stack, policy)
        if lineage != proof["lineage"]:
            raise ValueError("clone lineage changed")
        if manifest is not None and any(manifest.get(key) != old_manifest.get(key)
                                        for key in ("model", "groups", "incomplete_requests")):
            raise ValueError("continuation source selection differs")
        migration_body, migration = _read_pinned(stack, policy.control / "migration-receipt.jsonl")
        if migration != proof["migration_receipt"]:
            raise ValueError("clone migration receipt changed")
        migration_events = [_decode(line) for line in migration_body.splitlines()]
        planned = proof["vector_authorities"]
        planned_paths = [entry["path"] for entry in planned]
        if (not 1 <= len(planned_paths) <= 8 or len(set(planned_paths)) != len(planned_paths)
                or len(migration_events) != 2 * len(planned_paths) + 2
                or migration_events[0] != {"state": "authorized", "initial_inventory": proof["initial_inventory"],
                                           "lineage": proof["lineage"]}
                or migration_events[-1] != {"state": "validated", "final_inventory": proof["final_inventory"],
                                            "vector_authorities": planned}
                or [event.get("authority", {}).get("path") for event in migration_events[1:-1:2]] != planned_paths
                or any(event.get("state") != "prepared" for event in migration_events[1:-1:2])
                or migration_events[2:-1:2] != [{"state": "committed", "path": path} for path in planned_paths]):
            raise ValueError("clone migration receipt incomplete")
        with _Inventory(Path(proof["original_root"])) as source, _Inventory(Path(proof["backup_root"])) as backup:
            if (source.rows != backup.rows or source.rows != proof["initial_inventory"]
                    or len(source.rows) != policy.files or sum(row["bytes"] for row in source.rows) != policy.size):
                raise ValueError("clone source inventory changed")
            with _Inventory(root, extras=frozenset(_EXTRAS), lease=runtime_lease) as current:
                if current.rows != proof["final_inventory"]:
                    raise ValueError("prepared clone changed")
                actual = {row["path"] for row in source.rows if row["path"].endswith("/vector_index.db")}
                authorities = proof["vector_authorities"]
                if (type(authorities) is not list or not 1 <= len(authorities) <= 8
                        or len(authorities) != len(actual) or {entry["path"] for entry in authorities} != actual):
                    raise ValueError("clone authority inventory conflict")
                if ([row["path"] for row in current.rows] != [row["path"] for row in source.rows]
                        or any(old != new for old, new in zip(source.rows, current.rows) if old["path"] not in actual)):
                    raise ValueError("clone proof changed non-authority bytes")
                for entry in authorities:
                    authority, scope = _authority(root, entry["path"])
                    if (entry["scope"] != list(scope.key)
                            or entry["original_sha256"] != next(row["sha256"] for row in source.rows if row["path"] == entry["path"])
                            or entry["clone_sha256"] != next(row["sha256"] for row in current.rows if row["path"] == entry["path"])
                            or entry["original_identity"] != _identity((Path(proof["original_root"]) / entry["path"]).parent / "vectors/chroma.sqlite3")
                            or entry["clone_identity"] != _identity(authority.path.parent / "vectors/chroma.sqlite3")):
                        raise ValueError("clone native binding changed")
                    authority.validate_existing(lambda db: _expect_identity(db, entry["clone_identity"]))
                    validate(root, entry["path"], scope, authority=True)
        return {"files": policy.files, "bytes": policy.size, "authorities": len(authorities), "prior_calls": 0}
