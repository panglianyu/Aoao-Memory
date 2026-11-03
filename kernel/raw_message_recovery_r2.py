"""One finite in-place continuation of the sealed R1b checkpoint.

Old receipts, attempts, results and markers remain authorities and are never
rewritten. Importing this module performs no IO and creates no model client.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any

from kernel.challenge_diagnostics import DiagnosticError, open_private_file
from kernel.memory_pipeline_models import canonical
from kernel.raw_message_clone import _Inventory, _decode, _read_pinned
from kernel.raw_message_models import ConflictError
from kernel.raw_message_recovery import (
    RecoveryCallBudget, RecoveryReceipt, _ROW_FIELDS, _database, _validate_manifest,
    full_key, manifest_digest, readonly, scope_of, source_of,
)

ROOT = Path("G:/amc26/recovery-1.8-503r1b")
PRIOR_CONTROL = Path("G:/amc26/recovery-r1b-control")
CONTROL = Path("G:/amc26/recovery-r2-control")
SUPERVISOR_SOURCE = Path("G:/amc26/recovery_r1b_supervisor.py")
SCHEMA = "amc-recovery-r2"
_IDENTITY_FIELDS = tuple(field for field in _ROW_FIELDS if field not in {"attempts", "last_error_code"})


@dataclass(frozen=True)
class _Policy:
    root: Path = ROOT
    prior: Path = PRIOR_CONTROL
    control: Path = CONTROL
    supervisor_source: Path = SUPERVISOR_SOURCE


def _boundary(root: Path, prior: Path, policy: _Policy) -> None:
    if root != policy.root or prior != policy.prior or root.resolve() != root or prior.resolve() != prior:
        raise ValueError("fixed R2 checkpoint required")
    if (root / ".amc-recovery-r2-consumed").exists():
        raise ConflictError("R2 continuation already consumed")


def _lineage(stack: ExitStack, root: Path, prior: Path, policy: _Policy) -> tuple[dict, dict]:
    paths = {"manifest": prior / "manifest.json", "receipt": prior / "receipt.jsonl",
             "supervisor": prior / "supervisor.json", "clone_proof": root / ".amc-clone-proof.json",
             "source_pin": prior / "source-pin.json", "supervisor_source": policy.supervisor_source}
    bodies: dict[str, bytes] = {}
    proof: dict[str, Any] = {}
    for name, path in paths.items():
        bodies[name], proof[name] = _read_pinned(stack, path, maximum=512 * 1024)
    old = _decode(bodies["manifest"])
    _validate_manifest(root, old)
    events = [_decode(line) for line in bodies["receipt"].splitlines()]
    if (not 2 <= len(events) <= 64 or any(type(event) is not dict for event in events)
            or sum(event.get("state") == "call-started" for event in events) != 2):
        raise ConflictError("R1b model consumption not proven")
    final = events[-1].get("report")
    supervisor = _decode(bodies["supervisor"])
    source_pin = _decode(bodies["source_pin"])
    if (type(old) is not dict or Path(old.get("clone", "")) != root or len(old.get("groups", ())) != 2
            or sum(len(group["rows"]) for group in old["groups"]) != 18
            or type(final) is not dict or final.get("state") != "unknown" or final.get("closed") is not True
            or any(type(final.get(key)) is not int or final[key] != value for key, value in {
                "selected": 18, "calls": 2, "succeeded": 3, "failed": 10, "additional_attempts": 13,
                "original_incomplete": 2, "active_generation_count": 0, "ipc_inflight": 0,
                "model_owner_count": 0}.items())
            or final.get("embedding_closed") is not True
            or type(final.get("elapsed_seconds")) not in {int, float}
            or not math.isfinite(final["elapsed_seconds"]) or not 0 < final["elapsed_seconds"] < 600
            or type(supervisor) is not dict or supervisor.get("job_active") != 0
            or type(supervisor.get("job_active")) is not int or supervisor.get("success") is not False
            or supervisor.get("forced") is not True or supervisor.get("status") != "helper_unsettled"
            or supervisor.get("helper") != final
            or type(source_pin) is not dict or source_pin.get("clone_proof_sha256") != proof["clone_proof"]["sha256"]):
        raise ConflictError("R1b final checkpoint differs")
    authorized = [event for event in events if event.get("state") == "authorized"]
    databases = {group["database"] for group in old["groups"]}
    if (len(authorized) != 1 or authorized[0].get("manifest") != manifest_digest(old)
            or {event.get("database") for event in events if event.get("state") == "committed"} != databases
            or events[-1].get("state") != "unknown"):
        raise ConflictError("R1b authorization history differs")
    marker = root / ".amc-recovery-consumed"
    marker_body, _ = _read_pinned(stack, marker, maximum=64)
    if marker_body != b"recovery authorization consumed\n":
        raise ConflictError("R1b consumed marker differs")
    proof["prior_calls"] = 2
    return proof, old


def _task(db, expected: dict, *, allow_lease: bool = False) -> tuple[dict, list[dict]]:
    current = db.execute("SELECT d.*,m.agent_id,m.user_id,m.connector_id,m.conversation_id,m.content_hash,"
        "m.status AS raw_status,m.revision AS current_revision FROM derivation_outbox d "
        "JOIN raw_messages m USING(message_id) WHERE d.task_id=?", (expected["task_id"],)).fetchone()
    if (current is None or any(current[field] != expected[field] for field in _IDENTITY_FIELDS)
            or current["raw_status"] != "active" or current["revision"] != current["current_revision"]
            or (not allow_lease or current["status"] == "succeeded")
            and any(current[field] is not None for field in ("lease_token", "lease_owner", "lease_until"))):
        raise ConflictError("R2 current source conflict")
    result = {field: current[field] for field in _ROW_FIELDS}
    result["expected_status"] = current["status"]
    result["expected_available_at"] = current["available_at"]
    if (type(current["available_at"]) not in {int, float} or not math.isfinite(current["available_at"])
            or current["available_at"] < 0 or current["status"] == "pending" and current["available_at"] != 0):
        raise ConflictError("R2 availability conflict")
    anchors = [dict(row) for row in db.execute("SELECT memory_id,store_ref,status FROM derivation_results "
        "WHERE task_id=? ORDER BY memory_id,store_ref", (expected["task_id"],))]
    return result, anchors


def _success_proof(stores, row: dict, anchors: list[dict]) -> str:
    """Use the existing validators, read-only snapshots, and real source anchors."""
    from system_memory_god.storage.derived_evidence import validate_payload
    from system_memory_god.storage.memory_ledger import query_ledger_ids

    scope, source = scope_of(row), source_of(row)
    run = stores.runs(scope).get_many((row["task_id"],)).get(row["task_id"])
    if (run is None or run["source"] != asdict(source) or run["domain"] != row["target_domain"]
            or run["state"] != "ledger_written" or run["fence"] != row["attempts"]):
        raise ConflictError("R2 succeeded run proof conflict")
    outputs = tuple(validate_payload(row["target_domain"], json.loads(value)) for value in run["outputs"])
    if list(outputs) != run["outputs"]:
        raise ConflictError("R2 frozen output conflict")
    cached = stores.extractions(scope).get(source, row["content_hash"])
    if cached is None or tuple(canonical(value) for value in cached[row["target_domain"]]) != outputs:
        raise ConflictError("R2 succeeded extraction proof conflict")
    records = stores.evidence(scope).by_tasks((row["task_id"],)).get(row["task_id"], [])
    expected = sorted((record["memory_id"], record["store_ref"], "active") for record in records)
    if (len(records) != len(outputs) or [record["memory_id"] for record in records] != run["result_ids"]
            or any(record["source"] != asdict(source) or record["domain"] != row["target_domain"]
                   or canonical(record["payload"]) != value for record, value in zip(records, outputs))
            or sorted((item["memory_id"], item["store_ref"], item["status"]) for item in anchors) != expected):
        raise ConflictError("R2 succeeded evidence proof conflict")
    ledger = {}
    if records:
        ids = tuple(record["memory_id"] for record in records)
        with stores.ledger(scope)._db.readonly_snapshot() as db:
            inspected = query_ledger_ids(db, ids)
            if any(not inspected.get(key, {}).get("has_item") or inspected[key]["sources"] != (source,) for key in ids):
                raise ConflictError("R2 succeeded ledger proof conflict")
            ledger = {key: {"has_item": value["has_item"], "sources": [asdict(item) for item in value["sources"]]}
                      for key, value in inspected.items()}
    return hashlib.sha256(canonical({"row": row, "anchors": anchors, "run": run,
                                    "evidence": records, "ledger": ledger}).encode()).hexdigest()


def build_manifest(root: Path, prior_control: Path, *, runtime_lease, _policy: _Policy | None = None) -> dict:
    from system_memory_god.storage.scoped_stores import ScopedStores

    policy = _policy or _Policy()
    _boundary(root, prior_control, policy)
    with ExitStack() as stack:
        lineage, old = _lineage(stack, root, prior_control, policy)
        with _Inventory(root, lease=runtime_lease) as inventory:
            databases = sorted(root.glob("users/*/raw/messages.db"))
            if not 1 <= len(databases) <= 8:
                raise ConflictError("R2 raw database bound conflict")
            old_groups = {group["database"]: group for group in old["groups"]}
            groups: list[dict] = []
            retained: list[dict] = []
            states: Counter[tuple[str, int]] = Counter()
            keys: dict[tuple, Counter[str]] = {}
            for path in databases:
                relative = path.relative_to(root).as_posix()
                with readonly(path, frozen=True) as db:
                    if db.execute("SELECT 1 FROM derivation_events WHERE action LIKE 'operator_recovery:r2:%' LIMIT 1").fetchone():
                        raise ConflictError("R2 event already consumed")
                    if db.execute("SELECT 1 FROM purge_receipts LIMIT 1").fetchone():
                        raise ConflictError("R2 purge conflict")
                    selected = old_groups.get(relative)
                    if selected is None:
                        if db.execute("SELECT 1 FROM derivation_outbox WHERE status IN ('pending','leased') LIMIT 1").fetchone():
                            raise ConflictError("R2 unrelated unfinished task")
                        continue
                    ids = {row["task_id"] for row in selected["rows"]}
                    if any(item[0] not in ids for item in db.execute(
                            "SELECT task_id FROM derivation_outbox WHERE status IN ('pending','leased')")):
                        raise ConflictError("R2 unrelated unfinished task")
                    stores = ScopedStores(path.parents[1] / "derived")
                    remaining = []
                    for expected in selected["rows"]:
                        row, anchors = _task(db, expected)
                        events = [item[0] for item in db.execute("SELECT action FROM derivation_events WHERE task_id=? "
                            "AND action LIKE 'operator_recovery:%'", (row["task_id"],))]
                        if events != ["operator_recovery:once:" + manifest_digest(old)]:
                            raise ConflictError("R2 task authorization history conflict")
                        state = row["expected_status"]
                        states[(state, row["attempts"])] += 1
                        scope, source = scope_of(row), source_of(row)
                        key = full_key(scope, source)
                        keys.setdefault(key, Counter())[state] += 1
                        if state == "succeeded":
                            retained.append({"database": relative, "row": row,
                                             "proof_sha256": _success_proof(stores, row, anchors)})
                        else:
                            if anchors or stores.runs(scope).get_many((row["task_id"],)):
                                raise ConflictError("R2 unfinished derived result conflict")
                            if stores.extractions(scope).get(source, row["content_hash"]) is not None:
                                raise ConflictError("R2 unfinished cache conflict")
                            remaining.append(row)
                    if remaining:
                        groups.append({"database": relative, "user_key": selected["user_key"],
                                       "user_id": selected["user_id"], "rows": remaining})
            if (states != {("succeeded", 4): 3, ("failed", 4): 10, ("pending", 3): 5}
                    or len(keys) != 4 or sorted(sorted(value.items()) for value in keys.values())
                    != sorted([[('succeeded', 3)], [('failed', 5)], [('failed', 5)], [('pending', 5)]])):
                raise ConflictError("R2 exact checkpoint task set differs")
            with readonly(root / "challenge.sqlite3", frozen=True) as db:
                requests = [dict(row) for row in db.execute("SELECT * FROM challenge_requests WHERE complete=0 LIMIT 3")]
            if requests != old["incomplete_requests"] or len(requests) != 2:
                raise ConflictError("R2 original incomplete requests differ")
            # All existing authorities, including users without failed tasks.
            from kernel.raw_message_clone import _authority, _validate_vector
            for item in inventory.rows:
                if item["path"].endswith("/vector_index.db"):
                    _, scope = _authority(root, item["path"])
                    _validate_vector(root, item["path"], scope, authority=True)
            inventory.recheck()
            checkpoint = {"files": inventory.rows, "sha256": hashlib.sha256(canonical(inventory.rows).encode()).hexdigest()}
            return {"schema": SCHEMA, "clone": root.as_posix(), "prior_control": prior_control.as_posix(),
                    "model": old["model"], "groups": groups, "retained": retained, "incomplete_requests": requests,
                    "lineage": lineage, "checkpoint": checkpoint}


def preflight(root: Path, manifest: dict, *, prior_control: Path, checkpoint_sha256: str,
              runtime_lease, _policy: _Policy | None = None) -> dict:
    if (type(manifest) is not dict or manifest.get("schema") != SCHEMA
            or manifest.get("clone") != root.as_posix() or manifest.get("prior_control") != prior_control.as_posix()
            or not re.fullmatch("[0-9a-f]{64}", checkpoint_sha256)
            or manifest.get("checkpoint", {}).get("sha256") != checkpoint_sha256):
        raise ConflictError("R2 checkpoint binding differs")
    if build_manifest(root, prior_control, runtime_lease=runtime_lease, _policy=_policy) != manifest:
        raise ConflictError("R2 checkpoint changed during preflight")
    return {"selected": 18, "remaining": 15, "retained": 3, "prior_calls": 2, "new_call_limit": 3}


class R2Receipt(RecoveryReceipt):
    def __init__(self, path: Path, manifest: dict, *, _policy: _Policy | None = None):
        import threading

        policy = _policy or _Policy()
        if path != policy.control / "receipt.jsonl" or manifest.get("schema") != SCHEMA:
            raise ValueError("fixed R2 receipt required")
        self._lock = threading.Lock()
        descriptor = open_private_file(policy.root / ".amc-recovery-r2-consumed", exclusive=True)
        try:
            os.write(descriptor, b"R2 continuation consumed\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._fd = open_private_file(path, exclusive=True)
        self.digest = manifest_digest(manifest)
        self.broken = False
        self.committed: set[str] = set()
        self.expected = {group["database"] for group in manifest["groups"]}
        try:
            self.append("authorized", manifest=self.digest)
        except BaseException:
            os.close(self._fd)
            raise

    def commit_group(self, root: Path, group: dict) -> None:
        from kernel.raw_message_store import RawMessageStore

        self.append("prepared", database=group["database"])
        with RawMessageStore(_database(root, group["database"]))._db(write=True) as db:
            if db.execute("SELECT 1 FROM derivation_events WHERE action LIKE 'operator_recovery:r2:%' LIMIT 1").fetchone():
                raise ConflictError("R2 database authorization consumed")
            for expected in group["rows"]:
                row, anchors = _task(db, expected)
                if row != expected or anchors:
                    raise ConflictError("R2 compare-and-set preflight conflict")
            for expected in group["rows"]:
                if expected["expected_status"] == "failed":
                    changed = db.execute("UPDATE derivation_outbox SET status='pending',available_at=0 WHERE task_id=? "
                        "AND status='failed' AND attempts=? AND lease_owner IS NULL AND lease_token IS NULL "
                        "AND lease_until IS NULL AND available_at=?",
                        (expected["task_id"], expected["attempts"], expected["expected_available_at"])).rowcount
                    if changed != 1:
                        raise ConflictError("R2 compare-and-set conflict")
                elif expected["expected_status"] != "pending":
                    raise ConflictError("R2 succeeded task must not be claimed")
                db.execute("INSERT INTO derivation_events(task_id,action,occurred_at,error_code) VALUES(?,?,?,?)",
                    (expected["task_id"], "operator_recovery:r2:" + self.digest, time.time(), expected["last_error_code"]))
        self.append("committed", database=group["database"])
        self.committed.add(group["database"])


class R2CallBudget(RecoveryCallBudget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.terminal_event = asyncio.Event()

    def abort(self) -> None:
        self._disabled = True
        self.terminal_event.set()

    async def create(self, **kwargs):
        if self.terminal_event.is_set():
            raise RuntimeError("R2 model budget stopped")
        try:
            response = await super().create(**kwargs)
            try:
                finish = response.choices[0].finish_reason
            except (AttributeError, IndexError, TypeError):
                finish = "unknown"
            if finish != "stop":
                raise DiagnosticError("finish", finish_reason=finish)
            return response
        except BaseException:
            self.abort()
            raise


def terminal(root: Path, manifest: dict) -> dict:
    from system_memory_god.storage.scoped_stores import ScopedStores

    succeeded = failed = additional = 0
    for item in manifest["retained"]:
        with readonly(root / item["database"]) as db:
            row, anchors = _task(db, item["row"])
        stores = ScopedStores((root / item["database"]).parents[1] / "derived")
        if row != item["row"] or _success_proof(stores, row, anchors) != item["proof_sha256"]:
            raise ConflictError("R2 retained success changed")
        succeeded += 1
    for group in manifest["groups"]:
        stores = ScopedStores((root / group["database"]).parents[1] / "derived")
        with readonly(root / group["database"]) as db:
            for expected in group["rows"]:
                row, anchors = _task(db, expected, allow_lease=True)
                if row["attempts"] not in {expected["attempts"], expected["attempts"] + 1}:
                    raise ConflictError("R2 unexpected additional attempt")
                state = row["expected_status"]
                succeeded += state == "succeeded"
                failed += state == "failed"
                additional += row["attempts"] == expected["attempts"] + 1
                if state == "succeeded":
                    _success_proof(stores, row, anchors)
    with readonly(root / "challenge.sqlite3") as db:
        requests = [dict(row) for row in db.execute("SELECT * FROM challenge_requests WHERE complete=0 LIMIT 3")]
    if requests != manifest["incomplete_requests"]:
        raise ConflictError("R2 changed original incomplete requests")
    return {"succeeded": succeeded, "failed": failed, "additional_attempts": additional, "original_incomplete": 2}
