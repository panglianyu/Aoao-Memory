"""Durable extraction snapshots and local commit checkpoints.

This journal is not an Outbox lease authority. A larger fence invalidates old
journal writers; the worker must separately validate Raw revision/lease before
external writes. Publication is intentionally absent until that gate exists.
"""

from dataclasses import asdict
import json
from pathlib import Path
from typing import Callable

from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical, derived_memory_id, identity
from .pipeline_database import PipelineDatabase, paged_rows

_DDL = """
CREATE TABLE derivation_runs (
 task_id TEXT PRIMARY KEY, snapshot TEXT NOT NULL, fence INTEGER NOT NULL,
 token TEXT NOT NULL, state TEXT NOT NULL);
"""
_STAGES = ("prepared", "domain_written", "ledger_written")
_DOMAINS = frozenset({"vector", "entity", "rule", "graph", "journal", "schedule"})


def _lease(fence: int, token: str) -> None:
    if type(fence) is not int or not 1 <= fence <= 2**63 - 1:
        raise ValueError("invalid derivation fence")
    identity(token)


def _snapshot(source: SourceRevision, domain: str, outputs: tuple[str, ...], reason: str) -> str:
    if type(source) is not SourceRevision or domain not in _DOMAINS:
        raise ValueError("invalid derivation source")
    if type(outputs) is not tuple or len(outputs) > 100 or any(type(value) is not str for value in outputs):
        raise ValueError("invalid extraction outputs")
    if (outputs and reason != "") or (not outputs and reason not in {"not_applicable", "policy_blocked"}):
        raise ValueError("explicit extraction outcome required")
    encoded = canonical({"source": asdict(source), "domain": domain,
                         "outputs": outputs, "empty_reason": reason})
    if len(encoded) > 2 * 1024 * 1024 or any(not value.strip() for value in outputs):
        raise ValueError("invalid extraction size")
    return encoded


def _result(row: tuple) -> dict:
    result = json.loads(row[0])
    task_id = result["source"]["task_id"]
    result["result_ids"] = [derived_memory_id(task_id, i) for i in range(len(result["outputs"]))]
    result["state"] = row[3]
    result["fence"] = row[1]
    return result


class DerivationRuns:
    def __init__(self, root: Path, scope: PipelineScope):
        self._db = PipelineDatabase(root, scope, "derivation_runs.db", _DDL, 0x414C4452)

    def validate_existing(self) -> None:
        self._db.validate_existing()

    def purge_inventory(self, raw_message_id: str,
                        validate_sources: Callable[[tuple[tuple[SourceRevision, str], ...]], None]
                        ) -> dict[str, dict]:
        """Validate every frozen run in one read-only partition snapshot."""
        identity(raw_message_id)
        target: dict[str, dict] = {}
        with self._db.readonly_snapshot() as db:
            batch: list[tuple[SourceRevision, str]] = []
            for task_id, encoded, fence, token, state in paged_rows(
                    db, "SELECT task_id,snapshot,fence,token,state "
                    "FROM derivation_runs ORDER BY task_id"):
                snapshot = json.loads(encoded)
                source = SourceRevision(**snapshot["source"])
                domain = snapshot["domain"]
                outputs = tuple(snapshot["outputs"])
                if (source.task_id != task_id or state not in _STAGES
                        or encoded != _snapshot(source, domain, outputs,
                                                snapshot["empty_reason"])):
                    raise ValueError("derivation run inventory conflict")
                _lease(fence, token)
                batch.append((source, domain))
                if len(batch) == 256:
                    validate_sources(tuple(batch))
                    batch.clear()
                if source.raw_message_id == raw_message_id:
                    target[task_id] = _result((encoded, fence, token, state))
            if batch:
                validate_sources(tuple(batch))
        return target

    def prepare(self, source: SourceRevision, domain: str, outputs: tuple[str, ...],
                fence: int, token: str, *, empty_reason: str = "") -> dict:
        _lease(fence, token)
        snapshot = _snapshot(source, domain, outputs, empty_reason)
        with self._db.transaction() as db:
            row = db.execute("SELECT snapshot,fence,token,state FROM derivation_runs WHERE task_id=?",
                             (source.task_id,)).fetchone()
            if row is not None:
                if row[:3] != (snapshot, fence, token):
                    raise ValueError("derivation preparation conflict")
                return _result(row)
            db.execute("INSERT INTO derivation_runs VALUES (?,?,?,?, 'prepared')",
                       (source.task_id, snapshot, fence, token))
        return _result((snapshot, fence, token, "prepared"))

    def get(self, task_id: str) -> dict | None:
        identity(task_id)
        with self._db.transaction() as db:
            row = db.execute("SELECT snapshot,fence,token,state FROM derivation_runs WHERE task_id=?",
                             (task_id,)).fetchone()
        return _result(row) if row else None

    def get_many(self, task_ids: tuple[str, ...]) -> dict[str, dict]:
        if type(task_ids) is not tuple or not 1 <= len(task_ids) <= 256:
            raise ValueError("invalid derivation lookup")
        for task_id in task_ids:
            identity(task_id)
        marks = ",".join("?" for _ in task_ids)
        with self._db.readonly_snapshot() as db:
            rows = db.execute("SELECT task_id,snapshot,fence,token,state FROM derivation_runs "
                              "WHERE task_id IN (" + marks + ")", task_ids).fetchall()
            return {row[0]: _result(row[1:]) for row in rows}

    def purge_task(self, task_id: str, raw_message_id: str) -> None:
        """Remove frozen extraction text only for its exact Raw source."""
        identity(task_id)
        identity(raw_message_id)
        with self._db.transaction() as db:
            row = db.execute("SELECT snapshot FROM derivation_runs WHERE task_id=?",
                             (task_id,)).fetchone()
            if row is None:
                return
            if json.loads(row[0])["source"]["raw_message_id"] != raw_message_id:
                raise ValueError("run purge source conflict")
            db.execute("DELETE FROM derivation_runs WHERE task_id=?", (task_id,))

    def resume(self, task_id: str, fence: int, token: str) -> dict:
        identity(task_id)
        _lease(fence, token)
        with self._db.transaction() as db:
            row = db.execute("SELECT snapshot,fence,token,state FROM derivation_runs WHERE task_id=?",
                             (task_id,)).fetchone()
            if row is None or fence < row[1] or (fence == row[1] and token != row[2]):
                raise ValueError("derivation fence conflict")
            db.execute("UPDATE derivation_runs SET fence=?,token=? WHERE task_id=?", (fence, token, task_id))
        return _result((row[0], fence, token, row[3]))

    def advance(self, task_id: str, fence: int, token: str, stage: str) -> None:
        identity(task_id)
        _lease(fence, token)
        if stage not in _STAGES:
            raise ValueError("invalid commit checkpoint")
        with self._db.transaction() as db:
            row = db.execute("SELECT fence,token,state FROM derivation_runs WHERE task_id=?",
                             (task_id,)).fetchone()
            if row is None or row[:2] != (fence, token):
                raise ValueError("derivation fence conflict")
            step = _STAGES.index(stage) - _STAGES.index(row[2])
            if step not in {0, 1}:
                raise ValueError("invalid commit transition")
            db.execute("UPDATE derivation_runs SET state=? WHERE task_id=?", (stage, task_id))
