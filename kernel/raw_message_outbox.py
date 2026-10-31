"""Private Outbox steps; callers own the single SQLite transaction."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid

from kernel.raw_message_models import (
    ConflictError,
    DerivationSpec,
    RawMessageError,
    ResultAnchor,
    Scope,
)
from kernel.raw_message_validation import _clock, _hash, _id, _now, _predicate, _scope


def insert_tasks(
    db: sqlite3.Connection,
    message_id: str,
    revision: int,
    specs: tuple[DerivationSpec, ...],
) -> None:
    for spec in specs:
        task_id = "drv_" + _hash(
            json.dumps(
                [
                    message_id,
                    revision,
                    spec.processor,
                    spec.processor_version,
                    spec.target_domain,
                ]
            )
        )
        existing = db.execute(
            "SELECT model,prompt_hash FROM derivation_outbox WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if existing:
            if tuple(existing) != (spec.model, spec.prompt_hash):
                raise ConflictError("processor provenance conflict")
            continue
        now = _now()
        db.execute(
            """INSERT INTO derivation_outbox(task_id,message_id,revision,processor,
            processor_version,target_domain,model,prompt_hash,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                task_id,
                message_id,
                revision,
                spec.processor,
                spec.processor_version,
                spec.target_domain,
                spec.model,
                spec.prompt_hash,
                now,
                now,
            ),
        )
        db.execute(
            "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'created',?)",
            (task_id, time.time()),
        )


def claim(
    db: sqlite3.Connection,
    scope: Scope,
    worker_id: str,
    *,
    now: float | None = None,
    lease_seconds: int = 60,
) -> dict | None:
    _scope(scope)
    _id(worker_id)
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
        raise RawMessageError("invalid lease duration")
    now = _clock(now)
    predicate, scope_params = _predicate(scope, "m")
    row = db.execute(
        """SELECT d.* FROM derivation_outbox d JOIN raw_messages m USING(message_id)
        WHERE """
        + predicate
        + """ AND m.status IN ('active','edited')
        AND d.revision=m.revision AND ((d.status='pending' AND d.available_at<=?)
          OR (d.status='leased' AND d.lease_until<=?))
        ORDER BY d.available_at,d.created_at,d.task_id LIMIT 1""",
        [*scope_params, now, now],
    ).fetchone()
    if not row:
        return None
    db.execute(
        """UPDATE derivation_outbox SET status='leased',attempts=attempts+1,
        lease_owner=?,lease_token=?,lease_until=?,updated_at=? WHERE task_id=?""",
        (
            worker_id,
            uuid.uuid4().hex,
            now + lease_seconds,
            _now(),
            row["task_id"],
        ),
    )
    db.execute(
        "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,?,?)",
        (
            row["task_id"],
            "reclaimed" if row["status"] == "leased" else "claimed",
            now,
        ),
    )
    return dict(
        db.execute(
            "SELECT * FROM derivation_outbox WHERE task_id=?", (row["task_id"],)
        ).fetchone()
    )


def _leased(
    db: sqlite3.Connection, scope: Scope, task_id: str, lease_token: str, now: float
) -> sqlite3.Row:
    predicate, params = _predicate(scope, "m")
    row = db.execute(
        """SELECT d.* FROM derivation_outbox d JOIN raw_messages m USING(message_id)
        WHERE d.task_id=? AND d.lease_token=? AND d.status='leased' AND d.lease_until>?
        AND """
        + predicate
        + """ AND m.status IN ('active','edited')
        AND m.revision=d.revision""",
        [task_id, lease_token, now, *params],
    ).fetchone()
    if not row:
        raise ConflictError("lease unavailable or stale")
    return row


def claim_partition(db: sqlite3.Connection, scope: Scope, worker_id: str, *,
                    now: float | None = None, lease_seconds: int = 60,
                    max_active: int = 2) -> dict | None:
    """Serialize a full partition inside the caller's Raw write transaction."""
    _scope(scope)
    _id(worker_id)
    if not scope.connector_id or not scope.conversation_id:
        raise RawMessageError("complete worker scope required")
    if type(max_active) is not int or not 1 <= max_active <= 16:
        raise RawMessageError("invalid worker concurrency")
    now = _clock(now)
    active = db.execute("""SELECT m.connector_id,m.conversation_id,m.agent_id,m.user_id
        FROM derivation_outbox d JOIN raw_messages m USING(message_id)
        WHERE d.status='leased' AND d.lease_until>?""", (now,)).fetchall()
    if len(active) >= max_active or any(tuple(row) == (
            scope.connector_id, scope.conversation_id, scope.agent_id, scope.user_id) for row in active):
        return None
    return claim(db, scope, worker_id, now=now, lease_seconds=lease_seconds)


def renew(db: sqlite3.Connection, scope: Scope, task_id: str, lease_token: str, *,
          now: float | None = None, lease_seconds: int = 60) -> dict:
    _scope(scope)
    _id(task_id)
    _id(lease_token)
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
        raise RawMessageError("invalid lease duration")
    now = _clock(now)
    row = _leased(db, scope, task_id, lease_token, now)
    until = max(row["lease_until"], now + lease_seconds)
    db.execute("UPDATE derivation_outbox SET lease_until=?,updated_at=? WHERE task_id=?",
               (until, _now(), task_id))
    return {**dict(row), "lease_until": until}


def complete(
    db: sqlite3.Connection,
    scope: Scope,
    task_id: str,
    lease_token: str,
    results: tuple[ResultAnchor, ...],
    *,
    now: float | None = None,
) -> None:
    _scope(scope)
    _id(task_id)
    _id(lease_token)
    if len(results) > 100:
        raise RawMessageError("too many result anchors")
    seen = set()
    for result in results:
        if not isinstance(result, ResultAnchor):
            raise RawMessageError("invalid result anchor")
        _id(result.memory_id)
        _id(result.store_ref)
        if result.memory_id in seen:
            raise RawMessageError("duplicate result anchor")
        seen.add(result.memory_id)
    now_input = now
    now = _clock(now_input)
    _leased(db, scope, task_id, lease_token, now)
    for result in results:
        db.execute(
            "INSERT INTO derivation_results(task_id,memory_id,store_ref) VALUES(?,?,?)",
            (task_id, result.memory_id, result.store_ref),
        )
    # Recheck after inserting anchors, so a slow insert still rolls back on expiry.
    now = _clock(now_input)
    _leased(db, scope, task_id, lease_token, now)
    db.execute(
        """UPDATE derivation_outbox SET status='succeeded',lease_until=NULL,
        lease_owner=NULL,lease_token=NULL,updated_at=? WHERE task_id=?""",
        (_now(), task_id),
    )
    db.execute(
        "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'succeeded',?)",
        (task_id, now),
    )


def fail(
    db: sqlite3.Connection,
    scope: Scope,
    task_id: str,
    lease_token: str,
    error_code: str,
    *,
    retry_after: int | None = None,
    now: float | None = None,
) -> None:
    """No exception text is stored: only a closed set of non-sensitive codes."""
    _scope(scope)
    _id(task_id)
    _id(lease_token)
    if error_code not in {
        "timeout",
        "unavailable",
        "invalid_output",
        "processor_error",
    }:
        raise RawMessageError("invalid failure code")
    if retry_after is not None and (
        type(retry_after) is not int or not 0 <= retry_after <= 86400
    ):
        raise RawMessageError("invalid retry delay")
    now = _clock(now)
    _leased(db, scope, task_id, lease_token, now)
    db.execute(
        """UPDATE derivation_outbox SET status=?,available_at=?,last_error_code=?,
        lease_until=NULL,lease_owner=NULL,lease_token=NULL,updated_at=? WHERE task_id=?""",
        (
            "pending" if retry_after is not None else "failed",
            now + (retry_after or 0),
            error_code,
            _now(),
            task_id,
        ),
    )
    db.execute(
        "INSERT INTO derivation_events(task_id,action,occurred_at,error_code) VALUES(?,?,?,?)",
        (
            task_id,
            "retry" if retry_after is not None else "failed",
            now,
            error_code,
        ),
    )


def specs_for_revision(
    db: sqlite3.Connection, message_id: str, revision: int
) -> tuple[DerivationSpec, ...]:
    return tuple(
        DerivationSpec(
            r["processor"],
            r["processor_version"],
            r["target_domain"],
            r["model"],
            r["prompt_hash"],
        )
        for r in db.execute(
            "SELECT * FROM derivation_outbox WHERE message_id=? AND revision=?",
            (message_id, revision),
        )
    )


def supersede(db: sqlite3.Connection, message_id: str) -> None:
    for task in db.execute(
        "SELECT task_id FROM derivation_outbox WHERE message_id=? AND status<>'superseded'",
        (message_id,),
    ).fetchall():
        db.execute(
            "INSERT INTO derivation_events(task_id,action,occurred_at) VALUES(?,'superseded',?)",
            (task["task_id"], time.time()),
        )
    db.execute(
        """UPDATE derivation_results SET status='superseded' WHERE task_id IN
        (SELECT task_id FROM derivation_outbox WHERE message_id=?)""",
        (message_id,),
    )
    db.execute(
        """UPDATE derivation_outbox SET status='superseded',lease_token=NULL,
        lease_owner=NULL,lease_until=NULL,updated_at=? WHERE message_id=?""",
        (_now(), message_id),
    )


def reconcile(db: sqlite3.Connection, scope: Scope) -> tuple[list, list, list]:
    issues = []
    predicate, params = _predicate(scope, "m")
    tasks = db.execute(
        """SELECT d.*,m.revision current_revision,m.status message_status
        FROM derivation_outbox d JOIN raw_messages m USING(message_id)
        WHERE """
        + predicate,
        params,
    ).fetchall()
    for task in tasks:
        if task["status"] in {"pending", "leased"} and (
            task["revision"] != task["current_revision"]
            or task["message_status"] == "forgotten"
        ):
            issues.append({"message_id": task["message_id"], "code": "stale_task"})
    result_rows = db.execute(
        """SELECT r.*,d.message_id,d.status task_status,d.revision,
        m.revision current_revision,m.status message_status FROM derivation_results r
        JOIN derivation_outbox d USING(task_id) JOIN raw_messages m USING(message_id)
        WHERE """
        + predicate,
        params,
    ).fetchall()
    for result in result_rows:
        if result["status"] == "active" and (
            result["task_status"] != "succeeded"
            or result["revision"] != result["current_revision"]
            or result["message_status"] == "forgotten"
        ):
            issues.append({"message_id": result["message_id"], "code": "stale_result"})
    return tasks, result_rows, issues
