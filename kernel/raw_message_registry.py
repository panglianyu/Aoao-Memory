"""Private identity registry; display labels never become identity keys."""

from __future__ import annotations

import sqlite3
from kernel.raw_message_models import ConflictError, RawMessageError, Scope, ScopeError
from kernel.raw_message_validation import _id, _limits, _now, _predicate, _scope

_CONVERSATION_KEYS = (
    "agent_id",
    "user_id",
    "connector_id",
    "provider_conversation_id",
    "conversation_type",
)


def _connector_scope(db: sqlite3.Connection, scope: Scope, connector_id: str) -> None:
    if not _limits(scope, connector_id):
        raise ScopeError("connector unavailable in scope")
    if (
        scope.conversation_id is not None
        and not db.execute(
            """SELECT 1 FROM conversations WHERE conversation_id=?
        AND agent_id=? AND user_id=? AND connector_id=?""",
            (scope.conversation_id, scope.agent_id, scope.user_id, connector_id),
        ).fetchone()
    ):
        raise ScopeError("connector unavailable in conversation scope")


def _identity(row: sqlite3.Row, keys: tuple[str, ...], values: tuple) -> None:
    if tuple(row[key] for key in keys) != values:
        raise ConflictError("registry identity conflict")


def register_connector(
    db: sqlite3.Connection,
    scope: Scope,
    connector_id: str,
    kind: str,
    account_ref: str,
    display_name: str,
) -> None:
    _scope(scope)
    for value in (connector_id, kind, account_ref, display_name):
        _id(value)
    _connector_scope(db, scope, connector_id)
    existing = db.execute(
        "SELECT * FROM connectors WHERE connector_id=?", (connector_id,)
    ).fetchone()
    identity = (scope.agent_id, scope.user_id, kind, account_ref)
    if existing:
        _identity(existing, ("agent_id", "user_id", "kind", "account_ref"), identity)
        db.execute(
            "UPDATE connectors SET display_name=? WHERE connector_id=?",
            (display_name, connector_id),
        )
    else:
        if db.execute(
            "SELECT 1 FROM connectors WHERE agent_id=? AND user_id=? AND kind=? AND account_ref=?",
            identity,
        ).fetchone():
            raise ConflictError("connector identity conflict")
        db.execute(
            "INSERT INTO connectors VALUES(?,?,?,?,?,?)",
            (connector_id, *identity, display_name),
        )


def register_conversation(
    db: sqlite3.Connection,
    scope: Scope,
    conversation_id: str,
    connector_id: str,
    provider_conversation_id: str,
    conversation_type: str,
    display_name: str,
) -> None:
    _scope(scope)
    for value in (
        conversation_id,
        connector_id,
        provider_conversation_id,
        display_name,
    ):
        _id(value)
    if conversation_type not in {"group", "dm", "web", "system"}:
        raise RawMessageError("invalid conversation type")
    if not _limits(scope, connector_id, conversation_id):
        raise ScopeError("conversation unavailable in scope")
    if not db.execute(
        "SELECT 1 FROM connectors WHERE connector_id=? AND agent_id=? AND user_id=?",
        (connector_id, scope.agent_id, scope.user_id),
    ).fetchone():
        raise ScopeError("connector unavailable in scope")
    existing = db.execute(
        "SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,)
    ).fetchone()
    identity = (
        scope.agent_id,
        scope.user_id,
        connector_id,
        provider_conversation_id,
        conversation_type,
    )
    if existing:
        _identity(existing, _CONVERSATION_KEYS, identity)
        db.execute(
            "UPDATE conversations SET display_name=? WHERE conversation_id=?",
            (display_name, conversation_id),
        )
    else:
        _new_conversation(db, conversation_id, identity, display_name)


def _new_conversation(
    db: sqlite3.Connection, conversation_id: str, identity: tuple, display_name: str
) -> None:
    if db.execute(
        "SELECT 1 FROM conversations WHERE connector_id=? AND provider_conversation_id=?",
        identity[2:4],
    ).fetchone():
        raise ConflictError("conversation identity conflict")
    db.execute(
        "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?)",
        (conversation_id, *identity, display_name, _now()),
    )


def list_connectors(db: sqlite3.Connection, scope: Scope) -> list[dict]:
    _scope(scope)
    query = "SELECT * FROM connectors WHERE agent_id=? AND user_id=?"
    params = [scope.agent_id, scope.user_id]
    if scope.connector_id is not None:
        query += " AND connector_id=?"
        params.append(scope.connector_id)
    if scope.conversation_id is not None:
        query += " AND connector_id IN (SELECT connector_id FROM conversations WHERE conversation_id=?)"
        params.append(scope.conversation_id)
    return [dict(row) for row in db.execute(query + " ORDER BY connector_id", params)]


def list_conversations(
    db: sqlite3.Connection, scope: Scope, *, connector_id: str | None = None
) -> list[dict]:
    _scope(scope)
    predicate, params = _predicate(scope)
    query = "SELECT * FROM conversations WHERE " + predicate
    if connector_id is not None:
        _id(connector_id)
        query += " AND connector_id=?"
        params.append(connector_id)
    return [
        dict(row)
        for row in db.execute(query + " ORDER BY created_at,conversation_id", params)
    ]
