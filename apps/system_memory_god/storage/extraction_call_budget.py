"""Private append-only SDK reservations shared by every domain of one source."""

from dataclasses import dataclass
import hashlib
import json
import re

from kernel.challenge_diagnostics import DiagnosticError
from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical
from .pipeline_database import PipelineDatabase

_HASH = re.compile(r"[0-9a-f]{64}")
_REASONS = frozenset({"finish", "content", "json", "group_shape", "domain_payload",
                      "cache_integrity", "source_budget", "prepare", "published", "retirement", "unknown"})
_FINISH = frozenset({"stop", "length", "content_filter", "tool_calls",
                     "insufficient_system_resource", "aborted", "unknown"})
_DDL = """
CREATE TABLE extraction_budget_groups(
 budget_key TEXT PRIMARY KEY,identity_json TEXT NOT NULL,
 origin TEXT NOT NULL CHECK(origin IN ('fresh','legacy_unknown')));
CREATE TABLE extraction_budget_calls(
 budget_key TEXT NOT NULL REFERENCES extraction_budget_groups(budget_key),
 number INTEGER NOT NULL CHECK(number BETWEEN 1 AND 3),
 PRIMARY KEY(budget_key,number));
CREATE TABLE extraction_budget_events(
 budget_key TEXT NOT NULL,number INTEGER NOT NULL,event INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('reserved','completed','failed','unknown')),
 reason TEXT NOT NULL,finish TEXT NOT NULL,
 PRIMARY KEY(budget_key,number,event),
 FOREIGN KEY(budget_key,number) REFERENCES extraction_budget_calls(budget_key,number));
"""


def _identity(scope, source, content_hash):
    if (type(scope) is not PipelineScope or type(source) is not SourceRevision
            or type(content_hash) is not str or not _HASH.fullmatch(content_hash)):
        raise DiagnosticError("source_budget", "invalid grouped budget identity")
    # task_id and target_domain deliberately do not identify a grouped SDK call.
    return canonical([scope.key, source.raw_message_id, source.revision, content_hash,
                      source.processor, source.processor_version, source.model, source.prompt_hash])


def group_budget_key(scope, source, content_hash):
    return hashlib.sha256(_identity(scope, source, content_hash).encode("ascii")).hexdigest()


@dataclass(frozen=True)
class CallReservation:
    key: str
    number: int
    feedback: str | None


def _conflict():
    return DiagnosticError("source_budget", "grouped budget integrity unavailable")


class ExtractionCallBudget:
    def __init__(self, root, scope):
        self._db = PipelineDatabase(root, scope, "extraction_call_budget.db", _DDL, 0x414C4342)

    @staticmethod
    def _history(db, key):
        calls = db.execute("SELECT number FROM extraction_budget_calls WHERE budget_key=? ORDER BY number",
                           (key,)).fetchall()
        if [row[0] for row in calls] != list(range(1, len(calls) + 1)) or len(calls) > 3:
            raise _conflict()
        result = []
        for (number,) in calls:
            events = db.execute("SELECT event,state,reason,finish FROM extraction_budget_events "
                "WHERE budget_key=? AND number=? ORDER BY event", (key, number)).fetchall()
            if (not events or len(events) > 2 or events[0] != (0, "reserved", "unknown", "unknown")
                    or len(events) == 2 and (events[1][0] != 1 or events[1][1] not in {
                        "completed", "failed", "unknown"} or events[1][2] not in _REASONS
                        or events[1][3] not in _FINISH)):
                raise _conflict()
            _, state, reason, finish = events[-1]
            result.append({"number": number, "state": state, "reason": reason, "finish": finish})
        return tuple(result)

    def reserve(self, source, content_hash, *, origin):
        identity = _identity(self._db.scope, source, content_hash)
        key = group_budget_key(self._db.scope, source, content_hash)
        if type(origin) is not str or origin not in {"fresh", "legacy_unknown"}:
            raise _conflict()
        denied = None
        reservation = None
        with self._db.transaction() as db:
            row = db.execute("SELECT identity_json,origin FROM extraction_budget_groups WHERE budget_key=?",
                             (key,)).fetchone()
            if row is None:
                db.execute("INSERT INTO extraction_budget_groups VALUES(?,?,?)", (key, identity, origin))
                row = (identity, origin)
            if row[0] != identity or row[1] not in {"fresh", "legacy_unknown"}:
                raise _conflict()
            if row[1] == "legacy_unknown":
                denied = "grouped extraction legacy budget unknown"
            else:
                history = self._history(db, key)
                # A prior owner died without a terminal fact. Its reservation is
                # consumed, and recovery appends uncertainty rather than refunding.
                for call in history:
                    if call["state"] == "reserved":
                        db.execute("INSERT INTO extraction_budget_events VALUES(?,?,1,'unknown','unknown','unknown')",
                                   (key, call["number"]))
                if len(history) >= 3:
                    denied = "grouped extraction budget exhausted"
                else:
                    number = len(history) + 1
                    db.execute("INSERT INTO extraction_budget_calls VALUES(?,?)", (key, number))
                    db.execute("INSERT INTO extraction_budget_events VALUES(?,?,0,'reserved','unknown','unknown')",
                               (key, number))
                    feedback = next((call["reason"] for call in reversed(history)
                                     if call["reason"] in {"json", "group_shape", "domain_payload"}), None)
                    reservation = CallReservation(key, number, feedback)
        # legacy_unknown must itself commit before refusing the first SDK call.
        if denied is not None:
            raise DiagnosticError("source_budget", denied)
        return reservation

    def finish(self, reservation, *, state, reason="unknown", finish="unknown"):
        if (type(reservation) is not CallReservation or not _HASH.fullmatch(reservation.key)
                or type(reservation.number) is not int or not 1 <= reservation.number <= 3
                or type(state) is not str or state not in {"completed", "failed", "unknown"}
                or type(reason) is not str or reason not in _REASONS
                or type(finish) is not str or finish not in _FINISH):
            raise _conflict()
        with self._db.transaction() as db:
            row = db.execute("SELECT identity_json FROM extraction_budget_groups WHERE budget_key=?",
                             (reservation.key,)).fetchone()
            if row is None:
                raise _conflict()
            try:
                bound = json.loads(row[0])
                if (bound[0] != list(self._db.scope.key)
                        or hashlib.sha256(row[0].encode("ascii")).hexdigest() != reservation.key):
                    raise _conflict()
            except (ValueError, IndexError, TypeError, UnicodeError):
                raise _conflict() from None
            history = self._history(db, reservation.key)
            call = next((item for item in history if item["number"] == reservation.number), None)
            if call is None:
                raise _conflict()
            expected = {"number": reservation.number, "state": state, "reason": reason, "finish": finish}
            if call["state"] != "reserved":
                if call != expected:
                    raise _conflict()
                return
            db.execute("INSERT INTO extraction_budget_events VALUES(?,?,1,?,?,?)",
                       (reservation.key, reservation.number, state, reason, finish))

    def history(self, source, content_hash):
        key = group_budget_key(self._db.scope, source, content_hash)
        self._db._path_check()
        if not self._db.path.exists():
            if any(self._db.path.with_name(self._db.path.name + suffix).exists()
                   for suffix in ("-wal", "-shm", "-journal")):
                raise _conflict()
            return ()
        with self._db.readonly_snapshot() as db:
            return self._history(db, key)
