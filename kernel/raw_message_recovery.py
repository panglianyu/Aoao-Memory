"""One-shot failed-task authorization for an explicitly isolated challenge clone.

No model, database or filesystem work occurs at import. Private manifests contain
identifiers; public reports contain only finite states and counts.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import ExitStack, closing, contextmanager
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import threading
import time

from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical
from kernel.raw_message_models import ConflictError

_ROW_FIELDS = ("task_id", "message_id", "revision", "processor", "processor_version",
               "target_domain", "model", "prompt_hash", "attempts", "last_error_code",
               "agent_id", "user_id", "connector_id", "conversation_id", "content_hash")
_CALL_BUDGET_CLAIMED = False


@contextmanager
def readonly(path: Path, *, frozen: bool = False):
    """Existing ordinary files only, with finite SQL work and lock wait."""
    from kernel.challenge_diagnostics import _pinned_directories, check_private_path
    from kernel.project_workspace_io import open_bound_read_file

    if type(frozen) is not bool:
        raise ValueError("invalid frozen read boundary")
    check_private_path(path)
    began = time.monotonic()
    steps = 0

    def limit():
        nonlocal steps
        steps += 1000
        return int(steps > 20_000_000 or time.monotonic() - began > 10)

    with ExitStack() as pins:
        if frozen:
            pins.enter_context(_pinned_directories(path.parent))
            for suffix in ("", "-wal", "-shm"):
                current = path.with_name(path.name + suffix)
                if suffix and not current.exists():
                    continue
                check_private_path(current)
                stream = pins.enter_context(open_bound_read_file(path.parent, current))
                opened, named = os.fstat(stream.fileno()), current.lstat()
                if (opened.st_nlink != 1 or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
                    raise ValueError("frozen raw file changed")
        # Ordinary WAL reading remains necessary: immutable mode would ignore
        # committed WAL frames. Read pins force SQLite's read-only SHM fallback.
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.set_progress_handler(limit, 1000)
            yield db


def scope_of(row: dict) -> PipelineScope:
    return PipelineScope(row["user_id"], row["agent_id"], row["connector_id"], row["conversation_id"])


def source_of(row: dict) -> SourceRevision:
    return SourceRevision(row["message_id"], row["revision"], row["task_id"], row["processor"],
                          row["processor_version"], row["model"], row["prompt_hash"])


def full_key(scope: PipelineScope, source: SourceRevision) -> tuple:
    return (*scope.key, source.raw_message_id, source.revision, source.processor,
            source.processor_version, source.model, source.prompt_hash)


def manifest_digest(manifest: dict) -> str:
    return hashlib.sha256(canonical(manifest).encode()).hexdigest()


def _selected(db, rows: tuple[dict, ...]) -> None:
    """Check every row before the caller mutates any row in this database."""
    if not rows or len(rows) > 18 or len({row["task_id"] for row in rows}) != len(rows):
        raise ConflictError("recovery selection conflict")
    if db.execute("SELECT 1 FROM derivation_events WHERE action LIKE 'operator_recovery:%' LIMIT 1").fetchone():
        raise ConflictError("recovery authorization consumed")
    if db.execute("SELECT 1 FROM derivation_outbox WHERE status IN ('pending','leased') LIMIT 1").fetchone():
        raise ConflictError("recovery clone is not quiescent")
    if db.execute("SELECT 1 FROM purge_receipts LIMIT 1").fetchone():
        raise ConflictError("recovery purge conflict")
    for expected in rows:
        row = db.execute("SELECT d.*,m.agent_id,m.user_id,m.connector_id,m.conversation_id,"
                         "m.content_hash,m.revision AS current_revision,m.status AS raw_status "
                         "FROM derivation_outbox d JOIN raw_messages m USING(message_id) "
                         "WHERE d.task_id=?", (expected["task_id"],)).fetchone()
        if (row is None or row["status"] != "failed" or row["raw_status"] != "active"
                or row["revision"] != row["current_revision"]
                or any(row[field] != expected[field] for field in _ROW_FIELDS)
                or any(row[field] is not None for field in ("lease_owner", "lease_token", "lease_until"))
                or type(row["attempts"]) is not int or row["attempts"] < 1
                or db.execute("SELECT 1 FROM derivation_results WHERE task_id=? LIMIT 1",
                              (expected["task_id"],)).fetchone()):
            raise ConflictError("recovery source conflict")


def recover_failed_rows(db, rows: tuple[dict, ...], digest: str) -> None:
    if not re.fullmatch("[0-9a-f]{64}", digest):
        raise ValueError("invalid recovery authorization")
    _selected(db, rows)
    action = "operator_recovery:once:" + digest
    for row in rows:
        changed = db.execute("UPDATE derivation_outbox SET status='pending',available_at=0 "
                             "WHERE task_id=? AND status='failed' AND attempts=? "
                             "AND lease_token IS NULL AND lease_owner IS NULL AND lease_until IS NULL",
                             (row["task_id"], row["attempts"])).rowcount
        if changed != 1:
            raise ConflictError("recovery compare-and-set conflict")
        db.execute("INSERT INTO derivation_events(task_id,action,occurred_at,error_code) VALUES(?,?,?,?)",
                   (row["task_id"], action, time.time(), row["last_error_code"]))


def _clone(root: Path) -> None:
    from kernel.challenge_diagnostics import check_private_path

    if not root.is_absolute() or root.resolve() != root or ".." in root.parts:
        raise ValueError("explicit recovery clone required")
    check_private_path(root, directory=True)
    marker = root / ".amc-recovery-clone"
    check_private_path(marker)
    if marker.stat().st_size != 21 or marker.read_bytes() != b"AMC-RECOVERY-CLONE-1\n":
        raise ValueError("recovery clone marker required")


def _database(root: Path, relative: str) -> Path:
    if not re.fullmatch(r"users/[0-9a-f]{64}/raw/messages\.db", relative):
        raise ValueError("invalid recovery database path")
    path = root / relative
    if path.resolve() != path or not path.is_relative_to(root):
        raise ValueError("recovery database escaped clone")
    return path


def build_manifest(root: Path) -> dict:
    """Inspect only the frozen clone. Never reads Raw content or model output."""
    _clone(root)
    with readonly(root / "challenge.sqlite3", frozen=True) as db:
        identity = dict(db.execute("SELECT name,value FROM challenge_identity"))
        if (set(identity) != {"schema", "profile", "model"} or identity["schema"] != "1"
                or identity["profile"] != "memory-full-v1"):
            raise ValueError("challenge identity conflict")
        users = [dict(row) for row in db.execute("SELECT user_key,user_id FROM challenge_users LIMIT 9")]
        requests = [dict(row) for row in db.execute("SELECT * FROM challenge_requests WHERE complete=0 LIMIT 3")]
    if len(users) > 8 or len(requests) != 2 or any(row["progress"] != row["message_count"] for row in requests):
        raise ValueError("challenge frozen request set conflict")
    groups = []
    known = {row["user_key"]: row["user_id"] for row in users}
    discovered = sorted(root.glob("users/*/raw/messages.db"))
    if not 1 <= len(discovered) <= 8:
        raise ValueError("recovery database count conflict")
    for path in discovered:
        relative = path.relative_to(root).as_posix()
        _database(root, relative)
        key = path.parents[1].name
        if key not in known or hashlib.sha256(known[key].encode()).hexdigest() != key:
            raise ValueError("recovery binding conflict")
        with readonly(path, frozen=True) as db:
            if db.execute("SELECT 1 FROM derivation_events WHERE action LIKE 'operator_recovery:%' LIMIT 1").fetchone():
                raise ConflictError("recovery authorization consumed")
            if db.execute("SELECT 1 FROM derivation_outbox WHERE status IN ('pending','leased') LIMIT 1").fetchone():
                raise ConflictError("recovery clone is not quiescent")
            rows = [dict(row) for row in db.execute("SELECT d.*,m.agent_id,m.user_id,m.connector_id,"
                "m.conversation_id,m.content_hash FROM derivation_outbox d "
                "JOIN raw_messages m USING(message_id) WHERE d.status='failed' ORDER BY d.task_id LIMIT 19")]
            selected = [{field: row[field] for field in _ROW_FIELDS} for row in rows]
            if selected:
                _selected(db, tuple(selected))
        if selected:
            groups.append({"database": relative, "user_key": key, "user_id": known[key], "rows": selected})
    manifest = {"schema": 1, "clone": str(root), "model": identity["model"],
                "groups": groups, "incomplete_requests": requests}
    _validate_manifest(root, manifest)
    return manifest


def _validate_manifest(root: Path, manifest: dict) -> None:
    from system_memory_god.grouped_extractor import memory_specs
    from kernel.raw_message_service import stable_id

    if (type(manifest) is not dict or set(manifest) != {"schema", "clone", "model", "groups", "incomplete_requests"}
            or manifest["schema"] != 1 or manifest["clone"] != str(root)
            or type(manifest["groups"]) is not list or not 1 <= len(manifest["groups"]) <= 8):
        raise ValueError("invalid recovery manifest")
    specs = {spec.target_domain: spec for spec in memory_specs("memory-full-v1", manifest["model"])}
    rows = []
    databases = set()
    for group in manifest["groups"]:
        if set(group) != {"database", "user_key", "user_id", "rows"}:
            raise ValueError("invalid recovery manifest group")
        _database(root, group["database"])
        if group["database"] in databases:
            raise ValueError("duplicate recovery database")
        databases.add(group["database"])
        if (group["database"] != "users/" + group["user_key"] + "/raw/messages.db"
                or hashlib.sha256(group["user_id"].encode()).hexdigest() != group["user_key"]):
            raise ValueError("invalid recovery binding")
        for row in group["rows"]:
            if set(row) != set(_ROW_FIELDS):
                raise ValueError("invalid recovery row")
            spec = specs.get(row["target_domain"])
            scope_of(row)
            source_of(row)
            if (spec is None or row["target_domain"] == "vector"
                    or (row["processor"], row["processor_version"], row["model"], row["prompt_hash"])
                    != (spec.processor, spec.processor_version, spec.model, spec.prompt_hash)
                    or row["user_id"] != "amc-" + group["user_key"]
                    or row["agent_id"] != "amc-agent" or row["connector_id"] != "amc-web"
                    or row["conversation_id"] != stable_id("amc-agent", row["user_id"], "amc-web", "memory")
                    or row["last_error_code"] != "invalid_output"
                    or type(row["attempts"]) is not int or not 1 <= row["attempts"] < 2**63 - 1
                    or not re.fullmatch("[0-9a-f]{64}", row["content_hash"])):
                raise ValueError("recovery profile conflict")
            rows.append(row)
    if (len(rows) != 18 or len({row["task_id"] for row in rows}) != 18
            or len({full_key(scope_of(row), source_of(row)) for row in rows}) != 4
            or Counter(row["target_domain"] for row in rows)
            != {"entity": 4, "graph": 3, "journal": 4, "rule": 3, "schedule": 4}):
        raise ValueError("recovery frozen task set conflict")


def preflight(root: Path, manifest: dict) -> dict:
    from system_memory_god.storage.scoped_stores import ScopedStores

    _clone(root)
    _validate_manifest(root, manifest)
    # Rebuild catches removed/added failures, altered request identities, and DBs
    # outside the selected groups without ever claiming work.
    if build_manifest(root) != manifest:
        raise ConflictError("recovery frozen manifest differs")
    cached: dict[tuple, bool] = {}
    for group in manifest["groups"]:
        stores = ScopedStores(_database(root, group["database"]).parents[1] / "derived")
        for row in group["rows"]:
            scope, source = scope_of(row), source_of(row)
            runs = stores.runs(scope)
            if runs._db.path.exists():
                if runs.get_many((row["task_id"],)):
                    raise ConflictError("recovery derivation snapshot conflict")
            key = full_key(scope, source)
            if key not in cached:
                cached[key] = stores.extractions(scope).get(source, row["content_hash"]) is not None
    if sum(cached.values()) != 1 or len(cached) != 4:
        raise ConflictError("recovery extraction cache set conflict")
    if sum(cached[full_key(scope_of(row), source_of(row))]
           for group in manifest["groups"] for row in group["rows"]) != 3:
        raise ConflictError("recovery cached task count conflict")
    return {"selected": 18, "databases": len(manifest["groups"]),
            "cached_keys": 1, "uncached_keys": 3}


class RecoveryReceipt:
    """An exclusive, append-only fsynced marker; any existence denies reentry."""

    def __init__(self, path: Path, manifest: dict):
        from kernel.challenge_diagnostics import open_private_file

        self._lock = threading.Lock()
        consumed = open_private_file(Path(manifest["clone"]) / ".amc-recovery-consumed", exclusive=True)
        try:
            os.write(consumed, b"recovery authorization consumed\n")
            os.fsync(consumed)
        finally:
            os.close(consumed)
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

    def append(self, state: str, **fields) -> None:
        with self._lock:
            if self.broken:
                raise RuntimeError("recovery receipt unavailable")
            encoded = (canonical({"state": state, "monotonic": time.monotonic(), **fields}) + "\n").encode()
            try:
                view = memoryview(encoded)
                while view:
                    written = os.write(self._fd, view)
                    if written <= 0:
                        raise OSError("recovery receipt write failed")
                    view = view[written:]
                os.fsync(self._fd)
            except BaseException:
                self.broken = True
                raise

    def commit_group(self, root: Path, group: dict) -> None:
        from kernel.raw_message_store import RawMessageStore

        name = group["database"]
        self.append("prepared", database=name)
        RawMessageStore(_database(root, name)).recover_selected_failed(tuple(group["rows"]), self.digest)
        self.append("committed", database=name)
        self.committed.add(name)

    @property
    def all_committed(self) -> bool:
        return not self.broken and self.expected == self.committed

    def close(self) -> None:
        os.close(self._fd)


class RecoveryCallBudget:
    """Exactly one process-local budget; durable starts are never refundable."""

    def __init__(self, manifest: dict, receipt: RecoveryReceipt, client, deadline: float, *,
                 provider_base_url: str | None = None):
        from system_memory_god.storage.scoped_stores import ScopedStores

        global _CALL_BUDGET_CLAIMED
        if _CALL_BUDGET_CLAIMED:
            raise RuntimeError("recovery process budget already allocated")
        _CALL_BUDGET_CLAIMED = True
        self.receipt, self.client, self.deadline = receipt, client, deadline
        self.provider_base_url = provider_base_url
        self._disabled = False
        self.allowed = {}
        for group in manifest["groups"]:
            stores = ScopedStores((Path(manifest["clone"]) / group["database"]).parents[1] / "derived")
            for row in group["rows"]:
                scope, source = scope_of(row), source_of(row)
                if stores.extractions(scope).get(source, row["content_hash"]) is None:
                    self.allowed[row["task_id"]] = full_key(scope, source)
        if len(set(self.allowed.values())) != 3:
            raise ValueError("recovery call key set conflict")
        self._outcomes: dict[tuple, tuple[bool, object]] = {}
        self._lock = asyncio.Lock()

    @property
    def calls(self) -> int:
        return len(self._outcomes)

    async def create(self, *, scope: PipelineScope, source: SourceRevision, **request):
        from system_memory_god.grouped_extractor import _GROUP_PROMPT, provider_request_options

        key = full_key(scope, source)
        provider_options = provider_request_options(self.provider_base_url)
        if (self._disabled or self.allowed.get(source.task_id) != key or not self.receipt.all_committed
                or set(request) != {"model", "max_tokens", "temperature", "response_format", "messages"} | set(provider_options)
                or any(request.get(name) != value for name, value in provider_options.items())
                or request["model"] != source.model or type(request["max_tokens"]) is not int
                or not 1 <= request["max_tokens"] <= 2048 or request["temperature"] != 0
                or request["response_format"] != {"type": "json_object"}
                or len(request["messages"]) != 2
                or request["messages"][0] != {"role": "system", "content": _GROUP_PROMPT}):
            raise ValueError("recovery model request denied")
        async with self._lock:
            terminal = getattr(self, "terminal_event", None)
            if self._disabled or terminal is not None and terminal.is_set():
                raise ValueError("recovery model budget stopped")
            if time.monotonic() >= self.deadline:
                raise TimeoutError("recovery business deadline exceeded")
            if key not in self._outcomes:
                if len(self._outcomes) >= 3:
                    raise ValueError("recovery call budget exhausted")
                # Persist first. The in-RAM exception sentinel consumes budget
                # even if cancellation arrives before the SDK yields a result.
                try:
                    self.receipt.append("call-started", key=hashlib.sha256(canonical(key).encode()).hexdigest())
                except BaseException:
                    self._disabled = True
                    raise
                self._outcomes[key] = (False, RuntimeError("recovery model outcome unknown"))
                try:
                    if time.monotonic() >= self.deadline:
                        raise TimeoutError("recovery business deadline exceeded")
                    response = await asyncio.wait_for(self.client.chat.completions.create(**request),
                                                     timeout=min(60, self.deadline - time.monotonic()))
                except BaseException as error:
                    self._outcomes[key] = (False, error)
                else:
                    self._outcomes[key] = (True, response)
            success, outcome = self._outcomes[key]
            if not success:
                if not isinstance(outcome, BaseException):
                    raise RuntimeError("recovery model outcome unavailable")
                raise outcome
            return outcome


def recovery_extractor(stores, model: str, budget: RecoveryCallBudget):
    from system_memory_god.grouped_extractor import GroupedDailyExtractor

    class DeniedClient:
        def __getattr__(self, name):
            raise RuntimeError("unguarded recovery model entry denied")

    class RecoveryGroupedExtractor(GroupedDailyExtractor):
        async def _create_response(self, *, scope, source, **request_kwargs):
            return await budget.create(scope=scope, source=source, **request_kwargs)

        async def extract_guarded(self, *args, **kwargs):
            try:
                return await super().extract_guarded(*args, **kwargs)
            except BaseException:
                abort = getattr(budget, "abort", None)
                if abort is not None:
                    abort()
                raise

    return RecoveryGroupedExtractor(stores, DeniedClient(), model, provider_base_url=budget.provider_base_url)
