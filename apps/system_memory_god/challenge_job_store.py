"""Private durable job receipts; never a replacement for Raw or its leases."""

from __future__ import annotations

import asyncio
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import threading
from typing import Any

from kernel.memory_pipeline_models import identity
from kernel.raw_message_models import ConflictError
from .challenge_store import ordinary_directory


MAX_PAYLOAD_BYTES = 2 * 1024**2
MAX_PENDING = 30
MAX_HISTORY = 65536
MAX_PAGES = 65536
PAGE_SIZE = 4096
STATES = frozenset({"QUEUED", "RUNNING", "SETTLING", "SUCCEEDED", "FAILED"})
TERMINAL = frozenset({"SUCCEEDED", "FAILED"})
ERRORS = frozenset({"closed", "expired", "interrupted", "execution_failed", "owner_unsettled"})
_APP_ID = 0x414A4F42


class JobCapacityError(RuntimeError):
    """A verified pre-insert storage limit, with no new durable acceptance."""


@dataclass(frozen=True)
class JobReceipt:
    job_id: str
    state: str
    received_utc: str
    expires_utc: str


@dataclass(frozen=True)
class JobStatus:
    job_id: str
    state: str
    received_utc: str
    expires_utc: str
    error_code: str | None = None
    result: dict[str, Any] | None = None


@dataclass(frozen=True)
class _Record:
    job_id: str
    user_id: str
    session_id: str
    request_id: str
    digest: str
    message_count: int
    state: str
    received_utc: str
    expires_utc: str

    def receipt(self) -> JobReceipt:
        return JobReceipt(self.job_id, self.state, self.received_utc, self.expires_utc)


def canonical_utc(value: str) -> datetime:
    if type(value) is not str:
        raise ValueError("canonical UTC required")
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError("canonical UTC required") from None
    if (parsed.tzinfo != timezone.utc or parsed.isoformat() != value
            or not value.endswith("+00:00")):
        raise ValueError("canonical UTC required")
    return parsed


def _ordinary_file(path: Path) -> os.stat_result:
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or getattr(info, "st_file_attributes", 0) & 1024):
        raise ValueError("ordinary private job file required")
    return info


def validate_runtime_lease(lease, root: Path) -> None:
    if (not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts
            or root.resolve() != root or getattr(lease, "root", None) != root
            or type(getattr(lease, "descriptor", None)) is not int):
        raise ValueError("matching open runtime lease required")
    ordinary_directory(root)
    try:
        opened = os.fstat(lease.descriptor)
        named = _ordinary_file(root / ".amc-entry.lock")
    except OSError:
        raise ValueError("matching open runtime lease required") from None
    if ((opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_nlink != 1):
        raise ValueError("runtime lease identity changed")


class ChallengeJobStore:
    """One OS owner, fixed schema, bounded pending payload and finite history."""

    def __init__(self, root: Path, *, model: str, runtime_lease):
        if (not isinstance(root, Path) or root.name != "jobs" or not root.is_absolute()
                or ".." in root.parts or root.resolve() != root):
            raise ValueError("fixed private jobs directory required")
        validate_runtime_lease(runtime_lease, root.parent)
        identity(model)
        ordinary_directory(root)
        root.mkdir(exist_ok=True)
        ordinary_directory(root)
        self.root, self.model, self.runtime_lease = root, model, runtime_lease
        self.database = root / "jobs.sqlite3"
        self._epoch = secrets.token_hex(16)
        self._guard = threading.Lock()
        self._pending: set[asyncio.Task] = set()
        self._closed = False
        self._closing = False
        self._descriptor: int | None = None
        self._db_identity: tuple[int, int] | None = None
        self._directory_identity = (root.stat().st_dev, root.stat().st_ino)
        self._lock_path = root / ".job-owner.lock"
        try:
            self._check_paths()
            if os.name != "nt":
                raise OSError("Windows job owner lease required")
            import msvcrt

            self._descriptor = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            os.set_inheritable(self._descriptor, False)
            opened, named = os.fstat(self._descriptor), _ordinary_file(self._lock_path)
            if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
                raise ValueError("job owner lease identity changed")
            os.lseek(self._descriptor, 0, os.SEEK_SET)
            msvcrt.locking(self._descriptor, msvcrt.LK_NBLCK, 1)
            self._initialize()
        except BaseException:
            if self._descriptor is not None:
                os.close(self._descriptor)
                self._descriptor = None
            raise

    def _check_paths(self) -> None:
        validate_runtime_lease(self.runtime_lease, self.root.parent)
        ordinary_directory(self.root)
        info = self.root.stat()
        if (info.st_dev, info.st_ino) != self._directory_identity:
            raise ValueError("job directory identity changed")
        allowed = {".job-owner.lock", "jobs.sqlite3", "jobs.sqlite3-journal"}
        for path in self.root.iterdir():
            if path.name not in allowed:
                raise ValueError("foreign private job file")
            named = _ordinary_file(path)
            if named.st_size > PAGE_SIZE * MAX_PAGES + 1024**2:
                raise ValueError("private job file exceeds storage limit")
            if path == self.database and self._db_identity is not None:
                if (named.st_dev, named.st_ino) != self._db_identity:
                    raise ValueError("job database identity changed")

    def _connect(self):
        self._check_paths()
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA page_size=" + str(PAGE_SIZE))
            if db.execute("PRAGMA max_page_count=" + str(MAX_PAGES)).fetchone()[0] != MAX_PAGES:
                raise RuntimeError("private job database exceeds page limit")
            db.execute("PRAGMA secure_delete=ON")
            if db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                raise RuntimeError("private job journal unavailable")
            db.execute("PRAGMA synchronous=FULL")
            return db
        except BaseException:
            db.close()
            raise

    def _initialize(self):
        with closing(self._connect()) as db:
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if not tables:
                with db:
                    db.execute("PRAGMA application_id=" + str(_APP_ID))
                    db.execute("CREATE TABLE job_identity(name TEXT PRIMARY KEY,value TEXT NOT NULL)")
                    db.executemany("INSERT INTO job_identity VALUES(?,?)", (
                        ("schema", "amc-private-jobs-v1"), ("model", self.model)))
                    db.execute("""CREATE TABLE jobs(
                        job_id TEXT PRIMARY KEY,user_key TEXT NOT NULL,user_id TEXT NOT NULL,
                        session_id TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,
                        message_count INTEGER NOT NULL,payload TEXT,received_utc TEXT NOT NULL,
                        expires_utc TEXT NOT NULL,state TEXT NOT NULL,error_code TEXT,result TEXT,
                        owner_epoch TEXT NOT NULL,UNIQUE(user_key,request_id),
                        CHECK(state IN ('QUEUED','RUNNING','SETTLING','SUCCEEDED','FAILED')),
                        CHECK(message_count>0 AND message_count<=256),
                        CHECK(state NOT IN ('SUCCEEDED','FAILED') OR payload IS NULL))""")
            elif (tables != {"jobs", "job_identity"}
                    or dict(db.execute("SELECT name,value FROM job_identity")) != {
                        "schema": "amc-private-jobs-v1", "model": self.model}
                    or db.execute("PRAGMA application_id").fetchone()[0] != _APP_ID):
                raise ValueError("unrecognized private job database")
            if db.execute("PRAGMA page_size").fetchone()[0] != PAGE_SIZE:
                raise ValueError("invalid private job page size")
            with db:
                db.execute("""UPDATE jobs SET state='FAILED',error_code='interrupted',
                    payload=NULL,result=NULL WHERE state NOT IN ('SUCCEEDED','FAILED')""")
            info = _ordinary_file(self.database)
            self._db_identity = info.st_dev, info.st_ino
        self._check_paths()

    @staticmethod
    def _record(row) -> _Record:
        return _Record(**{key: row[key] for key in _Record.__dataclass_fields__})

    @staticmethod
    def _status(row) -> JobStatus:
        result = json.loads(row["result"]) if row["result"] is not None else None
        return JobStatus(row["job_id"], row["state"], row["received_utc"],
                         row["expires_utc"], row["error_code"], result)

    def _run(self, operation, *args):
        with self._guard:
            if self._closed:
                raise RuntimeError("job store closed")
            with closing(self._connect()) as db:
                if operation == "find":
                    user, request = args
                    row = db.execute("SELECT * FROM jobs WHERE user_key=? AND request_id=?",
                        (hashlib.sha256(user.encode()).hexdigest(), request)).fetchone()
                    return self._record(row) if row is not None else None
                if operation == "accept":
                    values, payload, digest, received, expires = args
                    key = hashlib.sha256(values["user_id"].encode()).hexdigest()
                    with db:
                        row = db.execute("SELECT * FROM jobs WHERE user_key=? AND request_id=?",
                                         (key, values["request_id"])).fetchone()
                        if row is not None:
                            if row["user_id"] != values["user_id"] or row["digest"] != digest:
                                raise ConflictError("job request content conflict")
                            return self._record(row), False
                        count = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
                        pending, size = db.execute("SELECT COUNT(*),COALESCE(SUM(length(CAST(payload AS BLOB))),0) "
                            "FROM jobs WHERE state NOT IN ('SUCCEEDED','FAILED')").fetchone()
                        if count >= MAX_HISTORY or pending >= MAX_PENDING or size + len(payload.encode()) > MAX_PENDING * MAX_PAYLOAD_BYTES:
                            raise JobCapacityError("private job storage capacity reached")
                        job_id = secrets.token_urlsafe(32)
                        db.execute("""INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,'QUEUED',NULL,NULL,?)""",
                            (job_id, key, values["user_id"], values["session_id"], values["request_id"],
                             digest, len(values["messages"]), payload, received, expires, self._epoch))
                        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
                    self._check_paths()
                    return self._record(row), True
                job_id = args[0]
                row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
                if row is None:
                    raise KeyError("job unavailable")
                if operation == "status":
                    return self._status(row)
                if row["owner_epoch"] != self._epoch:
                    raise RuntimeError("job owner changed")
                if operation == "running":
                    if row["state"] != "QUEUED":
                        raise RuntimeError("job cannot start")
                    with db:
                        db.execute("UPDATE jobs SET state='RUNNING' WHERE job_id=?", (job_id,))
                elif operation == "settling":
                    if row["state"] not in {"RUNNING", "SETTLING"}:
                        raise RuntimeError("job cannot settle")
                    with db:
                        db.execute("UPDATE jobs SET state='SETTLING',error_code='owner_unsettled' WHERE job_id=?", (job_id,))
                elif operation == "finish":
                    _, state, code, result = args
                    if state == "SUCCEEDED" and (
                            row["state"] == "QUEUED" or result != {"success": True,
                            "user_id": row["user_id"], "session_id": row["session_id"],
                            "request_id": row["request_id"]}):
                        raise ValueError("job success identity conflict")
                    if row["state"] in TERMINAL:
                        if (row["state"] != state or row["error_code"] != code
                                or (json.loads(row["result"]) if row["result"] else None) != result):
                            raise RuntimeError("job terminal conflict")
                    else:
                        with db:
                            db.execute("UPDATE jobs SET state=?,error_code=?,result=?,payload=NULL WHERE job_id=?",
                                (state, code, json.dumps(result, separators=(",", ":")) if result else None, job_id))
                else:
                    raise ValueError("invalid private job operation")
                self._check_paths()
                return self._status(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())

    async def _call(self, operation, *args):
        if self._closed or self._closing:
            raise RuntimeError("job store closed")
        pending = asyncio.create_task(asyncio.to_thread(self._run, operation, *args))
        self._pending.add(pending)

        def done(task):
            self._pending.discard(task)
            if not task.cancelled():
                task.exception()

        pending.add_done_callback(done)
        cancelled = False
        while not pending.done():
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        value = pending.result()
        if cancelled:
            raise asyncio.CancelledError
        return value

    async def find(self, user_id: str, request_id: str) -> _Record | None:
        return await self._call("find", user_id, request_id)

    async def accept(self, values: dict, payload: str, digest: str, *,
                     received_utc: str, expires_utc: str) -> tuple[_Record, bool]:
        from .challenge_runtime import _external_id, _messages

        if type(values) is not dict or set(values) != {"user_id", "session_id", "request_id", "messages"}:
            raise ValueError("invalid durable job input")
        normalized: dict[str, Any] = {key: _external_id(values[key]) for key in ("user_id", "session_id", "request_id")}
        normalized["messages"] = _messages(values["messages"])
        expected = json.dumps(normalized, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"), allow_nan=False)
        received, expires = canonical_utc(received_utc), canonical_utc(expires_utc)
        if (not 0 < (expires - received).total_seconds() <= 1700
                or len(payload.encode()) > MAX_PAYLOAD_BYTES
                or hashlib.sha256(payload.encode()).hexdigest() != digest
                or expected != payload):
            raise ValueError("invalid durable job input")
        return await self._call("accept", values, payload, digest, received_utc, expires_utc)

    async def status(self, job_id: str) -> JobStatus:
        if type(job_id) is not str or not 1 <= len(job_id) <= 128:
            raise ValueError("invalid opaque job id")
        return await self._call("status", job_id)

    async def running(self, job_id: str) -> JobStatus:
        return await self._call("running", job_id)

    async def settling(self, job_id: str) -> JobStatus:
        return await self._call("settling", job_id)

    async def finish(self, job_id: str, *, state: str, error_code=None, result=None) -> JobStatus:
        if (state not in TERMINAL or error_code is not None and error_code not in ERRORS
                or state == "SUCCEEDED" and (error_code is not None or type(result) is not dict
                    or set(result) != {"success", "user_id", "session_id", "request_id"}
                    or result.get("success") is not True)
                or state == "FAILED" and (error_code is None or result is not None)):
            raise ValueError("invalid durable job terminal")
        return await self._call("finish", job_id, state, error_code, result)

    async def aclose(self) -> None:
        self._closing = True
        if self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        if any(not task.done() for task in self._pending):
            raise RuntimeError("job store IO remains owned")
        import msvcrt

        self._check_paths()
        if self._descriptor is not None:
            os.lseek(self._descriptor, 0, os.SEEK_SET)
            msvcrt.locking(self._descriptor, msvcrt.LK_UNLCK, 1)
            os.close(self._descriptor)
            self._descriptor = None
        self._closed = True
