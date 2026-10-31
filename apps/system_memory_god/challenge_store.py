"""Private, durable identities and request progress for the challenge host."""

from __future__ import annotations

import asyncio
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sqlite3
import stat
import threading

from kernel.raw_message_models import ConflictError


@dataclass(frozen=True)
class UserBinding:
    user_id: str
    key: str


@dataclass(frozen=True)
class RequestProgress:
    received_utc: str
    progress: int
    complete: bool


def ordinary_directory(path: Path) -> None:
    """Only host-selected ordinary directories may hold challenge state."""
    for current in (path, *path.parents):
        if not current.exists() and not current.is_symlink():
            continue
        info = current.lstat()
        if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
                or getattr(info, "st_file_attributes", 0) & 1024):
            raise ValueError("challenge directory must be ordinary")


class ChallengeStore:
    def __init__(self, root: Path, *, model: str):
        self.root = root
        self.model = model
        self.database = root / "challenge.sqlite3"
        self._guard = threading.Lock()
        self._initialized = False

    async def lookup(self, user_id: str) -> UserBinding | None:
        return await asyncio.to_thread(self._run, "lookup", user_id)

    async def begin(self, user_id: str, request_id: str, digest: str,
                    count: int) -> tuple[UserBinding, RequestProgress]:
        return await asyncio.to_thread(self._run, "begin", user_id, request_id, digest, count)

    async def checkpoint(self, binding: UserBinding, request_id: str, progress: int) -> None:
        await asyncio.to_thread(self._run, "checkpoint", binding.key, request_id, progress)

    async def complete(self, binding: UserBinding, request_id: str) -> None:
        await asyncio.to_thread(self._run, "complete", binding.key, request_id)

    def _initialize(self, db: sqlite3.Connection) -> None:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables:
            if "challenge_identity" not in tables:
                raise RuntimeError("unrecognized challenge database")
            identity = dict(db.execute("SELECT name, value FROM challenge_identity"))
            if identity != {"schema": "1", "profile": "memory-full-v1", "model": self.model}:
                raise RuntimeError("challenge configuration differs from durable identity")
        else:
            with db:
                db.execute("CREATE TABLE challenge_identity(name TEXT PRIMARY KEY, value TEXT NOT NULL)")
                db.executemany("INSERT INTO challenge_identity VALUES(?,?)", (
                    ("schema", "1"), ("profile", "memory-full-v1"), ("model", self.model)))
                db.execute("""CREATE TABLE challenge_users(
                    user_key TEXT PRIMARY KEY, user_id TEXT UNIQUE NOT NULL)""")
                db.execute("""CREATE TABLE challenge_requests(
                    user_key TEXT NOT NULL REFERENCES challenge_users(user_key),
                    request_id TEXT NOT NULL, body_sha256 TEXT NOT NULL,
                    received_utc TEXT NOT NULL, message_count INTEGER NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(user_key, request_id),
                    CHECK(message_count>0 AND progress>=0 AND progress<=message_count),
                    CHECK(complete IN (0,1)))""")
        db.execute("PRAGMA journal_mode=WAL")
        self._initialized = True

    def _run(self, operation: str, *values):
        with self._guard:
            ordinary_directory(self.root)
            self.root.mkdir(parents=True, exist_ok=True)
            ordinary_directory(self.root)
            if self.database.exists() or self.database.is_symlink():
                info = self.database.lstat()
                if (not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
                        or info.st_nlink != 1 or getattr(info, "st_file_attributes", 0) & 1024):
                    raise ValueError("challenge database must be an ordinary file")
            with closing(sqlite3.connect(self.database, timeout=5)) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("PRAGMA synchronous=FULL")
                if not self._initialized:
                    self._initialize(db)
                if operation in {"lookup", "begin"}:
                    user_id = values[0]
                    key = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
                    row = db.execute("SELECT user_id FROM challenge_users WHERE user_key=?", (key,)).fetchone()
                    if row is not None and row[0] != user_id:
                        raise RuntimeError("challenge identity collision")
                    binding = UserBinding(user_id, key)
                    if operation == "lookup":
                        return binding if row is not None else None
                    _, request_id, digest, count = values
                    with db:
                        db.execute("INSERT OR IGNORE INTO challenge_users VALUES(?,?)", (key, user_id))
                        db.execute("""INSERT OR IGNORE INTO challenge_requests
                            (user_key,request_id,body_sha256,received_utc,message_count)
                            VALUES(?,?,?,?,?)""", (key, request_id, digest,
                            datetime.now(timezone.utc).isoformat(), count))
                        request = db.execute("""SELECT body_sha256,received_utc,message_count,
                            progress,complete FROM challenge_requests WHERE user_key=? AND request_id=?""",
                            (key, request_id)).fetchone()
                        if request[0] != digest or request[2] != count:
                            raise ConflictError("challenge request content conflict")
                    return binding, RequestProgress(request[1], request[3], bool(request[4]))
                with db:
                    if operation == "checkpoint":
                        key, request_id, progress = values
                        result = db.execute("""UPDATE challenge_requests SET progress=MAX(progress,?)
                            WHERE user_key=? AND request_id=?""", (progress, key, request_id))
                    elif operation == "complete":
                        result = db.execute("""UPDATE challenge_requests SET complete=1
                            WHERE user_key=? AND request_id=? AND progress=message_count""", values)
                    else:
                        raise RuntimeError("unknown challenge journal operation")
                    if result.rowcount != 1:
                        raise RuntimeError("challenge request progress unavailable")
