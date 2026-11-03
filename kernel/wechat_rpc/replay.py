"""显式创建的本地 nonce 账本；提交成功后才接受请求，不自动重建丢失数据。"""

import hashlib
import hmac
import math
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from protocols.sandbox_rpc import (
    MAX_REPLAY_ENTRIES,
    SandboxAuthenticationError,
    SandboxReplayError,
)


RETENTION_SECONDS = 600
MAX_DATABASE_BYTES = 2 * 1024 * 1024
_SCHEMA = {
    "metadata": "CREATE TABLE metadata (version INTEGER, key_id TEXT, "
    "capacity INTEGER, high_water REAL)",
    "nonces": "CREATE TABLE nonces (nonce TEXT PRIMARY KEY, expires REAL NOT NULL)",
}


class ReplayStoreError(SandboxAuthenticationError):
    """账本不可用时拒绝请求，不泄露文件路径或底层数据库错误。"""


@contextmanager
def _fixed_errors() -> Iterator[None]:
    try:
        yield
    except SandboxReplayError:
        raise
    except (OSError, sqlite3.Error, ValueError, TypeError, OverflowError):
        raise ReplayStoreError("防重放记录不可用") from None


def _key_id(key: bytes) -> str:
    if not isinstance(key, bytes) or len(key) < 32:
        raise ValueError
    return hmac.new(key, b"aoao.wechat.replay.v1", hashlib.sha256).hexdigest()


def _path(value: Path, *, existing: bool) -> Path:
    path = Path(value)
    if (
        not path.is_absolute()
        or str(path).startswith("\\\\")
        or ".." in path.parts
        or any(":" in part for part in path.parts[1:])
        or path.resolve(strict=existing) != path
    ):
        raise ValueError
    if existing:
        info = path.stat()
        if not path.is_file() or info.st_nlink != 1:
            raise ValueError
        if not 0 < info.st_size <= MAX_DATABASE_BYTES:
            raise ValueError
    return path


def _time(value: float) -> float:
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or not 0 <= value <= 2**52
    ):
        raise ValueError
    return float(value)


def _metadata(connection: sqlite3.Connection, key_id: str) -> tuple[int, float]:
    schema = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY name"
    ).fetchall()
    if schema != sorted(_SCHEMA.items()):
        raise ValueError
    rows = connection.execute(
        "SELECT version, key_id, capacity, high_water FROM metadata LIMIT 2"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError
    version, saved_key, capacity, high_water = rows[0]
    if (
        version != 1
        or saved_key != key_id
        or type(capacity) is not int
        or not 1 <= capacity <= MAX_REPLAY_ENTRIES
    ):
        raise ValueError
    return capacity, _time(high_water)


@dataclass(frozen=True, slots=True)
class SqliteReplayStore:
    _file: Path = field(repr=False)
    _binding: str = field(repr=False)

    @classmethod
    def create(
        cls, path: Path, *, key: bytes, capacity: int = MAX_REPLAY_ENTRIES
    ) -> "SqliteReplayStore":
        """仅初始化新文件；调用方须先保证私有目录和可信时钟。"""
        with _fixed_errors():
            binding = _key_id(key)
            if type(capacity) is not int or not 1 <= capacity <= MAX_REPLAY_ENTRIES:
                raise ValueError
            target = _path(path, existing=False)
            with target.open("xb"):
                pass
            with closing(cls._connect(target)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA.values():
                    connection.execute(statement)
                connection.execute(
                    "INSERT INTO metadata VALUES (1, ?, ?, ?)",
                    (binding, capacity, 0),
                )
                connection.commit()
        return cls.open(target, key=key)

    @classmethod
    def open(cls, path: Path, *, key: bytes) -> "SqliteReplayStore":
        """只打开已有账本；缺失、旧版本、不同密钥或损坏均不清空重建。"""
        with _fixed_errors():
            target, binding = _path(path, existing=True), _key_id(key)
            with closing(cls._connect(target)) as connection:
                _metadata(connection, binding)
                if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise ValueError
            return cls(target, binding)

    @staticmethod
    def _connect(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(
            path.as_uri() + "?mode=rw", uri=True, isolation_level=None, timeout=0.2
        )
        try:
            if connection.execute("PRAGMA journal_mode").fetchone() != ("delete",):
                raise ValueError
            connection.execute("PRAGMA synchronous=FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    def verify_key(self, key: bytes) -> None:
        with _fixed_errors():
            if not hmac.compare_digest(self._binding, _key_id(key)):
                raise ValueError

    def consume(self, nonce: str, *, now: float | None = None) -> None:
        """原子认领已认证 nonce；拒绝重放、时钟倒退和未过期账本满载。"""
        with _fixed_errors():
            if not isinstance(nonce, str) or not re.fullmatch(
                r"nonce_[0-9a-f]{32}", nonce
            ):
                raise ValueError
            target = _path(self._file, existing=True)
            with closing(self._connect(target)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                capacity, high_water = _metadata(connection, self._binding)
                # 默认在获得写锁后取时，避免线程调度次序被误判成时钟倒退。
                checked_at = _time(time.time() if now is None else now)
                if checked_at <= 0 or checked_at < high_water:
                    raise ValueError
                connection.execute(
                    "DELETE FROM nonces WHERE expires < ?", (checked_at,)
                )
                if (
                    connection.execute(
                        "SELECT 1 FROM nonces WHERE nonce = ?", (nonce,)
                    ).fetchone()
                    is not None
                ):
                    raise SandboxReplayError("请求已被消费")
                if (
                    connection.execute("SELECT COUNT(*) FROM nonces").fetchone()[0]
                    >= capacity
                ):
                    raise ValueError
                connection.execute(
                    "INSERT INTO nonces VALUES (?, ?)",
                    (nonce, checked_at + RETENTION_SECONDS),
                )
                connection.execute("UPDATE metadata SET high_water = ?", (checked_at,))
                connection.commit()
