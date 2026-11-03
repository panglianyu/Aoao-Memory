"""Host 的显式隔离 BLOB 库；不导出路径、不挂载静态路由或自动执行文件。"""

import hashlib
import os
import re
import secrets
import sqlite3
import stat
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from kernel.wechat_artifact_content import (
    MAX_IMAGE_PIXELS, MAX_IMAGE_SIDE, ArtifactImportError, normalize_content, prepare_artifact,
)
from protocols.wechat_artifacts import MAX_ARTIFACT_BYTES, ArtifactDescriptor


MAX_DATABASE_BYTES = 32 * 1024 * 1024
_SCHEMA = {
    "metadata": "CREATE TABLE metadata (version INTEGER, max_assets INTEGER, max_bytes INTEGER)",
    "assets": "CREATE TABLE assets (asset_id TEXT PRIMARY KEY, content_type TEXT, "
    "size_bytes INTEGER, sha256 TEXT, width INTEGER, height INTEGER, data BLOB)",
}


def _quota(max_assets: int, max_bytes: int) -> None:
    if (
        type(max_assets) is not int or not 1 <= max_assets <= 16
        or type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024
    ):
        raise ArtifactImportError("隔离资产配额非法")


def _usage(connection: sqlite3.Connection) -> tuple[int, int, int, int]:
    rows = connection.execute(
        "SELECT * FROM metadata WHERE typeof(version)='integer' "
        "AND typeof(max_assets)='integer' AND typeof(max_bytes)='integer' LIMIT 2"
    ).fetchall()
    if (
        len(rows) != 1 or rows[0][0] != 1
        or connection.execute("SELECT count(*) FROM metadata").fetchone() != (1,)
    ):
        raise ValueError
    _, max_assets, max_bytes = rows[0]
    _quota(max_assets, max_bytes)
    count, total, biggest, invalid = connection.execute(
        "SELECT count(*), coalesce(sum(length(data)),0), coalesce(max(length(data)),0), "
        "coalesce(sum(CASE WHEN typeof(data)!='blob' OR typeof(size_bytes)!='integer' "
        "OR size_bytes!=length(data) OR size_bytes<=0 OR typeof(asset_id)!='text' "
        "OR length(asset_id)!=40 OR typeof(sha256)!='text' OR length(sha256)!=64 "
        "OR typeof(content_type)!='text' OR content_type NOT IN ('image/png','application/octet-stream') "
        "OR (width IS NOT NULL AND typeof(width)!='integer') "
        "OR (height IS NOT NULL AND typeof(height)!='integer') "
        "THEN 1 ELSE 0 END),0) FROM assets"
    ).fetchone()
    if count > max_assets or total > max_bytes or biggest > MAX_ARTIFACT_BYTES or invalid:
        raise ValueError
    return count, total, max_assets, max_bytes


@contextmanager
def _fixed_errors() -> Iterator[None]:
    try:
        yield
    except (OSError, sqlite3.Error, ValueError, TypeError, OverflowError):
        raise ArtifactImportError("隔离资产操作未确认；勿自动重试") from None


def _path(value: Path, *, existing: bool) -> Path:
    path = Path(value)
    if (
        not path.is_absolute() or str(path).startswith("\\\\") or ".." in path.parts
        or any(":" in part for part in path.parts[1:])
        or path.resolve(strict=existing) != path
    ):
        raise ValueError
    for candidate in (path, *path.parents):
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            if candidate == path and not existing:
                continue
            raise
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError
    if any(os.path.lexists(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError
    if existing:
        info = path.stat()
        if not path.is_file() or info.st_nlink != 1 or not 0 < info.st_size <= MAX_DATABASE_BYTES:
            raise ValueError
        with path.open("rb") as source:
            header = source.read(100)
        if (
            header[:16] != b"SQLite format 3\x00" or header[16:18] != b"\x10\x00"
            or header[18:20] != b"\x01\x01"
        ):
            raise ValueError
    return path


def _asset_id(value: str) -> None:
    if type(value) is not str or not re.fullmatch(r"wxlocal_[0-9a-f]{32}", value):
        raise ArtifactImportError("本机资产标识非法")


@dataclass(frozen=True, slots=True, repr=False)
class ImportedArtifact:
    asset_id: str
    content_type: str
    size_bytes: int
    sha256: str
    width: int | None
    height: int | None

    def __post_init__(self) -> None:
        _asset_id(self.asset_id)
        if (
            type(self.size_bytes) is not int or not 1 <= self.size_bytes <= MAX_ARTIFACT_BYTES
            or type(self.sha256) is not str or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
            or type(self.content_type) is not str
        ):
            raise ArtifactImportError("本机资产元数据非法")
        if self.content_type == "application/octet-stream":
            if self.width is not None or self.height is not None:
                raise ArtifactImportError("不透明资产不能携带图片尺寸")
        elif self.content_type == "image/png":
            if (
                type(self.width) is not int or type(self.height) is not int
                or not 0 < self.width <= MAX_IMAGE_SIDE or not 0 < self.height <= MAX_IMAGE_SIDE
                or self.width * self.height > MAX_IMAGE_PIXELS
            ):
                raise ArtifactImportError("本机图片尺寸非法")
        else:
            raise ArtifactImportError("本机资产类型非法")


@dataclass(frozen=True, slots=True)
class HostArtifactQuarantine:
    """同步磁盘接口，应在工作线程调用；私有可信父目录由本机装配者提供。"""

    _file: Path = field(repr=False)

    @classmethod
    def create(
        cls, path: Path, *, max_assets: int = 16, max_total_bytes: int = 16 * 1024 * 1024,
    ) -> "HostArtifactQuarantine":
        with _fixed_errors():
            _quota(max_assets, max_total_bytes)
            target = _path(path, existing=False)
            with target.open("xb"):
                pass
            with closing(sqlite3.connect(target)) as connection, connection:
                connection.execute("PRAGMA page_size=4096")
                for statement in _SCHEMA.values():
                    connection.execute(statement)
                connection.execute("INSERT INTO metadata VALUES (1,?,?)", (max_assets, max_total_bytes))
            return cls.open(target)

    @classmethod
    def open(cls, path: Path) -> "HostArtifactQuarantine":
        with _fixed_errors():
            store = cls(_path(path, existing=True))
            with store._transaction():
                pass
            return store

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with _fixed_errors():
            target = _path(self._file, existing=True)
            with closing(sqlite3.connect(
                target.as_uri() + "?mode=rw", uri=True, timeout=1,
            )) as connection, connection:
                connection.execute("PRAGMA trusted_schema=OFF")
                if (
                    connection.execute("PRAGMA journal_mode").fetchone() != ("delete",)
                    or connection.execute("PRAGMA page_size").fetchone() != (4096,)
                    or connection.execute("PRAGMA page_count").fetchone()[0] > 8192
                    or connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]
                ):
                    raise ValueError
                connection.execute("PRAGMA max_page_count=8192")
                connection.execute("BEGIN IMMEDIATE")
                schema = connection.execute(
                    "SELECT name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY name"
                ).fetchall()
                if schema != sorted(_SCHEMA.items()):
                    raise ValueError
                _usage(connection)
                yield connection

    def import_bytes(self, descriptor: ArtifactDescriptor, data: bytes) -> ImportedArtifact:
        prepared = prepare_artifact(descriptor, data)
        receipt = ImportedArtifact(
            "wxlocal_" + secrets.token_hex(16), prepared.content_type, len(prepared.data),
            hashlib.sha256(prepared.data).hexdigest(), prepared.width, prepared.height,
        )
        with self._transaction() as connection:
            count, total, max_assets, max_bytes = _usage(connection)
            if count >= max_assets or total + receipt.size_bytes > max_bytes:
                raise ValueError
            connection.execute("INSERT INTO assets VALUES (?, ?, ?, ?, ?, ?, ?)", (
                receipt.asset_id, receipt.content_type, receipt.size_bytes, receipt.sha256,
                receipt.width, receipt.height, prepared.data,
            ))
        return receipt

    def read(self, asset_id: str) -> tuple[ImportedArtifact, bytes]:
        _asset_id(asset_id)
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM assets WHERE asset_id=?", (asset_id,)).fetchone()
            if row is None:
                raise ValueError
            receipt = ImportedArtifact(*row[:-1])
            if type(row[-1]) is not bytes or hashlib.sha256(row[-1]).hexdigest() != receipt.sha256:
                raise ValueError
        prepared = normalize_content(row[-1])
        if (
            prepared.content_type != receipt.content_type or prepared.width != receipt.width
            or prepared.height != receipt.height or prepared.data != row[-1]
        ):
            raise ArtifactImportError("本机资产内容与分类不符")
        return receipt, row[-1]
