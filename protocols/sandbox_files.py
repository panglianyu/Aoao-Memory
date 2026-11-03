"""隔离文件工作区的有界二进制分块传输合同。"""
from __future__ import annotations

import base64
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from protocols.sandbox_rpc import SandboxProtocolError, normalize_guest_path


MAX_FILE_TRANSFER_BYTES = 32 * 1024 * 1024
MAX_FILE_CHUNK_BYTES = 64 * 1024
_TRANSFER_ID_RE = re.compile(r"^transfer_[0-9a-f]{32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SandboxFileProtocolError(SandboxProtocolError):
    """文件传输元数据、分块或范围不满足闭集合同。"""


def validate_transfer_id(value: Any) -> str:
    if not isinstance(value, str) or not _TRANSFER_ID_RE.fullmatch(value):
        raise SandboxFileProtocolError("文件传输标识非法")
    return value


@dataclass(frozen=True, slots=True, repr=False)
class SandboxFileDescriptor:
    transfer_id: str
    path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        transfer_id = validate_transfer_id(self.transfer_id)
        if not isinstance(self.path, str):
            raise SandboxFileProtocolError("文件路径必须是文本")
        try:
            path = normalize_guest_path(self.path)
        except ValueError as exc:
            raise SandboxFileProtocolError("隔离文件路径非法") from exc
        if path == ".":
            raise SandboxFileProtocolError("隔离文件路径必须指向文件")
        if (
            type(self.size_bytes) is not int
            or not 0 < self.size_bytes <= MAX_FILE_TRANSFER_BYTES
            or not isinstance(self.sha256, str)
            or not _SHA256_RE.fullmatch(self.sha256)
        ):
            raise SandboxFileProtocolError("文件传输元数据非法")
        object.__setattr__(self, "transfer_id", transfer_id)
        object.__setattr__(self, "path", path)

    def to_payload(self) -> dict[str, Any]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "SandboxFileDescriptor":
        if not isinstance(value, Mapping) or set(value) != {
            "schema_version",
            "transfer_id",
            "path",
            "size_bytes",
            "sha256",
        }:
            raise SandboxFileProtocolError("文件传输元数据字段非法")
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise SandboxFileProtocolError("文件传输协议版本不兼容")
        return cls(
            transfer_id=value["transfer_id"],
            path=value["path"],
            size_bytes=value["size_bytes"],
            sha256=value["sha256"],
        )


@dataclass(frozen=True, slots=True, repr=False)
class SandboxFileChunk:
    transfer_id: str
    offset: int
    data: bytes
    final: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "transfer_id", validate_transfer_id(self.transfer_id))
        if (
            type(self.offset) is not int
            or not 0 <= self.offset < MAX_FILE_TRANSFER_BYTES
            or type(self.data) is not bytes
            or not 1 <= len(self.data) <= MAX_FILE_CHUNK_BYTES
            or type(self.final) is not bool
            or self.offset + len(self.data) > MAX_FILE_TRANSFER_BYTES
        ):
            raise SandboxFileProtocolError("文件传输分块非法")

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "transfer_id": self.transfer_id,
            "offset": self.offset,
            "data_b64": base64.b64encode(self.data).decode("ascii"),
            "final": self.final,
        }

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "SandboxFileChunk":
        try:
            if not isinstance(value, Mapping) or set(value) != {
                "schema_version",
                "transfer_id",
                "offset",
                "data_b64",
                "final",
            }:
                raise ValueError
            encoded = value["data_b64"]
            if (
                type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or not isinstance(encoded, str)
                or not 1 <= len(encoded) <= 4 * ((MAX_FILE_CHUNK_BYTES + 2) // 3)
            ):
                raise ValueError
            data = base64.b64decode(encoded, validate=True)
            if base64.b64encode(data).decode("ascii") != encoded:
                raise ValueError
            return cls(
                transfer_id=value["transfer_id"],
                offset=value["offset"],
                data=data,
                final=value["final"],
            )
        except (TypeError, ValueError):
            raise SandboxFileProtocolError("文件传输分块非法") from None


def validate_chunk_request(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "transfer_id",
        "offset",
        "limit",
    }:
        raise SandboxFileProtocolError("文件分块读取参数非法")
    transfer_id = validate_transfer_id(value["transfer_id"])
    offset = value["offset"]
    limit = value["limit"]
    if (
        type(offset) is not int
        or not 0 <= offset < MAX_FILE_TRANSFER_BYTES
        or type(limit) is not int
        or not 1 <= limit <= MAX_FILE_CHUNK_BYTES
    ):
        raise SandboxFileProtocolError("文件分块读取范围非法")
    return {"transfer_id": transfer_id, "offset": offset, "limit": limit}


def validate_transfer_request(value: Mapping[str, Any]) -> str:
    if not isinstance(value, Mapping) or set(value) != {"transfer_id"}:
        raise SandboxFileProtocolError("文件传输请求字段非法")
    return validate_transfer_id(value["transfer_id"])
