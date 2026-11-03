"""不透明 Guest 资产读取合同；元数据不证明文件类型、安全性或微信身份。"""

import base64
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Protocol, runtime_checkable


MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
MAX_CHUNK_BYTES = 64 * 1024


class WeChatArtifactError(ValueError):
    """固定错误，不携带资产内容、路径或原始参数。"""


def _asset_id(value: str) -> None:
    if type(value) is not str or not re.fullmatch(r"wxasset_[0-9a-f]{32}", value):
        raise WeChatArtifactError("资产标识非法")


@dataclass(frozen=True, slots=True, repr=False)
class ArtifactDescriptor:
    asset_id: str
    size_bytes: int
    sha256: str
    content_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        _asset_id(self.asset_id)
        if (
            type(self.size_bytes) is not int
            or not 1 <= self.size_bytes <= MAX_ARTIFACT_BYTES
            or type(self.sha256) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
            or type(self.content_type) is not str
            or self.content_type != "application/octet-stream"
        ):
            raise WeChatArtifactError("资产元数据非法")

    def to_payload(self) -> dict:
        return asdict(self)

    @classmethod
    def from_payload(cls, value: dict) -> "ArtifactDescriptor":
        if type(value) is not dict or set(value) != {
            "asset_id", "size_bytes", "sha256", "content_type",
        }:
            raise WeChatArtifactError("资产元数据非法")
        return cls(**value)


def validate_artifact_request(value: Mapping) -> None:
    if not isinstance(value, Mapping) or set(value) != {"asset_id", "offset", "limit"}:
        raise WeChatArtifactError("资产读取参数非法")
    _asset_id(value["asset_id"])
    if (
        type(value["offset"]) is not int
        or not 0 <= value["offset"] < MAX_ARTIFACT_BYTES
        or type(value["limit"]) is not int
        or not 1 <= value["limit"] <= MAX_CHUNK_BYTES
    ):
        raise WeChatArtifactError("资产读取范围非法")


@dataclass(frozen=True, slots=True, repr=False)
class ArtifactChunk:
    descriptor: ArtifactDescriptor
    offset: int
    data: bytes

    def __post_init__(self) -> None:
        if type(self.descriptor) is not ArtifactDescriptor or type(self.data) is not bytes:
            raise WeChatArtifactError("资产分块非法")
        validate_artifact_request({
            "asset_id": self.descriptor.asset_id, "offset": self.offset,
            "limit": len(self.data),
        })
        if self.offset + len(self.data) > self.descriptor.size_bytes:
            raise WeChatArtifactError("资产分块越界")

    def to_payload(self) -> dict:
        return {
            "schema_version": 1, "artifact": self.descriptor.to_payload(),
            "offset": self.offset, "data_b64": base64.b64encode(self.data).decode("ascii"),
        }

    @classmethod
    def from_payload(cls, value: dict) -> "ArtifactChunk":
        try:
            if (
                type(value) is not dict
                or set(value) != {"schema_version", "artifact", "offset", "data_b64"}
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or type(value["data_b64"]) is not str
                or not 1 <= len(value["data_b64"]) <= 4 * ((MAX_CHUNK_BYTES + 2) // 3)
            ):
                raise ValueError
            data = base64.b64decode(value["data_b64"], validate=True)
            if base64.b64encode(data).decode("ascii") != value["data_b64"]:
                raise ValueError
            return cls(ArtifactDescriptor.from_payload(value["artifact"]), value["offset"], data)
        except (ValueError, TypeError):
            raise WeChatArtifactError("资产分块响应非法") from None


@runtime_checkable
class ArtifactReadPort(Protocol):
    """可信本机端口，只做有界同步内存切片；不做文件/网络/SDK I/O。"""

    def read(self, *, asset_id: str, offset: int, limit: int) -> ArtifactChunk: ...


_OPERATION_ID_RE = re.compile(r"wxsend_[0-9a-f]{32}")
_CONVERSATION_ID_RE = re.compile(r"wxguest_[0-9a-f]{32}")


@dataclass(frozen=True, slots=True, repr=False)
class ArtifactUploadRequest:
    """Host 明确绑定到一次发送和一个会话的附件元数据。"""

    operation_id: str
    conversation_id: str
    artifact: ArtifactDescriptor
    filename: str
    kind: str

    def __post_init__(self) -> None:
        if (
            type(self.operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(self.operation_id) is None
            or type(self.conversation_id) is not str
            or _CONVERSATION_ID_RE.fullmatch(self.conversation_id) is None
            or type(self.artifact) is not ArtifactDescriptor
            or type(self.filename) is not str
            or not 1 <= len(self.filename.encode("utf-8")) <= 128
            or self.filename in {".", ".."}
            or "/" in self.filename
            or "\\" in self.filename
            or any(ord(character) < 32 or ord(character) == 127 for character in self.filename)
            or self.kind not in {"image", "file"}
        ):
            raise WeChatArtifactError("附件上传元数据非法")

    def to_payload(self) -> dict:
        return {
            "schema_version": 2,
            "operation": "begin",
            "operation_id": self.operation_id,
            "conversation_id": self.conversation_id,
            "artifact": self.artifact.to_payload(),
            "filename": self.filename,
            "kind": self.kind,
        }

    @classmethod
    def from_payload(cls, value: Mapping) -> "ArtifactUploadRequest":
        if not isinstance(value, Mapping) or set(value) != {
            "schema_version", "operation", "operation_id", "conversation_id",
            "artifact", "filename", "kind",
        } or value["schema_version"] != 2 or value["operation"] != "begin":
            raise WeChatArtifactError("附件上传元数据非法")
        return cls(
            value["operation_id"], value["conversation_id"],
            ArtifactDescriptor.from_payload(dict(value["artifact"])),
            value["filename"], value["kind"],
        )


@dataclass(frozen=True, slots=True, repr=False)
class ArtifactUploadChunk:
    artifact: ArtifactDescriptor
    offset: int
    data: bytes

    def __post_init__(self) -> None:
        if type(self.artifact) is not ArtifactDescriptor or type(self.data) is not bytes:
            raise WeChatArtifactError("附件上传分块非法")
        validate_artifact_request({
            "asset_id": self.artifact.asset_id,
            "offset": self.offset,
            "limit": len(self.data),
        })
        if self.offset + len(self.data) > self.artifact.size_bytes:
            raise WeChatArtifactError("附件上传分块越界")

    def to_payload(self) -> dict:
        return {
            "schema_version": 2,
            "operation": "chunk",
            "artifact": self.artifact.to_payload(),
            "offset": self.offset,
            "data_b64": base64.b64encode(self.data).decode("ascii"),
        }

    @classmethod
    def from_payload(cls, value: Mapping) -> "ArtifactUploadChunk":
        try:
            if (
                not isinstance(value, Mapping)
                or set(value) != {"schema_version", "operation", "artifact", "offset", "data_b64"}
                or value["schema_version"] != 2
                or value["operation"] != "chunk"
                or type(value["data_b64"]) is not str
                or not 1 <= len(value["data_b64"]) <= 4 * ((MAX_CHUNK_BYTES + 2) // 3)
            ):
                raise ValueError
            data = base64.b64decode(value["data_b64"], validate=True)
            if base64.b64encode(data).decode("ascii") != value["data_b64"]:
                raise ValueError
            return cls(ArtifactDescriptor.from_payload(dict(value["artifact"])), value["offset"], data)
        except (ValueError, TypeError):
            raise WeChatArtifactError("附件上传分块非法") from None


def artifact_upload_control(operation: str, asset_id: str) -> dict:
    if operation not in {"commit", "abort"}:
        raise WeChatArtifactError("附件上传控制非法")
    _asset_id(asset_id)
    return {"schema_version": 2, "operation": operation, "asset_id": asset_id}


def validate_artifact_upload_payload(value: Mapping) -> None:
    if not isinstance(value, Mapping):
        raise WeChatArtifactError("附件上传参数非法")
    payload = dict(value)
    operation = payload.get("operation")
    if operation == "begin":
        ArtifactUploadRequest.from_payload(payload)
    elif operation == "chunk":
        ArtifactUploadChunk.from_payload(payload)
    elif operation in {"commit", "abort"}:
        asset_id = payload.get("asset_id")
        if type(asset_id) is not str or payload != artifact_upload_control(operation, asset_id):
            raise WeChatArtifactError("附件上传控制非法")
    else:
        raise WeChatArtifactError("附件上传参数非法")


@runtime_checkable
class ArtifactUploadPort(Protocol):
    async def begin(self, request: ArtifactUploadRequest) -> None: ...
    async def write(self, chunk: ArtifactUploadChunk) -> None: ...
    async def commit(self, asset_id: str) -> None: ...
    async def abort(self, asset_id: str) -> None: ...
