"""容器与 Windows VM 共用的受限 Sandbox RPC 协议。"""
from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import math
import re
import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Callable
from urllib.parse import urlsplit


SANDBOX_RPC_VERSION = 1
MAX_RPC_PAYLOAD_BYTES = 256 * 1024
MAX_REPLAY_ENTRIES = 4096
_REQUEST_ID_RE = re.compile(r"^rpc_[0-9a-f]{32}$")
_NONCE_RE = re.compile(r"^nonce_[0-9a-f]{32}$")
_SIGNATURE_RE = re.compile(r"^[0-9a-f]{64}$")
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_RESPONSE_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class SandboxProtocolError(ValueError):
    """RPC 结构、路径或参数不符合闭集协议。"""


class SandboxAuthenticationError(SandboxProtocolError):
    """请求签名、时钟或响应绑定校验失败。"""


class SandboxReplayError(SandboxAuthenticationError):
    """已经消费过相同 nonce。"""


class SandboxCapabilityError(SandboxAuthenticationError):
    """已认证主体请求了未授权能力。"""


class SandboxCapability(str, Enum):
    """guest agent 可暴露的最小能力闭集。"""

    HEALTH = "health"
    EXEC = "exec"
    FILE_LIST = "file_list"
    FILE_READ = "file_read"
    FILE_WRITE = "file_write"
    FILE_MOVE = "file_move"
    FILE_PUT_BEGIN = "file_put_begin"
    FILE_PUT_CHUNK = "file_put_chunk"
    FILE_PUT_ABORT = "file_put_abort"
    FILE_GET_OPEN = "file_get_open"
    FILE_GET_CHUNK = "file_get_chunk"
    FILE_GET_CLOSE = "file_get_close"
    ARTIFACT_PULL = "artifact_pull"
    ARTIFACT_PUSH = "artifact_push"
    WECHAT_STATUS = "wechat_status"
    WECHAT_COMMAND = "wechat_command"
    WECHAT_INPUT = "wechat_input"


def _json_normalize(value: Any) -> Any:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        normalized = json.loads(encoded.decode("utf-8"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SandboxProtocolError("payload 必须是有限、可序列化的 JSON") from exc
    if len(encoded) > MAX_RPC_PAYLOAD_BYTES:
        raise SandboxProtocolError("payload 过大")
    return normalized


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _validated_payload(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SandboxProtocolError("payload 必须是 JSON 对象")
    normalized = _json_normalize(_thaw_json(value))
    if not isinstance(normalized, dict):
        raise SandboxProtocolError("payload 必须是 JSON 对象")
    return _freeze_json(normalized)


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _strict_keys(value: Mapping[str, Any], expected: set[str]) -> None:
    if set(value) != expected:
        raise SandboxProtocolError("RPC envelope 字段不完整或包含未知字段")


def _finite_timestamp(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SandboxProtocolError("timestamp 非法")
    try:
        timestamp = float(value)
    except OverflowError:
        raise SandboxProtocolError("timestamp 非法") from None
    if not math.isfinite(timestamp):
        raise SandboxProtocolError("timestamp 非法")
    return timestamp


@dataclass(frozen=True, slots=True)
class SandboxRpcRequest:
    request_id: str
    nonce: str
    timestamp: float
    deadline_seconds: int
    capability: SandboxCapability
    payload: Mapping[str, Any] = field(default_factory=dict)
    signature: str = ""
    protocol_version: int = SANDBOX_RPC_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.protocol_version) is not int
            or self.protocol_version != SANDBOX_RPC_VERSION
        ):
            raise SandboxProtocolError("Sandbox RPC 版本不兼容")
        if not isinstance(self.request_id, str) or not _REQUEST_ID_RE.fullmatch(
            self.request_id
        ):
            raise SandboxProtocolError("request_id 格式非法")
        if not isinstance(self.nonce, str) or not _NONCE_RE.fullmatch(self.nonce):
            raise SandboxProtocolError("nonce 格式非法")
        if not isinstance(self.deadline_seconds, int) or isinstance(
            self.deadline_seconds, bool
        ):
            raise SandboxProtocolError("deadline_seconds 必须是整数")
        if not 1 <= self.deadline_seconds <= 300:
            raise SandboxProtocolError("deadline_seconds 必须在 1 到 300 之间")
        timestamp = _finite_timestamp(self.timestamp)
        try:
            capability = SandboxCapability(self.capability)
        except (TypeError, ValueError) as exc:
            raise SandboxProtocolError("未知 Sandbox capability") from exc
        object.__setattr__(self, "capability", capability)
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "payload", _validated_payload(self.payload))
        if not isinstance(self.signature, str) or (
            self.signature and not _SIGNATURE_RE.fullmatch(self.signature)
        ):
            raise SandboxProtocolError("signature 格式非法")

    @classmethod
    def create(
        cls,
        capability: SandboxCapability | str,
        payload: Mapping[str, Any] | None = None,
        *,
        timestamp: float | None = None,
        deadline_seconds: int = 30,
    ) -> "SandboxRpcRequest":
        try:
            normalized_capability = SandboxCapability(capability)
        except (TypeError, ValueError) as exc:
            raise SandboxProtocolError("未知 Sandbox capability") from exc
        return cls(
            request_id=f"rpc_{secrets.token_hex(16)}",
            nonce=f"nonce_{secrets.token_hex(16)}",
            timestamp=time.time() if timestamp is None else timestamp,
            deadline_seconds=deadline_seconds,
            capability=normalized_capability,
            payload=payload or {},
        )

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "nonce": self.nonce,
            "timestamp": self.timestamp,
            "deadline_seconds": self.deadline_seconds,
            "capability": self.capability.value,
            "payload": _thaw_json(self.payload),
        }

    def to_payload(self) -> dict[str, Any]:
        return {**self.unsigned_payload(), "signature": self.signature}

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "SandboxRpcRequest":
        if not isinstance(value, Mapping):
            raise SandboxProtocolError("RPC request 必须是对象")
        _strict_keys(
            value,
            {
                "protocol_version", "request_id", "nonce", "timestamp",
                "deadline_seconds", "capability", "payload", "signature",
            },
        )
        return cls(
            protocol_version=value["protocol_version"],
            request_id=value["request_id"],
            nonce=value["nonce"],
            timestamp=value["timestamp"],
            deadline_seconds=value["deadline_seconds"],
            capability=value["capability"],
            payload=value["payload"],
            signature=value["signature"],
        )


@dataclass(frozen=True, slots=True)
class SandboxRpcResponse:
    request_id: str
    ok: bool
    code: str
    timestamp: float
    payload: Mapping[str, Any] = field(default_factory=dict)
    signature: str = ""
    protocol_version: int = SANDBOX_RPC_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.protocol_version) is not int
            or self.protocol_version != SANDBOX_RPC_VERSION
        ):
            raise SandboxProtocolError("Sandbox RPC 版本不兼容")
        if not isinstance(self.request_id, str) or not _REQUEST_ID_RE.fullmatch(
            self.request_id
        ):
            raise SandboxProtocolError("response request_id 格式非法")
        if not isinstance(self.ok, bool):
            raise SandboxProtocolError("response ok 必须是布尔值")
        if not isinstance(self.code, str) or not _RESPONSE_CODE_RE.fullmatch(
            self.code
        ):
            raise SandboxProtocolError("response code 格式非法")
        object.__setattr__(self, "timestamp", _finite_timestamp(self.timestamp))
        object.__setattr__(self, "payload", _validated_payload(self.payload))
        if not isinstance(self.signature, str) or (
            self.signature and not _SIGNATURE_RE.fullmatch(self.signature)
        ):
            raise SandboxProtocolError("response signature 格式非法")

    @classmethod
    def create(
        cls,
        request_id: str,
        *,
        ok: bool,
        code: str,
        payload: Mapping[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> "SandboxRpcResponse":
        return cls(
            request_id=request_id,
            ok=ok,
            code=code,
            timestamp=time.time() if timestamp is None else timestamp,
            payload=payload or {},
        )

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "ok": self.ok,
            "code": self.code,
            "timestamp": self.timestamp,
            "payload": _thaw_json(self.payload),
        }

    def to_payload(self) -> dict[str, Any]:
        return {**self.unsigned_payload(), "signature": self.signature}

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "SandboxRpcResponse":
        if not isinstance(value, Mapping):
            raise SandboxProtocolError("RPC response 必须是对象")
        _strict_keys(
            value,
            {
                "protocol_version", "request_id", "ok", "code", "timestamp",
                "payload", "signature",
            },
        )
        return cls(
            protocol_version=value["protocol_version"],
            request_id=value["request_id"],
            ok=value["ok"],
            code=value["code"],
            timestamp=value["timestamp"],
            payload=value["payload"],
            signature=value["signature"],
        )


class SandboxSigner:
    """HMAC 作为 mTLS 之外的请求绑定与防重放第二层。"""

    def __init__(self, key: bytes, *, clock: Callable[[], float] = time.time) -> None:
        if not isinstance(key, bytes) or len(key) < 32:
            raise SandboxProtocolError("Sandbox RPC HMAC key 至少需要 32 字节")
        self._key = key
        self._clock = clock

    def _signature(self, value: Mapping[str, Any]) -> str:
        return hmac.new(self._key, _canonical_bytes(value), hashlib.sha256).hexdigest()

    def sign_request(self, request: SandboxRpcRequest) -> SandboxRpcRequest:
        return replace(request, signature=self._signature(request.unsigned_payload()))

    def verify_request_signature(self, request: SandboxRpcRequest) -> None:
        expected = self._signature(request.unsigned_payload())
        if not request.signature or not hmac.compare_digest(expected, request.signature):
            raise SandboxAuthenticationError("Sandbox RPC 请求签名无效")

    def sign_response(self, response: SandboxRpcResponse) -> SandboxRpcResponse:
        return replace(response, signature=self._signature(response.unsigned_payload()))

    def verify_response(
        self,
        response: SandboxRpcResponse,
        *,
        expected_request_id: str,
        max_clock_skew_seconds: float = 30,
    ) -> None:
        if response.request_id != expected_request_id:
            raise SandboxAuthenticationError("Sandbox RPC response request_id 不匹配")
        expected = self._signature(response.unsigned_payload())
        if not response.signature or not hmac.compare_digest(expected, response.signature):
            raise SandboxAuthenticationError("Sandbox RPC 响应签名无效")
        if abs(self._clock() - response.timestamp) > max_clock_skew_seconds:
            raise SandboxAuthenticationError("Sandbox RPC 响应时间无效")


class SandboxRequestVerifier:
    """服务端签名、时钟、能力与 nonce 防重放闸门。"""

    def __init__(
        self,
        key: bytes,
        *,
        allowed_capabilities: set[SandboxCapability | str],
        max_clock_skew_seconds: float = 30,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not 1 <= max_clock_skew_seconds <= 300:
            raise SandboxProtocolError("max_clock_skew_seconds 非法")
        self._clock = clock
        self._max_clock_skew = float(max_clock_skew_seconds)
        self._signer = SandboxSigner(key, clock=clock)
        self._allowed = frozenset(
            SandboxCapability(item) for item in allowed_capabilities
        )
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()

    def verify(self, request: SandboxRpcRequest) -> SandboxRpcRequest:
        self._signer.verify_request_signature(request)
        now = self._clock()
        age = now - request.timestamp
        if age < -self._max_clock_skew or age > request.deadline_seconds:
            raise SandboxAuthenticationError("Sandbox RPC 请求时间或期限无效")
        if request.capability not in self._allowed:
            raise SandboxCapabilityError(
                f"Sandbox capability 未授权: {request.capability.value}"
            )
        with self._lock:
            self._prune(now)
            if request.nonce in self._seen:
                raise SandboxReplayError("Sandbox RPC nonce 重放已拒绝")
            if len(self._seen) >= MAX_REPLAY_ENTRIES:
                # 未过期 nonce 不可被挤出，否则仍有效的旧请求可以重新执行。
                raise SandboxAuthenticationError("Sandbox RPC 防重放容量已满")
            self._seen[request.nonce] = now
        return request

    def _prune(self, now: float) -> None:
        cutoff = now - (300 + self._max_clock_skew)
        while self._seen:
            _, seen_at = next(iter(self._seen.items()))
            if seen_at >= cutoff:
                break
            self._seen.popitem(last=False)


def normalize_guest_path(path: Any) -> str:
    """把宿主提供的路径约束为 guest workspace 内的 POSIX 逻辑路径。"""
    value = str(path or "").strip()
    if value in {"", "."}:
        return "."
    if len(value) > 1024 or _CONTROL_RE.search(value) or "\\" in value:
        raise SandboxProtocolError("guest path 非法")
    if re.match(r"^[A-Za-z]:", value) or value.startswith(("/", "//")):
        raise SandboxProtocolError("guest path 必须是逻辑相对路径")
    logical = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in logical.parts):
        raise SandboxProtocolError("guest path 含目录穿越")
    return logical.as_posix()


def validate_exec_payload(
    payload: Mapping[str, Any],
    *,
    max_timeout_seconds: float = 300,
) -> dict[str, Any]:
    """校验 exec 闭集参数；不接受 shell command 字符串。"""
    if not isinstance(payload, Mapping):
        raise SandboxProtocolError("exec payload 必须是对象")
    if "argv" not in payload:
        raise SandboxProtocolError("exec payload 必须提供 argv")
    allowed_keys = {"argv", "cwd", "env", "timeout_seconds", "stdin"}
    if not set(payload).issubset(allowed_keys):
        raise SandboxProtocolError("exec payload 含未知字段")
    argv = payload["argv"]
    if not isinstance(argv, (list, tuple)) or not 1 <= len(argv) <= 64:
        raise SandboxProtocolError("exec argv 必须是 1 到 64 项的列表")
    normalized_argv: list[str] = []
    for item in argv:
        if not isinstance(item, str) or not item or len(item) > 4096 or "\0" in item:
            raise SandboxProtocolError("exec argv 项非法")
        normalized_argv.append(item)

    timeout_value = payload.get("timeout_seconds", min(30, max_timeout_seconds))
    if isinstance(timeout_value, bool) or not isinstance(timeout_value, (int, float)):
        raise SandboxProtocolError("exec timeout_seconds 必须是数字")
    timeout = float(timeout_value)
    if not 1 <= timeout <= min(300, float(max_timeout_seconds)):
        raise SandboxProtocolError("exec timeout_seconds 超出策略上限")

    env_value = payload.get("env", {})
    if not isinstance(env_value, Mapping) or len(env_value) > 64:
        raise SandboxProtocolError("exec env 必须是最多 64 项的对象")
    env: dict[str, str] = {}
    for key, item in env_value.items():
        if not isinstance(key, str) or not _ENV_KEY_RE.fullmatch(key):
            raise SandboxProtocolError("exec env key 非法")
        if not isinstance(item, str) or len(item) > 4096 or "\0" in item:
            raise SandboxProtocolError("exec env value 非法")
        env[key] = item

    stdin = payload.get("stdin", "")
    if not isinstance(stdin, str) or len(stdin.encode("utf-8")) > 64 * 1024:
        raise SandboxProtocolError("exec stdin 非法或过大")
    return {
        "argv": normalized_argv,
        "cwd": normalize_guest_path(payload.get("cwd", ".")),
        "env": env,
        "timeout_seconds": timeout,
        "stdin": stdin,
    }


def validate_sandbox_endpoint(endpoint: Any) -> str:
    """RPC 只允许 HTTPS 到明确数字地址或宿主回环。"""
    value = str(endpoint or "").strip()
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise SandboxProtocolError("Sandbox endpoint 格式非法") from exc
    if parsed.scheme != "https" or not host or port is None:
        raise SandboxProtocolError("Sandbox endpoint 必须是带端口的 HTTPS 地址")
    if parsed.username is not None or parsed.password is not None:
        raise SandboxProtocolError("Sandbox endpoint 不允许内嵌凭据")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise SandboxProtocolError("Sandbox endpoint 不允许路径、查询或片段")
    if host != "localhost":
        try:
            ipaddress.ip_address(host)
        except ValueError as exc:
            raise SandboxProtocolError(
                "Sandbox endpoint 必须使用数字 IP 或 localhost"
            ) from exc
    return value.rstrip("/")
