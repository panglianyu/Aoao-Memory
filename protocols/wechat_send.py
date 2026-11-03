"""指定会话的文本发送闭集；本地回显不是服务端送达回执。"""

from collections.abc import Mapping
from dataclasses import dataclass
import re
from typing import Protocol, runtime_checkable


OUTCOMES = frozenset({"rejected", "confirmed_local_echo", "unknown"})


def validate_send_identity(operation_id: str, conversation_id: str) -> None:
    if (
        type(operation_id) is not str or not re.fullmatch(r"wxsend_[0-9a-f]{32}", operation_id)
        or type(conversation_id) is not str or not re.fullmatch(r"wxguest_[0-9a-f]{32}", conversation_id)
    ):
        raise ValueError("微信发送标识非法")


@dataclass(frozen=True, slots=True, repr=False)
class TextSendRequest:
    operation_id: str
    conversation_id: str
    text: str

    def __post_init__(self) -> None:
        validate_send_identity(self.operation_id, self.conversation_id)
        if (
            type(self.text) is not str or not self.text.strip()
            or len(self.text.encode("utf-8")) > 4096
            or any((ord(c) < 32 and c not in "\n\t") or ord(c) == 127 for c in self.text)
        ):
            raise ValueError("微信发送正文非法或过长")

    def to_payload(self) -> dict:
        return {"operation": "send_text", "operation_id": self.operation_id,
                "conversation_id": self.conversation_id, "text": self.text}

    @classmethod
    def from_payload(cls, payload: Mapping) -> "TextSendRequest":
        if not isinstance(payload, Mapping) or set(payload) != {
            "operation", "operation_id", "conversation_id", "text"
        } or payload.get("operation") != "send_text":
            raise ValueError("微信发送参数非法")
        return cls(payload["operation_id"], payload["conversation_id"], payload["text"])


@dataclass(frozen=True, slots=True, repr=False)
class MediaSendRequest:
    operation_id: str
    conversation_id: str
    asset_id: str
    kind: str

    def __post_init__(self) -> None:
        validate_send_identity(self.operation_id, self.conversation_id)
        if (
            type(self.asset_id) is not str
            or not re.fullmatch(r"wxasset_[0-9a-f]{32}", self.asset_id)
            or self.kind not in {"image", "file"}
        ):
            raise ValueError("微信附件发送参数非法")

    def to_payload(self) -> dict:
        return {
            "operation": "send_media",
            "operation_id": self.operation_id,
            "conversation_id": self.conversation_id,
            "asset_id": self.asset_id,
            "kind": self.kind,
        }

    @classmethod
    def from_payload(cls, payload: Mapping) -> "MediaSendRequest":
        if (
            not isinstance(payload, Mapping)
            or set(payload) != {
                "operation", "operation_id", "conversation_id", "asset_id", "kind",
            }
            or payload.get("operation") != "send_media"
        ):
            raise ValueError("微信附件发送参数非法")
        return cls(
            payload["operation_id"], payload["conversation_id"],
            payload["asset_id"], payload["kind"],
        )


def validate_send_query(payload: Mapping) -> None:
    if not isinstance(payload, Mapping) or set(payload) != {
        "operation", "operation_id", "conversation_id"
    } or payload.get("operation") != "send_status":
        raise ValueError("微信发送结果查询非法")
    validate_send_identity(payload["operation_id"], payload["conversation_id"])


def send_result(request: TextSendRequest | MediaSendRequest, outcome: str) -> dict:
    if outcome not in OUTCOMES:
        raise ValueError("微信发送结果非法")
    return {"schema_version": 1, "operation_id": request.operation_id,
            "conversation_id": request.conversation_id, "outcome": outcome}


def parse_send_result(payload: dict, operation_id: str, conversation_id: str) -> str:
    if (
        type(payload) is not dict or set(payload) != {
            "schema_version", "operation_id", "conversation_id", "outcome"
        } or type(payload["schema_version"]) is not int or payload["schema_version"] != 1
        or payload["operation_id"] != operation_id or payload["conversation_id"] != conversation_id
        or type(payload["outcome"]) is not str or payload["outcome"] not in OUTCOMES
    ):
        raise ValueError("微信发送回执与原操作不匹配")
    return payload["outcome"]


def parse_chat_status(payload: dict) -> dict:
    if (
        type(payload) is not dict or set(payload) != {"schema_version", "state", "conversation_id"}
        or type(payload["schema_version"]) is not int or payload["schema_version"] != 1
        or type(payload["state"]) is not str or payload["state"] not in {"ready", "paused", "failed"}
        or type(payload["conversation_id"]) is not str
        or not re.fullmatch(r"wxguest_[0-9a-f]{32}", payload["conversation_id"])
    ):
        raise ValueError("微信会话状态非法")
    return payload


@runtime_checkable
class TextSendPort(Protocol):
    async def send_text(self, request: TextSendRequest, *, deadline: float) -> dict: ...

    async def send_status(self, operation_id: str, conversation_id: str) -> dict: ...

    def chat_status(self) -> dict: ...


@runtime_checkable
class MediaSendPort(Protocol):
    async def send_media(self, request: MediaSendRequest, *, deadline: float) -> dict: ...
