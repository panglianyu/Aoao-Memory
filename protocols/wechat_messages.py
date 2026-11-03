"""未认证的微信文本观察值；不是主人身份、文件引用或执行指令。"""

import json
import math
import re
from dataclasses import asdict, dataclass, fields
from typing import TypeAlias

from protocols.wechat_artifacts import ArtifactDescriptor, WeChatArtifactError


MAX_EVENT_BYTES = 8192


def _text(value: str, limit: int, *, multiline: bool = False) -> None:
    if type(value) is not str or not 0 < len(value) <= limit:
        raise ValueError
    if len(value.encode("utf-8")) > limit or any(
        (ord(char) < 32 and not (multiline and char in "\t\r\n")) or ord(char) == 127
        for char in value
    ):
        raise ValueError


class WeChatMessageError(ValueError):
    """固定协议错误，不附带消息正文或个人标识。"""


@dataclass(frozen=True, slots=True, repr=False)
class WeChatTextEvent:
    event_id: str
    chat_name: str
    chat_type: str
    sender: str
    sender_id: str | None
    content: str
    observed_at: float
    schema_version: int = 1

    def __post_init__(self) -> None:
        try:
            if type(self.schema_version) is not int or self.schema_version != 1:
                raise ValueError
            if type(self.event_id) is not str or not re.fullmatch(
                r"wxevt_[0-9a-f]{32}", self.event_id
            ):
                raise ValueError
            if type(self.chat_type) is not str or self.chat_type not in {
                "direct",
                "group",
            }:
                raise ValueError
            _text(self.chat_name, 256)
            _text(self.sender, 256)
            if self.sender_id is not None:
                _text(self.sender_id, 128)
            _text(self.content, 4096, multiline=True)
            if (
                type(self.observed_at) not in (int, float)
                or not 0 <= self.observed_at <= 2**53 - 1
                or not math.isfinite(self.observed_at)
            ):
                raise ValueError
            object.__setattr__(self, "observed_at", float(self.observed_at))
            encoded = json.dumps(
                self.to_payload(), ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if len(encoded) > MAX_EVENT_BYTES:
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise WeChatMessageError("微信文本事件格式非法") from None

    @classmethod
    def from_payload(cls, value: dict) -> "WeChatTextEvent":
        if type(value) is not dict or set(value) != {item.name for item in fields(cls)}:
            raise WeChatMessageError("微信文本事件格式非法")
        return cls(**value)

    def to_payload(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True, repr=False)
class WeChatMediaEvent:
    event_id: str
    chat_name: str
    chat_type: str
    sender: str
    sender_id: str | None
    content: str
    observed_at: float
    kind: str
    capture_mode: str
    artifact: ArtifactDescriptor | None
    media_error: str | None
    schema_version: int = 2

    def __post_init__(self) -> None:
        try:
            if type(self.schema_version) is not int or self.schema_version != 2:
                raise ValueError
            if type(self.event_id) is not str or not re.fullmatch(
                r"wxevt_[0-9a-f]{32}", self.event_id
            ):
                raise ValueError
            if type(self.chat_type) is not str or self.chat_type not in {
                "direct",
                "group",
            }:
                raise ValueError
            _text(self.chat_name, 256)
            _text(self.sender, 256)
            if self.sender_id is not None:
                _text(self.sender_id, 128)
            _text(self.content, 4096, multiline=True)
            if (
                type(self.observed_at) not in (int, float)
                or not 0 <= self.observed_at <= 2**53 - 1
                or not math.isfinite(self.observed_at)
                or type(self.kind) is not str
                or self.kind not in {"image", "emotion"}
                or type(self.capture_mode) is not str
                or self.capture_mode not in {"preview", "original", "unavailable"}
            ):
                raise ValueError
            if self.capture_mode == "unavailable":
                if self.artifact is not None:
                    raise ValueError
                if self.media_error is None:
                    raise ValueError
                _text(self.media_error, 256)
            elif type(self.artifact) is not ArtifactDescriptor or self.media_error is not None:
                raise ValueError
            object.__setattr__(self, "observed_at", float(self.observed_at))
            encoded = json.dumps(
                self.to_payload(), ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if len(encoded) > MAX_EVENT_BYTES:
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise WeChatMessageError("微信媒体事件格式非法") from None

    @classmethod
    def from_payload(cls, value: dict) -> "WeChatMediaEvent":
        try:
            if type(value) is not dict or set(value) != {item.name for item in fields(cls)}:
                raise ValueError
            prepared = dict(value)
            if prepared["artifact"] is not None:
                prepared["artifact"] = ArtifactDescriptor.from_payload(prepared["artifact"])
            return cls(**prepared)
        except (ValueError, TypeError, KeyError, WeChatArtifactError):
            raise WeChatMessageError("微信媒体事件格式非法") from None

    def to_payload(self) -> dict:
        return {
            "event_id": self.event_id,
            "chat_name": self.chat_name,
            "chat_type": self.chat_type,
            "sender": self.sender,
            "sender_id": self.sender_id,
            "content": self.content,
            "observed_at": self.observed_at,
            "kind": self.kind,
            "capture_mode": self.capture_mode,
            "artifact": self.artifact.to_payload() if self.artifact is not None else None,
            "media_error": self.media_error,
            "schema_version": self.schema_version,
        }


WeChatEvent: TypeAlias = WeChatTextEvent | WeChatMediaEvent


def parse_event(payload: dict) -> WeChatEvent:
    if type(payload) is not dict or type(payload.get("schema_version")) is not int:
        raise WeChatMessageError("微信事件格式非法")
    if payload["schema_version"] == 1:
        return WeChatTextEvent.from_payload(payload)
    if payload["schema_version"] == 2:
        return WeChatMediaEvent.from_payload(payload)
    raise WeChatMessageError("微信事件格式非法")
