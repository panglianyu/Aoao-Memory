"""文本收信批次与只读/确认端口；不授予身份或微信操作权限。"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from protocols.wechat_messages import (
    WeChatEvent,
    WeChatMediaEvent,
    WeChatMessageError,
    WeChatTextEvent,
    parse_event,
)


def _token(value: str, prefix: str) -> None:
    if type(value) is not str or not re.fullmatch(prefix + r"[0-9a-f]{32}", value):
        raise ValueError


@dataclass(frozen=True, slots=True, repr=False)
class InboxBatch:
    stream_id: str
    batch_id: str | None
    sequences: tuple[int, ...]
    events: tuple[WeChatEvent, ...]
    overflowed: bool = False

    def __post_init__(self) -> None:
        try:
            _token(self.stream_id, "wxstream_")
            if (
                type(self.sequences) is not tuple
                or type(self.events) is not tuple
                or not len(self.sequences) == len(self.events) <= 16
                or type(self.overflowed) is not bool
                or any(type(item) not in {WeChatTextEvent, WeChatMediaEvent} for item in self.events)
                or any(
                    type(item) is not int or not 1 <= item <= 2**53 - 1
                    for item in self.sequences
                )
                or any(
                    left >= right
                    for left, right in zip(self.sequences, self.sequences[1:])
                )
                or len({event.event_id for event in self.events}) != len(self.events)
            ):
                raise ValueError
            if self.events:
                if self.batch_id is None:
                    raise ValueError
                _token(self.batch_id, "wxbatch_")
            elif self.batch_id is not None:
                raise ValueError
        except (ValueError, TypeError):
            raise WeChatMessageError("微信收信批次格式非法") from None

    def to_payload(self) -> dict:
        return {
            "schema_version": 1,
            "stream_id": self.stream_id,
            "batch_id": self.batch_id,
            "sequences": list(self.sequences),
            "events": [event.to_payload() for event in self.events],
            "overflowed": self.overflowed,
        }

    @classmethod
    def from_payload(cls, value: dict) -> "InboxBatch":
        try:
            if (
                type(value) is not dict
                or set(value)
                != {
                    "schema_version",
                    "stream_id",
                    "batch_id",
                    "sequences",
                    "events",
                    "overflowed",
                }
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or type(value["sequences"]) is not list
                or type(value["events"]) is not list
                or not len(value["sequences"]) == len(value["events"]) <= 16
            ):
                raise ValueError
            return cls(
                value["stream_id"],
                value["batch_id"],
                tuple(value["sequences"]),
                tuple(parse_event(item) for item in value["events"]),
                value["overflowed"],
            )
        except (ValueError, TypeError, KeyError):
            raise WeChatMessageError("微信收信批次格式非法") from None


def validate_inbox_request(value: Mapping) -> None:
    """只接受 read/ack；在持久化 nonce 或触碰缓冲之前调用。"""
    try:
        if not isinstance(value, Mapping):
            raise ValueError
        operation = value.get("operation")
        if operation == "read" and set(value) == {"operation", "limit"}:
            if type(value["limit"]) is not int or not 1 <= value["limit"] <= 16:
                raise ValueError
        elif operation == "ack" and set(value) == {
            "operation",
            "stream_id",
            "batch_id",
        }:
            _token(value["stream_id"], "wxstream_")
            _token(value["batch_id"], "wxbatch_")
        else:
            raise ValueError
    except (ValueError, TypeError):
        raise WeChatMessageError("微信收信请求参数非法") from None


def parse_acknowledgement(value: dict, *, stream_id: str, batch_id: str) -> bool:
    if (
        type(value) is not dict
        or set(value) != {"schema_version", "stream_id", "batch_id", "acknowledged"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["stream_id"] != stream_id
        or value["batch_id"] != batch_id
        or type(value["acknowledged"]) is not bool
    ):
        raise WeChatMessageError("微信收信确认响应非法")
    return value["acknowledged"]


@runtime_checkable
class TextInboxPort(Protocol):
    """可信本机端口：实现仅短小同步内存操作，不启动驱动或做阻塞 I/O。"""

    def read(self, *, limit: int = 16) -> InboxBatch: ...

    def acknowledge(self, *, stream_id: str, batch_id: str) -> bool: ...
