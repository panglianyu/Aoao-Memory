"""只读监听诊断端口；不包含微信身份、操作能力或原生监听保证。"""

import re
from typing import Protocol, runtime_checkable


STATUS_KEYS = frozenset({
    "state", "configuration_applied", "read_enabled", "wechat_ready", "execution_enabled",
    "failure_phase", "failed_target_id", "accepted_count", "outcome_unknown", "targets",
})
TARGET_KEYS = frozenset({
    "conversation_id", "source_state", "applied", "accepted_count", "outcome_unknown",
})


@runtime_checkable
class ListeningStatusPort(Protocol):
    """可信本机实现只能返回短小内存快照；不得调用 SDK 或阻塞 I/O。"""

    def status(self) -> dict: ...


def parse_listening_status(value: dict) -> dict:
    """严格版本/字段/类型与有界计数；返回脱离输入的快照，不接受能力升级。"""
    try:
        _record(value, {"schema_version", "listening"})
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError
        data = value["listening"]
        _record(data, STATUS_KEYS)
        _choice(data["state"], {"new", "applying", "ready", "polling", "failed", "stopped"})
        for key in ("configuration_applied", "read_enabled", "outcome_unknown"):
            if type(data[key]) is not bool:
                raise ValueError
        if data["wechat_ready"] is not False or data["execution_enabled"] is not False:
            raise ValueError
        if data["state"] in {"ready", "polling"} and not data["configuration_applied"]:
            raise ValueError
        _count(data["accepted_count"])
        if type(data["targets"]) is not list or not 1 <= len(data["targets"]) <= 64:
            raise ValueError
        rows = [_target(row) for row in data["targets"]]
        # 读取中只允许一个源处于 reading；其他源要么就绪，要么已明确暂停。
        source_states = [row["source_state"] for row in rows]
        if data["state"] == "new" and any(state != "new" for state in source_states):
            raise ValueError
        if data["state"] == "ready" and (
            "ready" not in source_states
            or any(state not in {"ready", "failed"} for state in source_states)
        ):
            raise ValueError
        if data["state"] == "polling" and (
            source_states.count("reading") != 1
            or any(state not in {"ready", "reading", "failed"} for state in source_states)
        ):
            raise ValueError
        if data["state"] == "failed" and any(
            state not in {"failed", "closed"} for state in source_states
        ):
            raise ValueError
        if data["state"] == "stopped" and any(state != "closed" for state in source_states):
            raise ValueError
        if data["read_enabled"] != (
            data["state"] == "ready" and "ready" in source_states
        ):
            raise ValueError
        ids = {row["conversation_id"] for row in rows}
        if (
            len(ids) != len(rows)
            or data["accepted_count"] != sum(row["accepted_count"] for row in rows)
            or data["outcome_unknown"] != any(row["outcome_unknown"] for row in rows)
            or data["configuration_applied"] and not all(row["applied"] for row in rows)
        ):
            raise ValueError
        if data["failure_phase"] is None:
            if (
                data["failed_target_id"] is not None
                or data["state"] == "failed"
                or "failed" in source_states
            ):
                raise ValueError
        else:
            _choice(data["failure_phase"], {"apply", "poll"})
            _choice(data["failed_target_id"], ids)
            failed_row = next(
                row for row in rows if row["conversation_id"] == data["failed_target_id"]
            )
            if (
                data["state"] not in {"ready", "polling", "failed", "stopped"}
                or data["state"] in {"ready", "polling"}
                and data["failure_phase"] != "poll"
                or data["state"] in {"ready", "polling"}
                and failed_row["source_state"] != "failed"
            ):
                raise ValueError
        return {**data, "targets": rows}
    except (ValueError, TypeError, KeyError):
        raise ValueError("微信监听状态响应非法") from None


def _record(value: dict, keys: set | frozenset) -> None:
    if type(value) is not dict or set(value) != keys:
        raise ValueError


def _choice(value: str, choices: set) -> None:
    if type(value) is not str or value not in choices:
        raise ValueError


def _count(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 2**53 - 1:
        raise ValueError


def _target(row: dict) -> dict:
    _record(row, TARGET_KEYS)
    key = row["conversation_id"]
    if type(key) is not str or not re.fullmatch(r"wxguest_[0-9a-f]{32}", key):
        raise ValueError
    _choice(row["source_state"], {"new", "binding", "ready", "reading", "failed", "closed"})
    if type(row["applied"]) is not bool or type(row["outcome_unknown"]) is not bool:
        raise ValueError
    _count(row["accepted_count"])
    return dict(row)
