"""Workspace/Browser 事件与安全契约。

本模块只定义跨内核、Web 与前端共享的稳定协议，不启动浏览器进程。大体积
截图和下载文件不得塞进事件 data；事件只携带 revision、受限元数据和产物引用。
"""
from __future__ import annotations

import ipaddress
import json
import math
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import urlsplit


WORKSPACE_PROTOCOL_VERSION = 1
MAX_EVENT_DATA_BYTES = 64 * 1024
MAX_URL_CHARS = 4096
MAX_TITLE_CHARS = 512
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


class WorkspaceEventKind(str, Enum):
    """浏览器工作空间可观察生命周期。"""

    WORKSPACE_CREATED = "workspace_created"
    SESSION_READY = "session_ready"
    TAB_OPENED = "tab_opened"
    NAVIGATION_STARTED = "navigation_started"
    NAVIGATION_COMPLETED = "navigation_completed"
    SNAPSHOT_READY = "snapshot_ready"
    ACTION_STARTED = "action_started"
    ACTION_COMPLETED = "action_completed"
    DOWNLOAD_READY = "download_ready"
    APPROVAL_REQUIRED = "approval_required"
    SANDBOX_READY = "sandbox_ready"
    FILE_CHANGED = "file_changed"
    TERMINAL_STARTED = "terminal_started"
    TERMINAL_OUTPUT = "terminal_output"
    TERMINAL_COMPLETED = "terminal_completed"
    ERROR = "error"
    TAB_CLOSED = "tab_closed"
    SESSION_CLOSED = "session_closed"
    WORKSPACE_CLOSED = "workspace_closed"


class BrowserActionRisk(str, Enum):
    """浏览器动作风险档；owner 闸门之外，sensitive 仍需显式审批。"""

    READ = "read"
    INTERACT = "interact"
    SENSITIVE = "sensitive"


BROWSER_ACTION_RISKS: Mapping[str, BrowserActionRisk] = MappingProxyType({
    "open": BrowserActionRisk.READ,
    "navigate": BrowserActionRisk.READ,
    "inspect": BrowserActionRisk.READ,
    "screenshot": BrowserActionRisk.READ,
    "back": BrowserActionRisk.READ,
    "forward": BrowserActionRisk.READ,
    "reload": BrowserActionRisk.READ,
    "wait": BrowserActionRisk.READ,
    "list_tabs": BrowserActionRisk.READ,
    "list_sessions": BrowserActionRisk.READ,
    "click": BrowserActionRisk.INTERACT,
    "type": BrowserActionRisk.INTERACT,
    "select": BrowserActionRisk.INTERACT,
    "scroll": BrowserActionRisk.INTERACT,
    "close_tab": BrowserActionRisk.INTERACT,
    "close_session": BrowserActionRisk.INTERACT,
    "download": BrowserActionRisk.SENSITIVE,
    "upload": BrowserActionRisk.SENSITIVE,
    "submit": BrowserActionRisk.SENSITIVE,
    "authenticate": BrowserActionRisk.SENSITIVE,
})


def browser_action_risk(action: str) -> BrowserActionRisk:
    """返回已登记动作风险；未知动作 fail-closed，不允许运行时自由扩权。"""
    key = str(action or "").strip().lower()
    try:
        return BROWSER_ACTION_RISKS[key]
    except KeyError as exc:
        raise ValueError(f"未知浏览器动作: {action}") from exc


def browser_action_requires_approval(action: str) -> bool:
    return browser_action_risk(action) is BrowserActionRisk.SENSITIVE


def validate_browser_url(url: str, *, allow_private_networks: bool = False) -> str:
    """校验导航 URL 的静态边界。

    运行时仍必须在 DNS 解析后和每次重定向后复核目标 IP，防 DNS rebinding；
    此处只负责拒绝危险 scheme、凭据 URL、控制字符和显式私网地址。
    """
    value = str(url or "").strip()
    if not value or len(value) > MAX_URL_CHARS:
        raise ValueError("URL 为空或过长")
    if _CONTROL_CHARS_RE.search(value):
        raise ValueError("URL 含控制字符")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
    except ValueError as exc:
        raise ValueError("URL 格式非法") from exc
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("仅允许 http/https URL")
    if not host:
        raise ValueError("URL 缺少主机名")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL 不允许内嵌账号或密码")

    normalized_host = host.rstrip(".").lower()
    if not allow_private_networks:
        if normalized_host == "localhost" or normalized_host.endswith(".localhost"):
            raise ValueError("默认禁止访问本机地址")
        try:
            ip = ipaddress.ip_address(normalized_host)
        except ValueError:
            ip = None
        if ip is not None and not ip.is_global:
            raise ValueError("默认禁止访问私网或特殊地址")
    return value


def _validate_id(name: str, value: str, *, optional: bool = False) -> str:
    text = str(value or "")
    if optional and not text:
        return ""
    if not _ID_RE.fullmatch(text):
        raise ValueError(f"{name} 格式非法")
    return text


@dataclass(frozen=True)
class WorkspaceEvent:
    """不可变、可 JSON 序列化的工作空间事件。"""

    workspace_id: str
    session_id: str
    kind: WorkspaceEventKind
    revision: int
    run_id: str = ""
    turn_id: str = ""
    tab_id: str = ""
    url: str = ""
    title: str = ""
    status: str = ""
    data: Mapping[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        _validate_id("workspace_id", self.workspace_id)
        _validate_id("session_id", self.session_id)
        _validate_id("run_id", self.run_id, optional=True)
        _validate_id("turn_id", self.turn_id, optional=True)
        _validate_id("tab_id", self.tab_id, optional=True)
        if isinstance(self.kind, str):
            try:
                object.__setattr__(self, "kind", WorkspaceEventKind(self.kind))
            except ValueError as exc:
                raise ValueError(f"未知 workspace event: {self.kind}") from exc
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 0:
            raise ValueError("revision 必须是非负整数")
        if not math.isfinite(float(self.timestamp)) or float(self.timestamp) <= 0:
            raise ValueError("timestamp 必须是有效时间")
        if len(str(self.url)) > MAX_URL_CHARS:
            raise ValueError("事件 URL 过长")
        if len(str(self.title)) > MAX_TITLE_CHARS:
            raise ValueError("事件标题过长")
        if len(str(self.status)) > 64:
            raise ValueError("事件状态过长")
        if not isinstance(self.data, Mapping):
            raise TypeError("data 必须是映射")
        frozen_data = MappingProxyType(dict(self.data))
        try:
            encoded = json.dumps(
                dict(frozen_data), ensure_ascii=False, allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("事件 data 必须可安全 JSON 序列化") from exc
        if len(encoded) > MAX_EVENT_DATA_BYTES:
            raise ValueError("事件 data 超过 64 KiB；大产物必须使用引用")
        object.__setattr__(self, "data", frozen_data)

    def to_payload(self) -> dict[str, Any]:
        return {
            "protocol_version": WORKSPACE_PROTOCOL_VERSION,
            "workspace_id": self.workspace_id,
            "session_id": self.session_id,
            "kind": self.kind.value,
            "revision": self.revision,
            "timestamp": float(self.timestamp),
            "run_id": self.run_id,
            "turn_id": self.turn_id,
            "tab_id": self.tab_id,
            "url": str(self.url),
            "title": str(self.title),
            "status": str(self.status),
            "data": dict(self.data),
        }
