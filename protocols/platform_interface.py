# G:\Aoao_OS\protocols\platform_interface.py
"""
统一平台 Adapter Interface (0.3.25.2 Phase 3 架子)。

每个外接平台 app (apps/qq, apps/bilibili, apps/discord, apps/telegram) 必须实现
本 Protocol, 经 AppBus/AoaoPlug 接入主流程。约束 (规格第九节):
  - 平台插件互相禁止 import; UI 禁止直接访问平台 SDK; 平台插件禁止直接写记忆库。
  - 凭证只存 config/<platform>.yaml, 禁止出现在前端/日志/配置导出。

实现约定:
  - 状态串与 kernel/platform_hub 的 PlatformStatus 对齐 (not_installed/not_connected/
    connecting/connected/relogin/error/paused)。
  - 目标行 dict 字段与 platform_hub 原始清单契约一致:
    {"platform_target_id", "original_name", "target_type", "listening",
     "last_active", "message_count", "is_master_dm"}。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class PlatformInterface(Protocol):
    """外接平台 Adapter 统一接口。各平台 app 的 AoaoPlug 实现本协议。"""

    # ---------------- 生命周期 ----------------

    async def connect(self) -> bool:
        """建立平台连接 (登录/鉴权/拉起监听)。成功返回 True。"""
        ...

    async def disconnect(self) -> bool:
        """断开连接并释放资源 (子进程/长连接/轮询任务)。"""
        ...

    def status(self) -> str:
        """当前连接状态 (platform_hub PlatformStatus 字符串)。"""
        ...

    def health_check(self) -> Dict[str, Any]:
        """轻量探活: {"ok": bool, "message": str, "detail": {...}}。"""
        ...

    # ---------------- 监听目标 ----------------

    async def list_targets(self) -> List[dict]:
        """平台可监听目标清单 (群/好友/频道/直播间/UP主等, 字段见模块 docstring)。"""
        ...

    async def subscribe_target(self, target_id: str) -> bool:
        """开始监听目标 (platform_target_id)。"""
        ...

    async def unsubscribe_target(self, target_id: str) -> bool:
        """停止监听目标。"""
        ...

    async def update_target_policy(self, target_id: str, policy: dict) -> bool:
        """下发目标级策略 (reply_policy/免打扰等, 平台侧可按需消费)。"""
        ...

    # ---------------- 发送 ----------------

    async def send_text(self, text: str, target: str = "") -> Any:
        """发送文本消息到目标 (空 target 回退主人默认目标)。"""
        ...

    async def send_image(self, path: str, target: str = "") -> Any:
        """发送本地图片。"""
        ...

    async def send_file(self, path: str, target: str = "") -> Any:
        """发送本地文件。"""
        ...

    # ---------------- 账号 ----------------

    @property
    def account(self) -> str:
        """当前登录账号显示名 (hub 平台列表展示)。"""
        ...


# 平台目标类型 (与 kernel/platform_hub 常量同源, 供 adapter 实现对齐)
TARGET_TYPES: Dict[str, str] = {
    "group": "群聊",
    "friend": "好友",
    "channel": "频道",
    "live_room": "直播间",
    "creator": "UP主",
    "dm": "私信",
}


class PlatformNotImplementedError(NotImplementedError):
    """骨架 app 占位: 真实接入未完成时抛此异常, 平台管理层将其转为明确错误提示。"""
