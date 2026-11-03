# G:\Aoao_OS\kernel\platform_hub.py
"""
统一外接平台管理层 (0.3.25.0 Phase 1)。

职责:
  - 平台状态聚合: 从各平台 app 实例字段 + 暂停标记合成统一 PlatformInfo。
  - 监听目标: 平台原目标清单 (群/好友等, 来自平台 app 查询命令) 与本地策略
    (显示名/监听内容/回复策略/记忆策略/权限/免打扰) 合并成 TargetRow。
  - 策略持久化: data/platform/listening_targets.json (原子写, 复用 listen_chat_store 模式)。
    平台原名称/平台对象 ID 只读, 永远来自平台侧; 本地只存用户可改字段。
  - 连接日志: 环状缓冲 (上限 200 条), 内存驻留。
  - 暂停语义: 记 paused 标记 + 断开 app (停收消息), resume 恢复连接并清标记。

铁律:
  - 不 import 任何平台 app 模块 (只经 bus.available_apps 实例访问, 遵循 AppBus 总线红线)。
  - 不写记忆库, 不调 LLM。
  - 对平台查询指令全部设超时, 超时降级 (状态未知而非卡死 UI)。

数据文件结构 (data/platform/listening_targets.json):
  {
    "version": 1,
    "paused_platforms": ["wechat"],
    "targets": [
      {
        "id": "wechat:group:学习交流群",          # 本地派生: platform:type:platform_target_id
        "platform": "wechat",
        "platform_target_id": "学习交流群",        # 平台原 ID (微信无稳定 ID, 用会话名)
        "original_name": "学习交流群",             # 只读, 平台侧
        "display_name": "学习交流群",              # 可改
        "target_type": "group",                    # group|friend|channel|live_room|creator|dm
        "enabled": true,                           # 启停开关
        "listen_events": [],                       # 监听内容多选
        "reply_policy": "smart",                   # mention_only|smart|always|listen_only
        "allow_proactive_speech": true,
        "allow_send_image": true,
        "allow_send_file": true,
        "allow_send_voice": true,
        "max_replies_per_turn": 1,
        "min_reply_interval": 0,
        "daily_proactive_limit": 0,
        "short_term_memory": true,
        "long_term_memory": true,
        "extract_relations": true,
        "interest_records": true,
        "remember_non_master": false,
        "memory_namespace": "",                    # 建议 platform + account_id + target_id
        "is_master": false,
        "is_trusted": false,
        "tool_permission": "none",                 # none|safe|confirm|full
        "allow_local_ops": false,
        "allow_settings": false,
        "allow_high_risk": false,
        "require_master_confirm": false,
        "quiet_hours": {                           # 免打扰
          "enabled": false, "start": "22:00", "end": "08:00",
          "weekdays": "", "holidays": "",
          "urgent_only": false, "keep_memory": true,
        },
      }
    ]
  }
"""
from __future__ import annotations

import asyncio
import ast
import json
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------- 平台注册表

# 平台状态枚举 (string 值即 REST 契约, 前端 frames.ts 镜像)
STATUS_NOT_INSTALLED = "not_installed"
STATUS_NOT_CONNECTED = "not_connected"
STATUS_CONNECTING = "connecting"
STATUS_CONNECTED = "connected"
STATUS_RELOGIN = "relogin"
STATUS_ERROR = "error"
STATUS_PAUSED = "paused"

# 目标类型
TYPE_GROUP = "group"
TYPE_FRIEND = "friend"
TYPE_CHANNEL = "channel"
TYPE_LIVE_ROOM = "live_room"
TYPE_CREATOR = "creator"
TYPE_DM = "dm"

# 回复策略
POLICY_MENTION_ONLY = "mention_only"
POLICY_SMART = "smart"
POLICY_ALWAYS = "always"
POLICY_LISTEN_ONLY = "listen_only"

# 工具权限
TOOL_NONE = "none"
TOOL_SAFE = "safe"
TOOL_CONFIRM = "confirm"
TOOL_FULL = "full"

# 内置平台元数据。app_name 指向 bus 上的平台 app; 无对应 app 即"未安装"。
# filters 是前端监听对象筛选选项 (Phase 3 新平台接入后由对应 app 真实填充)。
@dataclass(frozen=True)
class PlatformMeta:
    name: str
    display_name: str
    app_name: str
    filters: tuple = ("全部",)


PLATFORM_META: tuple = (
    PlatformMeta("wechat", "微信", "wechat", ("全部", "群聊", "好友")),
    PlatformMeta("feishu", "飞书", "feishu", ("全部", "好友")),
    PlatformMeta("qq", "QQ", "qq", ("全部", "群聊", "好友")),
    PlatformMeta("bilibili", "B站", "bilibili", ("全部", "直播间", "UP主", "动态", "私信", "评论区")),
    PlatformMeta("discord", "Discord", "discord", ("全部", "服务器", "频道", "话题", "私信")),
    PlatformMeta("telegram", "Telegram", "telegram", ("全部", "群组", "频道", "私信")),
)

_META_BY_NAME: Dict[str, PlatformMeta] = {m.name: m for m in PLATFORM_META}

# 平台查询指令超时 (子进程通信可能慢, 超时降级不卡 UI)
_TARGET_QUERY_TIMEOUT = 6.0
# 平台原始清单内存缓存秒数 (避免每次打开页面都调子进程查询)
_TARGET_CACHE_TTL = 30.0
# 连接日志环状缓冲上限
_LOG_LIMIT = 200


# ---------------------------------------------------------------- 策略模型

@dataclass(frozen=True)
class QuietHours:
    enabled: bool = False
    start: str = "22:00"
    end: str = "08:00"
    weekdays: str = ""       # 工作日规则 (如 "1,2,3,4,5", 空=不限制)
    holidays: str = ""       # 节假日规则 (如 "quiet"/"ignore", 空=不限制)
    urgent_only: bool = False   # 免打扰期间只处理紧急消息
    keep_memory: bool = True    # 免打扰期间是否继续记录记忆

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "QuietHours":
        raw = raw or {}
        return cls(
            enabled=bool(raw.get("enabled", False)),
            start=str(raw.get("start", "22:00")),
            end=str(raw.get("end", "08:00")),
            weekdays=str(raw.get("weekdays", "")),
            holidays=str(raw.get("holidays", "")),
            urgent_only=bool(raw.get("urgent_only", False)),
            keep_memory=bool(raw.get("keep_memory", True)),
        )


@dataclass(frozen=True)
class TargetPolicy:
    """监听对象策略。全部字段可本地持久化; original_name/platform_target_id 除外。"""

    id: str
    platform: str
    platform_target_id: str
    original_name: str = ""
    display_name: str = ""
    target_type: str = TYPE_GROUP
    enabled: bool = True
    listen_events: tuple = ()
    reply_policy: str = POLICY_SMART
    allow_proactive_speech: bool = True
    allow_send_image: bool = True
    allow_send_file: bool = True
    allow_send_voice: bool = True
    max_replies_per_turn: int = 1
    min_reply_interval: int = 0
    daily_proactive_limit: int = 0
    short_term_memory: bool = True
    long_term_memory: bool = True
    extract_relations: bool = True
    interest_records: bool = True
    remember_non_master: bool = False
    memory_namespace: str = ""
    is_master: bool = False
    is_trusted: bool = False
    tool_permission: str = TOOL_NONE
    allow_local_ops: bool = False
    allow_settings: bool = False
    allow_high_risk: bool = False
    require_master_confirm: bool = False
    quiet_hours: QuietHours = field(default_factory=QuietHours)

    @classmethod
    def from_dict(cls, raw: dict) -> "TargetPolicy":
        return cls(
            id=str(raw.get("id", "")),
            platform=str(raw.get("platform", "")),
            platform_target_id=str(raw.get("platform_target_id", "")),
            original_name=str(raw.get("original_name", "")),
            display_name=str(raw.get("display_name", "")),
            target_type=str(raw.get("target_type", TYPE_GROUP)),
            enabled=bool(raw.get("enabled", True)),
            listen_events=tuple(raw.get("listen_events") or ()),
            reply_policy=str(raw.get("reply_policy", POLICY_SMART)),
            allow_proactive_speech=bool(raw.get("allow_proactive_speech", True)),
            allow_send_image=bool(raw.get("allow_send_image", True)),
            allow_send_file=bool(raw.get("allow_send_file", True)),
            allow_send_voice=bool(raw.get("allow_send_voice", True)),
            max_replies_per_turn=int(raw.get("max_replies_per_turn", 1)),
            min_reply_interval=int(raw.get("min_reply_interval", 0)),
            daily_proactive_limit=int(raw.get("daily_proactive_limit", 0)),
            short_term_memory=bool(raw.get("short_term_memory", True)),
            long_term_memory=bool(raw.get("long_term_memory", True)),
            extract_relations=bool(raw.get("extract_relations", True)),
            interest_records=bool(raw.get("interest_records", True)),
            remember_non_master=bool(raw.get("remember_non_master", False)),
            memory_namespace=str(raw.get("memory_namespace", "")),
            is_master=bool(raw.get("is_master", False)),
            is_trusted=bool(raw.get("is_trusted", False)),
            tool_permission=str(raw.get("tool_permission", TOOL_NONE)),
            allow_local_ops=bool(raw.get("allow_local_ops", False)),
            allow_settings=bool(raw.get("allow_settings", False)),
            allow_high_risk=bool(raw.get("allow_high_risk", False)),
            require_master_confirm=bool(raw.get("require_master_confirm", False)),
            quiet_hours=QuietHours.from_dict(raw.get("quiet_hours")),
        )

    def to_dict(self) -> dict:
        data = asdict(self)
        data["listen_events"] = list(data["listen_events"])
        return data


def target_id_for(platform: str, target_type: str, platform_target_id: str) -> str:
    """本地目标 ID 派生: platform:type:platform_target_id (冒号分隔, 微信用会话名)。"""
    return f"{platform}:{target_type}:{platform_target_id}"


# ---------------------------------------------------------------- 平台查询

def _result_text(result: Any) -> str:
    """ToolResult / 裸字符串统一取文本 (镜像 routes_console._payload 契约)。"""
    text = getattr(result, "text", None)
    return text if isinstance(text, str) else str(result)


def _parse_literal_list(text: str) -> List[str]:
    """解析 Python 字面量字符串列表 ("['群A', '群B']" 等, wx_bridge 返回格式)。

    ast.literal_eval 只接受字面量, 无执行风险; 失败降级按行拆, 再失败返回空。
    """
    text = (text or "").strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except (ValueError, SyntaxError):
        pass
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return lines


def _parse_literal_data(text: str) -> Any:
    """解析可能含 ok/data 的 JSON 或 Python 字面量字典; 失败返回 None。"""
    text = (text or "").strip()
    if not text:
        return None
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
            if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
                return parsed["data"]
            return parsed
        except (ValueError, SyntaxError, TypeError):
            continue
    return None


def _friend_name(entry: Any) -> str:
    """好友条目取名字: dict 优先 nickname/remark/wxid 键 (wxautox GetFriendDetails
    字段名不完全可控, 防御性取, 全缺回退字符串形式)。"""
    if isinstance(entry, dict):
        for key in ("nickname", "remark", "wxid", "name"):
            val = entry.get(key)
            if val:
                return str(val)
    return str(entry)


# ---------------------------------------------------------------- 平台 Hub

class PlatformHub:
    """统一平台管理层: 状态聚合 + 目标策略存取 + 连接日志。"""

    def __init__(self, store_path: str) -> None:
        self.store_path = store_path
        self._lock = asyncio.Lock()
        self._paused: set = set()
        self._policies: Dict[str, TargetPolicy] = {}
        self._logs: Dict[str, List[dict]] = {}
        self._target_cache: Dict[str, tuple] = {}  # platform -> (ts, raw)
        self._deleted: set = set()  # 本会话已删除目标 ID (防重复 delete 误报成功)

    # ---------------------------------------------------------- 持久化

    async def load(self) -> None:
        def _do_load() -> dict:
            if os.path.exists(self.store_path):
                try:
                    with open(self.store_path, "r", encoding="utf-8") as f:
                        return json.load(f)
                except (json.JSONDecodeError, OSError) as e:
                    logger.error(f"[platform_hub] 策略文件损坏, 按空库启动: {e}")
            return {}
        data = await asyncio.to_thread(_do_load)
        self._paused = set(data.get("paused_platforms") or [])
        for raw in data.get("targets") or []:
            try:
                policy = TargetPolicy.from_dict(raw)
                if policy.id:
                    self._policies[policy.id] = policy
            except (TypeError, ValueError) as e:
                logger.warning(f"[platform_hub] 跳过损坏策略条目: {e}")

    async def _save(self) -> None:
        async with self._lock:
            payload = {
                "version": 1,
                "paused_platforms": sorted(self._paused),
                "targets": [p.to_dict() for p in self._policies.values()],
            }

            def _do_save() -> None:
                os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
                tmp = self.store_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                if os.path.exists(self.store_path):
                    try:
                        os.replace(self.store_path, self.store_path + ".bak")
                    except OSError:
                        pass
                os.replace(tmp, self.store_path)

            await asyncio.to_thread(_do_save)

    # ---------------------------------------------------------- 状态聚合

    def _platform_status(self, meta: PlatformMeta, bus: Any) -> str:
        app = (getattr(bus, "available_apps", {}) or {}).get(meta.app_name)
        if app is None:
            return STATUS_NOT_INSTALLED
        active = getattr(bus, "active_apps", {}) or {}
        if meta.name in self._paused:
            return STATUS_PAUSED
        if meta.name not in active:
            return STATUS_NOT_CONNECTED
        if meta.name == "wechat" and hasattr(app, "linux_channel_connected"):
            if not app.linux_channel_connected:
                return STATUS_NOT_CONNECTED
        # 已连接: 平台 app 自身异常信号 (微信看门狗熔断 -> error)
        crashes = getattr(app, "_consecutive_crashes", 0)
        max_crashes = getattr(app, "_max_crashes", 0)
        if crashes and max_crashes and crashes >= max_crashes:
            return STATUS_ERROR
        return STATUS_CONNECTED

    def _platform_account(self, meta: PlatformMeta, bus: Any) -> str:
        app = (getattr(bus, "available_apps", {}) or {}).get(meta.app_name)
        if app is None:
            return ""
        if meta.name == "wechat":
            # 微信无账号概念: 展示绑定窗口昵称 (launcher.bind_nickname)
            launcher = (getattr(app, "_config", {}) or {}).get("launcher", {}) or {}
            return str(launcher.get("bind_nickname", "") or "")
        if meta.name == "feishu":
            return str(getattr(app, "account", "") or "")
        return ""

    def _platform_error(self, meta: PlatformMeta, bus: Any) -> str:
        if self._platform_status(meta, bus) != STATUS_ERROR:
            return ""
        app = (getattr(bus, "available_apps", {}) or {}).get(meta.app_name)
        crashes = getattr(app, "_consecutive_crashes", 0)
        max_crashes = getattr(app, "_max_crashes", 0)
        return f"守护进程连续崩溃 {crashes}/{max_crashes} 次, 看门狗熔断"

    async def list_platforms(self, bus: Any) -> List[dict]:
        rows = []
        for meta in PLATFORM_META:
            status = self._platform_status(meta, bus)
            error = self._platform_error(meta, bus)
            try:
                targets = await self._load_targets(meta.name, bus)
            except ValueError:
                app = (getattr(bus, "available_apps", {}) or {}).get("wechat")
                if meta.name != "wechat" or not callable(getattr(app, "list_linux_targets", None)):
                    raise
                targets = []
                status = STATUS_ERROR
                error = "微信目标目录不可用；请核对后恢复"
            counts: Dict[str, int] = {}
            for t in targets:
                counts[t["target_type"]] = counts.get(t["target_type"], 0) + 1
            rows.append({
                "name": meta.name,
                "display_name": meta.display_name,
                "installed": meta.app_name in (getattr(bus, "available_apps", {}) or {}),
                "status": status,
                "account": self._platform_account(meta, bus),
                "target_counts": counts,
                "filters": list(meta.filters),
                "error": error,
            })
        return rows

    # ---------------------------------------------------------- 目标清单

    async def _load_targets(self, platform: str, bus: Any) -> List[dict]:
        """平台原始目标清单 (缓存 TTL 内直接复用)。"""
        meta = _META_BY_NAME.get(platform)
        if meta is None:
            return []
        cached = self._target_cache.get(platform)
        now = time.time()
        if cached and (now - cached[0]) < _TARGET_CACHE_TTL:
            return cached[1]
        if meta.name == "wechat":
            raw = await self._wechat_targets(bus)
        elif meta.name == "feishu":
            raw = await self._feishu_targets(bus)
        else:
            raw = []
        self._target_cache[platform] = (now, raw)
        return raw

    async def _wechat_targets(self, bus: Any) -> List[dict]:
        """微信目标: listen_chats (已监听会话) + get_groups/get_friends 分类 + 活跃账本。

        全部指令设超时, 失败降级 (分类未知不致命)。"""
        app = (getattr(bus, "available_apps", {}) or {}).get("wechat")
        if app is None:
            return []
        linux_targets = getattr(app, "list_linux_targets", None)
        if callable(linux_targets):
            records = await linux_targets()
            connected = "wechat" in (getattr(bus, "active_apps", {}) or {})
            if hasattr(app, "linux_channel_connected"):
                connected = connected and app.linux_channel_connected
            channel_status = getattr(app, "linux_channel_status", {})
            if not isinstance(channel_status, dict):
                channel_status = {}
            channel_state = channel_status.get("state", "unknown")
            allowed_send_ids = set(channel_status.get(
                "allowed_send_conversation_ids", [],
            ))
            return [{
                "platform_target_id": row.conversation_id,
                "original_name": row.chat_name,
                "display_name": row.display_name,
                "target_type": {"direct": TYPE_FRIEND, "group": TYPE_GROUP,
                                "unknown": "unknown"}[row.chat_type],
                  "enabled": row.enabled,
                  "listening": connected and row.enabled,
                  "send_enabled": (
                      row.enabled and row.conversation_id in allowed_send_ids
                  ),
                  "capability_status": (
                      "unverified" if row.chat_type == "unknown" else
                      "paused" if not row.enabled else
                      "pending_recovery" if channel_state == "pending_recovery" else
                      "offline" if not connected else
                      "read_only" if row.chat_type == "group" and
                      row.conversation_id not in allowed_send_ids else "ready"
                  ),
                "last_active": 0.0, "message_count": 0, "is_master_dm": False,
                "linux_guest": True,
            } for row in records]
        monitored = list((getattr(app, "_config", {}) or {}).get("listen_chats", []) or [])

        async def _query(command: str, timeout: float):
            try:
                return await bus.send_instruction("wechat", command, timeout=timeout)
            except Exception as e:
                logger.warning(f"[platform_hub] wechat {command} 查询失败: {e}")
                return None

        groups_result, friends_result, overview_result = await asyncio.gather(
            _query("get_groups", _TARGET_QUERY_TIMEOUT),
            _query("get_friends", _TARGET_QUERY_TIMEOUT),
            _query("wechat_overview", _TARGET_QUERY_TIMEOUT),
        )
        groups = set(_parse_literal_list(_result_text(groups_result))) if groups_result else set()
        friends = set()
        if friends_result:
            data = _parse_literal_data(_result_text(friends_result))
            if isinstance(data, list):
                friends = {_friend_name(e) for e in data}
        activity: Dict[str, dict] = {}
        if overview_result:
            parsed = _parse_literal_data(_result_text(overview_result))
            if isinstance(parsed, dict):
                activity = parsed.get("activity") or {}

        rows: List[dict] = []
        seen: set = set()
        # 已监听会话优先 (原名称即会话名; 类型经群/好友集合判定, 未知降级 group)
        for chat in monitored:
            if not chat or chat in seen:
                continue
            seen.add(chat)
            if chat in groups:
                ttype = TYPE_GROUP
            elif chat in friends:
                ttype = TYPE_FRIEND
            else:
                ttype = TYPE_GROUP
            act = activity.get(chat) or {}
            rows.append({
                "platform_target_id": chat,
                "original_name": chat,
                "target_type": ttype,
                "listening": True,  # 在平台监听列表中
                "last_active": float(act.get("last_ts", 0) or 0),
                "message_count": int(act.get("count", 0) or 0),
                "is_master_dm": bool(act.get("is_master_dm", False)),
            })
        # 群/好友候选 (未监听的也可在 UI 添加监听)
        for g in sorted(groups):
            if g in seen:
                continue
            seen.add(g)
            rows.append({
                "platform_target_id": g,
                "original_name": g,
                "target_type": TYPE_GROUP,
                "listening": False,
                "last_active": 0.0,
                "message_count": 0,
                "is_master_dm": False,
            })
        return rows

    async def _feishu_targets(self, bus: Any) -> List[dict]:
        """飞书目标: 静态 1v1 老板 (飞书通道只服务老板本人)。"""
        app = (getattr(bus, "available_apps", {}) or {}).get("feishu")
        if app is None:
            return []
        user_map = getattr(app, "user_map", None) or {}
        master_id = getattr(app, "master_open_id", "") or ""
        display = next(iter(user_map.values()), "老板") if user_map else "老板"
        return [{
            "platform_target_id": master_id or display,
            "original_name": display,
            "target_type": TYPE_FRIEND,
            "listening": True,  # 飞书 1v1 恒在监听
            "last_active": 0.0,
            "message_count": 0,
            "is_master_dm": True,
        }]

    # ---------------------------------------------------------- 目标行 (策略合并)

    async def list_targets(self, platform: str, bus: Any) -> List[dict]:
        meta = _META_BY_NAME.get(platform)
        if meta is None:
            return []
        if meta.app_name not in (getattr(bus, "available_apps", {}) or {}):
            return []
        raw_rows = await self._load_targets(platform, bus)
        rows = []
        for raw in raw_rows:
            pid = str(raw.get("platform_target_id", ""))
            ttype = str(raw.get("target_type", TYPE_GROUP))
            tid = target_id_for(platform, ttype, pid)
            orig = str(raw.get("original_name", "") or pid)
            policy = self._policies.get(tid)
            if policy is None:
                # 列表 id 用 conversation_id, 策略 id 用 chat_name; 按原名回退匹配
                policy = self._policies.get(target_id_for(platform, ttype, orig))
            rows.append({
                "id": tid,
                "platform": platform,
                "platform_target_id": pid,
                "original_name": orig,
                "display_name": policy.display_name if policy and policy.display_name else
                                str(raw.get("display_name", orig)),
                "target_type": ttype,
                "enabled": (bool(raw.get("enabled", False)) and (policy.enabled if policy else True))
                           if raw.get("linux_guest") else (policy.enabled if policy else True),
                "listening": bool(raw.get("listening", False)) if raw.get("linux_guest") else
                             bool(raw.get("listening", False)) or policy is not None,
                "last_active": float(raw.get("last_active", 0) or 0),
                "message_count": int(raw.get("message_count", 0) or 0),
                "is_master_dm": bool(raw.get("is_master_dm", False)),
                "policy": policy.to_dict() if policy else None,
                "capability_status": raw.get("capability_status"),
                "send_enabled": bool(raw.get("send_enabled", False)),
            })
        return rows

    def invalidate_target_cache(self, platform: str) -> None:
        """清平台原始清单缓存 (sync 联系人 / 添加删除监听后调用)。"""
        self._target_cache.pop(platform, None)

    def find_policy_for(self, platform: str, chat_name: str) -> Optional[TargetPolicy]:
        """按平台 + 对话名查目标策略 (回复裁决用; 群/好友/私信类型逐个试)。

        未保存策略返回 None (裁决回退通用闸门)。"""
        for ttype in (TYPE_GROUP, TYPE_FRIEND, TYPE_DM):
            policy = self._policies.get(target_id_for(platform, ttype, chat_name))
            if policy is not None:
                return policy
        return None

    # ---------------------------------------------------------- 同步设置

    _SYNC_DEFAULT: dict = {
        "auto_sync_contacts": True,
        "sync_interval_sec": 300,
        "refresh_on_startup": True,
        "auto_reconnect": True,
        "max_retries": 3,
        "fetch_batch_size": 50,
        "concurrency_limit": 2,
        "per_platform_queue_limit": 200,
        "retry_failed": True,
        "history_backfill_hours": 24,
        "log_retention_days": 7,
    }

    async def load_sync_settings(self) -> dict:
        def _do_load() -> dict:
            path = self.store_path.replace("listening_targets.json", "sync_settings.json")
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        return data
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning(f"[platform_hub] 同步设置损坏, 用默认: {e}")
            return {}
        data = await asyncio.to_thread(_do_load)
        merged = dict(self._SYNC_DEFAULT)
        merged.update({k: v for k, v in data.items() if k in self._SYNC_DEFAULT})
        return merged

    async def save_sync_settings(self, patch: dict) -> dict:
        current = dict(self._SYNC_DEFAULT)
        async with self._lock:
            def _do_load() -> dict:
                path = self.store_path.replace("listening_targets.json", "sync_settings.json")
                if os.path.exists(path):
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        if isinstance(data, dict):
                            return data
                    except (json.JSONDecodeError, OSError):
                        pass
                return {}
            current.update(await asyncio.to_thread(_do_load))
            for key, value in patch.items():
                if key in self._SYNC_DEFAULT:
                    current[key] = value

            def _do_save(data: dict) -> None:
                path = self.store_path.replace("listening_targets.json", "sync_settings.json")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)

            await asyncio.to_thread(_do_save, current)
        return current

    # ---------------------------------------------------------- 配置导出/导入

    _SECRET_KEYWORDS = (
        "key", "secret", "token", "password", "cookie", "credential", "authorization",
    )

    @classmethod
    def _redact(cls, value: Any, key: str = "") -> Any:
        """递归脱敏: key 命中密钥关键词的值替换为掩码 (导出兜底防线)。"""
        if isinstance(value, dict):
            return {k: cls._redact(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._redact(v, key) for v in value]
        if any(kw in key.lower() for kw in cls._SECRET_KEYWORDS):
            return "***"
        return value

    async def export_config(self, bus: Any) -> dict:
        """导出配置 (不含任何凭证): 平台摘要 + 目标策略 + 同步设置 + 连接日志计数。"""
        platforms = await self.list_platforms(bus)
        payload = {
            "version": 1,
            "exported_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "platforms": [
                {"name": p["name"], "display_name": p["display_name"],
                 "installed": p["installed"], "status": p["status"]}
                for p in platforms
            ],
            "targets": [p.to_dict() for p in self._policies.values()],
            "sync_settings": await self.load_sync_settings(),
        }
        return self._redact(payload)

    async def import_config(self, data: dict) -> int:
        """导入目标策略 (平台原信息字段只读, 忽略); 返回导入条数。"""
        count = 0
        for raw in data.get("targets") or []:
            try:
                policy = TargetPolicy.from_dict(raw)
                if not policy.id or policy.platform not in _META_BY_NAME:
                    continue
                # 原名称/对象 ID 只读: 保留本地既有值 (新目标用导入值作种子)
                existing = self._policies.get(policy.id)
                if existing is not None:
                    policy = TargetPolicy.from_dict({
                        **policy.to_dict(),
                        "original_name": existing.original_name,
                        "platform_target_id": existing.platform_target_id,
                    })
                self._policies[policy.id] = policy
                count += 1
            except (TypeError, ValueError) as e:
                logger.warning(f"[platform_hub] 跳过损坏导入条目: {e}")
        await self._save()
        return count

    # ---------------------------------------------------------- 策略操作

    def _ensure_policy(self, platform: str, row: dict, default_master: bool = False) -> TargetPolicy:
        tid = target_id_for(platform, str(row.get("target_type", TYPE_GROUP)), str(row.get("platform_target_id", "")))
        policy = self._policies.get(tid)
        if policy is None:
            policy = TargetPolicy(
                id=tid,
                platform=platform,
                platform_target_id=str(row.get("platform_target_id", "")),
                original_name=str(row.get("original_name", "") or row.get("platform_target_id", "")),
                display_name=str(row.get("original_name", "") or row.get("platform_target_id", "")),
                target_type=str(row.get("target_type", TYPE_GROUP)),
                is_master=default_master,
                is_trusted=default_master,
            )
            self._policies[tid] = policy
        return policy

    def _policy_from_id(self, target_id: str) -> Optional[TargetPolicy]:
        """从目标 ID 派生默认策略 (platform:type:platform_target_id)。

        未保存过策略的目标 (UI 直接改配置) 用此兜底, 原名称回退平台目标 ID。
        """
        parts = target_id.split(":", 2)
        if len(parts) != 3:
            return None
        platform, ttype, pid = parts
        return TargetPolicy(
            id=target_id,
            platform=platform,
            platform_target_id=pid,
            original_name=pid,
            display_name=pid,
            target_type=ttype,
        )

    async def toggle_target(self, platform: str, target_id: str, enabled: bool,
                            bus: Any = None) -> Optional[dict]:
        app = (getattr(bus, "available_apps", {}) or {}).get("wechat") if bus is not None else None
        if platform == "wechat" and app is not None and callable(getattr(app, "list_linux_targets", None)):
            records = await app.list_linux_targets()
            row = next((record for record in records if
                        target_id == target_id_for("wechat", {
                            "direct": TYPE_FRIEND, "group": TYPE_GROUP,
                            "unknown": "unknown",
                        }[record.chat_type], record.conversation_id)), None)
            if row is None or row.chat_type == "unknown":
                return None
            await app.set_linux_target_enabled(row.conversation_id, enabled)
            self.invalidate_target_cache("wechat")
            return {"display_name": row.display_name, "enabled": enabled}
        policy = self._policies.get(target_id)
        if policy is None:
            policy = self._policy_from_id(target_id)
            if policy is None:
                return None
            self._policies[target_id] = policy
        self._policies[target_id] = TargetPolicy.from_dict({**policy.to_dict(), "enabled": enabled})
        await self._save()
        return self._policies[target_id].to_dict()

    async def update_target_policy(self, target_id: str, patch: dict, *, bus: Any = None) -> Optional[dict]:
        """合并更新策略可改字段 (original_name/platform_target_id 只读, 忽略补丁)。

        未保存过策略的目标自动建默认策略再合并 (原名称回退平台目标 ID)。"""
        if target_id.startswith("wechat:") and ":wxguest_" in target_id:
            if set(patch) == {"send_enabled"}:
                if type(patch["send_enabled"]) is not bool:
                    raise ValueError("发送许可必须是明确的布尔值")
                app = (getattr(bus, "available_apps", {}) or {}).get("wechat")
                setter = getattr(app, "set_linux_target_send_enabled", None)
                if not callable(setter):
                    raise ValueError("Linux 微信发送许可管理尚未就绪")
                records = await app.list_linux_targets()
                row = next((record for record in records if record.chat_type != "unknown"
                            and target_id == target_id_for("wechat", {
                                "direct": TYPE_FRIEND, "group": TYPE_GROUP,
                            }[record.chat_type], record.conversation_id)), None)
                if row is None:
                    raise ValueError("Linux 微信目标未核验")
                await setter(row.conversation_id, patch["send_enabled"])
                self.invalidate_target_cache("wechat")
                return {"target_id": target_id, "send_enabled": patch["send_enabled"]}
            if set(patch) != {"display_name"}:
                raise ValueError("Linux 微信目标仅支持本地显示别名；其他权限不可在此授予")
        policy = self._policies.get(target_id)
        if policy is None:
            policy = self._policy_from_id(target_id)
            if policy is None:
                return None
            self._policies[target_id] = policy
        current = policy.to_dict()
        allowed = {
            "display_name", "enabled", "listen_events", "reply_policy",
            "allow_proactive_speech", "allow_send_image", "allow_send_file",
            "allow_send_voice", "max_replies_per_turn", "min_reply_interval",
            "daily_proactive_limit", "short_term_memory", "long_term_memory",
            "extract_relations", "interest_records", "remember_non_master",
            "memory_namespace", "is_master", "is_trusted", "tool_permission",
            "allow_local_ops", "allow_settings", "allow_high_risk",
            "require_master_confirm", "quiet_hours",
        }
        for key in allowed:
            if key in patch:
                current[key] = patch[key]
        updated = TargetPolicy.from_dict(current)
        self._policies[target_id] = updated
        await self._save()
        return updated.to_dict()

    async def delete_target(self, platform: str, target_id: str, bus: Any) -> bool:
        """删除监听: 调平台 remove 命令 (微信) + 删本地策略。

        未保存过策略的目标 (无 policy) 也从 ID 解析后照常下发平台 remove。"""
        app = (getattr(bus, "available_apps", {}) or {}).get("wechat")
        if platform == "wechat" and app is not None and callable(getattr(app, "list_linux_targets", None)):
            records = await app.list_linux_targets()
            row = next((record for record in records if
                        target_id == target_id_for("wechat", {
                            "direct": TYPE_FRIEND, "group": TYPE_GROUP,
                            "unknown": "unknown",
                        }[record.chat_type], record.conversation_id)), None)
            remove = getattr(app, "remove_linux_target", None)
            if row is None or row.chat_type == "unknown" or not callable(remove):
                return False
            await remove(row.conversation_id)
            self._deleted.add(target_id)
            self._policies.pop(target_id, None)
            await self._save()
            self.invalidate_target_cache(platform)
            return True
        policy = self._policies.get(target_id)
        if policy is None:
            if target_id in self._deleted:
                return False
            policy = self._policy_from_id(target_id)
            if policy is None:
                return False
        self._deleted.add(target_id)
        self._policies.pop(target_id, None)
        if platform == "wechat" and policy.platform_target_id:
            try:
                await bus.send_instruction(
                    "wechat", "remove_listen_chat",
                    chat_name=policy.platform_target_id, timeout=_TARGET_QUERY_TIMEOUT,
                )
            except Exception as e:
                logger.warning(f"[platform_hub] wechat remove_listen_chat 失败: {e}")
        await self._save()
        self.invalidate_target_cache(platform)
        return True

    async def add_target(self, platform: str, platform_target_id: str, bus: Any) -> Optional[dict]:
        """添加监听: 微信调 add_listen_chat + 建默认策略。返回新目标行。"""
        app = (getattr(bus, "available_apps", {}) or {}).get("wechat")
        if platform == "wechat" and app is not None and callable(getattr(app, "list_linux_targets", None)):
            records = await app.list_linux_targets()
            row = next((record for record in records if
                        record.conversation_id == platform_target_id), None)
            enable = getattr(app, "set_linux_target_enabled", None)
            if row is None or row.chat_type == "unknown":
                verify_add = getattr(app, "add_linux_target", None)
                if not callable(verify_add):
                    return None
                row = await verify_add(row.chat_name if row is not None else platform_target_id)
            else:
                if not callable(enable):
                    return None
                await enable(row.conversation_id, True)
            self.invalidate_target_cache(platform)
            rows = await self.list_targets(platform, bus)
            return next((item for item in rows if
                         item["platform_target_id"] == row.conversation_id), None)
        if platform == "wechat":
            try:
                await bus.send_instruction(
                    "wechat", "add_listen_chat",
                    chat_name=platform_target_id, timeout=_TARGET_QUERY_TIMEOUT,
                )
            except Exception as e:
                logger.warning(f"[platform_hub] wechat add_listen_chat 失败: {e}")
        self.invalidate_target_cache(platform)
        rows = await self.list_targets(platform, bus)
        row = next((r for r in rows if r["platform_target_id"] == platform_target_id), None)
        if row is None:
            return None
        self._ensure_policy(platform, row)
        await self._save()
        return row

    # ---------------------------------------------------------- 暂停标记

    async def set_platform_paused(self, platform: str, paused: bool) -> None:
        if paused:
            self._paused.add(platform)
        else:
            self._paused.discard(platform)
        await self._save()

    def is_platform_paused(self, platform: str) -> bool:
        return platform in self._paused

    # ---------------------------------------------------------- 连接日志

    def add_log(self, platform: str, level: str, message: str) -> None:
        bucket = self._logs.setdefault(platform, [])
        bucket.append({
            "ts": time.time(),
            "level": level,
            "message": message,
        })
        if len(bucket) > _LOG_LIMIT:
            del bucket[: len(bucket) - _LOG_LIMIT]

    def recent_logs(self, platform: str, limit: int = 100) -> List[dict]:
        bucket = self._logs.get(platform, [])
        return list(bucket[-limit:])

    # ---------------------------------------------------------- 状态切换辅助

    async def mark_connect(self, platform: str, bus: Any) -> None:
        self._paused.discard(platform)
        await self._save()
        self.add_log(platform, "info", "已连接")

    async def mark_disconnect(self, platform: str, reason: str = "") -> None:
        self.add_log(platform, "info", f"已断开{(': ' + reason) if reason else ''}")
