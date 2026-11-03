# G:\Aoao_OS\kernel\app_bus.py
"""
Aoao OS 核心事件总线 (Phase 1 强类型版本)

严格锁定 Message 数据载体: 所有通过 AppBus 传递的数据必须封装为 Message 对象。
任何试图传入 dict / str / 其他类型的调用都会立即抛出 TypeError。

[架构红线 - 内外双轨隔离原则]
AppBus 是 Aoao OS 的"内轨"核心中枢，以下 Native 组件享有第一公民特权：
  - aluo_brain (主脑推理引擎)
  - system_memory (记忆与潜意识系统，含 ReflectionDaemon / SchedulerDaemon)
  - feishu (主通信通道)

这些 Native 组件绝不允许被降级为 MCP Server。MCP (外轨) 仅适用于：
  - 外部第三方应用接入
  - 外部静态文档读取
  - 未来新增的非核心工具

任何试图将 Native 组件包装为 MCP Server 的操作都是架构违规。
"""
import os
import json
import importlib
import sys
import inspect
import traceback
import asyncio
import math
import secrets
import time
import weakref
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from protocols.standard_plug import AoaoPlug
from kernel.logger import get_logger
from kernel.message import Message
from kernel.tool_builder import ToolBuilder
from kernel.tool_result import ToolResult
from kernel.chat_scheduler import ChatScheduler
from kernel.app_lifecycle import CoordinatedAppLifecycle
from kernel.daily_memory_contracts import DailyRecallOutcome, RecallPurpose, recall_deadline, public_daily_health

if TYPE_CHECKING:
    from kernel.raw_message_service import PreparedReply, RawMessageService

logger = get_logger(__name__)


# ==========================================
# [0.3.28.0] 行动权威: 手动断开标记持久化
# 背景: 2026-08-08 老板 web 断开微信后, 大脑因未完成发送任务反复自调
# connect_app 重连, 形成对抗循环。老板手动断开的 app 内核不得自行重连。
# ==========================================

def _manual_disconnect_path(apps_dir: str) -> str:
    """标记文件路径: <项目根>/data/state/manual_disconnected.json。

    [0.3.37.0] 从 data/ 顶层归位 data/state/ (机器状态件目录,
    规范见 data/README.md)。
    """
    base = os.path.dirname(os.path.abspath(apps_dir))  # apps_dir 的上级即项目根
    return os.path.join(base, "data", "state", "manual_disconnected.json")


def _legacy_manual_disconnect_path(apps_dir: str) -> str:
    """[0.3.37.0] 升级前旧路径 (data/ 顶层), 供读侧兜底迁移。"""
    base = os.path.dirname(os.path.abspath(apps_dir))
    return os.path.join(base, "data", "manual_disconnected.json")


def _load_manual_disconnect_set(apps_dir: str) -> set:
    """加载标记集; 文件缺失/损坏静默降级空集 (不因标记文件挡住启动)。

    [0.3.37.0] 旧路径有件而新路径无: 加载并顺手迁移到 data/state/
    (boot 前防御, 不依赖 migrate_once 时序); 迁移失败回退直读旧路径。
    """
    path = _manual_disconnect_path(apps_dir)
    legacy = _legacy_manual_disconnect_path(apps_dir)
    if not os.path.exists(path) and os.path.exists(legacy):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            os.replace(legacy, path)
            logger.info("[Kernel] 手动断开标记自愈迁移: data/ 顶层 -> data/state/")
        except OSError as e:
            logger.warning(f"[Kernel] 手动断开标记迁移失败(非致命, 回退读旧路径): {e}")
            path = legacy
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {str(n) for n in data.get("apps", [])}
    except (OSError, ValueError):
        return set()


def _save_manual_disconnect_set(apps_dir: str, names: set) -> None:
    """原子写 (tmp + fsync + os.replace); 失败仅告警不阻断断连流程。"""
    path = _manual_disconnect_path(apps_dir)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"apps": sorted(names)}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError as e:
        logger.warning(f"[Kernel] 手动断开标记持久化失败: {e}")


def _is_new_style_callback(callback: Any) -> bool:
    """
    检测回调函数是否为新式接口 (接收 Message 对象)。

    判定规则 (满足任一即视为新式):
      - 第一个非 self 参数名为 'msg'
      - 第一个非 self 参数注解为 Message
    """
    try:
        sig = inspect.signature(callback)
        params = list(sig.parameters.values())
        # 跳过 self 参数 (绑定方法)
        if params and params[0].name == "self":
            params = params[1:]
        if not params:
            return False
        first_param = params[0]
        if first_param.name == "msg":
            return True
        annotation = first_param.annotation
        if annotation is Message:
            return True
        if isinstance(annotation, str) and annotation == "Message":
            return True
        return False
    except (ValueError, TypeError):
        return False


class AppBus:
    """
    系统核心事件总线。

    通信铁律:
      1. submit_input(msg) 只接收 Message 实例, 投入队列供主循环顺序消费
      2. emit_msg(msg) 只接收 Message 实例, 分发给所有订阅了对应 event_name 的回调
      3. 任何非 Message 类型的输入都会抛出 TypeError
    """

    # 动态超时默认值 (严禁一刀切): 不同 app 耗时差异极大。
    # 三级回退: 调用方显式 timeout > manifest.timeout > 此默认。
    DEFAULT_INSTRUCTION_TIMEOUT = 15   # 普通指令默认 (微信秒回类)
    DEFAULT_CONNECT_TIMEOUT = 30       # 连接默认 (资源初始化略重)

    # [0.2.23.0 性能刀4] 扫描黑名单: 这些目录有 main.py 但不注册进总线。
    # web_ui 模块级 import chainlit (重依赖白导, 拖慢 scan_apps 导入段),
    # 且它从不在 config/bios.yaml 自启名单, 是独立 web 入口 (不经 AppBus),
    # 跳过导入零风险, 实测白捡 1-2s 启动时间。
    SCAN_BLACKLIST = frozenset({"web_ui"})

    def __init__(self, apps_dir: str):
        self.apps_dir = apps_dir
        self.available_apps: Dict[str, AoaoPlug] = {}
        self.active_apps: Dict[str, AoaoPlug] = {}
        self._coordinated_lifecycle = CoordinatedAppLifecycle(self)

        # 强类型消息队列: 只允许 Message 实例
        self._message_queue: asyncio.Queue[Message] = asyncio.Queue()

        # 优先级事件: 外设推送 priority 消息(私聊老板/被@)时置位,
        # 主循环心跳据此立即唤醒秒回, 不必等满心跳周期。
        self._priority_event: asyncio.Event = asyncio.Event()

        # [抢占软熔断 v0.4.1] 老板本人 priority 消息进来时置位。
        # Brain._run_tool_loop 每轮循环开头检查, 命中则在当前 LLM/工具轮回填后
        # break, 让当前 think 提前结束 (不硬中断进行中 API 请求, 防 tool_calls
        # 半截状态污染)。主循环据此优先 drain 老板消息。
        self._master_preempt_flag: bool = False

        # 全局事件订阅表 (Pub/Sub)
        self.subscribers: Dict[str, List] = {}

        # 工具定义生成器（从 app_bus.py 抽离，按 manifest["hidden"] 过滤）
        self._tool_builder = ToolBuilder()

        # [阶段2 0.1.12.1] 通用消息调度器: per-chat 攒批 + pending 回灌 + 老板强插。
        # 时序层, 不做语义判断(语义交 MessageReviewer)。各渠道插件拆信封后调
        # scheduler.add, flush 后逐条 submit_input 进本类 _message_queue, 主循环
        # drain_batch 照常取。延迟绑定 bus 引用(构造时 loop 可能未起, ChatScheduler
        # 内 asyncio.Lock 在 3.10+ 不需 running loop)。
        self.scheduler: ChatScheduler = ChatScheduler(self)

        # [0.3.28.0] 手动断开标记集: 老板主动断开的 app, 内核不得自行重连
        # (connect_app 拒绝 / 开机自启跳过), 直到老板手动连接 (by_user=True 清标)。
        self._manually_disconnected: set = _load_manual_disconnect_set(apps_dir)
        self.raw_messages: Optional["RawMessageService"] = None
        self._daily_derivation = None
        self._daily_retrieval = None
        self._daily_runtime = None
        self._daily_last_recall: dict | None = None
        self._daily_legacy_provider = None
        self._daily_legacy_bindings: dict = {}
        self._daily_legacy_tasks: set[asyncio.Task] = set()
        self._daily_legacy_stopping = False
        self._turn_original: Optional[Message] = None
        self._raw_agent_id: Optional[Callable[[], str]] = None
        self._raw_local_delivery: dict[tuple[str, str], Callable] = {}
        self._raw_handoff: set[tuple[str, str]] = set()
        self._raw_review_ready: weakref.WeakValueDictionary[int, Message] = weakref.WeakValueDictionary()
        self._raw_review_issued: weakref.WeakValueDictionary[str, Message] = weakref.WeakValueDictionary()
        self._raw_review_pending: weakref.WeakValueDictionary[str, Message] = weakref.WeakValueDictionary()

    def find_memory_provider(self) -> str:
        """
        在已注册 app 中查找声明了 memory_provider=True 的记忆后端。
        Brain 和 main.py 共用此方法，避免发现逻辑重复。
        返回 app 名称，未找到则返回空字符串。
        """
        for name, app in self.available_apps.items():
            try:
                if app.manifest.get("memory_provider"):
                    return name
            except Exception:
                continue
        return ""

    # ==========================================
    # 强类型 API (唯一合法入口)
    # ==========================================

    def attach_raw_messages(self, service: "RawMessageService", agent_id: Callable[[], str]) -> None:
        """Trusted explicit assembly only; does not load configuration or enable apps."""
        if self.raw_messages is not None or not callable(agent_id):
            raise ValueError("raw capture assembly unavailable")
        self.raw_messages = service
        self._raw_agent_id = agent_id

    def attach_daily_derivation(self, processor) -> None:
        """Trusted host assembly only; not an execute_command/LLM tool route."""
        if self._daily_derivation is not None or not callable(getattr(processor, "process", None)):
            raise ValueError("daily derivation assembly unavailable")
        self._daily_derivation = processor

    def attach_daily_retrieval(self, retrieval) -> None:
        """Trusted host assembly; never exposed as an LLM plugin command."""
        raw = self.raw_messages
        if (self._daily_retrieval is not None or raw is None
                or getattr(retrieval, "raw", None) is not raw.store
                or raw.profile.derived_root is None
                or getattr(getattr(retrieval, "stores", None), "root", None)
                    != raw.profile.derived_root.resolve()):
            raise ValueError("daily retrieval assembly unavailable")
        self._daily_retrieval = retrieval

    def attach_daily_runtime(self, runtime) -> None:
        """Trusted factory binding, after Raw creation and before channel startup."""
        if (self._daily_runtime is not None or self._daily_derivation is not None
                or self._daily_retrieval is not None
                or getattr(runtime, "raw", None) is not self.raw_messages
                or not all(callable(getattr(runtime, name, None))
                           for name in ("start", "close", "health"))
                or not callable(getattr(runtime.derivation, "process", None))):
            raise ValueError("daily runtime assembly unavailable")
        self.attach_daily_retrieval(runtime.retrieval)
        self.attach_daily_derivation(runtime.derivation)
        self._daily_runtime = runtime

    def daily_memory_health(self) -> dict:
        """No conversation content or identifiers in the runtime health surface."""
        if self._daily_runtime is None:
            return {"state": "disabled"}
        return self._daily_runtime.health()

    def daily_memory_public_health(self) -> dict:
        try:
            result = public_daily_health(self.daily_memory_health())
            snapshot = self.raw_turn_snapshot()
            if snapshot and (self._raw_agent_id is None or snapshot.get("agent_id") != self._raw_agent_id()):
                self._daily_last_recall = None
        except Exception:
            result = public_daily_health(None)
            self._daily_last_recall = None
        if self._daily_last_recall is not None:
            result["last_recall"] = public_daily_health(self._daily_last_recall)
        return result

    def attach_daily_legacy(self, provider, bindings) -> None:
        """Only explicit local-owner mappings, never display-name group fallback."""
        from kernel.daily_memory_bootstrap import LegacySourceBinding
        from kernel.raw_message_models import Scope
        from kernel.raw_message_service import stable_id

        if (self.raw_messages is None or self._daily_legacy_provider is not None
                or type(bindings) is not tuple or provider not in self.available_apps.values()
                or not callable(getattr(provider, "read_daily_legacy", None))):
            raise ValueError("explicit legacy provider assembly required")
        selected = {}
        profile = self.raw_messages.profile
        for mapping in bindings:
            if type(mapping) is not LegacySourceBinding or mapping.owner_id != profile.owner_id:
                raise ValueError("legacy owner mapping differs")
            connector = next((item for item in profile.connectors if item.agent_id == mapping.agent_id
                and item.connector_id == mapping.connector_id and item.source in {"web", "console"}), None)
            chat = next((item for item in connector.conversations if item.provider_id == mapping.provider_id), None) if connector else None
            if chat is None or chat.kind == "group":
                raise ValueError("legacy conversation mapping unavailable")
            scope = Scope(mapping.agent_id, mapping.owner_id, mapping.connector_id,
                stable_id(mapping.agent_id, mapping.owner_id, mapping.connector_id, mapping.provider_id))
            if scope in selected:
                raise ValueError("duplicate legacy mapping")
            selected[scope] = mapping
        self._daily_legacy_provider, self._daily_legacy_bindings = provider, selected

    async def read_daily_legacy(self, query: str, *, deadline: float) -> DailyRecallOutcome:
        from kernel.raw_message_models import Scope

        if self._daily_legacy_stopping:
            return DailyRecallOutcome([], "unavailable", ("legacy_unavailable",))
        raw, turn = self.raw_messages, self._turn_original
        if raw is None or turn is None or self._raw_agent_id is None:
            return DailyRecallOutcome([], "unavailable", ("authority_denied",))
        try:
            deadline = recall_deadline(purpose="interactive", deadline=deadline)
            snapshot = self.raw_turn_snapshot()
            if not snapshot or snapshot.get("agent_id") != self._raw_agent_id():
                return DailyRecallOutcome([], "unavailable", ("authority_denied",))
            scope = Scope(snapshot["agent_id"], snapshot["owner_id"], snapshot["connector_id"], snapshot["conversation_id"])
            mapping = self._daily_legacy_bindings.get(scope)
            if mapping is None:
                return DailyRecallOutcome([], "unavailable", ("legacy_unmapped",))
            await asyncio.wait_for(raw.validate_turn(turn, deadline=deadline), max(0, deadline - time.monotonic()))
            if self._daily_legacy_stopping or self._daily_legacy_tasks:
                return DailyRecallOutcome([], "unavailable", ("legacy_unavailable",))
            read_legacy = getattr(self._daily_legacy_provider, "read_daily_legacy", None)
            if not callable(read_legacy):
                return DailyRecallOutcome([], "unavailable", ("legacy_unavailable",))
            pending = asyncio.create_task(read_legacy(scope, mapping, query))
            self._daily_legacy_tasks.add(pending)

            def settled(task):
                self._daily_legacy_tasks.discard(task)
                if not task.cancelled():
                    task.exception()

            pending.add_done_callback(settled)
            items = await asyncio.wait_for(asyncio.shield(pending), max(0, deadline - time.monotonic()))
            await asyncio.wait_for(raw.validate_turn(turn, deadline=deadline), max(0, deadline - time.monotonic()))
            if (self._turn_original is not turn or self.raw_turn_snapshot() != snapshot
                    or self._raw_agent_id() != snapshot["agent_id"]):
                return DailyRecallOutcome([], "unavailable", ("authority_denied",))
            if time.monotonic() >= deadline:
                return DailyRecallOutcome([], "unavailable", ("legacy_timeout",))
            return DailyRecallOutcome(items, "ready")
        except (asyncio.TimeoutError, TimeoutError):
            return DailyRecallOutcome([], "unavailable", ("legacy_timeout",))
        except Exception:
            return DailyRecallOutcome([], "unavailable", ("legacy_unavailable",))

    async def settle_daily_legacy(self) -> bool:
        self._daily_legacy_stopping = True
        if not self._daily_legacy_tasks:
            return True
        _, pending = await asyncio.wait(tuple(self._daily_legacy_tasks), timeout=5)
        return not pending

    def daily_legacy_inflight_provider(self):
        """Keep the concrete store owner alive, not every memory plugin."""
        if any(not task.done() for task in self._daily_legacy_tasks):
            return self._daily_legacy_provider
        return None

    async def recall_daily(self, query: str, *, top_k: int = 10,
                            neighbors: int = 0, start_ts=None, end_ts=None,
                            memory_scope: str = "auto", deadline: float | None = None,
                            purpose: RecallPurpose = "warm"):
        """Backward-compatible list surface; disabled still selects legacy mode."""
        if self._daily_retrieval is None:
            return None
        return (await self.recall_daily_with_status(query, top_k=top_k, neighbors=neighbors,
            start_ts=start_ts, end_ts=end_ts, memory_scope=memory_scope,
            deadline=deadline, purpose=purpose)).items

    async def recall_daily_with_status(self, query: str, *, top_k: int = 10,
                            neighbors: int = 0, start_ts=None, end_ts=None,
                            memory_scope: str = "auto", deadline: float | None = None,
                            purpose: RecallPurpose = "warm") -> DailyRecallOutcome:
        issued_turn = self._turn_original
        result = await self._recall_daily_with_status(query, top_k=top_k, neighbors=neighbors,
            start_ts=start_ts, end_ts=end_ts, memory_scope=memory_scope, deadline=deadline, purpose=purpose)
        if self._turn_original is issued_turn:
            self._daily_last_recall = {"state": result.status, "reason_codes": result.reason_codes,
                                       "result_count": len(result.items)}
        return result

    async def _recall_daily_with_status(self, query: str, *, top_k: int = 10,
                            neighbors: int = 0, start_ts=None, end_ts=None,
                            memory_scope: str = "auto", deadline: float | None = None,
                            purpose: RecallPurpose = "warm") -> DailyRecallOutcome:
        """Current sealed user turn is the only ordinary retrieval authority."""
        try:
            deadline = recall_deadline(purpose=purpose, deadline=deadline)
        except (ValueError, TypeError):
            return DailyRecallOutcome([], "unavailable", ("invalid_query",))
        if self._daily_retrieval is None:
            return DailyRecallOutcome([], "unavailable", ("disabled",))
        raw, turn = self.raw_messages, self._turn_original
        if raw is None or turn is None or self._raw_agent_id is None:
            return DailyRecallOutcome([], "unavailable", ("authority_denied",))
        try:
            snapshot = self.raw_turn_snapshot()
            if not snapshot or snapshot.get("agent_id") != self._raw_agent_id():
                return DailyRecallOutcome([], "unavailable", ("authority_denied",))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return DailyRecallOutcome([], "unavailable", ("deadline_exceeded",))
            await asyncio.wait_for(
                raw.validate_turn(turn, deadline=deadline),
                timeout=remaining)
            from kernel.raw_message_models import Scope
            scope = Scope(snapshot["agent_id"], snapshot["owner_id"],
                          snapshot["connector_id"], snapshot["conversation_id"])
            options = {"top_k": top_k, "neighbors": neighbors,
                       "start_ts": start_ts, "end_ts": end_ts,
                       "memory_scope": memory_scope, "deadline": deadline}
            retrieval = self._daily_retrieval
            service = getattr(self._daily_runtime, "search", None)
            search_status = getattr(retrieval, "search_with_status", None)
            if callable(service):
                if purpose != "warm":
                    options["purpose"] = purpose
                pending = service(scope, query, **options)
            elif callable(search_status):
                if purpose != "warm":
                    options["purpose"] = purpose
                pending = search_status(scope, query, **options)
            else:
                pending = retrieval.search(scope, query, **options)
            result = await asyncio.wait_for(pending, timeout=max(0, deadline - time.monotonic()))
            await asyncio.wait_for(raw.validate_turn(turn, deadline=deadline),
                                   timeout=max(0, deadline - time.monotonic()))
            if (self._turn_original is not turn or self.raw_turn_snapshot() != snapshot
                    or self._raw_agent_id() != snapshot["agent_id"]):
                return DailyRecallOutcome([], "unavailable", ("authority_denied",))
            if time.monotonic() >= deadline:
                return DailyRecallOutcome([], "unavailable", ("deadline_exceeded",))
            return result if isinstance(result, DailyRecallOutcome) else DailyRecallOutcome(result, "ready")
        except (asyncio.TimeoutError, TimeoutError):
            return DailyRecallOutcome([], "unavailable", ("deadline_exceeded",))
        except Exception as exc:
            logger.warning("daily recall denied: %s", type(exc).__name__)
            from kernel.raw_message_models import RawMessageError
            reason = "authority_denied" if isinstance(exc, RawMessageError) else "retrieval_error"
            return DailyRecallOutcome([], "unavailable", (reason,))

    async def read_daily_original_chunk(self, message_id: str, *, expected_revision: int,
                                        expected_content_hash: str, offset_chars: int = 0,
                                        limit_chars: int = 4096,
                                        deadline: float | None = None):
        """Internal original replay, authorized by the same sealed daily turn."""
        started = time.monotonic()
        if deadline is not None and (type(deadline) not in {int, float}
                                     or not math.isfinite(deadline)):
            return None
        deadline = min(deadline, started + 0.3) if deadline is not None else started + 0.3
        raw, retrieval, turn = self.raw_messages, self._daily_retrieval, self._turn_original
        if (raw is None or retrieval is None or turn is None or self._raw_agent_id is None
                or not callable(getattr(retrieval, "read_content_chunk", None))):
            return None
        try:
            snapshot = self.raw_turn_snapshot()
            if not snapshot or snapshot.get("agent_id") != self._raw_agent_id():
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await asyncio.wait_for(raw.validate_turn(turn, deadline=deadline), timeout=remaining)
            from kernel.raw_message_models import Scope
            scope = Scope(snapshot["agent_id"], snapshot["owner_id"],
                          snapshot["connector_id"], snapshot["conversation_id"])
            result = await retrieval.read_content_chunk(scope, message_id,
                expected_revision=expected_revision, expected_content_hash=expected_content_hash,
                offset_chars=offset_chars, limit_chars=limit_chars, deadline=deadline)
            if (self._turn_original is not turn or self.raw_turn_snapshot() != snapshot
                    or self._raw_agent_id() != snapshot["agent_id"]):
                return None
            return result
        except Exception as exc:
            logger.warning("daily original read denied: %s", type(exc).__name__)
            return None

    async def recent_daily(self, *, limit: int = 12):
        """Raw backfill for the sealed current turn; no display-name lookup."""
        if self._daily_retrieval is None or self.raw_messages is None:
            return []
        if self._turn_original is None or self._raw_agent_id is None:
            return []
        try:
            snapshot = self.raw_turn_snapshot()
            if not snapshot or snapshot.get("agent_id") != self._raw_agent_id():
                return []
            await self.raw_messages.validate_turn(self._turn_original)
            from kernel.raw_message_models import Scope
            scope = Scope(snapshot["agent_id"], snapshot["owner_id"],
                          snapshot["connector_id"], snapshot["conversation_id"])
            return await asyncio.to_thread(self.raw_messages.store.recent, scope,
                                           limit=limit)
        except Exception as exc:
            logger.warning("daily backfill denied: %s", type(exc).__name__)
            return []

    async def forget_daily(self, message_id: str) -> bool:
        """Forget one Raw ID inside the current verified conversation only."""
        if self._daily_retrieval is None or self.raw_messages is None:
            return False
        if self._turn_original is None or self._raw_agent_id is None:
            return False
        try:
            snapshot = self.raw_turn_snapshot()
            if not snapshot or snapshot.get("agent_id") != self._raw_agent_id():
                return False
            await self.raw_messages.validate_turn(self._turn_original)
            from kernel.raw_message_models import Scope
            scope = Scope(snapshot["agent_id"], snapshot["owner_id"],
                          snapshot["connector_id"], snapshot["conversation_id"])
            row = await asyncio.to_thread(self.raw_messages.store.get, scope, message_id,
                                          include_inactive=True)
            if row is None or row.status not in {"active", "edited", "forgotten"}:
                return False
            await asyncio.to_thread(self.raw_messages.store.forget, scope, message_id)
            return True
        except Exception as exc:
            logger.warning("daily forget denied: %s", type(exc).__name__)
            return False

    async def derive_raw_message(self, scope, task, message, guard, complete=None):
        if self._daily_derivation is None:
            raise RuntimeError("daily derivation unavailable")
        return await self._daily_derivation.process(scope, task, message, guard,
                                                    complete=complete)

    def daily_owns(self, msg: Message) -> bool:
        return bool(self._daily_derivation is not None and self.raw_messages is not None
                    and msg.get("_daily_owned") is True and self.raw_messages.verify(msg))

    async def emit_user_memory(self, event: Message, original: Optional[Message] = None,
                               *, owned: bool = True) -> None:
        if not owned:
            # Only explicitly unowned callers may retain the legacy path.
            if (original is not None or self._turn_original is not None
                    or any(key.startswith(("_raw_", "_daily_")) or key in {
                        "origin_message_id", "conversation_id", "connector_id"
                    } for key in event.metadata)):
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("owned event cannot use legacy delivery")
            await self.emit_msg(event)
            return
        if self._daily_derivation is not None and self.raw_messages is not None:
            original = original if original is not None else self._turn_original
            agent_id = self._raw_agent_id() if self._raw_agent_id else None
            if original is None or original.get("_raw_agent_id") != agent_id:
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("daily turn unavailable")
            await self.raw_messages.validate_turn(original)
            if self._raw_agent_id is None or self._raw_agent_id() != agent_id:
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("daily agent changed")
            event = self.raw_messages.project_saved_event(original, event)
        await self.emit_msg(event)

    async def emit_tool_memory(self, event: Message, *, turn: Optional[dict] = None) -> None:
        if self._daily_derivation is not None and self.raw_messages is not None:
            original = await self.raw_messages.restore_turn(turn) if turn is not None else self._turn_original
            if original is None or original.get("_raw_agent_id") != self._raw_agent_id():
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("daily turn unavailable")
            event = await self.raw_messages.capture_turn_event(original, event)
        await self.emit_msg(event)

    def register_raw_local_delivery(self, source: str, event_name: str, callback: Callable) -> None:
        """Register one awaited display handoff; ordinary subscribers remain asynchronous."""
        key = (source, event_name)
        if key in self._raw_local_delivery or not callable(callback):
            raise ValueError("local delivery adapter unavailable")
        self._raw_local_delivery[key] = callback

    async def prepare_input(self, msg: Message, *, agent_id: Optional[str] = None) -> Message:
        """Commit before entering a batching window; keep accepted identity on requeue."""
        if self.raw_messages is None:
            return msg
        if self._raw_agent_id is None:
            raise ValueError("raw capture identity unavailable")
        resolved_agent_id = agent_id if agent_id is not None else self._raw_agent_id()
        return await self.raw_messages.capture(msg, resolved_agent_id)

    async def accept_input(self, msg: Message) -> Message:
        accepted = await self.prepare_input(msg)
        if await self.claim_input_dispatch(accepted):
            self.submit_input(accepted)
        return accepted

    async def claim_input_dispatch(self, msg: Message) -> bool:
        if self.raw_messages is None:
            return True
        claimed = await self.raw_messages.claim_input_dispatch(msg)
        if claimed:
            self._raw_handoff.add((msg.get("origin_message_id"), msg.get("_raw_seal")))
        return claimed

    def take_review_handoff(self, msg: Message) -> str:
        """Issue a one-use local capability only for a queue-drained Raw message."""
        if (self.raw_messages is None or not self.raw_messages.verify(msg)
                or self._raw_review_ready.pop(id(msg), None) is not msg):
            from kernel.raw_message_models import RawMessageError
            raise RawMessageError("review handoff unavailable")
        token = secrets.token_urlsafe(24)
        self._raw_review_issued[token] = msg
        return token

    async def authorize_review_requeue(self, msgs: list[Message], tokens: tuple[str, ...]) -> None:
        from kernel.raw_message_models import RawMessageError
        if (self.raw_messages is None or self._raw_agent_id is None
                or len(msgs) != len(tokens) or len(set(tokens)) != len(tokens)
                or len({id(msg) for msg in msgs}) != len(msgs)):
            raise RawMessageError("review handoff unavailable")
        if any(self._raw_review_issued.get(token) is not msg for msg, token in zip(msgs, tokens)):
            raise RawMessageError("review handoff unavailable")
        for msg, token in zip(msgs, tokens):
            if self._raw_review_issued.pop(token, None) is not msg:
                raise RawMessageError("review handoff unavailable")
        agent_id = self._raw_agent_id()
        for msg in msgs:
            if not await self.raw_messages.review_handoff_allowed(msg, agent_id):
                raise RawMessageError("review input unavailable")
        if self._raw_agent_id() != agent_id:
            raise RawMessageError("review agent changed")
        for msg, token in zip(msgs, tokens):
            self._raw_review_pending[token] = msg

    async def submit_reviewed_input(self, msg: Message, token: str) -> None:
        from kernel.raw_message_models import RawMessageError
        if self.scheduler.closed:
            raise RawMessageError("review scheduler closed")
        if self._raw_review_pending.pop(token, None) is not msg or self._raw_agent_id is None:
            raise RawMessageError("review handoff unavailable")
        agent_id = self._raw_agent_id()
        if (self.raw_messages is None
                or not await self.raw_messages.review_handoff_allowed(msg, agent_id)
                or self._raw_agent_id() != agent_id):
            raise RawMessageError("review input unavailable")
        if self.scheduler.closed:
            raise RawMessageError("review scheduler closed")
        self._enqueue_input(msg, mark_dispatched=False)

    def bind_raw_turn(self, msg: Message) -> None:
        """Snapshot the verified input's ownership and outbound identity."""
        if self.raw_messages is None:
            return
        if not self.raw_messages.verify(msg):
            from kernel.raw_message_models import RawMessageError
            raise RawMessageError("invalid turn input")
        self._turn_original = msg
        self._daily_last_recall = None

    def clear_raw_turn(self) -> None:
        self._turn_original = None
        self._daily_last_recall = None

    def reply_text(self, text: str, turn_id: str, *, original: Optional[Message] = None) -> str:
        """Carry the producing turn with its string output, not a global last receipt."""
        from kernel.raw_message_service import ReplyIdentity, TurnReply
        from kernel.raw_message_models import RawMessageError
        if isinstance(text, TurnReply) or self.raw_messages is None:
            return text
        original = original if original is not None else self._turn_original
        if original is None:
            return text
        if not self.raw_messages.verify(original) or not isinstance(turn_id, str) or not turn_id:
            raise RawMessageError("reply turn required")
        return TurnReply(text, ReplyIdentity(original, turn_id))

    def raw_turn_snapshot(self) -> dict:
        msg = self._turn_original
        if msg is None or self.raw_messages is None:
            return {}
        if not self.raw_messages.verify(msg):
            from kernel.raw_message_models import RawMessageError
            raise RawMessageError("invalid turn input")
        return {
            "agent_id": msg.get("_raw_agent_id"), "event_id": msg.get("origin_message_id"),
            "source": msg.source, "target": msg.get("chat_name") or msg.source,
            "conversation_id": msg.get("conversation_id"),
            "owner_id": self.raw_messages.profile.owner_id,
            "connector_id": msg.get("connector_id"), "revision": msg.get("_raw_revision"),
        }

    @property
    def _raw_turn(self) -> dict:
        """Compatibility read view; never a second mutable ownership authority."""
        return self.raw_turn_snapshot()

    async def restore_raw_turn(self, saved: dict) -> None:
        self.clear_raw_turn()
        if self.raw_messages is None:
            return
        from kernel.raw_message_models import RawMessageError
        resolver = self._raw_agent_id
        if not saved or resolver is None or saved.get("agent_id") != resolver():
            raise RawMessageError("saved task agent unavailable")
        original = await self.raw_messages.restore_turn(saved)
        if original.get("_raw_agent_id") != resolver():
            raise RawMessageError("saved task agent changed")
        self.bind_raw_turn(original)

    async def _prepare_raw_reply(self, name: str, kwargs: dict,
                                 event_id: Optional[str],
                                 expected_agent_id: Optional[str] = None, *,
                                 turn_original: Optional[Message] = None,
                                 source_event_id: Optional[str] = None,
                                 target_hint: Optional[str] = None,
                                 claim_outbound: bool = True) -> Optional["PreparedReply"]:
        if self.raw_messages is None:
            return None
        from kernel.raw_message_models import RawMessageError
        from kernel.raw_message_service import TurnReply, stable_id
        text = kwargs.get("text")
        if isinstance(text, TurnReply):
            reply_original = text.identity.original
            event_id = stable_id(reply_original.get("origin_message_id"), text.identity.turn_id)
            if name != reply_original.source:
                raise RawMessageError("reply source conflict")
        current_agent_id = self._raw_agent_id() if self._raw_agent_id else ""
        if expected_agent_id is not None and current_agent_id != expected_agent_id:
            raise RawMessageError("outbound agent changed")
        agent_id = expected_agent_id or (turn_original.get("_raw_agent_id") if turn_original else current_agent_id)
        if agent_id != current_agent_id:
            raise RawMessageError("outbound agent changed")
        target = (target_hint if target_hint is not None
                  else kwargs.get("target_user") or kwargs.get("target") or "")
        if turn_original is not None and not target:
            target = turn_original.get("chat_name") or turn_original.source
        identity = event_id or source_event_id
        if not identity or not isinstance(text, str) or not isinstance(target, str) or not target:
            raise RawMessageError("resolved outbound identity required")
        ticket = await self.raw_messages.prepare_reply(
            agent_id, name, target, text, event_id or stable_id(identity, name, target, text),
            original=turn_original, claim_outbound=claim_outbound)
        if self._raw_agent_id is not None and self._raw_agent_id() != agent_id:
            await self._record_raw_delivery(ticket, "failed")
            raise RawMessageError("outbound agent changed")
        return ticket

    async def _record_raw_delivery(self, ticket: Optional["PreparedReply"], status: str) -> bool:
        if ticket is None or self.raw_messages is None:
            return True
        from kernel.raw_message_models import RawMessageError
        try:
            await self.raw_messages.record_delivery(ticket, status)
            return True
        except RawMessageError:
            logger.error("送达凭据未保存；发送结果未知，禁止自动重发。")
            return False

    @staticmethod
    def _delivery_result(name: str, ticket: Optional["PreparedReply"], raw: Any) -> tuple[ToolResult, str]:
        from kernel.raw_message_service import RawDeliveryReceipt

        if ticket is not None and name == "feishu" and type(raw) is RawDeliveryReceipt:
            valid = (raw.connector_id == ticket.connector_id
                     and raw.provider_id == ticket.provider_id
                     and raw.status in {"confirmed", "failed", "unknown"})
            status = raw.status if valid else "unknown"
            result = (ToolResult(name, True, "text", None, raw.text)
                      if status == "confirmed"
                      else ToolResult.error(name, raw.text if valid else "送达回执身份不一致"))
            return result, status
        if ticket is not None:
            return ToolResult.error(name, "发送结果未知；请核对渠道状态，禁止自动重发。"), "unknown"
        result = ToolResult.from_raw(name, raw)
        return result, "confirmed" if result.ok else "failed"

    @staticmethod
    def _send_kwargs(name: str, kwargs: dict, ticket: Optional["PreparedReply"],
                     idempotency_key: Optional[str]) -> dict:
        values = dict(kwargs)
        if ticket is not None and name == "feishu":
            values["target_user"] = ticket.provider_id
            values["connector_id"] = ticket.connector_id
        if idempotency_key is not None:
            values["idempotency_key"] = idempotency_key
        return values

    def submit_input(self, msg: Message) -> None:
        """
        将输入消息投入队列, 供主循环顺序消费。

        [铁律] msg 必须是 Message 实例, 否则抛出 TypeError。
        """
        if not isinstance(msg, Message):
            raise TypeError(
                f"[Kernel] submit_input() 只接受 Message 实例, "
                f"收到非法类型: {type(msg).__name__}。"
                f"请使用 Message.user_input() 等工厂方法封装数据。"
            )
        internal = False
        if self.raw_messages is not None and msg.msg_type.value == "user_input":
            internal = msg.source in {"system_scheduler", "system_memory_god", "auto_tester"}
            identity = (msg.get("origin_message_id"), msg.get("_raw_seal"))
            if not internal and (
                not self.raw_messages.verify(msg) or identity not in self._raw_handoff
            ):
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("unclaimed durable input")
            if not internal:
                self._raw_handoff.remove(identity)
        self._enqueue_input(msg, mark_dispatched=(self.raw_messages is not None
                             and msg.msg_type.value == "user_input" and not internal))

    def _enqueue_input(self, msg: Message, *, mark_dispatched: bool) -> None:
        self._message_queue.put_nowait(msg)
        if mark_dispatched and self.raw_messages is not None:
            try:
                self.raw_messages.mark_input_dispatched(msg)
            except Exception:
                logger.error("已入队消息的交付事件未能写入；保留领取状态，禁止自动重投。")
        # [0.3.25.1] 消息池入池 hook (可选挂载, 热路径 fire-and-forget, 绝不阻塞主流程)
        if getattr(self, "pool", None) is not None:
            self.pool.record(msg)
        # 优先级消息 (私聊老板/被@/控制台交互) 立即唤醒主循环心跳, 秒回。
        if msg.metadata.get("priority"):
            self._priority_event.set()
            # [抢占软熔断] 老板本人 priority 消息 -> 置抢占旗, 让进行中的 think
            # 提前结束 (软熔断, 见 Brain._run_tool_loop)。非老板 priority (被@)
            # 不抢占, 仅秒回排队。
            if msg.metadata.get("is_master"):
                self._master_preempt_flag = True
                logger.info(
                    f"[Kernel] 老板消息抢占旗置位: src={msg.source}, "
                    f"queue_size={self._message_queue.qsize()}"
                )
            logger.info(
                f"[Kernel] 优先级消息入队 (秒回): src={msg.source}, "
                f"queue_size={self._message_queue.qsize()}"
            )
        else:
            logger.info(
                f"[Kernel] 消息入队: type={msg.msg_type.value}, "
                f"src={msg.source}, queue_size={self._message_queue.qsize()}"
            )

    def pending_count(self) -> int:
        """[0.2.23.7 review M2] 队列排队消息数 (公开 API)。

        供 boot 播报让位判定等外部读取, 替代直取 _message_queue 私成员
        (属性改名会让消费方无声失效)。异常回退 0 (不致命场景)。
        """
        try:
            return self._message_queue.qsize()
        except Exception:
            return 0

    def drain_batch(self) -> list:
        """非阻塞取出信箱当前全部消息 (心跳批阅用)。取后信箱清空。

        返回 list[Message]; 信箱空时返回 []。Priority 标记不在此处理,
        由主循环通过 _priority_event 感知。
        """
        batch: list = []
        while True:
            try:
                msg = self._message_queue.get_nowait()
                batch.append(msg)
                self._mark_reviewable(msg)
            except Exception:
                break
        return batch

    def _mark_reviewable(self, msg: Message) -> None:
        if (self.raw_messages is not None and msg.msg_type.value == "user_input"
                and self.raw_messages.verify(msg)):
            self._raw_review_ready[id(msg)] = msg

    def peek_pending(self) -> list:
        """[0.3.21.0] 信箱排队消息非破坏性快照 (忙时接单回执看门狗用)。

        只读不消费。单线程事件循环内安全: 主循环仅在非 thinking 窗口 drain,
        看门狗仅在 thinking 窗口 peek, 二者互斥。读内部 deque 是 asyncio.Queue
        无公开 peek API 的既有妥协 (pending_count 同源)。
        """
        try:
            return list(self._message_queue._queue)  # type: ignore[attr-defined]
        except Exception:
            return []

    async def get_message(self) -> Message:
        """阻塞等待并返回下一条输入消息 (web_ui/旧路径用, 心跳批阅改用 drain_batch)。"""
        msg = await self._message_queue.get()
        self._mark_reviewable(msg)
        return msg

    async def emit_msg(self, msg: Message) -> None:
        """
        向所有订阅了对应 event_name 的回调分发消息。
        每个回调包装为 asyncio.Task, fire-and-forget 模式。

        [铁律] msg 必须是 Message 实例, 否则抛出 TypeError。
        """
        if not isinstance(msg, Message):
            raise TypeError(
                f"[Kernel] emit_msg() 只接受 Message 实例, "
                f"收到非法类型: {type(msg).__name__}。"
                f"请使用 Message.agent_speak() / Message.tool_result() 等工厂方法封装数据。"
            )

        ticket = None
        from kernel.raw_message_service import TurnReply, stable_id
        reply = msg.content if isinstance(msg.content, TurnReply) else None
        original = reply.identity.original if reply is not None else self._turn_original
        local_source = original.source if original is not None else None
        if self.raw_messages is not None and msg.event_name in {"on_agent_speak", "on_agent_stream_end"}:
            if original is not None and msg.content and (reply is not None or local_source in {"web", "console"}):
                turn_id = reply.identity.turn_id if reply is not None else msg.get("turn_id")
                reply_id = stable_id(original.get("origin_message_id"), turn_id) if turn_id else msg.id
                ticket = await self._prepare_raw_reply(original.source, {"text": msg.content}, reply_id,
                    turn_original=original, claim_outbound=local_source in {"web", "console"})
        event_name = msg.event_name or ""
        if (self._daily_derivation is not None and self.raw_messages is not None
                and event_name in {"on_agent_speak", "on_agent_stream_end"}
                and (original is not None or msg.get("_daily_owned"))):
            if original is None or original.get("_raw_agent_id") != self._raw_agent_id():
                from kernel.raw_message_models import RawMessageError
                raise RawMessageError("daily reply turn unavailable")
            msg = await self.raw_messages.capture_turn_event(original, msg, ticket)
        adapter = self._raw_local_delivery.get(
            (str(local_source or ""), event_name)
        ) if ticket and ticket.fresh else None
        delivered = False
        if adapter is not None:
            try:
                delivered = await asyncio.wait_for(adapter(msg), timeout=5) is True
            except asyncio.CancelledError:
                if ticket and ticket.fresh:
                    await self._record_raw_delivery(ticket, "unknown")
                raise
            except Exception:
                logger.warning("本地最终回复交付未确认。")
        for callback in self.subscribers.get(event_name, []):
            # _safe_emit 内部已捕获并记录全部异常, 任务不会向外泄漏
            asyncio.create_task(self._safe_emit(callback, msg))
        if ticket and ticket.fresh and not await self._record_raw_delivery(ticket, "confirmed" if delivered else "unknown"):
            from kernel.raw_message_models import RawMessageError
            raise RawMessageError("final reply delivery receipt unavailable")

    # ==========================================
    # 订阅注册 (强制新式接口)
    # ==========================================

    def subscribe(self, event_name: str, callback: Any) -> None:
        """
        注册事件订阅。

        [铁律] 回调必须为新式接口: 第一个参数名为 'msg' 或注解为 Message。
        旧式回调 (如 text/kwargs 签名) 会被拒绝, 强制调用方先完成迁移。
        """
        if not _is_new_style_callback(callback):
            cb_name = getattr(callback, "__qualname__", str(callback))
            raise TypeError(
                f"[Kernel] subscribe() 拒绝旧式回调 '{cb_name}'。"
                f"回调签名必须为 'async def handler(msg: Message)' 形式。"
                f"请迁移该回调以接收 Message 对象。"
            )

        if event_name not in self.subscribers:
            self.subscribers[event_name] = []

        # 防止同一回调重复注册
        if callback in self.subscribers[event_name]:
            return

        self.subscribers[event_name].append(callback)
        logger.debug(f"[Kernel] 神经节点接入: 已订阅事件 '{event_name}'")

    async def _safe_emit(self, callback: Any, msg: Message) -> None:
        """执行单个订阅回调, 捕获并记录全部异常, 绝不向外泄漏。"""
        cb_name = getattr(callback, "__qualname__", None) or str(callback)
        try:
            await callback(msg)
        except Exception as e:
            logger.error(
                f"[Kernel] 事件回调异常: {cb_name} 处理 "
                f"{msg.event_name} 时出错: {e}",
                exc_info=True,
            )

    # ==========================================
    # 插件生命周期管理
    # ==========================================

    def scan_apps(self) -> None:
        logger.info(f"[Kernel] 正在扫描应用仓库: {self.apps_dir}")
        if self.apps_dir not in sys.path:
            sys.path.append(self.apps_dir)

        for folder in os.listdir(self.apps_dir):
            # [0.2.23.0 性能刀4] 黑名单目录不导入 (治 chainlit 白导拖慢启动)
            if folder in self.SCAN_BLACKLIST:
                logger.info(f"[Kernel] 扫描黑名单跳过: {folder} (不注册进总线)")
                continue
            app_path = os.path.join(self.apps_dir, folder)
            if os.path.isdir(app_path) and "main.py" in os.listdir(app_path):
                try:
                    module = importlib.import_module(f"apps.{folder}.main")
                    for attr in dir(module):
                        cls = getattr(module, attr)
                        if isinstance(cls, type) and issubclass(cls, AoaoPlug) and cls is not AoaoPlug:
                            instance = cls()
                            instance.bus = self
                            name = instance.manifest.get("name", "unknown")
                            self.available_apps[name] = instance
                            desc = instance.manifest.get("description", "")[:15]
                            logger.info(f"[Kernel] 成功注册能力模块: {name} ({desc}...)")
                except Exception as e:
                    logger.error(f"[Kernel] 加载 {folder} 失败: {e}")

    def generate_dynamic_tools(self) -> List[Dict]:
        """生成 LLM 工具定义。实际逻辑已抽离到 ToolBuilder，此处为薄代理。"""
        return self._tool_builder.build_tools(self.available_apps)

    def _resolve_connect_timeout(self, name: str, override: Optional[float]) -> float:
        """三级回退: 调用方显式 > manifest.connect_timeout > 默认。"""
        if override is not None:
            return override
        manifest_timeout = self.available_apps[name].manifest.get("connect_timeout")
        if manifest_timeout:
            return float(manifest_timeout)
        return self.DEFAULT_CONNECT_TIMEOUT

    def _resolve_instruction_timeout(
        self, name: str, override: Optional[float]
    ) -> float:
        """三级回退: 调用方显式 > manifest.timeout > 默认。"""
        if override is not None:
            return override
        manifest_timeout = self.available_apps[name].manifest.get("timeout")
        if manifest_timeout:
            return float(manifest_timeout)
        return self.DEFAULT_INSTRUCTION_TIMEOUT

    # ==========================================
    # [0.3.28.0] 行动权威: 手动断开标记读写
    # ==========================================

    def is_manually_disconnected(self, name: str) -> bool:
        """该 app 是否被老板手动断开且尚未手动重连。"""
        return name in self._manually_disconnected

    def mark_manual_disconnect(self, name: str) -> None:
        """打上手动断开标记并持久化 (幂等)。"""
        if name not in self._manually_disconnected:
            self._manually_disconnected.add(name)
            _save_manual_disconnect_set(self.apps_dir, self._manually_disconnected)
            logger.info(f"[Kernel] {name} 已标记为老板手动断开, 内核不再自行重连")

    def clear_manual_disconnect(self, name: str) -> None:
        """清除手动断开标记并持久化 (幂等)。"""
        if name in self._manually_disconnected:
            self._manually_disconnected.discard(name)
            _save_manual_disconnect_set(self.apps_dir, self._manually_disconnected)
            logger.info(f"[Kernel] {name} 手动断开标记已清除 (老板手动连接)")

    async def connect_app(
        self, name: str, *, timeout: Optional[float] = None, by_user: bool = False
    ) -> str:
        """
        启动 app。timeout 三级回退 (调用方 > manifest.connect_timeout > 默认 30s)。
        返回字符串 (保持旧签名兼容, Brain 据文本判定)。

        [0.3.28.0] by_user=True (老板在控制台手动连接): 清除手动断开标记后放行;
        内核/LLM 自调连接命中老板手动断开标记时直接拒绝 (不得重连老板断开的 app)。
        """
        if self._coordinated_lifecycle.busy(name):
            return f"系统警告: {name} 正在连接或关闭，请等待当前操作完成。"
        if by_user:
            self.clear_manual_disconnect(name)
        elif name in self._manually_disconnected:
            logger.warning(f"[Kernel] 拒绝自动连接 {name}: 老板已手动断开")
            return (
                f"系统拦截: {name} 已被老板手动断开, 内核禁止自行重连。"
                f"请转告老板: 如需使用, 请在 Web 控制台手动连接。"
            )
        if name in self.active_apps:
            return f"App {name} 已经在运行了。"
        if name not in self.available_apps:
            return f"找不到 App {name}，无法启动。"
        logger.info(f"[Kernel] 正在启动 {name}...")
        app = self.available_apps[name]
        resolved_timeout = self._resolve_connect_timeout(name, timeout)
        try:
            if app.manifest.get("coordinated_lifecycle") is True:
                connected = await self._coordinated_lifecycle.connect(name, app, timeout=resolved_timeout)
                return (f"系统通报: {name} 已启动。" if connected
                        else f"系统警告: {name} 启动未完成，未注册为运行中。")
            connect_result = await asyncio.wait_for(app.on_connect(), timeout=resolved_timeout)
            if connect_result is False:
                return f"系统警告: {name} 启动失败 (on_connect 返回 False)。"
            self.active_apps[name] = app
            app.is_connected = True
            return f"系统通报: {name} 已启动。"
        except asyncio.TimeoutError:
            logger.error(f"[Kernel] {name} 启动超时 ({resolved_timeout}s)。")
            return f"系统警告: {name} 启动超时 ({resolved_timeout}s)。"
        except Exception as e:
            traceback.print_exc()
            return f"系统警告: {name} 启动失败: {e}"

    async def disconnect_app(self, name: str, by_user: bool = False) -> str:
        """断开 app。[0.3.28.0] by_user=True (老板手动断开) 打持久标记,
        此后内核 connect_app/开机自启均不得重连, 直到老板手动连接清标。"""
        if (name in self.active_apps
                and self.active_apps[name] is self.daily_legacy_inflight_provider()):
            return "系统警告: 旧记忆读取仍在结算；本次关闭未完成。"
        if self._coordinated_lifecycle.owns(name):
            if by_user:
                self.mark_manual_disconnect(name)
            try:
                if not await self._coordinated_lifecycle.disconnect(name):
                    return f"系统警告: {name} 活动实例已变化；本次关闭未完成，请核对。"
                return f"系统通报: {name} 已关闭，本机插件入口已撤下。"
            except Exception:
                return f"系统警告: {name} 关闭未完成；仍禁止重新连接。"
        if name not in self.active_apps:
            return f"App {name} 没开。"
        logger.info(f"[Kernel] 正在关闭 {name}...")
        app = self.active_apps[name]
        try:
            await app.on_disconnect()
            del self.active_apps[name]
            app.is_connected = False
            if by_user:
                self.mark_manual_disconnect(name)
            return f"系统通报: {name} 已关闭，资源释放。"
        except Exception as e:
            return f"系统警告: {name} 关闭出错: {e}"

    async def send_instruction(
        self,
        name: str,
        command: str,
        trigger_msg: Optional[Message] = None,
        *,
        timeout: Optional[float] = None,
        idempotency_key: Optional[str] = None,
        expected_agent_id: Optional[str] = None,
        **kwargs: Any,
    ) -> ToolResult:
        """
        向已启动 app 发送指令, 返回结构化 ToolResult。

        超时三级回退: 调用方 timeout 参数 > manifest.timeout > DEFAULT_INSTRUCTION_TIMEOUT。
        严禁一刀切: image_generator/doc_parser 等慢 app 应在 manifest 声明 timeout。

        [0.3.27.0 阶段3 规格五.4] idempotency_key: 插件声明 idempotent=true 时,
        内核幂等层经此参数透传幂等键 (run_id+tool_call_id+canonical_args), app
        可自行去重; 未声明的 app 自动吸收忽略 (execute_command **kwargs 兜底)。

        Returns:
            ToolResult: __str__ 仍输出旧前缀字符串, 旧调用点 str(result) 零改动。
        """
        from kernel.raw_message_models import RawMessageError

        class PreTransportRejected(Exception):
            pass

        if trigger_msg is not None and not isinstance(trigger_msg, Message):
            raise TypeError(
                f"[Kernel] send_instruction() 的 trigger_msg 必须是 Message 或 None, "
                f"收到非法类型: {type(trigger_msg).__name__}"
            )

        if name not in self.active_apps:
            error_msg = f"系统警告: App {name} 尚未启动，请先连接它。"
            await self.emit_msg(Message.system_error(content=error_msg, source="Kernel"))
            return ToolResult.error(name, error_msg)

        app = self.active_apps[name]
        resolved_timeout = self._resolve_instruction_timeout(name, timeout)
        ticket = None
        initiating_turn = self._turn_original if expected_agent_id is None else None
        outbound_agent_id = expected_agent_id
        if command == "send_text" and self.raw_messages is not None:
            try:
                from kernel.raw_message_service import TurnReply
                text = kwargs.get("text")
                if isinstance(text, TurnReply):
                    initiating_turn = text.identity.original
                current_agent_id = self._raw_agent_id() if self._raw_agent_id else ""
                if initiating_turn is not None:
                    source_agent_id = initiating_turn.get("_raw_agent_id")
                    if (not isinstance(source_agent_id, str) or not source_agent_id
                            or source_agent_id != current_agent_id
                            or (expected_agent_id is not None and expected_agent_id != source_agent_id)):
                        raise RawMessageError("outbound agent changed")
                outbound_agent_id = (expected_agent_id or
                                     (initiating_turn.get("_raw_agent_id") if initiating_turn else
                                      current_agent_id))
                source_event_id = (initiating_turn.get("origin_message_id")
                                   if initiating_turn is not None else None)
                target = kwargs.get("target_user") or kwargs.get("target")
                if not target and initiating_turn is not None and initiating_turn.source == name:
                    target = initiating_turn.get("chat_name") or name
                if initiating_turn is not None:
                    await self.raw_messages.validate_turn(initiating_turn)
                same_chat = (initiating_turn is not None and initiating_turn.source == name
                             and (not target or target == (initiating_turn.get("chat_name") or name)))
                ticket = await self._prepare_raw_reply(
                    name, kwargs, idempotency_key, outbound_agent_id,
                    turn_original=initiating_turn if same_chat else None,
                    source_event_id=source_event_id, target_hint=target or "",
                )
                if ticket is not None and not ticket.fresh:
                    return ToolResult.error(name, "发送尝试已有记录；请核对送达状态，禁止自动重发。")
            except RawMessageError:
                return ToolResult.error(name, "回复未保存；未执行发送。")
        try:
            if ticket is not None and initiating_turn is not None and self.raw_messages is not None:
                try:
                    await self.raw_messages.validate_turn(initiating_turn)
                except RawMessageError as exc:
                    raise PreTransportRejected from exc
            # 统一传递 msg 参数 (旧 App 的 **kwargs 会吸收 msg=None)
            exec_kwargs = self._send_kwargs(name, kwargs, ticket, idempotency_key)
            async def execute_authorized():
                if ticket is not None and (self._raw_agent_id is None
                                           or self._raw_agent_id() != outbound_agent_id
                                           or self.active_apps.get(name) is not app):
                    raise PreTransportRejected
                return await app.execute_command(command, msg=trigger_msg, **exec_kwargs)
            raw = await asyncio.wait_for(
                execute_authorized(),
                timeout=resolved_timeout,
            )
            result, status = self._delivery_result(name, ticket, raw)
            if not await self._record_raw_delivery(ticket, status):
                return ToolResult.error(name, "发送后的凭据未保存；结果未知，请勿重试。")
            return result
        except asyncio.TimeoutError:
            await self._record_raw_delivery(ticket, "unknown")
            logger.error(
                f"[Kernel] {name}.{command} 执行超时 ({resolved_timeout}s)。"
            )
            return ToolResult.error(
                name, f"指令 {command} 执行超时 ({resolved_timeout}s)"
            )
        except asyncio.CancelledError:
            await self._record_raw_delivery(ticket, "unknown")
            raise
        except PreTransportRejected:
            await self._record_raw_delivery(ticket, "failed")
            return ToolResult.error(name, "发送前身份已变更；未执行发送。")
        except RawMessageError:
            await self._record_raw_delivery(ticket, "unknown")
            return ToolResult.error(name, "发送结果未知；请核对渠道状态，禁止自动重发。")
        except Exception:
            await self._record_raw_delivery(ticket, "unknown")
            logger.error("[Kernel] App 指令执行异常，详情已隐藏。")
            await self.emit_msg(
                Message.system_error(content="App 指令执行异常，详情已隐藏。", source=name)
            )
            if ticket is not None:
                return ToolResult.error(name, "发送结果未知；请核对渠道状态，禁止自动重发。")
            return ToolResult.error(name, "App 指令执行异常，详情已隐藏。")

    def get_active_list(self) -> List[str]:
        return list(self.active_apps.keys())
