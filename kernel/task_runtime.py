# kernel/task_runtime.py
"""
[0.3.27.0 工具架构改造 阶段1] TaskRuntime Module -- 任务运行时与 TaskRun。

规格: "任务状态保存在 Brain 单例字段中, 挂起恢复后可能串会话、串权限、
串出站目标。" TaskRun 承载本轮不可变身份与可恢复状态:

- run_id: 本轮任务唯一标识 (与 turn_id 区分: turn 是单次 think, run 跨挂起/恢复)。
- source / chat_name / session_key: 会话归属 (恢复关联键要素)。
- principal: 不可变 Principal 快照 -- 恢复执行时权限复核用快照, 不继承当前消息。
- status: PROBING -> RUNNING -> WAITING_USER -> RESUMING -> COMPLETED/FAILED/CANCELLED。
- messages: 完整模型消息 (挂起时保存, 恢复时续用)。
- llm_rounds / tool_batch_rounds / total_tool_calls: 三类独立上限计数。
- executed_ledger: 工具幂等账本 (tool_call_id -> CLAIMED/COMMITTED/FAILED/AMBIGUOUS)。
- outbound_texts / outbound_targets / failure_count: 出站与熔断账本 (阶段1 与
  brain 引用共享, 行为完全保持; 阶段3+ 迁移为 run 独占)。
- pending_question / deadline: ask_user 挂起信息 (阶段4 启用)。
- preempted / cancel_requested: 抢占与取消信号。
- work_status_revision: WorkStatus 递增版本号 (前端按 run_id+revision 丢弃陈旧帧)。

阶段1 契约: Brain._think_impl 入口创建 TaskRun (与 brain 双写引用共享),
旧工具循环仍运行, 行为完全保持, 全量测试绿。后续阶段逐步把读取点迁到 run。
"""
import enum
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kernel.principal import Principal

# ==========================================
# 任务状态 (字符串常量, 与项目 WORK_STATE_* 风格一致)
# ==========================================
PROBING = "probing"                # 首轮快速认知, UI 不可见
RUNNING = "running"                # 已产生工具调用, 正式任务模式
WAITING_USER = "waiting_user"      # ask_user 等待用户回答
RESUMING = "resuming"              # 用户回答/超时后恢复执行
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"

# 幂等账本状态 (规格 五.4)
CLAIMED = "CLAIMED"                # 已认领, 执行中
COMMITTED = "COMMITTED"            # 已提交, 恢复只回放结果
FAILED_STATE = "FAILED"            # 本地失败, 可重试
AMBIGUOUS = "AMBIGUOUS"            # 外部副作用已发生但本地未提交, 禁自动重试


# ==========================================
# 结构化运行结果 (规格: 不能再用空字符串/中文文案/_thinking 猜状态)
# ==========================================
class RunResultKind(str, enum.Enum):
    COMPLETED = "completed"
    SUSPENDED = "suspended"        # ask_user 挂起, 释放 Brain
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RunResult:
    """一轮任务运行的最终结果。reply 仅 COMPLETED 时携带最终文本。"""
    kind: RunResultKind
    reply: str = ""
    run_id: str = ""
    reason: str = ""

    @staticmethod
    def completed(reply: str, run_id: str = "") -> "RunResult":
        return RunResult(RunResultKind.COMPLETED, reply=reply, run_id=run_id)

    @staticmethod
    def suspended(run_id: str = "", reason: str = "") -> "RunResult":
        return RunResult(RunResultKind.SUSPENDED, run_id=run_id, reason=reason)

    @staticmethod
    def failed(reason: str = "", run_id: str = "") -> "RunResult":
        return RunResult(RunResultKind.FAILED, reason=reason, run_id=run_id)

    @staticmethod
    def cancelled(reason: str = "", run_id: str = "") -> "RunResult":
        return RunResult(RunResultKind.CANCELLED, reason=reason, run_id=run_id)


# ==========================================
# TaskRun
# ==========================================
@dataclass
class TaskRun:
    """本轮任务运行态。阶段1 与 Brain 字段双写共享, 后续阶段迁移为 run 独占。

    非 frozen: 运行中状态持续更新。principal 引用不可变 (frozen dataclass)。
    """

    run_id: str
    turn_id: str
    session_key: str
    source: str
    chat_name: str
    principal: "Principal"
    is_dm: bool = False             # [0.3.27.0 阶段4] 是否老板 1v1 (ask_user 闸门)
    conversation_id: str = ""
    raw_agent_id: str = ""
    origin_message_id: str = ""
    raw_turn: dict = field(default_factory=dict)

    # ---- 状态机 ----
    status: str = PROBING
    started_at: float = 0.0
    deadline: float = 0.0           # ask_user 超时截止 (epoch), 阶段4 启用

    # ---- 上下文与轮次 ----
    messages: List[dict] = field(default_factory=list)
    thinking_mode: str = "enabled"
    model_name: str = ""
    model_source: str = ""
    protocol_version: int = 0
    protocol_turn_start: int = 0
    llm_rounds: int = 0             # 实际模型请求次数 (修 continue 绕过)
    research_policy: str = "undetermined"
    research_admission: dict = field(default_factory=dict)
    research_usage: dict = field(default_factory=dict)
    research_id: str = ""
    research_status: str = ""
    tool_batch_rounds: int = 0      # 工具批轮数
    total_tool_calls: int = 0       # 工具调用总数

    # ---- 幂等账本: tool_call_id -> {state, result, revision} ----
    executed_ledger: Dict[str, dict] = field(default_factory=dict)
    tool_revision: int = 0          # 工具定义版本 (幂等键要素)
    tool_receipts: List[dict] = field(default_factory=list)  # 真实回执，不等同幂等提交成功

    # ---- 出站与熔断账本 (阶段1 与 brain 引用共享) ----
    outbound_texts: List[str] = field(default_factory=list)
    outbound_targets: List[tuple] = field(default_factory=list)
    failure_count: Dict[str, int] = field(default_factory=dict)
    resume_state_version: int = 0
    user_input: str = ""
    skill_save_requested: bool = False
    last_tool_called: Optional[str] = None
    outbound_text_count: int = 0
    outbound_media_count: int = 0
    structured_targets: List[str] = field(default_factory=list)

    # ---- ask_user 挂起信息 (阶段4 启用) ----
    pending_question: Optional[dict] = None
    # {ask_id, text, options, safe_default, safe_is_default, deadline}

    # ---- 抢占与取消信号 ----
    preempted: bool = False
    cancel_requested: bool = False

    # ---- WorkStatus ----
    work_status_revision: int = 0
    stop_reason: str = ""          # 终止原因用于 UI；不把预算耗尽提升为目标完成

    # ==========================================
    # 状态辅助
    # ==========================================
    def claim_tool(self, tool_call_id: str, canonical_arguments: str) -> str:
        """幂等认领: 已 COMMITTED 返回 replay; 否则置 CLAIMED 返回 execute。"""
        entry = self.executed_ledger.get(tool_call_id)
        if entry is not None:
            if entry.get("state") == COMMITTED:
                return "replay"
            if entry.get("state") == AMBIGUOUS:
                return "ambiguous"
            return "execute"
        self.executed_ledger[tool_call_id] = {
            "state": CLAIMED,
            "arguments": canonical_arguments,
            "revision": self.tool_revision,
            "result": "",
        }
        return "execute"

    def commit_tool(self, tool_call_id: str, result: str) -> None:
        entry = self.executed_ledger.get(tool_call_id)
        if entry is None:
            entry = self.executed_ledger[tool_call_id] = {
                "state": CLAIMED, "arguments": "", "revision": self.tool_revision,
            }
        entry["state"] = COMMITTED
        entry["result"] = result

    def fail_tool(self, tool_call_id: str) -> None:
        entry = self.executed_ledger.get(tool_call_id)
        if entry is None:
            return
        entry["state"] = FAILED_STATE

    def mark_ambiguous(self, tool_call_id: str) -> None:
        entry = self.executed_ledger.get(tool_call_id)
        if entry is None:
            self.executed_ledger[tool_call_id] = {
                "state": AMBIGUOUS, "arguments": "", "revision": self.tool_revision,
            }
            return
        entry["state"] = AMBIGUOUS

    def bump_status_frame(self) -> int:
        """WorkStatus 递增版本号: 每次状态变化调用, 前端据此丢弃陈旧帧。"""
        self.work_status_revision += 1
        return self.work_status_revision


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def create_run(turn_id: str, source: str, chat_name: str,
               principal: "Principal", is_dm: bool = False, *,
               conversation_id: str = "", raw_agent_id: str = "",
               origin_message_id: str = "") -> TaskRun:
    """工厂: 从 Brain 本轮上下文创建 TaskRun (阶段1 brain._think_impl 入口调用)。"""
    import time as _t
    from kernel.brain import _session_key  # 延迟导入防循环 (brain 顶层 import 本模块)
    return TaskRun(
        run_id=new_run_id(),
        turn_id=turn_id,
        session_key=_session_key(chat_name, source, conversation_id),
        source=source,
        chat_name=chat_name,
        principal=principal,
        is_dm=is_dm,
        conversation_id=conversation_id, raw_agent_id=raw_agent_id,
        origin_message_id=origin_message_id,
        started_at=_t.time(),
    )
