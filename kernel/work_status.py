# kernel/work_status.py
"""
[0.3.22.1 P3] 工作状态帧发射层 -> [0.3.27.0 阶段5] TaskRun 真实投影。

老板 2026-08-03 原话需求: "状态栏里面可以加一个工作状态栏, 显示目前正在干啥"。

[0.3.27.0 阶段5 规格七] 深化:
1. 不再从 brain._thinking 推断: snapshot/publish 读 TaskRun 真实状态
   (idle/task_thinking/tool_running/waiting_user/completed/failed/cancelled)。
2. 首轮普通聊天不发布 WorkStatus; 收到首个工具调用后才发布任务开始。
3. 工具参数绝不原样进状态栏: 用 manifest 的 progress_label 生成安全描述。
4. 事件字段: run_id/turn_id/revision/phase/activity/active_tool/queue/
   started_at/elapsed_ms; 前端按 run_id+revision 丢弃陈旧状态。
5. WebSocket 与 REST 快照读同一份投影 (bus 侧当前 run 快照)。
6. limit_reached 只表示预算停止的 UI 警告，TaskRun 原终态不变，不证明目标完成。

契约:
- publish(bus, state, activity, queue, run=None) 经 bus.emit_msg 广播;
  web/bridge.py 订阅 on_work_status 转 WS 帧。
- 去重: state+activity+queue+revision 连续同帧跳过。
- 状态帧永不穿透主流程: publish 内部兜底 try/except。

循环导入约束: 顶层只 import kernel.logger/message; TaskQueue 走函数内延迟导入。
"""
import json

from kernel.logger import get_logger
from kernel.message import Message
from kernel.tool_builder import tool_name_for_app
from kernel.execution_evidence import needs_receipt_review

logger = get_logger(__name__)

# [0.3.27.0 阶段5] 状态语义扩展 (旧 thinking/tool 兼容映射: thinking->task_thinking,
# tool->tool_running; 旧前端按字符串透传, 新字段向后兼容)
WORK_STATE_IDLE = "idle"
WORK_STATE_TASK_THINKING = "task_thinking"
WORK_STATE_TOOL_RUNNING = "tool_running"
WORK_STATE_WAITING_USER = "waiting_user"
WORK_STATE_COMPLETED = "completed"
WORK_STATE_FAILED = "failed"
WORK_STATE_LIMIT_REACHED = "limit_reached"
WORK_STATE_CANCELLED = "cancelled"
# 旧状态常量 (存量调用方/测试兼容)
WORK_STATE_THINKING = WORK_STATE_TASK_THINKING
WORK_STATE_TOOL = WORK_STATE_TOOL_RUNNING

# [0.3.27.0 阶段5] 危险工具参数前缀黑名单: activity 出现这些前缀时剥离参数段
_UNSAFE_ACTIVITY_MARKERS = ("工具: ", "调用: ", "执行: ")


def _safe_activity(activity: str) -> str:
    """工具参数绝不原样进状态栏: 剥离"工具: <参数拼装>"式裸描述。

    阶段3 manifest progress_label 渲染进工具描述后, 发射点应直接传
    progress_label; 兜底剥离旧式拼接。
    """
    if not activity:
        return activity
    for marker in _UNSAFE_ACTIVITY_MARKERS:
        if activity.startswith(marker):
            return activity[len(marker):][:30]
    return activity


def _manifest_progress_label(bus, function_name: str, arguments: str) -> str:
    try:
        parsed = json.loads(arguments)
    except (json.JSONDecodeError, TypeError):
        return ""
    if not isinstance(parsed, dict):
        return ""
    command = parsed.get("command")
    legacy_app = (
        str(parsed.get("app_name") or "")
        if function_name == "instruct_app" else ""
    )
    for app_name, app in getattr(bus, "available_apps", {}).items():
        app_name = str(app_name)
        if legacy_app and legacy_app != app_name:
            continue
        if not legacy_app and function_name != tool_name_for_app(app_name):
            continue
        try:
            commands = app.manifest.get("commands", [])
        except Exception:
            return ""
        for item in commands if isinstance(commands, list) else []:
            if isinstance(item, dict) and item.get("name") == command:
                return str(item.get("progress_label") or "")[:30]
    return ""


def activity_for_tool_calls(bus, tool_calls) -> str:
    """从 manifest 生成安全进度文案，绝不把模型参数投影到状态栏。"""
    labels = []
    for call in tool_calls or ():
        function = getattr(call, "function", None)
        name = str(getattr(function, "name", "") or "")
        arguments = getattr(function, "arguments", "")
        label = _manifest_progress_label(bus, name, arguments)
        fallback = name.removeprefix("use_")[:30] or "执行任务"
        value = label or fallback
        if value not in labels:
            labels.append(value)
    return "、".join(labels)[:30] or "执行任务"


def queue_labels(bus) -> tuple:
    """工作列表排队条目摘要 (desc 截断 20 字防帧过大), 无真账本返 ()。"""
    ledger = getattr(bus, "task_ledger", None)
    if ledger is None:
        return ()
    from kernel.task_queue import TaskQueue  # 延迟导入防循环
    if not isinstance(ledger, TaskQueue):
        return ()
    try:
        return tuple((item.desc or "")[:20] for item in ledger.pending_items())
    except Exception:
        return ()


def _run_projection(run) -> dict:
    """TaskRun -> 投影字段 (run_id/turn_id/revision/phase/started_at)。"""
    import time as _t
    started = float(getattr(run, "started_at", 0.0) or 0.0)
    projection = {
        "run_id": getattr(run, "run_id", ""),
        "turn_id": getattr(run, "turn_id", ""),
        "revision": int(getattr(run, "work_status_revision", 0)),
        "phase": getattr(run, "status", ""),
        "started_at": started,
        "elapsed_ms": int((_t.time() - started) * 1000) if started else 0,
    }
    from kernel.model_profile import is_deepseek
    mode = getattr(run, "thinking_mode", None)
    if is_deepseek(getattr(run, "model_name", "")) and mode in ("enabled", "disabled"):
        projection["thinking_mode"] = mode
    return projection


def _state_from_run(run) -> str:
    """TaskRun 状态 -> 状态栏 state (规格七 状态集)。"""
    status = getattr(run, "status", "")
    if (status == "failed" and getattr(run, "stop_reason", "") == "max_steps"
            and not needs_receipt_review(run)):
        return WORK_STATE_LIMIT_REACHED
    mapping = {
        "probing": WORK_STATE_IDLE,        # 首轮快速认知 UI 不可见
        "running": WORK_STATE_TASK_THINKING,
        "waiting_user": WORK_STATE_WAITING_USER,
        "resuming": WORK_STATE_TASK_THINKING,
        "completed": WORK_STATE_COMPLETED,
        "failed": WORK_STATE_FAILED,
        "cancelled": WORK_STATE_CANCELLED,
    }
    return mapping.get(status, WORK_STATE_IDLE)


async def publish(bus, state: str, activity: str = "", queue=None,
                  run=None) -> None:
    """广播一帧工作状态; 连续同帧去重跳过; 任何异常只落 warning。

    [0.3.27.0 阶段5] run 可选: TaskRun 投影 (run_id/revision/phase/elapsed)
    附加进 metadata; 普通聊天 (无 run 且无任务) 不发帧由调用方保证。
    """
    if queue is None:
        queue = queue_labels(bus)
    activity = _safe_activity(activity)
    proj = _run_projection(run) if run is not None else {}
    key = (state, activity, tuple(queue), proj.get("run_id", ""),
           proj.get("revision", 0))
    try:
        if getattr(bus, "_work_status_last_key", None) == key:
            return
        await bus.emit_msg(Message.work_status(
            state, activity=activity, queue=list(queue), **proj))
        bus._work_status_last_key = key
    except Exception as e:
        logger.warning(f"[work_status] 状态帧发射失败 (非致命): {e}")


def snapshot(bus, brain) -> dict:
    """REST /api/status work 字段快照 (TaskRun 投影, 不再读 _thinking 推断)。

    brain._current_run 是 TaskRun 真实状态源; 无 run (普通聊天/空闲) -> idle。
    """
    run = getattr(brain, "_current_run", None) if brain is not None else None
    if run is None:
        return {"state": WORK_STATE_IDLE, "activity": "待机", "queue": []}
    proj = _run_projection(run)
    state = _state_from_run(run)
    return {
        "state": state,
        "activity": (
            "工具轮次已达上限，请核对已有结果" if state == WORK_STATE_LIMIT_REACHED
            else getattr(run, "status", "") or "待机"
        ),
        "queue": list(queue_labels(bus)),
        **proj,
    }
