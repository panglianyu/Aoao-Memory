# kernel/task_store.py
"""
[0.3.27.0 工具架构改造 阶段4] TaskStore -- 挂起任务持久化层。

规格 六 "ask_user 挂起恢复" + 四.4:
- 挂起时保存 TaskRun (完整 messages + 工具账本 + 待答问题 + deadline),
  释放 Brain; 恢复时按 ask_id+run_id+source+chat_name+Principal 关联,
  追加回答工具结果后重新执行, 不重放已 COMMITTED 调用。
- 两个真实 Adapter: 内存 (测试) + JSON (生产/重启恢复)。
- 持久化目录限定 data/task_runtime/, 原子替换写入 (先写 tmp 再 os.replace)。
- 超时安全: 60s 后只读/可逆任务用声明的安全默认值, 不可逆任务取消。

循环导入约束: 顶层只 import json/os/dataclasses/logger。TaskRun 走 duck typing
(序列化字段白名单, 不 import task_runtime 防成环)。调用方: kernel/brain.py
(挂起/恢复/超时), kernel/tool_loop.py (挂起检测); 测试见 tests/test_task_store.py。
"""
import json
import asyncio
import os
import time
from typing import Any, Dict, Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

DEFAULT_DATA_DIR = os.path.join("data", "task_runtime")
ASK_TIMEOUT_SECONDS = 60.0

# 可序列化字段白名单 (TaskRun 与 dict 互转, 不含函数/锁等不可序列化物)
_SERIAL_FIELDS = (
    "run_id", "turn_id", "session_key", "source", "chat_name",
    "conversation_id", "raw_agent_id", "origin_message_id", "raw_turn",
    "is_dm", "status", "started_at", "deadline", "llm_rounds",
    "tool_batch_rounds", "total_tool_calls", "tool_revision",
    "work_status_revision", "preempted", "cancel_requested", "stop_reason",
    "thinking_mode", "model_name", "model_source", "protocol_version", "protocol_turn_start",
    "resume_state_version", "outbound_text_count", "outbound_media_count",
    "user_input", "skill_save_requested", "last_tool_called",
    "research_policy", "research_admission", "research_usage", "research_id", "research_status",
)
_LEDGER_FIELDS = ("executed_ledger", "outbound_texts", "outbound_targets",
                  "failure_count", "pending_question", "structured_targets")


async def save_run_snapshot(store, run) -> str:
    """等待拥有的写盘线程收尾后才传播取消，防恢复后迟到写回复活旧任务。"""
    worker = asyncio.create_task(asyncio.to_thread(store.save, run))
    cancelled = False
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    try:
        result = worker.result()
    except Exception as exc:
        if cancelled:
            raise asyncio.CancelledError() from exc
        raise
    if cancelled:
        raise asyncio.CancelledError()
    return result


def _principal_to_dict(p) -> dict:
    try:
        return {
            "actor_id": getattr(p, "actor_id", ""),
            "role": getattr(p, "role", "guest"),
            "channel": getattr(p, "channel", ""),
            "authenticated_by": getattr(p, "authenticated_by", ""),
        }
    except Exception:
        return {"role": "guest"}


def _dict_to_principal(d: dict):
    from kernel.principal import Principal  # 延迟导入防循环
    role = str(d.get("role", "guest") or "guest")
    actor = str(d.get("actor_id", "") or "unknown")
    channel = str(d.get("channel", "") or "system")
    auth = str(d.get("authenticated_by", "") or "store:restore")
    return Principal.owner(actor, channel, auth) if role == "owner" \
        else Principal.guest(actor, channel, auth)


def run_to_dict(run) -> dict:
    """TaskRun -> 可序列化 dict (字段白名单 + principal 快照)。"""
    from kernel.execution_evidence import normalize_receipts
    from kernel.web_research.history import (
        project_executed_ledger, project_research_history, research_identity,
    )
    out: Dict[str, Any] = {}
    for f in _SERIAL_FIELDS:
        out[f] = getattr(run, f, None)
    from kernel.web_research.policy import serialize_research_admission
    if out.get("research_admission"):
        out["research_admission"] = serialize_research_admission(out["research_admission"])
    out["principal"] = _principal_to_dict(getattr(run, "principal", None))
    messages = [
        m if isinstance(m, dict) else {"role": "system", "content": str(m)}
        for m in (getattr(run, "messages", None) or [])
    ]
    research_id, research_status = research_identity(run)
    out["research_id"] = research_id
    out["research_status"] = research_status
    out["messages"] = project_research_history(
        messages, research_id=research_id, status=research_status,
    )
    for f in _LEDGER_FIELDS:
        value = getattr(run, f, None)
        out[f] = project_executed_ledger(
            value, research_id=research_id, status=research_status,
        ) if f == "executed_ledger" else value
    out["tool_receipts"] = normalize_receipts(getattr(run, "tool_receipts", []))
    return out


def dict_to_run(d: dict):
    """dict -> TaskRun (延迟导入 task_runtime 防循环)。"""
    from kernel.task_runtime import TaskRun
    from kernel.execution_evidence import normalize_receipts
    run = TaskRun(
        run_id=str(d.get("run_id", "")),
        turn_id=str(d.get("turn_id", "")),
        session_key=str(d.get("session_key", "")),
        source=str(d.get("source", "")),
        chat_name=str(d.get("chat_name", "")),
        principal=_dict_to_principal(d.get("principal") or {}),
    )
    for f in _SERIAL_FIELDS:
        if f in ("run_id", "turn_id", "session_key", "source", "chat_name"):
            continue
        if f in d:
            setattr(run, f, d[f])
    if run.thinking_mode not in ("enabled", "disabled"):
        raise ValueError("Invalid saved thinking mode")
    from kernel.web_research.history import (
        project_executed_ledger, project_research_history, research_identity,
    )
    run.research_id, run.research_status = research_identity(run)
    run.messages = project_research_history(
        list(d.get("messages") or []),
        research_id=run.research_id,
        status=run.research_status,
    )
    for f in _LEDGER_FIELDS:
        value = d.get(f)
        if f == "executed_ledger":
            value = project_executed_ledger(
                value or {}, research_id=run.research_id, status=run.research_status,
            )
        setattr(run, f, value)
    targets = run.outbound_targets or []
    if not isinstance(targets, list) or any(
        not isinstance(row, (list, tuple)) or len(row) != 2
        or not all(isinstance(value, str) for value in row)
        for row in targets
    ):
        raise ValueError("Invalid saved outbound targets")
    run.outbound_targets = [tuple(row) for row in targets]
    run.structured_targets = run.structured_targets or []
    if (type(run.resume_state_version) is not int or run.resume_state_version not in (0, 1)
            or any(type(value) is not int or value < 0 for value in
                   (run.outbound_text_count, run.outbound_media_count))
            or not isinstance(run.structured_targets, list)
            or not all(isinstance(value, str) for value in run.structured_targets)):
        raise ValueError("Invalid saved outbound state")
    if (not isinstance(run.user_input, str)
            or type(run.skill_save_requested) is not bool
            or not (run.last_tool_called is None or isinstance(run.last_tool_called, str))
            or (run.resume_state_version == 1 and not all(name in d for name in (
                "user_input", "skill_save_requested", "last_tool_called",
                "outbound_text_count", "outbound_media_count", "structured_targets")))):
        raise ValueError("Invalid saved turn context")
    run.tool_receipts = normalize_receipts(d.get("tool_receipts", []))
    from kernel.web_research.policy import research_admission_scope, restored_research_admission
    from kernel.web_research.budget import validate_saved_usage
    run.research_admission = restored_research_admission(
        d.get("research_admission"), scope=research_admission_scope(run),
        input_text=run.user_input, legacy_policy=run.research_policy,
    )
    run.research_policy = "deny"
    run.research_usage = validate_saved_usage(run.research_usage)
    from kernel.model_usage import validate_model_call_count
    validate_model_call_count(
        run.llm_rounds,
        research_calls=run.research_usage.get("model_calls", 0),
        terminal_summary=run.status == "failed",
    )
    return run


class TaskStore:
    """挂起任务存取接口 (Adapter 模式)。"""

    def save(self, run) -> str:
        """持久化一个挂起任务, 返回 run_id。"""
        raise NotImplementedError

    def load(self, run_id: str) -> Optional[object]:
        """按 run_id 取回 TaskRun; 不存在返回 None。"""
        raise NotImplementedError

    def delete(self, run_id: str) -> bool:
        """删除一个挂起任务 (恢复完成/超时取消后调用)。"""
        raise NotImplementedError

    def list_pending(self) -> list:
        """返回全部挂起 run_id 列表。"""
        raise NotImplementedError

    def sweep_timeouts(self, brain=None) -> list:
        """超时处理: 返回 [(run_id, action)]; action in ("resume_default", "cancel")。

        只读/可逆任务 (pending_question.safe_is_default=true) -> resume_default
        (用安全默认值恢复); 发消息/删文件/改配置/不可逆 -> cancel。
        brain 可选: 恢复执行器 (brain.resume_ask); 缺省仅返回判定结果。
        """
        return []


class MemoryTaskStore(TaskStore):
    """内存 Adapter (测试/单进程)。"""

    def __init__(self):
        self._runs: Dict[str, Any] = {}

    def save(self, run) -> str:
        self._runs[run.run_id] = run
        return run.run_id

    def load(self, run_id: str):
        return self._runs.get(run_id)

    def delete(self, run_id: str) -> bool:
        return self._runs.pop(run_id, None) is not None

    def list_pending(self) -> list:
        return list(self._runs.keys())

    def sweep_timeouts(self, brain=None) -> list:
        now = time.time()
        out = []
        for rid in list(self._runs.keys()):
            run = self._runs[rid]
            if run.status != "waiting_user":
                continue
            if run.deadline and now < run.deadline:
                continue
            q = run.pending_question or {}
            if q.get("safe_is_default"):
                out.append((rid, "resume_default"))
                if brain is not None:
                    try:
                        default = str(q.get("safe_default") or "好，继续。")
                        import asyncio
                        _loop = asyncio.get_event_loop()
                        _loop.create_task(
                            brain.resume_ask(rid, default, timeout_default=True))
                    except Exception as e:
                        # 无运行事件循环 (测试/后台) 时仅返回判定结果
                        logger.warning(f"[task_store] 超时默认恢复调度失败: {e}")
            else:
                out.append((rid, "cancel"))
                run.status = "cancelled"
                self.delete(rid)
                logger.info(f"[task_store] 高风险任务超时取消: {rid}")
        return out


class JsonTaskStore(TaskStore):
    """JSON Adapter (生产): data/task_runtime/runs/<run_id>.json, 原子替换写。

    重启恢复: 启动时 load 全部 pending 交给 brain.resume_ask 或超时清理。
    """

    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        self._dir = os.path.join(data_dir, "runs")
        os.makedirs(self._dir, exist_ok=True)

    def _path(self, run_id: str) -> str:
        # run_id 是内核生成的 hex 短串, 白名单字符校验防路径穿越
        if not run_id or not all(c.isalnum() for c in run_id):
            raise ValueError(f"非法 run_id: {run_id!r}")
        return os.path.join(self._dir, f"{run_id}.json")

    def save(self, run) -> str:
        tmp = self._path(run.run_id) + ".tmp"
        final = self._path(run.run_id)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(run_to_dict(run), f, ensure_ascii=False, indent=1)
        os.replace(tmp, final)  # 原子替换
        return run.run_id

    def load(self, run_id: str):
        try:
            with open(self._path(run_id), encoding="utf-8") as f:
                return dict_to_run(json.load(f))
        except (FileNotFoundError, ValueError):
            return None  # 非法 run_id (路径穿越) 安全拒绝
        except Exception as e:
            logger.warning(f"[task_store] 读取挂起任务失败 {run_id}: {e}")
            return None

    def delete(self, run_id: str) -> bool:
        try:
            os.remove(self._path(run_id))
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.warning(f"[task_store] 删除挂起任务失败 {run_id}: {e}")
            return False

    def list_pending(self) -> list:
        out = []
        try:
            for fn in os.listdir(self._dir):
                if fn.endswith(".json"):
                    out.append(fn[:-5])
        except FileNotFoundError:
            pass
        return out

    def sweep_timeouts(self, brain=None) -> list:
        now = time.time()
        out = []
        for rid in self.list_pending():
            run = self.load(rid)
            if run is None or run.status != "waiting_user":
                continue
            if run.deadline and now < run.deadline:
                continue
            q = run.pending_question or {}
            if q.get("safe_is_default"):
                out.append((rid, "resume_default"))
                if brain is not None:
                    try:
                        default = str(q.get("safe_default") or "好，继续。")
                        import asyncio
                        _loop = asyncio.get_event_loop()
                        _loop.create_task(
                            brain.resume_ask(rid, default, timeout_default=True))
                    except Exception as e:
                        # 无运行事件循环 (测试/后台) 时仅返回判定结果
                        logger.warning(f"[task_store] 超时默认恢复调度失败: {e}")
            else:
                out.append((rid, "cancel"))
                run.status = "cancelled"
                self.delete(rid)
                logger.info(f"[task_store] 高风险任务超时取消: {rid}")
        return out


def create_store(data_dir: str = DEFAULT_DATA_DIR, memory_only: bool = False) -> TaskStore:
    """工厂: 测试传 memory_only=True; 生产默认 JSON Adapter。"""
    if memory_only:
        return MemoryTaskStore()
    return JsonTaskStore(data_dir=data_dir)
