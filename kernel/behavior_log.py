# -*- coding: utf-8 -*-
"""[阶段4] 行为自查日志: 记录阿露出站行为 + 记忆变更, 供 behavior_recall 主动查。

通用模块, 不焊任何渠道: brain._run_tool_loop 收集 use_<app> 出站命令时调用,
system_memory remember/forget 时调用。落 data/logs/behavior_log.jsonl。
治"阿露不知自己发了什么"(15:10 编造妮子回复被老板指出才承认)。

不被动注入 prompt (不改交互), 阿露需要时主动调 behavior_recall 工具查。
守 [[capability-generic-not-coupled]]。
"""
import json
import os
import threading
import time
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

_LOCK = threading.Lock()
# [0.3.37.0] 归位 data/logs/ + 锁定绝对路径 (不依赖进程 CWD);
# data/ 顶层零散文件规范见 data/README.md。旧路径残留由
# kernel.data_layout.migrate_once() 在 boot 首行自愈迁移。
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DATA_DIR = os.path.join(_BASE, "data", "logs")
_FILENAME = "behavior_log.jsonl"


def _log_path(data_dir: str = _DEFAULT_DATA_DIR) -> str:
    return os.path.join(data_dir, _FILENAME)


def append_behavior(
    action_type: str,
    content: str,
    app: str = "",
    target_user: str = "",
    op: str = "",
    turn_id: str = "",
    data_dir: str = _DEFAULT_DATA_DIR,
    actor: str = "",
    role: str = "",
    channel: str = "",
) -> None:
    """追加一条行为日志到 data/logs/behavior_log.jsonl。

    action_type: "outbound" (说了什么话) | "memory_change" (改了啥记忆)
    content: 消息文本 / 记忆内容
    app: 出站渠道 (wechat/feishu/...), memory_change 时空
    target_user: 出站目标, memory_change 时空
    op: memory_change 时的操作 (remember/forget), outbound 时空
    turn_id: 本轮 think 标识 (可选, 供同轮聚合)
    actor/role/channel: [0.3.15.0 P2] 调用主体审计字段 (Principal 三元组:
        谁/什么身份档/哪个渠道触发), 满足报告 P1 验收 "任意工具调用能回答
        谁、在哪个会话、因为什么权限、调用了什么"。旧调用方不传则空串,
        旧记录无字段读取侧 dict.get 兼容。
    """
    record = {
        "ts": time.time(),
        "action_type": action_type,
        "app": app,
        "target_user": target_user,
        "content": content,
        "op": op,
        "turn_id": turn_id,
        "actor": actor,
        "role": role,
        "channel": channel,
    }
    line = json.dumps(record, ensure_ascii=False)
    try:
        with _LOCK:
            os.makedirs(data_dir, exist_ok=True)
            with open(_log_path(data_dir), "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        logger.warning(f"[BehaviorLog] 写入失败(非致命): {e}")


def read_recent(
    n: int = 10,
    action_type: Optional[str] = None,
    query: Optional[str] = None,
    data_dir: str = _DEFAULT_DATA_DIR,
) -> list:
    """读最近 N 条 (可选按 action_type 过滤, query 子串匹配 content/target_user)。

    返回 list[dict], 按时间倒序 (最近在前)。文件不存在时返回 []。
    """
    path = _log_path(data_dir)
    if not os.path.exists(path):
        return []
    records = []
    try:
        with _LOCK:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if action_type and rec.get("action_type") != action_type:
                        continue
                    if query:
                        q = query.lower()
                        hay = (str(rec.get("content", "")) + " " + str(rec.get("target_user", ""))).lower()
                        if q not in hay:
                            continue
                    records.append(rec)
    except Exception as e:
        logger.warning(f"[BehaviorLog] 读取失败(非致命): {e}")
        return []
    # 最近在前
    return list(reversed(records[-n:])) if n else list(reversed(records))


def summarize_recent(
    n: int = 5,
    action_type: Optional[str] = None,
    data_dir: str = _DEFAULT_DATA_DIR,
) -> str:
    """生成近期行为摘要文本 (供 behavior_recall 工具返回 / 调试)。空时返回空串。"""
    records = read_recent(n=n, action_type=action_type, data_dir=data_dir)
    if not records:
        return ""
    lines = []
    for rec in records:
        ts = rec.get("ts", 0)
        at = rec.get("action_type", "")
        if at == "outbound":
            app = rec.get("app", "")
            tgt = rec.get("target_user", "")
            content = rec.get("content", "")
            # [0.3.15.0 P2] 有主体审计字段时附带 (旧记录无字段 -> 不附)。
            _actor = rec.get("actor", "")
            _who = f" [主体:{_actor}/{rec.get('role', '')}]" if _actor else ""
            lines.append(f"[{ts}] 对 {tgt}({app}) 发送: {content}{_who}")
        elif at == "memory_change":
            op = rec.get("op", "")
            content = rec.get("content", "")
            lines.append(f"[{ts}] 记忆{op}: {content}")
        else:
            lines.append(f"[{ts}] {at}: {rec.get('content', '')}")
    return "\n".join(lines)
