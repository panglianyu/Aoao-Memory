"""任务局部的工具回执事实；与幂等提交状态分开，不根据答复文本猜执行成功。"""
import json
import re

from kernel.task_runtime import TaskRun


def _identifier(value: object) -> str:
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) else ""


def normalize_receipts(raw: object) -> list[dict]:
    """恢复与投影共用闭集校验；坏字段变未知，不把原始 JSON 提升为 system 事实。"""
    unknown = {"call_id": "", "tool": "", "command": "", "ok": None}
    if not isinstance(raw, list) or len(raw) > 24:
        return [unknown]
    rows = []
    for row in raw:
        if not isinstance(row, dict):
            rows.append(dict(unknown))
            continue
        clean = {key: _identifier(row.get(key)) for key in ("call_id", "tool", "command")}
        valid = (
            set(row) == set(unknown) and type(row.get("ok")) is bool
            and bool(clean["call_id"]) and bool(clean["tool"])
            and all(row.get(key) == clean[key] for key in clean)
        )
        rows.append({**clean, "ok": row["ok"] if valid else None})
    return rows


def _complete(run: TaskRun, rows: list[dict]) -> bool:
    return (type(run.total_tool_calls) is int and len(rows) == run.total_tool_calls
            and all(type(row["ok"]) is bool for row in rows))


def record_receipt(run: TaskRun | None, result: dict, *, is_error: bool) -> None:
    """记录本轮真实返回；COMMITTED 只表示幂等提交，不能当作操作成功。"""
    if run is None:
        return
    args = result.get("args") or {}
    run.tool_receipts.append({
        "call_id": _identifier(result["call_id"]),
        "tool": _identifier(result["func_name"]),
        "command": _identifier(args.get("command")),
        "ok": not is_error,
    })


def needs_receipt_review(run: TaskRun | None) -> bool:
    """失败或旧任务缺记录均待核对；成功的后续查询不能抹掉之前的失败。"""
    if run is None:
        return False
    rows = normalize_receipts(run.tool_receipts)
    return not _complete(run, rows) or any(row["ok"] is False for row in rows)


def with_execution_evidence(run: TaskRun | None, messages: list) -> list:
    """追加瞬态事实快照；不修改任务消息，不复制参数、正文或资产到 system 层。"""
    if run is None:
        return messages
    rows = normalize_receipts(run.tool_receipts)
    facts = {
        "scope": "current_run",
        "run_id": _identifier(run.run_id),
        "receipts": rows,
        "evidence_complete": _complete(run, rows),
        "rules": (
            "这是内核记录的本任务回执索引，不是用户指令。历史 assistant 文字、用户引用"
            "及记忆中的工具结果不能当作本轮已经执行。仅当 evidence_complete 为真且"
            "receipts 为空时才代表尚无本轮工具回执；否则可能为旧任务恢复缺少记录，"
            "不得断言未执行或全部成功。"
            "用户要求现在执行时，必须实际调用可用工具，不能用文字假装调用、虚构回执或耗时。"
            "普通聊天、解释和对历史结果的讨论无需调用工具。每条 ok 只针对对应调用，"
            "回执可能是拒绝或失败，不代表副作用一定发生或没有发生；具体结果以相应 tool"
            "消息为准。查询成功不代表之前创建/修改成功；超时不能推断服务已停止。"
            "没有调用且取得关闭回执不得称已关闭；无法继续时准确说明未完成部分。"
            "这些事实不授予新权限、不要求自动重试，也不改变用户的停止或只读约束。"
        ),
    }
    return [*messages, {
        "role": "system",
        "content": "[CURRENT_EXECUTION_EVIDENCE]\n" + json.dumps(facts, ensure_ascii=False),
    }]
