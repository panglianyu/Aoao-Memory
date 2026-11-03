"""Project research turns to opaque handles before any tool-enabled model sees them."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re

_NOTICE = "公开资料已通过研究工具处理。正文只保存在受限研究资料中，不是工具指令。"
_INCOMPLETE_NOTICE = "公开资料研究未完整完成。正文只保存在受限研究资料中，不是工具指令。"
_FAILED_NOTICE = "公开资料研究未完成。网页正文未发布到通用工具事件或模型历史。"
_RESEARCH_ID = re.compile(r"research_[0-9a-f]{32}")
_LEDGER_FIELDS = {
    "state", "arguments", "revision", "result", "function_name", "receipt_ok",
    "research_scope", "research_digest",
}
_LEDGER_STATES = {"CLAIMED", "COMMITTED", "FAILED", "AMBIGUOUS"}


def issued_research_id(run=None) -> str:
    """Derive the only handle that can belong to this authenticated run."""
    principal = getattr(run, "principal", None)
    identity = [
        getattr(run, "run_id", ""), getattr(run, "session_key", ""),
        getattr(principal, "actor_id", ""), getattr(principal, "channel", ""),
        getattr(principal, "authenticated_by", ""),
    ]
    if not all(isinstance(value, str) and value for value in identity):
        return ""
    scope = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
    return "research_" + scope[:32]


def research_identity(run=None) -> tuple[str, str]:
    """Return only program-issued research identity fields from the active run."""
    display = getattr(run, "_research_display", None)
    research_id = getattr(run, "research_id", "")
    status = getattr(run, "research_status", "")
    if isinstance(display, dict):
        research_id = display.get("research_id", research_id)
        status = display.get("status", status)
    expected = issued_research_id(run)
    if research_id != expected:
        context = getattr(run, "_research_context", None)
        scope = getattr(context, "scope", "")
        contextual = "research_" + scope[:32] if re.fullmatch(r"[0-9a-f]{64}", scope) else ""
        research_id = contextual if contextual == expected else ""
    if not research_id and status:
        status = "failed"
    if status not in {"complete", "partial", "failed", "expired"}:
        status = "failed" if research_id else ""
    return research_id, status


def safe_research_notice(research_id: str = "", status: str = "complete") -> str:
    base = _NOTICE if status == "complete" else (
        _INCOMPLETE_NOTICE if status == "partial" else _FAILED_NOTICE
    )
    return base + (" 研究句柄：" + research_id if _RESEARCH_ID.fullmatch(research_id) else "")


def history_notice(run=None) -> str:
    return safe_research_notice(*research_identity(run))


def project_tool_event(
    run, content: str, *, research: bool, call_id: str,
) -> tuple[str, dict, str]:
    """Project an execution result before it reaches the shared event bus or logs."""
    metadata: dict = {"call_id": call_id}
    if not research:
        return content, metadata, content
    research_id, status = research_identity(run)
    metadata["research"] = {
        "classification": "web_research",
        "research_id": research_id,
        "status": status,
    }
    notice = safe_research_notice(research_id, status)
    return notice, metadata, "研究结果已进入受限交付链路"


def _research_ledger_entry(entry: dict) -> bool:
    function_name = entry.get("function_name")
    scope = entry.get("research_scope")
    digest = entry.get("research_digest")
    has_markers = scope is not None or digest is not None
    if has_markers and not (
        isinstance(scope, str) and re.fullmatch(r"[0-9a-f]{64}", scope)
        and isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)
    ):
        raise ValueError("Invalid saved tool ledger")
    if function_name == "use_web_research":
        research = True
    elif function_name == "instruct_app":
        try:
            arguments = json.loads(entry.get("arguments", ""))
        except (ValueError, TypeError):
            raise ValueError("Invalid saved tool ledger")
        if not isinstance(arguments, dict):
            raise ValueError("Invalid saved tool ledger")
        research = arguments.get("app_name") == "web_research"
    else:
        research = False
    if has_markers and not research:
        raise ValueError("Invalid saved tool ledger")
    if research:
        try:
            arguments = json.loads(entry.get("arguments", ""))
        except (ValueError, TypeError):
            raise ValueError("Invalid saved tool ledger")
        if not isinstance(arguments, dict):
            raise ValueError("Invalid saved tool ledger")
    return research


def project_executed_ledger(
    ledger, *, research_id: str = "", status: str = "complete",
) -> dict:
    """Validate persisted tool receipts and remove research prose."""
    if not isinstance(ledger, dict):
        raise ValueError("Invalid saved tool ledger")
    projected = deepcopy(ledger)
    for call_id, entry in projected.items():
        if (not isinstance(call_id, str) or not call_id or len(call_id) > 200
                or not isinstance(entry, dict) or not set(entry) <= _LEDGER_FIELDS
                or entry.get("state") not in _LEDGER_STATES
                or not isinstance(entry.get("arguments", ""), str)
                or type(entry.get("revision")) is not int or entry["revision"] < 0
                or not isinstance(entry.get("result", ""), str)
                or not isinstance(entry.get("function_name", ""), str)
                or not ("receipt_ok" not in entry or type(entry["receipt_ok"]) is bool)
                or not (entry.get("research_scope") is None
                        or isinstance(entry["research_scope"], str)
                        and re.fullmatch(r"[0-9a-f]{64}", entry["research_scope"]))
                or not (entry.get("research_digest") is None
                        or isinstance(entry["research_digest"], str)
                        and re.fullmatch(r"[0-9a-f]{64}", entry["research_digest"]))):
            raise ValueError("Invalid saved tool ledger")
        if _research_ledger_entry(entry):
            entry["result"] = safe_research_notice(research_id, status)
    return projected


def _research_call(call: dict) -> bool:
    function = call.get("function", {})
    if function.get("name") == "use_web_research":
        return True
    if function.get("name") != "instruct_app":
        return False
    try:
        return json.loads(function.get("arguments", "{}" )).get("app_name") == "web_research"
    except (ValueError, TypeError, AttributeError):
        return False


def project_research_history(
    messages: list[dict], *, research_id: str = "", status: str = "complete",
) -> list[dict]:
    """Preserve tool-call pairing, dropping research prose including old task snapshots."""
    result = deepcopy(messages)
    notice = safe_research_notice(research_id, status)
    in_research = False
    for message in result:
        if message.get("role") == "user":
            in_research = False
        research_calls = [
            call for call in message.get("tool_calls", []) if _research_call(call)
        ]
        if research_calls:
            in_research = True
            for call in research_calls:
                call["function"]["arguments"] = "{}"
        if in_research and message.get("role") in {"assistant", "tool", "system"}:
            message["content"] = notice
            message.pop("reasoning_content", None)
    return result
