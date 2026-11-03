"""Program-owned display records, partitioned by authenticated owner and session."""
from __future__ import annotations

import hashlib
import json
import math
import time
import re

from kernel.web_research.artifacts import receipt_digest, validate_receipt
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.context import ResearchContext
from kernel.web_research.receipt import error_receipt
from kernel.web_research.validation import validate_research_id

_MAX_UNIX_TIMESTAMP = 253402300799


def _valid_created_at(value) -> bool:
    return (type(value) in {int, float} and 0 <= value <= _MAX_UNIX_TIMESTAMP
            and (type(value) is int or math.isfinite(value)))


def owner_scope(principal, session_key: str) -> str:
    if not principal.is_owner or not principal.actor_id or not principal.authenticated_by or not session_key:
        raise ValueError("permission_denied")
    return hashlib.sha256(json.dumps([principal.actor_id, principal.channel,
        principal.authenticated_by, session_key], ensure_ascii=False).encode()).hexdigest()


def build_display(receipt: dict, *, research_id: str) -> dict:
    command, payload = receipt["command"], receipt["payload"]
    blocks: list[dict] = []
    sources: list[dict] = []
    if command == "research":
        blocks = [{"text": b["text"], "source_numbers": b["source_numbers"]} for b in payload["blocks"]]
        blocks.extend({"text": "仍待核实：" + gap, "source_numbers": []} for gap in payload["unanswered_questions"])
        sources = [{k: s[k] for k in ("number", "title", "url")} for s in payload["sources"]]
    elif command == "web_search":
        for source in {s["source_id"]: s for s in payload["results"]}.values():
            number = len(sources) + 1
            sources.append({"number": number, "title": source["title"], "url": source["url"]})
            blocks.append({"text": source.get("snippet") or source["title"], "source_numbers": [number]})
        blocks.extend({"text": error_receipt(row["error"])["text"], "source_numbers": []}
                      for row in payload["queries"] if row.get("error"))
    else:
        sources = [{"number": 1, "title": payload["title"], "url": payload["final_url"]}]
        blocks = [{"text": (f"第 {line['page']} 页，" if line.get("page") else "") +
                   f"行 {line['line']}：{line['text']}", "source_numbers": [1]} for line in payload["content"]]
    notice = "已核对引用；事实仍以来源和证据范围为限。" if command == "research" else "以下是候选资料或原文摘录，尚未独立核实。"
    if receipt["status"] != "complete":
        notice = "本次研究未完整完成。" + notice
    return {"schema_version": 1, "research_id": research_id, "status": receipt["status"],
            "notice": notice, "blocks": blocks, "sources": sources}


def save_display(store, context, receipt: dict) -> dict:
    validate_receipt(receipt, context, store)
    owner = owner_scope(context.principal, context.session_key)
    run = context.budget.run
    display = build_display(receipt, research_id="research_" + context.scope[:32])
    record = {"schema_version": 1, "run_id": context.run_id, "session_key": context.session_key,
              "owner_scope": owner, "turn_id": getattr(run, "turn_id", context.run_id),
              "created_at": getattr(run, "started_at", 0) or time.time(), "receipt": receipt}
    with store._lock:
        try:
            index = store._read(owner, "history.json")["entries"]
        except ValueError as exc:
            if str(exc) != "artifact_expired":
                raise
            index = []
        entry = {"scope": context.scope, "turn_id": record["turn_id"], "created_at": record["created_at"],
                 "digest": receipt_digest(receipt)}
        entries = [row for row in index if row["scope"] != context.scope][-99:] + [entry]
        store._write(context.scope, "display.json", record)
        store._write(owner, "history.json", {"schema_version": 1, "entries": entries})
    return display


def read_display_receipt(store, principal, session_key: str, entry: dict) -> tuple[dict, ResearchContext]:
    record = store._read(entry["scope"], "display.json")
    if (record.get("schema_version") != 1 or record.get("owner_scope") != owner_scope(principal, session_key)
            or record.get("session_key") != session_key):
        raise ValueError("permission_denied")
    context = ResearchContext(record["run_id"], session_key, principal, ResearchBudget(ResearchConfig()))
    if context.scope != entry["scope"] or receipt_digest(record["receipt"]) != entry["digest"]:
        raise ValueError("invalid_artifact")
    return validate_receipt(record["receipt"], context, store), context


def list_displays(store, principal, session_key: str, cursor: int = 0) -> dict:
    if type(cursor) is not int or not 0 <= cursor <= 100:
        raise ValueError("invalid_artifact")
    owner = owner_scope(principal, session_key)
    try:
        index = store._read(owner, "history.json")
    except ValueError as exc:
        if str(exc) == "artifact_expired":
            return {"messages": [], "next_cursor": None}
        raise
    if index.get("schema_version") != 1 or type(index.get("entries")) is not list or len(index["entries"]) > 100:
        raise ValueError("invalid_artifact")
    if any(
        type(entry) is not dict
        or not re.fullmatch(r"[0-9a-f]{64}", entry.get("scope", ""))
        or not isinstance(entry.get("turn_id"), str)
        or not entry["turn_id"] or len(entry["turn_id"]) > 100
        or not _valid_created_at(entry.get("created_at"))
        or not re.fullmatch(r"[0-9a-f]{64}", entry.get("digest", ""))
        for entry in index["entries"]
    ):
        raise ValueError("invalid_artifact")
    entries = list(reversed(index["entries"]))
    messages: list[dict] = []
    used = 64
    consumed = 0
    for entry in entries[cursor:cursor + 50]:
        try:
            receipt, context = read_display_receipt(store, principal, session_key, entry)
            display = build_display(receipt, research_id="research_" + context.scope[:32])
            # The structured blocks carry the body; do not duplicate it in the wire fallback.
            content = display["notice"]
        except (ValueError, KeyError, TypeError, OSError):
            display = {"schema_version": 1, "research_id": "research_" + entry["scope"][:32],
                "status": "expired", "notice": "研究资料已过期、缺失或未通过校验，不能恢复引用。",
                "blocks": [], "sources": []}
            content = display["notice"]
        message = {"turn_id": entry["turn_id"], "at": entry["created_at"] * 1000,
                   "content": content, "status": "error" if display["status"] in {"failed", "expired"} else "ok",
                   "research": display}
        size = len(json.dumps(message, ensure_ascii=False).encode())
        if used + size > 250000:
            research_id = "research_" + entry["scope"][:32]
            notice = "该研究记录过大，未恢复正文或引用；可使用研究句柄继续受控追问。"
            display = {"schema_version": 1, "research_id": research_id,
                       "status": "partial", "notice": notice,
                       "blocks": [], "sources": []}
            messages.append({"turn_id": entry["turn_id"], "at": entry["created_at"] * 1000,
                             "content": notice, "status": "ok", "research": display})
            consumed += 1
            break
        messages.append(message)
        used += size
        consumed += 1
    next_cursor = cursor + consumed
    return {"messages": messages, "next_cursor": next_cursor if next_cursor < len(entries) else None}


def delivery_metadata(brain) -> dict:
    display = getattr(getattr(brain, "_current_run", None), "_research_display", None)
    if display is None:
        saved = getattr(brain, "_last_research_delivery", None)
        if saved and saved[0] == getattr(brain, "_current_turn_id", None):
            display = saved[1]
    return {"research": display} if isinstance(display, dict) else {}


def find_receipt(store, principal, session_key: str, research_id: str):
    validate_research_id(research_id)
    owner = owner_scope(principal, session_key)
    index = store._read(owner, "history.json")
    if index.get("schema_version") != 1 or not isinstance(index.get("entries"), list) or len(index["entries"]) > 100:
        raise ValueError("invalid_artifact")
    for entry in index["entries"]:
        if "research_" + entry["scope"][:32] == research_id:
            return read_display_receipt(store, principal, session_key, entry)
    raise ValueError("artifact_expired")
