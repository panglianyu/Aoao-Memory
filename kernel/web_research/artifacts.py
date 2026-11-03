"""Validate stored evidence against trusted identity and immutable source bodies."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json

from kernel.web_research.answer import build_answer
from kernel.web_research.evidence import plain_text, validate_claim
from kernel.web_research.receipt import render_receipt
from kernel.web_research.context import (
    ResearchCallIdentity,
    research_call_identity,
    validate_research_call_identity,
)
from protocols.web_research import canonical_url, source_id

ARTIFACT_VERSION = 2


def receipt_digest(receipt: dict) -> str:
    return hashlib.sha256(json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                                     allow_nan=False).encode()).hexdigest()


def _check_search(payload: dict) -> None:
    queries = payload["queries"]
    if type(queries) is not list or not 1 <= len(queries) <= 4:
        raise ValueError("invalid_artifact")
    results = [source for query in queries for source in query["results"]]
    if len(results) > 32 or results != payload["results"]:
        raise ValueError("invalid_artifact")
    for source in results:
        if source_id(source["url"]) != source["source_id"] or canonical_url(source["url"]) != source["url"]:
            raise ValueError("invalid_artifact")
        for field, limit in (("title", 512), ("snippet", 1600)):
            if not isinstance(source.get(field, ""), str) or len(source.get(field, "")) > limit:
                raise ValueError("invalid_artifact")


def _check_window(payload: dict, snapshot) -> None:
    original = snapshot.to_dict()
    for key, value in original.items():
        if key != "content" and json.dumps(payload.get(key)) != json.dumps(value):
            raise ValueError("invalid_artifact")
    rows = payload["content"]
    if not isinstance(rows, list) or len(rows) > 200:
        raise ValueError("invalid_artifact")
    numbers = []
    for row in rows:
        number = row["line"]
        if type(number) is not int or not 1 <= number <= len(snapshot.content):
            raise ValueError("invalid_artifact")
        if row != asdict(snapshot.content[number - 1]):
            raise ValueError("invalid_artifact")
        numbers.append(number)
    if numbers != sorted(set(numbers)) or payload.get("total_lines") != len(snapshot.content):
        raise ValueError("invalid_artifact")


def _check_answer(payload: dict, snapshots: dict) -> None:
    if len(payload["claims"]) > 10 or len(payload["sources"]) > 8:
        raise ValueError("invalid_artifact")
    claims = [validate_claim(claim, snapshots) for claim in payload["claims"]]
    gaps = [plain_text(value, limit=400) for value in payload["unanswered_questions"]]
    if len(gaps) > 12 or payload["status"] not in {"complete", "partial", "failed"}:
        raise ValueError("invalid_artifact")
    expected = build_answer(claims, snapshots, gaps=gaps, status=payload["status"],
        stop_reason=payload["stop_reason"], usage=payload["usage"], research_id=payload["research_id"])
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ValueError("invalid_artifact")


def validate_receipt(receipt: dict, context, store) -> dict:
    context.validate(network=False)
    if receipt.get("schema_version") != ARTIFACT_VERSION or receipt.get("scope") != context.scope:
        raise ValueError("invalid_artifact")
    command, payload = receipt["command"], receipt["payload"]
    if command == "research":
        refs = list(dict.fromkeys(source["snapshot_id"] for source in payload["sources"]))
    elif command == "web_search":
        refs = []
    elif command in {"web_open", "web_find", "web_pdf_page"}:
        refs = [payload["snapshot_id"]]
    else:
        raise ValueError("invalid_artifact")
    if refs != receipt["snapshot_ids"]:
        raise ValueError("invalid_artifact")
    snapshots = {ref: store.load(context.scope, ref) for ref in refs}
    if command == "research":
        _check_answer(payload, snapshots)
    elif command == "web_search":
        _check_search(payload)
    else:
        _check_window(payload, snapshots[refs[0]])
    expected = render_receipt(command, payload, context.scope)
    if expected != receipt:
        raise ValueError("invalid_artifact")
    return expected


def _validated_state(store, context) -> dict:
    state = store.load_state(context.scope)
    if state.get("schema_version") != ARTIFACT_VERSION:
        raise ValueError("invalid_artifact")
    if (state.get("run_id"), state.get("session_key"), state.get("principal_key")) != (
            context.run_id, context.session_key, context.scope):
        raise ValueError("permission_denied")
    return state


def bind_receipt_to_call(store, context, receipt: dict,
                         identity: ResearchCallIdentity) -> None:
    trusted_identity = validate_research_call_identity(identity.to_dict())
    state = _validated_state(store, context)
    stored_receipt = validate_receipt(state["receipt"], context, store)
    if receipt_digest(stored_receipt) != receipt_digest(receipt):
        raise ValueError("invalid_artifact")
    previous = state.get("call_identity")
    if previous is not None:
        if not isinstance(previous, dict):
            raise ValueError("invalid_artifact")
        if validate_research_call_identity(previous) != trusted_identity:
            raise ValueError("invalid_artifact")
    state["call_identity"] = trusted_identity
    store.save_state(context.scope, state)


def load_receipt(store, context, entry: dict,
                 expected_identity: ResearchCallIdentity) -> dict:
    if entry.get("research_scope") != context.scope:
        raise ValueError("permission_denied")
    expected = validate_research_call_identity(expected_identity.to_dict())
    state = _validated_state(store, context)
    stored_identity = state.get("call_identity")
    if not isinstance(stored_identity, dict):
        raise ValueError("invalid_artifact")
    if validate_research_call_identity(stored_identity) != expected:
        raise ValueError("invalid_artifact")
    function_name = entry.get("function_name")
    arguments = entry.get("arguments")
    revision = entry.get("revision")
    if (not isinstance(function_name, str) or not isinstance(arguments, str)
            or not isinstance(revision, int) or isinstance(revision, bool)):
        raise ValueError("invalid_artifact")
    ledger_identity = research_call_identity(
        expected_identity.call_id,
        function_name,
        arguments,
        revision,
    )
    if ledger_identity.to_dict() != expected:
        raise ValueError("invalid_artifact")
    receipt = validate_receipt(state["receipt"], context, store)
    if (receipt_digest(receipt) != entry.get("research_digest")
            or (receipt["status"] != "failed") is not entry.get("receipt_ok")):
        raise ValueError("invalid_artifact")
    return receipt
