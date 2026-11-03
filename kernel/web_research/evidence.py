"""Validate reference existence separately from model judgments of factual support."""
from __future__ import annotations
import hashlib
import json
import re
from urllib.parse import urlsplit

from protocols.web_research import SourceSnapshot


def plain_text(value, *, limit: int = 1200) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("evidence_invalid")
    if re.search(r"[\x00-\x08\x0b-\x1f\x7f]|(?:https?://|file:|data:|javascript:)", value, re.I):
        raise ValueError("evidence_invalid")
    return value.strip()


def validate_claim(raw: dict, snapshots: dict[str, SourceSnapshot]) -> dict:
    if type(raw) is not dict or raw.get("kind") not in {"fact", "allegation", "inference", "unknown"}:
        raise ValueError("evidence_invalid")
    text = plain_text(raw.get("text"))
    confidence = raw.get("confidence", "unknown")
    if confidence not in {"high", "medium", "low", "unknown"}:
        raise ValueError("evidence_invalid")
    reason = plain_text(raw.get("confidence_reason", "证据仍有限"), limit=400)
    questions = raw.get("question_ids", [])
    if not isinstance(questions, list) or len(questions) > 6 or any(
        not isinstance(q, str) or not re.fullmatch(r"q[1-6]", q) for q in questions
    ):
        raise ValueError("evidence_invalid")
    rows = raw.get("evidence", [])
    if not isinstance(rows, list) or len(rows) > 12 or (not rows and raw["kind"] != "unknown"):
        raise ValueError("evidence_invalid")
    evidence = _checked_evidence(rows, snapshots, raw["kind"])
    attribution = raw.get("attributed_to")
    if raw["kind"] == "allegation":
        attribution = plain_text(attribution, limit=200)
    elif attribution is not None:
        attribution = plain_text(attribution, limit=200)
    return {"claim_id": "claim_" + hashlib.sha256(text.encode()).hexdigest()[:24],
            "text": text, "kind": raw["kind"], "confidence": confidence,
            "confidence_reason": reason, "question_ids": questions,
            "attributed_to": attribution, "evidence": evidence}


def origin_group(snapshot: SourceSnapshot) -> str:
    from tld import get_fld
    return get_fld(snapshot.final_url, fail_silently=True) or (urlsplit(snapshot.final_url).hostname or "")


def independent_sources(evidence: list[dict], snapshots: dict[str, SourceSnapshot]) -> int:
    groups: set[str] = set()
    content_hashes: set[str] = set()
    excerpts: set[str] = set()
    for item in evidence:
        if item["relation"] != "supports":
            continue
        source = snapshots[item["snapshot_id"]]
        normalized = " ".join(item["quote"].lower().split())
        if source.content_hash in content_hashes or normalized in excerpts:
            continue
        content_hashes.add(source.content_hash)
        excerpts.add(normalized)
        groups.add(origin_group(source))
    return len(groups)


def _checked_evidence(rows: list[dict], snapshots: dict[str, SourceSnapshot], kind: str) -> list[dict]:
    evidence = []
    for row in rows:
        if type(row) is not dict:
            raise ValueError("evidence_invalid")
        snapshot = snapshots.get(row.get("snapshot_id", ""))
        start, end = row.get("line_start"), row.get("line_end")
        if snapshot is None or row.get("source_id") != snapshot.source_id or not (
            type(start) is int and type(end) is int and 1 <= start <= end <= len(snapshot.content)
            and end - start < 12
        ):
            raise ValueError("evidence_invalid")
        quote = row.get("quote")
        passage = "\n".join(x.text for x in snapshot.content[start - 1:end])
        relation = row.get("relation", "supports")
        if not isinstance(quote, str) or not quote.strip() or len(quote) > 2000 or quote not in passage:
            raise ValueError("evidence_invalid")
        if relation not in {"supports", "refutes", "context"}:
            raise ValueError("evidence_invalid")
        if row.get("page") is not None and any(x.page != row["page"] for x in snapshot.content[start - 1:end]):
            raise ValueError("evidence_invalid")
        item = {"source_id": snapshot.source_id, "snapshot_id": snapshot.snapshot_id,
                "line_start": start, "line_end": end, "quote": quote, "relation": relation,
                "page": snapshot.content[start - 1].page,
                "extraction_origin": "pdf_text" if snapshot.mime_type == "application/pdf" else "html"}
        item["evidence_id"] = "ev_" + hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()[:24]
        evidence.append(item)
    if kind != "unknown" and not any(x["relation"] == "supports" for x in evidence):
        raise ValueError("evidence_invalid")
    return evidence
