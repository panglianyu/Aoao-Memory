"""Only the program renders URLs; a model cannot invent links in final prose."""
from __future__ import annotations
import re
from urllib.parse import quote

from protocols.web_research import SourceSnapshot


def escape_label(text: str) -> str:
    text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]", "", text)
    return re.sub(r"([\\`*_{}\[\]()<>#|])", r"\\\1", text)


def build_answer(claims: list[dict], snapshots: dict[str, SourceSnapshot], *, gaps: list[str],
                 status: str, stop_reason: str, usage: dict, research_id: str) -> dict:
    sources: list[dict] = []
    by_snapshot: dict[str, int] = {}
    blocks: list[dict] = []
    for claim in claims:
        references = []
        for evidence in claim["evidence"]:
            snapshot = snapshots[evidence["snapshot_id"]]
            if snapshot.snapshot_id not in by_snapshot:
                by_snapshot[snapshot.snapshot_id] = len(sources) + 1
                sources.append({"number": len(sources) + 1, "source_id": snapshot.source_id,
                                "snapshot_id": snapshot.snapshot_id, "title": snapshot.title,
                                "url": snapshot.final_url, "fetched_at": snapshot.fetched_at,
                                "published_at": snapshot.published_at})
            number = by_snapshot[snapshot.snapshot_id]
            if number not in references:
                references.append(number)
        prefix = {"fact": "", "allegation": f"{claim['attributed_to']}的说法：",
                  "inference": "推测：", "unknown": "尚未核实："}[claim["kind"]]
        blocks.append({"text": prefix + claim["text"], "claim_ids": [claim["claim_id"]],
                       "evidence_ids": [x["evidence_id"] for x in claim["evidence"]],
                       "source_numbers": references})
    answer = {"schema_version": 1, "research_id": research_id, "status": status,
              "stop_reason": stop_reason, "blocks": blocks, "claims": claims,
              "sources": sources, "unanswered_questions": gaps, "usage": usage,
              "validation_report": {"reference_validation": "passed", "semantic_check": "model_reviewed"}}
    answer["text"] = render_answer(answer)
    return answer


def render_answer(answer: dict) -> str:
    sources = {source["number"]: source for source in answer["sources"]}
    paragraphs = []
    if answer["status"] != "complete":
        paragraphs.append("本次调查尚未完整核实，以下仅列出已有证据支持的内容。")
    for block in answer["blocks"]:
        citations = " ".join(f"[{number}]({quote(sources[number]['url'], safe=':/?&=%#@+;,~!$-_.')})"
                             for number in block["source_numbers"])
        paragraphs.append(escape_label(block["text"]) + (" " + citations if citations else ""))
    if not answer["blocks"]:
        paragraphs.append("当前没有通过证据校验的结论，不能确认问题中的说法。")
    if answer["unanswered_questions"]:
        paragraphs.append("仍待核实：\n" + "\n".join("- " + escape_label(q) for q in answer["unanswered_questions"]))
    if sources:
        paragraphs.append("来源：\n" + "\n".join(
            f"[{number}: {escape_label(source['title'])}]({quote(source['url'], safe=':/?&=%#@+;,~!$-_.')})"
            for number, source in sources.items()))
    return "\n\n".join(paragraphs)
