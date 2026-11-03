"""Render bounded research receipts without interpreting web content as commands."""
from __future__ import annotations
from urllib.parse import quote

from kernel.web_research.answer import escape_label, render_answer

ERRORS = {
    "invalid_artifact": "保存的研究资料未通过完整性校验，无法回放。",
    "network_undetermined": "尚未明确本轮是否允许联网，未发送网络请求。",
    "permission_denied": "联网研究只允许已验证身份的老板在当前任务使用。",
    "network_forbidden": "本轮要求不联网，已停止网络请求。",
    "provider_unconfigured": "搜索服务尚未完成本机配置，可先使用公开网页链接读取功能。",
    "provider_unavailable": "本机搜索服务当前不可用，请确认 SearXNG 已启动。",
    "provider_response_invalid": "本机搜索服务返回了无效响应，本次结果未采用。",
    "provider_error": "搜索服务请求失败，本次没有获得候选资料。",
    "provider_auth_failed": "搜索服务凭据无效，请检查本地配置。",
    "budget_exhausted": "本轮研究已达到时间或费用预算，已停止继续请求。",
    "cancelled": "本次研究已取消。",
    "needs_ocr": "该 PDF 需要 OCR，当前版本不能可靠读取其正文。",
    "needs_browser": "网页没有可提取的正文，可能需要浏览器或登录。",
    "artifact_expired": "本轮来源快照已过期或不存在，无法回放引用。",
    "blocked_url": "该网址不符合公开网页访问范围，未访问。",
    "model_error": "研究模型请求失败，本次未生成已核实结论。",
    "rate_limited": "搜索服务限流，本次未能完成检索。",
    "network_error": "资料请求失败，当前无法访问来源。",
    "timeout": "资料请求超时，本次研究未完成。",
    "response_too_large": "资料超过当前读取大小或页数限制。",
    "unsupported_content": "该地址的内容格式不在网页或 PDF 读取范围内。",
    "parse_failed": "正文解析失败，不能把该来源当作已读取的证据。",
    "evidence_invalid": "候选证据校验失败，未作为结论交付。",
}


def error_receipt(code: object) -> dict:
    code = code if isinstance(code, str) else "research_failed"
    return {"schema_version": 2, "command": "error", "status": "failed",
            "error_code": code if isinstance(code, str) and code in ERRORS else "research_failed",
            "snapshot_ids": [], "text": ERRORS.get(code, "研究未完成，请检查参数、来源可访问性或本地服务配置。")}


def render_receipt(command: str, payload: dict, scope: str) -> dict:
    refs: list[str] = []
    status = payload.get("status", "complete")
    if command == "research":
        text = render_answer(payload)
        refs = [s["snapshot_id"] for s in payload["sources"]]
    elif command == "web_search":
        failures = sum(bool(row.get("error")) for row in payload["queries"])
        status = "failed" if failures == len(payload["queries"]) else "partial" if failures else "complete"
        rows = list({s["source_id"]: s for s in payload["results"]}.values())[:32]
        text = "以下是搜索候选资料，尚未阅读正文或核实其中说法。"
        for i, row in enumerate(rows, 1):
            url = quote(row["url"], safe=":/?&=%#@+;,~!$-_.")
            text += f"\n\n[{i}: {escape_label(row['title'])}]({url})\n{escape_label(row.get('snippet', ''))}"
        if not rows:
            text += "\n没有获得候选结果。"
        for result in payload["queries"]:
            if result.get("error"):
                text += "\n" + error_receipt(result["error"])["text"]
    else:
        refs = [payload["snapshot_id"]]
        url = quote(payload["final_url"], safe=":/?&=%#@+;,~!$-_.")
        text = f"网页资料摘录，尚未独立核实：\n[{escape_label(payload['title'])}]({url})"
        for line in payload["content"]:
            location = f"第 {line['page']} 页，" if line.get("page") else ""
            text += f"\n{location}行 {line['line']}：{escape_label(line['text'])}"
        if payload.get("window_truncated") or payload.get("truncated"):
            text += "\n仅展示部分正文；引用时须核对原文上下文。"
        if not payload["content"]:
            text += "\n当前范围内没有匹配文字。"
    return {"schema_version": 2, "command": command, "scope": scope,
            "status": status, "snapshot_ids": refs,
            "payload": payload, "text": text}
