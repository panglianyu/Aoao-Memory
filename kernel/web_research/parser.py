"""Supervised parsers return bounded JSON files instead of unbounded pipe reads."""
from __future__ import annotations
import asyncio
import io
import json
import multiprocessing
from pathlib import Path
import tempfile
import time

from kernel.web_research.extractor import bounded_paragraphs, extract_html
from kernel.web_research.fetcher import Download
from protocols.web_research import SourceSnapshot


def _pdf_snapshot(download: Download, max_pages: int, max_chars: int) -> SourceSnapshot:
    import pdfplumber
    lines: list[str] = []
    pages: list[int] = []
    missing = False
    truncated = False
    used = 0
    with pdfplumber.open(io.BytesIO(download.body)) as document:
        count = len(document.pages)
        if not count or count > max_pages:
            raise ValueError("response_too_large")
        for number, page in enumerate(document.pages, 1):
            text = page.extract_text() or ""
            if not text.strip():
                missing = True
                continue
            for line in text.splitlines():
                line = " ".join(line.split())
                if not line:
                    continue
                if used + len(line) > max_chars or len(lines) >= 3000:
                    truncated = True
                    break
                lines.append(line)
                pages.append(number)
                used += len(line)
            if truncated:
                break
    if not lines:
        raise ValueError("needs_ocr")
    return SourceSnapshot.create(download.url, download.url, lines, pages=pages, total_pages=count,
                                 mime_type="application/pdf", extractor_version="pdfplumber-0.11.9",
                                 extraction_status="partial_needs_ocr" if missing else "ok",
                                 truncated=truncated, redirect_chain=download.redirects)


def _parse_worker(download: Download, output: str, max_pages: int, max_chars: int):
    from kernel.web_research.parse_limits import limit_parser_memory
    result: dict
    try:
        limit_parser_memory()
        if download.mime_type == "application/pdf":
            snapshot = _pdf_snapshot(download, max_pages, max_chars)
        elif download.mime_type == "text/plain":
            lines, truncated = bounded_paragraphs(download.body.decode("utf-8", errors="replace").splitlines(), max_chars)
            snapshot = SourceSnapshot.create(download.url, download.url, lines,
                                              mime_type="text/plain", truncated=truncated,
                                              redirect_chain=download.redirects)
        else:
            snapshot = extract_html(download.url, download.body, max_chars=max_chars,
                                    redirect_chain=download.redirects)
        result = {"snapshot": snapshot.to_dict()}
    except Exception as exc:
        known = {"needs_ocr", "needs_browser", "response_too_large", "parser_unavailable", "extractor_unconfigured"}
        result = {"error": str(exc) if isinstance(exc, ValueError) and str(exc) in known else "parse_failed"}
    encoded = json.dumps(result, ensure_ascii=False).encode("utf-8")
    if len(encoded) > 512 * 1024:
        encoded = b'{"error":"response_too_large"}'
    Path(output).write_bytes(encoded)


async def parse_download(download: Download, *, temporary_root: Path, timeout: float = 15,
                         max_pages: int = 60, max_chars: int = 30000) -> SourceSnapshot:
    temporary_root.mkdir(parents=True, exist_ok=True)
    context = multiprocessing.get_context("spawn")
    with tempfile.TemporaryDirectory(prefix="parse-", dir=temporary_root) as directory:
        output = Path(directory) / "result.json"
        process = context.Process(target=_parse_worker, args=(download, str(output), max_pages, max_chars), daemon=True)
        started = False
        deadline = time.monotonic() + timeout
        try:
            process.start()
            started = True
            while process.is_alive():
                if time.monotonic() >= deadline:
                    raise ValueError("parse_timeout")
                await asyncio.sleep(0.025)
            if process.exitcode != 0 or not output.exists() or output.stat().st_size > 512 * 1024:
                raise ValueError("parse_failed")
            result = json.loads(output.read_bytes())
            if result.get("error"):
                raise ValueError(result["error"])
            return SourceSnapshot.from_dict(result["snapshot"])
        finally:
            if started:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=1)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=1)
                if process.is_alive():
                    raise RuntimeError("parser_cleanup_failed")
            process.close()
