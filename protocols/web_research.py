"""Immutable source snapshots and the public research wire contract."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from typing import Sequence

from kernel.public_http import _target

SCHEMA_VERSION = 1
MAX_PAGE_CHARS = 30000
ID_PATTERN = re.compile(r"^(?:src|snap)_[0-9a-f]{32}$")


def canonical_url(url: str) -> str:
    value = _target(url)
    parts = urlsplit(value)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/",
                       urlencode(query), ""))


def source_id(url: str) -> str:
    return "src_" + hashlib.sha256(canonical_url(url).encode()).hexdigest()[:32]


def _validate_metadata(metadata: dict) -> None:
    if (type(metadata.get("truncated", False)) is not bool
            or metadata.get("mime_type", "text/html") not in {"text/html", "text/plain", "application/pdf"}
            or metadata.get("extraction_status", "ok") not in {"ok", "limited", "partial_needs_ocr"}
            or metadata.get("date_origin", "unknown") not in {"unknown", "html_metadata_heuristic"}):
        raise ValueError("invalid_snapshot")
    total_pages = metadata.get("total_pages")
    if total_pages is not None and (type(total_pages) is not int or not 1 <= total_pages <= 60):
        raise ValueError("invalid_snapshot")
    for key, limit in (("links", 100), ("redirect_chain", 4)):
        values = metadata.get(key, ())
        if not isinstance(values, (tuple, list)) or len(values) > limit:
            raise ValueError("invalid_snapshot")
        metadata[key] = tuple(canonical_url(value) for value in values)


@dataclass(frozen=True)
class ContentLine:
    line: int
    text: str
    page: int | None = None


@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str
    snapshot_id: str
    final_url: str
    title: str
    content: tuple[ContentLine, ...]
    content_hash: str
    fetched_at: str
    published_at: str | None = None
    updated_at: str | None = None
    author: str | None = None
    mime_type: str = "text/html"
    extractor_version: str = "research-v1"
    extraction_status: str = "ok"
    truncated: bool = False
    total_pages: int | None = None
    links: tuple[str, ...] = ()
    redirect_chain: tuple[str, ...] = ()
    date_origin: str = "unknown"

    @classmethod
    def create(cls, url: str, title: str, paragraphs: list[str], *,
               pages: Sequence[int | None] | None = None, **metadata) -> SourceSnapshot:
        safe_url = canonical_url(url)
        if not isinstance(title, str) or len(title) > 512 or not paragraphs:
            raise ValueError("invalid_snapshot")
        if any(not isinstance(p, str) or not p.strip() for p in paragraphs):
            raise ValueError("invalid_snapshot")
        if sum(map(len, paragraphs)) > MAX_PAGE_CHARS or len(paragraphs) > 3000:
            raise ValueError("response_too_large")
        page_numbers = pages if pages is not None else [None] * len(paragraphs)
        if len(page_numbers) != len(paragraphs) or any(
            p is not None and (type(p) is not int or not 1 <= p <= 60) for p in page_numbers
        ):
            raise ValueError("invalid_page")
        content = tuple(ContentLine(i + 1, p, page_numbers[i]) for i, p in enumerate(paragraphs))
        digest = hashlib.sha256(json.dumps([asdict(x) for x in content], ensure_ascii=False).encode()).hexdigest()
        fetched = metadata.pop("fetched_at", datetime.now(timezone.utc).isoformat())
        if not isinstance(fetched, str) or len(fetched) > 80:
            raise ValueError("invalid_snapshot")
        try:
            if datetime.fromisoformat(fetched).tzinfo is None:
                raise ValueError("invalid_snapshot")
        except ValueError as exc:
            raise ValueError("invalid_snapshot") from exc
        for key, limit in (("published_at", 80), ("updated_at", 80), ("author", 200), ("extractor_version", 80)):
            value = metadata.get(key)
            if value is not None and (not isinstance(value, str) or len(value) > limit):
                raise ValueError("invalid_snapshot")
        _validate_metadata(metadata)
        value = cls(source_id(safe_url), "", safe_url, title, content, digest, fetched, **metadata)
        identity = hashlib.sha256(json.dumps(asdict(value), sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:32]
        return replace(value, snapshot_id="snap_" + identity)

    def to_dict(self) -> dict:
        return {"schema_version": SCHEMA_VERSION, **asdict(self)}

    @classmethod
    def from_dict(cls, raw: dict) -> SourceSnapshot:
        if type(raw) is not dict or raw.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("invalid_snapshot")
        data = {k: v for k, v in raw.items() if k != "schema_version"}
        lines = data.pop("content", None)
        if not isinstance(lines, list) or not lines or any(type(x) is not dict for x in lines):
            raise ValueError("invalid_snapshot")
        expected = {key: data.pop(key, None) for key in ("source_id", "snapshot_id", "content_hash")}
        if [x.get("line") for x in lines] != list(range(1, len(lines) + 1)):
            raise ValueError("invalid_snapshot")
        url = data.pop("final_url")
        title = data.pop("title")
        value = cls.create(url, title, [x.get("text") for x in lines],
                           pages=[x.get("page") for x in lines], **data)
        if any(getattr(value, key) != item for key, item in expected.items()):
            raise ValueError("invalid_snapshot")
        return value

    def window(self, start_line: int = 1, line_count: int = 80) -> dict:
        if type(start_line) is not int or type(line_count) is not int or not (
            1 <= start_line <= len(self.content) and 1 <= line_count <= 200
        ):
            raise ValueError("invalid_line_range")
        result = self.to_dict()
        result["content"] = [asdict(x) for x in self.content[start_line - 1:start_line - 1 + line_count]]
        result["total_lines"] = len(self.content)
        result["window_truncated"] = start_line - 1 + line_count < len(self.content)
        return result
