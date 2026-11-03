"""Pure, reusable validation for public research command values."""
from __future__ import annotations

import re

from protocols.web_research import ID_PATTERN


def validate_query(query: str) -> str:
    if not isinstance(query, str) or not query.strip() or len(query) > 1024:
        raise ValueError("invalid_query")
    if re.search(r"[\x00-\x1f\x7f]|(?:sk-|tvly-)[A-Za-z0-9_-]{12,}|-----BEGIN .*PRIVATE KEY", query):
        raise ValueError("sensitive_query")
    return " ".join(query.split())


def validate_domains(domains: list[str] | None) -> list[str]:
    if domains is None:
        return []
    if type(domains) is not list or len(domains) > 20:
        raise ValueError("invalid_domains")
    result = []
    for domain in domains:
        if not isinstance(domain, str) or len(domain) > 253:
            raise ValueError("invalid_domains")
        try:
            normalized = domain.encode("idna").decode("ascii").lower().rstrip(".")
        except UnicodeError as exc:
            raise ValueError("invalid_domains") from exc
        if not re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}", normalized):
            raise ValueError("invalid_domains")
        if normalized not in result:
            result.append(normalized)
    return result


def validate_research_id(research_id: str) -> str:
    if not isinstance(research_id, str) or not re.fullmatch(r"research_[0-9a-f]{32}", research_id):
        raise ValueError("invalid_artifact")
    return research_id


def validate_source_id(value: str) -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value) or not value.startswith("src_"):
        raise ValueError("invalid_artifact")
    return value


def validate_snapshot_id(value: str) -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value) or not value.startswith("snap_"):
        raise ValueError("invalid_artifact")
    return value
