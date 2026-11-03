"""Validated local configuration; credentials are never part of public output."""
from __future__ import annotations
from dataclasses import dataclass, fields
import os
from pathlib import Path
import re
import yaml


@dataclass(frozen=True)
class ResearchConfig:
    enabled: bool = True
    provider: str = "tavily"
    api_key_env: str = "AOAO_WEB_RESEARCH_API_KEY"
    searxng_url: str = "http://127.0.0.1:18080"
    owner_only: bool = True
    max_rounds: int = 3
    max_subquestions: int = 6
    max_queries_per_round: int = 4
    max_search_requests: int = 12
    max_open_pages: int = 8
    max_query_concurrency: int = 4
    max_fetch_concurrency: int = 3
    max_page_chars: int = 30000
    max_html_bytes: int = 2097152
    max_pdf_bytes: int = 8388608
    max_pdf_pages: int = 60
    max_redirects: int = 3
    request_timeout_seconds: int = 15
    research_timeout_seconds: int = 180
    admission_max_tokens: int = 256
    max_model_calls: int = 8
    max_total_input_tokens: int = 48000
    max_total_output_tokens: int = 8000
    max_retry_per_request: int = 1
    retention_days: int = 7
    storage_limit_bytes: int = 268435456
    max_download_bytes: int = 33554432
    require_primary_source: bool = True
    controversial_topics_min_sources: int = 2

    def __post_init__(self):
        for field in fields(self):
            value, default = getattr(self, field.name), field.default
            if type(default) is int:
                minimum = 0 if field.name in {"max_redirects", "max_retry_per_request"} else 1
                if type(value) is not int or not minimum <= value <= default:
                    raise ValueError("invalid_research_config: " + field.name)
            elif type(default) is bool and type(value) is not bool:
                raise ValueError("invalid_research_config: " + field.name)
        if self.provider not in {"tavily", "searxng", "bing_rss"} or self.owner_only is not True:
            raise ValueError("unsupported_research_policy")
        if not isinstance(self.api_key_env, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", self.api_key_env):
            raise ValueError("invalid_credential_reference")
        if self.searxng_url != "http://127.0.0.1:18080":
            raise ValueError("invalid_provider_endpoint")

    @classmethod
    def load(cls, path: Path) -> ResearchConfig:
        if not path.exists():
            return cls()
        if path.stat().st_size > 16384:
            raise ValueError("invalid_research_config")
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if type(raw) is not dict or set(raw) != {"web_research"} or type(raw["web_research"]) is not dict:
            raise ValueError("invalid_research_config")
        try:
            return cls(**raw["web_research"])
        except TypeError as exc:
            raise ValueError("invalid_research_config") from exc

    def credential(self) -> str:
        return os.environ.get(self.api_key_env, "").strip()

    def provider_ready(self) -> bool:
        return self.provider in {"searxng", "bing_rss"} or bool(self.credential())
