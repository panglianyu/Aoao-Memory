"""Google-only local search using the audited public HTTP transport."""
from __future__ import annotations

import asyncio
import re
from typing import Protocol
from urllib.parse import urlencode, urljoin, urlsplit

import aiohttp

from kernel.public_http import EgressResponse, PublicHttpTransport
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.validation import validate_domains, validate_query
from protocols.web_research import canonical_url, source_id

GOOGLE_SEARCH_URL = "https://www.google.com/wml/search"
_ALLOWED_REDIRECT_HOSTS = frozenset({"www.google.com", "consent.google.com"})
_HTML_TYPES = frozenset({"text/html", "application/xhtml+xml", "text/vnd.wap.wml"})
_SEARCH_RESPONSE_LIMIT = 1024 * 1024
_USER_AGENT = "NokiaN72/5.0819.4.0.1 Series60/2.8 Profile/MIDP-2.0 Configuration/CLDC-1.1"


class Parser(Protocol):
    async def parse(self, body: bytes, *, timeout: float) -> list[dict[str, str]]: ...
    async def close(self) -> None: ...


def _locale(query: str) -> tuple[str, str]:
    if any("\u3400" <= char <= "\u9fff" for char in query):
        return "CN", "zh"
    return "US", "en"


def _time_limit(days: int | None) -> str | None:
    if days is None or days > 366:
        return None
    if days <= 1:
        return "d"
    if days <= 7:
        return "w"
    if days <= 31:
        return "m"
    return "y"


def _request_url(query: str, domains: list[str], recency_days: int | None) -> str:
    country, language = _locale(query)
    scoped = query
    if domains:
        sites = " OR ".join(f"site:{domain}" for domain in domains)
        scoped = f"{sites} {query}" if len(domains) == 1 else f"({sites}) {query}"
    params = {
        "q": scoped,
        "sca_esv": "1",
        "filter": "1",
        "start": "0",
        "hl": f"{language}-{country}",
        "lr": f"lang_{language}",
        "cr": f"country{country}",
    }
    if (limit := _time_limit(recency_days)) is not None:
        params["tbs"] = f"qdr:{limit}"
    return GOOGLE_SEARCH_URL + "?" + urlencode(params)


class LocalSearchProvider:
    def __init__(self, config: ResearchConfig, *, transport=None, parser: Parser) -> None:
        if aiohttp.__version__ != "3.13.3":
            raise ValueError("unsupported_http_runtime")
        self.config = config
        self.transport = transport or PublicHttpTransport(
            max_response_bytes=_SEARCH_RESPONSE_LIMIT,
            timeout_seconds=config.request_timeout_seconds,
        )
        self.parser = parser
        self._slots = asyncio.Semaphore(config.max_query_concurrency)

    async def close(self) -> None:
        try:
            await self.transport.close()
        finally:
            await self.parser.close()

    async def search(self, query: str, *, domains: list[str] | None = None,
                     recency_days: int | None = None, budget: ResearchBudget) -> dict:
        query = validate_query(query)
        filters = validate_domains(domains)
        if recency_days is not None and (type(recency_days) is not int or not 1 <= recency_days <= 3650):
            raise ValueError("invalid_recency")
        async with self._slots:
            response = await self._fetch_redirects(_request_url(query, filters, recency_days), budget)
        if _challenge_page(response.body):
            raise ValueError("provider_challenge")
        rows = await self.parser.parse(
            response.body,
            timeout=min(budget.remaining(), self.config.request_timeout_seconds),
        )
        sources, dropped = _normalize(rows, filters, query)
        time_limit = _time_limit(recency_days)
        return {
            "schema_version": 1,
            "query": query,
            "results": list(sources.values())[:8],
            "dropped_sources": dropped,
            "failed_engines": [],
            "filters": {
                "domains": filters,
                "recency_days": recency_days,
                "provider_time_range": time_limit,
                "date_semantics": "provider_time_range_approximate" if time_limit else "provider_publication_or_update",
            },
            "usage": budget.snapshot(),
        }

    async def _fetch_redirects(self, url: str, budget: ResearchBudget) -> EgressResponse:
        seen: set[str] = set()
        headers = {
            "Accept": "text/html, application/xhtml+xml, text/vnd.wap.wml",
            "Accept-Encoding": "gzip, deflate",
            "Cookie": "CONSENT=YES+",
            "User-Agent": _USER_AGENT,
        }
        for hop in range(self.config.max_redirects + 1):
            if url in seen:
                raise ValueError("provider_response_invalid")
            seen.add(url)
            budget.reserve("search")
            reserved = budget.reserve_download(_SEARCH_RESPONSE_LIMIT)
            actual = None
            try:
                response = await asyncio.wait_for(
                    self.transport.fetch(
                        url, method="GET", headers=headers, response_limit=reserved,
                        mime_limits={mime: reserved for mime in _HTML_TYPES},
                    ),
                    min(budget.remaining(), self.config.request_timeout_seconds),
                )
                actual = max(len(response.body), response.wire_bytes)
            except asyncio.TimeoutError as exc:
                raise ValueError("provider_unavailable") from exc
            finally:
                budget.finish_download(reserved, actual)
            if response.status == 200:
                mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                if mime not in _HTML_TYPES:
                    raise ValueError("provider_response_invalid")
                return response
            if response.status == 429:
                raise ValueError("rate_limited")
            if response.status not in {301, 302, 303, 307, 308} or hop >= self.config.max_redirects:
                raise ValueError("provider_response_invalid" if 300 <= response.status < 400 else "provider_error")
            location = response.headers.get("location")
            if not isinstance(location, str) or not location or len(location) > 4096:
                raise ValueError("provider_response_invalid")
            candidate = canonical_url(urljoin(url, location))
            parts = urlsplit(candidate)
            if (parts.scheme != "https"
                    or (parts.hostname or "").lower().rstrip(".") not in _ALLOWED_REDIRECT_HOSTS):
                raise ValueError("provider_response_invalid")
            url = candidate
        raise ValueError("provider_response_invalid")


def _normalize(rows: object, filters: list[str], query: str) -> tuple[dict[str, dict], int]:
    if type(rows) is not list or len(rows) > 8:
        raise ValueError("provider_response_invalid")
    sources: dict[str, dict] = {}
    dropped = 0
    for row in rows:
        try:
            if type(row) is not dict:
                raise ValueError("invalid_source")
            url = canonical_url(row["href"])
            host = (urlsplit(url).hostname or "").lower().rstrip(".")
            if filters and not any(host == domain or host.endswith("." + domain) for domain in filters):
                raise ValueError("domain_filter")
            title, snippet = row.get("title"), row.get("body", "")
            if not isinstance(title, str) or not title.strip() or not isinstance(snippet, str):
                raise ValueError("invalid_source")
        except (KeyError, TypeError, ValueError):
            dropped += 1
            continue
        identifier = source_id(url)
        sources[identifier] = {
            "source_id": identifier,
            "title": title[:512],
            "url": url,
            "snippet": snippet[:1600],
            "source_type": "unknown",
            "published_at": None,
            "provider": "google",
            "originating_queries": [query],
            "domains": filters,
        }
    return sources, dropped


def _challenge_page(body: bytes) -> bool:
    sample = body[:128 * 1024].decode("utf-8", errors="ignore").casefold()
    return bool(
        re.search(r"<title[^>]*>\s*before you continue to google\b", sample)
        or re.search(r"<form[^>]+action=[\"'][^\"']*consent\.google\.com", sample)
        or re.search(r"<(?:form|div)[^>]+(?:id|class)=[\"'][^\"']*(?:captcha-form|g-recaptcha)", sample)
        or ("unusual traffic" in sample and "/sorry/" in sample)
    )
