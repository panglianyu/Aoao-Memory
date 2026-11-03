"""Bounded Bing RSS search through the shared audited public HTTP transport."""
from __future__ import annotations

import asyncio
import html
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

from kernel.public_http import EgressResponse, PublicHttpError, PublicHttpTransport
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.validation import validate_domains, validate_query
from protocols.web_research import canonical_url, source_id

BING_RSS_URL = "https://www.bing.com/search"
_ALLOWED_HOSTS = frozenset({"www.bing.com", "cn.bing.com"})
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_XML_TYPES = frozenset({"application/rss+xml", "application/xml", "text/xml"})
_SEARCH_RESPONSE_LIMIT = 256 * 1024
_MAX_ITEMS = 64


class BingRssProvider:
    def __init__(self, config: ResearchConfig, *, transport=None) -> None:
        if config.provider != "bing_rss":
            raise ValueError("invalid_provider_endpoint")
        self.config = config
        self.transport = transport or PublicHttpTransport(
            max_response_bytes=_SEARCH_RESPONSE_LIMIT,
            timeout_seconds=config.request_timeout_seconds,
        )
        self._slots = asyncio.Semaphore(config.max_query_concurrency)

    async def close(self) -> None:
        await self.transport.close()

    async def search(self, query: str, *, domains: list[str] | None = None,
                     recency_days: int | None = None, budget: ResearchBudget) -> dict:
        query = validate_query(query)
        filters = validate_domains(domains)
        if recency_days is not None:
            raise ValueError("unsupported_recency")
        request_url = BING_RSS_URL + "?" + urlencode({
            "q": _scoped_query(query, filters),
            "format": "rss",
        })
        async with self._slots:
            response = await self._fetch_redirect(request_url, budget)
        sources, dropped = _parse_rss(response.body, filters, query)
        return {
            "schema_version": 1,
            "query": query,
            "results": list(sources.values())[:8],
            "dropped_sources": dropped,
            "failed_engines": [],
            "filters": {
                "domains": filters,
                "recency_days": None,
                "provider_time_range": None,
                "date_semantics": "unavailable",
                "recency_capability": "unsupported",
            },
            "usage": budget.snapshot(),
        }

    async def _fetch_redirect(self, request_url: str, budget: ResearchBudget) -> EgressResponse:
        original_query = parse_qs(urlsplit(request_url).query, keep_blank_values=True)
        url = request_url
        for hop in range(2):
            response = await self._request(url, budget)
            if response.status == 200:
                content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                if content_type not in _XML_TYPES or len(response.body) > _SEARCH_RESPONSE_LIMIT:
                    raise ValueError("provider_response_invalid")
                return response
            if response.status == 429:
                raise ValueError("rate_limited")
            if response.status not in _REDIRECT_STATUSES or hop > 0:
                raise ValueError(
                    "provider_response_invalid" if 300 <= response.status < 400 else "provider_error"
                )
            url = _redirect_url(url, response.headers.get("location"), original_query)
        raise ValueError("provider_response_invalid")

    async def _request(self, url: str, budget: ResearchBudget) -> EgressResponse:
        budget.reserve("search")
        reserved = budget.reserve_download(_SEARCH_RESPONSE_LIMIT)
        actual = None
        try:
            response = await asyncio.wait_for(
                self.transport.fetch(
                    url,
                    method="GET",
                    headers={
                        "Accept": "application/rss+xml, application/xml, text/xml",
                        "Accept-Encoding": "gzip, deflate",
                        "User-Agent": "Aoao-WebResearch/1.0",
                    },
                    response_limit=reserved,
                    mime_limits={mime: reserved for mime in _XML_TYPES},
                ),
                min(budget.remaining(), self.config.request_timeout_seconds),
            )
            actual = max(len(response.body), response.wire_bytes)
            return response
        except (asyncio.TimeoutError, PublicHttpError, OSError) as exc:
            raise ValueError("provider_unavailable") from exc
        finally:
            budget.finish_download(reserved, actual)


def _scoped_query(query: str, domains: list[str]) -> str:
    if not domains:
        return query
    sites = " OR ".join(f"site:{domain}" for domain in domains)
    return f"({sites}) {query}"


def _redirect_url(current_url: str, location: str | None,
                  original_query: dict[str, list[str]]) -> str:
    if not isinstance(location, str) or not location or len(location) > 4096:
        raise ValueError("provider_response_invalid")
    candidate = urljoin(current_url, location)
    try:
        parts = urlsplit(candidate)
        host = (parts.hostname or "").lower().rstrip(".")
        port = parts.port
    except ValueError as exc:
        raise ValueError("provider_response_invalid") from exc
    query = parse_qs(parts.query, keep_blank_values=True)
    if (
        parts.scheme != "https"
        or host not in _ALLOWED_HOSTS
        or port not in {None, 443}
        or parts.username
        or parts.password
        or parts.fragment
        or parts.path != "/search"
        or query != original_query
    ):
        raise ValueError("provider_response_invalid")
    return candidate


def _parse_rss(body: bytes, filters: list[str], query: str) -> tuple[dict[str, dict], int]:
    try:
        root = ElementTree.fromstring(
            body,
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
    except (ParseError, DefusedXmlException, RecursionError, ValueError) as exc:
        raise ValueError("provider_response_invalid") from exc
    channels = list(root) if root.tag == "rss" else []
    if len(channels) != 1 or channels[0].tag != "channel":
        raise ValueError("provider_response_invalid")
    items = channels[0].findall("item")
    if len(items) > _MAX_ITEMS:
        raise ValueError("provider_response_invalid")
    sources: dict[str, dict] = {}
    dropped = 0
    for item in items:
        try:
            if len(item) > 16:
                raise ValueError("invalid_source")
            title = _text(item.findtext("title"), 512)
            snippet = _text(item.findtext("description"), 1600)
            url = canonical_url(_text(item.findtext("link"), 4096))
            host = (urlsplit(url).hostname or "").lower().rstrip(".")
            if not title or (filters and not any(
                    host == domain or host.endswith("." + domain) for domain in filters)):
                raise ValueError("invalid_source")
        except (TypeError, ValueError):
            dropped += 1
            continue
        identifier = source_id(url)
        sources[identifier] = {
            "source_id": identifier,
            "title": title,
            "url": url,
            "snippet": snippet,
            "source_type": "unknown",
            "published_at": None,
            "provider": "bing_rss",
            "originating_queries": [query],
            "domains": filters,
        }
    return sources, dropped


def _text(value: str | None, limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid_source")
    return " ".join(html.unescape(value).split())[:limit]
