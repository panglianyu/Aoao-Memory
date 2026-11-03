"""Search through the fixed loopback SearXNG service."""
from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp

from kernel.public_http import EgressResponse
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.validation import validate_domains, validate_query
from protocols.web_research import canonical_url, source_id

SEARXNG_BASE_URL = "http://127.0.0.1:18080"
_SEARCH_RESPONSE_LIMIT = 1024 * 1024
_ALLOWED_QUERY_KEYS = frozenset({"q", "format", "categories", "language", "safesearch", "time_range"})


class LoopbackSearxngTransport:
    """A narrowly scoped HTTP client that cannot be redirected off the local service."""

    def __init__(self, base_url: str, *, max_response_bytes: int = _SEARCH_RESPONSE_LIMIT,
                 timeout_seconds: float = 15) -> None:
        if base_url != SEARXNG_BASE_URL:
            raise ValueError("invalid_provider_endpoint")
        self.base_url = base_url
        self.max_response_bytes = max_response_bytes
        self.timeout_seconds = timeout_seconds
        self._session: aiohttp.ClientSession | None = None

    async def close(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            await session.close()

    async def fetch(self, url: str, *, response_limit: int) -> EgressResponse:
        parts = urlsplit(url)
        keys = set(parse_qs(parts.query, keep_blank_values=True))
        if (parts.scheme != "http" or parts.netloc != "127.0.0.1:18080"
                or parts.path != "/search" or parts.fragment or not keys <= _ALLOWED_QUERY_KEYS):
            raise ValueError("invalid_provider_endpoint")
        limit = min(response_limit, self.max_response_bytes)
        if type(limit) is not int or limit < 1:
            raise ValueError("budget_exhausted")
        if self._session is None:
            self._session = aiohttp.ClientSession(
                cookie_jar=aiohttp.DummyCookieJar(), trust_env=False,
                connector=aiohttp.TCPConnector(force_close=True, limit=4),
                timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
            )
        try:
            async with self._session.get(
                    url, headers={"Accept": "application/json", "Accept-Encoding": "identity"},
                    allow_redirects=False) as response:
                if (response.content_length or 0) > limit:
                    raise ValueError("response_too_large")
                body = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body) > limit:
                        raise ValueError("response_too_large")
                headers = {key.lower(): value for key, value in response.headers.items()}
                if headers.get("content-encoding", "").lower() not in {"", "identity"}:
                    raise ValueError("provider_response_invalid")
                return EgressResponse(response.status, headers, bytes(body), len(body))
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            raise ValueError("provider_unavailable") from exc


class SearxngProvider:
    def __init__(self, config: ResearchConfig, *, transport=None) -> None:
        if config.provider != "searxng" or config.searxng_url != SEARXNG_BASE_URL:
            raise ValueError("invalid_provider_endpoint")
        self.config = config
        self.transport = transport or LoopbackSearxngTransport(
            config.searxng_url, timeout_seconds=config.request_timeout_seconds,
        )
        self._slots = asyncio.Semaphore(config.max_query_concurrency)

    async def close(self) -> None:
        await self.transport.close()

    async def search(self, query: str, *, domains: list[str] | None = None,
                     recency_days: int | None = None, budget: ResearchBudget) -> dict:
        query = validate_query(query)
        filters = validate_domains(domains)
        if recency_days is not None and (type(recency_days) is not int or not 1 <= recency_days <= 3650):
            raise ValueError("invalid_recency")
        time_range = _time_range(recency_days)
        params = {
            "q": _scoped_query(query, filters),
            "format": "json",
            "categories": "general",
            "language": "all",
            "safesearch": "1",
        }
        if time_range is not None:
            params["time_range"] = time_range
        request_url = self.config.searxng_url + "/search?" + urlencode(params)
        async with self._slots:
            response = await self._request(request_url, budget)
        raw = _decode_response(response)
        failures = _engine_failures(raw.get("unresponsive_engines", []))
        rows = raw.get("results")
        if type(rows) is not list or len(rows) > 100:
            raise ValueError("provider_response_invalid")
        sources, dropped = _parse_sources(rows, filters, query)
        return {
            "schema_version": 1,
            "query": query,
            "results": list(sources.values())[:8],
            "dropped_sources": dropped,
            "failed_engines": failures,
            "filters": {
                "domains": filters,
                "recency_days": recency_days,
                "provider_time_range": time_range,
                "date_semantics": "provider_time_range_approximate" if recency_days is not None
                else "provider_publication_or_update",
            },
            "usage": budget.snapshot(),
        }

    async def _request(self, request_url: str, budget: ResearchBudget) -> EgressResponse:
        for attempt in range(self.config.max_retry_per_request + 1):
            budget.reserve("search")
            reserved = budget.reserve_download(_SEARCH_RESPONSE_LIMIT)
            actual = None
            try:
                response = await asyncio.wait_for(
                    self.transport.fetch(request_url, response_limit=reserved),
                    min(budget.remaining(), self.config.request_timeout_seconds),
                )
                actual = max(len(response.body), getattr(response, "wire_bytes", 0))
            except asyncio.TimeoutError as exc:
                if attempt == self.config.max_retry_per_request:
                    raise ValueError("provider_unavailable") from exc
                continue
            except ValueError as exc:
                if str(exc) != "provider_unavailable" or attempt == self.config.max_retry_per_request:
                    raise
                continue
            finally:
                budget.finish_download(reserved, actual)
            if response.status == 200:
                return response
            if response.status == 429:
                raise ValueError("rate_limited")
            if response.status in {500, 502, 503, 504} and attempt < self.config.max_retry_per_request:
                continue
            raise ValueError("provider_response_invalid" if 300 <= response.status < 400 else "provider_error")
        raise ValueError("provider_unavailable")


def _decode_response(response: EgressResponse) -> dict:
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise ValueError("provider_response_invalid")
    try:
        raw = json.loads(response.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("provider_response_invalid") from exc
    if type(raw) is not dict:
        raise ValueError("provider_response_invalid")
    return raw


def _time_range(recency_days: int | None) -> str | None:
    if recency_days is None or recency_days >= 366:
        return None
    if recency_days <= 1:
        return "day"
    if recency_days <= 30:
        return "month"
    return "year"


def _scoped_query(query: str, domains: list[str]) -> str:
    if not domains:
        return query
    sites = " OR ".join(f"site:{domain}" for domain in domains)
    return f"({sites}) {query}"


def _engine_failures(rows: object) -> list[dict[str, str]]:
    if type(rows) is not list or len(rows) > 50:
        raise ValueError("provider_response_invalid")
    failures = []
    for row in rows:
        if (not isinstance(row, (list, tuple)) or len(row) < 2
                or not isinstance(row[0], str) or not isinstance(row[1], str)):
            raise ValueError("provider_response_invalid")
        failures.append({"engine": row[0][:80], "reason": row[1][:160]})
    return failures


def _parse_sources(rows: list, filters: list[str], query: str) -> tuple[dict, int]:
    sources: dict[str, dict] = {}
    dropped = 0
    for row in rows:
        try:
            if type(row) is not dict:
                raise ValueError("invalid_source")
            url = canonical_url(row["url"])
            host = (urlsplit(url).hostname or "").lower().rstrip(".")
            if filters and not any(host == domain or host.endswith("." + domain) for domain in filters):
                raise ValueError("domain_filter")
            title = row.get("title")
            snippet = row.get("content", "")
            if not isinstance(title, str) or not isinstance(snippet, str):
                raise ValueError("invalid_source")
        except (KeyError, TypeError, ValueError):
            dropped += 1
            continue
        identifier = source_id(url)
        published = row.get("publishedDate", row.get("published_date"))
        sources[identifier] = {
            "source_id": identifier,
            "title": title[:512],
            "url": url,
            "snippet": snippet[:1600],
            "source_type": "unknown",
            "published_at": published if isinstance(published, str) else None,
            "provider": "searxng",
            "originating_queries": [query],
            "domains": filters,
        }
    return sources, dropped
