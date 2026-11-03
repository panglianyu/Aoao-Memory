"""Tavily search through a fixed authenticated endpoint; never fetch vendor answers."""
from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
import json
from urllib.parse import urlsplit

from kernel.public_http import PublicHttpError, PublicHttpTransport
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.validation import validate_domains, validate_query
from protocols.web_research import canonical_url, source_id


class TavilyProvider:
    def __init__(self, config: ResearchConfig, *, transport=None):
        self.config = config
        self.transport = transport or PublicHttpTransport(max_response_bytes=1024 * 1024,
                                                         timeout_seconds=config.request_timeout_seconds)
        self._slots = asyncio.Semaphore(config.max_query_concurrency)

    async def close(self) -> None:
        await self.transport.close()

    async def search(self, query: str, *, domains: list[str] | None = None,
                     recency_days: int | None = None, budget: ResearchBudget) -> dict:
        query = validate_query(query)
        filters = validate_domains(domains)
        if recency_days is not None and (type(recency_days) is not int or not 1 <= recency_days <= 3650):
            raise ValueError("invalid_recency")
        credential = self.config.credential()
        if not credential:
            raise ValueError("provider_unconfigured")
        request = {"query": query, "search_depth": "basic", "auto_parameters": False,
                   "include_answer": False, "include_raw_content": False, "max_results": 8,
                   "include_domains": filters, "include_usage": True}
        if recency_days is not None:
            request["start_date"] = (datetime.now(timezone.utc) - timedelta(days=recency_days)).date().isoformat()
        async with self._slots:
            response = await self._request(request, credential, budget)
        try:
            raw = json.loads(response.body)
            rows = raw["results"]
            if not isinstance(rows, list) or len(rows) > 20:
                raise ValueError("provider_response_invalid")
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("provider_response_invalid") from exc
        sources, dropped = _parse_sources(rows, filters, query)
        usage = raw.get("usage")
        credits = usage.get("credits") if isinstance(usage, dict) else None
        if type(credits) is int and 0 <= credits <= 100:
            budget.usage["credits"] += credits
        return {"schema_version": 1, "query": query, "results": list(sources.values()),
                "dropped_sources": dropped, "filters": {"domains": filters, "recency_days": recency_days,
                "date_semantics": "provider_publication_or_update"}, "usage": budget.snapshot()}

    async def _request(self, request: dict, credential: str, budget: ResearchBudget):
        for attempt in range(self.config.max_retry_per_request + 1):
            budget.reserve("search")
            reserved = budget.reserve_download(1024 * 1024)
            actual = None
            try:
                response = await asyncio.wait_for(self.transport.fetch(
                    "https://api.tavily.com/search", method="POST",
                    headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json"},
                    body=json.dumps(request).encode(), response_limit=reserved), min(budget.remaining(), self.config.request_timeout_seconds))
                actual = max(len(response.body), getattr(response, "wire_bytes", 0))
            except (PublicHttpError, asyncio.TimeoutError) as exc:
                if attempt == self.config.max_retry_per_request:
                    raise ValueError("network_error") from exc
                continue
            finally:
                budget.finish_download(reserved, actual)
            if response.status in {401, 403}:
                raise ValueError("provider_auth_failed")
            if response.status == 200:
                return response
            if response.status not in {429, 500, 502, 503, 504} or attempt == self.config.max_retry_per_request:
                raise ValueError("rate_limited" if response.status == 429 else "provider_error")
            try:
                delay = max(0.0, float(response.headers.get("retry-after", "1")))
            except ValueError:
                delay = 1.0
            if not delay < min(10, budget.remaining()):
                raise ValueError("rate_limited")
            await asyncio.sleep(delay)
        raise ValueError("provider_error")


def _parse_sources(rows: list, filters: list[str], query: str) -> tuple[dict, int]:
    sources: dict[str, dict] = {}
    dropped = 0
    for row in rows:
        try:
            url = canonical_url(row["url"])
            host = (urlsplit(url).hostname or "").lower().rstrip(".")
            if filters and not any(host == d or host.endswith("." + d) for d in filters):
                raise ValueError("domain_filter")
            if not isinstance(row.get("title"), str) or not isinstance(row.get("content", ""), str):
                raise ValueError("invalid_source")
        except (KeyError, TypeError, ValueError):
            dropped += 1
            continue
        identifier = source_id(url)
        sources[identifier] = {"source_id": identifier, "title": row["title"][:512],
            "url": url, "snippet": row.get("content", "")[:1600], "source_type": "unknown",
            "published_at": row.get("published_date") if isinstance(row.get("published_date"), str) else None,
            "provider": "tavily", "originating_queries": [query], "domains": filters}
    return sources, dropped
