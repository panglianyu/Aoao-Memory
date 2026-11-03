"""Read-only page retrieval; each redirect is a new validated public target."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

from kernel.public_http import PublicHttpError, PublicHttpTransport, _target
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig


@dataclass(frozen=True)
class Download:
    url: str
    body: bytes
    mime_type: str
    redirects: tuple[str, ...]


class WebFetcher:
    def __init__(self, config: ResearchConfig, *, transport=None):
        self.config = config
        self.transport = transport or PublicHttpTransport(
            max_response_bytes=config.max_pdf_bytes,
            timeout_seconds=config.request_timeout_seconds,
        )
        self._slots = asyncio.Semaphore(config.max_fetch_concurrency)

    async def close(self) -> None:
        await self.transport.close()

    async def download(self, url: str, budget: ResearchBudget,
                       domains: tuple[str, ...] = ()) -> Download:
        budget.reserve("open")
        async with self._slots:
            return await asyncio.wait_for(self._download(url, budget, domains),
                                           min(budget.remaining(), self.config.request_timeout_seconds))

    async def _download(self, url: str, budget: ResearchBudget, domains: tuple[str, ...]) -> Download:
        chain: list[str] = []
        for hop in range(self.config.max_redirects + 1):
            budget.remaining()
            try:
                target = _target(url)
                host = (urlsplit(target).hostname or "").lower().rstrip(".")
                if domains and not any(host == d or host.endswith("." + d) for d in domains):
                    raise ValueError("domain_filter")
            except ValueError as exc:
                raise ValueError("blocked_url") from exc
            if target in chain:
                raise ValueError("redirect_loop")
            reserved = budget.reserve_download(self.config.max_pdf_bytes)
            actual = None
            try:
                response = await self.transport.fetch(target, method="GET", headers={
                    "Accept": "text/html,application/pdf,text/plain;q=0.8",
                    "User-Agent": "Aoao-WebResearch/1.0",
                }, response_limit=reserved, mime_limits={"text/html": self.config.max_html_bytes,
                    "application/xhtml+xml": self.config.max_html_bytes, "text/plain": self.config.max_html_bytes,
                    "application/pdf": self.config.max_pdf_bytes})
                actual = max(len(response.body), getattr(response, "wire_bytes", 0))
            except PublicHttpError as exc:
                raise ValueError("network_error") from exc
            finally:
                budget.finish_download(reserved, actual)
            if response.status in {301, 302, 303, 307, 308}:
                if hop == self.config.max_redirects or not response.headers.get("location"):
                    raise ValueError("redirect_limit")
                chain.append(target)
                url = urljoin(target, response.headers["location"])
                continue
            return self._response_download(target, response, chain)
        raise ValueError("redirect_limit")

    def _response_download(self, target: str, response, chain: list[str]) -> Download:
        if response.status != 200:
            raise ValueError({429: "rate_limited", 401: "access_denied", 403: "access_denied"}.get(response.status, "http_error"))
        mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if mime not in {"text/html", "application/xhtml+xml", "text/plain", "application/pdf"}:
            raise ValueError("unsupported_content")
        if len(response.body) > (self.config.max_pdf_bytes if mime == "application/pdf" else self.config.max_html_bytes):
            raise ValueError("response_too_large")
        if not response.body:
            raise ValueError("parse_failed")
        if mime == "application/pdf" and not response.body.startswith(b"%PDF-"):
            raise ValueError("unsupported_content")
        return Download(target, response.body, mime, tuple(chain))
