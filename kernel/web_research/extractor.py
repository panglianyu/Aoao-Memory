"""Pure HTML extraction. Network access is exclusively owned by WebFetcher."""
from __future__ import annotations
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from protocols.web_research import SourceSnapshot, canonical_url


def bounded_paragraphs(paragraphs, limit: int = 30000) -> tuple[list[str], bool]:
    result: list[str] = []
    used = 0
    truncated = False
    for item in paragraphs:
        text = " ".join(str(item).split())
        if not text:
            continue
        if used + len(text) > limit or len(result) >= 3000:
            if used < limit and len(result) < 3000:
                result.append(text[:limit - used])
            truncated = True
            break
        result.append(text)
        used += len(text)
    if not result:
        raise ValueError("parse_failed")
    return result, truncated


def extract_html(url: str, body: bytes, *, max_chars: int = 30000,
                 redirect_chain: tuple[str, ...] = ()) -> SourceSnapshot:
    if len(body) > 2097152:
        raise ValueError("response_too_large")
    soup = BeautifulSoup(body, "lxml")
    if sum(1 for _ in soup.descendants) > 50000:
        raise ValueError("response_too_large")
    title = soup.title.get_text(" ", strip=True)[:512] if soup.title else url
    for element in soup.select("script,style,nav,footer,header,aside,form,iframe,noscript,svg"):
        element.decompose()
    metadata: dict = {}
    status = "ok"
    try:
        import trafilatura
    except ImportError as exc:
        raise ValueError("extractor_unconfigured") from exc
    extracted = trafilatura.extract(str(soup), url=url, output_format="json", with_metadata=True,
                                    include_comments=False, include_links=True, include_tables=True)
    text = ""
    if extracted:
        metadata = json.loads(extracted)
        text = metadata.get("text") or ""
    if not text.strip():
        main = soup.select_one("article,main,[role=main]")
        if main is None:
            raise ValueError("needs_browser")
        text = "\n".join(p.get_text(" ", strip=True) for p in main.select("h1,h2,h3,p,li,tr"))
        status = "limited"
    paragraphs, truncated = bounded_paragraphs(text.splitlines(), max_chars)
    links = _extract_links(soup, url)
    # Date extraction is heuristic; never substitute fetched_at for publication.
    date = metadata.get("date")
    author = metadata.get("author")
    return SourceSnapshot.create(url, str(metadata.get("title") or title)[:512], paragraphs,
                                 published_at=str(date)[:80] if date else None,
                                 date_origin="html_metadata_heuristic" if date else "unknown",
                                 author=str(author)[:200] if author else None,
                                 extractor_version="trafilatura-" + trafilatura.__version__,
                                 extraction_status=status, truncated=truncated, links=tuple(links),
                                 redirect_chain=redirect_chain)


def _extract_links(soup, url: str) -> list[str]:
    links: list[str] = []
    for link in soup.select("article a[href], main a[href]")[:100]:
        try:
            safe = canonical_url(urljoin(url, str(link["href"])))
        except ValueError:
            continue
        if safe not in links:
            links.append(safe)
    return links
