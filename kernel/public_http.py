"""Public HTTP transport shared by browser and research adapters."""
from __future__ import annotations

import asyncio
import ipaddress
import socket
import zlib
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit

import aiohttp

from kernel.workspace_events import validate_browser_url

Resolver = Callable[[str, int], Awaitable[Iterable[str]]]


async def resolve_host_addresses(host: str, port: int) -> tuple[str, ...]:
    rows = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return tuple(sorted({str(row[4][0]).split("%", 1)[0] for row in rows}))


class PublicHttpError(ValueError):
    """请求未通过出口策略。"""


def _public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return bool(
        address.is_global
        and not address.is_multicast
        and not address.is_reserved
        and not getattr(address, "ipv4_mapped", None)
        and not getattr(address, "sixtofour", None)
        and not getattr(address, "teredo", None)
    )


def _target(url: str) -> str:
    try:
        value = validate_browser_url(url)
        parsed = urlsplit(value)
        if parsed.port not in {None, 80 if parsed.scheme == "http" else 443}:
            raise ValueError("仅允许 HTTP 80 / HTTPS 443 端口")
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError:
            address = None
        if address is not None and not _public_address(str(address)):
            raise ValueError("目标不是公网单播地址")
        return value
    except ValueError as exc:
        raise PublicHttpError(str(exc)) from exc


class _PublicResolver(aiohttp.abc.AbstractResolver):
    def __init__(self, resolver: Resolver) -> None:
        self._resolver = resolver

    async def resolve(self, host, port=0, family=socket.AF_INET):
        addresses = tuple(await self._resolver(host, port))
        if not addresses or any(not _public_address(ip) for ip in addresses):
            raise OSError("DNS 包含非公网地址")
        return [
            dict(
                hostname=host,
                host=ip,
                port=port,
                family=socket.AF_INET6 if ":" in ip else socket.AF_INET,
                proto=socket.IPPROTO_TCP,
                flags=socket.AI_NUMERICHOST,
            )
            for ip in addresses
        ]

    async def close(self):
        pass


@dataclass(frozen=True, slots=True)
class EgressResponse:
    status: int
    headers: dict[str, str]
    body: bytes
    wire_bytes: int = 0


_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    }
)


def _headers(values: Mapping[str, str]) -> dict[str, str]:
    connection = values.get("connection", values.get("Connection", ""))
    removed = _HOP_HEADERS | {item.strip().lower() for item in connection.split(",")}
    return {k.lower(): v for k, v in values.items() if k.lower() not in removed}


def _decoded_body(body: bytes, encoding: str, limit: int) -> bytes:
    """CDP fulfill 接收解码后的正文；不允许压缩炸弹绕过响应配额。"""
    if not body or encoding in {"", "identity"}:
        return body
    if encoding not in {"gzip", "deflate"}:
        raise PublicHttpError("网页响应压缩格式未开放")
    decoder = zlib.decompressobj(31 if encoding == "gzip" else 15)
    try:
        decoded = decoder.decompress(body, limit + 1)
    except zlib.error as exc:
        raise PublicHttpError("网页响应压缩数据损坏") from exc
    if len(decoded) > limit or decoder.unconsumed_tail:
        raise PublicHttpError("网页响应解压后超过大小限制")
    if not decoder.eof or decoder.unused_data:
        raise PublicHttpError("网页响应压缩数据不完整或包含额外流")
    return decoded


class PublicHttpTransport:
    """Connection-time address checks, bounded decoding and explicit redirects."""

    def __init__(self, *, resolver: Resolver = resolve_host_addresses,
                 max_response_bytes: int = 8 * 1024 * 1024,
                 timeout_seconds: float = 30) -> None:
        self._resolver = resolver
        self._max_response_bytes = max_response_bytes
        self._timeout_seconds = timeout_seconds
        self._session: aiohttp.ClientSession | None = None
        self._slots = asyncio.Semaphore(8)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            await session.close()

    async def fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        body: bytes | None = None,
        response_limit: int | None = None,
        mime_limits: Mapping[str, int] | None = None,
    ) -> EgressResponse:
        value = _target(url)
        if method not in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}:
            raise PublicHttpError("HTTP 方法未开放")
        if body is not None and len(body) > 256 * 1024:
            raise PublicHttpError("请求体超过 256 KiB")
        try:
            operation = (self._fetch(value, method, headers or {}, body)
                         if response_limit is None and mime_limits is None else
                         self._fetch(value, method, headers or {}, body, response_limit, mime_limits))
            return await asyncio.wait_for(operation, self._timeout_seconds)
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError, ValueError) as exc:
            if isinstance(exc, PublicHttpError):
                raise
            raise PublicHttpError("网页出口请求失败或超时") from exc

    async def _fetch(self, url, method, headers, body, response_limit=None, mime_limits=None) -> EgressResponse:
        limit = min(self._max_response_bytes, response_limit if response_limit is not None else self._max_response_bytes)
        if type(limit) is not int or limit < 1:
            raise PublicHttpError("网页响应预算不足")
        async with self._slots:
            if self._session is None:
                connector = aiohttp.TCPConnector(
                    resolver=_PublicResolver(self._resolver),
                    use_dns_cache=False,
                    force_close=True,
                    limit=8,
                )
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    cookie_jar=aiohttp.DummyCookieJar(),
                    trust_env=False,
                    auto_decompress=False,
                    timeout=aiohttp.ClientTimeout(total=self._timeout_seconds),
                )
                # 固定 aiohttp 3.13.3 没有公开的重试开关；写入响应不确定时
                # 禁止其将 PUT/DELETE 自动重放。升级依赖时必须保留断线回归。
                self._session._retry_connection = False
            # 每一跳返回浏览器，再次通过 Fetch.requestPaused；禁止库自动跳转。
            request_headers = _headers(headers)
            # 只协商具备有界解码能力的编码，不转发 Chromium 的 br/zstd 声明。
            request_headers["accept-encoding"] = "gzip, deflate"
            async with self._session.request(
                method,
                url,
                headers=request_headers,
                data=body,
                allow_redirects=False,
            ) as response:
                has_body = method != "HEAD" and response.status not in {204, 304}
                mime = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                if mime_limits is not None and 200 <= response.status < 300:
                    if mime not in mime_limits:
                        raise PublicHttpError("unsupported_content")
                    limit = min(limit, mime_limits[mime])
                if (
                    has_body
                    and (response.content_length or 0) > limit
                ):
                    raise PublicHttpError("网页响应超过大小限制")
                chunks = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    chunks.extend(chunk)
                    if len(chunks) > limit:
                        raise PublicHttpError("网页响应超过大小限制")
                response_headers = _headers(response.headers)
                encoding = response_headers.pop("content-encoding", "").strip().lower()
                decoded = _decoded_body(
                    bytes(chunks), encoding, limit
                )
                cookies = response.headers.getall("Set-Cookie", [])
                if cookies:
                    response_headers["set-cookie"] = "\n".join(cookies)
                return EgressResponse(response.status, response_headers, decoded, len(chunks))
