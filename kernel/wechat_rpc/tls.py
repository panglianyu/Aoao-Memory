"""专用 CA 验证、双方叶证书钉住和固定数字地址策略。"""

import asyncio
import hashlib
import hmac
import ipaddress
import re
import ssl
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from protocols.sandbox_rpc import SandboxProtocolError, validate_sandbox_endpoint


class WeChatTransportError(SandboxProtocolError):
    """连接失败；公开消息不包含证书、密钥、请求正文或底层错误文本。"""


def private_ipv4(value: str) -> str:
    try:
        if not isinstance(value, str):
            raise ValueError
        address = ipaddress.IPv4Address(value)
    except (ValueError, TypeError):
        raise WeChatTransportError("RPC 地址必须是固定的私网或回环 IPv4") from None
    networks = ("127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    if not any(address in ipaddress.IPv4Network(net) for net in networks):
        raise WeChatTransportError("RPC 地址必须是固定的私网或回环 IPv4")
    return str(address)


def endpoint_address(endpoint: str) -> tuple[str, int]:
    parsed = urlsplit(validate_sandbox_endpoint(endpoint))
    host = private_ipv4(parsed.hostname)
    if parsed.port is None or not 1 <= parsed.port <= 65535:
        raise WeChatTransportError("RPC 端口非法")
    return host, parsed.port


@dataclass(frozen=True)
class PeerIdentity:
    ca_file: Path
    cert_file: Path
    key_file: Path
    peer_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.peer_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.peer_sha256
        ):
            raise WeChatTransportError("必须明确配置对端叶证书 SHA-256")
        for name in ("ca_file", "cert_file", "key_file"):
            try:
                path = Path(getattr(self, name))
            except TypeError:
                raise WeChatTransportError("TLS 文件必须是本机绝对路径") from None
            if (
                not path.is_absolute()
                or str(path).startswith("\\\\")
                or ".." in path.parts
            ):
                raise WeChatTransportError("TLS 文件必须是本机绝对路径")
            object.__setattr__(self, name, path)

    def _context(self, *, server: bool) -> ssl.SSLContext:
        try:
            for path in (self.ca_file, self.cert_file, self.key_file):
                if (
                    path.resolve(strict=True) != path
                    or not path.is_file()
                    or not 0 < path.stat().st_size <= 65536
                ):
                    raise ValueError
            context = ssl.SSLContext(
                ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT
            )
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=str(self.ca_file))
            # 显式空密码阻止 OpenSSL 对加密私钥弹出交互式口令提示。
            context.load_cert_chain(
                str(self.cert_file), str(self.key_file), password=""
            )
            if server:
                context.num_tickets = 0
            return context
        except (OSError, ValueError):
            raise WeChatTransportError("TLS 身份文件不可用") from None

    async def context(self, *, server: bool = False) -> ssl.SSLContext:
        return await asyncio.to_thread(self._context, server=server)

    def verify_peer(self, ssl_object: ssl.SSLObject | None) -> None:
        certificate = ssl_object.getpeercert(binary_form=True) if ssl_object else None
        if not certificate or not hmac.compare_digest(
            hashlib.sha256(certificate).hexdigest(), self.peer_sha256
        ):
            raise WeChatTransportError("TLS 对端身份不匹配")
