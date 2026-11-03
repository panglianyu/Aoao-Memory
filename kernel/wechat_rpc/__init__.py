"""微信 Guest 的受限 HTTPS 协议准备层；不注册生产插件或启动微信。"""

from typing import TYPE_CHECKING, Any

from kernel.wechat_rpc.tls import PeerIdentity, WeChatTransportError

if TYPE_CHECKING:
    from kernel.wechat_rpc.client import WeChatRpcClient
    from kernel.wechat_rpc.server import WeChatRpcServer

__all__ = ["PeerIdentity", "WeChatRpcClient", "WeChatRpcServer", "WeChatTransportError"]


def __getattr__(name: str) -> Any:
    # 只读证书预检不应因尚未部署 aiohttp 而无法运行；原公开导入方式保持兼容。
    if name == "WeChatRpcClient":
        from kernel.wechat_rpc.client import WeChatRpcClient

        return WeChatRpcClient
    if name == "WeChatRpcServer":
        from kernel.wechat_rpc.server import WeChatRpcServer

        return WeChatRpcServer
    raise AttributeError(name)
