"""单个状态会话的内存撤销闸门；只关不重开，不代表跨进程撤销。"""

from threading import Event

from kernel.wechat_rpc.tls import WeChatTransportError


class StatusRevocation:
    def __init__(self) -> None:
        self._closed = Event()

    @property
    def revoked(self) -> bool:
        return self._closed.is_set()

    def revoke(self) -> None:
        self._closed.set()

    def check(self) -> None:
        if self.revoked:
            raise WeChatTransportError("状态会话已关闭")

    async def verify(self) -> None:
        """默认只查内存；受控状态策略可以追加异步登记检查。"""
        self.check()
