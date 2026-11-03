"""显式加入的插件生命周期协调；不改变旧插件，不创建业务资源或授权。"""

import asyncio
import math
from dataclasses import dataclass
from typing import Protocol

from protocols.standard_plug import AoaoPlug


class AppLifecycleError(ValueError):
    """固定错误；不回显插件异常或配置。"""


class AppRegistry(Protocol):
    available_apps: dict[str, AoaoPlug]
    active_apps: dict[str, AoaoPlug]

    def is_manually_disconnected(self, name: str) -> bool: ...


@dataclass(eq=False)
class _Connection:
    name: str
    app: AoaoPlug
    connecting: asyncio.Task
    stopping: bool = False
    cancel_requested: bool = False
    closing: asyncio.Task | None = None


async def _wait_owned(task: asyncio.Task) -> None:
    """调用者取消不能遗弃已拥有的清理；底层失败优先，成功后仍传回取消。"""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    task.result()
    if cancelled:
        raise asyncio.CancelledError


class CoordinatedAppLifecycle:
    """同一事件循环内使用；注册、撤下与在途记录更新不跨 await。"""

    def __init__(self, registry: AppRegistry) -> None:
        self._registry = registry
        self._connections: dict[str, _Connection] = {}

    @property
    def _available(self) -> dict[str, AoaoPlug]:
        return self._registry.available_apps

    @property
    def _active(self) -> dict[str, AoaoPlug]:
        return self._registry.active_apps

    def owns(self, name: str) -> bool:
        return name in self._connections

    def busy(self, name: str) -> bool:
        item = self._connections.get(name)
        return item is not None and (item.stopping or self._active.get(name) is not item.app)

    async def connect(self, name: str, app: AoaoPlug, *, timeout: float) -> bool:
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise AppLifecycleError("插件连接超时预算非法")
        if name in self._connections or name in self._active:
            return False
        deadline = asyncio.get_running_loop().time() + timeout
        item = _Connection(name, app, asyncio.create_task(app.on_connect()))
        self._connections[name] = item
        try:
            # wait 只等待，不因超时或调用者取消重复 cancel 原始连接；统一走关闭门。
            done, _ = await asyncio.wait({item.connecting}, timeout=timeout)
            if not done or asyncio.get_running_loop().time() > deadline:
                raise asyncio.TimeoutError
            result = item.connecting.result()
            if (
                result is False or item.stopping or self._available.get(name) is not app
                or name in self._active
                or self._registry.is_manually_disconnected(name)
            ):
                await _wait_owned(self._start_close(item))
                return False
            self._active[name] = app
            app.is_connected = True
            return True
        except BaseException as error:
            await _wait_owned(self._start_close(item))
            if not isinstance(error, Exception) or isinstance(error, asyncio.TimeoutError):
                raise
            raise AppLifecycleError("插件连接未完成；未注册为运行中") from None

    async def disconnect(self, name: str) -> bool:
        item = self._connections.get(name)
        if item is None:
            return False
        await _wait_owned(self._start_close(item, retry=True))
        return name not in self._active

    def _start_close(self, item: _Connection, *, retry: bool = False) -> asyncio.Task:
        item.stopping = True
        if self._active.get(item.name) is item.app:
            del self._active[item.name]
        if not item.connecting.done() and not item.cancel_requested:
            item.cancel_requested = True
            item.connecting.cancel()
        if item.closing is None or retry and item.closing.done():
            item.closing = asyncio.create_task(self._finish_close(item))
        return item.closing

    async def _finish_close(self, item: _Connection) -> None:
        await asyncio.gather(item.connecting, return_exceptions=True)
        try:
            if await item.app.on_disconnect() is False:
                raise AppLifecycleError("插件关闭未完成；仍禁止重新连接")
        except Exception:
            raise AppLifecycleError("插件关闭未完成；仍禁止重新连接") from None
        if self._active.get(item.name) is item.app:
            del self._active[item.name]
        item.app.is_connected = False
        if self._connections.get(item.name) is item:
            del self._connections[item.name]
