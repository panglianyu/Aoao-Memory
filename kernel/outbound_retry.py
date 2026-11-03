# kernel/outbound_retry.py
"""[0.3.27.2] 出站补发兜底队列: send_text 传输失败 -> 入队 -> 后台补发。

2026-08-06 21:21 事故: wxautox4 子进程卡死致 wechat.send_text 超时 (15s),
已生成的回复永久丢失 (老板微信没收到回)。看门狗 ~12s 后重建守护进程渠道
恢复, 但回复没有补发机制。本模块补上: 任何 app 的 send_text 传输失败
(ok=False/异常, 由 brain_outbound.handle_submit_reply 判定入队) 进补发队列,
后台任务按间隔经 bus.send_instruction 通用接口重发 (能力通用不耦合红线),
成功移除 / 超次数或超期放弃。策略拦截类失败不入队 (那是合法拦截不是传输故障)。

设计:
- 队列懒挂载 bus.outbound_retry (boot 零改动);
- worker 后台任务按需拉起 (入队时 ensure), 队列空自动退出, 再入队再拉起;
- PendingOutbound 不可变, 状态流转一律 replace 新实例 (防隐藏副作用)。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from typing import Any, List

from kernel.logger import get_logger

logger = get_logger(__name__)

_MAX_ATTEMPTS = 3            # 补发尝试上限 (含首次补发)
_RETRY_INTERVAL_SEC = 8.0    # 相邻两次补发间隔
_EXPIRE_SEC = 180.0          # 入队超期即弃 (太迟的回复不再有意义, 防错位刷屏)
_SEND_TIMEOUT_SEC = 20.0     # 单次补发等待上限 (app_bus 内部另有指令超时)
_WORKER_POLL_SEC = 2.0       # worker 轮询到期项粒度
_OFFLINE_POLL_SEC = 30.0     # [0.3.28.0] 渠道离线挂起项的复查间隔
_OFFLINE_NOTIFIED: set = set()  # [0.3.28.0] (app,target) 离线挂只告警一次防刷屏


@dataclass(frozen=True)
class PendingOutbound:
    """补发条目不可变快照: app + 目标对话 + 文本 + 重试簿记。"""

    app: str
    target: str
    text: str
    attempts: int = 0
    created_ts: float = 0.0
    next_retry_ts: float = 0.0


class OutboundRetryQueue:
    """内存补发队列 (asyncio 单事件循环访问, append/重建列表天然原子)。"""

    def __init__(self) -> None:
        self._items: List[PendingOutbound] = []

    def enqueue(self, app: str, target: str, text: str) -> PendingOutbound:
        now = time.time()
        item = PendingOutbound(
            app=app, target=target, text=text,
            attempts=0, created_ts=now, next_retry_ts=now + _RETRY_INTERVAL_SEC,
        )
        self._items.append(item)
        return item

    def pending_count(self) -> int:
        return len(self._items)

    def pop_due(self, now: float) -> List[PendingOutbound]:
        """摘出所有到期条目, 队列内移除; 未到期原样保留。"""
        due = [it for it in self._items if it.next_retry_ts <= now]
        if due:
            self._items = [it for it in self._items if it.next_retry_ts > now]
        return due

    def requeue(self, item: PendingOutbound) -> None:
        """失败回排: attempts+1 + 顺延重试时刻 (新实例, 不改原条目)。"""
        self._items.append(replace(
            item, attempts=item.attempts + 1,
            next_retry_ts=time.time() + _RETRY_INTERVAL_SEC,
        ))

    def suspend(self, item: PendingOutbound) -> None:
        """[0.3.28.0] 离线挂起: 原样回排不计 attempts, 只顺延复查时刻。
        渠道恢复 (app 重新上线) 后自动续发; 超期检查仍生效 (太迟的回复自然失效)。"""
        self._items.append(replace(item, next_retry_ts=time.time() + _OFFLINE_POLL_SEC))


def get_queue(bus: Any) -> OutboundRetryQueue:
    """懒挂载补发队列到 bus (测试/无队列环境自动创建)。"""
    q = getattr(bus, "outbound_retry", None)
    if not isinstance(q, OutboundRetryQueue):
        q = OutboundRetryQueue()
        bus.outbound_retry = q
    return q


def _send_succeeded(result: Any) -> bool:
    """补发是否真送达: ok 位 + 文案无'失败' (与 agent_runtime 判定同形)。"""
    if not bool(getattr(result, "ok", True)):
        return False
    return "失败" not in str(getattr(result, "text", None) or "")


async def _try_send(bus: Any, item: PendingOutbound) -> bool:
    try:
        result = await asyncio.wait_for(
            bus.send_instruction(
                item.app, "send_text", text=item.text, target_user=item.target),
            timeout=_SEND_TIMEOUT_SEC,
        )
    except Exception as e:
        logger.warning(f"[出站补发] 发送异常 ({item.app}/{item.target}): {e}")
        return False
    return _send_succeeded(result)


def allows_automatic_send_retry(bus: Any, app: str) -> bool:
    """通道可明确禁止盲补发；缺省保留旧渠道行为。"""
    if getattr(bus, "raw_messages", None) is not None:
        return False
    registered = getattr(bus, "active_apps", {}).get(app)
    if registered is None:
        registered = getattr(bus, "available_apps", {}).get(app)
    manifest = getattr(registered, "manifest", {})
    return not isinstance(manifest, dict) or manifest.get("automatic_send_retry") is not False


async def _worker_tick(bus: Any) -> None:
    """处理一轮所有到期条目 (供 worker 循环与测试共用)。"""
    q = get_queue(bus)
    now = time.time()
    for item in q.pop_due(now):
        if not allows_automatic_send_retry(bus, item.app):
            logger.warning("[出站补发] 通道禁止自动重发；未执行旧队列条目")
            continue
        if now - item.created_ts > _EXPIRE_SEC:
            logger.warning(
                f"[出站补发] 超期放弃 ({item.app}/{item.target}): {item.text[:60]}")
            continue
        # [0.3.28.1] 渠道永不可达: app 名不在总线注册表 (如 'web' -- web 控制台
        # 是事件广播渠道, 从无 send app) -> 直接弃件, 不得永久挂起占队列
        # (2026-08-09 实证: 挂起条目等一个永不上线的渠道)。注册表缺字段
        # (测试/最小 bus) 保持旧挂起语义不误伤。
        registry = getattr(bus, "available_apps", None)
        if registry is not None and item.app not in registry:
            logger.warning(
                f"[出站补发] 渠道不可达, 弃件 ({item.app}/{item.target}): "
                f"总线无此 app, 永不送达: {item.text[:60]}")
            continue
        # [0.3.28.0] 渠道离线 (如微信被老板手动断开) 不空转重试: 挂起不计次,
        # app 重新上线后自动续发。老板的断开是权威决定, 补发不得诱导重连。
        if item.app not in (getattr(bus, "active_apps", None) or {}):
            key = (item.app, item.target)
            if key not in _OFFLINE_NOTIFIED:
                logger.info(
                    f"[出站补发] 渠道离线, 挂起条目待渠道恢复 "
                    f"({item.app}/{item.target}): {item.text[:60]}")
                _OFFLINE_NOTIFIED.add(key)
            q.suspend(item)
            continue
        if await _try_send(bus, item):
            logger.info(
                f"[出站补发] 补发成功 (第 {item.attempts + 1} 次) -> "
                f"{item.app}({item.target}): {item.text[:60]}")
            _OFFLINE_NOTIFIED.discard((item.app, item.target))
            continue
        if item.attempts + 1 >= _MAX_ATTEMPTS:
            logger.warning(
                f"[出站补发] 重试 {item.attempts + 1} 次仍失败, 放弃 "
                f"({item.app}/{item.target}): {item.text[:60]}")
            continue
        q.requeue(item)


# 后台任务强引用集 (create_task 弱引用, 不持即被 GC)
_BACKGROUND_TASKS: set = set()


async def _worker(bus: Any) -> None:
    """后台补发循环: 队列空即退出 (再入队由 enqueue_and_ensure_worker 重拉)。"""
    q = get_queue(bus)
    while q.pending_count() > 0:
        await _worker_tick(bus)
        if q.pending_count() > 0:
            await asyncio.sleep(_WORKER_POLL_SEC)


def enqueue_and_ensure_worker(bus: Any, app: str, target: str, text: str) -> None:
    """入队 + 确保后台补发任务在跑 (幂等: 任务存活不重复拉)。"""
    if not allows_automatic_send_retry(bus, app):
        return
    q = get_queue(bus)
    q.enqueue(app, target, text)
    logger.info(f"[出站补发] 已入队 -> {app}({target}): {text[:60]}")
    task = getattr(bus, "_outbound_retry_task", None)
    if task is not None and not task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("[出站补发] 无事件循环, 条目仅入队不拉后台任务")
        return
    new_task = loop.create_task(_worker(bus))
    bus._outbound_retry_task = new_task
    _BACKGROUND_TASKS.add(new_task)
    new_task.add_done_callback(_BACKGROUND_TASKS.discard)
