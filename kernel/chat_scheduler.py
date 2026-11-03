# kernel/chat_scheduler.py
"""[阶段2 0.1.12.1] 通用消息调度器 (时序层): per-chat 攒批 + pending 回灌 + 老板强插。

路线B 轻量接入: flush 后逐条 bus.submit_input(保 priority/is_master 语义), 主循环
drain_batch 不变。治三类问题(全日志实证 aoao.log):
  1. "每条都回碎回": per-chat 攒批窗口, 同 chat 短时间内多条攒成一批合并 think。
  2. "H3 时间序错乱": 旧 wechat _process_chat_messages 按 sender 拆信封提前提交, 同 chat
     A->B->A 被拆成 A(两句合并)+B 丢时间序; 本层在 chat 级攒批保原始时间序, flush 后
     batch 保序进 think_batch, 副产品修复 H3。
  3. "跨群插队": 跨 chat 串行由 think_batch 按 chat_name 分组独立 think 保证
     (已有, brain.py:1088); 老板强插由 bus _priority_event + _master_preempt_flag 保证
     (已有, app_bus.py:138)。

规则:
  - chat_key = (source, chat_name) 复合键, 跨渠道不碰撞。
  - per-chat 攒批窗口: 群聊 GROUP_WINDOW(5s, 0.2.7.3 对齐老板设计); 老板 is_master=True 自适应 [0.1.12.5]:
    首条 MASTER_WINDOW(0.8s 近秒回) / 连发第2条起 ADAPTIVE_WINDOW(2.5s) /
    纠正信号(说错了/改成/算了) CORRECTION_WINDOW(4.0s), 覆盖连发/纠正合并回一次。
  - flush 后逐条 bus.submit_input(保 priority/is_master 语义, 主循环 drain_batch 照常取)。
  - 老板强插: is_master 消息 add 时立即 bus._priority_event.set() + _master_preempt_flag=True
    (唤醒主循环秒回 + 抢占软熔断, 不等 flush)。
  - pending 回灌: MessageReviewer(0.1.12.2) 判定暂缓的消息 requeue 回缓冲, 短窗口
    PENDING_REWINDOW(1.0s) 重审; 防饿死 MAX_DEFER(3 轮) 强制 deliver。

语义分组/权重判定交 MessageReviewer(0.1.12.2), 本层只管时序不做语义判断
(capability-generic-not-coupled: 通用层不焊任一 app)。
"""
import asyncio
import os
import weakref
from collections import OrderedDict
from typing import List, Optional, Tuple

import yaml

from kernel.logger import get_logger
from kernel.message import Message
from kernel.speak_gate import SpeakGate
from kernel.text_signals import is_correction_signal

logger = get_logger(__name__)

# 项目根 (本文件在 kernel/, 上跳一层), 读 config/character.yaml 取角色名。
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_character_name() -> str:
    """[0.2.8.3] 读角色名 (抢占预判给闸门用); 缺文件/异常回退 '阿露' (不阻塞收信)。

    [0.3.17.0 P4-b] 活动智能体名优先 (注册表为源), 注册表不可用回退 character.yaml。
    """
    try:
        from kernel.character_registry import get_active
        active = get_active()
        # 内置阿露名走 character.yaml (单一信源); 非内置智能体用注册表名
        if active.name and not active.builtin:
            return active.name
    except Exception as e:
        logger.debug(f"[ChatScheduler] 注册表读取失败, 回退 character.yaml: {e}")
    try:
        with open(os.path.join(_BASE_DIR, "config", "character.yaml"),
                  "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return str(data.get("name", "阿露"))
    except Exception:
        return "阿露"


class ChatScheduler:
    """通用 per-chat 消息调度器 (时序层, 不做语义判断)。

    用法:
      bus.scheduler = ChatScheduler(bus)
      await bus.scheduler.add(msg)              # 渠道插件拆信封后调
      # flush 自动触发 bus.submit_input, 主循环 drain_batch 照常取
      await bus.scheduler.requeue(key, msgs)    # reviewer pending 回灌
    """

    GROUP_WINDOW: float = 5.0       # 群聊攒批窗口(秒) [0.2.7.3] 对齐老板设计: 5秒持续接水, 旧2.5s批次太碎
    MASTER_WINDOW: float = 0.8      # 老板首条攒批窗口(秒, 近秒回)
    ADAPTIVE_WINDOW: float = 2.5    # [0.1.12.5] 老板连发第2条起延长窗口(秒, 覆盖连发合并)
    CORRECTION_WINDOW: float = 4.0  # [0.1.12.5] 老板纠正信号窗口(秒, 覆盖慢纠正合并)
    PENDING_REWINDOW: float = 1.0   # pending 回灌后重审窗口(秒, 短于新消息窗口)
    MAX_DEFER: int = 3              # pending 防饿死上限(轮), 超过强制 deliver

    def __init__(self, bus=None) -> None:
        self._bus = bus
        # per-chat 攒批缓冲: chat_key -> 待 think 的消息列表(保序)
        self._buffers: "OrderedDict[Tuple[str, str], List[Message]]" = OrderedDict()
        # per-chat 窗口定时器: chat_key -> asyncio.TimerHandle
        self._timers: dict = {}
        # per-chat pending 暂缓计数: chat_key -> {msg_id: defer_count}
        self._defer_counts: dict = {}
        self._defer_refs: dict = {}
        self._review_tokens: dict[int, str] = {}
        # 调度器内部锁(保护 _buffers/_timers/_defer_counts)
        self._lock: asyncio.Lock = asyncio.Lock()
        self._closed = False

    @staticmethod
    def _chat_key(msg: Message) -> Tuple[str, str]:
        """复合 chat_key = (source, chat_name), 跨渠道不碰撞。"""
        source = msg.source or ""
        chat = msg.get("chat_name", "") if hasattr(msg, "get") else ""
        return (source, chat)

    @classmethod
    def _select_master_window(cls, master_count: int, is_correction: bool) -> float:
        """[0.1.12.5] 老板攒批窗口选择 (纯逻辑, 可单测)。

        - 纠正信号 -> CORRECTION_WINDOW (覆盖慢纠正, 如"说错了其实是这样")
        - 连发 (master_count >= 1, 缓冲已有老板消息) -> ADAPTIVE_WINDOW
        - 首条 -> MASTER_WINDOW (近秒回)
        master_count = 本条 append 前缓冲里已有老板消息数 (首条=0)。
        时序信号调窗口, 非内容审核 (topic/weight 交 MessageReviewer)。
        """
        if is_correction:
            return cls.CORRECTION_WINDOW
        if master_count >= 1:
            return cls.ADAPTIVE_WINDOW
        return cls.MASTER_WINDOW

    async def add(self, msg: Message) -> None:
        """消息入调度器: 归入对应 chat 攒批缓冲, 启动/重置窗口。

        [0.1.12.5] 老板消息自适应窗口: 首条 MASTER_WINDOW(0.8s 近秒回) /
        连发第2条起 ADAPTIVE_WINDOW(2.5s) / 纠正信号 CORRECTION_WINDOW(4.0s)。
        覆盖老板连发两句 / 纠正"说错了其实是这样"合并回一次 (治 0.8s 窗口拆两次回复)。
        群聊用 GROUP_WINDOW(5s)。is_master 立即触发 bus._priority_event +
        _master_preempt_flag (保秒回+抢占, 不等 flush)。
        """
        if self._closed:
            raise RuntimeError("scheduler closed")
        prepare = getattr(self._bus, "prepare_input", None)
        if callable(prepare):
            msg = await prepare(msg)
        claim = getattr(self._bus, "claim_input_dispatch", None)
        if callable(claim) and not await claim(msg):
            return
        if self._closed:
            raise RuntimeError("scheduler closed after durable save")
        chat_key = self._chat_key(msg)
        is_master = bool(msg.metadata and msg.metadata.get("is_master"))
        async with self._lock:
            if self._closed:
                raise RuntimeError("scheduler closed after durable save")
            buf = self._buffers.setdefault(chat_key, [])
            # [0.1.12.5] append 前数已有老板消息 (首条=0), 据此+纠正信号选自适应窗口
            master_count = sum(
                1 for m in buf if m.metadata and m.metadata.get("is_master")
            )
            is_correction = is_correction_signal(msg.content) if is_master else False
            buf.append(msg)
            window = (
                self._select_master_window(master_count, is_correction)
                if is_master else self.GROUP_WINDOW
            )
            old_timer = self._timers.get(chat_key)
            if old_timer is not None:
                old_timer.cancel()
            loop = asyncio.get_event_loop()
            self._timers[chat_key] = loop.call_later(
                window,
                lambda ck=chat_key: asyncio.create_task(self._flush(ck)),
            )
        # 老板消息立即唤醒主循环秒回 (不等 flush, 保 priority 秒回语义)
        if is_master and self._bus is not None:
            self._bus._priority_event.set()
            # [0.2.8.3] 抢占收紧: 将被闸门潜水的老板消息 (群里跟别人闲聊没喊阿露)
            # 不抢占 -- 抢占只会白白熔断阿露在途 think; 过闸的 (私聊/提及/关键词)
            # 照旧抢占保秒回。预判与闸门同源 (SpeakGate.evaluate), 不另立标准。
            preempt = self._will_preempt(msg)
            if preempt:
                self._bus._master_preempt_flag = True
            mode = (
                "纠正" if is_correction
                else ("连发" if master_count >= 1 else "首条")
            )
            logger.info(
                f"[ChatScheduler] 老板消息入攒批(src={msg.source}, chat={chat_key[1]}, "
                f"模式={mode}, 窗口 {window}s 后 flush); 已唤醒主循环"
                f"{', 抢占旗' if preempt else ', 将潜水不抢占'}"
            )

    def _will_preempt(self, msg: Message) -> bool:
        """[0.2.8.3] 闸门预判: 这条老板消息会不会过闸 (过则值得抢占在途 think)。

        复用 SpeakGate.evaluate 单一决策源 (dm/提及/关键词/非管辖/闸门关 -> 过)。
        闸门与角色名懒加载各一次 (收信热路径不重复读盘); 预判异常放行 (宁可多
        抢占一次也不漏老板秒回)。
        """
        try:
            if getattr(self, "_gate", None) is None:
                self._gate = SpeakGate.load()
            if getattr(self, "_char_name", None) is None:
                self._char_name = _load_character_name()
            md = msg.metadata or {}
            chat = md.get("chat_name", "")
            return self._gate.evaluate(
                [msg], chat, msg.source or "", True, self._char_name
            ).passed
        except Exception as e:
            logger.warning(f"[ChatScheduler] 抢占预判异常, 放行抢占 (保老板秒回): {e}")
            return True

    async def _flush(self, chat_key: Tuple[str, str]) -> None:
        """窗口结束: 该 chat 缓冲逐条 bus.submit_input(保 priority/is_master), 清缓冲。"""
        async with self._lock:
            batch = self._buffers.pop(chat_key, [])
            review_tokens = [self._review_tokens.pop(id(msg), None) for msg in batch]
            self._timers.pop(chat_key, None)
        if not batch:
            return
        if self._bus is None:
            return
        for msg, token in zip(batch, review_tokens):
            if self._closed:
                break
            try:
                if token is not None:
                    await self._bus.submit_reviewed_input(msg, token)
                else:
                    self._bus.submit_input(msg)
            except Exception as e:
                logger.warning(f"[ChatScheduler] flush submit_input 失败: {e}")
        # [0.2.7.3] flush 即"批次就绪"信号: 唤醒主循环立即 drain, 不让批次在队列
        # 干等 HEARTBEAT(5s)。老板设计: 5秒窗口结束阿露立刻统一看统一回, 不吃心跳
        # 拖尾 (旧路径仅 priority 消息唤醒, 群批最坏拖到 窗口+心跳≈10s, 体感"没动静")。
        # 蓄水阶段 (未 flush) 不唤醒, 由 submit_input 的 priority 分支负责老板秒回。
        self._bus._priority_event.set()

    def _remember_defer(self, chat_key: Tuple[str, str], msg: Message) -> None:
        """Keep a defer count only while its message remains alive in this process."""
        refs = self._defer_refs.setdefault(chat_key, {})
        mid = msg.id
        if mid in refs and refs[mid]() is msg:
            return

        def cleanup(_ref):
            counts = self._defer_counts.get(chat_key)
            if counts is not None:
                counts.pop(mid, None)
                if not counts:
                    self._defer_counts.pop(chat_key, None)
            live_refs = self._defer_refs.get(chat_key)
            if live_refs is not None:
                live_refs.pop(mid, None)
                if not live_refs:
                    self._defer_refs.pop(chat_key, None)

        refs[mid] = weakref.ref(msg, cleanup)

    async def _authorize_requeue(self, chat_key: Tuple[str, str], msgs: List[Message],
                                 handoffs: tuple[str, ...] | None) -> bool:
        raw_enabled = getattr(self._bus, "raw_messages", None) is not None
        if raw_enabled:
            from kernel.raw_message_models import RawMessageError
            if handoffs is None or len(handoffs) != len(msgs) or any(
                    self._chat_key(msg) != chat_key for msg in msgs):
                raise RawMessageError("review handoff required")
            await self._bus.authorize_review_requeue(msgs, handoffs)
        return raw_enabled

    async def requeue(self, chat_key: Tuple[str, str], msgs: List[Message],
                      *, handoffs: tuple[str, ...] | None = None) -> None:
        """reviewer pending 消息回灌: 放回该 chat 缓冲, 短窗口重审。

        防饿死: 单条消息 defer_count 超 MAX_DEFER 立即 submit(不攒批), 防低权重消息
        永远 pending 不被回。
        """
        if not msgs:
            return
        if self._closed:
            raise RuntimeError("scheduler closed")
        raw_enabled = await self._authorize_requeue(chat_key, msgs, handoffs)
        immediate = await self._stage_requeue(chat_key, msgs, handoffs, raw_enabled)
        for m, token in immediate:
            if self._closed:
                raise RuntimeError("scheduler closed after review authorization")
            try:
                if token is not None:
                    await self._bus.submit_reviewed_input(m, token)
                else:
                    self._bus.submit_input(m)
            except Exception as e:
                logger.warning(f"[ChatScheduler] requeue immediate submit 失败: {e}")

    async def _stage_requeue(self, chat_key: Tuple[str, str], msgs: List[Message],
                             handoffs: tuple[str, ...] | None,
                             raw_enabled: bool) -> list[tuple[Message, str | None]]:
        immediate: list[tuple[Message, str | None]] = []
        async with self._lock:
            if self._closed:
                raise RuntimeError("scheduler closed after review authorization")
            counts = self._defer_counts.setdefault(chat_key, {})
            buf = self._buffers.setdefault(chat_key, [])
            for index, m in enumerate(msgs):
                token = handoffs[index] if raw_enabled and handoffs is not None else None
                mid = m.id
                cnt = counts.get(mid, 0) + 1
                counts[mid] = cnt
                self._remember_defer(chat_key, m)
                if cnt > self.MAX_DEFER:
                    immediate.append((m, token))
                    counts.pop(mid, None)
                    self._defer_refs.get(chat_key, {}).pop(mid, None)
                    logger.info(
                        f"[ChatScheduler] pending 防饿死强制 deliver: "
                        f"msg={mid[:8]} defer={cnt}"
                    )
                else:
                    buf.append(m)
                    if token is not None:
                        self._review_tokens[id(m)] = token
            # 重置短窗口定时器(若有 pending 回灌到缓冲)
            if buf:
                old_timer = self._timers.get(chat_key)
                if old_timer is not None:
                    old_timer.cancel()
                loop = asyncio.get_event_loop()
                self._timers[chat_key] = loop.call_later(
                    self.PENDING_REWINDOW,
                    lambda ck=chat_key: asyncio.create_task(self._flush(ck)),
                )
        return immediate

    def pending_messages_for_chat(self, chat_key: Tuple[str, str]) -> int:
        """某 chat 当前攒批缓冲里的消息数(调试/日志用)。"""
        buf = self._buffers.get(chat_key)
        return len(buf) if buf else 0

    @property
    def closed(self) -> bool:
        return self._closed

    async def shutdown(self) -> None:
        """关闭: 取消所有定时器, 清缓冲(残留消息丢弃, 主循环已停)。"""
        self._closed = True
        async with self._lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()
            self._buffers.clear()
            self._defer_counts.clear()
            self._defer_refs.clear()
            self._review_tokens.clear()
