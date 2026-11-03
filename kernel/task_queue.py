# kernel/task_queue.py
"""
[0.3.21.0 P1 Hermes 式任务循环] 工作列表 (忙时接单回执) 层。

老板 2026-08-03 实证痛点: 阿露忙时再发消息卡住不回。参考 Hermes Agent
busy_input_mode=queue + busy_ack 范式: 忙时老板消息立即回执"排进工作列表",
消息本身不消费, 仍由下一轮 drain 正常批阅 (单飞串行不变, 守不并发红线)。

分工:
- 抢占: 老板 priority 消息入队时 AppBus.submit_input 已自动置
  _master_preempt_flag (软熔断), 本模块不重复造。
- 回执: 主循环忙碌分支每心跳调 busy_watch_once(bus, ledger, pending),
  对排队中的老板 1v1 消息即时回一句 (群聊一律不回, 群里不说心里话)。
- [0.3.22.0 P2] 回执前轻秘书分诊: 闲聊走有界轻答 (kernel/secretary.py),
  任务/拿不准照常排队回执; 轻答失败回退现行为。
- 账本: TaskQueue 记录在排回执与当前任务标签, 供 P3 状态栏复用。

循环导入约束: 只 import kernel.logger / dataclasses; Message 走 duck typing。
调用方: kernel/agent_runtime.py 主循环; 测试见 tests/test_task_queue.py。
"""
import uuid
from dataclasses import dataclass

from kernel.logger import get_logger
# [review H2] 出站留痕复用: 回执也是阿露说过的话 (behavior_log 审计),
# 只动 _outbound_texts_this_turn 与 behavior_log, 不碰熔断账本。
from kernel.brain_outbound import record_outbound_text
# [0.3.22.0 P2] 轻秘书分诊: 忙时老板闲聊有界轻答, 任务照常排队。
# (secretary 顶层只 import kernel.logger, 无循环; classify 内部延迟反引本模块)
from kernel.secretary import classify_busy_message, secretary_config, secretary_reply
# [0.3.22.1 P3] 工作状态栏: 入队即广播队列变化 (work_status 顶层不反引本模块)
from kernel import work_status as _work_status

logger = get_logger(__name__)

# [停止类指令] 命中即回执"马上停" (抢占旗由 submit_input 自动置位, 此处只回话)
STOP_KEYWORDS = (
    "先停", "先别弄", "先别做", "暂停", "停下", "别做了",
    "不用做了", "算了不做", "取消任务",
)


def is_stop_command(text: str) -> bool:
    """停止类指令识别: 子串命中即算 (短指令场景, 宁宽勿漏)。"""
    if not text:
        return False
    return any(kw in text for kw in STOP_KEYWORDS)


def new_task_id() -> str:
    return uuid.uuid4().hex[:8]


@dataclass(frozen=True)
class TaskItem:
    """工作列表条目: 忙碌期间回执过、等待下一轮批阅处理的老板消息。"""

    task_id: str
    desc: str
    source: str
    chat: str
    enqueued_ts: float


class TaskQueue:
    """工作列表账本 (内存态): 回执去重 + 排队条目 + 当前任务标签。

    非线程安全: 只在主事件循环单线程内使用。
    """

    def __init__(self) -> None:
        self._items: list = []
        self._acked_keys: set = set()
        self._current_label: str = ""
        # [0.3.22.0 P2] 轻秘书账本: 已轻答消息键 (批阅快照标注防双答) +
        # 本忙期轻答计数 (max_per_busy_period 防刷屏), flush 同清。
        self._secretary_replied: set = set()
        self.secretary_count: int = 0

    def set_current(self, label: str) -> None:
        self._current_label = label or ""

    def current_label(self) -> str:
        return self._current_label

    def enqueue(self, item: TaskItem) -> int:
        """入队, 返回排队位置 (从 1 起)。"""
        self._items.append(item)
        return len(self._items)

    def pending_items(self) -> tuple:
        return tuple(self._items)

    def mark_acked(self, key: tuple) -> bool:
        """回执去重: 首次 True 并记账, 重复 False。"""
        if key in self._acked_keys:
            return False
        self._acked_keys.add(key)
        return True

    def mark_secretary_replied(self, key: tuple) -> None:
        """[0.3.22.0 P2] 记账: 该消息已被轻秘书轻答 (供批阅快照标注)。"""
        self._secretary_replied.add(key)

    def secretary_replied(self, key: tuple) -> bool:
        return key in self._secretary_replied

    def bump_secretary(self) -> None:
        """轻答成功计数 (调用方 secretary_reply 成功时调)。"""
        self.secretary_count += 1

    def flush(self) -> int:
        """drain 消费排队消息后调用: 清条目/回执记录/轻答计数, 返回清空条目数。

        [审查整改 H2] _secretary_replied 不在此清: 生产时序 flush 先于
        think_batch, 标记须活到快照拼装 (防双答), 由 think_batch 返回后
        调 flush_secretary_marks() 单独清。
        """
        n = len(self._items)
        self._items = []
        self._acked_keys = set()
        self.secretary_count = 0
        return n

    def flush_secretary_marks(self) -> None:
        """[审查整改 H2] think_batch 返回后清轻答标记 (键集每忙期 <= 3, 无膨胀)。"""
        self._secretary_replied = set()


def _msg_ack_key(msg) -> tuple:
    """消息身份键: 同内容/同人/同渠道视为同一条 (回执去重用)。"""
    md = msg.metadata or {}
    return (
        msg.source,
        md.get("chat_name", "") or "",
        md.get("sender", "") or "",
        (msg.content or "")[:80],
    )


def _is_boss_dm(msg) -> bool:
    """老板 1v1 判定: feishu 恒 1v1; wechat 需 is_dm_with_master。群聊 False。"""
    md = msg.metadata or {}
    if not md.get("is_master"):
        return False
    if msg.source == "feishu":
        return True
    return bool(md.get("is_dm_with_master"))


async def busy_watch_once(bus, brain, ledger: TaskQueue, pending: list) -> None:
    """[忙时接单看门狗] 对排队中的老板 1v1 消息即时回执 (不消费消息)。

    调用点二: 主循环忙碌分支 (后台 think 场景) + 工具循环轮间 (批阅 think
    期间主循环阻塞在本方法 await, 忙碌分支不可达, 故轮间也要跑)。
    回执走 bus.send_instruction 直发, 不占出站熔断账本。
    群聊/第三人一律不回 (群里不说心里话)。brain 仅用于出站留痕。
    """
    # [0.3.27.0 阶段5 规格七 收口] 机械忙时回执默认关闭: 进度/回执不进聊天。
    # config/brain.yaml feedback.busy_receipt=true 可临时开启。轻答随同关闭
    # (feedback.secretary 受同一闸门)。入队/状态帧保留 (状态栏 queue 数据源)。
    if brain is not None:
        _cfg = (getattr(brain, "brain_config", None) or {}).get("feedback", {}) or {}
        if not _cfg.get("busy_receipt", False):
            return
    if not pending:
        return
    for msg in pending:
        if not _is_boss_dm(msg):
            continue
        key = _msg_ack_key(msg)
        if not ledger.mark_acked(key):
            continue
        content = msg.content or ""
        if is_stop_command(content):
            reply = "好，手头的马上停，先看你这条。"
        else:
            label = ledger.current_label() or "手头的活"
            # [0.3.22.0 P2] 轻秘书分诊: 闲聊走有界轻答 (不排队不回执),
            # 任务/拿不准照常排队回执。轻答失败回退现行为, 最坏 = 0.3.21.0。
            cfg = secretary_config(brain)
            kind = classify_busy_message(content, chat_max_len=cfg["chat_max_len"])
            if kind == "chat" and ledger.secretary_count < cfg["max_per_busy_period"]:
                try:
                    light = await secretary_reply(brain, bus, ledger, msg)
                except Exception as e:
                    logger.warning(f"[task_queue] 轻秘书异常 (非致命): {e}")
                    light = None
                if light is not None:
                    ledger.mark_secretary_replied(key)
                    # 轻答已送达: 消息不消费, 仍进下轮批阅 (入史防双答)
                    continue
            item = TaskItem(
                task_id=new_task_id(),
                desc=content[:40],
                source=msg.source,
                chat=(msg.metadata or {}).get("chat_name", "") or "",
                enqueued_ts=0.0,
            )
            pos = ledger.enqueue(item)
            # [0.3.22.1 P3] 工作状态栏: 队列变化即广播 (publish 内部去重+兜底)
            try:
                await _work_status.publish(
                    bus, _work_status.WORK_STATE_THINKING, f"忙着「{label}」")
            except Exception:
                pass  # 状态帧永不穿透回执主流程
            reply = (
                f"收到，我正忙着「{label}」，你这条排进工作列表第 {pos} 位，"
                f"弄完手头的马上处理。"
            )
        kwargs = {"text": reply}
        chat = (msg.metadata or {}).get("chat_name", "") or ""
        if chat:
            kwargs["target_user"] = chat
        try:
            logger.info(
                f"[task_queue] 忙时回执 -> {msg.source}({chat or '默认'}): {reply[:40]}"
            )
            await bus.send_instruction(msg.source, "send_text", **kwargs)
            # [review H2] 出站留痕: 阿露要感知自己发过回执 (历史/behavior_log)
            if brain is not None:
                try:
                    record_outbound_text(brain, f"[忙时回执] {reply}", msg.source, chat)
                except Exception:
                    pass  # 测试桩无账本字段时静默, 留痕是增益不是前提
        except Exception as e:
            logger.warning(f"[task_queue] 忙时回执发送失败 ({msg.source}): {e}")
