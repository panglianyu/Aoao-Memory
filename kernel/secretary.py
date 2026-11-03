# kernel/secretary.py
"""
[0.3.22.0 P2 Hermes 式任务循环] 轻秘书层 (忙时闲聊有界并发应答)。

老板 2026-08-03 拍板方向 (STATE.md 0.3.21.0 挂起项): 忙时老板发闲聊
("在吗" / "忙啥呢") 不该收到机械的"排进工作列表"回执, 该轻答; 任务照常排队。

契约:
1. classify_busy_message 三分类: stop (停止词) / task (任务标记/带图/长文) /
   chat (短且无任务标记的纯闲聊)。拿不准一律 task (回退 0.3.21.0 排队回执)。
2. 轻答有界 (守老板"单飞串行"红线): SECRETARY_SLOT 单槽位信号量 -- 忙时最多
   1 个并发轻答 LLM 调用; 调用不传 tools 参数 (物理无法调工具); 单次超时不
   重试; 超次 (max_per_busy_period, 调用方查账本) 回退。
3. secretary_reply 任何失败 (禁用/抢占中/槽位占用/超时/异常/空回复/发送失败)
   -> 返 None, 调用方回退排队回执。最坏 = 0.3.21.0 现状, 零回归。
4. 轻答成功: send_text 直发 (同 ACK 路径, 不占熔断账本) + behavior_log 留痕 +
   入史 (user+assistant 成对落老板会话桶, 下一轮批阅可见, 防双答)。
5. 反馈永不穿透: 一切异常只落 warning, 不破坏任务主流程。

循环导入约束: 顶层只 import kernel.logger; is_stop_command 走函数内延迟导入
(task_queue 顶层导入本模块, 反向顶层导入会成环); behavior_log 同延迟导入。
调用方: kernel/task_queue.py busy_watch_once; 测试见 tests/test_secretary.py。
"""
import asyncio
import re
from typing import Optional

from kernel.logger import get_logger
from kernel.model_profile import request_options

# [审查整改 H1] 信封剥离: 生产渠道消息带 "[外部感知:...]" 信封头 (仅前缀就超
# chat_max_len 字符), 分类/轻答/入史前必须剥信封, 否则长度闸门恒判 task,
# 轻秘书生产全灭。与 brain._ENVELOPE_RE 同款 (避循环导入, speak_gate 同款复制例)。
_ENVELOPE_RE = re.compile(r"^\[外部感知:[^\]]*\]\s*")

logger = get_logger(__name__)

# [轻答默认配置] config/brain.yaml secretary 段缺键回退值 (一键关: enabled=false)。
SECRETARY_DEFAULTS = {
    "enabled": True,           # false = 整体回退 0.3.21.0 排队回执行为
    "timeout": 12.0,           # 轻答 LLM 单次超时 (秒), 不重试 (快速失败)
    "max_tokens": 140,         # 轻答上限 (一句话)
    "max_per_busy_period": 3,  # 单次忙碌期轻答上限 (调用方查账本), 防刷屏
    "chat_max_len": 20,        # 超过此长度一律按任务排队 (安全方向)
}

# [有界并发核心] 单槽位信号量: 忙时最多 1 个轻答 LLM 调用在飞。
# 与"按群并发执行任务"是两回事: 轻答无工具/无副作用/单槽/短超时/可一键关。
SECRETARY_SLOT = asyncio.Semaphore(1)

# [任务标记] 命中即判任务 -> 排队回执 (0.3.21.0 行为)。宁宽勿漏: 误判闲聊为
# 任务只是回退现行为; 误判任务为闲聊也有消息不消费兜底 (仍进下轮批阅执行)。
TASK_MARKERS = (
    "帮我", "帮忙", "给我", "请把", "搜一下", "搜索", "查一下", "查询",
    "下载", "生成", "整理", "翻译", "打开", "截屏", "截图", "画一", "画个",
    "放进", "放到", "发到", "改成", "安装", "卸载", "总结", "分析",
    "处理一下", "提醒我", "定时", "写一个", "写一篇", "写份", "写个",
)


def secretary_config(brain) -> dict:
    """读 config/brain.yaml secretary 段, 缺段/缺键回退默认。未知键忽略。"""
    cfg = dict(SECRETARY_DEFAULTS)
    src = getattr(brain, "brain_config", None) or {}
    section = src.get("secretary") if isinstance(src, dict) else None
    if isinstance(section, dict):
        for key in SECRETARY_DEFAULTS:
            if key in section:
                cfg[key] = section[key]
    return cfg


def classify_busy_message(text: str, chat_max_len: int = 20) -> str:
    """忙时老板消息三分类: "stop" / "task" / "chat"。拿不准一律 task。

    - stop: 停止词 (复用 task_queue.is_stop_command);
    - task: 空文本 / 带图 ([LOCAL_IMAGE:) / 长度超限 / 任务标记命中;
    - chat: 短且无任务标记的纯闲聊。
    """
    if text is None or not text.strip():
        return "task"
    # [审查整改 H1] 先剥信封再判: 生产消息 "[外部感知:...]" 前缀恒超长按任务
    text = _ENVELOPE_RE.sub("", text, count=1)
    if not text.strip():
        return "task"
    from kernel.task_queue import is_stop_command  # 延迟导入防循环
    if is_stop_command(text):
        return "stop"
    if "[LOCAL_IMAGE:" in text:
        return "task"
    if len(text) > chat_max_len:
        return "task"
    if any(marker in text for marker in TASK_MARKERS):
        return "task"
    return "chat"


def _build_secretary_prompt(brain, current_label: str, content: str) -> list:
    """轻答 messages: 人设一句 + 忙碌上下文 + 边界铁律; user 只带老板原文。"""
    character = getattr(brain, "character_config", None) or {}
    name = character.get("name", "阿露")
    identity = character.get("identity", "数字生命")
    label = current_label or "手头的活"
    system_prompt = (
        f"你是{name}，{identity}。你现在正忙着「{label}」，老板刚发来一条消息，"
        "请用一句话轻轻回应：自然、简短、贴近你的语气。"
        "铁律：不许承诺做任何事，不许提工具与任务细节，不许编造进展，"
        "绝对禁止表情符号。"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": content},
    ]


async def secretary_reply(brain, bus, ledger, msg) -> Optional[str]:
    """[忙时轻答] 对老板 1v1 闲聊做一次有界 LLM 轻答, 成功返回回复文本。

    任何环节失败返 None, 调用方 (busy_watch_once) 回退排队回执。
    老板闸门由调用方保证 (busy_watch_once 已过滤非老板 1v1)。
    ledger 用于取当前任务标签 (入提示) 与成功计数 (bump_secretary)。
    """
    cfg = secretary_config(brain)
    if not cfg["enabled"]:
        return None
    if getattr(bus, "_master_preempt_flag", False):
        logger.debug("[secretary] 老板已抢占待停, 跳过轻答")
        return None
    if SECRETARY_SLOT.locked():
        logger.debug("[secretary] 轻答槽位占用, 回退排队回执")
        return None
    # [审查整改 H1] 先剥信封: LLM/入史只喂纯用户话语, 不带 "[外部感知:...]" 头
    content = _ENVELOPE_RE.sub("", getattr(msg, "content", "") or "", count=1).strip()
    if not content:
        return None
    # [审查整改 H3] 真持单槽: locked() 预检到此无 await, 单线程循环无 TOCTOU;
    # 不阻塞等槽 (会挂死看门狗), 占用即回退语义不变, 各早退路径显式 release。
    await SECRETARY_SLOT.acquire()
    label = ""
    if ledger is not None:
        try:
            label = ledger.current_label() or ""
        except Exception:
            label = ""

    messages = _build_secretary_prompt(brain, label, content)
    timeout = float(cfg["timeout"])
    try:
        resp = await asyncio.wait_for(
            # 红线: 不传 tools/tool_choice -> 轻答物理无法调工具。
            brain.client.chat.completions.create(
                model=getattr(brain, "model_name", "") or "",
                messages=messages,
                max_tokens=int(cfg["max_tokens"]),
                **request_options(getattr(brain, "model_name", "")),
            ),
            timeout=timeout,
        )
        raw = resp.choices[0].message.content or ""
    except asyncio.TimeoutError:
        logger.warning(f"[secretary] 轻答超时 ({timeout}s), 回退排队回执")
        SECRETARY_SLOT.release()
        return None
    except Exception as exc:
        logger.warning(
            "[secretary] 轻答 LLM 调用失败, 回退排队回执 (type=%s)",
            type(exc).__name__)
        SECRETARY_SLOT.release()
        return None

    sanitizer = getattr(brain, "_sanitize_output", None)
    reply = sanitizer(raw) if callable(sanitizer) else raw.strip()
    reply = (reply or "").strip()
    if not reply:
        SECRETARY_SLOT.release()
        return None

    source = getattr(msg, "source", "") or ""
    chat = (getattr(msg, "metadata", None) or {}).get("chat_name", "") or ""
    kwargs = {"text": reply}
    if chat:
        kwargs["target_user"] = chat
    try:
        await bus.send_instruction(source, "send_text", **kwargs)
    except Exception as e:
        logger.warning(f"[secretary] 轻答发送失败 ({source}), 不落史: {e}")
        SECRETARY_SLOT.release()
        return None

    # [出站留痕] 只走 behavior_log 审计, 不碰 _outbound_texts_this_turn:
    # 轻答入史由下方精确落老板会话桶, 避免 _finalize_reply 按当前忙碌对话
    # 桶 flush 出站账本时造成重复历史行。
    try:
        from kernel import behavior_log  # 延迟导入 (测试可 monkeypatch)
        audit = {}
        audit_fn = getattr(brain, "_principal_audit_kwargs", None)
        if callable(audit_fn):
            try:
                audit = audit_fn() or {}
            except Exception:
                audit = {}
        behavior_log.append_behavior(
            "outbound", f"[忙时轻答] {reply}", app=source, target_user=chat,
            turn_id=getattr(brain, "_current_turn_id", ""), **audit,
        )
    except Exception as e:
        logger.warning(f"[secretary] behavior_log 留痕失败(非致命): {e}")

    # [入史防双答] user+assistant 成对落老板会话桶 (与批阅快照同桶),
    # 该闲聊消息仍会进下轮批阅, 阿露看到自己已轻答过 -> 不再重复回应。
    history = getattr(brain, "chat_history", None)
    lock = getattr(brain, "_history_lock", None)
    if isinstance(history, dict) and chat:
        async def _append_pair() -> None:
            bucket = history.setdefault(chat, [])
            bucket.append({"role": "user", "content": content})
            bucket.append({"role": "assistant", "content": f"[忙时轻答] {reply}"})
        try:
            if lock is not None:
                async with lock:
                    await _append_pair()
            else:
                await _append_pair()
        except Exception as e:
            logger.warning(f"[secretary] 轻答入史失败(非致命): {e}")

    if ledger is not None:
        bump = getattr(ledger, "bump_secretary", None)
        if callable(bump):
            try:
                bump()
            except Exception:
                pass
    logger.info(f"[secretary] 忙时轻答 -> {source}({chat or '默认'}): {reply[:40]}")
    SECRETARY_SLOT.release()
    return reply
