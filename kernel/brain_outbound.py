# kernel/brain_outbound.py
"""
[0.3.18.2 Brain 收缩 Phase 3] 出站路由层

[0.3.27.0 阶段6 规格八] 兼容层声明: 本模块函数签名保留 brain 参数仅为
Brain 薄委托兼容点。业务实现读取的"本轮出站账本"(_outbound_text_count/
_outbound_targets_this_turn/_outbound_texts_this_turn) 全部与 TaskRun
共享同一引用 (阶段1 双写), 不新增 Brain 单例可变状态。
。

Brain 的 7 个出站路由方法 (_acquire_outbound_slot 出站熔断 /
_handle_submit_reply 契约代发 / _resolve_reply_channel 渠道解析 /
_claim_structured_outbound 契约单次锁 / _auto_route_group_reply 按组代发 /
_record_outbound_text 出站留痕 / _reply_source_is_gated 闸门判定)
从 brain.py 原样搬入, 统一为模块级函数, brain 实例显式透传。

命名避让: kernel/outbound_router.py 已被文档分发事件路由器 (OutboundRouter,
监听 on_document_generated) 占用, 同名不同物, 本模块按 brain_* 家族惯例
命名 brain_outbound.py。

红线: 只搬家不改逻辑 (行为/文案零变化), Brain 保留同名一行薄委托
(测试夹具与 monkeypatch 引用点不漂移)。旗标收编 (TurnState) 是 Phase 5
的事, 本阶段只搬家, 不抽状态对象。

循环导入约束: 本模块只 import kernel 底层 (logger/message/reply_filters/
tool_result), Brain 类型仅 TYPE_CHECKING 期引用, 运行期走 duck typing。
"""
import asyncio
from typing import TYPE_CHECKING, Optional

from kernel.tool_result import ToolResult
from kernel.message import Message
from kernel.reply_filters import (
    is_interrupted_meta_statement,
    is_narration_statement,
    is_silence_meta_statement,
)
from kernel.logger import get_logger

if TYPE_CHECKING:  # 运行期不反引 brain, 防循环导入
    from kernel.brain import Brain

logger = get_logger(__name__)


async def acquire_outbound_slot(
    brain: "Brain", app_name: str, command: str, kwargs: dict
) -> Optional[str]:
    """[0.1.15.5] 出站熔断 (asyncio.Lock 保护, 治并发 send_text 竞态)。

    返回 None=放行, str=拦截原因。原 _handle_instruct_app 熔断逻辑提取于此,
    加锁串行化检查+计数+append, 防 gather 并发绕过同对话去重。
    """
    # [防御] 测试 fixture 用 object.__new__(Brain) 跳过 __init__, 可能没 _outbound_lock;
    # 生产环境 __init__ 已设, 此分支仅服务测试 fixture。
    lock = getattr(brain, "_outbound_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        brain._outbound_lock = lock
    async with lock:
        # [0.3.26.2 优化3/4] 目标发送能力 + 限流登记:
        #   send_image/file 类 -> allow_send_* 拦截; 任何出站 -> 限流计数。
        #   src/chat 优先 kwargs (per-target 发送), 兜底 brain 当前轮上下文。
        _chat = str(kwargs.get("chat_name") or kwargs.get("target_user")
                    or kwargs.get("target") or "")
        _src = str(kwargs.get("source") or "")
        if not _src:
            _src = str(getattr(brain, "_current_source", "") or "")
        if not _chat:
            _chat = str(getattr(brain, "_current_chat_name", "") or "")
        if _chat and _src in ("wechat", "feishu"):
            if command in ("send_image", "send_img", "image"):
                if not brain._hub_allow_send_kind(_src, _chat, "image"):
                    return "目标策略: 不允许向该对象发送图片"
            elif command in ("send_file", "file"):
                if not brain._hub_allow_send_kind(_src, _chat, "file"):
                    return "目标策略: 不允许向该对象发送文件"
            elif command in ("send_voice", "voice_to"):
                if not brain._hub_allow_send_kind(_src, _chat, "voice"):
                    return "目标策略: 不允许向该对象发送语音"
            brain._hub_note_reply(_src, _chat)
        # [单轮出站熔断 v2] 按类型分计数: 1 文本 + 1 图片/文件/语音 每轮。
        # [Problem 3 0.1.10.3] 文本熔断按 target: 同对话重复拦, 不同对话放行, 总 cap 防 runaway。
        if command in brain._OUTBOUND_TEXT_COMMANDS:
            _tgt = kwargs.get("target_user") or kwargs.get("target") or ""
            _targets = getattr(brain, "_outbound_targets_this_turn", [])
            if not isinstance(_targets, list):
                _targets = brain._outbound_targets_this_turn = []
            if (app_name, _tgt) in _targets:
                # [P0-a 0.2.6.1] 删 H4(0.1.12.2) 群聊3条例外: 一个回复意图 = 一条
                # 发送, 群聊/私聊统一同对话第2条即拦。H4 本意"群里多人各回一句",
                # 但不校验不同意图, qwen3.8-max-preview 把同一回复换措辞连发3遍
                # (2026-07-22 11:12 奶狗事件, data/logs/aoao.log 实证), 用户侧看到
                # 恰好3句刷屏。跨对话各放1条仍保留 (_targets 按 (app,target) 判重,
                # per-target 0.1.10.3), 总 cap 兜底防 runaway。
                logger.warning(
                    f"[Brain] 单轮出站熔断: 本轮已发文本 -> {app_name}({_tgt or '默认'}), "
                    f"拦截重复同对话 send_text"
                )
                return (
                    "本轮已成功发送过一条文本给该对话, 请勿重复发送。"
                    "如需补充内容, 请合并成一条消息或等下一轮再发。"
                )
            _total_limit = getattr(brain, "_OUTBOUND_TEXT_TOTAL_LIMIT", 3)
            if brain._outbound_text_count >= _total_limit:
                logger.warning(
                    f"[Brain] 单轮出站熔断: 本轮已发 {brain._outbound_text_count} 条文本 "
                    f"(跨对话上限 {_total_limit}), 拦截 {command} -> {app_name}"
                )
                return (
                    f"本轮已发送 {brain._outbound_text_count} 条文本消息 (跨对话上限), "
                    f"请等下一轮再发。"
                )
            brain._outbound_text_count += 1
        else:
            if brain._outbound_media_count >= brain._OUTBOUND_MEDIA_LIMIT:
                logger.warning(
                    f"[Brain] 单轮出站熔断: 本轮已发 {brain._outbound_media_count} 条图片/文件, "
                    f"拦截重复 {command} -> {app_name}"
                )
                return (
                    "本轮已成功发送过一条图片/文件消息, 请勿在同一回复里重复发送。"
                    "如需补充内容, 请合并成一条消息或等下一轮再发。"
                )
            brain._outbound_media_count += 1
        brain._outbound_sent_this_turn = True
        # [0.1.10.1] 记录 send_text 目标 (app, target), 供主循环判定老板是否直接收到
        if command == "send_text":
            _tgt = kwargs.get("target_user") or kwargs.get("target") or ""
            if not isinstance(getattr(brain, "_outbound_targets_this_turn", None), list):
                brain._outbound_targets_this_turn = []
            brain._outbound_targets_this_turn.append((app_name, _tgt))
    return None


async def handle_submit_reply(brain: "Brain", args: dict) -> ToolResult:
    """[0.2.8.0 出站收口] submit_reply 单次代发契约: LLM 只产文本, 系统代发一次。

    契约语义: 调一次发一次 -> 工具循环硬终止 (代发经 _acquire_outbound_slot 记
    _outbound_text_count, 复用 P0-b break); 本轮重复调用机械拦截; 沉默 = 不调用
    (合法已读不回)。发送走 send_instruction 统一接口 (flat kwargs, 渠道无关)。
    拒发返回 ToolResult(ok=False) 计入工具熔断, 防模型死循环重试。
    """
    from kernel.web_research.runtime import terminal_text
    if terminal_text(brain) is not None:
        return ToolResult.error("submit_reply", "研究回答由当前会话统一交付，本轮不再接受模型改写或转发。")
    text = str(args.get("text") or "").strip()
    if not text:
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None, text=(
            "失败: submit_reply 缺少非空 text。不想回复就不用调用 (合法已读不回, 无需解释)。"))
    app = brain._resolve_reply_channel()
    if not app:
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None, text=(
            "失败: 当前轮没有可代发的渠道上下文 (console/系统直连不发渠道消息)。"))
    # [0.2.9.1] target 必填: 空 target 静默回退当前对话是"撒谎"温床 (日志实证:
    # 她说"去群里回了"实际发进私聊)。send_text 转换路径会预解析 target 再传入。
    target = str(args.get("target") or "").strip()
    if not target:
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None, text=(
            "失败: submit_reply 缺少 target。请从快照末尾的可用对话列表取精确对话名"
            "作为 target, 不要省略 (省略会导致回复发错对话)。"))
    denied = await brain._acquire_outbound_slot(
        app, "send_text", {"target_user": target, "text": text}
    )
    if denied:
        return ToolResult(
            app="submit_reply", ok=False, kind="text", data=None,
            text=f"{denied} (契约代发拦截: 一回合一条, 勿重复)",
        )
    denied = await brain._claim_structured_outbound(target)
    if denied:
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None, text=denied)
    trigger = Message.user_input(
        content=brain._current_user_input,
        source=brain._current_source,
        chat_name=brain._current_chat_name,
    )
    try:
        result = await brain.bus.send_instruction(
            app, "send_text", trigger_msg=trigger, text=text, target_user=target,
        )
    except Exception as e:
        # [0.3.27.2] 发送异常: 槽位全回滚 (允许本轮补救) + 入补发队列。
        _release_outbound_slot(brain, app, target)
        logger.error(f"[submit_reply] 代发异常 -> {app}({target}): {e}")
        _enqueue_retry(brain, app, target, text)
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None,
                          text=f"代发失败: {e}")
    ok = bool(getattr(result, "ok", True))
    if not ok:
        # [0.3.27.2] 传输失败 (超时/渠道崩溃): 不记成功账。槽位回滚让本轮可重试,
        # 文本入补发队列待渠道恢复后补发。治 2026-08-06 21:21 微信丢回复事故
        # (旧实现失败也记"代发成功", 槽位占死, 回复永久丢失)。
        failure = str(getattr(result, "text", result) or "未知错误")
        _release_outbound_slot(brain, app, target)
        logger.warning(f"[submit_reply] 代发失败 -> {app}({target}): {failure[:80]}")
        _enqueue_retry(brain, app, target, text)
        return ToolResult(app="submit_reply", ok=False, kind="text", data=None,
                          text=f"代发失败: {failure}")
    brain._record_outbound_text(text, app, target)
    logger.info(f"[submit_reply] 代发成功 -> {app}({target or '默认'}): {text[:80]}")
    return ToolResult(app="submit_reply", ok=True, kind="text", data=None, text=(
        f"[submit_reply] 已代发 -> {app}({target or '默认'})。本轮回复完成。"))


def resolve_reply_channel(brain: "Brain") -> str:
    """[0.2.8.0] submit_reply 代发渠道: think_batch 组源优先, 直连活跃源次之, 无则空。

    [0.3.28.1] 渠道必须是 active_apps 里真能 send_text 的 app: web/console
    是"事件广播渠道" (回复走 agent_speak -> WS 广播/控制台打印), 总线上
    从无同名 send app。旧实现对组源无条件放行, 快照提示词又教模型调
    submit_reply -> 永久 "App web 尚未启动", 回复静默丢失 (2026-08-09
    刀塔测试事故实证)。返回空 -> 工具面不挂 submit_reply -> 模型走纯文本
    -> 主循环 agent_speak 兜底到屏幕 (对齐 0.2.22.1 console 处理哲学)。
    """
    app = getattr(brain, "_current_group_src", "") or getattr(brain, "_current_source", "")
    return app if app in getattr(brain.bus, "active_apps", {}) else ""


def _release_outbound_slot(brain: "Brain", app: str, target: str) -> None:
    """[0.3.27.2] 传输失败槽位回滚: targets/文本计数/契约锁/出站旗全复原。

    治 2026-08-06 21:21 旧病: send_text 超时被记成"代发成功"占死槽位,
    模型无法同轮重试, 主循环兜底也因 _outbound_sent_this_turn=True 不补发,
    回复永久丢失。回滚后本轮可重试, 兜底路由恢复裁决资格。
    """
    from kernel.outbound_retry import allows_automatic_send_retry

    if not allows_automatic_send_retry(brain.bus, app):
        # 保留本轮已尝试标记，避免工具或兜底换一个操作 ID 再次发送。
        return
    targets = getattr(brain, "_outbound_targets_this_turn", None)
    if isinstance(targets, list) and (app, target) in targets:
        targets.remove((app, target))
    if getattr(brain, "_outbound_text_count", 0) > 0:
        brain._outbound_text_count -= 1
    claimed = getattr(brain, "_structured_targets_this_turn", None)
    if isinstance(claimed, set):
        claimed.discard(target)
    brain._structured_outbound_this_turn = bool(claimed) if isinstance(claimed, set) else False
    brain._outbound_sent_this_turn = (
        bool(targets) if isinstance(targets, list) else False
    ) or getattr(brain, "_outbound_media_count", 0) > 0


def _enqueue_retry(brain: "Brain", app: str, target: str, text: str) -> None:
    """[0.3.27.2] 传输失败入补发队列 (看门狗重建渠道后由后台任务补发)。

    入队失败不穿透 (非致命): 最坏退回旧行为 (回复丢失), 不影响主链路。
    """
    try:
        from kernel import outbound_retry
        outbound_retry.enqueue_and_ensure_worker(brain.bus, app, target, text)
    except Exception as e:
        logger.warning(f"[submit_reply] 补发队列入队失败(非致命): {e}")


async def claim_structured_outbound(brain: "Brain", target: str) -> Optional[str]:
    """[0.2.8.0] 契约单次锁: 查+占 _structured_outbound_this_turn (与槽位共用锁串行)。

    返回 None=放行并已占位, str=拦截原因。治同批并发两个 submit_reply 双发。
    """
    lock = getattr(brain, "_outbound_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        brain._outbound_lock = lock
    async with lock:
        # [0.2.22.0] per-target 契约锁: 同对话一轮一条, 不同对话各一条 (跨聊回信)。
        claimed = getattr(brain, "_structured_targets_this_turn", None)
        if not isinstance(claimed, set):
            claimed = brain._structured_targets_this_turn = set()
        if target in claimed:
            return (
                f"本轮已通过 submit_reply 回复过对话[{target or '默认'}], 同对话一回合一条, "
                "请勿重复提交。如需回别的对话请用不同 target。"
            )
        claimed.add(target)
        brain._structured_outbound_this_turn = True  # 任一代发即抑制隐式收尾
    return None


async def auto_route_group_reply(brain: "Brain", src: str, chat: str, reply: str,
                                 is_dm_with_master: bool) -> bool:
    """[0.2.9.1] 按组代发: 模型吐纯文本没调工具时, 系统替她发到本组。

    补旧漏洞: think_batch 只把老板组 reply 返回主循环兜底, 非老板组纯文本回复
    死在日志 (群里"自言自语"用户收不到, 11:42 巨人网络群实证)。narration 元话语
    不入群 (防内心独白泄漏), 老板私聊放行 narration (跨对话确认)。本组已出站
    (契约/工具) 不重发; 代发成功记 targets 防主循环二次兜底双发。返回是否发出。
    """
    text = (reply or "").strip()
    if not text:
        return False
    # [0.3.27.1 结构闸] 老板抢占轮收尾 content 绝不自动出站: system_preempt
    # 提示词承诺"只输出一句纯文本结论进日志即可", 系统必须兑现 (2026-08-06
    # 22:38 事故: 收尾 content "这轮回复还没发出就被打断了,下一轮再回群里开火"
    # 被按组代发轰进群)。抢占轮的后续处置交给老板新消息那一轮。
    if getattr(brain, "_preempt_requested", False):
        logger.info(f"[Brain] 按组代发拦截: 老板抢占收尾文本不外发 ({src}/{chat}): {text[:60]}")
        return False
    if is_silence_meta_statement(text):
        return False
    # [0.3.20.0 止血] 系统兜底文案不出门: timeout/error 状态 = 本回复是 LLM 超时/
    # 网络短路兜底 (brain._run_tool_loop), 非阿露真实回复。2026-08-03 事故:
    # "思考超时，模型响应太久，请稍后再试。" 被按组代发轰进微信群。与 silence/
    # narration 同类列入闸门; 主循环兜底 (_compute_fallback_route) 与 idle
    # (_idle_reply_should_send) 同守, 三处锁死全部出站路径。
    status = getattr(brain, "_last_think_status", "ok")
    if status in ("timeout", "error"):
        logger.warning(
            f"[Brain] 按组代发拦截: 系统兜底文案不出门 (status={status}, "
            f"{src}/{chat}): {text[:60]}")
        return False
    targets = getattr(brain, "_outbound_targets_this_turn", None)
    if not isinstance(targets, list):
        targets = brain._outbound_targets_this_turn = []
    if (src, chat) in targets:
        return False
    # [0.3.27.1 词表闸] narration + 中断元话语 (被打断/还没发出/下一轮再回)
    # 群聊一律不发; 老板 1v1 私聊保留 (跨对话出站确认语义)。
    if (is_narration_statement(text) or is_interrupted_meta_statement(text)) \
            and not is_dm_with_master:
        logger.info(f"[Brain] 按组代发跳过群 narration/中断元话语 ({chat}): {text[:60]}")
        return False
    if getattr(brain.bus, "raw_messages", None) is not None:
        text = brain.bus.reply_text(reply, getattr(brain, "_current_turn_id", ""))
    return await _dispatch_group_reply(brain, src, chat, text, targets)


async def _dispatch_group_reply(brain: "Brain", src: str, chat: str, text: str,
                                targets: list) -> bool:
    """发送尝试与真实回执收口；不可补发通道先占位，失败不伪报成功。"""
    from kernel.outbound_retry import allows_automatic_send_retry

    no_retry = not allows_automatic_send_retry(brain.bus, src)
    if no_retry:
        # 不可补发通道在交出执行权前占位；此标志只抑制兜底，不代表成功。
        targets.append((src, chat))
        brain._outbound_sent_this_turn = True
    try:
        if getattr(brain.bus, "raw_messages", None) is not None:
            text = brain.bus.reply_text(text, getattr(brain, "_current_turn_id", ""))
        result = await brain.bus.send_instruction(src, "send_text", text=text, target_user=chat)
    except Exception as e:
        logger.warning(f"[Brain] 按组代发失败 ({src}/{chat}): {e}")
        return False
    if isinstance(result, ToolResult) and result.ok is not True:
        logger.warning("[Brain] 按组代发未获成功回执；不报告成功")
        return False
    if no_retry and not isinstance(result, ToolResult):
        logger.warning("[Brain] 按组代发回执非法；禁止自动补发")
        return False
    if not no_retry:
        targets.append((src, chat))
    brain._outbound_sent_this_turn = True
    logger.info(f"[Brain] [0.2.9.1] 按组代发成功 -> {src}({chat}): {text[:80]}")
    return True


def record_outbound_text(brain: "Brain", text: str, app: str, target: str) -> None:
    """[0.2.8.0] 契约出站留痕: 文本入史 (防 _finalize_reply 误报'思考超时') +
    behavior_log (同 use_<app> 出站路径, 治阿露不知自己发了什么)。"""
    brain._outbound_texts_this_turn.append(text)
    try:
        from kernel import behavior_log
        behavior_log.append_behavior(
            "outbound", text, app=app, target_user=target,
            turn_id=getattr(brain, "_current_turn_id", ""),
            **brain._principal_audit_kwargs(),
        )
    except Exception as e:
        logger.warning(f"[Brain] behavior_log 写入失败(非致命): {e}")


def reply_source_is_gated(brain: "Brain") -> bool:
    """[0.2.8.0] 当前回复上下文是否属闸门管辖通道 (组源优先, 直连源次之)。"""
    gate = getattr(brain, "_speak_gate", None)
    if gate is None:
        return False
    src = getattr(brain, "_current_group_src", "") or getattr(brain, "_current_source", "")
    return src in gate.gated_sources
