# kernel/task_feedback.py
"""
[0.3.21.0 P0 Hermes 式任务循环] 接单 ACK 反馈层。

老板 2026-08-03 实证痛点: 飞书发工具请求后阿露长时间无声, 完成后乱回一堆。
参考 Hermes Agent (Nous Research) busy_ack 范式: 忙时接单即时确认。

契约:
1. 首个含实际工具的批执行前, 老板 1v1 收到一句接单 ACK;
   群聊/console/boot/idle 一律不发 (群里不说心里话)。
2. 渠道解析: think_batch 下 _current_source 恒 "multi", 必须走
   _current_group_src (组真实渠道源), 与 brain_outbound.resolve_reply_channel
   同源逻辑, 治旧 interim 永远回退 feishu 的发错渠道病。
3. 每任务流最多一条 ACK (_task_ack_sent 防抖, _think_impl 入口重置)。
4. ACK 走 bus.send_instruction 直发, 不经 brain_outbound.acquire_outbound_slot,
   不占单轮出站熔断计数 -- ACK 后收尾 send_text 不被误拦。
5. 反馈永不穿透: 发送异常只落 warning, 不破坏任务主流程。

循环导入约束: 只 import kernel.logger, Brain 走 duck typing (TYPE_CHECKING)。
调用方为 kernel/brain.py 薄委托 (_maybe_send_batch_interim), 工具循环每轮
批前触发; 测试见 tests/test_task_feedback.py。
"""
import json
from typing import TYPE_CHECKING, Optional

from kernel.logger import get_logger
# [review H2] 出站留痕复用: ACK 也是阿露说过的话 (behavior_log 审计),
# 该函数只动 _outbound_texts_this_turn 与 behavior_log, 不碰熔断账本。
from kernel.brain_outbound import record_outbound_text

if TYPE_CHECKING:  # 运行期不反引 brain, 防循环导入
    from kernel.brain import Brain

logger = get_logger(__name__)

# [ACK 豁免名单] 纯检索/元动作工具: 整批仅此类工具时不接单
# (她马上就能答, 不需要"去干活"的接单语)。
ACK_SKIP_TOOLS = frozenset({
    "spotlight_search", "smart_recall", "archival_search",
    "memory_recall", "memory_adjust_impression",
    "submit_reply", "boost_interest",
})

# [按 app 接单文案] 禁表情符号 (项目铁律), 阿露语气。
APP_ACK_COPY = {
    "native_controller": "收到，正在调取视觉神经截屏，稍等我一下。",
    "image_generator": "收到，这个要画一会儿，我动手了。",
    "doc_gen": "收到，正在动笔写文档，稍等。",
    "doc_parser": "收到，我去读一下文档，稍等。",
    "local_file_manager": "收到，我去翻翻文件，稍等。",
    "agent_reach": "收到，这个要上网查一下，稍等。",
    "knowledge_base": "收到，我去翻翻知识库，稍等。",
    "image_search": "收到，我去搜图，稍等。",
}

# [通用兜底文案] 老板原话风格: "好滴我这就调用工具去搞"
GENERIC_ACK = "好滴收到，这就调用工具去搞，稍等我一下。"


def resolve_feedback_channel(brain: "Brain") -> str:
    """ACK 渠道解析: think_batch 组源优先, 直连活跃源次之, 无则空。

    与 brain_outbound.resolve_reply_channel 同源: _current_group_src 是
    think_batch 逐组置的真实渠道 (wechat/feishu), _current_source 在批阅
    路径恒 "multi" 不可用。
    """
    app = getattr(brain, "_current_group_src", "") or ""
    if app:
        return app
    src = getattr(brain, "_current_source", "") or ""
    return src if src in getattr(brain.bus, "active_apps", {}) else ""


def is_boss_private_group(brain: "Brain") -> bool:
    """ACK 特权闸门: 仅老板 1v1 享受接单反馈。

    feishu 恒老板 1v1 (备用保险通道, 无群聊); wechat 需本组
    is_dm_with_master (think_batch 逐组置 _current_group_is_dm);
    群聊/console/boot/idle 一律 False -- 群里不说心里话。
    """
    src = (getattr(brain, "_current_group_src", "")
           or getattr(brain, "_current_source", "") or "")
    if src == "feishu":
        return True
    if src == "wechat":
        return bool(getattr(brain, "_current_group_is_dm", False))
    return False


def build_ack_text(app_name: Optional[str]) -> str:
    """按目标 app 选接单文案, 未知 app 走通用兜底。"""
    if app_name and app_name in APP_ACK_COPY:
        return APP_ACK_COPY[app_name]
    return GENERIC_ACK


def _resolve_task_app(brain: "Brain", tool_calls: list) -> Optional[str]:
    """取批内首个实际任务工具的 app 名 (供文案选择), 纯豁免工具返回 None。"""
    for tool in tool_calls:
        fname = tool.function.name
        if fname in ACK_SKIP_TOOLS:
            continue
        try:
            args = json.loads(tool.function.arguments)
        except (json.JSONDecodeError, TypeError):
            args = {}
        if fname == "instruct_app":
            return args.get("app_name") or None
        if fname.startswith("use_"):
            resolver = getattr(brain, "_resolve_app_from_tool_name", None)
            if callable(resolver):
                try:
                    return resolver(fname) or fname[4:]
                except Exception:
                    return fname[4:]
            return fname[4:]
        apps = getattr(brain.bus, "active_apps", {})
        avail = getattr(brain.bus, "available_apps", {})
        if fname in apps or fname in avail:
            return fname
        return None  # 非豁免但不映射 app (如内核工具) -> 通用文案
    return None


def _has_task_tool(tool_calls: list) -> bool:
    """批内是否存在至少一个非豁免工具 (决定是否值得接单)。"""
    return any(t.function.name not in ACK_SKIP_TOOLS for t in tool_calls)


async def maybe_send_task_ack(brain: "Brain", tool_calls: list) -> None:
    """[接单 ACK] 首个工具批执行前给老板 1v1 发一句接单确认。

    防抖: _task_ack_sent 每任务流一条 (入口重置段复位)。
    豁免: 纯记忆检索/元动作批不接单; 群聊不发; 渠道解析不到不发。
    出站: bus.send_instruction 直发, 不占熔断账本 (收尾发送不受影响)。
    """
    # [0.3.27.0 阶段5 规格七 收口] 接单 ACK 默认关闭: 进度永不进聊天。
    # config/brain.yaml feedback.task_ack=true 可临时开启 (兼容测试/灰度)。
    _cfg = (getattr(brain, "brain_config", None) or {}).get("feedback", {}) or {}
    if not _cfg.get("task_ack", False):
        return
    if getattr(brain, "_task_ack_sent", False):
        return
    if not tool_calls or not _has_task_tool(tool_calls):
        return
    if not is_boss_private_group(brain):
        logger.debug("[task_feedback] 非老板1v1, 跳过接单 ACK (群里不说心里话)")
        return
    channel = resolve_feedback_channel(brain)
    if not channel or channel not in getattr(brain.bus, "active_apps", {}):
        logger.debug("[task_feedback] 渠道不可达, 跳过接单 ACK")
        return
    app_name = _resolve_task_app(brain, tool_calls)
    text = build_ack_text(app_name)
    kwargs = {"text": text}
    chat = getattr(brain, "_current_group_chat", "") or ""
    if chat:
        kwargs["target_user"] = chat
    try:
        logger.info(f"[task_feedback] 接单 ACK ({channel}/{chat or '默认'}): {app_name or '通用'}")
        await brain.bus.send_instruction(channel, "send_text", **kwargs)
        brain._task_ack_sent = True
        # [review H2] 出站留痕: 阿露要感知自己发过 ACK (历史/behavior_log),
        # 但不占熔断目标账本 (_outbound_targets_this_turn), 收尾发送不受影响。
        try:
            record_outbound_text(brain, f"[接单ACK] {text}", channel, chat)
        except Exception:
            pass  # 测试桩无账本字段时静默, 留痕是增益不是前提
    except Exception as e:
        logger.warning(f"[task_feedback] 接单 ACK 发送失败 ({channel}): {e}")
