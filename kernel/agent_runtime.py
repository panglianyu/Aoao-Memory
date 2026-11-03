"""kernel/agent_runtime.py -- 阿露 agent 运行时共享模块。

封装 boot() + run_main_loop() + shutdown(), 供 main_god.py(控制台入口)
与 desktop/agent_worker.py(UI 入口) 共用, 避免逻辑重复 (DRY + 模块化红线)。

主播模式(空闲主动发声) / reply 路由兜底 / 叙述性元话语判定 / 沉默决策判定
等主循环业务逻辑全部封装于此。入口只需:

    bus, brain = await boot()
    await run_main_loop(bus, brain)                       # main_god: 靠 exit 指令 break
    # 或 await run_main_loop(bus, brain, stop_event=ev)  # desktop: 外部 stop_event 退出
    await shutdown(bus)
"""
import asyncio
import os
import sys
import difflib

import yaml

# ==========================================
# 国内 HuggingFace 镜像源 (解决 bge-m3 等模型无法从 huggingface.co 下载)
# ==========================================
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"

from kernel.app_bus import AppBus
from kernel.brain import BUSY_REPLY, Brain
from kernel.message import Message
from kernel.logger import get_logger
from kernel.outbound_router import OutboundRouter
from kernel.daily_memory_contracts import DailyRuntimeFactory
from kernel.daily_memory_bootstrap import DailyMemorySettings, resolve_daily_memory
# [0.2.9.1] 叙述/沉默判定抽到 reply_filters 共享 (think_batch 按组代发同用);
# 别名导入保 main_god.py 的 re-export 兼容。
from kernel.reply_filters import (  # noqa: F401
    _NARRATION_MARKERS,
    _SILENCE_META_KEYWORDS,
    is_interrupted_meta_statement,
    is_narration_statement as _is_narration_statement,
    is_silence_meta_statement as _is_silence_meta_statement,
)
# [0.3.21.0 Hermes 式任务循环] 工作列表: 忙时老板消息即时回执 (queue 范式)
from kernel.task_queue import TaskQueue, busy_watch_once
from protocols.standard_plug import AoaoPlug

logger = get_logger("runtime")

# 项目根 (本文件在 kernel/, 上跳一层); 强制锁定本地运行库路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "runtime_env", "libs"))
APPS_DIR = os.path.join(BASE_DIR, "apps")


def get_bios_list() -> list:
    """读取 BIOS 配置文件, 获取需要物理通电的应用名单。"""
    path = os.path.join(BASE_DIR, "config", "bios.yaml")
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return config.get("auto_start", []) if config else []
    except Exception as e:
        logger.warning(f"[Boot] BIOS配置读取失败，返回空列表: {e}")
        return []


def get_bios_flag(key: str, default: bool = True) -> bool:
    """[0.3.26.2 修复1] 读 bios.yaml 布尔开关 (如 boot_announce)。

    缺键回退 default (保旧行为); 读取失败同样回退默认, 不抛。
    """
    path = os.path.join(BASE_DIR, "config", "bios.yaml")
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
            value = config.get(key, default)
            return bool(value) if isinstance(value, bool) else default
    except Exception:
        return default


# [0.2.23.4 性能刀2] 重通电组件 (资源初始化重: chroma+模型 / npx MCP 子进程 /
# 微信 multiprocess 守护 / 飞书 WebSocket)。启动分两段 gather: 重组先发让最长
# 段尽早开工, 轻组随后, 避免 mp spawn/npx/ws 全部同一瞬间起的 CPU 尖峰。
_HEAVY_BOOT_APPS = frozenset({"system_memory_god", "mcp_gateway", "wechat", "feishu"})


async def connect_bios_apps(bus, bios_apps: list) -> None:
    """[0.2.23.4 性能刀2] 并行通电: 重组 gather -> 轻组 gather。

    旧实现 for-await 串行, 14 app 各段相加 (实测 13+4s)。并发安全依据:
    connect_app 各自操作不同 active_apps 键 + 异常自含 (返回字符串不抛) +
    超时三级回退不变; bge-m3 单例双检锁线程安全 (刀3 后加载已移后台)。
    """
    heavy = [a for a in bios_apps if a in _HEAVY_BOOT_APPS]
    light = [a for a in bios_apps if a not in _HEAVY_BOOT_APPS]
    if heavy:
        logger.info(f"[Boot] 重组件并行通电 ({len(heavy)}): {heavy}")
        await asyncio.gather(*(bus.connect_app(name) for name in heavy))
    if light:
        logger.info(f"[Boot] 轻组件并行通电 ({len(light)}): {light}")
        await asyncio.gather(*(bus.connect_app(name) for name in light))


# ==========================================
# 主播模式 / 路由 / 沉默判定 辅助函数
# ==========================================

# [主播模式] 执行回执类无信息量片段关键词 -- 这类记忆不该当主播开口素材。
_RECEIPT_KEYWORDS: tuple[str, ...] = (
    "收到", "好的", "已发送", "已读", "完成", "知道了", "明白", "了解",
    "发送成功", "已送达", "已处理", "已保存", "已记录", "执行成功", "done",
)


def _is_receipt_fragment(line: str) -> bool:
    """判定一行素材是否为执行回执类无信息量片段。

    短句且整体被回执关键词主导即视为回执 (如 "收到"、"已发送给胖脸鱼"、"好的, 已保存")。
    长句 (含真实信息量) 即使命中关键词也不剔, 避免误杀。
    """
    s = line.strip().strip("-•*").strip()
    if not s:
        return True
    if len(s) > 16:
        return False
    return any(kw in s for kw in _RECEIPT_KEYWORDS)


def _filter_material_receipts(text: str) -> str:
    """剔除素材里的执行回执类片段, 返回有效内容。全为回执返回空串。"""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    kept = [ln for ln in lines if not _is_receipt_fragment(ln)]
    return "\n".join(kept).strip()


def _is_idle_repeat(new_reply: str, recent: list, threshold: float = 0.5) -> bool:
    """[主播模式] 判定 idle 回复是否与近期主动发言重复 (difflib 相似度)。

    防"无限循环一件事"(如反复聊同一计划)。与 recent 中任一条相似度 >= threshold 即判重复。
    空 reply 或无近期记录返回 False (不抑制)。
    """
    new = (new_reply or "").strip()
    if not new or not recent:
        return False
    return any(
        difflib.SequenceMatcher(None, new, (old or "").strip()).ratio() >= threshold
        for old in recent
    )


async def _fetch_idle_memory_material(bus) -> str:
    """[主播模式] 空闲发声前先翻往事拿素材, 防尬聊。无素材返回提示让脑端闭嘴。"""
    from kernel.raw_message_retrieval import daily_retrieval_enabled
    if daily_retrieval_enabled(bus):
        # Idle has no sealed user turn. Without an explicitly authorized stable
        # destination, do not infer scope from a display name or use old stores.
        return "(翻不到往事, 没素材, 建议沉默)"
    mem_provider = bus.find_memory_provider()
    if not mem_provider or mem_provider not in bus.active_apps:
        return "(无记忆后端, 没素材, 建议沉默或纯工作进展反馈)"
    try:
        result = await bus.send_instruction(
            mem_provider, "recall", query="最近和胖脸鱼聊的事 承诺 往事",
        )
        text = getattr(result, "text", None) or str(result or "")
        text = text.strip()
        if not text or text in ("暂无往事。", "- 暂无核心事实记录。", "暂无核心事实记录。"):
            return "(翻不到往事, 没素材, 建议沉默)"
        filtered = _filter_material_receipts(text)
        if not filtered or len(filtered) < 6:
            logger.info("[主播] 翻到的素材全是执行回执类无信息量内容, 判定无有效素材。")
            return "(翻到的全是执行回执类记忆, 无有效素材, 建议沉默)"
        return filtered[:300]
    except Exception as e:
        logger.warning(f"[主播] 翻往事失败: {e}")
        return "(翻往事失败, 没素材, 建议沉默)"


def _channel_send_succeeded(result) -> bool:
    """判定 send_instruction 结果是否真送达。飞书返回串含'已送达'为成功。"""
    text = getattr(result, "text", None) or str(result or "")
    return ("已送达" in text) or ("success" in text.lower())


# [0.2.9.1] 沉默/叙述元话语判定已迁至 kernel/reply_filters.py (文件顶部别名导入)。


def _speak_msg(brain, reply: str) -> Message:
    """[0.2.20.0] 构造携带 turn_id + 三态 status 的 agent_speak 消息。

    桌面侧据 turn_id 对流式已渲染气泡去重 (流式路径 token 流 + 整条兜底同 turn),
    据 status 三态配色 (ok/error/timeout)。brain 无对应属性时回退空/ok (测试夹具)。
    """
    from kernel.web_research.display import delivery_metadata
    from kernel.raw_message_service import TurnReply
    if getattr(getattr(brain, "bus", None), "raw_messages", None) is not None:
        reply = brain.bus.reply_text(reply, getattr(brain, "_current_turn_id", ""))
    turn_id = reply.identity.turn_id if isinstance(reply, TurnReply) else getattr(brain, "_current_turn_id", "")
    return Message.agent_speak(
        content=reply, source="main",
        turn_id=turn_id,
        status=getattr(brain, "_last_think_status", "ok"),
        **delivery_metadata(brain),
    )


async def _route_raw_idle_reply(bus, raw, reply: str, turn_id: str) -> bool:
    resolver = getattr(bus, "_raw_agent_id", None)
    agent_id = resolver() if callable(resolver) else ""
    if not agent_id or not turn_id:
        logger.warning("[主播] 主动消息缺稳定身份；未发送。")
        return False
    for source in ("wechat", "feishu"):
        if source not in bus.active_apps:
            continue
        target = raw.proactive_target(agent_id, source)
        if not target:
            continue
        result = await bus.send_instruction(
            source, "send_text", text=reply, target_user=target,
            idempotency_key=f"idle:{turn_id}", expected_agent_id=agent_id,
        )
        if result.ok:
            logger.info(f"[主播] 已推送 -> {source}")
            return True
        logger.warning(f"[主播] {source} 发送未确认；禁止跨渠道自动重发。")
        return False
    logger.warning("[主播] 没有唯一授权的主动消息目标；未发送。")
    return False


async def _route_idle_reply_to_boss(bus, reply: str, *, turn_id: str = "") -> bool:
    """[主播模式] 空闲内心独白主动推给老板。【微信优先, 飞书兜底】。"""
    raw = getattr(bus, "raw_messages", None)
    if raw is not None:
        return await _route_raw_idle_reply(bus, raw, reply, turn_id)
    if "wechat" in bus.active_apps:
        try:
            res = await bus.send_instruction(
                "wechat", "send_text", text=reply, target_user="胖脸鱼摆摆",
            )
            if _channel_send_succeeded(res) or (
                getattr(res, "ok", True) and "失败" not in str(getattr(res, "text", ""))):
                logger.info("[主播] 已推送 -> 微信(胖脸鱼摆摆)")
                return True
            logger.warning(f"[主播] 微信发送未确认: {getattr(res,'text','')}, 转飞书兜底")
        except Exception as e:
            logger.warning(f"[主播] 微信发送异常: {e}, 转飞书兜底")
    if "feishu" in bus.active_apps:
        try:
            result = await bus.send_instruction("feishu", "send_text", text=reply)
            if result.ok or _channel_send_succeeded(result):
                logger.info("[主播] 已推送 -> 飞书(胖脸鱼)")
                return True
            logger.warning("[主播] 飞书兜底未确认，内容仅留控制台。")
        except Exception as e:
            logger.warning(f"[主播] 飞书兜底也失败: {e}, 内容仅留控制台")
    return False


def _pool_mark_batch(bus, batch: list, stage: str) -> None:
    """[0.3.25.1] 消息池批量打标 (fire-and-forget, 任何异常不影响消息主流程)。

    bus.pool 可选 (测试/无 pool 环境静默跳过); 只标入池过的消息 id。
    """
    try:
        pool = getattr(bus, "pool", None)
        if pool is None or not batch:
            return
        pool.mark_batch([m.id for m in batch if getattr(m, "id", "")], stage)
    except Exception as e:
        logger.debug(f"[runtime] 消息池打标失败(非致命): {e}")


def _pick_primary(batch: list):
    """[内核2.0] 选本批"主对话方": priority 消息(私聊老板/@)优先, 否则取最后一条。"""
    if not batch:
        return None
    for m in batch:
        if m.metadata.get("priority"):
            return m
    return batch[-1]


def _primary_is_master(primary) -> bool:
    """主对话方是否老板本人 (只看 is_master)。

    [阶段1 H1 0.1.11.0] priority 是秒回调度优先级(老板私聊或被@阿露), 不是
    "这是老板"标识。非老板@阿露 priority=True 不应被当 master, 否则 narration
    路由条件 `and not is_master` 失效, 兜底路由把内心独白发到群里(实证 aoao.log
    17:50:54"已读不回继续潜水"发小乖赛高群)。只看 is_master。
    """
    if primary is None:
        return False
    md = primary.metadata or {}
    return bool(md.get("is_master"))


def _primary_is_dm_with_master(primary) -> bool:
    """主对话方是否老板 1v1 私聊窗口 (非群聊/非他人单聊)。

    [Fix 0.1.15.2 根因B] is_master 只标识"发件人是老板", 老板在群里发言也 True,
    但那是群聊不是私聊。narration 路由应只在老板私聊生效 (跨对话出站确认本意),
    群聊里 narration 一律不路由 (防内心独白发群)。判据: wechat 提交时打的
    is_dm_with_master (chat_name == master_name)。
    """
    if primary is None:
        return False
    md = primary.metadata or {}
    return bool(md.get("is_dm_with_master"))


def _boss_directly_received(primary, outbound_targets: list) -> bool:
    """[跨对话确认回信 0.1.10.1] 老板(主对话方)本轮是否已直接收到 send_text。

    outbound_targets 是 brain._outbound_targets_this_turn: [(app, target), ...]。
    老板私聊 (wechat, chat_name) 或 飞书1v1 (feishu, '') 命中即视为已直接收到。
    """
    if primary is None:
        return False
    psrc = primary.source
    pchat = primary.get("chat_name", "") if hasattr(primary, "get") else ""
    return any(app == psrc and tgt == pchat for app, tgt in outbound_targets)


def _should_route_reply(reply: str, primary, outbound_targets: list) -> bool:
    """[Fix B 0.1.10.5] reply 是否应兜底路由给主对话方 (老板或群)。

    防忘调工具丢回: 阿露没 send_text 给该对话时, reply 兜底路由 (治"做了活没发出去")。
    - 老板私聊(1v1): narration("已经...了"确认) 也路由 (跨对话出站确认, Problem 1)。
    - 群聊/他人单聊: narration 不路由 (防刷屏/内心独白), 仅非 narration 的实质回复路由。
    - silence 元话语 / 已直接 send_text 给该对话: 不路由。

    [Fix 0.1.15.2 根因B] 旧逻辑用 is_master 判"是否老板私聊", 但老板在群里发言
    is_master 也 True -> narration 被路由到群 (内心独白发群, 实证 2026-07-06 10:45:27
    "没@我，潜水" 发小乖赛高群)。改用 is_dm_with_master (chat_name==master_name)
    精确判 1v1 私聊, 群聊场景 narration 一律不路由。
    """
    if not reply or not reply.strip():
        return False
    if _is_silence_meta_statement(reply):
        return False
    if _boss_directly_received(primary, outbound_targets):
        return False
    is_dm = _primary_is_dm_with_master(primary)
    # [0.3.27.1] narration + 中断元话语 (被打断/还没发出/下一轮再回) 群聊不发,
    # 老板 1v1 私聊保留 (跨对话出站确认语义)。治心里话泄漏进群。
    if (_is_narration_statement(reply) or is_interrupted_meta_statement(reply)) and not is_dm:
        return False
    return True


def _reply_should_show_in_main(brain, reply: str) -> bool:
    """[0.3.27.3] 主对话显示裁决: 只有阿露真实回复进主对话流 (agent_speak)。

    系统兜底文案 (timeout/error) / 老板抢占收尾 / 中断元话语都是内部调度
    状态, 只落日志与消息聚合页, 不显示进主对话 (2026-08-06 老板反馈:
    主对话漏进微信消息与思考超时文案)。沉默元话语不进 (原语义保留)。
    """
    if not reply or not reply.strip():
        return False
    if _is_silence_meta_statement(reply):
        return False
    if getattr(brain, "_last_think_status", "ok") in ("timeout", "error"):
        return False
    if getattr(brain, "_preempt_requested", False):
        return False
    if is_interrupted_meta_statement(reply):
        return False
    return True


def _idle_reply_should_send(brain, reply: str) -> bool:
    """[0.3.20.0 止血] idle 主动发声是否可外发。

    系统兜底文案 (timeout/error 状态) 不是阿露真实回复, 不得经
    _route_idle_reply_to_boss 发给老板; silence 元话语同样不发 (原语义保留)。
    """
    if not reply or not reply.strip():
        return False
    if _is_silence_meta_statement(reply):
        return False
    status = getattr(brain, "_last_think_status", "ok")
    if status in ("timeout", "error"):
        logger.warning(f"[主播] 主动发声为系统兜底文案, 不外发 (status={status})")
        return False
    return True


def _compute_fallback_route(brain, reply: str, primary, outbound_targets: list) -> bool:
    """[0.3.6.2] 主循环兜底路由最终裁决: 该不该把 brain 回复兜底发给主对话方。

    在 _should_route_reply 基础上叠加两道出站收口:
    - 契约代发已出站 (_structured_outbound_this_turn) -> 不重发 (0.2.8.0 原有);
    - 本轮已按组代发/工具出站 (_outbound_sent_this_turn) -> 不重发。
      治混合批 (老板私聊+群) 串台: think_batch 返回老板组回复且已代发到老板私聊,
      但 _pick_primary 从混合批选中群消息, 旧兜底把老板私聊回复转发到群 (私聊串台)。
      按"这条回复发没发过"裁决, 不依赖 _pick_primary 选谁, 故选错也拦得住。

    [0.3.20.0 止血] timeout/error 状态 = 系统兜底文案, 非阿露真实回复, 一律不
    兜底外发 (治 2026-08-03 "模型响应太久" 泄漏到微信群)。与 brain_outbound
    按组代发闸门同守。
    """
    status = getattr(brain, "_last_think_status", "ok")
    if status in ("timeout", "error"):
        logger.warning(f"[路由] 系统兜底文案不兜底外发 (status={status}): {reply.strip()[:60]}")
        return False
    # [0.3.27.1 结构闸] 老板抢占轮收尾 content 不兜底外发 (system_preempt 承诺
    # 只进日志; 与按组代发同守, 治心里话泄漏)。
    if getattr(brain, "_preempt_requested", False):
        logger.info(f"[路由] 老板抢占收尾文本不兜底外发: {reply.strip()[:60]}")
        return False
    should = _should_route_reply(reply, primary, outbound_targets)
    if not should:
        return False
    if getattr(brain, "_structured_outbound_this_turn", False):
        logger.info(f"[路由] 契约代发已出站, 兜底不重发: {reply.strip()[:80]}")
        return False
    if getattr(brain, "_outbound_sent_this_turn", False):
        logger.info(f"[路由] 本轮已按组代发/工具出站, 兜底不重发 (防串台): {reply.strip()[:80]}")
        return False
    return True


async def _route_reply_to_primary(bus, batch, reply: str) -> None:
    """[内核2.0] 把脑端的文本 reply 路由到本批"主对话方"。"""
    from kernel.raw_message_service import TurnReply
    primary = reply.identity.original if isinstance(reply, TurnReply) else _pick_primary(batch)
    if primary is None:
        return
    src = primary.source
    if src in ("console", "multi", "boot"):
        return
    chat_name = primary.get("chat_name", "") if hasattr(primary, "get") else ""
    try:
        if src == "wechat":
            result = await bus.send_instruction("wechat", "send_text", text=reply, target_user=chat_name)
            if getattr(result, "ok", None) is not True:
                logger.warning("[路由] 微信未确认发送；不自动补发")
                return
            logger.info(f"[路由] 自动回信 -> 微信({chat_name or '默认'})")
        elif src == "feishu":
            await bus.send_instruction("feishu", "send_text", text=reply)
            logger.info("[路由] 自动回信 -> 飞书")
        elif src in bus.active_apps:
            await bus.send_instruction(src, "send_text", text=reply)
            logger.info(f"[路由] 自动回信 -> {src}")
    except Exception as e:
        logger.warning(f"[路由] 自动回信失败 ({src}/{chat_name}): {e}")


# ==========================================
# 运行时三段: boot / run_main_loop / shutdown
# ==========================================

# [0.2.23.5 性能刀1] 后台任务强引用集 (create_task 弱引用, 不持即被 GC)
_BACKGROUND_TASKS: set = set()

# [0.2.23.5 性能刀1] boot 播报让位参数: 用户活跃期一直让, 超时无空档即放弃
_ANNOUNCE_YIELD_TIMEOUT_SEC = 90.0
_ANNOUNCE_POLL_SEC = 0.5


async def _boot_announcement(bus, brain, active_list: list) -> None:
    """[0.2.23.5 性能刀1] 开机播报 (后台 fire-and-forget, 不卡 boot)。

    brain.think 自带重入拒绝锁 (忙时返回"系统正忙"), 但播报被拒就哑了, 故先
    轮询让位: 等上一轮 think 收尾 + 队列无排队用户消息才开口; 90s 内一直有
    用户活动 -> 放弃播报 (仪式性一句, 不值得挤占)。播报后走 system_voice
    语音 (与旧实现同), 拒绝性回复 (撞上重入锁) 不读。异常自含不穿透。
    """
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _ANNOUNCE_YIELD_TIMEOUT_SEC
        while loop.time() < deadline:
            thinking = getattr(brain, "_thinking", False)
            # [0.2.23.7 review M2] 走公开 pending_count (鸭子 bus 无此方法时
            # 回退私成员, 保测试夹具兼容)
            pending = getattr(bus, "pending_count", None)
            if callable(pending):
                queued = pending()
            else:
                try:
                    queued = bus._message_queue.qsize()
                except Exception:
                    queued = 0
            if not thinking and queued == 0:
                break
            await asyncio.sleep(_ANNOUNCE_POLL_SEC)
        else:
            logger.info("[Boot] 播报持续让位用户活动, 放弃开机播报 (非致命)。")
            return

        boot_prompt = (
            f"[System Boot] 内核通报：底层硬件（包括 {active_list}）已物理挂载成功。"
            f"请作为便利屋68社长阿露，向首领（胖脸鱼）简短汇报系统已全功率上线。"
            f"【禁止调用启动工具，直接说话】"
        )
        reply = await brain.think(boot_prompt, source_channel="boot")
        # [0.2.23.7 review M2] 撞重入锁的拒绝回复 (BUSY_REPLY 同源常量) 不读
        if not reply or BUSY_REPLY in reply:
            logger.info("[Boot] 播报让位给用户消息 (brain 忙), 跳过。")
            return
        logger.info(f"阿露(开机播报): {reply}")
        if "system_voice" in bus.active_apps:
            # [0.3.0.5] 语音三模式需要来源/自动旗/轮次去重。
            await bus.send_instruction(
                "system_voice", "speak", text=reply,
                source="boot", auto=True,
                turn_id=getattr(brain, "_current_turn_id", ""),
            )
    except Exception as e:
        logger.warning(f"[Boot] 后台开机播报异常 (非致命): {e}")


async def _display_console_reply(msg: Message) -> bool:
    """A local console handoff is confirmed only after stdout accepts the final text."""
    print(f"阿露: {msg.content}", flush=True)
    return True


async def boot(*, raw_profile=None, daily_derivation=None, daily_retrieval=None,
               daily_runtime_factory: DailyRuntimeFactory | None = None,
               daily_settings: DailyMemorySettings | None = None) -> tuple:
    """初始化 bus + brain + outbound_router, 按 bios 通电所有 app, 起床播报。

    Returns:
        (bus, brain): 已通电的总线与大脑实例。
    """
    if daily_settings is not None and (
            type(daily_settings) is not DailyMemorySettings or any(value is not None for value in
                (raw_profile, daily_derivation, daily_retrieval, daily_runtime_factory))):
        raise ValueError("daily settings and explicit injection are exclusive")
    if daily_runtime_factory is not None and (
            not callable(daily_runtime_factory) or raw_profile is None
            or raw_profile.derived_root is None
            or daily_derivation is not None or daily_retrieval is not None):
        raise ValueError("explicit runtime factory profile required; legacy injection is exclusive")
    if daily_derivation is not None and (raw_profile is None or raw_profile.derived_root is None):
        raise ValueError("explicit daily memory profile required")
    if daily_retrieval is not None and (daily_derivation is None or raw_profile is None
                                        or raw_profile.derived_root is None):
        raise ValueError("explicit daily retrieval profile and worker required")
    # [0.3.37.0] data/ 布局自愈: 顶层散落现役件归位 (幂等, 迁完零操作)。
    # 必须早于任何落盘读取 (AppBus 手动断开标记 / behavior_log 召回)。
    from kernel.data_layout import migrate_once
    migrate_once()

    logger.info("==========================================")
    logger.info("Aoao OS - Intelligent Agent Mode (100% Memory Edition)")
    logger.info("==========================================")

    bus = AppBus(APPS_DIR)
    bus.scan_apps()
    brain = Brain(bus)
    if daily_settings is not None:
        raw_profile, daily_runtime_factory = resolve_daily_memory(daily_settings, bus, brain)
    if raw_profile is not None:
        from kernel.raw_message_service import RawMessageService
        raw_service = RawMessageService(raw_profile)
        runtime = None
        try:
            bus.attach_raw_messages(raw_service, lambda: brain._active_agent_id)
            if daily_settings is not None and daily_settings.legacy_sources:
                bus.attach_daily_legacy(bus.available_apps[bus.find_memory_provider()], daily_settings.legacy_sources)
            bus.register_raw_local_delivery("console", "on_agent_speak", _display_console_reply)
            bus.register_raw_local_delivery("console", "on_agent_stream_end", _display_console_reply)
            if daily_runtime_factory is not None:
                runtime = daily_runtime_factory(raw_service, raw_profile, bus)
                bus.attach_daily_runtime(runtime)
                await runtime.start()
        except BaseException:
            if runtime is not None:
                await runtime.close()
            else:
                await raw_service.close()
            raise
    try:
        return await _finish_boot(bus, brain, daily_derivation, daily_retrieval)
    except BaseException:
        runtime = getattr(bus, "_daily_runtime", None)
        if runtime is not None:
            await runtime.close()
        raise


async def _finish_boot(bus, brain, daily_derivation, daily_retrieval) -> tuple:
    # [0.3.25.1] 消息池挂载 (聚合页数据基础): 入池 hook 在 app_bus.submit_input
    from kernel.message_pool import MessagePool
    bus.pool = MessagePool(os.path.join(BASE_DIR, "data", "message_pool"))
    # [0.3.25.1] 平台管理层挂载 (目标策略/回复裁决共享; web server 复用此实例)
    from kernel.platform_hub import PlatformHub
    bus.hub = PlatformHub(os.path.join(BASE_DIR, "data", "platform", "listening_targets.json"))
    await bus.hub.load()

    outbound_router = OutboundRouter(bus)
    outbound_router.attach()

    bios_apps = get_bios_list()
    # [0.3.28.0] 行动权威: 老板手动断开过的 app 不参与开机自启,
    # 直到老板在控制台手动连接 (connect_app by_user=True 清标)。
    skipped = [n for n in bios_apps if bus.is_manually_disconnected(n)]
    if skipped:
        logger.info(f"[Boot] 跳过自启 (老板手动断开, 等待手动连接): {skipped}")
        bios_apps = [n for n in bios_apps if n not in skipped]
    if bios_apps:
        logger.info(f"[Boot] 发现开机自启名单 {bios_apps}，正在执行内核级强行通电...")
        # [0.2.23.4 性能刀2] 串行 for-await 改分组并行 gather (重组先/轻组后)
        await connect_bios_apps(bus, bios_apps)

        active_list = bus.get_active_list()
        logger.info(f"[Boot] 硬件级通电完毕，当前在线组件: {active_list}。")

        # [0.2.23.5 性能刀1] boot 播报移出关键路径: 旧实现此处同步 await LLM
        # 往返 (实测 21s, 启动链路最大单点), 桌面/控制台窗口干等播报完才上线。
        # 改 fire-and-forget 后台任务 (行为变化老板已拍板: 播报从"开机完成即播"
        # 变"开机后异步播"); 任务内让位用户消息 (brain 重入锁 + 队列判空),
        # 90s 内没空档即放弃 (仪式性一句话不该占脑)。
        # [0.3.26.2 修复1] bios.yaml boot_announce=false 关闭开机播报 (老板
        # 反馈开机自动说话时机不对)。
        if get_bios_flag("boot_announce", default=True):
            task = asyncio.create_task(_boot_announcement(bus, brain, active_list))
            _BACKGROUND_TASKS.add(task)
            task.add_done_callback(_BACKGROUND_TASKS.discard)
        else:
            logger.info("[Boot] boot_announce=false, 跳过开机播报。")
    else:
        logger.info("[Boot] BIOS自启名单为空，系统静默待机。")

    if daily_derivation is not None:
        bus.attach_daily_derivation(daily_derivation)
        raw = bus.raw_messages
        if raw is None:
            raise ValueError("daily capture service unavailable")
        raw.start_worker(bus)
    if daily_retrieval is not None:
        bus.attach_daily_retrieval(daily_retrieval)
    return bus, brain


async def run_main_loop(bus, brain, stop_event=None) -> None:
    """主播模式主循环。

    Args:
        bus: AppBus 实例 (已通电)。
        brain: Brain 实例。
        stop_event: 可选的退出信号 (threading.Event / asyncio.Event, 需有 is_set())。
            传入后置位即退出循环 (desktop 用法)。不传则靠控制台 exit/quit/退朝
            指令 break (main_god 用法)。
    """
    # [主播模式] v0.5.5: 间隔 60s->600s(10分钟); 无回复上限 3->2。
    IDLE_PROACTIVE_THRESHOLD = 600.0
    IDLE_NO_REPLY_LIMIT = 2
    loop = asyncio.get_running_loop()
    last_think_ts = loop.time()
    last_idle_ts = loop.time()
    idle_no_reply_count = 0
    idle_standby = False
    recent_idle_replies: list = []
    # [0.3.21.0] 工作列表账本: 忙时老板 1v1 排队消息的回执去重与排队记录。
    # 挂 bus 上供 Brain 工具循环轮间看门狗共用同一账本 (review H1)。
    task_ledger = TaskQueue()
    bus.task_ledger = task_ledger

    while not (stop_event is not None and stop_event.is_set()):
        try:
            HEARTBEAT = 5.0
            try:
                await asyncio.wait_for(bus._priority_event.wait(), timeout=HEARTBEAT)
            except asyncio.TimeoutError:
                pass
            bus._priority_event.clear()

            if getattr(brain, "_thinking", False):
                if getattr(bus, "_master_preempt_flag", False):
                    logger.info("[主循环] 老板消息抢占中, 等待当前 think 软熔断收尾")
                # [0.3.21.0] 忙时接单看门狗: 排队中的老板 1v1 消息立即回执
                # (不消费消息, 下一轮 drain 照常批阅; 群聊不回, 群里不说心里话)
                try:
                    await busy_watch_once(bus, brain, task_ledger, bus.peek_pending())
                except Exception as e:
                    logger.warning(f"[主循环] 忙时回执看门狗异常 (非致命): {e}")
                logger.debug("[主循环] 上一轮 think 未结束, 本周期跳过 drain")
                continue

            batch = bus.drain_batch()
            # [0.3.25.1] 消息池打标: 本批已交 Brain
            _pool_mark_batch(bus, batch, "brain")
            try:
                qsize = bus._message_queue.qsize()
            except Exception:
                qsize = -1
            if qsize > 0:
                logger.info(
                    f"[诊断] drain 后队列仍残留 {qsize} 条 (可能 _thinking 占用致排队)"
                )

            now = loop.time()
            if not batch:
                if idle_standby:
                    continue
                if (now - last_think_ts > IDLE_PROACTIVE_THRESHOLD
                        and now - last_idle_ts > IDLE_PROACTIVE_THRESHOLD):
                    if idle_no_reply_count >= IDLE_NO_REPLY_LIMIT:
                        idle_standby = True
                        logger.info(
                            f"[主播] 连续 {idle_no_reply_count} 次主动发声无回复, "
                            f"进入待机, 等用户唤醒。"
                        )
                        continue
                    logger.info("[主循环] 主播模式: 空闲触发主动发声")
                    last_idle_ts = now
                    last_think_ts = now
                    memory_material = await _fetch_idle_memory_material(bus)
                    idle_no_reply_count += 1
                    recent_topics = " / ".join(
                        f"[{i+1}]{t[:40]}" for i, t in enumerate(recent_idle_replies)
                    ) or "(暂无, 首次主动发声)"
                    reply = await brain.think(
                        f"[主播空闲] 你空闲 10 分钟了, 想主动找胖脸鱼说点有营养的话。\n"
                        f"【可用素材源】1)系统翻的往事: {memory_material}  "
                        "2)你可主动调 memory_recall(翻更多往事) / web_search(找时事热点) "
                        "找话题——拿不准就搜, 别硬凑记忆反复嚼同一坨。\n"
                        "【空闲纪律】本轮禁止执行任何任务型操作 (查战绩/发文件/"
                        "控制应用等 use_* 工具一律不许调), 老板的任务等他自己发消息来;\n"
                        f"【你最近主动聊过的话题 - 严禁重复, 必须换新的】{recent_topics}\n"
                        "【内容铁律 - 禁止尬聊车轱辘话】只能选你真有话说的:\n"
                        "  1. 工作进展反馈: 你正在/刚做的事和真实想法/发现。\n"
                        "  2. 有意思的问题: 基于搜索到的时事/知识或往事, 问胖脸鱼一个有深度的问题。\n"
                        "  3. 观察/吐槽: 群聊见闻、对某事的看法。\n"
                        "  4. 回忆闲聊: 上面翻到的往事, 当朋友闲聊同步给他。\n"
                        "【禁止】'嗯/对/确实/哈哈/在呢/有空吗/无聊' 无信息量填充。"
                        "若素材和四类都没真话可说, 【输出必须完全为空】(什么都不输出)。"
                        "绝对禁止把'我决定闭嘴/没话说/继续待命/硬凑不如闭嘴'这类沉默解释写出来——"
                        "这些话对胖脸鱼毫无信息量, 沉默就是真沉默, 不要解释你为何沉默。\n"
                        "要发就直接把想说的内容输出出来 (系统会自动发给胖脸鱼),"
                        "不要调用任何微信/应用工具。",
                        source_channel="idle",
                        chat_name="胖脸鱼摆摆",
                    )
                    if _idle_reply_should_send(brain, reply):
                        try:
                            idle_preempted = bus._message_queue.qsize() > 0
                        except Exception:
                            idle_preempted = False
                        if idle_preempted:
                            logger.info(
                                "[主播] 空闲 think 期间有用户消息排队, 抑制空闲回复外发, "
                                "让位给正常回复。空闲内容仅留控制台。"
                            )
                            # [0.3.31.1 Batch 1] 结论移交不蒸发: 空闲轮做出的分析
                            # 暂存, 由下一轮批阅注入老板组参考 (修 16:23 事故:
                            # 答案做出来却被抑制规则掐死, 老板微信永无回复)。
                            if reply and reply.strip():
                                brain._idle_task_handoff = reply.strip()[:600]
                            await bus.emit_msg(_speak_msg(brain, reply))
                            logger.info(f"阿露(主播, 抑制外发): {reply}")
                        elif not getattr(brain, "_outbound_sent_this_turn", False):
                            if _is_idle_repeat(reply, recent_idle_replies):
                                logger.info(
                                    f"[主播] 抑制重复话题 (与近期主动发言相似): {reply.strip()[:60]}"
                                )
                                await bus.emit_msg(_speak_msg(brain, reply))
                            else:
                                idle_delivered = await _route_idle_reply_to_boss(
                                    bus, reply, turn_id=getattr(brain, "_current_turn_id", ""),
                                )
                                await bus.emit_msg(_speak_msg(brain, reply))
                                # [Fix 0.1.15.7] 记录实际出站文本 (游戏话题本身), 不是 reply 元话语
                                _idle_out = getattr(brain, "_outbound_texts_this_turn", [])
                                if idle_delivered:
                                    recent_idle_replies.extend(_idle_out or [reply.strip()])
                                    if len(recent_idle_replies) > 5:
                                        del recent_idle_replies[0]
                                logger.info(f"阿露(主播): {reply}")
                                if "system_voice" in bus.active_apps:
                                    # [0.3.0.5] 语音三模式需要来源/自动旗/轮次去重。
                                    await bus.send_instruction(
                                        "system_voice", "speak", text=reply,
                                        source="idle", auto=True,
                                        turn_id=getattr(brain, "_current_turn_id", ""),
                                    )
                        else:
                            await bus.emit_msg(_speak_msg(brain, reply))
                            logger.info(f"阿露(主播): {reply}")
                    else:
                        logger.info(
                            f"[主播] 空闲保持沉默 (不尬聊/兜底文案不外发)。未外发: {reply.strip()!r}"
                            if reply and reply.strip()
                            else "[主播] 空闲无话可说, 保持沉默 (不尬聊)。"
                        )
                continue

            if idle_standby:
                logger.info("[主播] 收到用户消息, 退出待机, 恢复主动发声。")
                idle_standby = False
            if idle_no_reply_count > 0:
                logger.info(
                    f"[主播] 收到 {len(batch)} 条消息, 无回复计数 {idle_no_reply_count}->0"
                )
            idle_no_reply_count = 0
            last_think_ts = loop.time()

            exit_hits = [m for m in batch
                         if m.source == "console" and m.content.strip().lower() in ("exit", "quit", "退朝")]
            if exit_hits:
                logger.info("阿露: 正在关机，保存状态...")
                break

            logger.info(
                f"[主循环] 心跳批阅: {len(batch)} 条消息, "
                f"渠道={[m.source for m in batch]}"
            )
            # [0.3.21.0] 工作列表流转: 清忙时排队账本 (排队消息已入本批),
            # 置当前任务标签 (下一段忙碌期回执引用)。
            task_ledger.flush()
            _pmsg = _pick_primary(batch)
            _plabel = ""
            if _pmsg is not None:
                _plabel = (_pmsg.metadata or {}).get("chat_name", "") or ""
            task_ledger.set_current(_plabel or "批阅处理")
            reply = await brain.think_batch(batch)
            # [审查整改 H2] 轻答标记后置清: 标记须活过 think_batch 快照拼装
            # (防双答标注), flush() 只清队列账本, 此处清标记集。
            task_ledger.flush_secretary_marks()
            last_think_ts = loop.time()

            if reply and reply.strip():
                outbound_targets = getattr(brain, "_outbound_targets_this_turn", [])
                primary = _pick_primary(batch)
                is_narration = _is_narration_statement(reply)
                is_silence = _is_silence_meta_statement(reply)
                # [Fix B 0.1.10.5] 防忘调工具丢回: 阿露没 send_text 给该对话时, reply 兜底
                # 路由给主对话方 (老板或群)。老板: narration 也路由 (跨对话确认); 群:
                # narration 不路由 (防刷屏), 仅实质回复路由。治"做了活没发出去"。
                # [0.3.6.2] 兜底裁决收口: 契约/按组代发已出站 -> 绝不重发, 治混合批
                # (老板私聊+群) _pick_primary 选群消息致老板私聊回复被转发到群的串台。
                should_route = _compute_fallback_route(brain, reply, primary, outbound_targets)
                if should_route:
                    await _route_reply_to_primary(bus, batch, reply)
                    who = "老板对话方" if _primary_is_master(primary) else "群对话方"
                    logger.info(f"[路由] {who}兜底回信 (跨对话出站确认/防忘调工具丢回)")
                elif is_narration:
                    logger.info(
                        f"[路由] 叙述性元话语不外发 (群防刷屏/老板已直接收到): "
                        f"{reply.strip()[:80]}"
                    )
                elif _boss_directly_received(primary, outbound_targets):
                    logger.info(
                        f"[路由] 已直接 send_text, reply 不重复外发: "
                        f"{reply.strip()[:80]}"
                    )
                # [0.3.27.3] 主对话流卫生: 兜底文案/抢占收尾/中断元话语不进
                # 主对话 (不发 agent_speak), 只落日志; 渠道入站归消息聚合页。
                show_in_main = _reply_should_show_in_main(brain, reply)
                if show_in_main:
                    await bus.emit_msg(_speak_msg(brain, reply))
                    # [0.3.25.1] 消息池打标: 本轮对批产生了真实回复 (路由/工具代发均已出站)
                    _pool_mark_batch(bus, batch, "replied")
                elif not is_silence:
                    logger.info(
                        f"[主对话] 内部状态文本不进主对话流 (兜底/抢占收尾/中断元话语): "
                        f"{reply.strip()[:80]}"
                    )
                logger.info(f"阿露(自言自语): {reply}")
                # [0.3.26.2 修复3] speak 触发条件放宽: 除路由/工具出站外,
                # 本地源 (console/web) 对话回复也出声 -- 旧条件 should_route or
                # outbound_targets 把 console/web (不路由不代发) 全排除, 导致
                # web 控制台对话无声。微信/飞书仍经 should_play 的 LOCAL_SOURCES
                # 过滤保持静音 (system_voice 侧收口, 安全语义不变)。
                _reply_src = getattr(primary, "source", "") if primary is not None else ""
                # [0.3.27.3] 兜底文案/内部收尾文本同样不出声 (与主对话显示同守)。
                if show_in_main \
                        and (should_route or outbound_targets or _reply_src in ("console", "web")) \
                        and "system_voice" in bus.active_apps:
                    # [0.3.0.5] 语音三模式需要来源/自动旗/轮次去重。
                    await bus.send_instruction(
                        "system_voice", "speak", text=reply,
                        source=_reply_src,
                        auto=True, turn_id=getattr(brain, "_current_turn_id", ""),
                    )

        except Exception as e:
            logger.error(f"内核循环异常: {e}")
            await asyncio.sleep(2)


async def shutdown(bus) -> None:
    """优雅关机: 逐个断开所有活跃组件 (每个 app 最多等 15 秒)。"""
    logger.info("[Shutdown] 正在逐个关闭组件...")
    shutdown_clean = True
    settle_legacy = getattr(type(bus), "settle_daily_legacy", None)
    if callable(settle_legacy):
        try:
            if not await settle_legacy(bus):
                shutdown_clean = False
                logger.error("[Shutdown] 旧记忆读取仍在结算，保留其提供者并继续无关收尾")
        except Exception as exc:
            shutdown_clean = False
            logger.error("[Shutdown] 旧记忆结算未完成 (%s)", type(exc).__name__)
    inflight_provider = getattr(type(bus), "daily_legacy_inflight_provider", None)

    def provider_is_owned(app):
        return callable(inflight_provider) and app is inflight_provider(bus)
    raw = getattr(bus, "raw_messages", None)
    daily_runtime = getattr(bus, "_daily_runtime", None)
    if daily_runtime is not None or raw is not None:
        try:
            if daily_runtime is not None:
                await daily_runtime.close()
            elif raw is not None:
                await raw.close()
        except Exception as exc:
            shutdown_clean = False
            logger.error("[Shutdown] 原始记忆收尾未完成 (%s), 继续关闭其他组件",
                         type(exc).__name__)
    scheduler = getattr(bus, "scheduler", None)
    if scheduler is not None:
        try:
            await scheduler.shutdown()
        except Exception as exc:
            shutdown_clean = False
            logger.error("[Shutdown] 调度器收尾未完成 (%s)", type(exc).__name__)
    shutdown_apps = []
    seen_apps: set[int] = set()
    for app in (getattr(bus, "available_apps", {}) or {}).values():
        if (
            isinstance(app, AoaoPlug)
            and type(app).on_system_shutdown is not AoaoPlug.on_system_shutdown
            and id(app) not in seen_apps
        ):
            seen_apps.add(id(app))
            shutdown_apps.append(app)
    # [0.3.25.1] 消息池收尾: flush 剩余落盘
    try:
        pool = getattr(bus, "pool", None)
        if pool is not None:
            await pool.close()
    except Exception as e:
        shutdown_clean = False
        logger.warning(f"[Shutdown] 消息池收尾异常(非致命): {e}")
    active_count = len(bus.active_apps)
    for idx, app_name in enumerate(list(bus.active_apps.keys()), 1):
        try:
            app = bus.active_apps.get(app_name)
            if app is None:
                continue
            if provider_is_owned(app):
                shutdown_clean = False
                continue
            logger.info(f"[Shutdown] ({idx}/{active_count}) 正在关闭 {app_name}...")
            result = await asyncio.wait_for(
                bus.disconnect_app(app_name), timeout=15
            )
            logger.info(f"[Shutdown] {result}")
        except asyncio.TimeoutError:
            shutdown_clean = False
            logger.error(
                f"[Shutdown] {app_name} 关闭超时 (15s), 跳过"
            )
        except Exception:
            shutdown_clean = False
            logger.exception(f"[Shutdown] {app_name} 关闭异常, 继续整机收尾")
    for app in shutdown_apps:
        if provider_is_owned(app):
            shutdown_clean = False
            continue
        name = str(getattr(app, "manifest", {}).get("name", "unknown"))
        logger.info(f"[Shutdown] 正在执行 {name} 的整机退出收尾...")
        try:
            await asyncio.wait_for(app.on_system_shutdown(), timeout=60)
        except asyncio.TimeoutError:
            shutdown_clean = False
            logger.error(f"[Shutdown] {name} 整机退出收尾超时 (60s)")
        except Exception as exc:
            shutdown_clean = False
            logger.error(f"[Shutdown] {name} 整机退出收尾失败: {exc}")
    if shutdown_clean:
        logger.info("[Shutdown] 所有组件已安全关闭, 阿露已离线。")
    else:
        logger.warning("[Shutdown] 关机流程结束但存在未完成收尾，请查看上方错误。")
