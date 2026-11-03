# kernel/tool_handlers.py
"""
[0.3.18.1 Brain 收缩 Phase 2] 工具处理器层

[0.3.27.0 阶段6 规格八] 兼容层声明: 本模块函数签名保留 brain 参数仅为
Brain 薄委托兼容点 (规格允许: 旧测试迁移完成后再删)。业务实现读取的
"本轮执行状态"全部走 TaskRun 共享引用 (brain._current_run 与 run 双写
引用共享: 出站账本/失败计数/权限主体), 不持有 Brain 单例可变执行状态。
执行点权限复核用 run.principal 不可变快照 (不继承当前消息)。

Brain 的 11 个 LLM 工具 handler (connect/disconnect/instruct_app +
learn_knowledge + memory_recall/remember/forget + behavior_recall +
web_search + adjust_impression + boost_interest) 与兴趣关键词刷新
从 brain.py 原样搬入, 统一为模块级 async def handle_xxx(brain, args) -> str。

红线: 只搬家不改逻辑 (行为/文案零变化), Brain 保留同名一行薄委托
(测试夹具与 monkeypatch 引用点不漂移)。submit_reply / 出站路由 /
工具循环留 Phase 3-4 再拆, 此处经 brain._handle_submit_reply 等路径调用。

循环导入约束: 本模块只 import kernel 底层 (logger/message/principal/
tool_result), Brain 类型仅 TYPE_CHECKING 期引用, 运行期走 duck typing。
"""
from typing import TYPE_CHECKING

from kernel.tool_result import ToolResult
from kernel.message import Message
from kernel.principal import require_owner
from kernel.logger import get_logger

if TYPE_CHECKING:  # 运行期不反引 brain, 防循环导入
    from kernel.brain import Brain

logger = get_logger(__name__)

_HOST_DESKTOP_APP = "native_controller"
_HOST_DESKTOP_DISABLED = (
    "已拦截：宿主桌面控制已停用。"
    "请使用隔离浏览器或文件 / 终端工作区完成任务。"
)


# =================== [App 生命周期] ===================

async def handle_connect_app(brain: "Brain", args: dict) -> str:
    app_name = args.get("app_name")
    if app_name == _HOST_DESKTOP_APP:
        logger.warning("[Brain] 已拦截宿主桌面控制器连接请求。")
        return _HOST_DESKTOP_DISABLED
    # [0.3.15.0 P2] 系统工具级高危闸门 (与 instruct_app 内的 connect_app 命令
    # 闸门互补: 那条守 use_<app> 路径, 这条守 LLM 直调系统工具路径)。
    _ok, _reason = require_owner(brain._principal_for_gate(), "app_lifecycle")
    if not _ok:
        logger.warning(
            f"[Brain] connect_app 系统工具被拦截 "
            f"(src={getattr(brain, '_current_source', '')}, {_reason})"
        )
        return (
            "已拦截：接通系统接口属于高危操作，"
            "需要老板本人明确指令，不能由对话第三方触发。"
        )
    res = await brain.bus.connect_app(app_name)
    brain._tools_is_dirty = True  # 生命周期变更 -> 工具树脏
    return res


async def handle_disconnect_app(brain: "Brain", args: dict) -> str:
    # [0.3.15.0 P2] 同 _handle_connect_app 闸门。
    _ok, _reason = require_owner(brain._principal_for_gate(), "app_lifecycle")
    if not _ok:
        logger.warning(
            f"[Brain] disconnect_app 系统工具被拦截 "
            f"(src={getattr(brain, '_current_source', '')}, {_reason})"
        )
        return (
            "已拦截：断开系统接口属于高危操作，"
            "需要老板本人明确指令，不能由对话第三方触发。"
        )
    app_name = args.get("app_name")
    res = await brain.bus.disconnect_app(app_name)
    brain._tools_is_dirty = True
    return res


# =================== [App 指令分发] ===================

async def handle_instruct_app(brain: "Brain", args: dict) -> str | ToolResult:
    """[幂等自愈] 目标 app 未连接但已注册时, 自动后台静默连接, 单 Turn 闭环。"""
    app_name = args.get("app_name")
    command = args.get("command")
    if not isinstance(command, str) or not command:
        return "失败: 缺少有效 command。"
    kwargs = brain._coerce_params(args.get("params", {}))
    if app_name == _HOST_DESKTOP_APP:
        logger.warning(
            f"[Brain] 已拦截宿主桌面控制指令 (command={command or '(空)'})."
        )
        return _HOST_DESKTOP_DISABLED
    # [P0-b 0.2.15.0] 本轮回复已代发后, 文本发送类命令逐次机械拦截
    # (治换措辞复读, 根因 2026-07-22 奶狗事件); 干活类工具与
    # send_file/send_image 等交付类命令放行 (见循环内 P0-b 注释)。
    if (
        getattr(brain, "_outbound_text_count", 0) > 0
        and command in brain._OUTBOUND_TEXT_COMMANDS
    ):
        logger.info(
            f"[Brain] [P0-b] 拦截重复发送: {command} (本轮已有文本出站)"
        )
        return (
            "[Kernel] 回复已代发, 禁止重复发送 (换措辞也不行)。"
            "请继续完成剩余工作 (如生成文档、发送文件), 或直接收尾。"
        )
    # [0.2.15.0 文档路由] doc_gen 生成时未指定目标 -> 注入当前会话真实
    # 上下文。msg.source 被 think_batch 压平成 multi 不可信 (见 memory
    # think-batch-flattens-source-to-multi), 用 brain 自持的会话上下文。
    if app_name == "doc_gen" and command == "generate" and isinstance(kwargs, dict):
        _src = getattr(brain, "_current_source", "")
        _chat = getattr(brain, "_current_chat_name", "")
        if not kwargs.get("target_channel") and _src and _src in brain.bus.active_apps:
            kwargs["target_channel"] = _src
        if (not kwargs.get("target_user") and _chat
                and _chat != getattr(brain, "_current_user_input", "")):
            kwargs["target_user"] = _chat
    # [0.2.8.0 出站收口] 管辖通道文本回复机械降级: 文字回复走 submit_reply 契约,
    # 裸 send_text 拒发+引导 (不靠 prompt 求模型自律)。媒体命令 (send_native_image/
    # send_file...) 与非管辖源 (idle/console 等) 不受影响。
    if (
        command == "send_text"
        and brain._structured_reply_enabled()
        and brain._reply_source_is_gated()
    ):
        # [0.2.9.1] 拒发改自动转换: 模型惯性调旧 send_text 时, 同参数转契约代发
        # (预解析 target = target_user 或当前对话), 省掉"撞墙再换"的往返,
        # 发送仍走单次槽位+去重, 反馈即真实代发结果 (她看不到"失败"就不会撒谎)。
        _t = str(kwargs.get("text") or "").strip()
        _tgt = (
            str(kwargs.get("target_user") or "").strip()
            or getattr(brain, "_current_group_chat", "")
            or getattr(brain, "_current_chat_name", "")
        )
        logger.info(
            f"[Brain] send_text 降级转换为契约代发 -> target={_tgt or '(无)'} "
            f"(src={getattr(brain, '_current_source', '')})"
        )
        return await brain._handle_submit_reply({"text": _t, "target": _tgt})
    # [权限闸门] 改变监听范围/热装MCP/插拔接口的操作只允许老板本人执行。
    # 拦截阿露 idle 自作主张 use_wechat(add_listen_chat) 去锁定/退出群——
    # 必须由老板明确指令。[0.3.15.0 P2] 收敛到统一闸门 require_owner:
    # 主体取 _principal_for_gate() (think_batch 透传的组主体, 混合身份批已降档
    # guest; 老链路兜底 _current_is_master)。行为/文案零变化。
    _HIGH_RISK_CMD_CAP = {
        "add_listen_chat": "listen_scope",
        "remove_listen_chat": "listen_scope",
        "add_server": "mcp_admin",
        "remove_server": "mcp_admin",
        "connect_app": "app_lifecycle",
        "disconnect_app": "app_lifecycle",
    }
    # 浏览器控制不能按 open/click 等通用命令名做全局拦截，否则会误伤其他
    # App；按目标 App 收口为独立 capability，并在插件内再次 fail-closed 复核。
    _gate_cap = (
        "browser_control"
        if app_name == "browser_workspace"
        else "project_files"
        if app_name == "project_workspace"
        else _HIGH_RISK_CMD_CAP.get(command)
    )
    _gate_ok, _gate_reason = (
        require_owner(brain._principal_for_gate(), _gate_cap)
        if _gate_cap is not None else (True, "")
    )
    if _gate_cap is not None and not _gate_ok:
        logger.warning(
            f"[Brain] 高危操作被拦截 (command={command}, capability={_gate_cap}, "
            f"src={getattr(brain, '_current_source', '')}, {_gate_reason})。"
            f"改变监听群/热装 MCP/插拔接口需老板明确指令。"
        )
        if command in ("connect_app", "disconnect_app"):
            return (
                "已拦截：接通/断开系统接口属于高危操作，"
                "需要老板本人明确指令，不能由对话第三方触发。"
            )
        if command in ("add_server", "remove_server"):
            return (
                "已拦截：热装/移除 MCP 服务器属于高危操作，"
                "需要老板本人明确指令，不能由对话第三方或自作主张触发。"
            )
        if _gate_cap == "browser_control":
            return (
                "已拦截：浏览器工作空间控制需要老板本人明确指令，"
                "不能由对话第三方或阿露自行扩权触发。"
            )
        if _gate_cap == "project_files":
            return (
                "已拦截：项目文件工作区只允许老板本人使用，"
                "不能由对话第三方或阿露自行扩权触发。"
            )
        return (
            "已拦截：添加/移除监听群需要老板本人明确指令。"
            "请让老板直接下达，不要自行决定监听范围。"
        )
    if not app_name:
        return "失败: 缺少 app_name。"
    if app_name not in brain.bus.active_apps:
        if app_name in brain.bus.available_apps:
            logger.info(f"[Brain] 幂等自愈: {app_name} 未连接, 自动后台启动。")
            connect_res = await brain.bus.connect_app(app_name)
            brain._tools_is_dirty = True
            if app_name not in brain.bus.active_apps:
                return f"系统警告: App {app_name} 自动启动失败 ({connect_res})。"
        else:
            return f"系统警告: App {app_name} 尚未注册，无法启动。"
    # 把本轮不可变 Principal 快照显式签入触发消息。插件不能读取 Brain 隐式状态，
    # 只能从这条 Message 再做自己的权限复核；缺失主体时仍按 guest 拒绝。
    _principal = brain._principal_for_gate()
    _run = getattr(brain, "_current_run", None)
    trigger = Message.user_input(
        content=brain._current_user_input,
        source=brain._current_source,
        chat_name=brain._current_chat_name,
        is_master=bool(_principal and _principal.is_owner),
        actor_id=getattr(_principal, "actor_id", ""),
        authenticated_by=getattr(_principal, "authenticated_by", ""),
        run_id=str(getattr(_run, "run_id", "") or ""),
        turn_id=str(
            getattr(_run, "turn_id", "")
            or getattr(brain, "_current_turn_id", "")
            or ""
        ),
    )
    # [内核2.0] 标记本轮已显式出站 (use_<app> 发消息类命令), 供 main_god 路由判定。
    if command in brain._OUTBOUND_TEXT_COMMANDS or command in brain._OUTBOUND_MEDIA_COMMANDS:
        # [空闲抢答抑制] 若当前是主播空闲 think (source=idle) 期间, 用户消息
        # 已排入信箱 (说明 think 卡顿的这几秒里用户来消息了), 则拦截这条空闲
        # 出站, 把发言权让给即将到来的正常 think_batch, 否则用户会收到"空闲抢答
        # + 正常回复"两条 (日志 20:13:34 R1 即此症)。
        if brain._current_source == "idle":
            try:
                pending = brain.bus._message_queue.qsize()
            except Exception:
                pending = 0
            if pending > 0:
                logger.info(
                    f"[Brain] 空闲抢答抑制: think 期间有 {pending} 条用户消息排队, "
                    f"拦截空闲出站 {command} -> {app_name}, 让位给正常回复"
                )
                return (
                    "用户刚刚发来消息, 已让位给正常回复通道, 本次空闲发声不发出。"
                )
        # [0.1.15.5 锁] 熔断提取到 _acquire_outbound_slot, asyncio.Lock 串行化
        # 检查+计数+append, 治 LLM 一轮并发多 send_text 绕过同对话去重。
        denied = await brain._acquire_outbound_slot(app_name, command, kwargs)
        if denied:
            return denied
    if app_name == "web_research":
        from kernel.web_research.runtime import dispatch
        return await dispatch(brain, command, trigger, kwargs)
    return await brain.bus.send_instruction(
        app_name, command, trigger_msg=trigger, **kwargs
    )


# =================== [知识 / 记忆工具] ===================

async def handle_learn_knowledge(brain: "Brain", args: dict) -> str:
    file_path = args.get("file_path", "")
    if not file_path:
        return "失败: 缺少 file_path 参数。"
    logger.info(f"[aluo_brain] [>] 知识吸收工具触发: {file_path}")
    if "doc_parser" in brain.bus.active_apps:
        return await brain.bus.send_instruction("doc_parser", "parse_document", path=file_path)
    return "失败: doc_parser 未启动，无法解析文档。请先在 bios.yaml 中启用 doc_parser。"


async def handle_memory_recall(brain: "Brain", args: dict) -> str:
    """[0.3.26.0 Phase 0 M0-01] 主动 recall 透传当前轮上下文: chat_name +
    asker_is_master 交给 provider 执行与 warm recall 相同的 ACL (群聊召不回私聊)。
    """
    from kernel.raw_message_retrieval import daily_retrieval_enabled
    if daily_retrieval_enabled(brain.bus):
        from kernel.raw_message_retrieval import render_daily_evidence
        query = args.get("query", "") or args.get("text", "")
        if not isinstance(query, str) or not query.strip():
            return "失败：需提供检索关键词"
        when_filter = str(args.get("when_filter", "") or "")
        start_ts = end_ts = None
        if when_filter:
            from system_memory_god.main import parse_when_filter
            parsed = parse_when_filter(when_filter) if len(when_filter) <= 100 else None
            if parsed is None:
                return "失败：无法解析时间过滤条件"
            start_ts, end_ts = parsed
        from kernel.daily_memory_contracts import recall_deadline
        deadline = recall_deadline(purpose="interactive")
        result = await brain.bus.recall_daily_with_status(query[:200], top_k=10, neighbors=2,
            start_ts=start_ts, end_ts=end_ts, purpose="interactive", deadline=deadline)
        evidence = render_daily_evidence(result.items)
        if result.status == "ready":
            if evidence:
                return evidence
            if start_ts is None and end_ts is None:
                legacy = await brain.bus.read_daily_legacy(query[:200], deadline=deadline)
                legacy_text = render_daily_evidence(legacy.items)
                if legacy_text:
                    return legacy_text
                if legacy.reason_codes != ("legacy_unmapped",) and legacy.status != "ready":
                    return "未命中新原文；旧来源暂不可用，不能断言没有相关记忆。"
            return "未检索到当前会话记忆。"
        if "authority_denied" in result.reason_codes:
            notice = "当前会话记忆权限校验未通过，未读取旧来源。"
        elif "deadline_exceeded" in result.reason_codes:
            notice = "记忆查询超时，不能据此判断没有相关记忆。"
        else:
            notice = "记忆查询暂时降级或不可用，不能据此判断没有相关记忆。"
        return evidence + "\n" + notice if evidence else notice
    mem_provider = brain._resolve_memory_provider()
    if mem_provider and mem_provider in brain.bus.active_apps:
        query = args.get("query", "") or args.get("text", "")
        kwargs_mem = {
            "query": query,
            "chat_name": str(getattr(brain, "_current_chat_name", "") or ""),
            "asker_is_master": bool(getattr(brain, "_current_is_master", False)),
        }
        when_filter = args.get("when_filter")
        if when_filter:
            kwargs_mem["when_filter"] = when_filter
        return await brain.bus.send_instruction(mem_provider, "recall", **kwargs_mem)
    return "失败: 记忆后端未启动。"


async def handle_memory_remember(brain: "Brain", args: dict) -> str:
    """[0.3.26.0 Phase 0 M0-02] 主动 remember 透传 chat/主体/1v1 标记,
    provider 据此派生 visibility 桶 (老板私聊 -> private, 群聊 -> group)。
    """
    from kernel.raw_message_retrieval import daily_retrieval_enabled
    if daily_retrieval_enabled(brain.bus):
        return "当前模式在接收消息时自动保存原文；未向旧记忆库额外写入。"
    mem_provider = brain._resolve_memory_provider()
    if mem_provider and mem_provider in brain.bus.active_apps:
        text = args.get("text", "") or args.get("content", "")
        importance = args.get("importance_score", args.get("importance", 0.8))
        return await brain.bus.send_instruction(
            mem_provider, "remember", text=text, importance_score=importance,
            chat_name=str(getattr(brain, "_current_chat_name", "") or ""),
            is_master=bool(getattr(brain, "_current_is_master", False)),
            is_dm_with_master=bool(getattr(brain, "_current_group_is_dm", False)),
        )
    return "失败: 记忆后端未启动。"


async def handle_memory_forget(brain: "Brain", args: dict) -> str:
    """[0.3.26.0 Phase 0 M0-03] 主动 forget 透传上下文 (精确 ID 优先,
    provider 侧闭环向量删除; chat 上下文供 ACL/审计)。"""
    from kernel.raw_message_retrieval import daily_retrieval_enabled
    if daily_retrieval_enabled(brain.bus):
        target = args.get("target", "") or args.get("entity_or_rule_id", "")
        if not isinstance(target, str) or not target.strip():
            return "需要当前会话内有效的原始消息 ID。"
        if await brain.bus.forget_daily(target.strip()):
            return "已从当前会话正常召回中移除，可按原始消息 ID 恢复。"
        return "需要当前会话内有效的原始消息 ID。"
    mem_provider = brain._resolve_memory_provider()
    if mem_provider and mem_provider in brain.bus.active_apps:
        target = args.get("target", "") or args.get("entity_or_rule_id", "")
        return await brain.bus.send_instruction(
            mem_provider, "forget", target=target,
            chat_name=str(getattr(brain, "_current_chat_name", "") or ""),
            is_master=bool(getattr(brain, "_current_is_master", False)),
        )
    return "失败: 记忆后端未启动。"


# =================== [行为自查] ===================

async def handle_behavior_recall(brain: "Brain", args: dict) -> str:
    """[阶段4 行为自查] 查近期出站行为 + 记忆变更, 治阿露不知自己发了什么。

    不动交互, 阿露主动调工具查。读 data/behavior_log.jsonl (kernel.behavior_log)。
    可选 query 子串过滤, n 限制条数(默认10)。返回文本摘要。
    """
    try:
        from kernel import behavior_log
    except Exception as e:
        return f"失败: behavior_log 模块不可用: {e}"
    query = (args.get("query") or "").strip() or None
    n = args.get("n")
    try:
        n = int(n) if n else 10
    except (TypeError, ValueError):
        n = 10
    if query:
        recs = behavior_log.read_recent(n=n, query=query)
        if not recs:
            return f"未查到匹配'{query}'的行为记录。"
        lines = []
        for rec in recs:
            at = rec.get("action_type", "")
            if at == "outbound":
                lines.append(f"对 {rec.get('target_user', '')}({rec.get('app', '')}) 发送: {rec.get('content', '')}")
            elif at == "memory_change":
                lines.append(f"记忆{rec.get('op', '')}: {rec.get('content', '')}")
        return "行为记录:\n" + "\n".join(lines)
    summary = behavior_log.summarize_recent(n=n)
    return f"近期行为记录:\n{summary}" if summary else "暂无行为记录。"


# =================== [主动联网] ===================

async def handle_web_search(brain: "Brain", args: dict) -> ToolResult:
    """Old saved calls must not silently turn into new paid search requests."""
    return ToolResult("web_search", False, "error", {"error_code": "migrated_tool"},
                      "旧联网开关已移除，尚未执行搜索。请使用 use_web_research 的 research 或 web_search 命令。")


# =================== [VIP 审批式调整] ===================

async def handle_adjust_impression(brain: "Brain", args: dict) -> str:
    """[VIP 审批式双轴调整] 仅胖脸鱼(老板)可调阿露对人的看法。

    权限闸门: 据 _current_is_master 放行 (think_batch 已正确传播 is_master_batch)。
    旧逻辑查 _current_source=='console' 已废弃: main_god 把 console 也走 think_batch,
    source 被压成 'multi', 致 console 老板 VIP 指令永远被拒 (2026-06-25 bug)。
    is_master 是正确的老板信号 (console/飞书1v1/微信胖脸鱼摆摆 = True; 阿露 idle = False),
    阿露自己不可调 (防情绪化乱改)。reason 必填。审批留痕由 system_memory_god 记。
    """
    # [0.3.15.0 P2] 统一闸门: impression 高危能力须 owner (行为/文案零变化)。
    _imp_ok, _imp_reason = require_owner(brain._principal_for_gate(), "impression")
    if not _imp_ok:
        logger.warning(
            f"[Brain] memory_adjust_impression 被非老板调用 "
            f"(src={getattr(brain, '_current_source', '')}, {_imp_reason}), 拒绝"
        )
        return "失败: 仅胖脸鱼(老板)本人可调整阿露对人的看法, 非老板无权限。"
    person = (args.get("person") or "").strip()
    reason = (args.get("reason") or "").strip()
    if not person:
        return "失败: 缺少 person (要调整看法的人名)。"
    if not reason:
        return "失败: 调整看法需说明理由 (reason 必填)。"
    fav_delta = float(args.get("favorability_delta", 0) or 0)
    att_delta = float(args.get("attention_delta", 0) or 0)
    mem_provider = brain._resolve_memory_provider()
    if not mem_provider or mem_provider not in brain.bus.active_apps:
        return "失败: 记忆后端未启动。"
    src = getattr(brain, "_current_source", "") or "console"  # 审计留痕用真实渠道
    try:
        res = await brain.bus.send_instruction(
            mem_provider, "adjust_impression",
            person=person, favorability_delta=fav_delta,
            attention_delta=att_delta, reason=reason, source=src,
        )
        return res.text if isinstance(res, ToolResult) and res.ok else f"调整失败: {res}"
    except Exception as e:
        logger.error(f"[Brain] adjust_impression 异常: {e}")
        return f"调整异常: {e}"


async def handle_boost_interest(brain: "Brain", args: dict) -> str:
    """[0.2.9.0] 老板"多关注X": 涨兴趣值 + 设衰减阻尼 (越小衰减越慢, 仍衰减)。

    权限闸门同 memory_adjust_impression: 仅 _current_is_master 放行, 防阿露自己
    情绪化乱涨。兴趣值驱动群聊闸门动态关键词 (准许看), 见 _refresh_interest_keywords。
    """
    # [0.3.15.0 P2] 统一闸门: interest 高危能力须 owner (行为/文案零变化)。
    _int_ok, _int_reason = require_owner(brain._principal_for_gate(), "interest")
    if not _int_ok:
        logger.warning(
            f"[Brain] boost_interest 被非老板调用 "
            f"(src={getattr(brain, '_current_source', '')}, {_int_reason}), 拒绝"
        )
        return "失败: 仅胖脸鱼(老板)本人可调整阿露的兴趣关注, 非老板无权限。"
    topic = (args.get("topic") or "").strip()
    reason = (args.get("reason") or "").strip()
    if not topic:
        return "失败: 缺少 topic (要关注的话题)。"
    if not reason:
        return "失败: 涨兴趣需说明理由 (reason 必填)。"
    delta = float(args.get("delta", 1.0) or 1.0)
    damping = float(args.get("damping", 0.5) or 0.5)
    mem_provider = brain._resolve_memory_provider()
    if not mem_provider or mem_provider not in brain.bus.active_apps:
        return "失败: 记忆后端未启动。"
    try:
        res = await brain.bus.send_instruction(
            mem_provider, "boost_interest",
            topic=topic, reason=reason, delta=delta, damping=damping,
        )
        return res.text if isinstance(res, ToolResult) and res.ok else f"兴趣调整失败: {res}"
    except Exception as e:
        logger.error(f"[Brain] boost_interest 异常: {e}")
        return f"兴趣调整异常: {e}"


# =================== [兴趣关键词刷新] ===================

async def refresh_interest_keywords(brain: "Brain") -> None:
    """[0.2.9.0] TTL 缓存拉记忆侧兴趣 top-N 注入闸门动态关键词。

    闸门不依赖记忆活性: 失败留旧缓存不阻塞 think。top_n=0 -> 动态关键词清空
    (回退纯提及门铃, 一键回滚)。旋钮: config/brain.yaml interest 段。
    """
    import time as _time
    cfg = getattr(brain, "_interest_cfg", None)
    if cfg is None:
        cfg = brain._interest_cfg = (brain._load_brain_config().get("interest") or {})
    top_n = int(cfg.get("top_n", 8))
    ttl = float(cfg.get("refresh_ttl", 300))
    now = _time.time()
    if now - getattr(brain, "_interest_refresh_ts", 0.0) < ttl:
        return
    brain._interest_refresh_ts = now
    gate = getattr(brain, "_speak_gate", None)
    if gate is None:
        return
    if top_n <= 0:
        gate.set_dynamic_keywords([])
        return
    try:
        provider = brain._resolve_memory_provider()
        if not provider or provider not in brain.bus.active_apps:
            return
        res = await brain.bus.send_instruction(provider, "list_interests", top_n=top_n)
        text = getattr(res, "text", "") or ""
        kws = brain._parse_interest_list(text)
        gate.set_dynamic_keywords(kws)
        if kws:
            logger.info(f"[Brain] 兴趣关键词刷新 ({len(kws)} 个): {kws}")
    except Exception as e:
        logger.warning(f"[Brain] 兴趣关键词刷新失败(非致命, 留旧缓存): {e}")


# =================== [0.3.27.0 阶段4 规格六: ask_user 询问用户] ===================

async def _send_ask_question(brain, run) -> None:
    """把问题发送到老板私聊 (wechat/feishu send_text); console/web 只落日志

    (Web 问题卡片属阶段5 状态栏收口)。发送失败不阻塞挂起 (老板可从 Web 回答)。
    """
    question = (run.pending_question or {}).get("text", "")
    if not question:
        return
    channel = run.source
    if channel in getattr(brain.bus, "active_apps", {}):
        try:
            kwargs = {"text": f"[需要你拍板] {question}"}
            if run.chat_name:
                kwargs["target_user"] = run.chat_name
            await brain.bus.send_instruction(channel, "send_text", **kwargs)
            return
        except Exception as e:
            logger.warning(f"[ask_user] 问题发送失败 ({channel}): {e}")
    logger.info(f"[ask_user] 问题 (console/web 侧): {question}")


async def handle_ask_user(brain: "Brain", args: dict) -> str:
    """[0.3.27.0 阶段4 规格六] ask_user 内核工具执行器。

    规则:
    1. 允许条件: Principal.is_owner + (老板1v1 或 本地 console/web) + 任务模式
       + 同一会话最多一个待答问题。
    2. 群聊/第三人/权限不明: 不发送问题, 回填 ask_user_unavailable,
       模型只能选安全默认/取消/说明无法继续。
    3. 通过: 组装 pending_question (ask_id/text/options/safe_default/deadline),
       发问题 -> 保存 TaskStore -> 返回已发送标记 (tool_loop 检测后挂起)。
    """
    question = str(args.get("question") or "").strip()
    if not question:
        return "Error: ask_user 缺少必需参数 question"
    run = getattr(brain, "_current_run", None)
    if run is None:
        return "Error: ask_user 需要任务运行上下文"
    # [规则1] 权限闸门: owner + 1v1/本地
    p = getattr(run, "principal", None)
    local_src = getattr(run, "source", "") in ("console", "web")
    if not (p is not None and p.is_owner and (local_src or getattr(run, "is_dm", False))):
        logger.info(f"[ask_user] 非老板1v1/本地, 拒绝询问 (src={run.source}, dm={run.is_dm})")
        return ("[ask_user_unavailable] 仅老板私聊或本地控制台可询问, "
                "请选择安全默认值继续、取消任务或说明无法继续")
    # [规则3] 同一会话最多一个待答问题
    if run.pending_question is not None:
        return "[ask_user_unavailable] 已有待答问题, 请等待老板回答"
    import time as _t
    options = list(args.get("options") or [])
    safe_default = str(args.get("safe_default") or "").strip()
    timeout = float(args.get("timeout") or 60.0)
    run.pending_question = {
        "ask_id": f"ask_{run.run_id}",
        "tool_call_id": "",  # tool_loop 回填当前 tool_call_id
        "text": question,
        "options": options,
        "safe_default": safe_default,
        "safe_is_default": bool(safe_default),
        "deadline": _t.time() + timeout,
    }
    run.deadline = run.pending_question["deadline"]
    run.status = "waiting_user"
    run.bump_status_frame()
    await _send_ask_question(brain, run)
    # [持久化] 挂起到 TaskStore (重启可恢复)
    try:
        store = brain._task_store()
        store.save(run)
    except Exception as e:
        logger.warning(f"[ask_user] 挂起持久化失败(非致命): {e}")
    logger.info(f"[ask_user] 问题已发送, 任务挂起等待回答 (run={run.run_id}, {timeout:.0f}s)")
    return "[ask_user_sent] 问题已发送给老板, 任务已挂起等待回答"
