# kernel/tool_loop.py
"""
[0.3.24.0 屎山 Phase 4] 工具循环实现层 (从 brain.py 搬体, Brain 收缩)。

搬体集 (STATE 0.3.18.x 前置分析 flag×method 矩阵执行):
- _to_assistant_message + LoopOutcome (原 brain.py 模块级, brain 重导出保
  import 路径: tests/test_brain_streaming.py 等直接 from kernel.brain import)
- send_interim_feedback   <- Brain._send_interim_feedback
- maybe_send_batch_interim <- Brain._maybe_send_batch_interim (接单 ACK 委托)
- execute_single_tool      <- Brain._execute_single_tool
- run_tool_loop            <- Brain._run_tool_loop
- finalize_reply           <- Brain._finalize_reply

红线 (搬体不得破坏):
1. 调用序不变: run_tool_loop 内 轮间看门狗 -> LLM 往返 -> _to_assistant_message
   -> 批前 ACK -> 工作状态帧 -> gather(execute_single_tool) -> 原序回填 (熔断/
   behavior_log/出站文本收集) -> 抢占检查。
2. monkeypatch 点保留: 本模块回调一律走 brain._xxx (brain 侧留同名薄委托),
   存量测试 patch brain._call_llm/brain._execute_single_tool 等零漂移。
3. 入口重置段留 brain._think_impl 不搬 (旗标收编是 Phase 5 TurnState 的事)。

循环导入: 顶层不 import kernel.brain; _session_key 在 finalize_reply 内延迟导入
(brain 顶层 import 本模块, 反向顶层导入成环)。task_queue/task_feedback/
work_status 顶层导入安全 (均不反引 brain/tool_loop)。
"""
import asyncio
import datetime
import json
import re
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Optional

from kernel.logger import get_logger
from kernel.message import Message
from kernel.tool_result import ToolResult
from kernel.tool_summary import SUMMARY_UNAVAILABLE, summarize_tool_results
from kernel.execution_evidence import needs_receipt_review, record_receipt, with_execution_evidence
from kernel.task_feedback import maybe_send_task_ack as _tf_maybe_send_task_ack
from kernel.task_queue import busy_watch_once as _tq_busy_watch_once
from kernel import work_status as _work_status
from kernel import task_runtime as _task_rt
from kernel.skill_manager import (
    save_skill,
    report_failure,
)
from kernel.model_conversation import ModelOutputLimitError, ModelProtocolError

logger = get_logger(__name__)

# 工具上限: 最大工具执行轮次 8，另允许 1 次禁止工具的有界总结；最大工具
# 调用总数 24 (total_tool_calls 独立计数), 单工具连续失败 3 (熔断)。达到
# 上限返回明确 max_steps, 不伪装成功/思考超时。
MAX_TOTAL_TOOL_CALLS = 24
TOOL_ERRORS_REPLY = "本轮存在失败或无法核对的工具回执，不能确认任务全部完成。已执行的操作不会因此撤销。"


def _to_assistant_message(msg) -> dict:
    """[0.2.22.1] 归一 LLM 回合为可序列化 assistant dict (修桌面/console 工具轮崩溃)。

    流式 _call_llm_stream 返回 dataclass _LLMReply; 若原样 append 进 messages, 下一次
    client.chat.completions.create 内部 json.dumps 抛 'Object of type _LLMReply is not
    JSON serializable' -> 重试3次 -> 网络短路。非流式 msg 是 OpenAI pydantic 对象, 统一
    归一也无副作用。兼容 pydantic / dict / SimpleNamespace 三种 tool_calls 元素形态;
    arguments 非 str 时 json 化。
    """
    import json as _json
    tcs = []
    for tc in (getattr(msg, "tool_calls", None) or []):
        fn = getattr(tc, "function", None)
        if fn is None and isinstance(tc, dict):
            fn = tc.get("function")
        if isinstance(fn, dict):
            name = fn.get("name") or ""
            args = fn.get("arguments")
        else:
            name = getattr(fn, "name", None) or ""
            args = getattr(fn, "arguments", None)
        if args is None:
            args = ""
        if not isinstance(args, str):
            args = _json.dumps(args, ensure_ascii=False)
        cid = getattr(tc, "id", None)
        if cid is None and isinstance(tc, dict):
            cid = tc.get("id")
        tcs.append({
            "id": cid or f"call_{len(tcs)}",
            "type": "function",
            "function": {"name": name, "arguments": args},
        })
    out = {"role": "assistant", "content": getattr(msg, "content", None)}
    reasoning = getattr(msg, "reasoning_content", None)
    if reasoning is not None:
        out["reasoning_content"] = reasoning
    if tcs:
        out["tool_calls"] = tcs
    return out


# [0.2.20.0] 工具循环返回: reply + 三态 status (ok/error/timeout), 供下游 UI
# 三态展示与日志归因。think() 对外契约不变 (仍返回净化后的 str)。
@dataclass(frozen=True)
class LoopOutcome:
    reply: str
    status: str = "ok"
    summary_only: bool = False


async def send_interim_feedback(brain, tool_name: str) -> None:
    """
    [可观测性] 向当前源渠道发送"安心反馈"消息。
    当调用耗时工具时，提前告知用户系统正在工作，避免用户焦虑。

    [防抖逻辑] 同一用户任务流中只允许发送一次，防止重试刷屏。
    [泛化] 使用 _current_source 动态选择通道，不再硬编码 feishu_bot。
    """
    # 防抖检查：当前任务流中是否已发送过安心反馈
    if brain._interim_sent:
        logger.debug(f"[aluo_brain] 跳过重复安心反馈: {tool_name}")
        return

    if tool_name not in brain.TIME_CONSUMING_TOOLS:
        return

    # 泛化: 优先使用当前源渠道, 回退到 feishu_bot
    channel = brain._current_source if (
        hasattr(brain, '_current_source')
        and brain._current_source in brain.bus.active_apps
    ) else "feishu"

    if channel not in brain.bus.active_apps:
        return

    feedback_msg = brain.TIME_CONSUMING_TOOLS[tool_name]
    full_message = f"收到指令，{feedback_msg}"

    try:
        logger.info(f"[aluo_brain] [>] 发送安心反馈 ({channel}): {tool_name}")
        await brain.bus.send_instruction(channel, "send_text", text=full_message)
        # 标记已发送，防止后续重试刷屏
        brain._interim_sent = True
    except Exception as e:
        logger.warning(f"[aluo_brain] [!] 安心反馈发送失败 ({channel}): {e}")


async def maybe_send_batch_interim(brain, tool_calls: list) -> None:
    """[0.3.21.0] 接单 ACK 薄委托 (逻辑在 kernel/task_feedback.py)。

    覆盖所有任务工具, 仅老板 1v1 (群聊不发心里话), 渠道走 _current_group_src。
    """
    await _tf_maybe_send_task_ack(brain, tool_calls)


def _validation_error(tool, func_name: str, reason: str) -> dict:
    """[0.3.27.0 阶段3] 结构化 validation error (规格五.1: 参数错误绝不执行)。

    返回与 execute_single_tool 同形的结果 dict, 零出站/零派发。
    """
    logger.warning(f"[Brain] 工具 {func_name} 参数校验失败: {reason}")
    return {
        "call_id": tool.id,
        "func_name": func_name,
        "args": {},
        "res_str": f"Error: 工具参数校验失败 - {reason}",
        "tool_result": ToolResult.error(
            func_name, f"参数校验失败: {reason}", {"validation": reason}),
    }


def _malformed_research_error(brain, tool) -> dict:
    from kernel.web_research.receipt import error_receipt
    from kernel.web_research.runtime import record_failure

    receipt = error_receipt("invalid_parameters")
    record_failure(brain, receipt["text"])
    logger.warning("[Brain] 公开资料研究参数校验失败，已拒绝执行")
    return {
        "call_id": tool.id,
        "func_name": "use_web_research",
        "args": {},
        "res_str": receipt["text"],
        "tool_result": ToolResult(
            "web_research", False, "error", receipt, receipt["text"],
        ),
    }


def _validated_tool_arguments(brain, tool, func_name: str):
    raw_args = tool.function.arguments
    try:
        args = json.loads(raw_args)
    except (json.JSONDecodeError, TypeError) as exc:
        error = (_malformed_research_error(brain, tool)
                 if func_name == "use_web_research" else _validation_error(
                     tool, func_name,
                     f"参数 JSON 解析失败: {exc} (原始: {str(raw_args)[:100]})"))
        return None, None, error
    if not isinstance(args, dict):
        error = (_malformed_research_error(brain, tool)
                 if func_name == "use_web_research" else _validation_error(
                     tool, func_name,
                     f"参数必须是 JSON 对象, 实际: {type(args).__name__}"))
        return None, None, error
    try:
        canonical = json.dumps(args, sort_keys=True, ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        error = (_malformed_research_error(brain, tool)
                 if func_name == "use_web_research" else _validation_error(
                     tool, func_name, "参数包含不受支持的 JSON 值"))
        return None, None, error
    return args, canonical, None


def _legacy_name_routing_enabled(brain) -> bool:
    """[0.3.27.0 阶段3] 模型幻造直接函数名的旧兼容映射开关 (规格五.1)。

    默认关闭: 禁止自动把未知函数名猜成 App 并执行。config/brain.yaml
    tools.legacy_name_routing=true 可开启 (生产若见模型惯性可临时开)。
    """
    cfg = (getattr(brain, "brain_config", None) or {}).get("tools", {}) or {}
    return bool(cfg.get("legacy_name_routing", False))


def _project_tool_completion(brain, func_name, args, tool_id, result, tool_result):
    from kernel.web_research.runtime import is_research_call, record_failure

    research = is_research_call(func_name, args)
    if tool_result is not None and tool_result.app == "web_research":
        research = True
    if not research:
        return result, result
    if (tool_result is not None and not tool_result.ok
            or tool_result is None and brain._is_tool_error(result)):
        record_failure(brain, result)
    from kernel.web_research.history import project_tool_event
    projected, _, log_value = project_tool_event(
        getattr(brain, "_current_run", None), result, research=True, call_id=tool_id,
    )
    return projected, log_value


async def execute_single_tool(brain, tool) -> dict:
    """
    [单工具执行] 串行闭环: 参数硬校验 -> 幂等认领 -> 派发执行 -> 幂等提交。

    [0.3.27.0 阶段3] 原"解析失败空字典继续执行"根因修复 (规格五.1):
      - JSON 解析失败/非对象 -> 结构化 validation error, 绝不执行。
      - use_<app> 缺 command -> validation error, 不派发。
      - 未知工具 -> 结构化错误, 不猜测 (幻造路由默认关, 配置开关可开)。
    [幂等认领 规格五.4]: 键 = run_id + tool_call_id + canonical_arguments +
    tool_revision (TaskRun.claim_tool); COMMITTED -> 回放不重执行;
    AMBIGUOUS -> 禁止自动重试; 执行成功后 commit_tool。

    返回 dict 含 res_str (回填文本) 与 tool_result (ToolResult 或 None)。
    """
    func_name = tool.function.name
    from kernel.web_research.context import bind_research_call, research_call_identity
    from kernel.web_research.runtime import is_research_call, replay, terminal_text
    brain._last_tool_called = func_name
    run = getattr(brain, "_current_run", None)

    # [参数硬校验] JSON 解析失败 / 合法 JSON 但非对象 -> validation error
    args, canonical, validation_error = _validated_tool_arguments(
        brain, tool, func_name,
    )
    if validation_error is not None:
        return validation_error

    # [幂等认领] 已 COMMITTED 只回放; AMBIGUOUS 禁止自动重试
    research_call = is_research_call(func_name, args)
    call_identity = research_call_identity(
        tool.id, func_name, canonical, run.tool_revision if run is not None else 0,
    ) if research_call else None
    if run is not None:
        existing = run.executed_ledger.get(tool.id)
        if existing is not None and existing.get("state") != _task_rt.AMBIGUOUS and (
            existing.get("function_name") != func_name
            or existing.get("arguments") != canonical
            or existing.get("revision") != run.tool_revision
        ):
            return _validation_error(tool, func_name, "调用编号与原函数、参数或版本不匹配；未执行，需要核对")
        claim = run.claim_tool(tool.id, canonical)
        if claim == "replay":
            entry = run.executed_ledger.get(tool.id, {})
            if type(entry.get("receipt_ok")) is not bool:
                return _validation_error(tool, func_name, "旧调用结果缺少可核对的状态；未重放或执行")
            logger.info(f"[aluo_brain] [=] 工具 {func_name} 幂等回放 (已提交)")
            if research_call:
                if call_identity is None:
                    raise RuntimeError("missing research call identity")
                with bind_research_call(call_identity):
                    result = await replay(brain, entry)
                return {"call_id": tool.id, "func_name": func_name, "args": args,
                        "res_str": result.text, "tool_result": result}
            return {
                "call_id": tool.id,
                "func_name": func_name,
                "args": args,
                "res_str": entry.get("result", "") or "已提交结果",
                "tool_result": ToolResult(
                    func_name, entry["receipt_ok"], "text" if entry["receipt_ok"] else "error",
                    None, entry.get("result", "") or "已提交结果",
                ),
            }
        if claim == "ambiguous":
            return _validation_error(
                tool, func_name,
                "该调用状态不确定 (AMBIGUOUS), 禁止自动重试, 需人工确认")
        run.executed_ledger[tool.id]["function_name"] = func_name

    if terminal_text(brain) is not None:
        return _validation_error(tool, func_name, "研究结果已生成，本轮不再执行其他工具。")
    logger.info(f"[aluo_brain] [>] 意图明确，准备分发任务给工具: {func_name}")
    logger.debug(f"[内部动作] {func_name} -> {args}")

    # [0.3.31.1 Batch 1] 空闲轮工具边界: 空闲轮禁抢应用/任务工具 (治
    # "空闲轮抢任务跑 184s 堵住老板消息队列"); kernel 只读工具按策略放行。
    if getattr(brain, "_current_source", "") == "idle":
        _idle_policy = getattr(brain, "_IDLE_TOOLS", "kernel_only")
        _is_app_tool = func_name.startswith("use_") or func_name in (
            "instruct_app", "connect_app", "disconnect_app")
        if _idle_policy != "all" and _is_app_tool:
            logger.info(
                f"[tool_loop] 空闲轮工具边界: 拒绝 {func_name} "
                f"(idle_tools={_idle_policy})")
            return _validation_error(
                tool, func_name,
                "空闲轮不允许执行应用工具。想说话就直接输出内容, 系统会转达给胖脸鱼。")

    handler = brain._tool_dispatch_registry.get(func_name)
    tool_result: Optional[ToolResult] = None

    # [工具树派发] use_<appname> -> 反解 app 名, 路由到 instruct handler
    if handler is None and func_name.startswith("use_"):
        app_name = brain._resolve_app_from_tool_name(func_name)
        if app_name:
            cmd = args.get("command", args.get("cmd", ""))
            if not cmd or not isinstance(cmd, str):
                return _validation_error(
                    tool, func_name, "use_<app> 缺少必需参数 command")
            args = {
                "app_name": app_name,
                "command": cmd,
                "params": args.get("params", {}),
            }
            handler = brain._handle_instruct_app

    # [兜底开关 规格五.1] 模型幻造直接函数名 (实为 app 名) -> 默认拒绝,
    # config/brain.yaml tools.legacy_name_routing=true 时保留旧兼容路由。
    if handler is None and (func_name in brain.bus.available_apps
                            or func_name in brain.bus.active_apps):
        if _legacy_name_routing_enabled(brain):
            logger.warning(f"[Brain] 模型幻造了直接函数名 '{func_name}'，自动路由到 instruct_app。")
            args = {
                "app_name": func_name,
                "command": args.get("command", args.get("cmd", "")),
                "params": args.get("params", {}),
            }
            handler = brain._handle_instruct_app
        else:
            return _validation_error(
                tool, func_name,
                f"'{func_name}' 是 app 名而非工具名, 请使用 use_{func_name} 工具")

    if handler is None:
        res_str = f"Error: 未知的工具函数 {func_name}"
    else:
        try:
            # [0.3.31.1 Batch 1] handler 统一超时兜底: bus 三级超时只罩 app
            # 指令, kernel 内部工具裸等 -> 挂住即整轮吊死; 超时返结构化错误
            # 进熔断计数。0 = 关回退旧行为。
            _hard_timeout = float(getattr(brain, "_TOOL_HARD_TIMEOUT", 300) or 0)
            binding = (bind_research_call(call_identity)
                       if call_identity is not None else nullcontext())
            with binding:
                if _hard_timeout > 0:
                    res = await asyncio.wait_for(handler(args), timeout=_hard_timeout)
                else:
                    res = await handler(args)
            # [结构化归一] handler 返回 ToolResult -> 拆字段; 否则 str() 归一化防类型陷阱
            if isinstance(res, ToolResult):
                tool_result = res
                res_str = res.text
            else:
                res_str = str(res) if res is not None else "操作失败"
        except asyncio.TimeoutError:
            logger.error(
                f"[Brain][watchdog] 工具 {func_name} 执行超时 "
                f"({_hard_timeout:.0f}s), 中断本次调用")
            tool_result = ToolResult(
                app=func_name, ok=False, kind="error", data=None,
                text=f"工具 {func_name} 执行超时 ({_hard_timeout:.0f}s), "
                     "本次调用已中断, 请勿原参数重试")
            res_str = tool_result.text
        except Exception as exc:
            logger.error(
                "[Brain] 工具 %s 执行异常 (type=%s)",
                func_name, type(exc).__name__)
            tool_result = ToolResult.error(
                func_name, "工具执行失败，请稍后重试。")
            res_str = tool_result.text

    projected_result, projected_log = _project_tool_completion(
        brain, func_name, args, tool.id, res_str, tool_result,
    )

    # [幂等提交] 执行完成即 COMMITTED (恢复/重试只回放结果不重执行)
    if run is not None and handler is not None:
        run.commit_tool(tool.id, projected_result)
        run.executed_ledger[tool.id]["receipt_ok"] = (
            tool_result.ok if tool_result is not None else not brain._is_tool_error(res_str)
        )
        research_data = tool_result.data if (
            tool_result is not None and tool_result.ok
            and tool_result.app == "web_research" and isinstance(tool_result.data, dict)
        ) else None
        scope = research_data.get("scope") if research_data is not None else None
        if (research_data is not None and isinstance(scope, str)
                and re.fullmatch(r"[0-9a-f]{64}", scope)):
            from kernel.web_research.artifacts import receipt_digest
            run.executed_ledger[tool.id]["research_scope"] = scope
            run.executed_ledger[tool.id]["research_digest"] = receipt_digest(research_data)

    res_str = brain._truncate_result(res_str)
    # [Fix 0.1.15.2 根因C] 工具返回结果落日志, 治"卡工具循环看不到返回"黑盒
    logger.info(f"[aluo_brain] [<] 工具返回 {func_name}: {projected_log[:200]}")
    return {"call_id": tool.id, "func_name": func_name, "args": args,
            "res_str": res_str, "tool_result": tool_result}


def _select_research_ids(tool_calls, ask_ids: set) -> set:
    from kernel.web_research.runtime import is_research_call

    if ask_ids:
        return set()
    for candidate in tool_calls:
        if candidate.function.name == "use_web_research":
            return {candidate.id}
        try:
            candidate_args = json.loads(candidate.function.arguments)
        except (ValueError, TypeError):
            continue
        if (isinstance(candidate_args, dict)
                and is_research_call(candidate.function.name, candidate_args)):
            return {candidate.id}
    return set()


async def run_tool_loop(brain, messages: list) -> LoopOutcome:
    """[工具执行循环] 并发执行 + 原序回填 + 熔断器。think() 只调度此方法。

    [0.2.20.0] 返回 LoopOutcome (reply + 三态 status): 流式路径 (_stream_active)
    走 _call_llm_stream, 其余 (wechat/feishu/idle/boot) 走原非流式 _call_llm,
    零回归面。timeout 与 error 区分供桌面三态展示; 工具循环期间流式侧只累积
    不刷字 (start 懒发射, 纯工具轮零 token 事件)。

    [Phase 4] 回调一律走 brain._xxx 薄委托, 存量 monkeypatch 点零漂移。
    """
    from kernel.model_usage import MAX_TASK_MODEL_CALLS, reserve_model_call
    raw_turn = (brain.bus.raw_turn_snapshot()
                if getattr(brain.bus, "raw_messages", None) is not None else None)
    loop_count = 0
    final_reply = ""
    final_status = "ok"
    run = getattr(brain, "_current_run", None)
    if run is not None:
        run.stop_reason = ""
    _task_started = False  # [0.3.27.0 阶段5] 首个工具调用到达才进任务模式
    # [0.3.21.0 计时] 每轮 LLM 往返/工具批耗时落日志, 治"很慢"黑盒, 供定位慢点
    _loop = asyncio.get_running_loop()
    _turn_t0 = _loop.time()
    while loop_count < 8 and (run is None or run.llm_rounds < MAX_TASK_MODEL_CALLS):
        # [0.3.21.0 review H1] 轮间忙时回执: 批阅 think 期间主循环阻塞在
        # 本方法 await, 主循环忙碌分支不可达, 看门狗须在工具循环轮间也跑
        # (steer 时序: 不打断不开新轮, 回执完继续本任务)。
        _ledger = getattr(brain.bus, "task_ledger", None)
        if _ledger is not None:
            try:
                # [Phase 4 红线2] 看门狗经 kernel.brain 模块惰性查找: 存量测试
                # monkeypatch kernel.brain._tq_busy_watch_once 的补丁点零漂移
                # (顶层 import brain 会成环, 运行期 brain 必已加载完毕)。
                import kernel.brain as _kb
                _watch = getattr(_kb, "_tq_busy_watch_once", _tq_busy_watch_once)
                await _watch(brain.bus, brain, _ledger, brain.bus.peek_pending())
            except Exception as _watch_e:
                logger.warning(f"[tool_loop] 忙时回执异常 (非致命): {_watch_e}")
        if loop_count == 0:
            logger.info("[aluo_brain] [>] 正在深度思考，分析用户意图...")
            # [0.3.31.2 Batch 2] prompt 规模日志: 供工具定义裁剪决策数据
            # (15:23 事故上下文 = 历史+召回+16 app 全量工具定义)。
            try:
                _prompt_chars = sum(
                    len(str((m or {}).get("content", "") or ""))
                    for m in messages if isinstance(m, dict))
                logger.info(
                    f"[tool_loop] prompt 规模 {_prompt_chars} 字符 (轮 1)")
            except Exception:
                pass
        else:
            logger.info(f"[aluo_brain] [>] 第 {loop_count + 1} 轮工具调用后继续推理...")

        _llm_t0 = _loop.time()
        # [0.3.31.0 Batch 0] 看门狗卡死点标记 (超时归因到具体轮次/阶段)
        brain._turn_stage = f"llm_round_{loop_count + 1}"
        # [0.3.27.0 阶段2] 实际发起模型请求即计数 (TaskRun.llm_rounds 唯一计数源,
        # 供 strategy_for 派生 FAST_CHAT/TASK_DELIBERATE; 不依赖 loop_count 防
        # continue 绕过)。超时/异常路径也计数 (真实发起过)。
        _run = getattr(brain, "_current_run", None)
        if _run is not None:
            reserve_model_call(_run)
        llm_messages = with_execution_evidence(_run, messages)
        try:
            if getattr(brain, "_stream_active", False):
                msg = await brain._call_llm_stream(llm_messages)
            else:
                response = await brain._call_llm(llm_messages)
                msg = response.choices[0].message
        except ModelOutputLimitError:
            logger.error("[Brain] 模型输出达到预算上限，未使用不完整结果")
            return LoopOutcome("模型输出达到预算上限，结果不完整，请缩小任务后重试。", "error")
        except ModelProtocolError:
            logger.error("[Brain] 模型响应协议异常，未使用不完整结果")
            return LoopOutcome("模型返回不完整或格式异常，请稍后重试。", "error")
        except asyncio.TimeoutError:
            logger.error(
                f"[Brain] LLM 请求超时 (stream={getattr(brain, '_stream_active', False)}, "
                f"重试 {brain.LLM_MAX_RETRIES} 次仍超时)"
            )
            return LoopOutcome("思考超时，模型响应太久，请稍后再试。", "timeout")
        except Exception:
            logger.error(f"[Brain] LLM 请求重试 {brain.LLM_MAX_RETRIES} 次仍失败")
            return LoopOutcome(
                f"API 网络短路（已重试 {brain.LLM_MAX_RETRIES} 次仍失败）。", "error")

        logger.info(
            f"[tool_loop] 第 {loop_count + 1} 轮 LLM 往返耗时 "
            f"{_loop.time() - _llm_t0:.1f}s (stream={getattr(brain, '_stream_active', False)})"
        )
        if not msg.tool_calls:
            from kernel.model_profile import is_deepseek
            if is_deepseek(getattr(brain, "model_name", "")):
                messages.append(_to_assistant_message(msg))
            final_reply = msg.content or "好的。"
            final_status = "tool_errors" if needs_receipt_review(_run) else "ok"
            logger.info(
                f"[tool_loop] 收尾 (共 {loop_count + 1} 轮, 总耗时 {_loop.time() - _turn_t0:.1f}s)"
            )
            # [0.3.27.0 阶段5] 任务收尾帧: 普通聊天 (_task_started=False) 零帧
            if _task_started or (_run is not None and (_run.tool_receipts or _run.total_tool_calls)):
                try:
                    if _run is not None:
                        _run.status = _task_rt.FAILED if final_status == "tool_errors" else _task_rt.COMPLETED
                        _run.bump_status_frame()
                    await _work_status.publish(
                        brain.bus,
                        _work_status.WORK_STATE_FAILED if final_status == "tool_errors" else _work_status.WORK_STATE_COMPLETED,
                        "本轮结束，存在失败或待核对回执" if final_status == "tool_errors" else "本轮执行结束",
                        run=_run)
                except Exception:
                    pass  # 状态帧永不穿透收尾
            break

        messages.append(_to_assistant_message(msg))

        # [0.3.27.0 阶段5 规格七] 接单 ACK 已收口 (进度永不进聊天, 只进状态栏):
        # 不再调用 maybe_send_batch_interim; task_feedback 保留兼容开关默认关。

        # [0.3.27.0 阶段5] 首个工具调用到达 -> 正式进入任务模式, 发布任务开始
        if not _task_started:
            _task_started = True
            if _run is not None:
                _run.status = _task_rt.RUNNING
                _run.bump_status_frame()
            try:
                await _work_status.publish(
                    brain.bus, _work_status.WORK_STATE_TASK_THINKING,
                    "任务处理中", run=_run)
            except Exception:
                pass  # 状态帧永不穿透工具循环
        # [状态帧] 工具批运行态: 仅工具名 (参数绝不进状态栏, _safe_activity 兜底)
        try:
            if _run is not None:
                _run.bump_status_frame()
            _activity = _work_status.activity_for_tool_calls(
                brain.bus, msg.tool_calls
            )
            await _work_status.publish(
                brain.bus, _work_status.WORK_STATE_TOOL_RUNNING,
                _activity, run=_run)
        except Exception:
            pass  # 状态帧永不穿透工具循环
        # [0.3.27.0 阶段3 串行执行] 删除无条件 gather (规格五.2): 每个工具
        # 完整闭环 (校验->权限->幂等->执行->提交->发射->回填->账本) 后再执行
        # 下一个, 副作用顺序 = 模型返回顺序。parallel_safe=true 只读工具的
        # 并发留待 manifest 声明后准入 (第一版全串行)。
        _tools_t0 = _loop.time()
        brain._turn_stage = f"tool_batch_{loop_count + 1}"  # [0.3.31.0] 卡死点
        _max_steps_hit = False
        if _run is not None:
            _run.tool_batch_rounds += 1
        circuit_triggered = False
        _skip_remaining_reason = ""
        _batch_system_hints: list[dict] = []
        # [0.3.27.0 阶段4 规格六 独占规则] 同响应含 ask_user + 其他工具时,
        # 只处理 ask_user, 其余不执行；但必须为每个 tool_call 回填明确回执，
        # 否则供应商协议会把挂起快照判为不完整，恢复后无法继续。
        _ask_ids = {t.id for t in msg.tool_calls if t.function.name == "ask_user"}
        _research_ids = _select_research_ids(msg.tool_calls, _ask_ids)
        _research_result = None
        for t in msg.tool_calls:
            if _ask_ids and t.id not in _ask_ids:
                logger.info(f"[tool_loop] ask_user 独占: {t.function.name} deferred")
                messages.append(brain._build_tool_message(
                    t.id,
                    "未执行：本批包含 ask_user；用户回答后由模型重新决定是否调用。",
                    None,
                ))
                continue
            if _research_ids and t.id not in _research_ids:
                messages.append(brain._build_tool_message(
                    t.id, "未执行：本批只执行一次公开资料研究，其他操作须由用户另行发起。", None))
                continue
            if _skip_remaining_reason:
                messages.append(brain._build_tool_message(
                    t.id, _skip_remaining_reason, None))
                continue
            # [工具上限 规格五.6] total_tool_calls 独立计数 (与 llm_rounds 分开)
            if _run is not None:
                _run.total_tool_calls += 1
                if _run.total_tool_calls > MAX_TOTAL_TOOL_CALLS:
                    _max_steps_hit = True
                    _skip_remaining_reason = "未执行：本轮工具调用已达上限。"
                    messages.append(brain._build_tool_message(
                        t.id, _skip_remaining_reason, None))
                    _batch_system_hints.append({
                        "role": "system",
                        "content": (
                            "<system_max_steps>\n"
                            "本轮工具调用总数已达上限, 任务已自动停止。\n"
                            "请立即用一句话向用户说明当前进展并收尾, 不要再调用任何工具。\n"
                            "</system_max_steps>"
                        ),
                    })
                    continue
            r = await brain._execute_single_tool(t)
            func_name = r["func_name"]
            # [0.3.27.0 阶段4] ask_user 挂起: 回填 tool_call_id (恢复时同 id 追加回答)
            if (
                _run is not None
                and _run.pending_question is not None
                and func_name == "ask_user"
            ):
                _run.pending_question["tool_call_id"] = t.id
            args = r["args"]
            res_str = r["res_str"]
            call_id = r["call_id"]
            tool_result = r.get("tool_result")
            if t.id in _research_ids:
                from kernel.web_research.receipt import error_receipt
                if tool_result is None or tool_result.app != "web_research":
                    receipt = error_receipt("research_failed")
                    tool_result = ToolResult("web_research", False, "error", receipt, receipt["text"])
                res_str = tool_result.text
                _research_result = tool_result
                if _run is not None:
                    _run._research_terminal_text = res_str
            is_error = (tool_result is not None and not tool_result.ok) or \
                       (tool_result is None and brain._is_tool_error(res_str))
            record_receipt(_run, r, is_error=is_error)

            from kernel.web_research.history import project_tool_event
            event_content, event_metadata, log_content = project_tool_event(
                _run, res_str, research=t.id in _research_ids, call_id=call_id,
            )
            logger.debug(f"  --- Kernel 反馈: {log_content}")
            memory_event = Message.tool_result(
                content=event_content,
                source=func_name,
                active_apps=list(brain.bus.active_apps.keys()),
                **event_metadata,
            )
            if raw_turn:
                await brain.bus.emit_tool_memory(memory_event, turn=raw_turn)
            else:
                await brain.bus.emit_msg(memory_event)
            messages.append(brain._build_tool_message(call_id, res_str, tool_result))
            if t.id in _research_ids:
                continue

            # [熔断器] 优先用结构化 ok 判定; 无 ToolResult 时降级关键词匹配
            if is_error:
                brain._tool_failure_count[func_name] = brain._tool_failure_count.get(func_name, 0) + 1
                failure_count = brain._tool_failure_count[func_name]
                logger.warning(f"[Brain] 工具 {func_name} 连续失败 {failure_count} 次")

                domain = func_name
                skill_disabled = report_failure(domain)
                if skill_disabled:
                    logger.error(f"[Brain] 技能树熔断: 领域 [{domain}] 有技能已被永久禁用")

                if failure_count >= 3:
                    logger.error(f"[Brain] 熔断器触发: {func_name} 连续失败 {failure_count} 次，强制终止任务")
                    cb_channel = brain._current_source if (
                        hasattr(brain, '_current_source')
                        and brain._current_source in brain.bus.active_apps
                    ) else "feishu"
                    if cb_channel in brain.bus.active_apps:
                        abort_msg = (
                            f"抱歉老板，该操作连续尝试 {failure_count} 次均失败"
                            f"（原因：{res_str[:100]}），任务已取消。"
                        )
                        try:
                            await brain.bus.send_instruction(cb_channel, "send_text", text=abort_msg)
                        except Exception as e:
                            logger.warning(f"[Brain] 熔断通知发送失败 ({cb_channel}): {e}")
                    circuit_breaker_hint = (
                        f"<system_circuit_breaker>\n"
                        f"[强制终止指令] 工具 {func_name} 已连续失败 {failure_count} 次，系统已强制终止该任务。\n"
                        f"请立即停止重试，用自然语言向用户道歉并解释失败原因。\n"
                        f"绝不允许再次调用该工具。\n"
                        f"</system_circuit_breaker>"
                    )
                    _batch_system_hints.append({
                        "role": "system", "content": circuit_breaker_hint})
                    circuit_triggered = True
                    _skip_remaining_reason = (
                        f"未执行：工具 {func_name} 已触发连续失败熔断，本批后续调用已取消。")
                else:
                    error_hint = brain._build_error_digest_hint(func_name, args, res_str)
                    _batch_system_hints.append({
                        "role": "system", "content": error_hint})
                    logger.info(f"[Brain] 检测到工具错误，已注入消化提示: {func_name}")
            else:
                if func_name in brain._tool_failure_count:
                    logger.info(f"[Brain] 工具 {func_name} 调用成功，重置失败计数")
                    brain._tool_failure_count[func_name] = 0
                # [0.3.31.3 Batch 3] 成功调用进工具调用记忆 (codex 式任务上下文
                # 数据源; 只记成功, 截断防膨胀, fail-safe)。
                try:
                    from kernel.tool_memory import record_tool_example
                    record_tool_example(
                        func_name, args, res_str,
                        turn_id=getattr(brain, "_current_turn_id", ""))
                except Exception as _rec_e:
                    logger.debug(f"[tool_loop] 工具记忆记账失败(非致命): {_rec_e}")
                # [P0 出站文本入史 0.1.9.12] 收集 use_<app>(send_text) 发出的 text,
                # 供 finalize_reply 落进 chat_history。治"提问丢失": 阿露经工具发的
                # 真实话语原本不进 history, 下一轮 LLM 看不到自己上一轮问了什么。
                if func_name.startswith("use_") or func_name == "instruct_app":
                    _cmd = args.get("command") if isinstance(args, dict) else None
                    if _cmd == "send_text":
                        _params = args.get("params", {}) if isinstance(args, dict) else {}
                        _text = _params.get("text") if isinstance(_params, dict) else None
                        # [0.2.20.0 修复] 被拦复读不入史: 拦截时 res_str 是拒发
                        # 通知, LLM args 的 text 并未真发, 收集会污染 chat_history。
                        if (_text and isinstance(_text, str) and _text.strip()
                                and not brain._is_outbound_denial(res_str)):
                            brain._outbound_texts_this_turn.append(_text.strip())
                    # [阶段4] 出站行为日志: 记"说了什么话", 治阿露不知自己发了什么
                    # 通用匹配 send_* 命令 (send_text/send_native_image/send_file...), 不焊渠道
                    # [0.2.20.0] 被拦截的未遂发送不记 (防 behavior_recall 谎报出站)
                    if (isinstance(_cmd, str) and _cmd.startswith("send_")
                            and not brain._is_outbound_denial(res_str)):
                        _params = args.get("params", {}) if isinstance(args, dict) else {}
                        _target = _params.get("target_user", "") if isinstance(_params, dict) else ""
                        _content = (_params.get("text") or _params.get("path") or _cmd) if isinstance(_params, dict) else _cmd
                        _app = func_name[4:] if func_name.startswith("use_") else ""
                        try:
                            from kernel import behavior_log
                            behavior_log.append_behavior(
                                "outbound", str(_content),
                                app=_app, target_user=str(_target),
                                turn_id=getattr(brain, "_current_turn_id", ""),
                                **brain._principal_audit_kwargs(),
                            )
                        except Exception as e:
                            logger.warning(f"[Brain] behavior_log 写入失败(非致命): {e}")
        # Provider 要求 assistant.tool_calls 后紧跟完整 tool 回执批；系统提示只能
        # 在整批闭合后追加，不能插在两个并列回执之间。
        messages.extend(_batch_system_hints)
        if _research_result is not None:
            complete = _research_result.ok and (_research_result.data or {}).get("status") == "complete"
            if _run is not None:
                _run.status = _task_rt.COMPLETED if complete else _task_rt.FAILED
                _run.stop_reason = "research_complete" if complete else "research_partial"
                _run.bump_status_frame()
            await _work_status.publish(brain.bus,
                _work_status.WORK_STATE_COMPLETED if complete else _work_status.WORK_STATE_FAILED,
                "资料查阅完成" if complete else "资料查阅未完整核实", run=_run)
            return LoopOutcome(_research_result.text, "ok" if complete else "tool_errors")
        if circuit_triggered:
            # [0.3.27.0 阶段2] 熔断轮也是真实模型请求轮, 必须先递增再 continue
            # (修 continue 绕过 loop_count 轮次上限, 规格 五.6)
            loop_count += 1
            # 让下一轮 LLM 在熔断提示下生成最终文本回复 (不再调用工具)
            continue

        # [P0-b 0.2.15.0] 文本出站后不再硬终止循环。
        # 旧版 (0.2.6.1) 一有文本出站就 break 整个工具循环 -- 治住了
        # 2026-07-22 奶狗事件 (同回复换措辞连发3遍), 却误杀了"先回收到
        # 再干活"的正常流程: 2026-07-24 阿露回了"收到! 正在撰写论文"后
        # 循环被掐断, doc_gen 从未被调用, 承诺烂尾 (老板实测: 很慢+没干活)。
        # 新版: 循环继续, 换措辞复读在 _handle_instruct_app 入口对文本发送
        # 类命令逐次机械拦截 (不依赖模型自律); 干活类工具 (doc_gen/
        # image_gen/mcp_*) 与文件/媒体交付 (send_file 等) 正常放行。
        # 空 final_reply 仍合法: finalize_reply 见出站记录非空即放行。

        logger.info(
            f"[tool_loop] 第 {loop_count + 1} 轮 {len(msg.tool_calls)} 个工具批执行耗时 "
            f"{_loop.time() - _tools_t0:.1f}s"
        )
        if _run is not None and _run.pending_question is not None:
            # [0.3.27.0 阶段4 规格六] ask_user 已发: 挂起任务 (状态已由 handler
            # 置 WAITING_USER + 持久化), 返回 suspended, 释放 Brain
            _run.bump_status_frame()
            # handler 的早期快照尚无 call_id/提交/回执；挂起前保存回填后的完整状态。
            try:
                from kernel.task_store import save_run_snapshot
                await save_run_snapshot(brain._task_store(), _run)
            except Exception as exc:
                logger.warning("[task_store] 挂起最终快照保存失败: %s", type(exc).__name__)
            try:
                await _work_status.publish(
                    brain.bus, _work_status.WORK_STATE_WAITING_USER,
                    "等待老板回答", run=_run)
            except Exception:
                pass
            return LoopOutcome("", "suspended")
        if _max_steps_hit:
            # [0.3.27.0 阶段3 规格五.6] 达工具上限返回明确 max_steps (不伪装成功)
            return LoopOutcome("", "max_steps")

        # [抢占软熔断 v0.4.1] 当前轮 LLM+工具已回填完毕 (状态干净), 若老板消息
        # 期间抢占, 注入"提前收尾"提示, 让下一轮 LLM 基于已有结果生成最终文本
        # 后即 break (无 tool_calls -> 上方 break), 不再发起更多工具轮。
        # 软熔断: 不硬 cancel 进行中 API 请求, 避免半截 tool_calls 污染 messages。
        # [v0.4.4 修正] 直接查 bus 旗, 而非入口同步的 _preempt_requested。
        # 否则 think 中途来的老板消息 (旗在 think 入口后才置) 抢占不了, 要等满 8 轮。
        if brain._preempt_requested or getattr(brain.bus, "_master_preempt_flag", False):
            if getattr(brain.bus, "_master_preempt_flag", False):
                brain.bus._master_preempt_flag = False
                brain._preempt_requested = True
            logger.info("[Brain] 老板抢占触发: 当前轮工具回填后提前收尾, 不再继续工具循环")
            messages.append({
                "role": "system",
                "content": (
                    "<system_preempt>\n"
                    "老板刚刚发来新消息, 当前任务需立即收尾让位。\n"
                    "请基于目前已完成的工具结果, 用一句话给出当前进展或结论, "
                    "不要再调用任何工具。后续会处理老板的新消息。\n"
                    "[重要] 不要向当前对话发送任何 send_text。你输出的收尾文本"
                    "系统只会记入内部日志, 绝不会发给任何对话/任何人, 所以不要写"
                    "给对话方看的内容, 也不要交代'下一轮再回/稍后补发'这类安排。\n"
                    "</system_preempt>"
                ),
            })
        loop_count += 1

    if not final_reply:
        final_reply = await summarize_tool_results(brain, messages)
        run = getattr(brain, "_current_run", None)
        if run is not None:
            run.status = _task_rt.FAILED
            run.stop_reason = "max_steps"
            run.bump_status_frame()
        try:
            await _work_status.publish(
                brain.bus,
                _work_status.snapshot(brain.bus, brain)["state"] if run is not None
                else _work_status.WORK_STATE_FAILED,
                "工具轮次已达上限，结果已汇总" if final_reply else "工具轮次已达上限，结果待核对",
                run=run,
            )
        except Exception:
            pass  # 状态帧失败不影响已有工具回执和结果总结。
        return LoopOutcome(final_reply or SUMMARY_UNAVAILABLE, "max_steps", summary_only=True)
    return LoopOutcome(final_reply, final_status)


async def finalize_reply(
    brain, final_reply: str, active_apps: list, is_boot_msg: bool, *, allow_actions: bool = True,
    empty_reply: str = SUMMARY_UNAVAILABLE,
) -> str:
    """[输出终结化] 技能编译 + 净化 + 图片路由 + 历史落盘, 从 think() 抽离。"""
    from kernel.web_research.runtime import terminal_text
    research_text = terminal_text(brain)
    if research_text is not None:
        final_reply = research_text
        allow_actions = False
        display = getattr(getattr(brain, "_current_run", None), "_research_display", None)
        brain._last_research_delivery = (getattr(brain, "_current_turn_id", ""), display)
    # [P0 0.1.9.12] final_reply 空但有 send_text 出站文本时不报"思考超时",
    # 让落盘逻辑把出站文本进 history (阿露只调工具没产最终文本的合法场景)。
    if not final_reply and not getattr(brain, "_outbound_texts_this_turn", []):
        return "思考超时，请稍后再试。"
    if is_boot_msg:
        return final_reply

    # [技能树] 在净化之前，先提取并编译技能配方
    if not allow_actions:
        final_reply = re.sub(r'<skill_recipe>.*?</skill_recipe>', '', final_reply, flags=re.DOTALL)
    if allow_actions and brain._skill_save_requested:
        recipe_match = re.search(
            r'<skill_recipe>\s*(.*?)\s*</skill_recipe>',
            final_reply, re.DOTALL
        )
        if recipe_match:
            recipe_content = recipe_match.group(1).strip()
            final_reply = re.sub(
                r'<skill_recipe>.*?</skill_recipe>',
                '', final_reply, flags=re.DOTALL
            ).strip()
            skill_domain = "general"
            skill_name = ""
            skill_recipe_text = ""
            for line in recipe_content.split("\n"):
                line = line.strip()
                if line.startswith("domain:") or line.startswith("domain："):
                    skill_domain = line.split(":", 1)[-1].split("：", 1)[-1].strip()
                elif line.startswith("name:") or line.startswith("name："):
                    skill_name = line.split(":", 1)[-1].split("：", 1)[-1].strip()
                elif line.startswith("recipe:") or line.startswith("recipe："):
                    skill_recipe_text = line.split(":", 1)[-1].split("：", 1)[-1].strip()
            if not skill_recipe_text:
                skill_recipe_text = recipe_content
            if not skill_name:
                skill_name = f"skill_{datetime.datetime.now().strftime('%H%M%S')}"
            try:
                saved_path = save_skill(
                    skill_name=skill_name,
                    domain=skill_domain,
                    prompt_recipe=skill_recipe_text,
                    metadata={
                        "source": "brain_auto_compile",
                        "trigger_tool": brain._last_tool_called,
                        "conversation_preview": brain._current_user_input[:200] if hasattr(brain, '_current_user_input') else "",
                    }
                )
                logger.info(f"[Brain] 技能自动编译成功: {saved_path}")
            except Exception as e:
                logger.error(f"[Brain] 技能自动编译失败: {e}")
        else:
            logger.warning(
                "[Brain] 检测到技能编译触发词，但 LLM 未输出有效的 <skill_recipe> 标签，"
                "跳过技能保存。"
            )

    # 进行暴力的净化清洗 (_sanitize_output 内部已剥离思考区标签)
    clean_reply = research_text if research_text is not None else brain._sanitize_output(final_reply)

    local_img_path = None
    img_match = re.search(r'\[LOCAL_IMAGE:(.+?)\]', clean_reply)
    raw_path_match = re.search(r'([a-zA-Z]:\\[^\s*<>|"]+\.(?:jpg|jpeg|png|gif|webp))', clean_reply, re.IGNORECASE)

    if research_text is not None:
        img_match = raw_path_match = None
    if img_match:
        local_img_path = img_match.group(1).strip()
        clean_reply = re.sub(r'\[LOCAL_IMAGE:.+?\]', '', clean_reply).strip()
    elif raw_path_match:
        local_img_path = raw_path_match.group(1).strip()
        clean_reply = clean_reply.replace(local_img_path, '').strip()

    if allow_actions and local_img_path and "feishu" in active_apps:
        logger.info(f"[Kernel] 截获原生图片投递请求，移交飞书图床: {local_img_path}")
        try:
            await brain.bus.send_instruction("feishu", "send_native_image", path=local_img_path)
        except Exception as e:
            logger.error(f"[原生图片发送失败]: {e}")

    if not allow_actions and not clean_reply:
        clean_reply = empty_reply

    # [Phase 4] _session_key 延迟导入防循环 (brain 顶层 import 本模块)
    from kernel.brain import _session_key
    async with brain._history_lock:
        out_texts = getattr(brain, "_outbound_texts_this_turn", [])
        cur_source = getattr(brain, "_current_source", "console")
        cur_chat = getattr(brain, "_current_chat_name", "")
        # [Fix 0.1.15.7] idle 主动发声的出站文本落老板私聊 wechat 桶 (而非 idle 桶),
        # 让下一轮 think 看得到自己发过什么 (治"主动发声没进上下文")。
        # idle 的 clean_reply (元话语"已经主动找...") 不进历史 (防污染)。
        if cur_source == "idle":
            skey_source = "wechat"
            persist_reply = False
        else:
            skey_source = cur_source
            persist_reply = True
        # [P0 0.1.9.12] 先落 send_text 出站文本再落 clean_reply, 同 skey 桶。
        # 治"提问丢失": 阿露经工具发的真实话语进 history, 下一轮 LLM 看得到。
        if out_texts or (persist_reply and clean_reply):
            skey = _session_key(cur_chat, skey_source, getattr(brain, "_current_conversation_id", ""))
            bucket = brain.chat_history.setdefault(skey, [])
            for _txt in out_texts:
                # 去重: clean_reply 与该出站文本同文则跳过, 避免连续重复 assistant 轮
                if persist_reply and clean_reply and _txt == clean_reply:
                    continue
                bucket.append({"role": "assistant", "content": _txt})
            if persist_reply and clean_reply:
                # 聊天记录只保存净化后的纯净文本，绝不让草稿污染历史上下文
                # [短期记忆 v0.4.1] 回复落回当前会话桶, 与对应 user 消息同桶保持轮次连续。
                from kernel.web_research.history import history_notice
                remembered = history_notice(getattr(brain, "_current_run", None)) if research_text is not None else clean_reply
                bucket.append({"role": "assistant", "content": remembered})

    return clean_reply
