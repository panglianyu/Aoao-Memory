# kernel/llm_execution.py
"""
[0.3.27.0 工具架构改造 阶段2] LLMExecution Module -- 本地调用策略层。

规格: "把以下 Implementation 从 Brain 收进去: 流式与非流式调用、
VisionMiddleware 注入、超时与重试、搜索参数、思考开关、推理内容内部
保留与展示隔离、响应和工具调用归一化。"

只保留两种本地调用策略:
- FAST_CHAT: 每个新输入的第一次模型调用 (快速认知, 直接回复)。
- TASK_DELIBERATE: 只要首轮产生过工具调用, 之后所有模型调用都使用此策略。

轮次必须按"实际发起模型请求次数"增加 (不能复用当前可能被 continue 绕过的
loop_count) -- run.llm_rounds 由 tool_loop 在每次实际调用成功后递增,
strategy_for 据此派生当前策略。

Brain 保留 _call_llm / _call_llm_stream 同名薄委托 (monkeypatch 点零漂移,
存量测试 patch brain._call_llm 替换实例方法后不经本模块)。
"""
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional

from kernel.logger import get_logger
from kernel.vision_parser import VisionMiddleware
from kernel.model_profile import is_deepseek, request_options
from kernel.model_conversation import (
    ModelOutputLimitError,
    ModelProtocolError,
    ModelServiceError,
    check_request_budget,
    project_messages,
)
from kernel.model_usage import prepare_model_attempt

logger = get_logger(__name__)

# ==========================================
# 调用策略
# ==========================================
FAST_CHAT = "fast_chat"
TASK_DELIBERATE = "task_deliberate"


def _validate_completion(message, finish_reason) -> None:
    if finish_reason == "length":
        raise ModelOutputLimitError("模型输出达到预算上限，结果不完整。")
    if finish_reason is not None and finish_reason not in ("stop", "tool_calls"):
        raise ModelProtocolError("模型响应未正常完成，请稍后重试。")
    if not (getattr(message, "content", None) or "").strip() \
            and not getattr(message, "tool_calls", None):
        raise ModelProtocolError("模型未返回完整的可见回复或工具调用。")


def strategy_for(brain) -> str:
    """当前策略: 本轮任务运行已产生过模型请求 -> TASK_DELIBERATE, 否则 FAST_CHAT。

    阶段1 的 TaskRun.llm_rounds 为唯一计数源 (修 continue 绕过 loop_count)。
    """
    run = getattr(brain, "_current_run", None)
    if run is not None and getattr(run, "llm_rounds", 0) > 0:
        return TASK_DELIBERATE
    return FAST_CHAT


# ==========================================
# 流式返回形状 (与 brain 原 _LLMReply 同形, 非流式路径零差异)
# ==========================================
@dataclass
class _FnView:
    name: str
    arguments: str


@dataclass
class _ToolCallView:
    id: str
    function: _FnView


@dataclass
class _LLMReply:
    content: str
    tool_calls: Optional[list]
    reasoning_content: Optional[str] = None


# ==========================================
# 非流式调用
# ==========================================
async def call_llm(
    brain, messages: list, strategy: str = FAST_CHAT, *, summary_only: bool = False,
) -> object:
    """[非流式] LLM 核心请求链手写指数退避重试 (零新依赖)。

    与旧 Brain._call_llm 逐行等价 (阶段2 纯搬体 + strategy 参数):
    - qwen enable_search 按当前输入/强制搜索旗标开;
    - VisionMiddleware.inject_vision 注入;
    - asyncio.wait_for 显式防 hang;
    - LLM_MAX_RETRIES 次重试, 指数退避。
    strategy 当前仅作日志归因与扩展点 (思考开关/搜索参数按策略差异化的
    预留位, 现两策略行为一致, 零行为变化)。
    summary_only 保留同一策略，但禁用工具、内置搜索及 SDK/应用重试。
    """
    mode = getattr(getattr(brain, "_current_run", None), "thinking_mode", "enabled")
    extra = request_options(brain.model_name, thinking_mode=mode).get("extra_body", {})
    last_err = None
    attempts = 1 if summary_only else brain.LLM_MAX_RETRIES
    for attempt in range(attempts):
        client = prepare_model_attempt(
            brain, retry=bool(attempt), summary_only=summary_only,
        )
        try:
            processed = await VisionMiddleware.inject_vision(messages)
            if is_deepseek(brain.model_name):
                processed = project_messages(processed)
                check_request_budget(processed, [] if summary_only else brain._get_system_tools())
            # 请求内容落日志 (DEBUG, 摘要防撑爆)
            try:
                sys_preview = next(
                    (m.get("content", "") if isinstance(m, dict) else getattr(m, "content", ""))
                    for m in processed
                    if (m.get("role") if isinstance(m, dict) else getattr(m, "role", "")) == "system"
                )
                user_msgs = [
                    m for m in processed
                    if (m.get("role") if isinstance(m, dict) else getattr(m, "role", "")) == "user"
                ]
                last_user = (
                    user_msgs[-1].get("content", "")
                    if isinstance(user_msgs[-1], dict) else getattr(user_msgs[-1], "content", "")
                ) if user_msgs else ""
                if isinstance(last_user, list):  # 多模态 user
                    last_user = " ".join(
                        p.get("text", "") for p in last_user
                        if isinstance(p, dict) and p.get("type") == "text"
                    )
                logger.debug(
                    f"[LLM请求] strategy={strategy} attempt={attempt + 1} "
                    f"model={brain.model_name} sys={str(sys_preview)[:120]!r} "
                    f"user={str(last_user)[:200]!r} "
                    f"enable_search={'enable_search' in extra}"
                )
            except Exception:
                pass
            tool_options = {"tool_choice": "none"} if summary_only else {
                "tools": brain._get_system_tools(), "tool_choice": "auto",
            }
            resp = await asyncio.wait_for(
                client.chat.completions.create(
                    model=brain.model_name,
                    messages=processed,
                    **tool_options,
                    extra_body=extra,
                ),
                timeout=getattr(brain, "LLM_TIMEOUT", 45),
            )
            _validate_completion(
                resp.choices[0].message,
                getattr(resp.choices[0], "finish_reason", None),
            )
            # 原始响应落日志 (DEBUG)
            try:
                raw = resp.choices[0].message
                tool_calls = getattr(raw, "tool_calls", None)
                tc_summary = (
                    f" tool_calls={[tc.function.name for tc in tool_calls]}"
                    if tool_calls else ""
                )
                logger.debug(f"[LLM响应] content={str(raw.content)[:300]!r}{tc_summary}")
            except Exception:
                pass
            return resp
        except (ModelOutputLimitError, ModelProtocolError, ModelServiceError):
            raise
        except Exception as e:
            last_err = e
            err_desc = (
                "请求超时(timeout)" if isinstance(e, asyncio.TimeoutError)
                else "模型服务请求失败"
            )
            logger.warning(
                f"[Brain] LLM 请求第 {attempt + 1}/{attempts} 次失败: {err_desc}"
            )
            if attempt < attempts - 1:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s 指数退避
    if isinstance(last_err, asyncio.TimeoutError):
        raise last_err
    raise ModelServiceError("模型服务请求失败，请稍后再试。") from None


# ==========================================
# 流式调用
# ==========================================
async def call_llm_stream(brain, messages: list, strategy: str = FAST_CHAT) -> _LLMReply:
    """[流式] 仅 _stream_active 闸门命中路径 (console/桌面对话直连)。

    与旧 Brain._call_llm_stream 逐行等价 (阶段2 纯搬体 + strategy 参数):
    - stream=True; 连接/首包期限 LLM_TIMEOUT; 每 token 等待 _stream_per_token_timeout;
    - reasoning_content 仅留协议字段，不渲染；同块 content/tool_calls 独立处理；
    - content 块过 IncrementalSanitizer -> start/token/reset bus 事件 (懒 start);
    - tool_calls delta 按 index 累积合并成 _ToolCallView, 与非流式同形;
    - 首字可见前失败重试, 首字可见后失败直抛 (不重试刷屏)。
    """
    from kernel.stream_sanitizer import IncrementalSanitizer
    from kernel.message import Message

    sanitizer = IncrementalSanitizer()
    coal_buf = ""
    started = False
    emitted_visible = False
    turn_id = getattr(brain, "_current_turn_id", "")

    async def _emit_piece(piece: str) -> None:
        nonlocal coal_buf, started
        if not piece:
            return
        if not started:
            started = True
            brain._stream_started_this_turn = True
            await brain.bus.emit_msg(Message.agent_stream_start(turn_id=turn_id))
        coal_buf += piece
        if len(coal_buf) >= getattr(brain, "_stream_emit_min_chars", 2):
            await brain.bus.emit_msg(Message.agent_stream_token(
                content=coal_buf, turn_id=turn_id))
            coal_buf = ""

    async def _emit_reset() -> None:
        nonlocal coal_buf
        coal_buf = ""
        if started:
            await brain.bus.emit_msg(Message.agent_stream_reset(turn_id=turn_id))

    async def _drain_sanitizer(events: list) -> None:
        for ev in events:
            if ev.kind == "reset":
                await _emit_reset()
            else:
                await _emit_piece(ev.text)

    mode = getattr(getattr(brain, "_current_run", None), "thinking_mode", "enabled")
    extra = request_options(brain.model_name, thinking_mode=mode).get("extra_body", {})
    per_token = getattr(brain, "_stream_per_token_timeout", 20.0)
    last_err: Optional[Exception] = None
    for attempt in range(brain.LLM_MAX_RETRIES):
        client = prepare_model_attempt(brain, retry=bool(attempt))
        content_parts: List[str] = []
        reasoning_parts: List[str] = []
        reasoning_seen = False
        tool_acc: Dict[int, dict] = {}
        sanitizer = IncrementalSanitizer()
        stream = None
        finish_reason = None
        try:
            selected_tools = brain._get_system_tools()
            hold_content = bool(selected_tools)
            content_chars = 0
            processed = await VisionMiddleware.inject_vision(messages)
            if is_deepseek(brain.model_name):
                processed = project_messages(processed)
                check_request_budget(processed, selected_tools)
            stream = await asyncio.wait_for(
                client.chat.completions.create(
                    model=brain.model_name,
                    messages=processed,
                    tools=selected_tools,
                    tool_choice="auto",
                    stream=True,
                    extra_body=extra,
                ),
                timeout=getattr(brain, "LLM_TIMEOUT", 45),
            )
            aiter = stream.__aiter__()
            while True:
                try:
                    chunk = await asyncio.wait_for(aiter.__anext__(), timeout=per_token)
                except StopAsyncIteration:
                    break
                choices = getattr(chunk, "choices", None)
                if not choices:
                    continue
                chunk_finish = getattr(choices[0], "finish_reason", None)
                if chunk_finish is not None:
                    finish_reason = chunk_finish
                delta = getattr(choices[0], "delta", None)
                if delta is None:
                    continue
                reasoning = getattr(delta, "reasoning_content", None)
                if reasoning is not None:
                    reasoning_seen = True
                    reasoning_parts.append(reasoning)
                piece = getattr(delta, "content", None)
                if piece:
                    content_chars += len(piece)
                    if content_chars > 200000:
                        raise ModelOutputLimitError("Model response exceeded the display buffer")
                    content_parts.append(piece)
                    if not hold_content:
                        emitted_visible = True
                        await _drain_sanitizer(sanitizer.feed(piece))
                tcs = getattr(delta, "tool_calls", None)
                if tcs:
                    for tc in tcs:
                        idx = getattr(tc, "index", 0) or 0
                        slot = tool_acc.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                        if getattr(tc, "id", None):
                            slot["id"] = tc.id
                        fn = getattr(tc, "function", None)
                        if fn is not None:
                            if getattr(fn, "name", None):
                                slot["name"] += fn.name
                            if getattr(fn, "arguments", None):
                                slot["arguments"] += fn.arguments
            if hold_content:
                _validate_completion(_LLMReply(content="".join(content_parts), tool_calls=list(tool_acc) or None), finish_reason)
                if not tool_acc:
                    emitted_visible = bool(content_parts)
                    await _drain_sanitizer(sanitizer.feed("".join(content_parts)))
            # 只在确定不会调用研究工具后释放可选工具轮的正文。
            await _drain_sanitizer(sanitizer.flush())
            if coal_buf:
                if not started:
                    started = True
                    brain._stream_started_this_turn = True
                    await brain.bus.emit_msg(Message.agent_stream_start(turn_id=turn_id))
                await brain.bus.emit_msg(Message.agent_stream_token(
                    content=coal_buf, turn_id=turn_id))
                coal_buf = ""
            tool_calls = None
            if tool_acc:
                tool_calls = [
                    _ToolCallView(
                        id=v["id"] or f"call_stream_{i}",
                        function=_FnView(name=v["name"], arguments=v["arguments"]),
                    )
                    for i, v in sorted(tool_acc.items())
                ]
            content = "".join(content_parts)
            _validate_completion(
                _LLMReply(content=content, tool_calls=tool_calls), finish_reason)
            logger.debug(
                f"[LLM流式响应] strategy={strategy} content={content[:300]!r} "
                f"tool_calls={[tc.function.name for tc in tool_calls] if tool_calls else ''}"
            )
            return _LLMReply(
                content=content, tool_calls=tool_calls,
                reasoning_content="".join(reasoning_parts) if reasoning_seen else None,
            )
        except (ModelOutputLimitError, ModelProtocolError, ModelServiceError):
            raise
        except Exception as e:
            last_err = e
            err_desc = (
                "请求超时(timeout)" if isinstance(e, asyncio.TimeoutError)
                else "模型服务请求失败"
            )
            logger.warning(
                f"[Brain] 流式 LLM 第 {attempt + 1}/{brain.LLM_MAX_RETRIES} 次失败: "
                f"{err_desc} (首字可见={emitted_visible})"
            )
            if emitted_visible:
                break
            if attempt < brain.LLM_MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
        finally:
            close = getattr(stream, "close", None)
            if close is not None:
                try:
                    await asyncio.wait_for(close(), timeout=2)
                except Exception:
                    logger.warning("[Brain] 模型流清理未完成")
    if isinstance(last_err, asyncio.TimeoutError):
        raise last_err
    raise ModelServiceError("模型服务请求失败，请稍后再试。") from None
