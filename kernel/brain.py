# G:\Aoao_OS\kernel\brain.py
"""
Aoao OS 主脑推理引擎 (Router Agent)

核心职责：
  - 精准理解用户意图（闲聊 vs 任务请求）
  - 智能分发任务（只在必要时调用工具）
  - 用自然语言与用户交流（闲聊时绝不动用工具）

[架构红线 - Native 第一公民特权]
Brain 是 Aoao OS 的"内轨"核心组件，享有以下特权：
  - 高优先级、低延迟的 AppBus 直接通信
  - 主动注入系统记忆上下文（system_memory）
  - 后台守护进程独立性（ReflectionDaemon）

绝不允许将 Brain 降级为 MCP Server 或任何外部协议包装。
"""
import json
import yaml
import os
import base64
import datetime
import hashlib
import re
import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional, Callable, List
from openai import AsyncOpenAI
from kernel.app_bus import AppBus
from kernel.tool_result import ToolResult
from kernel.web_research.display import delivery_metadata
from kernel.tool_builder import _sanitize_tool_name, tools_for_model
from kernel.message import Message
from kernel.prompts import build_system_prompt
from kernel.vision_parser import VisionMiddleware
from kernel.config_loader import load_api_config
from kernel.model_profile import configured_model, is_deepseek, mode_groups
from kernel.model_conversation import ModelProtocolError, ProtocolHistory, protocol_key, remember_run
from kernel.message_reviewer import MessageReviewer
from kernel.speak_gate import GateDecision, SpeakGate
# [0.3.18.1 Phase 2] require_owner 随工具处理器迁 kernel/tool_handlers.py, 此处仅留 Principal
from kernel.principal import Principal
# [0.3.18.2 Phase 3] is_narration_statement/is_silence_meta_statement 随出站路由迁 kernel/brain_outbound.py
from kernel.stream_sanitizer import IncrementalSanitizer
# [0.3.18.0 Brain 收缩 Phase 1] 纯函数层拆出模块 (Brain 保留同名薄委托):
from kernel.brain_messages import (
    build_error_digest_hint as _msg_build_error_digest_hint,
    build_tool_message as _msg_build_tool_message,
    build_user_message as _msg_build_user_message,
    coerce_params as _msg_coerce_params,
    extract_data_url_from_image_data as _msg_extract_data_url,
    is_outbound_denial as _msg_is_outbound_denial,
    is_tool_error as _msg_is_tool_error,
    parse_interest_list as _msg_parse_interest_list,
    truncate_result as _msg_truncate_result,
)
from kernel.brain_sanitize import sanitize_output as _sanitize_output_fn
# [0.3.21.0 Hermes 式任务循环] 接单 ACK 反馈层拆出模块 (Brain 保留薄委托):
# [0.3.21.0 review H1] 轮间忙时回执看门狗: 批阅 think 阻塞主循环, 队列内也要跑
from kernel.task_queue import TaskQueue as _TqTaskQueue
from kernel.task_queue import _msg_ack_key as _tq_msg_ack_key
# [Phase 4 红线2] 下一行导入是存量测试 monkeypatch 点 (kernel.brain._tq_busy_watch_once),
# kernel/tool_loop.py 运行期经本模块惰性查找 -- 勿当未使用导入删除。
from kernel.task_queue import busy_watch_once as _tq_busy_watch_once
# [0.3.22.1 P3] 工作状态栏: think 入/出 + 工具批发射状态帧 (web/bridge 转 WS)
from kernel import work_status as _work_status
# [0.3.27.0 阶段1 TaskRun] 任务运行时 Module: 本轮不可变身份与可恢复状态宿主
from kernel import task_runtime as _task_rt
from kernel.task_runtime import TaskRun as _TaskRun
from kernel import llm_execution as _llm_exec
# [0.3.24.0 Phase 4] 工具循环实现层搬体: 薄委托保方法名; 重导出保 import 路径
from kernel import tool_loop as _tool_loop
from kernel.tool_loop import LoopOutcome, _to_assistant_message  # noqa: F401
# [0.3.18.1 Brain 收缩 Phase 2] 工具处理器层拆出模块 (Brain 保留同名薄委托):
from kernel.tool_handlers import (
    handle_adjust_impression as _th_adjust_impression,
    handle_behavior_recall as _th_behavior_recall,
    handle_boost_interest as _th_boost_interest,
    handle_connect_app as _th_connect_app,
    handle_disconnect_app as _th_disconnect_app,
    handle_instruct_app as _th_instruct_app,
    handle_learn_knowledge as _th_learn_knowledge,
    handle_memory_forget as _th_memory_forget,
    handle_memory_recall as _th_memory_recall,
    handle_memory_remember as _th_memory_remember,
    handle_ask_user as _th_ask_user,
    handle_web_search as _th_web_search,
    refresh_interest_keywords as _th_refresh_interest_keywords,
)
# [0.3.18.2 Brain 收缩 Phase 3] 出站路由层拆出模块 (Brain 保留同名薄委托):
from kernel.brain_outbound import (
    acquire_outbound_slot as _ob_acquire_outbound_slot,
    auto_route_group_reply as _ob_auto_route_group_reply,
    claim_structured_outbound as _ob_claim_structured_outbound,
    handle_submit_reply as _ob_handle_submit_reply,
    record_outbound_text as _ob_record_outbound_text,
    reply_source_is_gated as _ob_reply_source_is_gated,
    resolve_reply_channel as _ob_resolve_reply_channel,
)
from kernel.logger import get_logger
from kernel.skill_manager import (
    get_env_hash,
    save_skill,
    report_failure,
    load_active_skills,
    get_all_active_skills,
    SKILLS_DIR,
)

logger = get_logger(__name__)

# [0.2.23.7 review M2] 重入拒绝回复常量: think 重入锁返回值。
# agent_runtime boot 播报据此判定"撞上用户 think 被拒" (import 同源,
# 治跨模块魔法字符串隐式契约 -- 改文案不再让播报在用户对话中插嘴)。
BUSY_REPLY = "（系统正忙，稍等片刻。）"


# 外设推送的消息带信封头: "[外部感知:wechat | 发件人:X | 对话:Y] 实际内容"。
# 进记忆前必须剥信封, 否则信封里的"对话:群名"会被实体提取污染成永久记忆。
_ENVELOPE_RE = re.compile(r"^\[外部感知:[^\]]*\]\s*")


def _strip_envelope(text: str) -> str:
    """剥离 [外部感知:...] 信封头, 返回纯用户话语。无信封则原样返回。"""
    if not text:
        return ""
    return _ENVELOPE_RE.sub("", text, count=1)


# [0.2.20.0 流式输出] 流式 LLM 调用的统一返回形状, 与非流式路径的
# resp.choices[0].message 同形 (_run_tool_loop / _execute_single_tool 按
# msg.content / msg.tool_calls / tc.id / tc.function.name / tc.function.arguments
# 原样消费, 两条路径对下游零差异)。
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


# [0.3.24.0 Phase 4] _to_assistant_message / LoopOutcome 迁 kernel/tool_loop.py,
# 文件头部重导出保旧 import 路径 (存量 from kernel.brain import ... 零改动)。

# [短期记忆 v0.4.1] 会话桶 key 归一: 空会话/控制台/批阅/空闲/开机统一进 _system 桶,
# 避免这些无明确对话方的上下文散落成无数空桶; 真实渠道(微信群/私聊/飞书)用 chat_name。
def _session_key(chat_name: str, source_channel: str, conversation_id: str = "") -> str:
    if conversation_id:
        return "conversation:" + conversation_id
    if chat_name:
        return chat_name
    if source_channel in ("console", "multi", "idle", "boot"):
        return "_system"
    return source_channel or "_system"


# [0.2.8.0 出站收口] submit_reply 单次代发契约工具 schema。structured_reply 开关
# (config/gate.yaml) 开启时并入工具面 (_get_system_tools); 关闭整体回退 P0 路径。
_SUBMIT_REPLY_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_reply",
        "description": (
            "提交回复, 系统代发 (同一对话一轮一条, 重复同对话会被拦截; 不同对话可各发一条)。"
            "不想回复就不调用 (合法已读不回, 无需解释)。"
            "注意: 你直接输出的纯文本只会进控制台日志, 用户收不到, 回复必须走本工具。"
            "【跨对话回信·重要】target 是这条话要发去的对话名。若你要回答/问候的人"
            "不是本轮跟你说话的人(例如老板让你回他爸/回某朋友), 必须把 target 设为"
            "那个人的对话名(如'曹宏'), 否则话会发错或发不出——你写在正文里'给XX的话'"
            "不会自动转达。此时本轮可发两条: 一条 target=对方(实际答复) + 一条 "
            "target=老板(简短确认'已转告/已回他'), 不同 target 不算重复。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "要发送的回复正文 (必填, 非空)"},
                "target": {"type": "string", "description": "目标对话精确名 (必填, 从快照可用对话列表取)"},
            },
            "required": ["text", "target"],  # [0.2.9.1] target 必填防空目标发错对话撒谎
        },
    },
}


# [0.3.36.0 B2c] 召回 token 预算 (研究借鉴: Hindsight max_tokens 截断 +
# OpenViking CJK 感知估算)。chars/4 通用估算对中文低估 4-6 倍, 这里 CJK
# 码位按 1.5 token、其余按 0.3 估, 与真实分词器量级对齐。
def _estimate_tokens(text) -> int:
    """CJK 感知 token 估算。空/非串返 0。"""
    if not text or not isinstance(text, str):
        return 0
    cjk = sum(1 for ch in text if ord(ch) >= 0x3000)
    return int(cjk * 1.5 + (len(text) - cjk) * 0.3) + 1


def _truncate_to_token_budget(text: str, budget: int) -> str:
    """按行从头部保留, 累计超预算即截掉尾部 (调用方已按优先级降序)。

    budget<=0 原样返回 (回退开关); 空行不计入。
    """
    if budget is None or budget <= 0 or not text:
        return text or ""
    kept: list = []
    used = 0
    for line in str(text).splitlines():
        cost = _estimate_tokens(line)
        if used + cost > budget:
            break
        kept.append(line)
        used += cost
    return "\n".join(kept)


class Brain:
    # 耗时工具列表：这些工具执行时间较长，需要提前给用户发送"安心反馈"
    TIME_CONSUMING_TOOLS = {
        "native_controller": "正在调取视觉神经，截取屏幕中..."
    }

    # 技能编译触发词：老板说出这些关键词时，自动将当前成功经验编译为技能
    SKILL_TRIGGER_PHRASES = [
        "总结成技能", "记住这次成功经验", "记住这个成功经验",
        "记住这个经验", "保存为技能", "编译成技能", "记录成技能",
        "沉淀为技能", "固化为技能", "写成技能", "存成技能",
    ]

    def __init__(self, bus: AppBus):
        self.bus = bus
        api_config = load_api_config()
        self.model_name = configured_model(api_config)
        self.client = AsyncOpenAI(
            base_url=api_config.get("base_url"),
            api_key=api_config.get("api_key"),
            max_retries=0,
        )
        self.character_config = self._load_config()
        # [v0.2.6.0] 上下文运行时配置 (窗口/warm_recall 上限), 缺 brain.yaml 回退默认。
        self.brain_config = self._load_brain_config()
        # [短期记忆 v0.4.1] chat_history 按会话(chat_name)分桶, 治多渠道串台。
        # 老板私聊桶不被群噪音挤掉; multi/idle/boot 进 "_system" 巡视桶 (短窗口)。
        self.chat_history: Dict[str, list] = {}
        self._model_history = ProtocolHistory()
        # [v0.2.6.0] 窗口/预热上限从 config/brain.yaml 读, 缺键回退原硬编码默认。
        _ctx_cfg = self.brain_config.get("context", {}) or {}
        self._SESSION_WINDOW = int(_ctx_cfg.get("session_window", 15))   # 普通会话桶保留轮数
        self._SYSTEM_WINDOW = int(_ctx_cfg.get("system_window", 6))      # _system 巡视桶保留轮数
        self._WARM_RECALL_CHARS = int(_ctx_cfg.get("warm_recall_chars", 300))
        self._WARM_RECALL_INJECT_CAP = int(_ctx_cfg.get("warm_recall_inject_cap", 1200))
        # [0.3.36.0 B2b] 跨轮去重窗口 (研究借鉴 OpenViking dedup_turns): 近期已
        # 注入的召回行不重复注入。0=关 (回退开关)。
        self._DEDUP_TURNS = int(_ctx_cfg.get("dedup_turns", 5) or 0)
        # 每个 agent/chat/scope 独立维护按轮次分桶的注入哈希。
        self._warm_injected_bufs: dict[tuple[str, str, str], deque] = {}
        # [0.3.36.0 B2c] warm 召回 token 预算 (研究借鉴 Hindsight max_tokens):
        # 0=关回退字符截断; >0 按 CJK 感知预算截断, 字符上限双保险。
        self._WARM_RECALL_TOKEN_BUDGET = int(
            _ctx_cfg.get("warm_recall_token_budget", 300) or 0)
        self._history_lock = asyncio.Lock()
        # [0.3.31.0 Batch 0] 整轮看门狗: think/think_batch 单轮硬上限, 超限
        # 优雅收尾兜底回复, 绝不无限吊死; _turn_stage 记最后阶段供卡死归因。
        _wd_cfg = self.brain_config.get("watchdog", {}) or {}
        self._TURN_TIMEOUT = float(_wd_cfg.get("turn_timeout", 180) or 0)
        self._turn_stage = ""
        # [0.3.31.1 Batch 1] 工具 handler 执行兜底上限 + 空闲轮工具边界
        self._TOOL_HARD_TIMEOUT = float(_wd_cfg.get("tool_hard_timeout", 300) or 0)
        self._IDLE_TOOLS = str(_wd_cfg.get("idle_tools", "kernel_only") or "kernel_only")
        # [0.3.31.2 Batch 2] LLM 单呼连接窗 45->60 (流式侧另有 per_token 看门狗;
        # llm_execution getattr(brain, LLM_TIMEOUT, 45) 据此生效)
        self.LLM_TIMEOUT = float(_wd_cfg.get("llm_timeout", 60) or 60)
        # [0.3.31.3 Batch 3] codex 式任务上下文装配层开关
        _tc_cfg = self.brain_config.get("task_context", {}) or {}
        self._TASK_CONTEXT_ENABLED = bool(_tc_cfg.get("enabled", True))
        # [0.3.31.1 Batch 1] 空闲轮被抑制的任务结论移交位 (批阅注入参考)
        self._idle_task_handoff = ""

        # [内核2.0] think 重入锁: 心跳 think_batch 与 web_ui 直连 think 互斥,
        # 一方在跑另一方直接返回, 防并发污染 chat_history / 工具循环状态。
        self._thinking: bool = False
        # [抢占软熔断 v0.4.1] 老板 priority 消息进来时由 AppBus 置 bus._master_preempt_flag;
        # think 入口把旗同步到本标志, _run_tool_loop 每轮循环开头检查, 命中则在当前
        # LLM/工具轮回填完成后 break, 让本次 think 提前结束。不硬中断进行中 API 请求
        # (防 tool_calls 半截状态污染 messages)。
        self._preempt_requested: bool = False
        # [0.1.15.5] 出站熔断锁: 治 LLM 一轮并发多 send_text 绕过同对话去重
        # (gather 并发 _execute_single_tool -> _handle_instruct_app, 两个 send_text
        # 同时检查 _targets 空 -> 都通过 -> 都发, aoao.log 2026-07-06 15:00:02 实证)
        self._outbound_lock = asyncio.Lock()
        # [内核2.0] 脑端活动状态 (发呆/监控编译/自言自语), 注入 prompt 给阿露"主观意识"。
        self._current_activity: str = "待机"
        # [权限闸门] 本轮 think 触发者是否老板本人 (is_master)。
        # 由 think/_think_impl 入口按 is_master 参数设置; add_listen_chat/remove_listen_chat
        # 等改变监听范围的操作据此放行 (拦截阿露 idle 自作主张去锁定/退出群)。默认 False=拒。
        self._current_is_master: bool = False

        # [防抖机制] 当前任务流中是否已发送过"正在执行"安心反馈
        # 同一用户任务流中只允许发送一次，防止重试刷屏
        self._interim_sent = False
        # [0.3.21.0] 接单 ACK 防抖旗标: 每任务流一条接单确认 (_think_impl 入口重置)
        self._task_ack_sent = False

        # [阶段2 0.1.12.2] 分身审核 agent (语义层): 分组+权重+本轮/暂缓分发+新面孔 Tag。
        # think_batch 每组 think 前审, 老板批免审, fallback 降级不阻塞。
        self.reviewer = MessageReviewer(self)
        # [P1 0.2.7.0 阶段1] 发言闸门 (渠道无关规则层): think_batch 每组 think 前判"回不回"。
        # 群聊默认潜水, 老板1v1/@阿露/强触发才过闸。纯规则零 LLM 延迟, 不依赖 reviewer。
        # think_batch 另有 lazy-init 兜底 (测试夹具 Brain.__new__ 跳过 __init__)。
        self._speak_gate = SpeakGate.load()
        # [H4 0.1.12.2] 本组 senders/chat 供 _handle_instruct_app 熔断判群聊(多人各回放行)
        self._current_group_senders: list = []
        self._current_group_chat: str = ""

        # [熔断器] 记录每个工具的连续失败次数，超过 3 次强制终止
        self._tool_failure_count: Dict[str, int] = {}

        # [MemoryProvider] 记忆后端 app 名称缓存，通过 bus.find_memory_provider() 发现
        # 替代旧的硬编码 "system_memory"，任何实现记忆接口的 app 都可以作为后端
        self._memory_provider: Optional[str] = None

        # [技能树] 当前对话中触发了技能编译请求的标记
        self._skill_save_requested: bool = False
        # [技能树] 当前对话中最后调用的工具名 (用于定位技能所属领域)
        self._last_tool_called: Optional[str] = None
        # [技能树] 当前对话中加载的技能元数据 (用于失败上报)
        self._active_skill_meta: Dict[str, List[Dict]] = {}

        # [内核2.0] 本轮 think 是否已通过 use_<app> 显式发出过出站消息。
        # main_god 据此判断: 脑端若已自送, reply 视为自言自语不重复路由;
        # 若没送且 reply 实质非空, 则把 reply 路由到本批主对话方 (防回复丢后台)。
        self._outbound_sent_this_turn: bool = False
        # [P0 出站文本入史 0.1.9.12] 本轮经 use_<app>(send_text) 发出的真实话语文本。
        # _run_tool_loop 收集, _finalize_reply 落进 chat_history, 治"提问丢失":
        # 阿露发问走工具不走 reply, 原本下一轮 LLM 看不到自己问过什么。
        self._outbound_texts_this_turn: list = []
        # [跨对话确认回信 0.1.10.1] 本轮 send_text 的 (app, target) 列表,
        # 供主循环判定老板是否直接收到: 跨对话发(给曹宏)则兜底回信确认给老板。
        self._outbound_targets_this_turn: list = []
        # [0.2.8.0 出站收口] 本轮是否已 submit_reply 契约代发。双重职责:
        # True -> 主循环兜底路由绝不重发 (双发防护)。
        self._structured_outbound_this_turn: bool = False
        # [0.2.22.0] 契约锁改为按 target: 同对话一轮一条, 不同对话各一条 (跨聊回信)。
        # 主循环兜底抑制仍用上面的 bool (任一代发即抑制隐式收尾)。
        self._structured_targets_this_turn: set = set()
        # [单轮出站熔断 v2] 按类型分计数: 1 文本 + 1 图片/文件/语音 每轮。
        # v1 总数 1/轮 致"发文本+发图"被拦 (图发不出 -> 自言自语说发了但群收不到,
        # 2026-06-25 bug)。v2 仍防"两文本/两图"刷屏, 但允许"文本+图"组合。
        self._outbound_text_count: int = 0
        self._outbound_media_count: int = 0
        self._OUTBOUND_TEXT_LIMIT: int = 1
        self._OUTBOUND_MEDIA_LIMIT: int = 1
        # [Problem 3 0.1.10.3] 跨对话文本总上限 (不同对话各 1 条, 最多 3 条/轮防 runaway)
        self._OUTBOUND_TEXT_TOTAL_LIMIT: int = 3
        self._OUTBOUND_TEXT_COMMANDS = frozenset({"send_text", "send_link", "send_moment"})
        self._OUTBOUND_MEDIA_COMMANDS = frozenset({"send_native_image", "send_image", "send_file", "send_audio"})

        # [0.2.20.0 流式输出] 流式配置 (brain.yaml streaming 段, 缺键全回退默认 =
        # 改造前行为)。仅 enabled_sources 列出的源走流式 LLM 调用 (默认 console,
        # 即桌面/控制台直连); wechat/feishu/idle/boot 保持非流式原样, 零回归面。
        _stream_cfg = self.brain_config.get("streaming", {}) or {}
        self._stream_enabled: bool = bool(_stream_cfg.get("enabled", True))
        self._stream_sources: list = list(_stream_cfg.get("enabled_sources", ["console"]))
        self._stream_per_token_timeout: float = float(_stream_cfg.get("per_token_timeout", 20))
        self._stream_emit_min_chars: int = int(_stream_cfg.get("emit_min_chars", 2))
        # [0.2.20.0] 本轮 think 流式运行态: _current_turn_id 本轮唯一标识 (顺带治
        # behavior_log 一直拿空 turn_id 的旧账); _stream_active/_stream_started_this_turn
        # 闸门流事件发射 (懒 start: 首个可渲染片段才发, 纯工具轮不起幻象气泡);
        # _last_think_status 还可为 max_steps：可交付进度说明，任务并未完成。
        self._current_turn_id: str = ""
        self._stream_active: bool = False
        self._stream_started_this_turn: bool = False
        self._last_think_status: str = "ok"

        # =================== [次世代工具流微内核] ===================
        # [派发注册表] 取代巨型 if/elif 长链, func_name -> async handler
        self._tool_dispatch_registry: Dict[str, Callable] = {
            "connect_app": self._handle_connect_app,
            "disconnect_app": self._handle_disconnect_app,
            "instruct_app": self._handle_instruct_app,
            "learn_knowledge_from_file": self._handle_learn_knowledge,
            "memory_recall": self._handle_memory_recall,
            "behavior_recall": self._handle_behavior_recall,
            "memory_remember": self._handle_memory_remember,
            "memory_forget": self._handle_memory_forget,
            "web_search": self._handle_web_search,
            "memory_adjust_impression": self._handle_adjust_impression,
            "submit_reply": self._handle_submit_reply,  # [0.2.8.0] 单次代发契约
            "boost_interest": self._handle_boost_interest,  # [0.2.9.0] 老板兴趣关注
            "ask_user": self._handle_ask_user,  # [0.3.27.0 阶段4] 询问老板 (挂起恢复)
        }
        # [脏标记缓存] 工具表仅在 app connect/disconnect 时变化, 无故每轮重建是白做工
        self._cached_system_tools: Optional[list] = None
        self._tools_is_dirty: bool = True
        # [防爆] 工具返回值软上限字符数 (超出按头尾各 1000 截断)
        self.MAX_RESULT_CHARS = 2000
        # [重试] LLM 核心请求最大重试次数 (含首次)。
        # [0.3.20.0 止血] 3 -> 1: 2026-08-03 多群超时事故, 3 次 x 45s 空等约
        # 2.25 分钟, 期间全渠道消息排队越积越多 (14 条/批实证)。兜底文案已不再
        # 出门 (brain_outbound/agent_runtime 三闸门), 快速失败成本低; API 稳态
        # 若需要重试可回调此值。
        self.LLM_MAX_RETRIES = 1

    async def _send_interim_feedback(self, tool_name: str) -> None:
        """[Phase 4] 实现迁 kernel/tool_loop.py, 薄委托保方法名。"""
        await _tool_loop.send_interim_feedback(self, tool_name)

    def _resolve_memory_provider(self) -> str:
        """
        自动发现记忆后端: 通过 bus.find_memory_provider() 查找。
        带缓存失效: 如果缓存的 provider 已不在 available_apps 中，重新扫描。
        """
        # 缓存有效: 已解析且仍在注册表中
        if self._memory_provider and self._memory_provider in self.bus.available_apps:
            return self._memory_provider
        # 重新发现
        provider = self.bus.find_memory_provider()
        if provider:
            logger.info(f"[Brain] 发现记忆提供者: {provider}")
        else:
            logger.warning("[Brain] 未发现记忆提供者 (memory_provider)，记忆注入将被跳过。")
        self._memory_provider = provider
        return provider

    def _load_config(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "config", "character.yaml")
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
        else:
            cfg = {"name": "阿露", "identity": "AI Daughter"}
        # [0.3.17.0 P4-b] 活动智能体人格叠加: 注册表为源, character.yaml 兜底。
        # name/identity 同名覆盖, persona/goals 追加 (空不追加 -> 默认阿露时
        # cfg 与改造前逐键一致), memory_policy 保持全局不覆盖。
        try:
            from kernel.character_registry import get_active
            active = get_active()
            self._active_agent_id = active.id
            cfg = dict(cfg)
            # [review H2] 内置阿露 name/identity 以 character.yaml 为唯一
            # 信源 (注册表不做人设第二信源, 防代码常量静默架空配置);
            # 非内置智能体才用注册表人格覆盖。persona/goals 两者皆可追加。
            if not active.builtin:
                if active.name:
                    cfg["name"] = active.name
                if active.identity:
                    cfg["identity"] = active.identity
            if active.persona:
                cfg["persona"] = active.persona
            if active.goals:
                cfg["goals"] = list(active.goals)
        except Exception as e:
            logger.warning(f"[Brain] 活动智能体读取失败, character.yaml 原样生效: {e}")
        return cfg

    def _load_brain_config(self) -> dict:
        """[v0.2.6.0] 读 config/brain.yaml 上下文运行时配置。缺文件/键回退代码默认。

        镜像 _load_config(character.yaml) 的 yaml.safe_load 范式; 各读取点用各自
        默认值兜底, 任何异常(文件缺失/格式错)都回退空 dict -> 行为同改造前。
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "config", "brain.yaml")
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"[Brain] brain.yaml 读取失败, 回退默认配置: {e}")
        return {}

    # [0.3.18.0 Phase 1] 实现迁 kernel/brain_messages.py, 薄委托保方法名不变
    @staticmethod
    def _coerce_params(raw_params) -> dict:
        return _msg_coerce_params(raw_params)

    def _truncate_result(self, res_str: str) -> str:
        return _msg_truncate_result(res_str, self.MAX_RESULT_CHARS)

    def _get_system_tools(self) -> list:
        """[脏标记缓存] 仅在脏时重建工具树, 否则直读缓存。

        [0.2.8.0] structured_reply 开启时并入 submit_reply 契约工具; 关闭则工具面
        不含它, LLM 只能用旧 use_<app>(send_text) 路径 (一键回退 P0)。
        """
        if self._tools_is_dirty or self._cached_system_tools is None:
            self._cached_system_tools = list(self.bus.generate_dynamic_tools())
            self._tools_is_dirty = False
            logger.debug("[Brain] 工具树脏标记命中, 已重建并缓存。")
        # [0.2.22.1] submit_reply 仅在本轮真有可代发通道时按需提供 (不进缓存, 因 channel 逐轮变):
        # console/桌面直连无 send app (bios 无 console app) -> 不挂, 逼模型走流式/隐式路由,
        # 修桌面/终端 "App console 尚未启动" + 兜底被压制 -> 无回复。wechat/feishu/群 仍挂。
        if self._structured_reply_enabled() and self._resolve_reply_channel():
            return tools_for_model(self._cached_system_tools + [_SUBMIT_REPLY_TOOL], getattr(self, "model_name", ""))
        return tools_for_model(self._cached_system_tools, getattr(self, "model_name", ""))

    def _structured_reply_enabled(self) -> bool:
        """[0.2.8.0] submit_reply 契约开关 (config/gate.yaml structured_reply, 默认开)。"""
        gate = getattr(self, "_speak_gate", None)
        if gate is None:
            gate = self._speak_gate = SpeakGate.load()
        return bool(getattr(gate, "structured_reply", True))

    def _resolve_app_from_tool_name(self, func_name: str) -> Optional[str]:
        """
        [工具树派发] 把 use_<appname> 工具函数名反解为真实 app 注册名。

        工具名经 _sanitize_tool_name 清洗 (非 ASCII 替换下划线),
        故按 available_apps 逐一清洗比对, 取首个匹配。
        """
        if not func_name.startswith("use_"):
            return None
        visible_apps = self.bus._tool_builder.visible_app_names(
            self.bus.available_apps
        )
        for app_name in visible_apps:
            if _sanitize_tool_name(app_name) == func_name:
                return app_name
        return None

    def _prompt_app_lists(self) -> tuple[list[str], list[str], list[str]]:
        """构建系统提示使用的可见应用列表，不影响内核实际注册状态。"""
        available = self.bus._tool_builder.visible_app_names(
            self.bus.available_apps
        )
        visible = set(available)
        active = [name for name in self.bus.active_apps if name in visible]
        dormant = [name for name in available if name not in self.bus.active_apps]
        return available, active, dormant

    async def _call_llm(self, messages: list, *, summary_only: bool = False):
        """[0.3.27.0 阶段2] 实现迁 kernel/llm_execution.py, 薄委托保方法名 (monkeypatch 点)。"""
        options = {"summary_only": True} if summary_only else {}
        return await _llm_exec.call_llm(
            self, messages, strategy=_llm_exec.strategy_for(self), **options)

    # =================== [0.2.20.0 流式输出] ===================

    async def _call_llm_stream(self, messages: list) -> _LLMReply:
        """[0.3.27.0 阶段2] 实现迁 kernel/llm_execution.py, 薄委托保方法名。"""
        return await _llm_exec.call_llm_stream(
            self, messages, strategy=_llm_exec.strategy_for(self))

    # =================== [0.3.15.0 P2 权限协议统一闸门] ===================
    def _hub_target_policy(self, src: str, chat: str):
        """[0.3.26.2 优化2/3/4] 查目标完整策略 (回复策略/免打扰/发送能力/限流)。

        仅对平台通道查询; 未挂 hub / 无策略 / 异常一律回退 None (走通用闸门)。
        """
        if not chat or src not in ("wechat", "feishu"):
            return None
        try:
            hub = getattr(self.bus, "hub", None)
            if hub is None:
                return None
            return hub.find_policy_for(src, chat)
        except Exception as e:
            logger.debug(f"[brain] 目标策略查询失败(非致命): {e}")
            return None

    def _hub_reply_policy(self, src: str, chat: str) -> str:
        """[0.3.25.1] 查目标级回复策略 (platform_hub 挂 bus.hub)。

        仅对平台通道查询; 未挂 hub / 无策略 / 异常一律回退空 (走通用闸门)。"""
        policy = self._hub_target_policy(src, chat)
        return policy.reply_policy if policy is not None else ""

    def _hub_quiet_hours_active(self, src: str, chat: str) -> bool:
        """[0.3.26.2 优化2] 免打扰时间窗判定: quiet_hours.enabled 且当前时间在
        [start, end) 内 -> True (该目标潜水不回复)。跨午夜区间 (22:00-08:00)
        正确处理。默认 False (未配置/异常不拦截)。"""
        policy = self._hub_target_policy(src, chat)
        if policy is None:
            return False
        try:
            qh = getattr(policy, "quiet_hours", None)
            if qh is None:
                return False
            # QuietHours 是 dataclass (非 dict): 用 getattr 兼容两种形态
            enabled = getattr(qh, "enabled", None)
            if enabled is None and isinstance(qh, dict):
                enabled = qh.get("enabled", False)
            if not enabled:
                return False
            start = str(getattr(qh, "start", "") or "").strip()
            end = str(getattr(qh, "end", "") or "").strip()
            if isinstance(qh, dict):
                start = str(qh.get("start", "") or "").strip()
                end = str(qh.get("end", "") or "").strip()
            if not start or not end:
                return False
            import time as _t
            now = _t.localtime()
            now_min = now.tm_hour * 60 + now.tm_min
            s_h, s_m = (int(x) for x in start.split(":"))
            e_h, e_m = (int(x) for x in end.split(":"))
            s_min, e_min = s_h * 60 + s_m, e_h * 60 + e_m
            if s_min < e_min:
                return s_min <= now_min < e_min
            # 跨午夜: 22:00-08:00 -> now >= 22:00 或 now < 08:00
            return now_min >= s_min or now_min < e_min
        except Exception as e:
            logger.debug(f"[brain] 免打扰判定失败(非致命): {e}")
            return False

    def _hub_allow_send_kind(self, src: str, chat: str, kind: str) -> bool:
        """[0.3.26.2 优化3] 发送能力判定: allow_send_image/file/voice。
        未配置策略默认放行 (零行为变化); kind: image/file/voice/plain。"""
        policy = self._hub_target_policy(src, chat)
        if policy is None:
            return True
        try:
            if kind == "image":
                return bool(getattr(policy, "allow_send_image", True))
            if kind == "file":
                return bool(getattr(policy, "allow_send_file", True))
            if kind == "voice":
                return bool(getattr(policy, "allow_send_voice", True))
            return True
        except Exception:
            return True

    def _hub_proactive_allowed(self, src: str, chat: str) -> bool:
        """[0.3.26.2 优化3] 主动发言能力: allow_proactive_speech。
        未配置策略默认放行。"""
        policy = self._hub_target_policy(src, chat)
        if policy is None:
            return True
        try:
            return bool(getattr(policy, "allow_proactive_speech", True))
        except Exception:
            return True

    def _hub_rate_limit_ok(self, src: str, chat: str) -> bool:
        """[0.3.26.2 优化4] 限流判定: max_replies_per_turn / min_reply_interval。
        未配置策略默认放行。轮内计数存 _target_turn_replies, 间隔戳存
        _target_last_reply_ts (每轮重置)。"""
        policy = self._hub_target_policy(src, chat)
        if policy is None:
            return True
        try:
            max_turn = int(getattr(policy, "max_replies_per_turn", 0) or 0)
            min_interval = float(getattr(policy, "min_reply_interval", 0) or 0)
            if max_turn > 0:
                counts = getattr(self, "_target_turn_replies", {})
                if counts.get(chat, 0) >= max_turn:
                    return False
            if min_interval > 0:
                import time as _t
                last = getattr(self, "_target_last_reply_ts", {}).get(chat, 0.0)
                if last and (_t.time() - last) < min_interval:
                    return False
            return True
        except Exception:
            return True

    def _hub_note_reply(self, src: str, chat: str) -> None:
        """[0.3.26.2 优化4] 出站后登记限流计数 (轮内 + 时间戳)。"""
        try:
            counts = getattr(self, "_target_turn_replies", None)
            if counts is None:
                counts = self._target_turn_replies = {}
            counts[chat] = counts.get(chat, 0) + 1
            ts = getattr(self, "_target_last_reply_ts", None)
            if ts is None:
                ts = self._target_last_reply_ts = {}
            import time as _t
            ts[chat] = _t.time()
        except Exception:
            pass

    def _principal_for_gate(self) -> "Principal":
        """闸门取本轮调用主体: 优先 think 链路透传的真实主体; 老夹具/老链路
        仅设 _current_is_master 时兜底派生 (存量闸门测试零破坏)。"""
        p = getattr(self, "_current_principal", None)
        if p is not None:
            return p
        ch = getattr(self, "_current_source", "") or "unknown"
        if getattr(self, "_current_is_master", False):
            return Principal.owner("legacy_owner", ch, "legacy:is_master")
        return Principal.guest("legacy_guest", ch, "legacy:is_master")

    def _principal_audit_kwargs(self) -> dict:
        """behavior_log 审计字段: 谁/哪个渠道/什么身份档 (报告 P1 验收标准)。
        无主体 (异常链路) 返空 dict, 调用方 ** 展开即跳过。"""
        p = getattr(self, "_current_principal", None)
        if p is None:
            return {}
        return {"actor": p.actor_id, "role": p.role, "channel": p.channel}

    def _task_store(self):
        """[0.3.27.0 阶段4] 挂起任务 Store (lazy 单例)。测试可换 MemoryTaskStore。

        生产默认 JSON Adapter (data/task_runtime/, 重启可恢复)。
        """
        store = getattr(self, "_task_store_inst", None)
        if store is None:
            from kernel.task_store import create_store
            store = self._task_store_inst = create_store()
        return store

    def _resume_protocol_compatible(self, run) -> bool:
        """A saved tool transcript may resume only under its original protocol."""
        return not is_deepseek(getattr(self, "model_name", "")) or (
            run.model_name == self.model_name and run.protocol_version == 1
        )

    @staticmethod
    def _resume_identity_compatible(run) -> bool:
        """不猜测旧 multi 快照的来源；只能恢复明确的原老板本地/私聊任务。"""
        principal = getattr(run, "principal", None)
        return (
            isinstance(principal, Principal) and principal.is_owner
            and bool(principal.actor_id) and bool(principal.authenticated_by)
            and run.source not in ("", "multi", "idle", "boot", "system")
            and principal.channel == run.source
            and (run.source in ("web", "console") or run.is_dm is True)
            and run.resume_state_version == 1
        )

    async def _report_resume_failure(self, run, *, reason: str) -> None:
        """Publish a fixed failure without exposing saved content or replaying actions."""
        run.status = _task_rt.FAILED
        run.stop_reason = reason
        run.bump_status_frame()
        self._last_think_status = "error"
        notice = "旧任务与当前模型协议不兼容，无法安全恢复；已停止，且没有重放任何操作。" \
            if reason == "incompatible_protocol" else \
            "任务恢复失败，已安全停止；已完成的操作不会自动重放。"
        try:
            await _work_status.publish(
                self.bus, _work_status.WORK_STATE_FAILED,
                "旧任务无法安全恢复" if reason == "incompatible_protocol" else "任务恢复失败",
                run=run,
            )
        except Exception:
            pass
        if run.source in getattr(self.bus, "active_apps", {}):
            try:
                await self.bus.send_instruction(
                    run.source, "send_text", text=notice, target_user=run.chat_name)
            except Exception as exc:
                logger.warning(
                    "[ask_user] 恢复失败通知未送达 (%s)", type(exc).__name__)

    @asynccontextmanager
    async def _execution_scope(self):
        """新任务、批处理与恢复共用执行权；只允许同一 asyncio Task 重入。"""
        current = asyncio.current_task()
        if getattr(self, "_execution_owner", None) is current:
            yield
            return
        lock = getattr(self, "_execution_lock", None)
        if lock is None:
            lock = self._execution_lock = asyncio.Lock()
        async with lock:
            self._execution_owner = current
            try:
                yield
            finally:
                self._execution_owner = None

    async def _continue_run(self, run) -> None:
        async with self._execution_scope():
            await self._continue_run_unlocked(run)

    async def _continue_run_unlocked(self, run) -> None:
        """[0.3.27.0 阶段4] 在恢复的 TaskRun 上下文中继续任务执行。

        用 run.messages 走 TASK_DELIBERATE (llm_rounds>0 策略); 完成回复
        按 run 原始会话代发 (不继承当前消息的权限/出站目标)。已 COMMITTED
        工具由幂等账本回放, 不重执行。
        """
        raw = getattr(self.bus, "raw_messages", None)
        if raw is not None:
            from kernel.raw_message_models import RawMessageError
            if run.raw_agent_id != getattr(self, "_active_agent_id", ""):
                raise RawMessageError("saved task agent unavailable")
            if (run.raw_turn.get("conversation_id") != run.conversation_id
                    or run.raw_turn.get("event_id") != run.origin_message_id
                    or run.raw_turn.get("source") != run.source):
                raise RawMessageError("saved task source unavailable")
            await self.bus.restore_raw_turn(run.raw_turn)
        self._current_conversation_id = run.conversation_id
        run.status = _task_rt.RUNNING
        self._current_run = run
        # 同步 brain 本轮会话上下文 (finalize 落史/出站路由读取)
        self._current_source = run.source
        self._current_user_input = run.user_input
        self._skill_save_requested = run.skill_save_requested
        self._last_tool_called = run.last_tool_called
        self._active_skill_meta = {}
        self._interim_sent = False
        self._suspended_this_turn = False
        self._last_think_status = "ok"
        self._current_chat_name = run.chat_name
        self._current_principal = run.principal
        self._current_is_master = run.principal.is_owner
        self._current_group_src = run.source
        self._current_group_chat = run.chat_name
        self._current_group_is_dm = run.is_dm
        self._current_turn_id = run.turn_id
        self._stream_started_this_turn = False
        self._stream_active = self._resolve_stream_active(run.source, run.source, False)
        self._outbound_texts_this_turn = run.outbound_texts
        self._outbound_targets_this_turn = run.outbound_targets
        self._outbound_text_count = run.outbound_text_count
        self._outbound_media_count = run.outbound_media_count
        self._outbound_sent_this_turn = bool(run.outbound_targets or run.outbound_media_count)
        self._structured_targets_this_turn = set(run.structured_targets or [])
        self._structured_outbound_this_turn = bool(self._structured_targets_this_turn)
        self._task_ack_sent = run.total_tool_calls > 0
        self._preempt_requested = False
        # 已取得执行权；当前回答入队时留下的旗标不能抢占它自己。
        self.bus._master_preempt_flag = False
        self._tool_failure_count = run.failure_count
        try:
            if not self._resume_protocol_compatible(run):
                await self._report_resume_failure(
                    run, reason="incompatible_protocol")
                return
            self._turn_stage = "tool_loop"  # [0.3.31.0 Batch 0] 卡死点标记
            outcome = await self._run_tool_loop(run.messages)
            if outcome.status == "suspended":
                return  # 再次 ask_user 挂起 (新一轮等待)
            if outcome.status == "max_steps" and not outcome.summary_only:
                run.status = _task_rt.FAILED
                return
            options = {"allow_actions": False} if outcome.summary_only or outcome.status == "tool_errors" else {}
            if outcome.status == "tool_errors":
                options["empty_reply"] = _tool_loop.TOOL_ERRORS_REPLY
            clean_reply = await self._finalize_reply(
                outcome.reply, list(self.bus.active_apps.keys()), False, **options)
            if clean_reply and raw is not None:
                clean_reply = self.bus.reply_text(clean_reply, run.turn_id)
            if outcome.status == "ok":
                remember_run(self, run)
            self._last_think_status = outcome.status
            if clean_reply and clean_reply.strip():
                if run.source in ("web", "console"):
                    # 本地渠道通过事件回 UI，不存在名为 web/console 的发送插件。
                    event = Message.agent_stream_end if (
                        self._stream_active and self._stream_started_this_turn
                    ) else Message.agent_speak
                    await self.bus.emit_msg(event(
                        content=clean_reply, turn_id=run.turn_id,
                        status=outcome.status, chat_name=run.chat_name,
                        **delivery_metadata(self),
                    ))
                else:
                    await self._auto_route_group_reply(
                        run.source, run.chat_name, clean_reply, run.is_dm)
            run.status = _task_rt.COMPLETED if outcome.status == "ok" else _task_rt.FAILED
        except Exception as exc:
            logger.error(
                "[ask_user] 恢复执行异常 (run=%s, type=%s)",
                run.run_id, type(exc).__name__)
            await self._report_resume_failure(run, reason="resume_failed")
        finally:
            self._current_run = None
            self._current_group_src = ""
            self._current_group_chat = ""
            self._current_group_is_dm = False

    def _find_pending_run(self, src: str, chat: str, principal: "Principal | None") -> str:
        """[0.3.27.0 阶段4] 找该 chat 的挂起任务 run_id (回答关联)。

        仅原任务同渠道、同会话、同主体的老板消息可作回答。
        """
        if principal is None or not principal.is_owner or principal.channel != src:
            return ""
        try:
            store = self._task_store()
            for rid in store.list_pending():
                run = store.load(rid)
                if run is None or run.status != _task_rt.WAITING_USER:
                    continue
                if getattr(self.bus, "raw_messages", None) is not None and (
                    run.conversation_id != getattr(self, "_current_conversation_id", "")
                    or run.raw_agent_id != self.bus._raw_turn.get("agent_id")
                ):
                    continue
                if (run.source == src and run.chat_name == chat
                        and run.principal is not None and run.principal.is_owner
                        and run.principal.channel == src
                        and run.principal.actor_id == principal.actor_id):
                    return rid
        except Exception as e:
            logger.warning(f"[ask_user] 挂起任务查找失败(非致命): {e}")
        return ""

    async def resume_ask(self, run_id: str, answer: str,
                         timeout_default: bool = False) -> None:
        async with self._execution_scope():
            await self._resume_ask_unlocked(run_id, answer, timeout_default)

    async def _resume_ask_unlocked(self, run_id: str, answer: str,
                                   timeout_default: bool = False) -> None:
        """[0.3.27.0 阶段4] 用户回答 (或超时安全默认) 后恢复挂起任务。

        关联键: ask_id + run_id + source + chat_name + Principal (store 内校验);
        在同一 tool_call_id 下追加工具结果; 不重放已 COMMITTED 调用。
        "算了"/"取消" 等停止词 -> 取消任务。
        """
        store = self._task_store()
        run = store.load(run_id)
        if run is None:
            logger.warning(f"[ask_user] 恢复失败: 挂起任务不存在 {run_id}")
            return
        if run.status != _task_rt.WAITING_USER:
            logger.info(f"[ask_user] 任务已不在等待态 (status={run.status}), 跳过恢复")
            return
        q = run.pending_question or {}
        if not self._resume_protocol_compatible(run):
            await self._report_resume_failure(
                run, reason="incompatible_protocol")
            from kernel.task_store import save_run_snapshot
            await save_run_snapshot(store, run)
            return
        if not self._resume_identity_compatible(run):
            await self._report_resume_failure(run, reason="unverified_identity")
            from kernel.task_store import save_run_snapshot
            await save_run_snapshot(store, run)
            return
        answer = (answer or "").strip()
        from kernel.task_queue import is_stop_command
        if answer.strip(" 。.!！") in {"取消", "算了", "停止"} or is_stop_command(answer):
            run.status = _task_rt.CANCELLED
            run.bump_status_frame()
            store.delete(run_id)
            await _work_status.publish(
                self.bus, _work_status.WORK_STATE_CANCELLED, "任务已取消", run=run)
            logger.info(f"[ask_user] 老板取消, 任务终止 (run={run_id})")
            return
        if not answer and timeout_default:
            answer = str(q.get("safe_default") or "好，继续。")
            logger.info(f"[ask_user] 超时使用安全默认值恢复 (run={run_id})")
        if not answer:
            answer = "继续"
        answer_message = {
            "role": "tool",
            "tool_call_id": q.get("tool_call_id", ""),
            "content": f"[用户回答] {answer}",
        }
        # 同一 ask 调用只保留一个结果：回答替换已发送占位，不能追加重复 tool 回执。
        for index, message in enumerate(run.messages):
            if message.get("role") == "tool" and message.get("tool_call_id") == answer_message["tool_call_id"]:
                run.messages[index] = answer_message
                break
        else:
            run.messages.append(answer_message)
        run.pending_question = None
        run.deadline = 0.0
        run.status = _task_rt.RESUMING
        logger.info(f"[ask_user] 恢复任务 (run={run_id}, 回答={answer[:40]})")
        from kernel.task_store import save_run_snapshot
        await save_run_snapshot(store, run)
        await self._continue_run(run)
        if run.status == _task_rt.COMPLETED:
            store.delete(run_id)
        elif run.status == _task_rt.FAILED:
            await save_run_snapshot(store, run)

    # =================== [工具 handler 注册表] ===================
    # [0.3.18.1 Phase 2] 实现迁 kernel/tool_handlers.py, 薄委托保方法名不变
    async def _handle_connect_app(self, args: dict) -> str:
        return await _th_connect_app(self, args)

    async def _handle_disconnect_app(self, args: dict) -> str:
        return await _th_disconnect_app(self, args)

    async def _handle_instruct_app(self, args: dict) -> str | ToolResult:
        return await _th_instruct_app(self, args)

    # [0.3.18.2 Phase 3] 实现迁 kernel/brain_outbound.py, 薄委托保方法名不变
    async def _acquire_outbound_slot(
        self, app_name: str, command: str, kwargs: dict
    ) -> Optional[str]:
        return await _ob_acquire_outbound_slot(self, app_name, command, kwargs)

    async def _handle_submit_reply(self, args: dict):
        return await _ob_handle_submit_reply(self, args)

    def _resolve_reply_channel(self) -> str:
        return _ob_resolve_reply_channel(self)

    async def _claim_structured_outbound(self, target: str) -> Optional[str]:
        return await _ob_claim_structured_outbound(self, target)

    async def _auto_route_group_reply(self, src: str, chat: str, reply: str,
                                      is_dm_with_master: bool) -> bool:
        return await _ob_auto_route_group_reply(self, src, chat, reply, is_dm_with_master)

    def _record_outbound_text(self, text: str, app: str, target: str) -> None:
        return _ob_record_outbound_text(self, text, app, target)

    def _reply_source_is_gated(self) -> bool:
        return _ob_reply_source_is_gated(self)

    # [0.3.18.1 Phase 2] 实现迁 kernel/tool_handlers.py, 薄委托保方法名不变
    async def _handle_learn_knowledge(self, args: dict) -> str:
        return await _th_learn_knowledge(self, args)

    async def _handle_memory_recall(self, args: dict) -> str:
        return await _th_memory_recall(self, args)

    async def _handle_memory_remember(self, args: dict) -> str:
        return await _th_memory_remember(self, args)

    async def _handle_memory_forget(self, args: dict) -> str:
        return await _th_memory_forget(self, args)

    async def _handle_behavior_recall(self, args: dict) -> str:
        return await _th_behavior_recall(self, args)

    async def _handle_web_search(self, args: dict) -> ToolResult:
        return await _th_web_search(self, args)

    async def _handle_adjust_impression(self, args: dict) -> str:
        return await _th_adjust_impression(self, args)

    async def _handle_boost_interest(self, args: dict) -> str:
        return await _th_boost_interest(self, args)

    async def _handle_ask_user(self, args: dict) -> str:
        """[0.3.27.0 阶段4] 实现迁 kernel/tool_handlers.py, 薄委托保方法名。"""
        run = getattr(self, "_current_run", None)
        if run is not None:
            # 挂起时保存完整本轮额度/声明，恢复不能沿用中间会话的单例状态。
            run.resume_state_version = 1
            run.user_input = getattr(self, "_current_user_input", "")
            run.skill_save_requested = getattr(self, "_skill_save_requested", False)
            run.last_tool_called = getattr(self, "_last_tool_called", None)
            run.outbound_text_count = getattr(self, "_outbound_text_count", 0)
            run.outbound_media_count = getattr(self, "_outbound_media_count", 0)
            run.structured_targets = sorted(getattr(self, "_structured_targets_this_turn", set()))
        return await _th_ask_user(self, args)

    async def _refresh_interest_keywords(self) -> None:
        return await _th_refresh_interest_keywords(self)

    # [0.3.18.0 Phase 1] 实现迁 kernel/brain_messages.py, 薄委托保方法名不变
    @staticmethod
    def _parse_interest_list(text: str) -> list:
        return _msg_parse_interest_list(text)

    async def _maybe_send_batch_interim(self, tool_calls: list) -> None:
        """[0.3.21.0] 薄委托: 接单 ACK 逻辑迁 kernel/task_feedback.py。

        旧契约为"并发安全安心反馈 (仅 native_controller 白名单)", 升级为
        接单 ACK: 覆盖所有任务工具, 仅老板 1v1 (群聊不发心里话), 渠道走
        _current_group_src 修发错渠道病。方法名零变化, monkeypatch 点不漂移。
        旧 _send_interim_feedback/TIME_CONSUMING_TOOLS 保留待 0.3.22.0 P3
        进度反馈复用。
        """
        await _tool_loop.maybe_send_batch_interim(self, tool_calls)

    async def _execute_single_tool(self, tool) -> dict:
        """[Phase 4] 实现迁 kernel/tool_loop.py, 薄委托保方法名 (monkeypatch 点零漂移)。"""
        return await _tool_loop.execute_single_tool(self, tool)

    async def _run_tool_loop(self, messages: list) -> LoopOutcome:
        """[Phase 4] 实现迁 kernel/tool_loop.py, 薄委托保方法名 (调用序/旗标语义零变化)。"""
        return await _tool_loop.run_tool_loop(self, messages)

    async def _finalize_reply(
        self, final_reply: str, active_apps: list, is_boot_msg: bool, *, allow_actions: bool = True,
        empty_reply: Optional[str] = None,
    ) -> str:
        """[Phase 4] 实现迁 kernel/tool_loop.py, 薄委托保方法名。"""
        options = {} if allow_actions else {"allow_actions": False}
        if empty_reply is not None:
            options["empty_reply"] = empty_reply
        return await _tool_loop.finalize_reply(self, final_reply, active_apps, is_boot_msg, **options)

    # [0.3.18.0 Phase 1] 实现迁 kernel/brain_messages.py, 薄委托保方法名不变
    @staticmethod
    def _extract_data_url_from_image_data(data: dict):
        return _msg_extract_data_url(data)

    @staticmethod
    def _build_tool_message(
        tool_call_id: str, result_str: str, tool_result: Optional[ToolResult] = None
    ):
        return _msg_build_tool_message(tool_call_id, result_str, tool_result)

    @staticmethod
    def _build_user_message(user_input: str, media_payloads: list = None):
        return _msg_build_user_message(user_input, media_payloads)

    # [0.3.18.0 Phase 1] 实现迁 kernel/brain_messages.py, 薄委托保方法名不变
    @staticmethod
    def _is_outbound_denial(res_str: str) -> bool:
        return _msg_is_outbound_denial(res_str)

    def _is_tool_error(self, result_str: str) -> bool:
        return _msg_is_tool_error(result_str)

    def _build_error_digest_hint(self, func_name: str, args: dict, error_msg: str) -> str:
        return _msg_build_error_digest_hint(func_name, args, error_msg)

    def _sanitize_output(self, text: str) -> str:
        # [0.3.18.0 Phase 1] 实现迁 kernel/brain_sanitize.py, 薄委托保方法名不变
        return _sanitize_output_fn(text)
    async def think_batch(self, batch: list) -> str:
        """[0.3.31.0 Batch 0] 批阅整轮看门狗壳: 单轮批阅 (含 N 组 think +
        全部工具循环) 硬上限, 超限优雅收尾兜底回复, 绝不无限吊死
        (16:23 批阅永吊死事故根治)。turn_timeout<=0 回退旧行为。
        """
        if not batch:
            return ""
        # getattr 防御: __new__ 老夹具无此属性 = 看门狗关闭 = 旧行为零破坏
        _turn_timeout = float(getattr(self, "_TURN_TIMEOUT", 0) or 0)
        if _turn_timeout <= 0:
            return await self._think_batch_impl(batch)
        try:
            return await asyncio.wait_for(
                self._think_batch_impl(batch), timeout=_turn_timeout)
        except asyncio.TimeoutError:
            logger.error(
                f"[Brain][watchdog] 批阅整轮超时 ({_turn_timeout:.0f}s), "
                f"卡死点={getattr(self, '_turn_stage', '')!r}, 优雅收尾兜底"
            )
            return ("呜呜……这轮要处理的事情太多，阿露脑子卡住没能回完。"
                    "老板再戳我一下，我马上处理")

    async def _think_batch_impl(self, batch: list) -> str:
        async with self._execution_scope():
            try:
                self._turn_stage = "batch_start"
                return await self._think_batch_unlocked(batch)
            finally:
                self._current_run = None
                self._current_group_src = ""
                self._current_group_chat = ""
                self._current_group_is_dm = False

    async def _think_batch_unlocked(self, batch: list) -> str:
        """[内核2.0] 心跳批阅入口: 一次性消费信箱积累的多渠道消息。

        [P1 0.1.10.0] 按 chat_name 分组, 每组单独 think 传真实 chat_name ->
        私聊/群 history 独立分桶, 治"群消息打断私聊"。source 仍传 "multi" 保
        _think_impl 跳过 user_speak emit (本方法已逐条剥信封 emit 过)。
        单 chat 批次单次 think (不涨 LLM 成本); 多 chat 批次 N 次 think。
        返回老板组 reply + 恢复其 _outbound_sent_this_turn 供主循环兜底路由。
        """
        if not batch:
            return ""
        # [0.3.27.0 阶段4] 挂起任务超时清理 (安全默认恢复/高风险取消), 每批一次
        try:
            self._task_store().sweep_timeouts(self)
        except Exception as e:
            logger.warning(f"[ask_user] 超时清理异常(非致命): {e}")
        # [0.3.26.2 优化4] 每轮批阅重置目标限流计数 (max_replies_per_turn 语义
        # 为"每轮最多 N 条", 与 _hub_rate_limit_ok/_hub_note_reply 配合)。
        try:
            self._target_turn_replies = {}
        except Exception:
            pass
        # 逐条广播 user_speak 给记忆后端。注意: content 必须是【纯用户话语】,
        # 不能带 [外部感知:...] 信封头 (否则信封里的"对话:群名"会被实体提取
        # 污染成永久记忆)。剥信封后, source/chat_name 走 metadata。
        for msg in batch:
            try:
                # 透传 chat_name/sender/is_master 给记忆后端: 实体提取闸门据此判定
                # 发言者是否老板本人, 群聊第三人发言不得覆盖核心人物设定。
                md = dict(msg.metadata) if msg.metadata else {}
                speak_md = {
                    "chat_name": md.get("chat_name", ""),
                    "sender": md.get("sender", ""),
                    "is_master": md.get("is_master", False),
                    # [0.3.16.0 P4] 透传 1v1 标记: 记忆录制侧据此派生 visibility
                    # 隐私桶 (老板私聊 obs -> private, 群聊硬 ACL 不召回)。
                    "is_dm_with_master": md.get("is_dm_with_master", False),
                    "active_apps": list(self.bus.active_apps.keys()),
                }
                for key in ("origin_message_id", "conversation_id", "connector_id", "session_id", "_raw_agent_id"):
                    if key in md:
                        speak_md[key] = md[key]
                _memory_event = Message.user_speak(
                    content=_strip_envelope(msg.content),
                    source=msg.source,
                    **speak_md,
                )
                if hasattr(self.bus, "emit_user_memory"):
                    await self.bus.emit_user_memory(_memory_event, msg)
                else:
                    await self.bus.emit_msg(_memory_event)
            except Exception as e:
                logger.warning(f"[Brain] 批阅 emit user_speak 失败: {e}")

        # 体积闸门: 超过 MAX 条只留最新 (非丢弃式, 保语义)
        MAX = 20
        visible = [m for m in batch if m.content and m.content.strip()]
        if not visible:
            return ""
        if len(visible) > MAX:
            visible = visible[-MAX:]

        # [P1 0.1.10.0] 按 chat_name 分组保序, 每组单独 think 传真实 chat_name ->
        # 私聊/群 history 独立分桶 (_session_key 在 chat_name 非空时返回 chat_name 本身,
        # 与 source 无关), 治"群消息打断私聊"。source 仍传 "multi": 保 _think_impl 跳过
        # user_speak emit (本方法已逐条剥信封 emit 过, 防双 emit 污染记忆)。
        # 单 chat 批次单次 think (最常见路径不涨 LLM 成本); 多 chat 批次 N 次 think。
        from collections import OrderedDict
        groups: "OrderedDict[str, list]" = OrderedDict()
        for m in visible:
            chat = m.get("chat_name", "") if hasattr(m, "get") else ""
            key = m.get("conversation_id", chat) if getattr(self.bus, "raw_messages", None) is not None else chat
            groups.setdefault(key, []).append(m)

        group_results: list = []  # [(chat, src, has_master, reply, outbound_sent)]
        # [0.3.14.0 P1 权限协议] 批次权限策略: strict 降档 (混合身份组 -> guest)
        # 或 legacy any() (config/brain.yaml permission.strict_batch_permission=false
        # 一键回滚 0.3.13.x 行为)。
        _perm_cfg = (getattr(self, "brain_config", None) or {}).get("permission", {}) or {}
        _strict_perm = bool(_perm_cfg.get("strict_batch_permission", True))
        raw_enabled = getattr(self.bus, "raw_messages", None) is not None
        grouped_messages = (
            mode_groups(visible, stable=raw_enabled)
            if is_deepseek(getattr(self, "model_name", ""))
            else list(groups.items())
        )
        raw_contexts = {}
        for chat, msgs in grouped_messages:
            src = msgs[0].source if msgs else "multi"
            conversation_id = msgs[0].get("conversation_id", "") if raw_enabled else ""
            if raw_enabled:
                chat = msgs[0].get("chat_name", "")
                saved_agent = msgs[0].get("_raw_agent_id")
                if saved_agent and saved_agent != getattr(self, "_active_agent_id", ""):
                    logger.error("已保存消息的 Agent 与当前 Agent 不同；保留原文，未执行该组。")
                    continue
                self._current_conversation_id = conversation_id
                if saved_agent:
                    self.bus.bind_raw_turn(msgs[0])
                    raw_contexts[(chat, src, conversation_id)] = msgs[0]
                else:
                    self.bus.clear_raw_turn()
            # has_master: 本组含老板消息 (旧 any 语义), 供发言闸门与回复路由选组,
            # 语义零变化。
            has_master = any(
                bool(m.metadata.get("is_master")) if m.metadata else False
                for m in msgs
            )
            # [0.3.14.0 P1] 组权限主体: strict 模式混合身份整组降档 guest, 治架构
            # 审查严重项 -- 老板与第三人同群同批时第三人内容不得继承主人高危能力。
            # 降档只收 think 权限环境 (高危闸门), 发言/召回/路由不受影响。
            _group_principal = Principal.from_batch(msgs, strict=_strict_perm)
            is_master_group = _group_principal.is_owner
            if _strict_perm and has_master and not is_master_group:
                logger.info(
                    f"[权限] 混合身份组降档 guest: {chat or '默认'}({src}), "
                    f"本批高危操作将拒 (actors={_group_principal.actor_id})"
                )
            # [0.3.27.0 阶段4] 挂起任务回答关联: 该 chat 有等待回答的任务时,
            # 本组老板消息作为回答消费 (其他会话消息绝不被当成答案), 不 think。
            # 同批夹有第三人或其他主体时，不把整批文字用作旧任务的老板回答。
            _answer_principal = _group_principal if all(
                (p := Principal.from_message(m)).is_owner
                and p.actor_id == _group_principal.actor_id and p.channel == src
                for m in msgs
            ) else None
            _pending_rid = self._find_pending_run(src, chat, _answer_principal)
            if _pending_rid:
                _answer = " ".join(_strip_envelope(m.content) for m in msgs)
                try:
                    await self.resume_ask(_pending_rid, _answer)
                except Exception as e:
                    logger.warning(f"[ask_user] 回答关联失败: {e}")
                group_results.append((chat, src, has_master, "",
                                      False, [], False, "ok", conversation_id))
                continue
            # [P1 0.2.7.0 阶段1 发言闸门] 渠道无关规则闸门: 群聊默认潜水, 老板1v1/@阿露/
            # 强触发才过闸。dive 组跳过 warm_recall/backfill/reviewer/think 全部, 零发送省
            # LLM 成本。P0 出站硬约束 (_acquire_outbound_slot + 工具循环硬终止) 全保留做双
            # 保险 -- 闸门是新增前置, 不动发送层。lazy-init 兜底测试夹具 (跳 __init__)。
            # [0.2.9.0] 兴趣动态门: TTL 缓存刷新 (失败留旧, 不阻塞); 每组调一次成本低
            await self._refresh_interest_keywords()
            _gate = getattr(self, "_speak_gate", None)
            if _gate is None:
                _gate = self._speak_gate = SpeakGate.load()
            _gate_char = (getattr(self, "character_config", {}) or {}).get("name", "阿露")
            # [0.3.25.1] 目标级回复策略优先 (platform_hub ListeningTarget.reply_policy):
            #   listen_only -> 恒潜水; always -> 恒过闸; mention_only -> 仅提及过闸;
            #   未配置/unknown -> 回退通用闸门 (smart 语义)。
            _target_policy = self._hub_reply_policy(src, chat)
            if _target_policy == "listen_only":
                _gate_decision = GateDecision(False, "目标策略: 只监听不回复")
            elif _target_policy == "always":
                _gate_decision = GateDecision(True, "目标策略: 始终回复")
            elif _target_policy == "mention_only":
                _mentioned_only = _gate._mentioned(msgs, _gate_char)
                _gate_decision = GateDecision(
                    _mentioned_only,
                    "目标策略: 提及过闸" if _mentioned_only else "目标策略: 仅提及, 无提及潜水",
                )
            else:
                _gate_decision = _gate.evaluate(msgs, chat, src, has_master, _gate_char)
            # [0.3.26.2 优化2/4] 免打扰 + 限流: 即使回复策略放行, 也在闸门层
            # 再否决 (最高优先级 -- 老板设了免打扰就是不想被打扰)。
            if _gate_decision.passed and self._hub_quiet_hours_active(src, chat):
                _gate_decision = GateDecision(False, "目标策略: 免打扰期间不回复")
            elif _gate_decision.passed and not self._hub_rate_limit_ok(src, chat):
                _gate_decision = GateDecision(False, "目标策略: 限流拦截 (本轮/间隔超限)")
            if not _gate_decision.passed:
                logger.info(
                    f"[闸门] 潜水跳过 {chat or '默认'}({src}): {_gate_decision.reason}"
                )
                # [0.3.20.0] 潜水路径未跑 think, 三态补 "ok" (末位与出站元组对齐)。
                group_results.append((chat, src, has_master, "", False, [], False, "ok", conversation_id))
                continue
            # [0.2.10.1] 只读批: 闸门以"图片消息过闸"放行 => 批内无提及 (提及判定排在
            # 图片之前)。图片文字行为拉齐: 照看, 但随口纯文本不代发, 想回必须蓄意
            # 调 submit_reply -- 治 0.2.9.3 后"她对每个表情包都嘴碎"的偏斜。
            _view_only = _gate_decision.reason == "图片消息过闸"
            contents = [_strip_envelope(m.content) for m in msgs]

            # [短期记忆 v0.4.1+ #2跨群分离 v0.5.4] 每组独立 smart_recall, 防跨群消息
            # 合并成一个查询致召回上下文混淆。只取近期 obs 流水账, 不碰核心事实/规矩。
            # [0.2.10.4 P1] 传 chat_name: 同群 obs 提权 + 跨群标注, 治串台。
            # [0.3.16.0 P4] 传 asker 上下文: 组含老板 -> owner 召回 (private 可见),
            # 纯第三人群 -> guest 召回 (private obs 硬过滤), 治私聊记忆泄露群聊。
            # [0.3.26.0 Phase 4] 本轮记忆范围 (规格 6.9): web chat/send 可传
            # memory_scope, 批次首条消息 metadata 透传 -> warm recall 按 scope
            # 收敛注入 (disabled 零注入), 选择结果真实传入 Recall Plan。
            _scope = ""
            for _m in msgs:
                if _m.metadata and _m.metadata.get("memory_scope"):
                    _scope = str(_m.metadata["memory_scope"]).strip().lower()
                    break
            rec = await self._warm_recall(
                " ".join(contents), chat_name=chat or "", asker_is_master=has_master,
                memory_scope=_scope,
            )
            if rec and rec.strip():
                if getattr(self, "_WARM_RECALL_TOKEN_BUDGET", 0) > 0:
                    # [0.3.36.0 B2c] 预算截断为主, 字符上限双保险
                    rec = _truncate_to_token_budget(
                        rec, self._WARM_RECALL_TOKEN_BUDGET)
                    rec = rec[:self._WARM_RECALL_CHARS * 2]
                else:
                    rec = rec[:self._WARM_RECALL_CHARS]  # 旧行为回退
                # 仅登记预算截断后真正进入 prompt 的行，避免未展示尾部被永久压制。
                rec = self._dedup_warm_lines(
                    rec, chat_name=chat or "", memory_scope=_scope, commit=True,
                )
            self._warm_recall_text = (
                f"[{chat or '默认'}近期] {rec}"
                if (rec and rec.strip()) else ""
            )
            # [v0.2.6.0] 短期记忆不足主动补历史: 桶薄(冷启动/该会话首条)才拉, 瞬态上下文,
            # 只注入本轮 think 不写 chat_history 桶。bucket 按 chat 取 (multi 源 skey=chat)。
            # getattr 兜底: 测试夹具 Brain.__new__ 跳过 __init__ 无 chat_history。
            _hist_buckets = getattr(self, "chat_history", None) or {}
            self._history_backfill_text = await self._backfill_history(
                chat, src, len(_hist_buckets.get(_session_key(chat, src, conversation_id), []))
            )

            # [双轴 prompt 上线 0.1.7.0] 每组 unique senders 查双轴, 供 _think_impl 注入。
            self._social_context_text = await self._resolve_social_context(
                self._unique_senders(msgs)
            )

            # [阶段2 0.1.12.4 方案A] 分身审核只打 Tag 不拦截: 完整 msgs 全送 think,
            # reviewer 输出每条 Tag+权重+建议, snapshot 注入 review_hint, 阿露按 Tag
            # 自己决定回不回 (4点决策3 最终决定权在阿露)。治 0.1.12.2/3 延迟/脱节/循环。
            _char_cfg = getattr(self, "character_config", {}) or {}
            review_ctx = {
                "char_name": _char_cfg.get('name', '阿露'),
                "identity": _char_cfg.get('identity', '数字生命'),
                "soul_memory": "",
                "social_context": self._social_context_text,
                "warm_recall": self._warm_recall_text,
                "known_members": [],
            }
            _reviewer = getattr(self, "reviewer", None)
            if _reviewer is not None:
                # [0.2.8.2 可靠性体检] 审核是富化不是关键路径: 异常必须降级不阻塞
                # (旧病: review() 抛异常穿透致整组 think_batch 崩溃丢批)。
                try:
                    review = await _reviewer.review(msgs, review_ctx)
                except Exception as e:
                    logger.warning(
                        f"[Reviewer] {chat or '默认'}({src}) review 异常, 降级放行: {e}"
                    )
                    review = {"fallback": True, "reason": f"异常:{type(e).__name__}"}
                # [0.2.8.2] 降级可见: 旧病 fallback 在 brain 层静默丢弃, 生产查不到
                # reviewer 命中率; 现每组降级打一行 (含原因)。
                if review.get("fallback"):
                    logger.warning(
                        f"[Reviewer] {chat or '默认'}({src}) 降级: "
                        f"{review.get('reason', '未知')}"
                    )
                review_hint = _reviewer.build_snapshot_hint(review, msgs)
                # [老板识别 v0.2.6.0] 规则化潜水 hint, 独立于 LLM 审核(老板批免审也照给):
                # 老板在群回别人没喊阿露 -> 提示默认潜水但保留接话自由。不走
                # build_snapshot_hint 的 fallback 闸门, 单独拼入 snapshot。
                master_hint = _reviewer.build_master_addressing_hint(msgs, review_ctx)
            else:
                review = {"fallback": True}
                review_hint = ""
                master_hint = ""
            # [方案A] 完整批次全送 think, 不 filter 不 requeue (不拦截消息)
            self._current_group_senders = self._unique_senders(msgs)
            self._current_group_chat = chat
            # [0.2.8.0] 组真实渠道源 (think_batch 传 source=multi, 契约代发/降级需真 src)
            self._current_group_src = src
            # [0.3.21.0] 本组是否老板 1v1: 接单 ACK 闸门用 (群聊不发心里话)
            self._current_group_is_dm = any(
                bool(m.metadata.get("is_dm_with_master")) if m.metadata else False
                for m in msgs
            )

            # 拼本组快照(完整 msgs): 每条带发送者 + "回复此消息"的精确 target 提示。
            # [P0 0.2.4.6 修复] 加 sender 显示, 治阿露分不清谁是谁(把胖脸鱼叫成球球):
            # 旧快照只列内容不列发送者, 主脑看不出哪条是谁说的 -> 叫错名/重复答/该回不回。
            # [0.2.8.0] 契约态: 回复走 submit_reply; 回退态 (structured_reply=false): 旧 send_text。
            local_reply = src in ("web", "console")
            if local_reply:
                tgt_hint = "直接输出回复正文，由当前本地对话显示，无需发送工具"
            elif self._structured_reply_enabled():
                tgt_hint = (
                    f"回复用 submit_reply(text, target={chat!r})"
                    if chat else "回复用 submit_reply(text)"
                )
            else:
                tgt_hint = (
                    f"回复用 use_{src}(target={chat!r}, command='send_text')"
                    if chat else f"回复用 use_{src}(command='send_text')"
                )
            # [0.3.22.0 P2] 已轻答标注: 忙时轻秘书轻答过的闲聊消息, 快照行加
            # 标注防阿露双答同一条。消息照常进批 (零丢失), 只认 TaskQueue 真账本
            # (MagicMock 等假账本一律忽略, 防存量夹具误标)。
            _tq_ledger = getattr(self.bus, "task_ledger", None)
            _has_replied_mark = isinstance(_tq_ledger, _TqTaskQueue)
            lines = []
            for i, m in enumerate(msgs):
                sender = (m.metadata.get("sender") if m.metadata else "") or ""
                prefix = f"{sender}说: " if sender else ""
                mark = ""
                if _has_replied_mark:
                    try:
                        if _tq_ledger.secretary_replied(_tq_msg_ack_key(m)):
                            mark = "  (忙时已轻答过, 除非老板重提, 不必再回这条)"
                    except Exception:
                        mark = ""  # 标注是增益不是前提, 异常不穿透快照拼装
                lines.append(f"{i+1}. {prefix}{m.content}{mark}  [{tgt_hint}]")
            numbered = "\n".join(lines)
            # [P1 0.2.4.7] 注入活对话列表, 治阿露主动回复拼错 target (同 src 缓存, 每批只取一次)。
            if not hasattr(self, "_live_chats_cache"):
                self._live_chats_cache = {}
            live_chats = self._live_chats_cache.get(src, "")
            if not live_chats:
                live_chats = await self._resolve_live_chats(src)
                self._live_chats_cache[src] = live_chats
            live_hint = (
                f"\n[当前可用对话列表 (target 必须从这取精确名, 禁止凭记忆拼): {live_chats}]\n"
                if live_chats else ""
            )
            if local_reply:
                dispatch_note = (
                    "[本地对话回复契约: 直接输出回复正文，系统会在当前对话界面显示。"
                    "不需要发送工具，也不要为了回复此消息切换到外接平台。"
                    "需要处理任务时仍可调用本轮已提供的任务工具。]"
                )
            elif self._structured_reply_enabled():
                # [0.2.8.0 出站收口] 单条代发契约: 发不发 = 调不调 submit_reply, 系统机械保一次。
                dispatch_note = (
                    "[回复契约: 全局审视上述消息, 想回就调 submit_reply(text, target) 一次, "
                    "系统替你代发 (一回合至多一次, 重复会被拦截), target 必填, 从消息末尾的 "
                    "[回复用...] 提示取精确名。不想回就不调 = 合法已读不回, 不用输出任何解释。"
                    "你直接输出的纯文本也会由系统代发本组, 但描述自己动作的元话语"
                    "(如'已回复/继续潜水')会被拦截不发。"
                    "潜意识分身审核参考你可无视, 最终决定权在你。]"
                )
            else:
                dispatch_note = (
                    "[你的自主处置权: 全局审视上述消息, 依据人设与心情决定回复其中 "
                    "0~N 个渠道。回复某渠道【必须】调 use_<app>(target=精确名, command='send_text', "
                    "params={'text':...}) 发送; 你直接输出的纯文本 reply 只会进控制台日志, "
                    "用户收不到。未显式调用工具的渠道视为已读不回(合法)。target 必填, 从消息 "
                    "末尾的 [回复用...] 提示里取精确名。潜意识分身审核参考你可无视, 最终决定权在你。]"
                )
            # [0.2.10.1] 只读批提示: 明示"没喊你, 默认只看不回", 克制嘴碎。
            view_only_hint = (
                "[本批因含图片过闸, 没人喊你: 默认只看不回, 随口输出不会被发送; "
                "特别想评论就调 submit_reply(text, target) 主动回复。]"
                if _view_only else ""
            )
            snapshot = (
                "[全渠道广播快照 — 过去心跳周期内收到的消息]\n"
                f"{numbered}\n"
                f"{review_hint}"
                f"{master_hint}"
                f"{live_hint}"
                f"{view_only_hint}"
                f"{dispatch_note}"
            )

            # [0.3.31.1 Batch 1] 空闲任务结论移交: 空闲轮被抑制外发的分析结论
            # 不蒸发, 注入老板组批阅供直接引用 (修"答案做出来却被掐死")。
            if is_master_group and getattr(self, "_idle_task_handoff", ""):
                snapshot += (
                    "\n[参考: 你空闲时已完成的相关分析结论, 可据此直接回复]\n"
                    + str(self._idle_task_handoff)[:600]
                )
                self._idle_task_handoff = ""
            self._current_activity = f"批阅 {chat or '默认'}"
            # [权限闸门] 本组权限主体 (0.3.14.0 P1): strict 下全 owner 才授权
            # add_listen_chat 等敏感操作; 阿露 idle/群第三人组/混合身份组 -> False -> 拦。
            # 比旧"组内 any"更紧: 老板与第三人同群同批不再给整组主人权限 (架构审查严重项)。
            reply = await self.think(
                snapshot,
                source_channel="multi",
                chat_name=chat,
                is_master=is_master_group,
                principal=_group_principal,
                **({"thinking_mode": msgs[0].metadata.get("thinking_mode", "enabled")}
                   if is_deepseek(getattr(self, "model_name", "")) else {}),
            )
            # [0.3.20.0 止血] 本组三态随组存档: 必须在 auto_route 的网络 await 之前
            # 捕获 (review H1: await 窗口内并发 think 可能改写 _last_think_status)。
            if raw_enabled and msgs[0].get("_raw_agent_id") and isinstance(reply, str) and reply:
                reply = self.bus.reply_text(reply, self._current_turn_id, original=msgs[0])
            status_after = getattr(self, "_last_think_status", "ok")
            # [0.2.9.1] 按组代发: 模型没调 submit_reply 只吐纯文本时系统代发本组
            # (补非老板群纯文本死日志漏洞; narration/沉默元话语由代发侧过滤)。
            # [0.2.10.1] 只读批不代发: 图片文字拉齐, 随口不发送 (蓄意调工具才发)。
            if reply and reply.strip() and src not in ("console", "multi", "boot") and not _view_only:
                _is_dm = any(
                    bool(m.metadata.get("is_dm_with_master")) if m.metadata else False
                    for m in msgs
                )
                await self._auto_route_group_reply(src, chat, reply, _is_dm)
            outbound_after = getattr(self, "_outbound_sent_this_turn", False)
            outbound_targets_after = list(getattr(self, "_outbound_targets_this_turn", []))
            structured_after = getattr(self, "_structured_outbound_this_turn", False)
            group_results.append((chat, src, has_master, reply, outbound_after,
                                  outbound_targets_after, structured_after, status_after,
                                  conversation_id))
            # [0.3.21.0 review H3] 组旗标用毕即清: 防残值泄漏到后续 idle/boot
            # 直连 think 路径, 误判老板 1v1 往微信发 ACK (旗标仅组循环内置)。
            self._current_group_src = ""
            self._current_group_is_dm = False
            self._current_group_chat = ""
            if reply and reply.strip():
                logger.info(f"[批阅] {chat or '默认'}({src}) 回复: {reply.strip()[:80]}")

        # 选老板组回复作主循环兜底路由对象 (与 _pick_primary 对齐: 首个 is_master 组)。
        # 无老板组取首组 (主循环 _primary_is_master=False 不外发, 仅日志)。
        boss = next((g for g in group_results if g[2]), group_results[0] if group_results else None)
        if boss is None:
            return ""
        # 恢复老板组出站状态供主循环 should_route 判定 (最后一次 think 未必是老板组)。
        self._outbound_sent_this_turn = boss[4]
        self._outbound_targets_this_turn = boss[5]
        self._structured_outbound_this_turn = boss[6]  # [0.2.8.0] 契约代发标记同恢复
        self._last_think_status = boss[7]  # [0.3.20.0] 老板组三态同恢复 (兜底闸门用)
        from kernel.raw_message_service import TurnReply
        if isinstance(boss[3], TurnReply):
            self._current_turn_id = boss[3].identity.turn_id
        if raw_enabled and (original := raw_contexts.get((boss[0], boss[1], boss[8]))) is not None:
            self.bus.bind_raw_turn(original)
            self._current_conversation_id = original.get("conversation_id", "")
        # [审查整改 M2] 批阅收尾即复位: think 均已收尾 (_thinking=False),
        # 留陈旧文案会让 REST 快照给出 idle+"批阅" 矛盾态。
        self._current_activity = "待机"
        return boss[3]

    def _unique_senders(self, msgs: list) -> list:
        """[双轴 0.1.7.0] 从 msgs 去重提取真实 sender (排除 未知/self)。"""
        senders: list = []
        seen: set = set()
        for m in msgs:
            s = (m.metadata.get("sender") if m.metadata else "") or ""
            if s and s not in ("未知", "self") and s not in seen:
                seen.add(s)
                senders.append(s)
        return senders

    async def _resolve_social_context(self, senders: list) -> str:
        """[双轴 0.1.7.0] 查 senders 亲疏文本, 供 _think_impl 注入 system prompt。
        0.3s 超时 + try 兜底, 失败返空串不阻塞 think (同 _warm_recall 模式)。"""
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if daily_retrieval_enabled(self.bus) or not senders:
            return ""
        try:
            mem_provider = self._resolve_memory_provider()
            if not mem_provider or mem_provider not in self.bus.active_apps:
                return ""
            result = await asyncio.wait_for(
                self.bus.send_instruction(mem_provider, "get_socials", persons=senders),
                timeout=0.3,
            )
            if isinstance(result, ToolResult):
                return result.text if result.ok and result.text else ""
            return str(result) if result else ""
        except asyncio.TimeoutError:
            logger.debug("[Brain] 双轴查询超时 (0.3s), 跳过亲疏注入")
            return ""
        except Exception as e:
            logger.warning(f"[Brain] 双轴上下文构建失败 (非致命): {e}")
            return ""

    async def _resolve_live_chats(self, src: str) -> str:
        """[P1 0.2.4.7] 取当前活对话列表供 snapshot 注入, 治阿露主动回复时拼错 target。

        实证 21:47:13/21:47:43 阿露把"拿豆橛子抽烂恁的腚"拼成"拿豆橛子抽烂的腚"、
        "张雪峰老师赛高"混拼, 连环 target 未解析 -> 工具循环空转一分钟。根因: 主动回复
        时消息尾部无 target 提示, 阿露靠记忆拼名易错。注入活列表让她照抄不拼。
        走统一接口 send_instruction(wechat, get_sessions), 不戳 wx_bridge 内部。
        0.8s 超时 + try 兜底, 失败返空串不阻塞 think。
        """
        if not src or src not in self.bus.active_apps:
            return ""
        try:
            result = await asyncio.wait_for(
                self.bus.send_instruction(src, "get_sessions"),
                timeout=0.8,
            )
            text = result.text if isinstance(result, ToolResult) and result.ok else (str(result) if result else "")
            return text.strip() if text else ""
        except asyncio.TimeoutError:
            logger.debug(f"[Brain] get_sessions 超时 (0.8s), 跳过活列表注入 (src={src})")
            return ""
        except Exception as e:
            logger.warning(f"[Brain] 活对话列表获取失败 (非致命): {e}")
            return ""

    async def _backfill_history(self, chat_name: str, src: str, bucket_len: int) -> str:
        """[v0.2.6.0] 短期记忆不足时主动补当前会话历史 (走 AppBus 统一接口 get_history)。

        治"阿露只能靠自己存过的记忆说话": 冷启动/该会话首条消息时 RAM 桶很薄, 主动拉
        最近 N 条历史补上下文。约束:
          - 走统一 send_instruction(src, get_history), 能力探测 manifest 含 get_history 才调,
            不焊死 wechat (capability-generic-not-coupled): 任何 app 暴露 get_history 都生效。
          - 瞬态上下文: 返回值只注入本轮 think, 不写 chat_history 桶 (防污染/重复计数)。
          - 隐私按构造安全: 按 chat_name 拉、各组各自 think 内注入, 老板私聊历史绝不进群。
          - 不碰老板抢占软熔断: think 组装层调用, 独立超时 + 失败优雅跳过, 不进工具循环。
        镜像 _resolve_live_chats(get_sessions) 的 guard->wait_for->ToolResult归一->优雅跳过。

        Returns:
            拼好的近期聊天记录文本; 不满足触发条件/不支持/超时/失败 -> 返空串。
        """
        # getattr 兜底: 测试夹具 Brain.__new__ 跳过 __init__ 时无 brain_config, 优雅返空。
        cfg = (getattr(self, "brain_config", None) or {}).get("history_backfill", {}) or {}
        if not cfg.get("enabled", True):
            return ""
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if daily_retrieval_enabled(self.bus):
            min_turns = int(cfg.get("min_bucket_turns", 4))
            if bucket_len >= min_turns:
                return ""
            rows = await self.bus.recent_daily(limit=max(1, min(int(cfg.get("fetch_n", 12)), 50)))
            return "\n".join(
                f"[{row.created_at} {row.role} {row.message_id}] {row.content[:500]}"
                for row in rows
            )[:3000]
        if not chat_name or not chat_name.strip():
            return ""
        if not src or src not in self.bus.active_apps:
            return ""
        min_turns = int(cfg.get("min_bucket_turns", 4))
        if bucket_len >= min_turns:
            return ""  # 桶不薄, 短期记忆够, 不补 (省 IO, 避免引入噪声)
        # 能力探测: 该 source app 的 manifest 声明 get_history 才调 (不焊特定 app)。
        # hasattr 兜底: 测试夹具 active_apps 可能是 set(无 .get), 此时 app=None 优雅返空。
        active = self.bus.active_apps
        app = active.get(src) if hasattr(active, "get") else None
        if app is None:
            return ""
        try:
            commands = (getattr(app, "manifest", {}) or {}).get("commands", []) or []
        except Exception:
            commands = []
        if not any(isinstance(c, dict) and c.get("name") == "get_history" for c in commands):
            return ""
        fetch_n = int(cfg.get("fetch_n", 12))
        timeout = float(cfg.get("timeout", 1.2))
        try:
            result = await asyncio.wait_for(
                self.bus.send_instruction(
                    src, "get_history", chat_name=chat_name, n=fetch_n,
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.debug(f"[Brain] 补历史超时 ({timeout}s), 跳过 (src={src}, chat={chat_name})")
            return ""
        except Exception as e:
            logger.warning(f"[Brain] 补历史异常 (非致命): {e}")
            return ""
        # get_history 返回 {"ok":True,"data":[{sender,content}]} -> text类 ToolResult
        # (.data=None, .text=整dict JSON)。解析 .text 取 data; 任何失败返空不污染 prompt。
        if not (isinstance(result, ToolResult) and result.ok):
            return ""
        try:
            payload = json.loads(result.text) if result.text else {}
            items = payload.get("data", []) if isinstance(payload, dict) else []
        except (json.JSONDecodeError, TypeError):
            return ""
        lines = []
        for it in items:
            if not isinstance(it, dict):
                continue
            sender = str(it.get("sender", "") or "")
            content = str(it.get("content", "") or "").strip()
            if content:
                lines.append(f"{sender}: {content}" if sender else content)
        if not lines:
            return ""
        logger.info(f"[Brain] 补历史成功 (src={src}, chat={chat_name}, {len(lines)}条)")
        return "\n".join(lines[-fetch_n:])

    def _dedup_warm_lines(self, text: str, chat_name: str = "",
                          memory_scope: str = "", commit: bool = True) -> str:
        """[0.3.36.0 B2b] 跨轮去重: 近期已注入过的召回行本轮跳过。

        agent/chat/scope 三元组独立；缓冲按轮次而不是按行计数。
        commit=False 只过滤不登记，供预算截断前使用；只有模型实际收到的
        最终文本才能 commit。_DEDUP_TURNS=0 原样返回；空行不保留。
        """
        if not text or not text.strip():
            return ""
        turns = getattr(self, "_DEDUP_TURNS", 5)
        if not turns or turns <= 0:
            return text
        buffers = getattr(self, "_warm_injected_bufs", None)
        if buffers is None:
            buffers = {}
            self._warm_injected_bufs = buffers
        key = (
            str(getattr(self, "_active_agent_id", "default") or "default"),
            str(chat_name or ""),
            str(memory_scope or "").strip().lower(),
        )
        buf = buffers.get(key)
        if buf is None or buf.maxlen != max(1, turns):
            if key not in buffers and len(buffers) >= 128:
                buffers.pop(next(iter(buffers)))
            buf = deque(maxlen=max(1, turns))
            buffers[key] = buf
        seen = set().union(*buf) if buf else set()
        kept: list = []
        current: set[str] = set()
        for line in str(text).splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            digest = hashlib.md5(stripped.encode("utf-8")).hexdigest()[:16]
            if digest in seen or digest in current:
                continue
            kept.append(line)
            current.add(digest)
        if commit and current:
            buf.append(current)
        return "\n".join(kept)

    async def _warm_recall(self, query: str, chat_name: str = "",
                           asker_is_master: bool = True,
                           memory_scope: str = "") -> str:
        """[短期记忆 v0.4.1] 异步预召回近期 obs 流水账 (smart_recall, 非 recall)。

        smart_recall 只查向量库 obs 前缀 (近期流水账), 不触发 spotlight 实体/规矩
        注入, 故只补短期上下文、不碰长期设定, 守护阿露学习进化本体。
        带 0.3s 超时: 未就绪则返回空串, 不阻塞 think 主路径。
        [0.2.10.4 P1] chat_name 透传 smart_recall: 同群 obs 提权, 跨群标注, 治串台。
        [0.3.16.0 P4] asker_is_master 透传: 记忆硬 ACL 按召回上下文过滤 (主人私聊
        obs 不在群聊召回)。默认 True 保直连老路径行为。
        [0.3.26.0 Phase 4] memory_scope 透传: disabled 时零注入 (provider 侧
        短路, 不查询不提交 hit), 其余 scope 由 provider 语义映射。
        """
        if not query or not query.strip():
            return ""
        scope = str(memory_scope or "").strip().lower()
        if scope == "disabled":
            return ""  # 规格验收: 禁用长期记忆时不注入且不提交 hit
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if daily_retrieval_enabled(self.bus):
            from kernel.raw_message_retrieval import render_daily_evidence
            try:
                deadline = time.monotonic() + 0.3
                items = await asyncio.wait_for(
                    self.bus.recall_daily(query[:200], top_k=5, neighbors=1,
                                          memory_scope=scope, deadline=deadline),
                    timeout=max(0.001, deadline - time.monotonic()),
                )
                return render_daily_evidence(items or [], max_chars=1200)
            except Exception as exc:
                logger.warning("daily warm recall unavailable: %s", type(exc).__name__)
                return ""
        mem_provider = self._resolve_memory_provider()
        if not mem_provider or mem_provider not in self.bus.active_apps:
            return ""
        try:
            result = await asyncio.wait_for(
                self.bus.send_instruction(
                    mem_provider, "smart_recall", text=query[:200],
                    chat_name=chat_name, asker_is_master=asker_is_master,
                    memory_scope=scope,
                ),
                timeout=0.3,
            )
        except asyncio.TimeoutError:
            logger.debug("[Brain] 预召回超时 (0.3s), 跳过短期上下文预热")
            return ""
        except Exception as e:
            logger.warning(f"[Brain] 预召回异常 (非致命): {e}")
            return ""
        # [v0.4.6 修复] send_instruction 返回 ToolResult (非 str)。
        # 此前成功路径直接 return 了 ToolResult 对象, 导致 _think_impl 注入
        # system_prompt 时 warm[:600] 下标访问炸 'ToolResult' object is not
        # subscriptable, 整条 think_batch 崩溃且回复发不出去。这里归一为纯文本:
        # 成功取 .text, 失败/异常返回空串, 不污染 prompt。
        if isinstance(result, ToolResult):
            _warm_text = result.text if result.ok and result.text else ""
        else:
            _warm_text = str(result) if result is not None else ""
        # [0.3.36.0 B2b] 跨轮去重: 近期已注入行不重复占 warm 块。
        return self._dedup_warm_lines(
            _warm_text, chat_name=chat_name, memory_scope=scope, commit=False,
        )

    async def think_structured(self, user_input: str, associative_memory: str = "",
                                source_channel: str = "console", chat_name: str = "",
                                media_payloads: list = None, is_master: bool = False,
                                principal: "Principal" = None, *, thinking_mode: str = "enabled") -> _task_rt.RunResult:
        """[0.3.27.0 阶段6 规格八] 结构化主路径: 返回 RunResult。

        think() 保留字符串兼容包装 (内部委托本方法); 主运行时 (agent_runtime)
        可切换到本方法消费结构化结果 (completed/suspended/failed/cancelled),
        不再用空字符串/中文文案/_thinking 猜状态。
        """
        run_before = getattr(self, "_current_run", None)
        self._suspended_this_turn = False
        reply = await self.think(
            user_input, associative_memory, source_channel, chat_name,
            media_payloads, is_master, principal,
            **({"thinking_mode": thinking_mode} if thinking_mode != "enabled" else {}),
        )
        rid = run_before.run_id if run_before is not None else ""
        if self._suspended_this_turn:
            return _task_rt.RunResult.suspended(run_id=rid)
        status = getattr(self, "_last_think_status", "ok")
        if status == "error":
            return _task_rt.RunResult.failed(reason="think_error", run_id=rid)
        if status == "timeout":
            return _task_rt.RunResult.failed(reason="llm_timeout", run_id=rid)
        if status == "max_steps":
            return _task_rt.RunResult.failed(reason="max_steps", run_id=rid)
        if status == "tool_errors":
            return _task_rt.RunResult.failed(reason="tool_errors", run_id=rid)
        return _task_rt.RunResult.completed(reply, run_id=rid)

    async def think(self, user_input: str, associative_memory: str = "", source_channel: str = "console", chat_name: str = "", media_payloads: list = None, is_master: bool = False, principal: "Principal" = None, *, thinking_mode: str = "enabled"):
        async with self._execution_scope():
            return await self._think_unlocked(
                user_input, associative_memory, source_channel, chat_name,
                media_payloads, is_master, principal, thinking_mode=thinking_mode,
            )

    async def _think_unlocked(self, user_input: str, associative_memory: str = "", source_channel: str = "console", chat_name: str = "", media_payloads: list = None, is_master: bool = False, principal: "Principal" = None, *, thinking_mode: str = "enabled"):
        if thinking_mode not in ("enabled", "disabled"):
            raise ValueError("Invalid thinking mode")
        # [内核2.0 重入锁] 心跳 think_batch 与 web_ui think 互斥。
        # 一方在跑, 另一方直接返回, 防并发污染 chat_history / 工具循环。
        if self._thinking:
            logger.warning(
                f"[Brain] think 重入拒绝 (正在处理上一轮): "
                f"src={source_channel}, input={user_input[:40]}..."
            )
            return BUSY_REPLY
        # [抢占软熔断 v0.4.1] 抢占只该由 _run_tool_loop 轮间 (本文件 619 行) 发现
        # "进行中 think 又来老板新消息" 触发。think 入口若旗子还在, 说明它是本次消息
        # 自身 submit 时置的悬空旗 (submit 时无进行中 think 可抢), 属自抢 -> 清掉不触发,
        # 否则会给处理自身消息的 think 注入"别调工具"的莫须有喊停, 把任务搅乱
        # (v0.4.x 实测: 空闲后首条老板消息 think 一开轮就被喊停, 表现为任务做不完/乱来)。
        if getattr(self.bus, "_master_preempt_flag", False):
            self.bus._master_preempt_flag = False
            logger.info("[Brain] 清理悬空抢占旗 (本次 think 自身消息置位, 不自抢)")
        self._preempt_requested = False
        self._thinking = True
        # [0.3.27.0 阶段5 规格七] 首轮普通聊天不发布 WorkStatus (零状态帧)。
        # 任务状态帧改由 tool_loop 收到首个工具调用后发布 (TaskRun 投影)。
        try:
            # [0.3.27.0 阶段6] think() 是字符串兼容包装, 内部走 think_structured
            # 主路径 (结构化结果取 reply); 行为零变化
            # [0.3.31.0 Batch 0] 整轮看门狗: 含工具循环的单轮硬上限, 超限优雅
            # 收尾 (finally 照常复位旗标); turn_timeout<=0 回退旧行为。
            self._turn_stage = "think_entry"
            # getattr 防御: __new__ 老夹具无此属性 = 看门狗关闭 = 旧行为零破坏
            _turn_timeout = float(getattr(self, "_TURN_TIMEOUT", 0) or 0)
            if _turn_timeout > 0:
                _result = await asyncio.wait_for(
                    self._think_impl(
                        user_input, associative_memory, source_channel,
                        chat_name, media_payloads, is_master, principal,
                        **({"thinking_mode": thinking_mode} if thinking_mode != "enabled" else {}),
                    ),
                    timeout=_turn_timeout,
                )
            else:
                _result = await self._think_impl(
                    user_input, associative_memory, source_channel,
                    chat_name, media_payloads, is_master, principal,
                    **({"thinking_mode": thinking_mode} if thinking_mode != "enabled" else {}),
                )
            if isinstance(_result, _task_rt.RunResult):
                return _result.reply
            return _result
        except asyncio.TimeoutError:
            logger.error(
                f"[Brain][watchdog] think 整轮超时 ({_turn_timeout:.0f}s), "
                f"卡死点={getattr(self, '_turn_stage', '')!r}, 优雅收尾兜底"
            )
            return ("呜……阿露这轮想得太多脑子卡住了，没能回完。"
                    "老板换个简单点的说法再问我一次好不好？")
        finally:
            # [短期记忆 v0.4.1] 清本轮预召回缓存, 防止泄漏到下一轮 think。
            self._warm_recall_text = ""
            # [双轴 prompt 上线 0.1.7.0] 清本轮亲疏缓存, 防泄漏到下一轮 think。
            self._social_context_text = ""
            self._thinking = False
            # [0.3.27.0 阶段6] 挂起旗标复位 (think_structured 读取后即清)
            self._suspended_this_turn = False
            # [0.3.27.0 阶段1 TaskRun] 本轮运行结束, 解除引用 (防串下一轮)
            self._current_run = None
            # [审查整改 M2] 复位活动标记: 治 REST 快照 idle+陈旧"批阅 X"矛盾,
            # 及直连路径下一轮 think 入口误用陈旧文案。
            self._current_activity = "待机"
            # [0.3.27.0 阶段5] 收尾状态帧由 tool_loop 按 TaskRun 结果发布
            # (completed/failed), 普通聊天全程零帧

    def _resolve_stream_active(self, eff_src: str, source_channel: str,
                               is_boot_msg: bool) -> bool:
        """[0.3.31.2 Batch 2] 流式判定: 白名单源 OR 批阅轮 (multi)。

        批阅非流式是 45s 一刀切超时主因 (15:23 事故: 批阅 22.6s/超时;
        流式对照组 10.7s): 流式首包快 + per_token 看门狗替代一次性掐线。
        streaming.batch_stream=false 一键回退旧行为; idle/boot 永不流式。
        """
        _cfg = getattr(self, "brain_config", None) or {}
        _streaming_cfg = _cfg.get("streaming", {}) or {}
        _batch_stream = bool(_streaming_cfg.get("batch_stream", True))
        return bool(
            getattr(self, "_stream_enabled", False)
            and ((eff_src in getattr(self, "_stream_sources", []))
                 or (source_channel == "multi" and _batch_stream))
            and source_channel not in ("idle", "boot")
            and not is_boot_msg
        )

    async def _load_prompt_memory(self, provider: str, active_apps: list[str],
                                  is_boot_msg: bool, current_time: str) -> tuple[str, str]:
        """Keep legacy global memory out of an explicitly scoped daily prompt."""
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if daily_retrieval_enabled(self.bus) or not provider or provider not in active_apps:
            return "", current_time
        soul_memory = ""
        try:
            soul_result = await self.bus.send_instruction(provider, "read_soul")
            if isinstance(soul_result, ToolResult) and soul_result.ok:
                soul_memory = soul_result.text
            if soul_memory.strip() in ("", "- 暂无核心事实记录。", "暂无核心事实记录。"):
                soul_memory = ""
        except Exception as exc:
            logger.error(f"[Brain] 读取灵魂记忆失败: {exc}")
        if not is_boot_msg:
            try:
                time_anchor = await self.bus.send_instruction(provider, "get_time_anchor")
                if isinstance(time_anchor, ToolResult) and time_anchor.ok:
                    current_time = time_anchor.text
            except Exception as exc:
                logger.error(f"[大脑] 时间锚点读取失败: {exc}")
        return soul_memory, current_time

    def _task_context_for_prompt(self, user_input: str) -> str:
        """Global tool examples are not scoped daily evidence."""
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if daily_retrieval_enabled(self.bus) or not getattr(self, "_TASK_CONTEXT_ENABLED", True):
            return ""
        try:
            from kernel.tool_memory import build_task_context_block
            return build_task_context_block(user_input or "")
        except Exception as exc:
            logger.debug(f"[Brain] 任务上下文装配失败(非致命): {exc}")
            return ""

    async def _think_impl(self, user_input: str, associative_memory: str = "", source_channel: str = "console", chat_name: str = "", media_payloads: list = None, is_master: bool = False, principal: "Principal" = None, *, thinking_mode: str = "enabled"):
        if any(cmd in user_input for cmd in ["清空大脑", "闭嘴重置", "清空记忆"]):
            self.chat_history.clear()
            if getattr(self, "_model_history", None) is not None:
                self._model_history.clear()
            return "（系统提示：所有会话短期缓存已清空。）"

        # [0.3.31.0 Batch 0] 看门狗卡死点标记 (超时归因用)
        self._turn_stage = "turn_reset"
        # [防抖重置] 新用户任务流开始，重置安心反馈标记和熔断计数器
        self._interim_sent = False
        self._task_ack_sent = False  # [0.3.21.0] 接单 ACK 每任务流一条
        self._tool_failure_count.clear()
        # [短期记忆 v0.4.1] 预召回缓存: think_batch 已填则注入, 否则本轮无预热。
        # 非 batch 路径 (console/boot/idle 直连 think) 默认空, 不做主动召回。
        if not hasattr(self, "_warm_recall_text"):
            self._warm_recall_text = ""
        # [内核2.0] 重置本轮出站标记
        self._outbound_sent_this_turn = False
        # [P0 0.1.9.12] 重置本轮出站文本收集, 防泄漏到下一轮 think
        self._outbound_texts_this_turn = []
        # [0.1.10.1] 重置本轮出站目标追踪, 防泄漏到下一轮 think
        self._outbound_targets_this_turn = []
        # [单轮出站熔断 v2] 重置本轮出站计数 (文本/媒体分开)
        self._outbound_text_count = 0
        self._outbound_media_count = 0
        # [0.2.8.0 出站收口] 重置契约代发标记 (防上轮泄漏致本轮兜底全哑)
        self._structured_outbound_this_turn = False
        self._structured_targets_this_turn = set()  # [0.2.22.0] per-target 契约锁重置
        # [主动联网] 重置本轮强制搜索标记

        # [技能树] 重置本轮对话的技能追踪状态
        self._skill_save_requested = False
        self._last_tool_called = None
        self._active_skill_meta = {}

        # [技能树] 检测技能编译触发词
        for phrase in self.SKILL_TRIGGER_PHRASES:
            if phrase in user_input:
                self._skill_save_requested = True
                logger.info(f"[Brain] 检测到技能编译触发词: '{phrase}'，将在任务成功后自动编译落盘。")
                break

        # [会话上下文] 记录当前消息的源渠道和对话窗口，供工具调用时传递给 trigger_msg
        # 前置条件: think() 在主循环中被顺序调用，不存在并发重叠。
        # 若未来引入并发 think()，需改为 contextvars 或显式参数传递。
        self._current_source = source_channel
        self._current_user_input = user_input
        self._current_chat_name = chat_name
        if source_channel != "multi":
            self._current_conversation_id = ""
            if getattr(self.bus, "raw_messages", None) is not None:
                self.bus.clear_raw_turn()
        # [权限闸门] 记录本轮触发者是否老板本人, 供 add_listen_chat 等敏感工具判定
        self._current_is_master = is_master
        # [0.3.15.0 P2 权限协议] 本轮调用主体: think_batch 透传组主体 (批次降档后的
        # 真实权限); 直连路径 (idle/boot/console 老链路) principal=None 时按 is_master
        # 兜底派生 (语义对齐)。闸门经 _principal_for_gate() 取, 老夹具仅设
        # _current_is_master 也能兜底派生, 存量测试零破坏。
        if principal is not None:
            self._current_principal = principal
        elif is_master:
            self._current_principal = Principal.owner(
                f"direct_{source_channel or 'unknown'}",
                source_channel or "unknown", "direct:is_master",
            )
        else:
            self._current_principal = Principal.guest(
                f"direct_{source_channel or 'unknown'}",
                source_channel or "unknown", "direct:is_master",
            )

        # [工具表] 已改为脏标记缓存 (_get_system_tools), 不再每轮重建
        is_boot_msg = user_input.startswith("[System Boot]")

        # [0.2.20.0 流式输出] 本轮 turn_id 赋值 (治 behavior_log 空 turn_id 旧账)
        # + 流式运行态重置 + 闸门决策。有效源 = think_batch 组源优先 (_current_group_src,
        # 批阅把 source 压成 multi, 组源才是真实渠道), 直连源兜底; 仅白名单源
        # (默认 [console]=桌面/控制台当面) 且非 boot/idle 走流式 LLM 调用。
        self._current_turn_id = uuid.uuid4().hex[:12]
        self._last_research_delivery = None
        self._last_think_status = "ok"
        self._stream_started_this_turn = False
        # [0.3.27.0 阶段1 TaskRun] 本轮任务运行时创建: 承载不可变身份
        # (run_id/source/chat_name/Principal 快照) 与可恢复状态。阶段1 双写
        # 引用共享 (行为完全保持): 可变账本 (list/dict) 与 brain 共享同一
        # 引用, 工具循环读写 brain 字段即同步到 run; messages 在组装后挂。
        # 阶段3+ 逐步把工具循环/出站层读取点迁移到 run, 阶段6 移除双写。
        # multi 是批处理调用方式，不是消息渠道；任务快照必须保存本组真实来源。
        _eff_src = (getattr(self, "_current_group_src", "") or source_channel) \
            if source_channel == "multi" else source_channel
        _dm = (
            _eff_src in ("console", "web", "feishu")
            or (source_channel == "multi" and bool(getattr(self, "_current_group_is_dm", False)))
        )
        self._current_run: _TaskRun = _task_rt.create_run(
            turn_id=self._current_turn_id,
            source=_eff_src,
            chat_name=chat_name,
            principal=self._current_principal,
            is_dm=_dm,
            conversation_id=getattr(self, "_current_conversation_id", ""),
            raw_agent_id=getattr(self.bus, "_raw_turn", {}).get("agent_id", ""),
            origin_message_id=getattr(self.bus, "_raw_turn", {}).get("event_id", ""),
        )
        _run = self._current_run
        if getattr(self.bus, "raw_messages", None) is not None:
            _run.raw_turn = self.bus.raw_turn_snapshot()
        _run.user_input = user_input
        _run.outbound_texts = self._outbound_texts_this_turn
        _run.outbound_targets = self._outbound_targets_this_turn
        _run.failure_count = self._tool_failure_count
        _run.llm_rounds = 0
        _run.thinking_mode = thinking_mode
        _run.model_name = getattr(self, "model_name", "")
        _run.model_source = _eff_src
        # [0.3.31.2 Batch 2] 流式判定抽 helper (批阅流式化治 45s 掐线, 可单测)
        self._stream_active = self._resolve_stream_active(
            _eff_src, source_channel, is_boot_msg)

        user_msg = None
        if not is_boot_msg:
            # 广播事件，让潜意识器官在后台默默记日记。
            # [内核2.0] 批阅模式 (source=multi) 下 user_speak 已由 think_batch 逐条
            # 剥信封 emit 过, 这里跳过避免重复 + 避免把整个快照(带信封)灌进记忆。
            # [上下文去污] idle 是阿露自身触发词 (非真用户语), 不 emit on_user_speak
            # (否则被 background_user_listener 当"用户说"记进向量库污染记忆)。
            if source_channel not in ("multi", "idle"):
                _memory_event = Message.user_speak(
                    content=user_input,
                    source="brain",
                    active_apps=list(self.bus.active_apps.keys()),
                )
                if getattr(self.bus, "raw_messages", None) is not None:
                    await self.bus.emit_user_memory(_memory_event, owned=False)
                else:
                    await self.bus.emit_msg(_memory_event)
            async with self._history_lock:
                user_msg = self._build_user_message(user_input, media_payloads)
                # [短期记忆 v0.4.1] 按会话分桶: 真实渠道用 chat_name,
                # console/multi/idle/boot 归并进 _system 巡视桶。
                # [上下文去污] idle 触发词不持久化进 chat_history (否则当 user 角色历史
                # 混进后续 think), 仅本次作 transient user 消息 (见下方 messages 组装)。
                if source_channel != "idle":
                    skey = _session_key(chat_name, source_channel, getattr(self, "_current_conversation_id", ""))
                    self.chat_history.setdefault(skey, []).append(user_msg)

        char_name = self.character_config.get('name', '阿露')
        identity = self.character_config.get('identity', '数字生命')
        
        active_apps = list(self.bus.active_apps.keys())
        prompt_available_apps, prompt_active_apps, dormant_apps = (
            self._prompt_app_lists()
        )
        current_time = datetime.datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
        
        soul_memory, current_time = await self._load_prompt_memory(
            self._resolve_memory_provider(), active_apps, is_boot_msg, current_time,
        )
        dynamic_rules, background_memory = "", ""

        app_info_str = "\n        ".join(
            [f"- {app_name}" for app_name in prompt_available_apps]
        )

        # 从已激活 app 的 manifest 中收集设备使用须知
        app_rules = self.bus._tool_builder.collect_app_rules(
            self.bus.available_apps
        )

        system_prompt = build_system_prompt(
            char_name, identity, soul_memory, dynamic_rules,
            current_time, prompt_active_apps, dormant_apps, app_info_str,
            background_memory,
            app_rules=app_rules,
            social_context=getattr(self, "_social_context_text", ""),
            # [0.3.17.0 P4-b] 活动智能体人设/目标注入 (空则 prompts 不渲染)
            persona=str(self.character_config.get("persona", "") or ""),
            goals=tuple(self.character_config.get("goals") or ()),
            model_name=getattr(self, "model_name", ""),
        )

        # [技能树] 输入端拦截注入: 在主脑组织 Prompt 发起 LLM 推理前，
        # 检索所有领域的活跃技能，将前人留下的满分技能准则强行拼装进 System Prompt
        all_skill_recipes, self._active_skill_meta = get_all_active_skills()
        if all_skill_recipes:
            skill_section = (
                f"\n\n<skill_inheritance_do_not_reply>\n"
                f"[技能传承 - 前人成功经验 (最高优先级行为指南)]\n"
                f"以下是系统从技能书库中检索到的、经过验证的高密度行为准则。\n"
                f"你必须严格遵循这些准则来执行当前任务，它们代表了过去的成功模式。\n"
                f"如果准则与你的默认行为冲突，以准则为准。\n\n"
                f"{all_skill_recipes}\n"
                f"</skill_inheritance_do_not_reply>"
            )
            system_prompt += skill_section
            total_skills = sum(len(v) for v in self._active_skill_meta.values())
            logger.info(f"[Brain] 已注入 {total_skills} 个传承技能到 System Prompt")

        # [技能树] 如果检测到编译触发词，追加技能编译指令到 System Prompt
        if self._skill_save_requested:
            compile_instruction = (
                f"\n\n<skill_compilation_task>\n"
                f"[技能编译指令 - 当前任务结束后必须执行]\n"
                f"老板要求你将本次成功经验总结为可复用的技能。\n"
                f"在你完成对用户的回复之后，你必须在回复的最末尾输出一个技能定义块，格式如下:\n"
                f"<skill_recipe>\n"
                f"domain: <领域名，如 wechat / image_generator / general>\n"
                f"name: <简短的技能名称，10字以内>\n"
                f"recipe: <高密度行为指南，包含成功的具体参数模式、关键步骤和避坑要点>\n"
                f"</skill_recipe>\n"
                f"注意: skill_recipe 标签内的内容用户不可见，系统会自动解析并编译落盘。\n"
                f"</skill_compilation_task>"
            )
            system_prompt += compile_instruction
            logger.info("[Brain] 已注入技能编译指令到 System Prompt")
        from kernel.raw_message_retrieval import daily_retrieval_enabled
        if associative_memory and not daily_retrieval_enabled(self.bus):
            system_prompt += (
                f"\n\n<system_memory_do_not_reply>\n"
                f"[核心指令：以下是系统提取的潜意识往事，仅供作为知识库参考！]\n"
                f"[你绝不能在回复中说“根据档案/潜意识/记忆”，必须自然地内化这些信息！]\n"
                f"[无论记忆内容有多混乱，你必须时刻保持便利屋68社长·阿露的傲娇人设！]\n"
                f"{associative_memory}\n"
                f"</system_memory_do_not_reply>"
            )

        # [短期记忆 v0.4.1] 预热近期流水账注入: 仅补短期上下文 (近期 obs), 不含核心
        # 事实/规矩。明确告知阿露这是系统灌的近期线索, 核心承诺/事实须主动 memory_recall
        # 核实、memory_remember 刻入——守护学习进化本体, 避免退化为靠灌水的聊天机器人。
        warm = getattr(self, "_warm_recall_text", "") or ""
        if warm and warm.strip():
            system_prompt += (
                f"\n\n<warm_context_do_not_reply>\n"
                f"[近期上下文预热 - 系统从流水账里替你捞的近期线索, 仅供补短期记忆]\n"
                f"{warm[:self._WARM_RECALL_INJECT_CAP]}\n"
                f"[边界: 这只是近期碎片, 不是核心事实。承诺/长期偏好/规矩你必须主动 "
                f"memory_recall 核实并 memory_remember 刻入, 那才是你学习进化的能力。]\n"
                f"[边界补充: 标注[跨群参考:X群]的记忆来自其他对话, 仅供背景理解, "
                f"绝不在当前对话主动提及其中的人/事/图, 否则就是串台。]\n"
                f"</warm_context_do_not_reply>"
            )

        # [0.3.31.3 Batch 3] codex 式任务上下文: 过去成功工具调用范例召回注入
        # (治"有 skill 有账号 ID 但想不起来调用"); 零命中零注入不打扰闲聊;
        # brain.yaml task_context.enabled=false 一键关。
        _task_ctx = self._task_context_for_prompt(user_input)
        if _task_ctx:
            system_prompt += (
                f"\n\n<task_experience_do_not_reply>\n"
                f"{_task_ctx}\n"
                f"</task_experience_do_not_reply>"
            )

        # [v0.2.6.0] 主动补历史注入: 桶薄时系统拉的近期聊天记录, 瞬态补短期记忆。
        # 仅 source_channel=="multi"(think_batch 路径, 唯一设置 _history_backfill_text 处)
        # 注入, 防陈旧值漏进 boot/idle/console。不写 chat_history 桶, 仅供理解上下文。
        backfill = getattr(self, "_history_backfill_text", "") or ""
        if backfill and backfill.strip() and source_channel == "multi":
            system_prompt += (
                f"\n\n<recent_history_do_not_reply>\n"
                f"[近期聊天记录 - 系统帮你补的本次对话近况, 仅供补短期记忆]\n"
                f"{backfill}\n"
                f"[边界: 这是系统拉的历史聊天, 不是你存的记忆, 仅供理解上下文, 别据此主动回复。]\n"
                f"</recent_history_do_not_reply>"
            )

        # [短期记忆 v0.4.1] 截断按会话桶独立: 老板私聊桶不被群噪音挤掉。
        # 每个桶各自保留最近 N 轮, _system 桶用更短窗口。
        async with self._history_lock:
            skey = _session_key(chat_name, source_channel, getattr(self, "_current_conversation_id", ""))
            bucket = self.chat_history.get(skey, [])
            window = self._SYSTEM_WINDOW if skey == "_system" else self._SESSION_WINDOW
            if len(bucket) > window:
                truncated = bucket[-window:]
                # 保持对话轮次完整性（偶数截断）
                if len(truncated) % 2 != 0:
                    truncated = truncated[1:]
                self.chat_history[skey] = truncated
                bucket = truncated

        messages = [{"role": "system", "content": system_prompt}]
        if is_boot_msg:
            messages.append({"role": "user", "content": user_input})
        else:
            from kernel.web_research.history import project_research_history
            messages.extend(project_research_history(bucket))
            if source_channel == "idle" and user_msg is not None:
                # idle 触发词本次作 user (transient, 未持久化进 chat_history, 仅本轮用)
                messages.append(user_msg)

        if is_deepseek(getattr(self, "model_name", "")) and not is_boot_msg and source_channel != "idle":
            if getattr(self, "_model_history", None) is None:
                self._model_history = ProtocolHistory()
            protocol_messages, turn_start = self._model_history.begin(protocol_key(self._current_run), bucket)
            messages = [{"role": "system", "content": system_prompt}] + protocol_messages
            self._current_run.protocol_version = 1
            self._current_run.protocol_turn_start = turn_start + 1

        # [0.3.27.0 阶段1 TaskRun] 完整模型消息挂到本轮任务运行时
        _run = getattr(self, "_current_run", None)
        if _run is not None:
            _run.messages = messages

        # [微内核解耦] think() 仅做调度: 工具循环 + 输出终结化全部抽离。
        # [0.2.20.0] LoopOutcome 携带三态: 传 _last_think_status 供主循环 agent_speak
        # 兜底携带给桌面; 空回复兜底文案"思考超时"归 error; 流式路径收尾 emit
        # stream_end (content=最终净化全文, UI 整体替换气泡, 兜底铁律)。
        outcome = await self._run_tool_loop(messages)
        if outcome.status == "suspended":
            # [0.3.27.0 阶段4] 任务挂起等待老板回答: 不产生最终回复/不落历史,
            # 空 reply 使主循环兜底路由不触发 (挂起期间其他会话正常处理)
            self._last_think_status = "ok"
            self._suspended_this_turn = True
            return ""
        options = {"allow_actions": False} if outcome.summary_only or outcome.status == "tool_errors" else {}
        if outcome.status == "tool_errors":
            options["empty_reply"] = _tool_loop.TOOL_ERRORS_REPLY
        clean_reply = await self._finalize_reply(outcome.reply, active_apps, is_boot_msg, **options)
        status = outcome.status
        # [0.3.27.0 阶段3 规格五.6] max_steps 有独立文案, 不伪装成功/思考超时
        if status == "max_steps":
            if not outcome.reply:
                clean_reply = "任务步骤已超出上限, 已自动停止, 需要调整的地方请告诉我。"
            # 已有回执的限额说明可正常交付；任务仍为 FAILED，不冒充 COMPLETED。
            status = "max_steps" if outcome.summary_only else "error"
        if clean_reply == "思考超时，请稍后再试。":
            status = "error"
        self._last_think_status = status
        if clean_reply and getattr(self.bus, "raw_messages", None) is not None:
            clean_reply = self.bus.reply_text(clean_reply, self._current_turn_id)
        # [0.3.27.0 阶段1 TaskRun] 收尾更新运行状态 (结构化, 不用 _thinking 猜)
        _run = getattr(self, "_current_run", None)
        if _run is not None:
            if _run.status == _task_rt.PROBING:
                _run.status = _task_rt.COMPLETED if status == "ok" else _task_rt.FAILED
            _run.messages = messages
            if status == "ok":
                remember_run(self, _run)
        if getattr(self, "_stream_active", False) and getattr(
                self, "_stream_started_this_turn", False):
            try:
                await self.bus.emit_msg(Message.agent_stream_end(
                    content=clean_reply or "",
                    turn_id=getattr(self, "_current_turn_id", ""),
                    status=status,
                    **delivery_metadata(self),
                ))
            except Exception as e:
                logger.warning(f"[Brain] stream_end 发射失败 (非致命): {e}")
        return clean_reply
