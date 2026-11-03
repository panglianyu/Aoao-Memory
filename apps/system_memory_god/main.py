# G:\Aoao_OS\apps\system_memory\main.py
"""
Aoao OS 记忆与潜意识系统 (System Memory)

核心职责：
  - 长期记忆存储与检索（ChromaDB 向量库）
  - 实体知识图谱维护（EntityStore）
  - 性格规则新陈代谢（RuleStore + ReflectionDaemon）
  - 时间锚点与主动意识（SchedulerDaemon）
  - 垃圾回收与记忆压缩（GarbageCollectorDaemon）

[架构红线 - 绝对核心资产，不可降级]
SystemMemory 是 Aoao OS 的灵魂资产，享有以下第一公民特权：
  - AppBus 内轨高优先级、低延迟的上下文主动注入能力
  - 后台守护进程（ReflectionDaemon / SchedulerDaemon）的独立性
  - 与 aluo_brain 的紧耦合记忆回路

绝不允许将 SystemMemory 重构或包装为 MCP Server。
MCP 外轨仅适用于外部第三方应用、静态文档读取和非核心工具。
"""
import os
import re
import json
import time
import base64
import asyncio
from datetime import datetime, timedelta
from openai import AsyncOpenAI
from kernel.agent_runtime import _is_narration_statement  # [Problem 2] 过滤 narration 不入 obs

# 强制关闭 ChromaDB 遥测
os.environ["ANONYMIZED_TELEMETRY"] = "False"

from protocols.standard_plug import AoaoPlug
from kernel.message import Message
from kernel.image_utils import read_image_as_data_url
from kernel.model_profile import is_deepseek, request_options
from kernel.model_conversation import check_request_budget, validate_deepseek_images
from system_memory_god.storage.vector_store import VectorStore
from system_memory_god.storage.file_io import FileIO
from system_memory_god.storage.entity_store import EntityStore
from system_memory_god.storage.rule_store import RuleStore
from system_memory_god.storage.graph_store import GraphStore
from system_memory_god.daemons.reflection import ReflectionDaemon
from system_memory_god.daemons.garbage_collector import GarbageCollectorDaemon
from system_memory_god.daemons.scheduler import SchedulerDaemon
from system_memory_god.utils.llm_client import LLMClient
from kernel.character_registry import MEMORY_MODE_SHARED
from kernel.logger import get_logger

logger = get_logger(__name__)

def _month_start(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _shift_month(value: datetime, months: int) -> datetime:
    index = value.year * 12 + value.month - 1 + months
    return value.replace(year=index // 12, month=index % 12 + 1, day=1)


def parse_when_filter(text: str, now_ts: float | None = None) -> tuple | None:
    """把 when_filter 自然语言解析为 (start_ts, end_ts) 秒级区间。

    例: "最近一周" -> (now-7d, None); "昨天" -> (昨天0点, 今天0点)。
    无法解析返回 None。纯函数无副作用, 单测直接钉死契约。
    """
    if not text or not text.strip():
        return None
    t = text.strip()
    now = datetime.fromtimestamp(
        time.time() if now_ts is None else float(now_ts),
    ).astimezone()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    def _range(start: datetime, end: datetime) -> tuple[float, float]:
        return start.timestamp(), end.timestamp()

    rolling = (
        (r"最近\s*(\d+)\s*(?:天|日)", 1),
        (r"最近\s*(\d+)\s*周", 7),
        (r"最近\s*(\d+)\s*个?月", 30),
    )
    for pattern, unit_days in rolling:
        match = re.search(pattern, t)
        if match:
            return _range(now - timedelta(days=int(match.group(1)) * unit_days), now)

    fixed_rolling = (
        ("最近一周", 7), ("一周内", 7),
        ("最近一个月", 30), ("一个月内", 30),
        ("最近一年", 365), ("一年内", 365),
        ("三天内", 3),
    )
    for phrase, days in fixed_rolling:
        if phrase in t:
            return _range(now - timedelta(days=days), now)

    if "前天" in t:
        return _range(today - timedelta(days=2), today - timedelta(days=1))
    if "昨天" in t:
        return _range(today - timedelta(days=1), today)
    if "今天" in t:
        return _range(today, today + timedelta(days=1))

    week_start = today - timedelta(days=today.weekday())
    if "上周" in t:
        return _range(week_start - timedelta(days=7), week_start)
    if "本周" in t:
        return _range(week_start, week_start + timedelta(days=7))

    month_start = _month_start(today)
    if "上个月" in t or "上月" in t:
        return _range(_shift_month(month_start, -1), month_start)
    if "本月" in t:
        return _range(month_start, _shift_month(month_start, 1))

    year_start = today.replace(month=1, day=1)
    if "去年" in t:
        return _range(year_start.replace(year=year_start.year - 1), year_start)
    if "今年" in t:
        return _range(year_start, year_start.replace(year=year_start.year + 1))
    return None


def derive_visibility(meta: dict) -> str:
    """[0.3.16.0 P4] 从消息 metadata 派生记忆隐私桶 (写入侧打桶, 召回侧硬 ACL)。

    桶契约:
      private  -- 老板1v1 (is_dm_with_master 且 is_master): 群聊永不召回,
                  仅同私聊窗口 + 系统直连可见 (报告 P4 验收: 私聊不泄群);
      group    -- 有对话名的群聊消息: 同群 + 主人上下文可召回;
      public   -- 无对话名 (console 当面/来源不明): 到处可见, 宁泄勿失
                  记忆连续性 (fail-safe 方向, 收紧靠渠道补 is_dm 签发)。

    is_dm_with_master 由 brain.think_batch speak_md 透传 (wechat 拆信封时打);
    渠道没打 is_dm 时老板私聊落 group/public (可见性放宽不漏记, 待渠道补齐)。
    """
    is_master = bool(meta.get("is_master", False))
    chat_name = str(meta.get("chat_name", "") or "")
    is_dm = bool(meta.get("is_dm_with_master", False))
    if is_dm and is_master:
        return "private"
    if chat_name:
        return "group"
    return "public"


def _ledger_row_to_dto(row: dict) -> dict:
    """[0.3.30.5 G04] ledger 原始行 -> 规格 5.1 MemoryItem DTO。

    live 与 inactive 投影必须同形 (Phase 1 验收: live 与 inactive DTO
    schema 相同); inactive 侧 (kernel/memory_projection) 已按此形状组装,
    live 侧此前直返原始行 (memory_id/content_preview 键名) 导致前端
    MemoryItemV2 字段落空。
    """
    store = str(row.get("store", "") or "")
    store_ref = str(row.get("store_ref", "") or "")
    entity_name = (store_ref.split(".", 1)[0]
                   if store == "entity" and "." in store_ref else None)
    return {
        "id": str(row.get("memory_id", "") or ""),
        "kind": str(row.get("kind", "") or ""),
        "store": store,
        "content": str(row.get("content_preview", "") or ""),
        "status": str(row.get("status", "active") or "active"),
        "created_at": row.get("created_at"),
        "valid_from": row.get("valid_from"),
        "valid_to": row.get("valid_to"),
        "speaker": str(row.get("speaker", "") or ""),
        "chat_name": str(row.get("chat_name", "") or ""),
        "visibility": str(row.get("visibility", "") or ""),
        "importance": row.get("importance"),
        "confidence": row.get("confidence"),
        "hit_count": int(row.get("hit_count", 0) or 0),
        "promoted": bool(row.get("promoted", False)),
        "source_count": int(row.get("source_count", 0) or 0),
        "has_history": bool(row.get("has_history", False)),
        "links": {"entity": entity_name, "graph_node": None,
                  "source": bool(store_ref)},
    }


# [0.3.34.0 B0a] live 规矩视图折叠: rules.json 老规矩未入 ledger, 此前
# kind=procedural_rule 只见入账顿悟 (8 条), 与 inactive 侧全量折叠不一致。
# 虚拟 DTO 与 inactive 投影同形 (规格 5.1), 只读零写。
def _preview160(text) -> str:
    if not text:
        return ""
    return str(text).strip()[:160]


def _norm_text(text) -> str:
    return " ".join(str(text or "").split())


def _rule_sort_ts(v) -> float:
    """created_at 兼容 float 秒与 'YYYY-MM-DD HH:MM:SS' 字符串 (无法解析=0)。"""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            import time as _t
            return _t.mktime(_t.strptime(v, "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError, OverflowError, OSError):
            return 0.0
    return 0.0


def _rule_to_dto(r: dict) -> dict:
    return {
        "id": f"rule_{r.get('id', '')}",
        "kind": "procedural_rule",
        "store": "rule",
        "content": _preview160(r.get("text", "")),
        "status": "active",
        "created_at": r.get("created_at"),
        "valid_from": None,
        "valid_to": None,
        "speaker": "",
        "chat_name": "",
        "visibility": "",
        "importance": None,
        "confidence": None,
        "hit_count": int(r.get("hit_count", 0) or 0),
        "promoted": False,
        "source_count": 0,
        "has_history": False,
        "links": {"entity": None, "graph_node": None, "source": False},
    }


async def _items_rules_merged(ledger, rule_store, status: str = "",
                              chat_name: str = "", speaker: str = "",
                              visibility: str = "", query: str = "",
                              start_ts: float = None, end_ts: float = None,
                              exclude_visibilities: tuple[str, ...] = (),
                              limit: int = 50, offset: int = 0) -> list:
    """[0.3.34.0 B0a] live 规矩合并视图 (只读零写, 守浏览红线)。

    ledger 入账顿悟规矩 + rules.json 全量规矩 (文本去重 ledger 优先,
    与 inactive 侧折叠同形)。虚拟规矩无 speaker/隐私桶/时间元数据:
    这些过滤面启用时只返 ledger 入账规矩 (不造假); 文本 query 对两侧
    生效。合并后按 created_at 倒序 + Python 分页 (数百条量级内存安全)。
    """
    restrictive = any([
        status not in ("", "active"),
        chat_name, speaker, visibility,
        start_ts is not None, end_ts is not None,
    ])
    if restrictive:
        rows = await asyncio.to_thread(
            ledger.list_items,
            kind="procedural_rule", status=status, chat_name=chat_name,
            speaker=speaker, visibility=visibility, query=query,
            start_ts=start_ts, end_ts=end_ts,
            exclude_visibilities=exclude_visibilities,
            limit=limit, offset=offset,
        )
        return [_ledger_row_to_dto(r) for r in rows]
    rows = await asyncio.to_thread(
        ledger.list_items, kind="procedural_rule", query=query,
        exclude_visibilities=exclude_visibilities, limit=200, offset=0,
    )
    dtos = [_ledger_row_to_dto(r) for r in rows]
    seen = {_norm_text(d["content"]) for d in dtos}
    for r in await rule_store.get_all_rules():
        if not isinstance(r, dict):
            continue
        text = str(r.get("text", "") or "").strip()
        if not text:
            continue
        if query and query not in text:
            continue
        key = _norm_text(_preview160(text))
        if key in seen:
            continue
        seen.add(key)
        dtos.append(_rule_to_dto(r))
    dtos.sort(key=lambda d: _rule_sort_ts(d.get("created_at")),
              reverse=True)
    start = max(0, offset)
    return dtos[start:start + max(1, min(limit, 200))]


# [0.3.17.0 P4-b D6] 灵魂文档: 新智能体身份 core 事实的固定实体名。
# 每个命名空间有自己独立的 entities.json, 固定名不跨角色冲突; snapshot
# 继承时源智能体的自我描述 (实体名=源智能体名) 降级非 core, 新身份上位。
IDENTITY_ENTITY = "自我身份"
_IDENTITY_GOAL_SEP = "；"


async def seed_agent_identity(entity_store, agent, base_name: str = "") -> bool:
    """[D6] 把智能体身份 core 事实播种进其实体库 (灵魂文档)。

    - shared / builtin: 跳过 (共用源库, 播种会污染阿露记忆);
    - snapshot: 先降级 base_name 实体名的 core 事实为非 core (源身份变史料,
      不删值), 再播种新身份;
    - 幂等: 值一致且已 core -> 整条跳过 (不累 hit_count/不产生版本), 注册表
      人设漂移 -> supersede 旧值入 history 新值 alive (重启自愈)。

    返回是否执行了播种。调用方自行 try/except, 失败永不阻塞 boot。
    """
    if agent.builtin or agent.memory_mode == MEMORY_MODE_SHARED:
        return False
    if base_name:
        demoted = await entity_store.demote_entity_core_facts(base_name)
        if demoted:
            logger.info(
                f"[Memory] 灵魂文档: 源智能体 [{base_name}] 继承 core 事实 "
                f"{demoted} 条降级为非 core (留作记忆史料)")
        # [review H3] 链式快照: 源库带着源自己的 [自我身份] core 事实,
        # 继承的名字不是自己 -> 整组降级再播种 (防新智能体自认是源)。
        inherited = await entity_store.get_entity(IDENTITY_ENTITY) or {}
        inherited_name = str((inherited.get("名字") or {}).get("value", ""))
        if inherited_name and inherited_name != agent.name:
            demoted_id = await entity_store.demote_entity_core_facts(
                IDENTITY_ENTITY)
            logger.info(
                f"[Memory] 灵魂文档: 继承的源身份 [{inherited_name}] "
                f"{demoted_id} 条降级为非 core")
    desired = {
        "名字": agent.name,
        "身份": agent.identity,
        "人设": agent.persona,
        "目标": _IDENTITY_GOAL_SEP.join(agent.goals),
    }
    current = await entity_store.get_entity(IDENTITY_ENTITY) or {}
    for key, value in desired.items():
        if not value:
            continue
        existing = current.get(key) or {}
        if (str(existing.get("value", "")) == value
                and existing.get("is_core_fact")):
            continue  # 幂等: 已播且未漂移, 整条跳过
        await entity_store.update_entity(
            IDENTITY_ENTITY, key, value, is_core_fact=True, weight=10)
    logger.info(f"[Memory] 灵魂文档就位: {agent.name} [{IDENTITY_ENTITY}]")
    return True


# [0.3.36.0 B2a] 捕获白名单铁律 (研究借鉴 OpenViking capture.mjs):
# 系统注入块绝不回收入库, 防合成文本"洗"成记忆。反例: Hindsight #1751
# 递归自污染日烧 5M token。保守原则: 只排已知系统标记形态, 不误伤
# 老板正常话语 (正文恰含"任务经验"等词但无标记形态的照常入库)。
CAPTURE_BLOCK_MARKERS = (
    "[任务经验", "task_experience_do_not_reply",
    "[技能传承", "skill_inheritance_do_not_reply",
    "[技能编译指令",
    "system_memory_do_not_reply", "[核心指令：以下是系统提取的潜意识往事",
    "[近期上下文预热", "warm_context_do_not_reply",
    "[外部感知:",
    "[系统告警",
)


def is_blocked_capture(text) -> bool:
    """返回 True = 判定为系统注入内容, 捕获层 (journal/向量/反思) 全丢弃。"""
    if not text or not isinstance(text, str):
        return False
    return any(marker in text for marker in CAPTURE_BLOCK_MARKERS)


class SystemMemory(AoaoPlug):
    def __init__(self):
        super().__init__()
        # [0.3.17.0 P4-b] 活动智能体记忆命名空间: root 跟随注册表活动智能体。
        # 任何异常回退阿露原路径 (data/memory), 零行为变化 -- boot 关键路径
        # fail-safe (registry.get_active 本已永不抛, 此处双保险)。
        self._active_agent = None
        try:
            from kernel.character_registry import get_active
            self._active_agent = get_active()
            self.graph_data_dir = self._active_agent.memory_root
            self.data_dir = os.path.join(self._active_agent.memory_root, "soul")
            os.makedirs(self.data_dir, exist_ok=True)
            if not self._active_agent.builtin:
                logger.info(
                    f"[Memory] 活动智能体 {self._active_agent.name} "
                    f"(mode={self._active_agent.memory_mode}), "
                    f"记忆根: {self.graph_data_dir}")
        except Exception as e:
            logger.warning(f"[Memory] 活动智能体读取失败, 回退阿露默认记忆: {e}")
            self._active_agent = None
            self.data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data/memory/soul")
            self.graph_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data/memory")
            os.makedirs(self.data_dir, exist_ok=True)

        self.vector_store = VectorStore(self.data_dir)
        self.file_io = FileIO(self.data_dir)
        self.entity_store = EntityStore(self.data_dir)
        self.rule_store = RuleStore(self.data_dir)
        self.llm_client = LLMClient()

        # [0.3.26.0 Phase 1] Memory Ledger 来源账本 (规格 4.1): 新写入稳定 ID +
        # 来源事件 + 派生关系 + 生命周期审计; 写失败进 backlog 绝不阻断主链。
        from system_memory_god.storage.memory_ledger import MemoryLedger
        self.ledger = MemoryLedger(
            os.path.join(self.data_dir, "memory_ledger.db"),
        )

        # [GraphStore Phase 1 后续] 知识/社交图谱 (SQLite + NetworkX)。
        # 放记忆根 (与 soul 同级, graph.db + edits_audit.jsonl + training_pairs.jsonl);
        # [0.3.17.0 P4-b] 根随活动智能体命名空间。
        # 通过 AppBus 暴露 graph_* 命令给 UI/老板控制台; reflection daemon 自动构图写入。
        # 守 capability-generic-not-coupled (不焊进 kernel, 不独立成 app)。
        self.graph_store = GraphStore(self.graph_data_dir)

        self.reflection_daemon = ReflectionDaemon(
            self.vector_store, self.file_io, self.rule_store, self.llm_client,
            self.graph_store, entity_store=self.entity_store,
            # [0.2.10.0] 反思缓冲持久化: 闪退/强退/关窗口不丢未反思对话, 开机续反
            buffer_path=os.path.join(self.data_dir, "reflection_buffer.jsonl"),
        )
        # [0.3.26.0 Phase 1] ledger 注入反思 daemon (episode 记账 + 派生链),
        # getattr 兜底: 测试夹具无此属性时反思照常运行。
        self.reflection_daemon._ledger = self.ledger

        # [0.3.26.0 Phase 2] Recall Plan 统一召回规划器 (规格 4.2): 聚合
        # vector/entity/rule/graph 候选 -> ACL -> 过滤 -> 预算 -> 解释。
        from system_memory_god.recall_plan import RecallPlan
        self.recall_plan = RecallPlan(
            vector_store=self.vector_store,
            entity_store=self.entity_store,
            rule_store=self.rule_store,
            graph_store=self.graph_store,
        )

        # [0.3.26.0 Phase 3] Curation 安全治理 (规格 4.4): 持久任务 +
        # 确定性 proposal + apply/undo。REST 层经主人权限闸门才暴露写操作。
        from system_memory_god.curation import CurationEngine
        from system_memory_god.storage.curation_runs import CurationRuns
        self.curation_runs = CurationRuns(
            os.path.join(self.data_dir, "curation_runs.db"),
        )
        self.curation = CurationEngine(
            self.curation_runs,
            entity_store=self.entity_store,
            rule_store=self.rule_store,
            vector_store=self.vector_store,
            ledger=self.ledger,
        )
        self.gc_daemon = GarbageCollectorDaemon(self.vector_store, self.entity_store)  # v0.6.0 双轴 attention 衰减
        self.scheduler_daemon = SchedulerDaemon(self.data_dir)

        self._background_tasks: set = set()  # 保留后台 task 引用，防止被 GC 回收
        # P3: 实体提取 LLM 并发信号量, 限制同时进行的提取请求数, 超出者排队 (不丢)。
        self._extract_sem = asyncio.Semaphore(self._EXTRACT_CONCURRENCY)

    def create_daily_memory(self, settings, *, client, model, profile_id="legacy-daily-v1"):
        """Trusted host hook: reuse the Brain client; never load credentials here."""
        from kernel.daily_memory_bootstrap import DailyMemorySettings
        from system_memory_god.memory_factory import create_memory_service
        from system_memory_god.derivation import StructuredDailyExtractor, daily_specs

        if type(settings) is not DailyMemorySettings or client is None:
            raise ValueError("explicit daily host configuration required")
        if profile_id not in {"legacy-daily-v1", "raw-vector-v1", "memory-full-v1"}:
            raise ValueError("unknown daily memory profile")
        from system_memory_god.grouped_extractor import memory_specs
        profile = settings.make_profile(daily_specs(model) if profile_id == "legacy-daily-v1"
                                        else memory_specs(profile_id, model))
        extractor = StructuredDailyExtractor(client, model) if profile_id == "legacy-daily-v1" else None

        def factory(raw, configured_profile, bus):
            if configured_profile is not profile:
                raise ValueError("daily factory profile mismatch")
            return create_memory_service(profile, mode=profile_id, raw=raw, bus=bus,
                extractor=extractor, client=client, model=model,
                embedding="bge-m3", semantic_mode="snapshot")

        return profile, factory

    async def read_daily_legacy(self, scope, mapping, query):
        """Strictly mapped, already warm, read-only vector evidence; no Raw claim."""
        from pathlib import Path
        from kernel.daily_memory_bootstrap import LegacySourceBinding

        active = self._active_agent
        if (type(mapping) is not LegacySourceBinding or active is None or active.id != scope.agent_id
                or mapping.agent_id != scope.agent_id or mapping.owner_id != scope.user_id
                or mapping.connector_id != scope.connector_id
                or Path(self.graph_data_dir).resolve() != mapping.memory_root.resolve()
                or not getattr(active, "builtin", False) and getattr(active, "memory_mode", "") == "shared"
                or type(query) is not str or not query.strip() or len(query) > 200
                or self.vector_store.collection is None):
            raise ValueError("legacy mapping or ready store unavailable")
        embedding = self.vector_store._embedding_function
        warmed = getattr(embedding, "warmed", None)
        if callable(warmed) and not warmed():
            raise RuntimeError("legacy embedding not already warm")
        pairs = await asyncio.to_thread(self.vector_store.query_memory_with_meta,
            query, n_results=10, prefer_chat=mapping.legacy_chat, asker_is_master=False,
            acl_enabled=True, commit_hits=False)
        return [{"domain": "legacy_source", "content": document,
                 "memory_id": str(metadata.get("id", "") or "")[:256]}
                for document, metadata in pairs if isinstance(document, str) and type(metadata) is dict
                and metadata.get("chat_name") == mapping.legacy_chat
                and metadata.get("visibility") in {"private", "public"}]

    async def _maybe_seed_identity(self) -> None:
        """[0.3.17.0 P4-b D6] 灵魂文档自检播种: 新智能体在自己记忆里要有可召回
        的身份文档。首 boot 播种 / 注册表人设改了重启同步 / snapshot 继承的源
        身份降级非 core。任何异常仅 warning, 永不阻塞 boot (记忆连续性优先)。
        """
        try:
            if self._active_agent is None:
                return
            base_name = ""
            if self._active_agent.base_agent_id:
                from kernel.character_registry import get_agent
                # [review M6] 同步文件 IO 不进事件循环线程
                base = await asyncio.to_thread(
                    get_agent, self._active_agent.base_agent_id)
                base_name = base.name if base is not None else ""
            await seed_agent_identity(
                self.entity_store, self._active_agent, base_name)
        except Exception as e:
            logger.warning(f"[Memory] 灵魂文档播种失败 (非致命): {e}")

    async def read_soul_async(self):
        """从实体图谱中提取核心事实，构建灵魂记忆。"""
        core_facts = await self.entity_store.get_core_facts()
        if not core_facts:
            return "- 暂无核心事实记录。"
        lines = []
        for fact in core_facts:
            lines.append(f"- 【{fact['entity']}】{fact['key']}：{fact['value']}")
        return "[核心身份记忆]\n" + "\n".join(lines)

    # ============ [0.3.26.0 Phase 1] Memory V2 只读方法 (LiveProjection 直调) ============
    # 经 kernel/memory_projection.py 的 LiveMemoryProjection 暴露给 /api/memory/v2/*,
    # 全只读: 不启动 daemon/不增加 hit/不打开写连接。fail-safe 同 execute_command。

    async def memory_overview_v2(self) -> dict:
        """ledger 聚合 + 存量真数 + daemon 状态 + backlog (规格 5.2 overview)。

        [0.3.30.5 G12/G12a] 指标修正:
        - provenance_covered 旧口径 (ledger 内 mem_ 前缀计数 / ledger 总数)
          恒等于 1.0 是假象 -> 改真实口径: 已入账条目 / 全部存量资产估算;
        - 新增各存储真实存量 (vector/entity/rule/graph) + 反思积压。
        """
        try:
            counts, _ledger_backlog = await asyncio.to_thread(
                lambda: (self.ledger.overview_counts(),
                         self.ledger.backlog_size()),
            )
            # ---- 存量真数 (每项独立 try, 缺能力落 None 不造假) ----
            _vec_total = None
            try:
                _col = getattr(self.vector_store, "collection", None)
                if _col is not None:
                    _vec_total = int(await asyncio.to_thread(_col.count))
            except Exception:
                _vec_total = None
            _entities_count, _entity_facts = None, 0
            try:
                _ents = await self.entity_store._load()
                _entities_count = len(_ents)
                for _props in _ents.values():
                    if not isinstance(_props, dict):
                        continue
                    for _key, _value in _props.items():
                        if str(_key).startswith("__"):
                            continue
                        _entity_facts += 1
                        if isinstance(_value, dict):
                            _entity_facts += len(_value.get("history") or [])
            except Exception:
                pass
            _rules_count = None
            try:
                _rules_count = int(await self.rule_store.get_rule_count())
            except Exception:
                pass
            _graph = None
            try:
                _graph = await self.graph_store.stats()
            except Exception:
                pass
            _journal_total = None
            try:
                _journal_total = int(await self.file_io.count_journal_entries())
            except Exception:
                pass
            _schedule_total = None
            try:
                _schedule_total = len(self.scheduler_daemon._reminders)
            except Exception:
                pass
            # 同单位、分 store 的估算。精确 missing/dangling 由只读
            # scripts/reconcile_memory_ledger.py 按 store_ref 集合给出。
            _source_totals = {
                "vector": _vec_total,
                "entity": _entity_facts,
                "rule": _rules_count,
                "graph": (sum(((_graph or {}).get(k) or 0)
                              for k in ("nodes", "edges"))
                          if _graph is not None else None),
                "journal": _journal_total,
                "schedule": _schedule_total,
            }
            _by_store = counts.get("by_store", {})
            _coverage_by_store = {}
            _asset_total = 0
            _covered_total = 0
            for _store, _source_total in _source_totals.items():
                if not isinstance(_source_total, int):
                    continue
                _ledger_total = int(_by_store.get(_store, 0) or 0)
                _matched_estimate = min(_ledger_total, _source_total)
                _asset_total += _source_total
                _covered_total += _matched_estimate
                _coverage_by_store[_store] = {
                    "ledger": _ledger_total,
                    "source": _source_total,
                    "coverage_estimate": round(
                        _matched_estimate / _source_total, 4,
                    ) if _source_total else 1.0,
                }
            _covered = (_covered_total / _asset_total
                        if _asset_total > 0 else 0.0)
            return {
                "by_kind": counts["by_kind"],
                "by_status": counts["by_status"],
                "by_store": _by_store,
                "total": counts["total"],
                "provenance_covered": round(_covered, 4),
                "provenance_exact": False,
                "provenance_by_store": _coverage_by_store,
                "provenance_basis": {
                    "ledger_items": counts["total"],
                    "tracked_assets": _asset_total,
                    "note": "精确值需运行只读 store_ref reconciliation",
                },
                "stocks": {
                    "vector_total": _vec_total,
                    "entities": _entities_count,
                    "entity_facts": _entity_facts,
                    "rules": _rules_count,
                    "graph": _graph,
                },
                "pending_reflection": len(
                    getattr(self.reflection_daemon, "buffer", []) or []),
                "ledger_backlog": _ledger_backlog,
                "daemons": {
                    "reflection": self._daemon_alive(self.reflection_daemon),
                    "gc": self._daemon_alive(self.gc_daemon),
                    "scheduler": self._daemon_alive(self.scheduler_daemon),
                },
            }
        except Exception as e:
            logger.warning(f"[Memory] overview_v2 失败 (非致命): {e}")
            return {"error": str(e)}

    @staticmethod
    def _daemon_alive(daemon) -> bool:
        t = getattr(daemon, "task", None)
        return bool(t is not None and not t.done())

    async def memory_items_v2(self, kind: str = "", status: str = "",
                              chat_name: str = "", speaker: str = "",
                              visibility: str = "", query: str = "",
                              start_ts: float = None, end_ts: float = None,
                              exclude_visibilities: tuple[str, ...] = (),
                              limit: int = 50, offset: int = 0) -> list:
        """ledger 分页筛选 (规格 5.2 items, cursor 分页)。

        [0.3.30.5 G04] 返回规格 5.1 DTO 形状 (与 inactive 投影同形)。
        [0.3.32.0 三梯队 G03] 过滤面补齐: speaker/visibility/query/时间。
        [0.3.34.0 B0a] kind=procedural_rule 折叠 rules.json 全量规矩
        (ledger 入账顿悟 + 未入账老规矩, 文本去重 ledger 优先)。
        """
        try:
            if kind == "procedural_rule":
                return await _items_rules_merged(
                    self.ledger, self.rule_store,
                    status=status, chat_name=chat_name, speaker=speaker,
                    visibility=visibility, query=query,
                    start_ts=start_ts, end_ts=end_ts,
                    exclude_visibilities=exclude_visibilities,
                    limit=limit, offset=offset)
            rows = await asyncio.to_thread(
                self.ledger.list_items, kind=kind, status=status,
                chat_name=chat_name, speaker=speaker,
                visibility=visibility, query=query,
                start_ts=start_ts, end_ts=end_ts,
                exclude_visibilities=exclude_visibilities,
                limit=limit, offset=offset,
            )
            return [_ledger_row_to_dto(r) for r in rows]
        except Exception as e:
            logger.warning(f"[Memory] items_v2 失败 (非致命): {e}")
            return []


    async def memory_item_detail_v2(self, memory_id: str) -> dict | None:
        """详情 + 来源链 + 派生项 + 生命周期事件 (规格 5.2 item detail)。

        [0.3.34.0 B0a] rule_ 前缀 = 规矩库虚拟项: 直读 rule_store,
        派生/事件为空 (老规矩无 ledger 记录, 如实返回)。
        """
        try:
            if memory_id.startswith("rule_"):
                rid = memory_id[len("rule_"):]
                for r in await self.rule_store.get_all_rules():
                    if isinstance(r, dict) and str(r.get("id", "")) == rid:
                        # [0.3.34.0 B1c] full_text 未截断全文供编辑回显
                        return {"item": _rule_to_dto(r),
                                "full_text": str(r.get("text", "") or ""),
                                "derivations": [], "events": []}
                return None
            def _ledger_detail():
                row = self.ledger.get_item(memory_id)
                if row is None:
                    return None
                return (
                    row,
                    self.ledger.derivations_of(memory_id),
                    self.ledger.events_of(memory_id),
                )

            loaded = await asyncio.to_thread(_ledger_detail)
            if loaded is None:
                return None
            row, derivations, events = loaded
            if row is None:
                return None
            return {
                "item": _ledger_row_to_dto(row),
                "derivations": derivations,
                "events": events,
            }
        except Exception as e:
            logger.warning(f"[Memory] item_detail_v2 失败 (非致命): {e}")
            return None

    async def memory_entities_v2(self, keyword: str = "", limit: int = 200) -> list:
        """结构化实体清单 (规格 5.2 entities): 实体 -> 事实 -> 当前值/核心标记。
        只读, 不触发 hit/不写。"""
        try:
            entities = await self.entity_store._load()
            rows = []
            for name, attrs in entities.items():
                if not isinstance(attrs, dict):
                    continue
                if keyword and keyword not in name:
                    continue
                facts = []
                for k, v in attrs.items():
                    if isinstance(v, dict):
                        facts.append({
                            "key": k,
                            "value": v.get("value", str(v)),
                            "is_core_fact": bool(v.get("is_core_fact", False)),
                        })
                    else:
                        facts.append({"key": k, "value": str(v),
                                      "is_core_fact": False})
                rows.append({"name": name, "facts": facts})
                if len(rows) >= max(1, min(limit, 500)):
                    break
            return rows
        except Exception as e:
            logger.warning(f"[Memory] entities_v2 失败 (非致命): {e}")
            return []

    async def recall_inspect_v2(self, query: str, chat_name: str = "",
                                asker_is_master: bool = True,
                                when_filter: str = "", include_graph: bool = False,
                                k: int = 10) -> dict:
        """召回检查器 (规格 5.2 recall/inspect): 固定 commit_hits=false 零写。

        返回: 规范化时间范围 / 候选来源 / ACL reason / 分项分数 / 淘汰原因 /
        最终注入顺序 / 总耗时。私密候选无权限只返回数量不返回内容。
        """
        try:
            result = await self.recall_plan.plan(
                query, chat_name=chat_name, asker_is_master=asker_is_master,
                when_filter=when_filter, k=max(1, min(k, 50)),
                include_graph=include_graph, commit_hits=False,
            )
            candidates = []
            for c in result.candidates:
                candidates.append({
                    "memory_id": c.memory_id,
                    "source": c.source,
                    "content": c.content,
                    "score": round(c.score, 3),
                    # [0.3.30.3 G17] 分项分真值 (Smallville 加权贡献 +
                    # semantic=cosine_sim), 不再恒 0。
                    "semantic": round(c.semantic, 3),
                    "recency": round(c.recency, 3),
                    "importance": round(c.importance, 3),
                    "hit_freq": round(c.hit_freq, 3),
                    "social": round(c.social, 3),
                    "same_chat_boost": round(c.same_chat_boost, 3),
                    "rank": c.rank,
                    "reason_code": c.reason_code,
                    "meta": {kk: vv for kk, vv in (c.meta or {}).items()
                             if kk in ("is_master", "chat_name", "entity",
                                       "prefix", "key")},
                })
            return {
                "query": query, "chat_name": chat_name,
                "asker_is_master": asker_is_master,
                "filtered_private_count": result.filtered_private_count,
                "acl_filtered": result.acl_filtered,
                "time_filtered": result.time_filtered,
                "type_filtered": result.type_filtered,
                "deduped": result.deduped,
                "budget_dropped": result.budget_dropped,
                "graph_expanded": result.graph_expanded,
                "reason_codes": result.reason_codes,
                "injected_ids": [c.memory_id for c in result.injected],
                "candidates": candidates,
                "duration_ms": result.duration_ms,
                "explain": result.explain(),
                "commit_hits": False,
            }
        except Exception as e:
            logger.warning(f"[Memory] recall_inspect_v2 失败(非致命): {e}")
            return {"error": str(e)}

    async def memory_entity_history_v2(self, entity_id: str) -> dict | None:
        """实体属性当前值 + 双时态历史 (规格 5.2 entity history)。
        entity_id 形如 "实体名.属性key"; 无 "." 时返回实体全属性概览。"""
        try:
            name, _, key = entity_id.partition(".")
            if not name:
                return None
            entities = await self.entity_store._load()
            attrs = entities.get(name)
            if not isinstance(attrs, dict):
                return None
            if not key:
                return {
                    "entity": name,
                    "facts": [
                        {"key": k, "value": (v.get("value", str(v))
                                             if isinstance(v, dict) else str(v))}
                        for k, v in attrs.items()
                    ],
                    "history": [],
                }
            if key not in attrs:
                return None
            v = attrs[key]
            history = await self.entity_store.entity_history(name, key)
            return {
                "entity": name,
                "fact": {
                    "key": key,
                    "value": v.get("value", str(v)) if isinstance(v, dict) else str(v),
                    "is_core_fact": bool(v.get("is_core_fact", False))
                    if isinstance(v, dict) else False,
                },
                "history": history or [],
            }
        except Exception as e:
            logger.warning(f"[Memory] entity_history_v2 失败 (非致命): {e}")
            return None

    @property
    def manifest(self):
        return {
            "name": "system_memory_god",
            "description": "潜意识管理系统。维护长期知识图谱、动态规矩提取、日程提醒。",
            "hidden": True,          # 不对 LLM 暴露为工具（Brain 通过 MemoryProvider 接口直接调用）
            "memory_provider": True,  # 声明自己是记忆提供者（Brain 通过此标记发现记忆后端）
        }

    async def on_connect(self):
        try:
            self.vector_store.connect()
            replay = await asyncio.to_thread(self.ledger.replay_backlog)
            if replay.get("read_error"):
                logger.warning("[Memory] Ledger outbox 暂时无法读取，已保留原文件")
            if replay["succeeded"]:
                logger.info(
                    f"[Memory] Ledger outbox 已重放 {replay['succeeded']} 条"
                )
            if replay["remaining"]:
                logger.warning(
                    f"[Memory] Ledger outbox 仍有 {replay['remaining']} 条待处理"
                )
            recovered = await self.curation.recover_interrupted()
            if recovered["recovered"]:
                logger.info(
                    f"[Memory] 已恢复 {recovered['recovered']} 条中断治理操作"
                )
            if recovered["pending_review"]:
                logger.warning(
                    f"[Memory] {recovered['pending_review']} 条治理操作需人工核验"
                )
            if recovered["runs_reconciled"]:
                logger.info(
                    f"[Memory] 已收敛 {recovered['runs_reconciled']} 个治理任务状态"
                )
            # [0.2.23.3 性能刀3] bge-m3 模型加载+probe 移后台预热: connect 秒回,
            # 13s 模型段不再卡 boot 关键路径 (预热期首次向量调用阻塞至就绪)。
            # [0.2.23.7 review M1] 任务入强引用集 (复用视觉编码器同款), 防 GC +
            # 异常有 app 级归属, 与内核 _BACKGROUND_TASKS 纪律对齐。
            warmup_task = asyncio.create_task(asyncio.to_thread(self.vector_store.warmup_embeddings))
            self._background_tasks.add(warmup_task)
            warmup_task.add_done_callback(self._background_tasks.discard)
            # [0.3.17.0 P4-b D6] 灵魂文档自检播种 (幂等; 失败非致命不阻 boot)
            await self._maybe_seed_identity()
            self.reflection_daemon.start()
            self.gc_daemon.start()

            # 挂载 bus 到 scheduler，使其能触发提醒事件
            if hasattr(self, 'bus'):
                self.scheduler_daemon.bus = self.bus
            self.scheduler_daemon.start()

            if hasattr(self, 'bus') and hasattr(self.bus, 'subscribe'):
                self.bus.subscribe("on_user_speak", self._background_user_listener)
                self.bus.subscribe("on_agent_speak", self._background_agent_listener)
                self.bus.subscribe("on_tool_finish", self._background_tool_listener)
                logger.info("[Memory] 潜意识神经网就绪。知识图谱挂载引擎已启动。")
            return True
        except Exception as e:
            logger.error(f"[Memory] 启动失败: {e}")
            return False

    async def on_disconnect(self):
        # [0.2.10.0] 临终反思: 关机前把未反思对话沉淀掉 (12s 封顶, 不超 shutdown 的 15s
        # 单 app 超时); 超时/失败也有持久文件兜底, 开机续反。老板短会话用法下
        # 这是记忆不蒸发的关键路径。
        try:
            await self.reflection_daemon.flush_final(timeout=12)
        except Exception as e:
            logger.warning(f"[Memory] 临终反思异常 (非致命, 已落盘): {e}")
        self.reflection_daemon.stop()
        self.gc_daemon.stop()
        self.scheduler_daemon.stop()
        return True

    async def _await_embeddings_ready(self) -> None:
        """[0.2.23.7 review H1] 嵌入未就绪时在线程池等, 严禁冻事件循环。

        刀3 的 LazyEF 栅栏 (Event.wait) 若直接在事件循环线程撞 (本 app 的
        add/query 是同步跑在 loop 上的, 不像 knowledge_base 全 to_thread),
        预热期用户说话 -> spotlight_search -> 整循环冻十几秒 (收信/心跳全停)。
        故所有向量入口先经 to_thread 等就绪, 阻塞挪进池线程。已就绪时快速直通。
        """
        # 鸭子容错: 测试 FakeVectorStore 无就绪接口时直通 (生产 VectorStore 恒有)
        ready = getattr(self.vector_store, "embeddings_ready", None)
        if callable(ready) and not ready():
            logger.info("[Memory] 嵌入预热中, 本次向量操作在线程池等待就绪...")
            wait = getattr(self.vector_store, "wait_embeddings_ready", None)
            if callable(wait):
                await asyncio.to_thread(wait)

    # ====================================================================
    # 后台潜意识监听器：实体锚定提取法 (Entity-Anchored Extraction)
    # ====================================================================
    # [成本优化] 实体提取预过滤闸门: 在发起 LLM 调用前先做本地启发式判定,
    # 命中以下任一条件直接跳过 LLM (视为 NONE), 省掉一次 API 调用:
    #   1. 过短 (去除空白/标点后不足 4 个有效字符) -> 多半是语气词
    #   2. 纯临时指令/停用词 (停下/算了/别/继续/好的/嗯 等) -> 属于短时记忆
    # 与 LLM prompt 内的"防过拟合机制"语义对齐, 但把判定前移到本地零成本完成。
    _EXTRACT_MIN_EFF_LEN = 4
    # P3: 实体提取 LLM 并发上限, 防高活跃群聊 fire-and-forget 并发打爆 API (请求风暴)。
    _EXTRACT_CONCURRENCY = 3
    _EXTRACT_STOP_WORDS = (
        "停下", "停一下", "停", "算了", "取消", "不用", "不要", "别", "继续",
        "好的", "好吧", "嗯嗯", "嗯", "哦", "是", "对的", "对", "OK", "ok",
        "Okay", "okay", "重来", "换一个", "换一个吧", "下一个", "再来", "再来一个",
        "知道了", "明白", "收到", "知道了啊", "无", "没有", "没了", "滚", "出来",
        "你好", "在吗", "在不在", "嗨", "哈喽",
    )

    @classmethod
    def _should_skip_entity_extraction(cls, text: str) -> bool:
        """实体提取预过滤闸门。返回 True 表示判定为无长期事实, 跳过 LLM。"""
        if not text or not isinstance(text, str):
            return True
        # 去除空白与标点, 统计有效字符
        cleaned = re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)
        if len(cleaned) < cls._EXTRACT_MIN_EFF_LEN:
            return True
        # 纯停用词: 去空白后整条命中停用词集合, 或以停用词开头且无后续实质内容
        stripped = text.strip()
        if stripped in cls._EXTRACT_STOP_WORDS:
            return True
        # 以停用词 + 语气助词结尾的短句 (如 "停下来", "别了") 也归为临时指令
        for sw in cls._EXTRACT_STOP_WORDS:
            if stripped.startswith(sw) and len(stripped) - len(sw) <= 2:
                return True
        return False

    @staticmethod
    def _gate_non_master_fact(is_core: bool, existing_is_core: bool) -> str:
        """[实体可信度闸门] 判定群聊第三人发言能否写入实体事实。

        返回:
          - "block": 该发言是核心类 (设定/身份等), 或会 supersede 一条已存在的
            is_core_fact=True 核心事实 -> 拦截, 不写核心设定 (流水账已入向量库 obs,
            不丢语义, 只是不进核心人物设定)。
          - "downgrade": 非核心事实, 允许写入但强制降级为非核心 (第三人陈述不拔高可信度)。

        背景: 2026-06-22 群里 Ab 开玩笑"我们都是小僵尸", 旧逻辑把它演进成
        【胖脸鱼.设定】并 supersede 掉真设定。本闸门堵此路径: 群聊第三人发言
        不得覆盖老板本人的核心人物设定。
        """
        if is_core or existing_is_core:
            return "block"
        return "downgrade"

    @staticmethod
    def _require_master(kwargs) -> bool:
        """[图谱权限闸门] graph 编辑类命令只认 is_master (UI/老板显式传)。

        守 think-batch-flattens-source-to-multi 教训: 不查 source=='console',
        只认 is_master。system_memory hidden 不暴露 LLM, 调用方为 UI/brain 内部,
        负责传 is_master=True; LLM/阿露侧无法直接调 (hidden) 故无越权风险。
        """
        return bool(kwargs.get("is_master"))

    def _ledger_track(self, kind: str, store: str, store_ref: str, content: str,
                      actor: str, speaker: str = "", chat_name: str = "",
                      visibility: str = "", is_master_source: bool = False,
                      importance: float | None = None,
                      entity_revision: float | None = None,
                      valid_from: float | None = None,
                      has_history: bool = False) -> str:
        """[0.3.30.4 G16] 新写入统一记账入口 (规格 4.1: 新数据必须写 ledger)。

        fail-safe: ledger 缺失/写失败返回空串, 绝不阻断消息主链 (失败进
        ledger backlog 待 reconciliation)。
        """
        ledger = getattr(self, "ledger", None)
        if ledger is None:
            return ""
        try:
            from system_memory_god.models import MemoryItem
            from system_memory_god.storage.memory_ledger import new_memory_id
            _mid = new_memory_id()
            item = MemoryItem(
                memory_id=_mid, kind=kind, store=store, store_ref=store_ref,
                content_preview=str(content)[:200], actor=actor,
                speaker=speaker, chat_name=chat_name, visibility=visibility,
                is_master_source=is_master_source, created_at=time.time(),
                importance=importance, entity_revision=entity_revision,
                valid_from=valid_from, has_history=has_history,
            )
            if store == "entity":
                result, canonical_id = ledger.upsert_entity_item(
                    item, queue_on_error=True,
                )
                if result == "error":
                    return ""
                return canonical_id
            elif not ledger.record_item(item):
                return ""
            canonical = ledger.find_by_store_ref(store, store_ref)
            return str(canonical.get("memory_id") or "") if canonical else ""
        except Exception as _e:
            logger.debug(f"[Memory] ledger 记账失败(非致命): {_e}")
            return ""

    def _resolve_vector_target(self, target: str) -> tuple[str, dict | None]:
        """兼容 Ledger ``mem_`` ID 与 Chroma ``store_ref`` 双入口。"""
        value = str(target or "").strip()
        ledger = getattr(self, "ledger", None)
        if ledger is None:
            return value, None
        if value.startswith("mem_"):
            row = ledger.get_item(value)
            if not row or row.get("store") != "vector":
                return "", row
            return str(row.get("store_ref") or ""), row
        return value, ledger.find_by_store_ref("vector", value)

    async def _background_user_listener(self, msg: Message):
        if getattr(getattr(self, "bus", None), "daily_owns", lambda _: False)(msg):
            return
        text = msg.content
        # [0.3.36.0 B2a] 捕获白名单: 系统注入内容绝不回收 (先于一切落盘)。
        if is_blocked_capture(text):
            logger.debug(f"[Memory] 捕获白名单拦截(系统注入标记): {str(text)[:40]}")
            return
        # [上下文去污] 用真实 sender 归因 (修 push_log 硬编码"胖脸鱼说"把非胖脸鱼的话也记成胖脸鱼说)。
        # 拆信封后每个 msg 已是单说话人内容, metadata.sender 即该说话人。
        _meta = msg.metadata or {}
        sender = _meta.get("sender", "") or "未知"
        sender_id = _meta.get("sender_id")  # 0.2.5.2 微信号锚点透传给图谱认人
        _journal_ref = await asyncio.to_thread(
            self.file_io.append_journal, f"{sender}: {text}",
        )
        if _journal_ref:
            await asyncio.to_thread(
                self._ledger_track,
                kind="observation", store="journal", store_ref=_journal_ref,
                content=f"{sender}: {text}", actor="user", speaker=sender,
                chat_name=str(_meta.get("chat_name", "") or ""),
                visibility=derive_visibility(_meta),
                is_master_source=bool(_meta.get("is_master", False)),
            )
        # [0.3.30.0 G16a] 预生成 obs doc_id: 向量存储与反思 buffer 同 ID,
        # 反思 episode 沉淀时派生链写真来源 (修 legacy_obs_unknown 硬编码断链)。
        import uuid as _uuid
        _obs_doc_id = f"obs_{_uuid.uuid4().hex}"
        img_paths = re.findall(r'\[LOCAL_IMAGE:(.+?)\]', text) + re.findall(r'([a-zA-Z]:\\[^\s*<>|"]+\.(?:jpg|jpeg|png|gif|webp))', text, re.IGNORECASE)
        for path in set(img_paths):
            task = asyncio.create_task(self._async_visual_memory_encoder(path.strip(), "用户发来的图片"))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        # 流水账无论如何都写入向量记忆 (召回不受闸门影响)
        # v0.6.0 双轴社交: 存 obs 前查说话人双轴打快照 + touch_interaction 演化 attention
        social = await self.entity_store.get_social(sender)
        try:
            await self.entity_store.touch_interaction(sender)
        except Exception as e:
            logger.warning(f"[Memory] touch_interaction 失败(非致命): {e}")
        await self._await_embeddings_ready()  # [H1] 栅栏等待挪出事件循环
        # [0.3.16.0 P4] 隐私桶派生 (写入侧打桶, 召回侧硬 ACL 消费), 纯函数见
        # derive_visibility (单测钉死契约)。
        _is_master_speaker = bool((msg.metadata or {}).get("is_master", False))
        _obs_chat = str((msg.metadata or {}).get("chat_name", "") or "")
        _visibility = derive_visibility(msg.metadata or {})
        try:
            _vector_result = await asyncio.to_thread(
                self.vector_store.add_memory,
                f"用户说：{text}", prefix="obs", speaker=sender,
                favorability=social.get("favorability", 50),
                attention=social.get("attention", 50),
                is_master=_is_master_speaker, chat_name=_obs_chat,
                visibility=_visibility, doc_id=_obs_doc_id,
            )
            _vector_written = _vector_result is not False
        except Exception as e:
            logger.warning(f"[Memory] 用户向量流水账写入失败，继续反思链: {e}")
            _vector_written = False
        # is_master 透传 reflection: 决定图谱三元组 source 分级。语义去重
        # 未产生该 doc_id 时不伪造来源引用，但对话仍进入反思缓冲。
        await asyncio.to_thread(
            self.reflection_daemon.push_log,
            f"{sender}说: {text}",
            is_master=bool(_meta.get("is_master", False)),
            sender_id=sender_id,
            sender_name=sender if sender != "未知" else None,
            doc_id=_obs_doc_id if _vector_written else None,
        )
        # [0.3.30.4 G16] 最高频写入路径进账本: 来源/隐私桶/生命周期可治理。
        if _vector_written:
            await asyncio.to_thread(
                self._ledger_track,
                kind="observation", store="vector", store_ref=_obs_doc_id,
                content=f"用户说：{text}", actor="user", speaker=sender,
                chat_name=_obs_chat, visibility=_visibility,
                is_master_source=_is_master_speaker,
            )

        # [成本优化] 预过滤闸门: 临时指令/过短消息直接跳过 entity 提取 LLM 调用
        if self._should_skip_entity_extraction(text):
            logger.debug(f"[Memory] 预过滤拦截实体提取 (临时指令/过短): {text[:30]}")
            return

        # 史诗级进化：放弃列表提取，改用【图谱节点生长法】
        prompt = """你是一个人类潜意识的【知识图谱节点提取器】。
任务：监听主人的话，提取【客观事实】或【长期偏好/规矩】，并将其挂载到具体的“实体（名词）”上。

【防过拟合机制】：临时指令（如”别找夜鹭了”、”停下”、”算了”）属于短时记忆，必须忽略（输出 NONE）。

如果包含长期事实或规矩，请严格按以下多行格式输出：
ENTITY: [核心实体名词，如"西瓜"、"夜鹭"、"阿露"]
KEY: [属性分类，如"偏好"、"禁忌"、"设定"]
VALUE: [具体内容，如"胖脸鱼极度讨厌该事物，禁止发送相关图" 或 "胖脸鱼非常喜欢吃"]

如果不包含长期事实或规矩，只输出 NONE。"""

        try:
            # P3: 信号量限流实体提取 LLM 并发, 防高活跃群聊 fire-and-forget 并发打爆 API。
            # 解析与写入在锁外 (本地快速操作), 仅串行化昂贵的 LLM HTTP 请求。
            async with self._extract_sem:
                response = await self.llm_client.async_client.chat.completions.create(
                    model=self.llm_client.model_name,
                    messages=[{"role": "system", "content": prompt}, {"role": "user", "content": text}],
                    temperature=0.1,
                    **request_options(self.llm_client.model_name),
                )
            res = response.choices[0].message.content.strip()

            if res != "NONE" and "ENTITY:" in res:
                entity, key, value = "", "", ""
                for line in res.split('\n'):
                    if line.startswith("ENTITY:"): entity = line.replace("ENTITY:", "").strip()
                    elif line.startswith("KEY:"): key = line.replace("KEY:", "").strip()
                    elif line.startswith("VALUE:"): value = line.replace("VALUE:", "").strip()

                if entity and key and value:
                    # 判断是否属于核心事实（生日、喜好、人际关系等需要长期记住的信息）
                    # [阶段3 A 0.1.13.0] 扩展硬属性字段: 加"称呼/性别/真名/昵称/名字/本名"。
                    # 旧版缺"称呼", 致 18:05:04 Ab 发"豆橛子森赛"称呼时 is_core=False, 闸门走
                    # downgrade 写入(实证 aoao.log), 第三人绕过闸门改硬属性。补齐后第三人
                    # is_master=False 对这些字段一律 block(只记 obs 流水账, 不进核心人物设定)。
                    core_keywords = ["生日", "喜好", "禁忌", "设定", "关系", "身份",
                                     "日期", "时间", "习惯", "称呼", "性别", "真名",
                                     "昵称", "名字", "本名"]
                    is_core = any(kw in key or kw in value for kw in core_keywords)
                    weight = 10 if is_core else 1

                    # [实体可信度闸门] 群聊第三人发言不得覆盖核心人物设定。
                    # 背景: 2026-06-22 群里 Ab 开玩笑"我们都是小僵尸", 旧逻辑把它演进成
                    # 【胖脸鱼.设定】并 supersede 掉真设定, 导致阿露顶嘴"这是底层设定"。
                    # 闸门: 非老板本人发言 (is_master=False), 若该事实是核心类 (设定/身份等),
                    # 或会 supersede 一条已存在的 is_core_fact=True 事实 -> 拦截不写核心设定
                    # (流水账已入向量库 obs, 不丢语义, 只是不进核心人物设定)。
                    md = dict(msg.metadata) if msg.metadata else {}
                    is_master = bool(md.get("is_master", False))  # R7: 默认False (保守, 未知发言者按非老板处理)。console(main_god.py:252)与wechat均显式设is_master, brain.py:925转发亦默认False, 故True默认是死代码; 翻False堵住"直接emit漏设is_master被误判老板"的footgun。
                    if not is_master:
                        existing = await self.entity_store.get_entity(entity)
                        existing_fact = (existing or {}).get(key, {})
                        will_override_core = bool(existing_fact.get("is_core_fact"))
                        if is_core or will_override_core:
                            speaker = md.get("sender", "") or md.get("chat_name", "")
                            logger.info(
                                f"[Memory] 实体可信度闸门拦截: 群聊第三人({speaker})发言不得覆盖"
                                f"核心设定, 跳过 【{entity}.{key}】={value[:40]}"
                            )
                            return
                        # 非核心事实: 允许写入, 但强制降级为非核心 (第三人陈述不拔高可信度)
                        is_core = False
                        weight = 1

                    updated = await self.entity_store.update_entity(
                        entity, key, value,
                        is_core_fact=is_core, weight=weight,
                        return_snapshot=True,
                    )
                    if updated is False:
                        return
                    entity_revision = (
                        float(updated.get("tx_time"))
                        if isinstance(updated, dict)
                        and updated.get("tx_time") is not None
                        else None
                    )
                    canonical_value = (
                        str(updated.get("value", value))
                        if isinstance(updated, dict) else value
                    )
                    tag = "[核心事实]" if is_core else "[实体]"
                    logger.info(f"[{tag} 海马体] 知识图谱节点生长: 【{entity}】 -> {key}: {value}")
                    # [0.3.30.4 G16] 在线实体抽取 fact 进账本 (store_ref=实体键)。
                    await asyncio.to_thread(
                        self._ledger_track,
                        kind="semantic_fact", store="entity",
                        store_ref=f"{entity}.{key}", content=canonical_value,
                        actor="user" if is_master else "unknown",
                        speaker=str(md.get("sender", "") or ""),
                        chat_name=str(md.get("chat_name", "") or ""),
                        is_master_source=is_master,
                        entity_revision=entity_revision,
                        valid_from=(
                            float(updated.get("valid_from"))
                            if isinstance(updated, dict)
                            and updated.get("valid_from") is not None
                            else None
                        ),
                        has_history=bool(
                            isinstance(updated, dict) and updated.get("history")
                        ),
                    )

        except Exception as exc:
            logger.error(
                "[Memory] 后台用户监听异常 (type=%s)", type(exc).__name__)

    async def _background_agent_listener(self, msg: Message):
        if getattr(getattr(self, "bus", None), "daily_owns", lambda _: False)(msg):
            return
        if isinstance((getattr(msg, "metadata", None) or {}).get("research"), dict):
            # 研究资料只进入受限证据存储，不能作为助手自述写入反思或长期记忆。
            return
        text = msg.content
        # [0.3.36.0 B2a] 捕获白名单: 阿露复述注入块同样绝不回收。
        if is_blocked_capture(text):
            logger.debug(f"[Memory] 捕获白名单拦截(系统注入标记): {str(text)[:40]}")
            return
        _agent_journal_ref = await asyncio.to_thread(
            self.file_io.append_journal, f"助手: {text}",
        )
        if _agent_journal_ref:
            await asyncio.to_thread(
                self._ledger_track,
                kind="observation", store="journal",
                store_ref=_agent_journal_ref, content=f"助手: {text}",
                actor="assistant", speaker="阿露",
                chat_name=str((getattr(msg, "metadata", None) or {}).get(
                    "chat_name", "") or ""),
                is_master_source=True,
            )
        # 阿露自述 is_master=True (可信源) -> 图谱三元组 source=master 激活
        await asyncio.to_thread(
            self.reflection_daemon.push_log,
            f"助手说: {text}", is_master=True,
        )
        # [Problem 2 0.1.10.2] 叙述性元话语 (已/已经.../已发送等声称动作的话) 不入向量库 obs,
        # 防幻觉种子: 这类"已经发了"声称若存 obs, 未来 warm_recall 浮顶会误导阿露复述
        # "已发"而不真调工具。真实动作由 _background_tool_listener 记 tool_result。
        if _is_narration_statement(text):
            return
        # [阶段3-B] 阿露自述 is_master=True (可信源, 不降权)
        # [0.2.10.4 P1] 标来源对话: 阿露回复也标群, 供召回同群提权。
        _agent_meta = getattr(msg, "metadata", None) or {}
        await self._await_embeddings_ready()  # [H1] 栅栏等待挪出事件循环
        # [0.3.30.4 G16] 预生成 doc_id: 向量存储与账本同 ID。
        import uuid as _uuid
        _agent_doc_id = f"obs_{_uuid.uuid4().hex}"
        _agent_chat = str(_agent_meta.get("chat_name", "") or "")
        _agent_result = await asyncio.to_thread(
            self.vector_store.add_memory,
            f"助手回复: {text}", prefix="obs", is_master=True,
            chat_name=_agent_chat, doc_id=_agent_doc_id,
        )
        _agent_written = _agent_result is not False
        if _agent_written:
            await asyncio.to_thread(
                self._ledger_track,
                kind="observation", store="vector", store_ref=_agent_doc_id,
                content=f"助手回复: {text}", actor="assistant", speaker="阿露",
                chat_name=_agent_chat, is_master_source=True,
            )

    async def _background_tool_listener(self, msg: Message):
        if getattr(getattr(self, "bus", None), "daily_owns", lambda _: False)(msg):
            return
        res = msg.content
        _tool_journal_ref = await asyncio.to_thread(
            self.file_io.append_journal, f"[系统动作结果]: {res[:100]}...",
        )
        if _tool_journal_ref:
            await asyncio.to_thread(
                self._ledger_track,
                kind="observation", store="journal",
                store_ref=_tool_journal_ref,
                content=f"[系统动作结果]: {res[:100]}...",
                actor="tool", speaker="system",
            )
        img_paths = re.findall(r'\[LOCAL_IMAGE:(.+?)\]', res)
        for path in set(img_paths):
            task = asyncio.create_task(self._async_visual_memory_encoder(path.strip(), "生成的图片"))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _async_visual_memory_encoder(self, img_path: str, source_desc: str):
        if not os.path.exists(img_path): return
        try:
            # [MIME 修复] 用真实文件头判定 MIME, 不再写死 jpeg。
            # read_image_as_data_url 在格式非法时返回 None, 此时跳过视觉编码, 不污染记忆。
            data_url = await asyncio.to_thread(read_image_as_data_url, img_path)
            if not data_url:
                logger.warning(f"[Memory] 视觉记忆编码跳过: 图片格式非法或不可读 -> {img_path}")
                return
            prompt = f"描述这张{source_desc}。"
            messages = [{"role": "user", "content": [{"type": "text", "text": prompt}, {"type": "image_url", "image_url": {"url": data_url}}]}]
            if is_deepseek(self.llm_client.model_name):
                validate_deepseek_images(messages)
                check_request_budget(messages, [])
            response = await self.llm_client.async_client.chat.completions.create(
                model=self.llm_client.model_name, messages=messages,
                **request_options(self.llm_client.model_name),
            )
            desc = response.choices[0].message.content.strip()

            # 视觉记忆也可以挂载到“视觉影像”实体上，但保留通用记录也OK
            # [阶段3-B] 阿露编码的视觉记忆 is_master=True (可信源)
            await self._await_embeddings_ready()  # [H1] 视觉编码 task 也跑在 loop 线程
            # [0.3.30.4 G16] 视觉 fact 进账本 (kind=visual, 规格 3.3 六类之一)。
            import uuid as _uuid
            _vis_doc_id = f"fact_{_uuid.uuid4().hex}"
            _vis_content = f"【视觉记忆】{desc} 路径：{img_path}"
            _visual_result = await asyncio.to_thread(
                self.vector_store.add_memory, _vis_content,
                prefix="fact", is_master=True, doc_id=_vis_doc_id,
            )
            _visual_written = _visual_result is not False
            if _visual_written:
                await asyncio.to_thread(
                    self._ledger_track,
                    kind="visual_fact", store="vector", store_ref=_vis_doc_id,
                    content=_vis_content, actor="assistant", speaker="视觉编码",
                    is_master_source=True,
                )
        except Exception as exc:
            logger.error(
                "[Memory] 视觉记忆编码异常 (type=%s)", type(exc).__name__)

    # ====================================================================
    # 供大脑主动调用的显式指令
    # ====================================================================
    def _matches_entity(self, entity_name: str, text: str) -> bool:
        """
        [0.3.23.0 薄委托] 同源纯函数移驻 kernel/memory_view.matches_entity
        (只读记忆窗口与活体共用一份匹配语义, 防双侧漂移; kernel 层不 import
        apps, 故方法内延迟导入)。语义不变: 单字拒 / 拉丁词边界 / CJK 子串。
        """
        from kernel.memory_view import matches_entity
        return matches_entity(entity_name, text)

    # [0.2.23.7 review H1] 触发嵌入的向量命令 (预热期须先异步等就绪);
    # 图谱/社交/提醒类命令不碰嵌入, 不加闸以免预热期连累仪表盘查询。
    # [0.3.26.0 Phase 0 任务13] spotlight_search 只查实体/规则不碰向量,
    # archival_search 查 journal 文件也不碰嵌入 -> 移出等待集, 预热期 UI
    # 探照灯/档案检索不再被嵌入栅栏拖累 (规格: spotlight 和 archival 不等待 embedding)。
    _VECTOR_COMMANDS = frozenset({
        "smart_recall", "recall", "remember",
    })

    # [0.3.30.0 G20] smart_recall 走 RecallPlan 时的注入上限 (对齐旧
    # query_memory_with_meta n_results 缺省 5, 保 warm 上下文体量不变)。
    _SMART_RECALL_K = 5

    async def execute_command(self, command: str, **kwargs):
        if command in self._VECTOR_COMMANDS:
            await self._await_embeddings_ready()
        # 探照灯检索机制 (Spotlight RAG) — 核心事实优先召回
        if command == "spotlight_search":
            text = kwargs.get("text", "")
            # [0.3.26.0 Phase 0 M0-05] 浏览零写: spotlight 默认 commit_hits=False
            # (Web UI /api/memory/search 走此命令, 浏览不得改变记忆排序);
            # 真实 Agent 召回 (recall 命令内部) 显式 commit_hits=True 才提交命中。
            commit_hits = bool(kwargs.get("commit_hits", False))
            known_entities = await self.entity_store._load()
            core_fact_lines = []
            normal_lines = []
            matched_entity_keys = []

            for entity_name, attributes in known_entities.items():
                # [清洗] 词边界匹配, 单字实体不污染
                if self._matches_entity(entity_name, text):
                    for k, v in attributes.items():
                        # 兼容旧格式：纯字符串 / 新格式：结构化对象
                        if isinstance(v, dict):
                            value_text = v.get("value", str(v))
                            is_core = v.get("is_core_fact", False)
                        else:
                            value_text = str(v)
                            is_core = False

                        line = f"- [注意] 关于【{entity_name}】的{k}：{value_text}"
                        if is_core:
                            core_fact_lines.append(line)
                        else:
                            normal_lines.append(line)
                        # [record_hit 接线] 实体被话题激活命中, 真实累加频次
                        if commit_hits:
                            matched_entity_keys.append((entity_name, k))

            # 核心事实绝对优先排在最前面
            matched_rules = core_fact_lines + normal_lines

            # 性格规矩注入（结构化 rules.json，按命中数取 Top-N，避免全量灌入挤占上下文）
            rules_text = await self.rule_store.get_active_rules_text(limit=8)
            if rules_text:
                # [record_hit 接线] 注入的每条规则累加 hit_count, 让 Top-N 排序反映真实激活频次
                if commit_hits:
                    for rule_line in rules_text.split("\n"):
                        raw = rule_line.lstrip("- ").strip()
                        if raw:
                            await self.rule_store.record_hit_by_text(raw)
                matched_rules.append(
                    "- 潜意识演化出的性格规矩 (按重要性取高频 Top-8)：\n"
                    f"{rules_text}"
                )

            # [Smallville 频次接线] 异步累加实体命中计数 (不阻塞召回)
            for entity_name, k in matched_entity_keys:
                try:
                    await self.entity_store.record_hit(entity_name, k)
                except Exception as e:
                    logger.debug(f"[Memory] 实体 record_hit 异常 (非致命): {e}")

            return "\n".join(matched_rules) if matched_rules else ""

        elif command == "smart_recall":
            keyword = kwargs.get("text", "")
            # [0.3.26.0 Phase 4] memory_scope: disabled 时零注入 (不查询不提交
            # hit), 其余 scope 由 RecallPlan._apply_scope 语义映射 (规格 6.9)。
            _scope = str(kwargs.get("memory_scope", "") or "auto").strip().lower()
            if _scope == "disabled":
                return ""
            # [0.2.10.4 P1] 接 chat_name: 传入 query_memory_with_meta 做同群提权,
            # 跨群 obs 在返回时标 [跨群参考:X群] 前缀, 治串台 (S9群回骨科群旧图)。
            current_chat = str(kwargs.get("chat_name", "") or "")
            # [0.3.16.0 P4] asker 上下文透传硬 ACL: brain._warm_recall 传组主体
            # (含老板 -> owner 召回 private 可见; 纯第三人群 -> guest, private 过滤)。
            # 默认 True 保老调用方 (直连/未透传) 行为。
            _asker_is_master = bool(kwargs.get("asker_is_master", True))
            # [0.3.26.0 Phase 0 M0-06] smart_recall 是否只查 obs 由显式
            # include_types 决定 (不依赖注释); 缺省 None = 不过滤 (保旧行为)。
            _include_types = kwargs.get("include_types")
            if isinstance(_include_types, str):
                _include_types = [t.strip() for t in _include_types.split(",") if t.strip()]
            # [0.3.26.0 Phase 0 M0-05] commit_hits: 真实 Agent 召回默认 True,
            # 浏览型请求显式 False 零写。
            _commit_hits = bool(kwargs.get("commit_hits", True))
            # [0.3.30.0 G20/G22] 迁入统一 Recall Plan (规格 4.2 Phase 2 任务1 收口):
            # scope 经 _apply_scope 真生效 (recents 时间窗/portrait 只事实/relations
            # 开图谱/rules 只规矩); 候选源集按 scope 门控, auto 只跑向量阶段守住
            # brain 0.3s 预召回预算; narration 过滤与来源/跨群标注经 render_text 同形。
            try:
                _sources = self.recall_plan.sources_for_scope(_scope)
                plan_result = await self.recall_plan.plan(
                    keyword, chat_name=current_chat,
                    asker_is_master=_asker_is_master,
                    k=self._SMART_RECALL_K,
                    include_types=_include_types,
                    include_graph="graph" in _sources,
                    commit_hits=_commit_hits,
                    memory_scope=_scope,
                    sources=_sources,
                    filter_narration=True,
                )
            except Exception as _plan_e:
                logger.warning(f"[Memory] smart_recall plan 失败, 回退旧路径: {_plan_e}")
                plan_result = None
            if plan_result is not None:
                return self.recall_plan.render_text(plan_result)
            # ---- 旧路径兜底 (仅 plan 异常时走, 与 0.3.29.x 完全一致, 一键可回退) ----
            pairs = self.vector_store.query_memory_with_meta(
                keyword, prefer_chat=current_chat,
                asker_is_master=_asker_is_master,
                commit_hits=_commit_hits,
            )
            if _include_types:
                pairs = [(doc, meta) for doc, meta in pairs
                         if str((meta or {}).get("prefix", "")) in _include_types]
            # [Problem 2 0.1.10.2] 过滤叙述性元话语 obs (旧"已经给曹叔叔打过招呼啦~"等
            # 声称动作的话), 防幻觉种子浮顶误导阿露复述"已发"而不真调工具。
            pairs = [
                (doc, meta) for doc, meta in pairs
                if not (doc.startswith("助手回复: ") and
                        _is_narration_statement(doc[len("助手回复: "):].strip()))
            ]
            # [零状态对齐] 无结果时返回空串, 严禁返回 "无往事。" 挤占主脑上下文
            if not pairs:
                return ""
            # [阶段3-B] 按来源标注: 第三人 obs (is_master=False) 加 [他人转述] 前缀,
            # 让 LLM 区分可信度 (豆橛子复发治本); 老 obs 无 is_master 默认 True 无前缀。
            # [0.2.10.4 P1] 跨群标注: obs 的 chat_name 与当前对话不同 -> 加 [跨群参考:X群],
            # 老数据无 chat_name (空串) 不标注 (中性, 同 is_master 默认值模式)。
            parts = []
            for doc, meta in pairs:
                is_master = bool((meta or {}).get("is_master", True))
                obs_chat = str((meta or {}).get("chat_name", "") or "")
                prefix = ""
                if not is_master:
                    prefix += "[他人转述]"
                if current_chat and obs_chat and obs_chat != current_chat:
                    prefix += f"[跨群参考:{obs_chat}]"
                parts.append(f"{prefix}{doc}" if prefix else doc)
            return f"检索线索：{' | '.join(parts)}"

        # =================== [MemGPT 内存自治工具] ===================
        # 主脑主动调用的第一类记忆管理原子操作 (通过 memory_recall/remember/forget 工具)
        elif command == "recall":
            # 主动翻阅: 统一 Recall Plan 流水线 (向量 + 实体 + 规则 + 图谱)
            query = kwargs.get("query", "") or kwargs.get("text", "")
            if not query:
                return "失败：需提供检索关键词"
            # [0.3.26.0 Phase 0 M0-01] 主动 recall 执行与 warm recall 相同的 ACL:
            # 透传 chat_name/asker_is_master (handler 层已从 brain 当前轮上下文注入,
            # 直连老调用方缺省 asker_is_master=True 保旧行为, 群聊召不回私聊)。
            current_chat = str(kwargs.get("chat_name", "") or "")
            asker_is_master = bool(kwargs.get("asker_is_master", True))
            # [0.3.26.0 Phase 0 M0-04] when_filter 后端生效: 解析失败返回明确错误。
            when_filter = str(kwargs.get("when_filter", "") or "")
            if when_filter and parse_when_filter(when_filter) is None:
                return f"失败：无法解析时间过滤条件「{when_filter}」"
            # [0.3.26.0 Phase 0 M0-05] commit_hits: 浏览型请求显式 False 零写。
            commit_hits = bool(kwargs.get("commit_hits", True))
            # [0.3.26.0 Phase 2] 统一 Recall Plan: 候选生成 + ACL + 类型/时间
            # 过滤 + 预算 + 结构化解释, 替代旧 向量+spotlight 二合一拼串。
            try:
                plan_result = await self.recall_plan.plan(
                    query, chat_name=current_chat,
                    asker_is_master=asker_is_master,
                    when_filter=when_filter,
                    k=int(kwargs.get("k", 10) or 10),
                    include_types=kwargs.get("include_types"),
                    include_graph=bool(kwargs.get("include_graph", False)),
                    commit_hits=commit_hits,
                )
            except Exception as _plan_e:
                logger.warning(f"[Memory] recall_plan 执行失败, 回退旧路径: {_plan_e}")
                plan_result = None
            if plan_result is not None and plan_result.injected:
                return self.recall_plan.render_text(plan_result)
            if plan_result is not None:
                return "未检索到相关记忆。"
            # 旧路径兜底 (plan 异常时): 向量 + spotlight
            pairs = self.vector_store.query_memory_with_meta(
                query, n_results=5, prefer_chat=current_chat,
                asker_is_master=asker_is_master, commit_hits=commit_hits,
            )
            spotlight = await self.execute_command(
                "spotlight_search", text=query, commit_hits=commit_hits,
            )
            parts = []
            if pairs:
                lines = []
                for doc, meta in pairs:
                    is_master = bool((meta or {}).get("is_master", True))
                    lines.append(f"- {'[他人转述]' if not is_master else ''}{doc}")
                parts.append("向量往事:\n" + "\n".join(lines))
            if spotlight:
                parts.append("实体/规矩:\n" + spotlight)
            return "\n\n".join(parts) if parts else "未检索到相关记忆。"

        elif command == "remember":
            # 主动刻入: 主脑把重要承诺/事实写进长期记忆
            content = kwargs.get("text", "") or kwargs.get("content", "")
            if not content:
                return "失败：需提供要记住的内容"
            importance = float(kwargs.get("importance_score",
                                         kwargs.get("importance", 0.8)))
            importance = max(0.0, min(1.0, importance))
            # [0.3.26.0 Phase 0 M0-02] 主动 remember 落上下文: chat_name/is_master/
            # is_dm_with_master 透传 (handler 从 brain 当前轮注入), 据此派生
            # visibility 桶 -- 老板私聊 fact -> private (群聊永不召回), 群聊 ->
            # group。缺省 is_master=True 保老调用方行为 (直连 console 视为主人)。
            _mem_meta = {
                "is_master": bool(kwargs.get("is_master", True)),
                "chat_name": str(kwargs.get("chat_name", "") or ""),
                "is_dm_with_master": bool(kwargs.get("is_dm_with_master", False)),
            }
            _mem_visibility = derive_visibility(_mem_meta)
            # [0.3.26.0 Phase 0 M0-03] 预生成 doc_id 并随返回透出: 调用方按
            # 该 ID 可精确 forget (向量删除闭环), 否则"记住"后无法定位抹除。
            import uuid as _uuid
            _mem_doc_id = f"fact_{_uuid.uuid4().hex}"
            ok = self.vector_store.add_memory(
                content, prefix="fact", importance=importance,
                is_master=_mem_meta["is_master"],
                chat_name=_mem_meta["chat_name"],
                visibility=_mem_visibility,
                doc_id=_mem_doc_id,
            )
            # [0.3.25.1] 消息池打标: 触发本记忆写入的入站消息标记 memory (pool 挂 bus)
            if ok:
                try:
                    _msg = kwargs.get("msg", None)
                    _pool = getattr(getattr(self, "bus", None), "pool", None)
                    if _pool is not None and _msg is not None and getattr(_msg, "id", ""):
                        _pool.mark(_msg.id, "memory")
                except Exception as e:
                    logger.debug(f"[Memory] 消息池打标失败(非致命): {e}")
            # [阶段4] 记忆变更审计: 记"改了啥记忆", 供 behavior_recall 查
            if ok:
                try:
                    from kernel import behavior_log
                    behavior_log.append_behavior("memory_change", content, op="remember")
                except Exception as e:
                    logger.warning(f"[Memory] behavior_log 写入失败(非致命): {e}")
            if ok:
                # [0.3.26.0 Phase 1] ledger 记来源 (规格 4.1: 新写入必须写账本,
                # 失败进 backlog 不阻断返回)。store_ref 关联向量 doc_id。
                try:
                    from system_memory_god.models import MemoryItem
                    from system_memory_god.storage.memory_ledger import new_memory_id
                    self.ledger.record_item(MemoryItem(
                        memory_id=new_memory_id(), kind="semantic_fact",
                        store="vector", store_ref=_mem_doc_id,
                        content_preview=content, actor="assistant",
                        speaker="", chat_name=_mem_meta["chat_name"],
                        visibility=_mem_visibility,
                        is_master_source=_mem_meta["is_master"],
                        created_at=time.time(), importance=importance,
                    ))
                except Exception as _ledger_e:
                    logger.warning(f"[Memory] ledger remember 记账失败(非致命): {_ledger_e}")
                return f"已刻入长期记忆 (id: {_mem_doc_id}, 重要度 {importance})。"
            return "记忆写入失败 (可能语义重复被去重)。"

        elif command == "forget":
            # 主动遗忘: 多级匹配, 让主脑自然语言指令 (如"忘掉胖脸鱼是小僵尸") 真能命中。
            # 匹配链: 精确向量 ID (M0-03 闭环) -> 规则文本 -> 实体节点(整删) ->
            # 单条事实(entity,key) -> value 模糊
            target = kwargs.get("target", "") or kwargs.get("entity_or_rule_id", "") or kwargs.get("text", "")
            if not target:
                return "失败：需提供要遗忘的实体名、规则文本或事实描述"
            # [0.3.26.0 Phase 0 M0-03] 精确 ID 遗忘优先: remember 返回的 doc_id /
            # memory_id 直达 Chroma 删除, 自然语言匹配链兜底。优先序: 精确 > 模糊
            # (规格任务 5: 明确自然语言模糊遗忘与精确 ID 遗忘的优先级)。
            _t = str(target).strip()
            if _t.startswith(("fact_", "obs_", "episode_", "mem_")):
                _vector_ref, _row = self._resolve_vector_target(_t)
                if not _vector_ref:
                    return f"未找到可操作的向量记忆 ID: {_t}"
                # [0.3.38.1 Batch 1] 软删除默认: status=forgotten 标记, 物理保留
                # 可恢复; self.soft_forget_enabled=False 回退物理删旧行为。
                _soft = getattr(self, "soft_forget_enabled", True)
                _ok = (self.vector_store.soft_forget(_vector_ref) if _soft
                       else self.vector_store.delete_by_id(_vector_ref))
                if _ok:
                    try:
                        from kernel import behavior_log
                        behavior_log.append_behavior(
                            "memory_change", _vector_ref, op="forget",
                        )
                    except Exception as _e:
                        logger.warning(f"[Memory] behavior_log 写入失败(非致命): {_e}")
                    # [0.3.30.4 G16b] ledger 生命周期: 反查 store_ref 置 forgotten
                    # 状态 + 记事件 (老数据无账本行如实跳过)。
                    try:
                        if _row:
                            self.ledger.record_event(
                                _row["memory_id"], "forgotten",
                                actor="master",
                                reason="主动遗忘(精确ID)" + ("[软删]" if _soft else "[物理]"))
                            self.ledger.mark_status(_row["memory_id"], "forgotten")
                    except Exception as _e:
                        logger.warning(f"[Memory] ledger forget 记账失败(非致命): {_e}")
                    return (f"已软遗忘向量记忆(可恢复): {_t}" if _soft
                            else f"已遗忘向量记忆: {_t}")
            # [阶段4] 记忆变更审计: 记"改了啥记忆", 供 behavior_recall 查
            try:
                from kernel import behavior_log
                behavior_log.append_behavior("memory_change", target, op="forget")
            except Exception as e:
                logger.warning(f"[Memory] behavior_log 写入失败(非致命): {e}")
            forgotten = []
            # 1) 数字ID视为规则序号; 2) 规则文本匹配
            rule_ok = await self.rule_store.forget_rule_by_text(str(target))
            if rule_ok:
                forgotten.append(f"规矩「{target}」")
            # 3) 实体节点整删 (target 恰为某 entity 名)
            entity_ok = await self.entity_store.forget_entity(target)
            if entity_ok:
                forgotten.append(f"实体节点「{target}」(含其全部属性)")
            # 4) 单条事实 (entity.key 形式, 如 "胖脸鱼.设定")
            if "." in target:
                ent_name, _, key_name = target.partition(".")
                fact_ok = await self.entity_store.forget_fact(ent_name.strip(), key_name.strip())
                if fact_ok:
                    forgotten.append(f"事实「{ent_name}.{key_name}」")
            # 5) value 模糊匹配 (最后兜底: target 是自然语言句子时靠语义片段命中)
            if not forgotten:
                hits = await self.entity_store.forget_by_value(target)
                for h in hits:
                    where = f"{h['entity']}.{h['key']}"
                    if h.get("fact_dropped"):
                        forgotten.append(f"事实「{where}」(整条删除, 删{h['removed']}个版本)")
                    else:
                        forgotten.append(f"事实「{where}」的命中版本(删{h['removed']}个, 已回退到上一版)")
            if forgotten:
                return "已遗忘: " + "、".join(forgotten)
            return f"未找到匹配的记忆「{target}」, 无需遗忘。"

        elif command == "restore_memory":
            # [0.3.38.1 Batch 1] 恢复软遗忘的向量记忆 (物理从未移除)。
            _t = str(kwargs.get("memory_id", "") or kwargs.get("target", "") or "").strip()
            if not _t:
                return "失败：需提供要恢复的记忆 ID"
            _vector_ref, _row = self._resolve_vector_target(_t)
            if not _vector_ref:
                return f"恢复失败 (不是可恢复的向量记忆): {_t}"
            if self.vector_store.restore_memory(_vector_ref):
                try:
                    if _row:
                        self.ledger.record_event(
                            _row["memory_id"], "restored",
                            actor="master", reason="软删除恢复")
                        self.ledger.mark_status(_row["memory_id"], "active")
                except Exception as _e:
                    logger.warning(f"[Memory] ledger restore 记账失败(非致命): {_e}")
                return f"已恢复记忆: {_t}"
            return f"恢复失败 (记忆不存在或已物理删除): {_t}"

        elif command == "purge_memory":
            # [0.3.38.1 Batch 1] 物理删除 (不可恢复): 软删后再确认要真抹除时用。
            _t = str(kwargs.get("memory_id", "") or kwargs.get("target", "") or "").strip()
            if not _t:
                return "失败：需提供要物理删除的记忆 ID"
            _vector_ref, _row = self._resolve_vector_target(_t)
            if not _vector_ref:
                return f"物理删除失败 (不是向量记忆): {_t}"
            if self.vector_store.delete_by_id(_vector_ref):
                try:
                    if _row:
                        self.ledger.record_event(
                            _row["memory_id"], "forgotten",
                            actor="master", reason="物理删除(purge)")
                        self.ledger.mark_status(_row["memory_id"], "forgotten")
                except Exception as _e:
                    logger.warning(f"[Memory] ledger purge 记账失败(非致命): {_e}")
                return f"已物理删除向量记忆: {_t}"
            return f"物理删除失败 (记忆不存在): {_t}"

        elif command == "adjust_impression":
            # v0.6.0 VIP 审批式双轴调整 (权限闸门在 brain._handle_adjust_impression 已判 source=console)
            person = kwargs.get("person", "")
            reason = kwargs.get("reason", "")
            fav_delta = float(kwargs.get("favorability_delta", 0) or 0)
            att_delta = float(kwargs.get("attention_delta", 0) or 0)
            if not person:
                return "失败: 缺少 person"
            if not reason:
                return "失败: 缺少 reason (调整看法需说明理由)"
            result = await self.entity_store.adjust_social(
                person, fav_delta, att_delta, reason, kwargs.get("source", "console")
            )
            if result.get("ok"):
                return (f"已调整对【{person}】的看法: 好感度 {result['favorability']:.0f}, "
                        f"注意度 {result['attention']:.0f} (理由: {reason})。审批记录已留痕。")
            return f"调整失败: {result.get('error', '未知错误')}"

        elif command == "boost_interest":
            # [0.2.9.0] 老板"多关注X": 涨兴趣值 + 设衰减阻尼 (权限闸门在 brain._handle_boost_interest)
            topic = str(kwargs.get("topic", "")).strip()
            reason = str(kwargs.get("reason", ""))
            if not topic:
                return "失败: 缺少 topic"
            if not reason:
                return "失败: 缺少 reason (涨兴趣需说明理由)"
            delta = float(kwargs.get("delta", 1.0) or 1.0)
            damping = kwargs.get("damping", 0.5)
            try:
                new_value = await self.entity_store.boost_interest(topic, delta, float(damping))
            except Exception as e:
                return f"兴趣调整失败: {e}"
            return (f"已上涨【{topic}】兴趣值至 {new_value} (理由: {reason}), "
                    f"衰减阻尼设为 {damping} (越小衰减越慢, 但仍会衰减)。")

        elif command == "list_interests":
            # [0.2.9.0] 兴趣 top-N 读路径 (brain 定期拉取注入闸门动态关键词)。读无权限闸门。
            top_n = int(kwargs.get("top_n", 8) or 8)
            entity = await self.entity_store.get_entity("阿露兴趣") or {}
            rows = []
            for topic, rec in entity.items():
                try:
                    rows.append((float(rec.get("value", 0)), topic))
                except (TypeError, ValueError):
                    continue
            rows.sort(key=lambda r: r[0], reverse=True)
            top = rows[:top_n]
            if not top:
                return "暂无兴趣记录。"
            return "阿露兴趣 top%d: %s" % (len(top), ", ".join(f"{t}({v:g})" for v, t in top))

        elif command == "get_social":
            # 双轴读路径 (单人): VIP 工具/HITL 查某人好感度/注意度。读无权限闸门。
            person = kwargs.get("person", "")
            if not person:
                return "失败: 缺少 person"
            social = await self.entity_store.get_social(person)
            return f"好感度 {social.get('favorability', 50):.0f}/注意度 {social.get('attention', 50):.0f}"

        elif command == "get_socials":
            # 双轴 prompt 上线 (0.1.7.0): 批量读多人双轴, 格式化文本供 brain 注入 system prompt。
            # 读路径无权限闸门 (只读)。按 attention 降序, cap top-8 防 prompt 体积爆。
            persons = kwargs.get("persons", []) or []
            if not persons:
                return ""
            socials = await self.entity_store.get_socials(persons)
            if not socials:
                return ""
            return "\n".join(
                f"- {s['name']}: 好感{s['favorability']:.0f}/注意{s['attention']:.0f}"
                for s in socials[:8]
            )
        
        elif command == "list_socials_raw":
            # P_Core-1 实体图谱: 返回所有实体双轴 JSON (供 UI 画节点, 非格式化文本)。
            # 读路径无权限闸门 (只读)。复用 get_socials 拿结构化 [{name,favorability,attention}]。
            import json as _json
            try:
                data = await self.entity_store._load()
            except Exception:
                return "[]"
            persons = [e for e in data.keys() if e and e not in ("未知", "self")]
            socials = await self.entity_store.get_socials(persons)
            return _json.dumps(socials, ensure_ascii=False)

        elif command == "list_episodes":
            # P_Core-3 入梦审查: 列出情景摘要记忆 (prefix=episode), 供 UI 展示"记住了什么"。
            # ReflectionDaemon 摘要写 vector_store(prefix=episode), 非独立日志文件。
            import json as _json
            limit = int(kwargs.get("limit", "20"))
            items = await asyncio.to_thread(self.vector_store.list_by_prefix, "episode", limit)
            return _json.dumps(items, ensure_ascii=False)

        elif command == "list_rules":
            # P_Core-3 入梦审查: 列出性格规则 (顿悟演化), 供 UI 展示"规矩"。
            import json as _json
            rules = await self.rule_store.get_all_rules()
            return _json.dumps(rules, ensure_ascii=False)

        elif command == "daemon_status":
            # P5 任务中心: 报 memory 三 daemon (reflection/gc/scheduler) task 存活状态。
            import json as _json
            def _alive(daemon) -> bool:
                if daemon is None:
                    return False
                t = getattr(daemon, "task", None)
                return t is not None and not t.done()
            status = {
                "reflection": _alive(getattr(self, "reflection_daemon", None)),
                "gc": _alive(getattr(self, "gc_daemon", None)),
                "scheduler": _alive(getattr(self, "scheduler_daemon", None)),
            }
            return _json.dumps(status, ensure_ascii=False)

        elif command == "archival_search":
            query = kwargs.get("query", "") or kwargs.get("text", "")
            limit = int(kwargs.get("limit", "10"))
            return await self.file_io.deep_archive_search(query, limit=limit) if query else "失败：需提供关键词"

        elif command == "set_reminder":
            remind_time = kwargs.get("time", "") or kwargs.get("text", "")
            message = kwargs.get("message", remind_time)
            recurring = kwargs.get("recurring", "none")
            if not remind_time:
                return "失败：需提供提醒时间，格式 YYYY-MM-DD HH:MM"
            try:
                reminder = self.scheduler_daemon.add_reminder(remind_time, message, recurring)
                await asyncio.to_thread(
                    self._ledger_track,
                    kind="episode", store="schedule",
                    store_ref=f"reminder_{reminder['id']}",
                    content=f"{remind_time} {message}", actor="user",
                    is_master_source=True,
                )
                return f"提醒已设定: {remind_time} — {message} (重复: {recurring})"
            except ValueError as e:
                return f"失败: {e}"

        elif command == "list_reminders":
            return self.scheduler_daemon.list_reminders()

        elif command == "remove_reminder":
            reminder_id = int(kwargs.get("id", 0))
            if reminder_id:
                ok = self.scheduler_daemon.remove_reminder(reminder_id)
                if ok:
                    row = self.ledger.find_by_store_ref(
                        "schedule", f"reminder_{reminder_id}",
                    )
                    if row:
                        self.ledger.mark_status(row["memory_id"], "forgotten")
                        self.ledger.record_event(
                            row["memory_id"], "forgotten", actor="owner",
                            reason="提醒已删除",
                        )
                return "提醒已删除。" if ok else "未找到该提醒。"
            return "失败：需提供提醒ID"

        elif command == "read_soul":
            return await self.read_soul_async()

        elif command == "entity_history":
            # [双时态版本链] 回溯某条事实的完整演进历史
            entity = kwargs.get("entity", "") or kwargs.get("name", "")
            key = kwargs.get("key", "") or kwargs.get("attribute", "")
            if not entity or not key:
                return "失败：需提供 entity 与 key"
            chain = await self.entity_store.entity_history(entity, key)
            if not chain:
                return f"未找到实体【{entity}】的属性【{key}】"
            lines = [f"【{entity}.{key}】版本链 (按时间倒序):"]
            for i, ver in enumerate(chain, 1):
                vf = time.strftime("%Y-%m-%d", time.localtime(ver["valid_from"])) if ver.get("valid_from") else "未知"
                vt = time.strftime("%Y-%m-%d", time.localtime(ver["valid_to"])) if ver.get("valid_to") else "至今"
                tag = "当前生效" if ver["status"] == "alive" else "已过期"
                lines.append(f"  {i}. [{tag}] {ver['value']}  (有效: {vf} ~ {vt})")
            return "\n".join(lines)

        elif command == "get_time_anchor":
            return self.scheduler_daemon.get_time_anchor_text()

        # =================== [GraphStore 图谱命令] ===================
        # 供 UI/老板控制台 (system_memory hidden, 不暴露 LLM)。读命令无闸门,
        # 编辑类 (edit/block/prune) 需 is_master=True (UI 显式传, 守 think-batch 教训)。
        elif command == "graph_stats":
            import json as _json
            s = await self.graph_store.stats()
            return _json.dumps(s, ensure_ascii=False)

        elif command == "vector_projection":
            # [0.2.13.0 向量星图] 真实 embedding PCA 2D 投影, 供 UI 散点星图。读无闸门。
            import json as _json
            pts = self.vector_store.projection_sample(limit=int(kwargs.get("limit", 400)))
            return _json.dumps(pts, ensure_ascii=False)

        elif command == "graph_time_range":
            # 返 occurred_at [min,max] (供前端 QDateEdit 定日期区间)。读无闸门。0.2.4.3
            import json as _json
            r = await self.graph_store.time_range(graph=kwargs.get("graph"))
            return _json.dumps(r, ensure_ascii=False)

        elif command == "graph_query":
            # 查子图 (JSON 供 UI Phase 2 画图)。读无闸门。
            # 0.2.3.0 分桶: graph 过滤时 edges 走 query_edges_by_graph (src/dst 严格同域),
            # include_edges 默认 True (前端画图要边)
            # 0.2.4.0 时间检索: start_ts/end_ts 有则走 query_*_by_time (occurred_at BETWEEN 切片)
            import json as _json
            graph = kwargs.get("graph")
            start_ts = kwargs.get("start_ts")
            end_ts = kwargs.get("end_ts")
            if start_ts is not None and end_ts is not None:
                # 时间切片模式 (双段式时空变焦, 前端 QDateEdit/QSlider 触发)
                nodes = await self.graph_store.query_nodes_by_time(
                    float(start_ts), float(end_ts),
                    kind=kwargs.get("kind"),
                    graph=graph,
                    active_only=bool(kwargs.get("active_only", False)),
                )
                edges = []
                if kwargs.get("include_edges", True):
                    edges = await self.graph_store.query_edges_by_time(
                        float(start_ts), float(end_ts), graph=graph
                    )
            else:
                nodes = await self.graph_store.query_nodes(
                    kind=kwargs.get("kind"),
                    graph=graph,
                    keyword=kwargs.get("keyword"),
                    active_only=bool(kwargs.get("active_only", False)),
                )
                edges = []
                if kwargs.get("include_edges", True):
                    if graph:
                        edges = await self.graph_store.query_edges_by_graph(graph)
                    else:
                        edges = await self.graph_store.query_edges(relation=kwargs.get("relation"))
            # [0.2.13.0 图谱防卡死] limit top-N 剪枝: 节点按 (is_core, weight) 降序取
            # 前 N, 边只保留两端都在保留集内的。治"全脑融合全量渲染卡死电脑"。
            limit = kwargs.get("limit")
            if limit is not None and len(nodes) > int(limit):
                capped = sorted(
                    nodes,
                    key=lambda n: (n.get("is_core", 0), n.get("weight", 0.5)),
                    reverse=True,
                )[: int(limit)]
                kept_ids = {n["id"] for n in capped}
                edges = [e for e in edges
                         if e.get("src") in kept_ids and e.get("dst") in kept_ids]
                nodes = capped
            # [0.2.21.0 图谱 UX] 剪枝后富化: degree + 社群 + (社交/全脑域)双轴合并。
            # 只增 key 向后兼容; 整段 try/except 兜底, 富化失败仍返回原 nodes/edges, 绝不破坏 graph_query。
            communities: list = []
            try:
                from apps.system_memory_god.storage.graph_metrics import (
                    enrich_graph, merge_social_axes,
                )
                nodes, communities = enrich_graph(nodes, edges)
                if graph in (None, "social"):
                    try:
                        raw = await self.execute_command("list_socials_raw")
                        socials = _json.loads(raw) if raw else []
                        if isinstance(socials, list):
                            nodes = merge_social_axes(nodes, socials)
                    except Exception as _se:
                        import logging as _lg
                        _lg.getLogger("system_memory_god").warning(
                            "[graph_query] socials 双轴合并跳过: %s", _se)
            except Exception as _ee:
                import logging as _lg
                _lg.getLogger("system_memory_god").warning(
                    "[graph_query] 图谱富化跳过: %s", _ee)
            return _json.dumps(
                {"nodes": nodes, "edges": edges, "communities": communities},
                ensure_ascii=False)

        elif command == "graph_edit":
            # 老板手动增节点/边 (upsert 语义, add_node/add_edge 内部已含 upsert)。
            # is_master 闸门。
            if not self._require_master(kwargs):
                return "失败: graph_edit 需老板权限 (is_master=True)"
            import json as _json
            action = kwargs.get("action", "")
            if action == "add_node":
                name = kwargs.get("name", "")
                if not name:
                    return "失败: 缺少 name"
                res = await self.graph_store.add_node(
                    kind=kwargs.get("kind", "person"),
                    graph=kwargs.get("graph", "social"),
                    name=name,
                    properties=kwargs.get("properties"),
                    source=kwargs.get("source", "master"),
                    is_core=bool(kwargs.get("is_core", False)),
                )
                return _json.dumps(res, ensure_ascii=False)
            elif action == "add_edge":
                src_name = kwargs.get("src_name", "")
                dst_name = kwargs.get("dst_name", "")
                relation = kwargs.get("relation", "")
                if not src_name or not dst_name or not relation:
                    return "失败: 缺少 src_name/dst_name/relation"
                graph = kwargs.get("graph", "social")
                # 按 name 精确匹配查 node id (query_nodes keyword 是 LIKE, 需过滤)
                src_nodes = await self.graph_store.query_nodes(graph=graph, keyword=src_name)
                dst_nodes = await self.graph_store.query_nodes(graph=graph, keyword=dst_name)
                src_id = next((n["id"] for n in src_nodes if n["name"] == src_name), None)
                dst_id = next((n["id"] for n in dst_nodes if n["name"] == dst_name), None)
                if not src_id:
                    return f"失败: 找不到节点 {src_name}"
                if not dst_id:
                    return f"失败: 找不到节点 {dst_name}"
                res = await self.graph_store.add_edge(
                    src_id, dst_id, relation,
                    properties=kwargs.get("properties"),
                    source=kwargs.get("source", "master"),
                    is_core=bool(kwargs.get("is_core", False)),
                )
                return _json.dumps(res, ensure_ascii=False)
            return "失败: 未知 graph_edit action (add_node/add_edge)"

        elif command == "graph_block":
            # 加黑名单 (immutable, 只增不删, 无 remove API)。is_master 闸门。
            if not self._require_master(kwargs):
                return "失败: graph_block 需老板权限 (is_master=True)"
            import json as _json
            pattern_type = kwargs.get("pattern_type", "node")
            match_key = kwargs.get("match_key", {})
            if not match_key:
                return "失败: 缺少 match_key"
            reason = kwargs.get("reason", "")
            actor = kwargs.get("actor", "boss")
            res = await self.graph_store.add_block(pattern_type, match_key, reason, actor)
            return _json.dumps(res, ensure_ascii=False)

        elif command == "graph_prune":
            # 修剪 (删节点/边 + 可选写黑名单防反弹)。is_master 闸门。
            # block=False 只删不封锁 (0.2.5.4 解耦, 批次修剪误识别节点用)。
            if not self._require_master(kwargs):
                return "失败: graph_prune 需老板权限 (is_master=True)"
            import json as _json
            target_id = kwargs.get("id", "")
            if not target_id:
                return "失败: 缺少 id"
            reason = kwargs.get("reason", "")
            actor = kwargs.get("actor", "boss")
            block = kwargs.get("block", True)
            if kwargs.get("action", "node") == "edge":
                res = await self.graph_store.prune_edge(target_id, reason, actor)
            else:
                res = await self.graph_store.prune_node(target_id, reason, actor, block=block)
            return _json.dumps(res, ensure_ascii=False)

        elif command == "graph_export":
            # 导出 training_pairs.jsonl (微调前置)。读无闸门。
            import json as _json
            path = kwargs.get("path") or os.path.join(self.graph_store.data_dir, "training_pairs.jsonl")
            n = await self.graph_store.export_training_pairs(path)
            return _json.dumps({"ok": True, "path": path, "count": n}, ensure_ascii=False)

        return "未知命令"

