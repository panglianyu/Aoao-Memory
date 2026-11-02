# -*- coding: utf-8 -*-
"""记忆升级工程 Phase 1 -- 内部值对象 (apps/system_memory_god/models.py)。

规格 4.1: 为 ledger/投影/治理提供类型安全的值对象, 不承担存储职责。
纯 dataclass, 无 IO, 单测直接钉死契约。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# 记忆类型 (规格 3.3 三维度之一)
MEMORY_KINDS = (
    "observation", "episode", "semantic_fact",
    "procedural_rule", "relationship", "visual_fact",
)

# 存储温度 (规格 3.3)
STORAGE_TEMPERATURES = ("working", "active", "archive")

# 生命周期 (规格 3.3)
LIFECYCLE_STATUSES = (
    "candidate", "active", "conflicted",
    "superseded", "forgotten", "pruned",
)

# visibility 桶 (与 Phase 0 derive_visibility 契约一致)
VISIBILITY_BUCKETS = ("private", "group", "public", "internal")

# 派生关系类型 (memory_derivations.relation)
DERIVATION_RELATIONS = (
    "summarized_from", "extracted_from", "inferred_from", "supersedes",
)


@dataclass(frozen=True)
class MemoryItem:
    """账本 memory_items 行投影 (规格 4.1 最低字段超集)。

    老数据无 ledger 记录时 memory_id 可为 None (懒解析生成 legacy_* 只读标识),
    其余字段尽量从底层存储 metadata 回填, 缺失字段用 None/空串如实呈现。
    """
    memory_id: Optional[str] = None          # mem_ + UUID4
    kind: str = "observation"                # MEMORY_KINDS
    store: str = ""                          # journal/vector/entity/rule/graph
    store_ref: str = ""                      # 底层文档/实体键/规则/边标识
    content_preview: str = ""                # 脱敏截断展示文本
    actor: str = "unknown"                   # user/assistant/tool/system/unknown
    speaker: str = ""                        # 人物或工具名
    chat_name: str = ""                      # 来源会话
    visibility: str = ""                     # VISIBILITY_BUCKETS, 空=未知
    is_master_source: bool = False           # 来源是否主人
    created_at: Optional[float] = None       # 记录时间 (秒)
    entity_revision: Optional[float] = None  # EntityStore tx_time，单调写 CAS
    valid_from: Optional[float] = None       # 事实有效起始, 可空
    valid_to: Optional[float] = None         # 事实有效结束, 可空
    status: str = "active"                   # LIFECYCLE_STATUSES
    confidence: Optional[float] = None       # 0~1
    importance: Optional[float] = None       # 0~1
    hit_count: int = 0
    promoted: bool = False
    source_count: int = 0
    has_history: bool = False


@dataclass(frozen=True)
class MemoryDerivation:
    """memory_derivations 行: 来源 -> 派生链。"""
    parent_id: str = ""
    child_id: str = ""
    relation: str = "extracted_from"         # DERIVATION_RELATIONS
    run_id: str = ""                         # 反思/治理任务
    created_at: Optional[float] = None


@dataclass(frozen=True)
class MemoryEvent:
    """memory_events 行: 生命周期事件, 只追加不覆盖。"""
    memory_id: str = ""
    event: str = "created"                   # created/recalled/promoted/superseded/forgotten/pruned/restored
    actor: str = "unknown"
    reason: str = ""
    created_at: Optional[float] = None


@dataclass(frozen=True)
class RecallRun:
    """recall_runs 行摘要: 检查器运行记录 (完整候选不持久化)。"""
    run_id: str = ""
    agent_id: str = ""
    query: str = ""
    created_at: Optional[float] = None
    total_candidates: int = 0
    acl_filtered: int = 0
    injected: int = 0
    duration_ms: int = 0
    reason_codes: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RecallCandidate:
    """recall_candidates 行: 候选分项评分 (脱敏摘要)。"""
    run_id: str = ""
    memory_id: str = ""
    source: str = ""                         # vector/entity/rule/graph
    score: float = 0.0
    rank: int = 0
    reason_code: str = ""                    # 淘汰/保留原因


def legacy_id(kind: str, store_ref: str) -> str:
    """老数据懒解析只读标识: legacy_<kind>_<store_ref 摘要>。

    不保证全局唯一, 仅 UI 展示/追踪用; 真实回溯走底层 store_ref。
    """
    digest = str(store_ref).replace(" ", "_")[:48]
    return f"legacy_{kind}_{digest}" if digest else f"legacy_{kind}_unknown"
