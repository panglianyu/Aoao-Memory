# -*- coding: utf-8 -*-
"""记忆升级工程 Phase 2 -- Recall Plan 统一召回策略与解释 (规格 4.2)。

统一以下现有分散路径: smart_recall / 第一公民 recall / 向量候选查询 /
Entity/Rule spotlight / 时间过滤 / 图谱一跳扩展 / 来源与跨群标注。

召回流水线固定顺序 (规格 4.2):
  1. 规范化 query 和时间范围;
  2. 生成向量、实体、规则候选;
  3. 先执行 visibility ACL;
  4. 执行类型和时间过滤;
  5. 计算语义、时效、重要度、命中、社交、同群分数;
  6. 对 Top-N 种子进行受限图谱扩展;
  7. 去重并执行 token/条数预算;
  8. 返回最终证据和结构化解释;
  9. 仅当 commit_hits=true 时增加 hit_count。

默认行为 (规格 4.2):
- Brain 的真实 recall: commit_hits=true;
- Web UI 搜索、预览和召回检查器: commit_hits=false;
- 群聊上下文缺失时不得自动假设主人; 只有 console/boot 明确系统上下文
  可使用 system-direct 权限;
- smart_recall 是否只查询 obs 必须由显式 include_types 决定。

ACL reason code (规格 6.7): private_other_chat / group_cross_chat_guest /
internal_guest / time_out_of_range / type_excluded / duplicate / budget_exceeded。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# [0.3.30.0 G18] 图谱一跳扩展硬限额 (防 N+1 查询拖慢召回; 全 async 失败降级)
_GRAPH_SEED_LIMIT = 5        # 最多 5 个种子名
_GRAPH_NODES_PER_SEED = 2    # 每个种子名最多取 2 个匹配节点
_GRAPH_EDGES_PER_NODE = 8    # 每个节点最多取 8 条边

# [0.3.30.0 G22] smart_recall 的 memory_scope -> 候选源集合映射:
# auto 保持历史行为 (warm 只查向量库, 守 brain 0.3s 预召回预算);
# portrait/relations/rules 按语义开对应候选阶段。
_SCOPE_SOURCES = {
    "auto": frozenset({"vector"}),
    "recents": frozenset({"vector"}),
    "portrait": frozenset({"vector", "entity"}),
    "relations": frozenset({"vector", "graph"}),
    "rules": frozenset({"rule"}),
}

# 淘汰原因 (规格 6.7 reason code)
REASON_PRIVATE_OTHER_CHAT = "private_other_chat"
REASON_GROUP_CROSS_CHAT_GUEST = "group_cross_chat_guest"
REASON_INTERNAL_GUEST = "internal_guest"
REASON_TIME_OUT_OF_RANGE = "time_out_of_range"
REASON_TYPE_EXCLUDED = "type_excluded"
REASON_DUPLICATE = "duplicate"
REASON_BUDGET_EXCEEDED = "budget_exceeded"


@dataclass
class RecallCandidate:
    """单条候选 + 分项评分 + 淘汰原因 (可变: 流水线内标记 rank/淘汰原因)。"""

    memory_id: str = ""
    source: str = ""                      # vector/entity/rule/graph
    content: str = ""
    score: float = 0.0
    rank: int = 0
    reason_code: str = ""                 # 空 = 保留
    # 分项 (Smallville 复合)
    semantic: float = 0.0
    recency: float = 0.0
    importance: float = 0.0
    hit_freq: float = 0.0
    social: float = 0.0
    same_chat_boost: float = 0.0
    # 溯源
    meta: dict = field(default_factory=dict)


@dataclass
class RecallPlanResult:
    """召回计划结果: 候选 + ACL 统计 + 注入顺序 + 解释 + 耗时。"""

    query: str = ""
    chat_name: str = ""
    asker_is_master: bool = True
    scope: str = "auto"                                # [Phase 4] 本轮记忆范围
    candidates: list = field(default_factory=list)     # 全部候选 (含淘汰)
    injected: list = field(default_factory=list)       # 最终注入顺序
    filtered_private_count: int = 0
    acl_filtered: int = 0
    time_filtered: int = 0
    type_filtered: int = 0
    deduped: int = 0
    budget_dropped: int = 0
    graph_expanded: int = 0
    duration_ms: int = 0
    reason_codes: dict = field(default_factory=dict)   # code -> 数量

    def explain(self) -> str:
        """结构化解释文本 (供检查器 UI / 调试)。"""
        lines = [
            f"查询「{self.query}」召回完成: 候选 {len(self.candidates)} 条, "
            f"注入 {len(self.injected)} 条 ({self.duration_ms}ms)",
        ]
        if self.acl_filtered:
            lines.append(f"ACL 过滤 {self.acl_filtered} 条 (私密无权限 "
                         f"{self.filtered_private_count} 条)")
        if self.time_filtered:
            lines.append(f"时间过滤 {self.time_filtered} 条")
        if self.type_filtered:
            lines.append(f"类型过滤 {self.type_filtered} 条")
        if self.deduped:
            lines.append(f"去重 {self.deduped} 条")
        if self.budget_dropped:
            lines.append(f"预算淘汰 {self.budget_dropped} 条")
        if self.graph_expanded:
            lines.append(f"图谱一跳扩展 {self.graph_expanded} 条")
        return "\n".join(lines)


class RecallPlan:
    """统一召回规划器。构造持存储句柄 (vector/entity/rule/graph 可缺省)。"""

    def __init__(self, vector_store=None, entity_store=None, rule_store=None,
                 graph_store=None):
        self.vector_store = vector_store
        self.entity_store = entity_store
        self.rule_store = rule_store
        self.graph_store = graph_store
        # [Phase 0 M0-06] 缺省 include_types=None 不过滤 (保旧行为);
        # 显式 ["obs"] 等才过滤, 不依赖注释。
        self.DEFAULT_INCLUDE_TYPES = None

    # ---------- scope 映射 ([0.3.26.0 Phase 4]) ----------

    @staticmethod
    def sources_for_scope(scope: str) -> frozenset:
        """[0.3.30.0 G22] memory_scope -> 候选源集合 (vector/entity/rule/graph)。

        未知 scope 落 auto 源集 (fail-safe 保 warm 历史行为)。
        """
        return _SCOPE_SOURCES.get(str(scope or "auto").strip().lower(),
                                  frozenset({"vector"}))

    @staticmethod
    def _apply_scope(scope: str, include_types, include_graph,
                     start_ts, end_ts):
        """把 memory_scope 规范化为 (include_types, include_graph, start_ts, end_ts)。

        scope 语义 (规格 6.9):
          auto      -- 原样透传 (零变化);
          recents   -- 只 observation/episode + 近 7 天时间窗;
          portrait  -- 只 semantic_fact (entity);
          relations -- 开图谱一跳;
          rules     -- 只 procedural_rule;
          disabled  -- 全禁 (返回空集, 调用方零注入)。
        """
        s = str(scope or "auto").strip().lower()
        if s == "auto" or not s:
            return include_types, include_graph, start_ts, end_ts
        if s == "disabled":
            return [], False, 0.0, 0.0  # start=end=0 -> 无候选
        if s == "recents":
            now = time.time()
            start = start_ts if start_ts is not None else (now - 7 * 86400)
            return ["observation", "episode"], include_graph, start, end_ts
        if s == "portrait":
            return ["semantic_fact"], False, start_ts, end_ts
        if s == "relations":
            return include_types, True, start_ts, end_ts
        if s == "rules":
            return ["procedural_rule"], False, start_ts, end_ts
        return include_types, include_graph, start_ts, end_ts

    # ---------- 入口 ----------

    async def plan(self, query: str, chat_name: str = "",
                   asker_is_master: bool = True, when_filter: str = "",
                   start_ts: Optional[float] = None,
                   end_ts: Optional[float] = None,
                   k: int = 10, include_types: Optional[list] = None,
                   include_graph: bool = False, commit_hits: bool = True,
                   visibility_context: str = "",
                   memory_scope: str = "auto",
                   sources: Optional[frozenset] = None,
                   filter_narration: bool = False) -> RecallPlanResult:
        """执行召回流水线 (规格 4.2 固定顺序)。全 fail-safe: 任一步失败
        只记候选缺失, 不抛 (真实召回路径容错优先)。

        [0.3.26.0 Phase 4] memory_scope 本轮记忆范围 (规格 6.9/任务 1):
          auto      -- 默认当前行为 (全部候选);
          recents   -- 只用近期经历 (时间窗收紧 + 只 observation/episode);
          portrait  -- 加载人物画像 (只 entity);
          relations -- 加载关系 (图谱一跳扩展);
          rules     -- 加载性格规则 (只 procedural_rule);
          disabled  -- 禁用长期记忆 (零注入, 不提交 hit)。

        [0.3.30.0 G20/G22] sources: 候选源集合门控 (None=全部, 第一公民 recall
        命令保持全源); smart_recall 按 scope 传 sources_for_scope() 结果, auto
        只跑向量阶段守住 brain 0.3s 预召回预算。
        filter_narration: True 时丢弃"助手回复: 已X"叙述性元话语候选
        (smart_recall 历史行为迁移保留; recall 命令缺省 False 零变化)。
        """
        t0 = time.time()
        _ALL_SOURCES = {"vector", "entity", "rule", "graph"}
        active_sources = _ALL_SOURCES if sources is None else set(sources)
        result = RecallPlanResult(
            query=query, chat_name=chat_name,
            asker_is_master=asker_is_master,
        )
        # [Phase 4] scope 规范化: 映射为 include_types/include_graph/时间窗。
        include_types, include_graph, start_ts, end_ts = self._apply_scope(
            memory_scope, include_types, include_graph, start_ts, end_ts,
        )
        if memory_scope == "disabled":
            result.scope = memory_scope
            result.duration_ms = 0
            return result
        # 1. 规范化时间范围 (when_filter 由调用方已解析; 此处兜底)
        if start_ts is None and when_filter:
            from system_memory_god.main import parse_when_filter
            parsed = parse_when_filter(when_filter)
            if parsed is not None:
                start_ts, end_ts = parsed

        # 2+3. 候选生成 + ACL 先行
        candidates: list = []
        private_filtered = 0
        # [0.3.30.0 G20] narration 判定懒导入 (kernel.reply_filters 是定义源,
        # 不经 system_memory_god.main 防循环引用)。
        _narration_check = None
        if filter_narration:
            try:
                from kernel.reply_filters import is_narration_statement as _narration_check
            except Exception as _imp_e:
                logger.debug(f"[RecallPlan] narration 判定导入失败(非致命): {_imp_e}")
                _narration_check = None
        # [0.3.30.3 G19] 前置淘汰候选 (ACL/时间): 独立收集不进流水线,
        # 末尾并回 result.candidates 供检查器展示 reason code (防被去重/预算
        # 阶段二次污染)。ACL 类 content 清空 (规格 10.2 不泄私密内容)。
        _pre_filtered: list = []
        _acl_rejects: list = []
        _ACL_REASONS = {REASON_PRIVATE_OTHER_CHAT, REASON_GROUP_CROSS_CHAT_GUEST,
                        REASON_INTERNAL_GUEST}
        if self.vector_store is not None and "vector" in active_sources:
            try:
                pairs = self.vector_store.query_memory_with_meta(
                    query, n_results=k * 3, prefer_chat=chat_name,
                    asker_is_master=asker_is_master,
                    commit_hits=commit_hits,
                    start_ts=start_ts, end_ts=end_ts,
                    reject_out=_acl_rejects, attach_breakdown=True,
                )
                for doc, meta in pairs:
                    doc_str = str(doc)
                    # [0.3.30.0 G20] smart_recall 历史行为迁移保留: 叙述性元话语
                    # obs ("助手回复: 已回复那个群" 等) 不入候选, 防幻觉种子浮顶。
                    if (_narration_check is not None
                            and doc_str.startswith("助手回复: ")
                            and _narration_check(doc_str[len("助手回复: "):].strip())):
                        continue
                    vis = str((meta or {}).get("visibility", "") or "")
                    if vis == "private":
                        # [0.3.30.0 G21] 对齐 vector_store._acl_allow (修两路判定
                        # 相反): chat 空 = system-direct, 主人上下文放行 (console/boot),
                        # guest 无上下文 fail-closed; chat 非空仅同私聊窗口保留。
                        # (ACL 开启时此层不触发, 为 ACL_ENABLED=False 回退态兜底)
                        if chat_name:
                            _allow_private = (meta.get("chat_name") == chat_name)
                        else:
                            _allow_private = bool(asker_is_master)
                        if not _allow_private:
                            private_filtered += 1
                            _pre_filtered.append(RecallCandidate(
                                memory_id="", source="vector", content="",
                                reason_code=REASON_PRIVATE_OTHER_CHAT, meta={},
                            ))
                            continue
                    # [0.3.30.3 G17] 分项分真值: vector_store 重排透出的
                    # _score_breakdown (加权贡献 + semantic=cosine_sim)。
                    meta_dict = dict(meta or {})
                    _bd = meta_dict.pop("_score_breakdown", None) or {}
                    candidates.append(RecallCandidate(
                        memory_id=str(meta_dict.get("doc_id", "") or ""),
                        source="vector", content=doc_str,
                        score=float(_bd.get("total", 0.0)),
                        semantic=float(_bd.get("semantic", 0.0)),
                        recency=float(_bd.get("recency", 0.0)),
                        importance=float(_bd.get("importance", 0.0)),
                        hit_freq=float(_bd.get("hit_freq", 0.0)),
                        social=float(_bd.get("social", 0.0)),
                        same_chat_boost=float(_bd.get("same_chat", 0.0)),
                        meta=meta_dict,
                    ))
            except Exception:
                pass  # 向量候选失败不阻断 (容错)
        # [0.3.30.3 G19] ACL/时间淘汰 -> reason code 候选 (规格 6.7)。
        for _doc_r, _meta_r, _reason_r in _acl_rejects:
            if _reason_r in _ACL_REASONS:
                if _reason_r == REASON_PRIVATE_OTHER_CHAT:
                    private_filtered += 1
                _pre_filtered.append(RecallCandidate(
                    memory_id="", source="vector", content="",
                    reason_code=_reason_r, meta={},
                ))
            else:
                result.time_filtered += 1
                _pre_filtered.append(RecallCandidate(
                    memory_id=str((_meta_r or {}).get("doc_id", "") or ""),
                    source="vector", content=str(_doc_r),
                    reason_code=_reason_r, meta=dict(_meta_r or {}),
                ))

        # 实体候选 (spotlight 同源匹配)
        if self.entity_store is not None and "entity" in active_sources:
            try:
                from kernel.memory_view import matches_entity
                entities = await self.entity_store._load()
                for entity_name, attrs in entities.items():
                    if not matches_entity(entity_name, query):
                        continue
                    if not isinstance(attrs, dict):
                        continue
                    for kk, v in attrs.items():
                        value_text = (v.get("value", str(v))
                                      if isinstance(v, dict) else str(v))
                        candidates.append(RecallCandidate(
                            memory_id=f"legacy_entity_{entity_name}.{kk}",
                            source="entity",
                            content=f"关于【{entity_name}】的{kk}: {value_text}",
                            score=0.0,
                            meta={"entity": entity_name, "key": kk},
                        ))
            except Exception:
                pass

        # 规则候选
        if self.rule_store is not None and "rule" in active_sources:
            try:
                rules_text = await self.rule_store.get_active_rules_text(limit=8)
                for line in rules_text.split("\n"):
                    raw = line.lstrip("- ").strip()
                    if raw:
                        candidates.append(RecallCandidate(
                            memory_id=f"legacy_rule_{raw[:32]}",
                            source="rule", content=raw, score=0.0,
                            meta={},
                        ))
            except Exception:
                pass

        # 4. 类型过滤 ([Phase 4] prefix 短名映射: obs->observation,
        # fact->semantic_fact, episode->episode, rule->procedural_rule)
        # [0.3.30.3 G19] 淘汰候选保留并标 type_excluded (检查器可见, 不静默丢)。
        _type_dropped: list = []
        if include_types:
            _PREFIX_KIND = {
                "obs": "observation", "fact": "semantic_fact",
                "episode": "episode", "rule": "procedural_rule",
            }
            _typed: list = []
            for c in candidates:
                _ok = (
                    c.source in include_types
                    or str(c.meta.get("prefix", "")) in include_types  # 短名直传兼容
                    or _PREFIX_KIND.get(str(c.meta.get("prefix", ""))) in include_types
                    or (c.source == "entity" and "semantic_fact" in include_types)
                    or (c.source == "rule" and "procedural_rule" in include_types)
                )
                if _ok:
                    _typed.append(c)
                else:
                    c.reason_code = REASON_TYPE_EXCLUDED
                    _type_dropped.append(c)
            result.type_filtered = len(_type_dropped)
            candidates = _typed

        # 5+6. 评分 (语义分近似: 保留向量序; 图谱一跳扩展)
        # 向量候选保留原始返回序 (Smallville 已在 vector_store 内排过),
        # 实体/规则候选按加入序稳定排后。
        for i, c in enumerate(candidates):
            c.rank = i

        if (include_graph and self.graph_store is not None
                and "graph" in active_sources):
            # [0.3.30.0 G18] 受限图谱一跳 (旧 get_neighbors 在 GraphStore 不存在,
            # AttributeError 被静默吞 -> 换真实 API query_nodes/query_edges/get_node):
            # 种子 = 实体候选 entity 名 + 向量候选 speaker (warm 路径无实体候选时
            # 也能扩展); 每步独立 try + debug 日志, 失败降级不静默。
            seed_names = {c.meta.get("entity") for c in candidates
                          if c.meta.get("entity")}
            seed_names |= {str(c.meta.get("speaker") or "") for c in candidates
                           if c.source == "vector" and c.meta.get("speaker")}
            seed_names.discard("")
            _seen_edges = set()
            _name_cache: dict = {}
            for name in list(seed_names)[:_GRAPH_SEED_LIMIT]:
                try:
                    nodes = (await self.graph_store.query_nodes(
                        keyword=str(name))) or []
                except Exception as _ge:
                    logger.debug(f"[RecallPlan] 图谱种子查询失败(非致命): {name}: {_ge}")
                    continue
                for node in nodes[:_GRAPH_NODES_PER_SEED]:
                    nid = str(node.get("id") or "")
                    nname = str(node.get("name") or name)
                    if not nid:
                        continue
                    try:
                        edges = (list(await self.graph_store.query_edges(src=nid))
                                 + list(await self.graph_store.query_edges(dst=nid)))
                    except Exception as _ge:
                        logger.debug(f"[RecallPlan] 图谱边查询失败(非致命): {nid}: {_ge}")
                        continue
                    for e in edges[:_GRAPH_EDGES_PER_NODE]:
                        peer_id = str(
                            e.get("dst") if e.get("src") == nid else (e.get("src") or ""))
                        if not peer_id:
                            continue
                        relation = str(e.get("relation") or "")
                        peer_name = await self._graph_node_name(peer_id, _name_cache)
                        key = (nname, relation, peer_name)
                        if key in _seen_edges:
                            continue
                        _seen_edges.add(key)
                        candidates.append(RecallCandidate(
                            memory_id=f"legacy_graph_{nname}->{peer_name}",
                            source="graph",
                            content=f"关系: {nname} --[{relation}]--> {peer_name}",
                            score=0.0,
                            meta={"from": nname, "to": peer_name,
                                  "relation": relation},
                        ))
                        result.graph_expanded += 1

        # 7. 去重 (按 content 去重) + 预算
        seen, deduped_cands = set(), []
        for c in candidates:
            key = str(c.content).strip()
            if key in seen:
                result.deduped += 1
                c.reason_code = REASON_DUPLICATE
                deduped_cands.append(c)  # 淘汰候选保留 (检查器可见 reason)
                continue
            seen.add(key)
            deduped_cands.append(c)
        candidates = deduped_cands

        # 预算: 保留 Top-k (rank 序), 其余标 budget_exceeded
        # (已有淘汰原因如 duplicate 的候选不再被覆盖, reason 单一优先)
        kept, dropped = [], []
        for c in candidates:
            if len(kept) < k:
                kept.append(c)
            else:
                if not c.reason_code:
                    c.reason_code = REASON_BUDGET_EXCEEDED
                dropped.append(c)
        result.budget_dropped = len(dropped)

        # [0.3.30.3 G19] 全量候选 = 注入 + 预算淘汰 + 类型淘汰 + ACL/时间淘汰,
        # 检查器按 reason_code 展示全貌; ACL 类计数口径 = 私密无权限数。
        result.candidates = kept + dropped + _type_dropped + _pre_filtered
        result.injected = [c for c in kept]
        result.acl_filtered = len([c for c in _pre_filtered
                                   if c.reason_code in _ACL_REASONS])
        result.filtered_private_count = private_filtered
        # reason code 汇总
        codes: dict = {}
        for c in result.candidates:
            if c.reason_code:
                codes[c.reason_code] = codes.get(c.reason_code, 0) + 1
        result.reason_codes = codes
        result.duration_ms = int((time.time() - t0) * 1000)
        return result

    async def _graph_node_name(self, node_id: str, cache: dict) -> str:
        """[0.3.30.0 G18] 解析图谱节点名 (带缓存, 失败非致命返 ID)。"""
        if node_id in cache:
            return cache[node_id]
        name = node_id
        try:
            node = await self.graph_store.get_node(node_id)
            if node:
                name = str(node.get("name") or node_id)
        except Exception as _ge:
            logger.debug(f"[RecallPlan] 图谱节点解析失败(非致命): {node_id}: {_ge}")
        cache[node_id] = name
        return name

    # ---------- 文本渲染 (兼容 execute_command 返回) ----------

    def render_text(self, result: RecallPlanResult) -> str:
        """把注入候选渲染成文本 (smart_recall/recall 命令的返回格式)。"""
        if not result.injected:
            return ""
        parts = []
        for c in result.injected:
            prefix = ""
            meta = c.meta or {}
            if c.source == "vector" and meta.get("is_master") is False:
                prefix += "[他人转述]"
            obs_chat = str(meta.get("chat_name", "") or "")
            if result.chat_name and obs_chat and obs_chat != result.chat_name:
                prefix += f"[跨群参考:{obs_chat}]"
            parts.append(f"{prefix}{c.content}" if prefix else c.content)
        return "检索线索：" + " | ".join(parts)
