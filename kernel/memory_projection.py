# kernel/memory_projection.py
"""
[0.3.26.0 记忆升级工程 Phase 1] 统一只读投影 (Memory Projection)。

规格 4.3: 为 live 和 inactive 智能体返回同形状的只读 DTO。

实现边界:
- live Adapter: 通过 AppBus 调用 memory provider 的只读命令 (ledger 查询 +
  现有只读命令), 不直接 import apps (守 capability-generic-not-coupled);
- inactive Adapter: 复用并扩展 kernel/memory_view.py (只读窗口);
- Projection 不直接修改 hit_count;
- REST 不直接 import apps.system_memory_god;
- 某能力在 inactive 模式不可安全读取时, 返回 capability=false,
  而不是退化成潜在写操作。

同形 DTO 契约 (规格 5.1 MemoryItem):
  id/kind/store/content/status/created_at/valid_from/valid_to/speaker/
  chat_name/visibility/importance/confidence/hit_count/promoted/
  source_count/has_history/links。
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)


def _preview(text: str, limit: int = 160) -> str:
    if not text:
        return ""
    return str(text).strip()[:limit]


def _norm(root: str) -> str:
    return os.path.normcase(os.path.abspath(root or ""))


def _timestamp(value) -> Optional[float]:
    """Normalize legacy numeric/ISO timestamps to the DTO unix-seconds form."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except (ValueError, OverflowError, OSError):
        return None


def _detail_envelope(item: dict, *, writable: bool,
                     full_text: str = "") -> dict:
    """Return the canonical detail envelope used by live and inactive views."""
    kind = str(item.get("kind", "") or "")
    can_restore = (
        writable
        and item.get("status") == "forgotten"
        and item.get("store") == "vector"
    )
    result = {
        "item": item,
        "derivations": [],
        "events": [],
        "capabilities": {
            "write": writable,
            "restore": can_restore,
            "rules_write": writable and kind == "procedural_rule",
            "curation": writable,
        },
    }
    if full_text:
        result["full_text"] = full_text
    return result


class LiveMemoryProjection:
    """活动智能体投影: AppBus -> system_memory_god 只读命令。

    全部只读: ledger 查询 (provenance) + 现有只读命令 (list_episodes /
    list_rules / daemon_status)。构造不触盘。
    """

    def __init__(self, bus, app_name: str = "system_memory_god"):
        self.bus = bus
        self.app_name = app_name

    def _provider(self):
        """经 AppBus 找 provider 实例 (不直接 import apps)。"""
        if not self.bus:
            return None
        find = getattr(self.bus, "_find_app", None)
        if callable(find):
            return find(self.app_name)
        # 兜底: active_apps dict 直接取
        return getattr(self.bus, "active_apps", {}).get(self.app_name)

    def _available(self) -> bool:
        return self._provider() is not None

    async def capabilities(self) -> dict:
        """capability map: 每项能力是否可用 + 不可用原因。"""
        if not self._available():
            return {
                "provider": "live",
                "available": False,
                "reason": "memory provider 未连接",
                "capabilities": {},
            }
        return {
            "provider": "live",
            "available": True,
            "reason": "",
            "capabilities": {
                "ledger": True, "overview": True, "items": True,
                "detail": True, "timeline": True, "entities": True,
                "graph": True, "graph_stats": True, "graph_nodes": True,
                "rules": True, "rules_write": True,
                "recall_inspect": True, "restore": True,
                "curation": True,
            },
        }

    async def overview(self) -> dict:
        """ledger 聚合 + daemon 状态 + backlog (provider 只读方法)。"""
        provider = self._provider()
        if provider is None:
            return {"error": "memory provider 不可用"}
        try:
            fn = getattr(provider, "memory_overview_v2", None)
            if callable(fn):
                return await fn()
            return {"error": "provider 未实现 memory_overview_v2"}
        except Exception as e:
            logger.warning(f"[MemoryProjection] overview 失败(非致命): {e}")
            return {"error": str(e)}

    async def items(self, kind: str = "", status: str = "", chat_name: str = "",
                    speaker: str = "", visibility: str = "", query: str = "",
                    start_ts: float = None, end_ts: float = None,
                    exclude_visibilities: tuple[str, ...] = (),
                    limit: int = 50, offset: int = 0) -> list:
        provider = self._provider()
        if provider is None:
            return []
        try:
            fn = getattr(provider, "memory_items_v2", None)
            if callable(fn):
                # [0.3.32.0 三梯队 G03] 过滤面透传 (provider 侧落 ledger SQL)
                return await fn(kind=kind, status=status, chat_name=chat_name,
                                speaker=speaker, visibility=visibility,
                                query=query, start_ts=start_ts, end_ts=end_ts,
                                exclude_visibilities=exclude_visibilities,
                                limit=limit, offset=offset)
            return []
        except Exception as e:
            logger.warning(f"[MemoryProjection] items 失败(非致命): {e}")
            return []

    async def item_detail(self, memory_id: str) -> Optional[dict]:
        provider = self._provider()
        if provider is None:
            return None
        try:
            fn = getattr(provider, "memory_item_detail_v2", None)
            if callable(fn):
                detail = await fn(memory_id)
                if detail is None:
                    return None
                if not isinstance(detail, dict):
                    return None
                item = detail.get("item")
                if not isinstance(item, dict):
                    item = detail
                    detail = _detail_envelope(item, writable=True)
                else:
                    detail = dict(detail)
                    detail.setdefault("derivations", [])
                    detail.setdefault("events", [])
                    detail["capabilities"] = _detail_envelope(
                        item, writable=True,
                    )["capabilities"]
                return detail
            return None
        except Exception as e:
            logger.warning(f"[MemoryProjection] item_detail 失败(非致命): {e}")
            return None


class InactiveMemoryProjection:
    """非活动智能体投影: ReadonlyMemoryView + 只读 DTO 组装。

    铁律 (同 memory_view): 零写副作用, 不启动 daemon, 不增加 hit,
    不打开可写 SQLite (graph.db 走 ro URI; chroma 走 isdir 守卫)。
    """

    def __init__(self, root: str):
        self.root = root
        self._view = None

    def _get_view(self):
        from kernel.memory_view import get_view
        if self._view is None:
            self._view = get_view(self.root)
        return self._view

    async def capabilities(self) -> dict:
        return {
            "provider": "inactive",
            "available": True,
            "reason": "",
            "capabilities": {
                "ledger": False, "overview": True, "items": True,
                "detail": True, "timeline": False, "entities": True,
                "graph": True, "graph_stats": True, "graph_nodes": False,
                "rules": True, "rules_write": False,
                "recall_inspect": False, "restore": False,
                "curation": False,
            },
        }

    async def overview(self) -> dict:
        try:
            view = self._get_view()
            stats = view.graph_stats()
            rules = view.rules_list()
            entities = view._load_json("entities.json")
            return {
                "provider": "inactive",
                "graph": stats,
                "rules_count": len(rules),
                "entities_count": len(entities),
                "daemons": {"running": False,
                            "note": "inactive 智能体不启动 daemon"},
                "ledger": {"available": False,
                           "note": "inactive 只读窗口不打开可写 SQLite"},
            }
        except Exception as e:
            logger.warning(f"[MemoryProjection] inactive overview 失败(非致命): {e}")
            return {"provider": "inactive", "error": str(e)}

    async def items(self, kind: str = "", status: str = "", chat_name: str = "",
                    speaker: str = "", visibility: str = "", query: str = "",
                    start_ts: float = None, end_ts: float = None,
                    exclude_visibilities: tuple[str, ...] = (),
                    limit: int = 50, offset: int = 0) -> list:
        """inactive 只读: 实体事实 + 规则 + 图谱节点折成同形 DTO。

        [0.3.32.0 三梯队 G03] 过滤面: 老存储无 speaker/隐私桶/时间元数据,
        仅 query 内容模糊过滤可用, 其余参数如实不生效 (不造假)。
        """
        try:
            view = self._get_view()
            rows: list[dict] = []
            entities = view._load_json("entities.json")
            for entity_name, attrs in entities.items():
                if not isinstance(attrs, dict):
                    continue
                for k, v in attrs.items():
                    if isinstance(v, dict):
                        value_text = v.get("value", str(v))
                        is_core = bool(v.get("is_core_fact", False))
                    else:
                        value_text = str(v)
                        is_core = False
                    rows.append({
                        "id": f"legacy_entity_{entity_name}.{k}",
                        "kind": "semantic_fact",
                        "store": "entity",
                        "content": _preview(f"{entity_name}.{k}: {value_text}"),
                        "status": "active",
                        "created_at": None,
                        "valid_from": None,
                        "valid_to": None,
                        "speaker": "",
                        "chat_name": "",
                        "visibility": "",
                        "importance": 1.0 if is_core else None,
                        "confidence": None,
                        "hit_count": 0,
                        "promoted": False,
                        "source_count": 0,
                        "has_history": False,
                        "links": {"entity": entity_name, "entity_key": str(k),
                                  "entity_value": _preview(value_text),
                                  "graph_node": None, "source": False},
                    })
            # 规则并入 (procedural_rule)
            for r in view.rules_list():
                if not isinstance(r, dict):
                    continue
                rows.append({
                    "id": f"legacy_rule_{r.get('id', '')}",
                    "kind": "procedural_rule",
                    "store": "rule",
                    "content": _preview(r.get("text", "")),
                    "status": "active",
                    "created_at": _timestamp(r.get("created_at")),
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
                })
            if kind:
                rows = [r for r in rows if r["kind"] == kind]
            if query:
                rows = [r for r in rows
                        if query in str(r.get("content", ""))]

            def _sort_key(r: dict):
                # rule 的 created_at 可能是 "YYYY-MM-DD HH:MM:SS" 字符串或 None,
                # 统一转秒级数值再排序, 无法解析视为 0 (旧数据靠后)。
                return _timestamp(r.get("created_at")) or 0.0

            rows.sort(key=_sort_key, reverse=True)
            start = max(0, offset)
            return rows[start:start + max(1, min(limit, 200))]
        except Exception as e:
            logger.warning(f"[MemoryProjection] inactive items 失败(非致命): {e}")
            return []

    async def item_detail(self, memory_id: str) -> Optional[dict]:
        try:
            view = self._get_view()
            if memory_id.startswith("legacy_entity_"):
                ent_key = memory_id[len("legacy_entity_"):]
                ent, _, key = ent_key.rpartition(".")
                entities = view._load_json("entities.json")
                attrs = entities.get(ent, {})
                if isinstance(attrs, dict) and key in attrs:
                    v = attrs[key]
                    value_text = v.get("value", str(v)) if isinstance(v, dict) else str(v)
                    item = {
                        "id": memory_id, "kind": "semantic_fact", "store": "entity",
                        "content": _preview(value_text), "status": "active",
                        "created_at": None, "valid_from": None, "valid_to": None,
                        "speaker": "", "chat_name": "",
                        "visibility": "", "importance": None, "confidence": None,
                        "hit_count": 0, "promoted": False, "source_count": 0,
                        "has_history": False,
                        "links": {"entity": ent, "entity_key": key,
                                  "graph_node": None, "source": False},
                    }
                    return _detail_envelope(
                        item, writable=False, full_text=value_text,
                    )
            if memory_id.startswith("legacy_rule_"):
                for r in view.rules_list():
                    if isinstance(r, dict) and f"legacy_rule_{r.get('id', '')}" == memory_id:
                        full_text = str(r.get("text", "") or "")
                        item = {
                            "id": memory_id, "kind": "procedural_rule", "store": "rule",
                            "content": _preview(full_text), "status": "active",
                            "created_at": _timestamp(r.get("created_at")), "valid_from": None,
                            "valid_to": None, "speaker": "",
                            "chat_name": "", "visibility": "",
                            "importance": None, "confidence": None,
                            "hit_count": int(r.get("hit_count", 0) or 0),
                            "promoted": False, "source_count": 0, "has_history": False,
                            "links": {"entity": None, "graph_node": None,
                                      "source": False},
                        }
                        return _detail_envelope(
                            item, writable=False, full_text=full_text,
                        )
            return None
        except Exception as e:
            logger.warning(f"[MemoryProjection] inactive item_detail 失败(非致命): {e}")
            return None


def get_projection(agent: object, active_root: str = "", bus: object = None) -> object:
    """按智能体活跃状态选投影 (同形 DTO 契约)。

    agent 须带 memory_root/memory_mode; active_root 为当前活动智能体记忆根
    (空串时按 memory_mode 推断: shared 视为活体)。bus 显式注入 live 投影
    (AgentRecord 无 bus 属性, REST 层从 request.app.state 传入); 缺省回退
    agent.bus (老调用方零变化)。REST 层经此工厂取实例。
    """
    root = getattr(agent, "memory_root", "") or ""
    if not root:
        return None
    mode = getattr(agent, "memory_mode", "") or ""
    is_active = (
        bool(active_root and _norm(root) == _norm(active_root))
        if active_root else mode == "shared"
    )
    if is_active:
        if bus is None:
            bus = getattr(agent, "bus", None)
        return LiveMemoryProjection(bus)
    return InactiveMemoryProjection(root)
