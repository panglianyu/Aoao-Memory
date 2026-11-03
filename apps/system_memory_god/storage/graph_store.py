# -*- coding: utf-8 -*-
"""[GraphStore] 知识图谱 + 社交图谱存储 (SQLite + NetworkX + JSONL)。

双图谱统一存储:
- 社交图谱 (graph=social): 节点=人/事件, 边=朋友/老板/参与/提及/承诺
- 知识图谱 (graph=knowledge): 节点=skill/知识结论/项目, 边=依赖/分类/来源

机制1 权重频次分级: 水友低权重潜伏, 频次升阶激活, master 无条件 1.0
机制2 黑名单熔断: 老板修剪写 immutable blocklist, 自动构图写入前拦截

守 /glue (不依赖外部图谱框架) + capability-generic-not-coupled (通过 AppBus 暴露)。
"""
import os
import json
import time
import uuid
import asyncio
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from kernel.logger import get_logger

logger = get_logger(__name__)


def _now() -> float:
    return time.time()


def _gen_id() -> str:
    return uuid.uuid4().hex


class GraphStore:
    """知识/社交图谱存储 (SQLite 持久化 + NetworkX 内存算法 + JSONL 审计/导出)。"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.db_path = os.path.join(data_dir, "graph.db")
        self.audit_path = os.path.join(data_dir, "edits_audit.jsonl")
        self._lock = asyncio.Lock()
        self._ensure_schema()

    # =================== Schema ===================
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 让 dict(row) 可用
        return conn

    def _ensure_schema(self):
        with self._conn() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, graph TEXT NOT NULL, name TEXT NOT NULL,
                    properties TEXT, source TEXT, weight REAL DEFAULT 0.3, hit_count INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 0, is_core INTEGER DEFAULT 0,
                    created_at REAL, updated_at REAL, occurred_at REAL
                );
                CREATE TABLE IF NOT EXISTS edges (
                    id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL, relation TEXT NOT NULL,
                    properties TEXT, source TEXT, weight REAL DEFAULT 0.3, hit_count INTEGER DEFAULT 0,
                    created_at REAL, occurred_at REAL,
                    FOREIGN KEY (src) REFERENCES nodes(id), FOREIGN KEY (dst) REFERENCES nodes(id)
                );
                CREATE TABLE IF NOT EXISTS blocklist (
                    id TEXT PRIMARY KEY, pattern_type TEXT NOT NULL, match_key TEXT NOT NULL,
                    reason TEXT, actor TEXT, created_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_nodes_kind ON nodes(kind);
                CREATE INDEX IF NOT EXISTS idx_nodes_graph ON nodes(graph);
                CREATE INDEX IF NOT EXISTS idx_nodes_active ON nodes(is_active);
                CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src);
                CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst);
                CREATE INDEX IF NOT EXISTS idx_edges_relation ON edges(relation);
            ''')
            # 0.2.4.0 老库迁移: 加 occurred_at 列 (事件发生时间, 区别 created_at 入库时间)
            # 先加列再建 occurred_at 索引 (老库 CREATE TABLE IF NOT EXISTS 跳过, 列未加时
            # executescript 里 CREATE INDEX 引用 occurred_at 会 OperationalError)
            self._add_column_if_missing(conn, "nodes", "occurred_at", "REAL")
            self._add_column_if_missing(conn, "edges", "occurred_at", "REAL")
            # 旧数据 occurred_at NULL -> 回填 created_at (保时间检索不漏老数据)
            conn.execute("UPDATE nodes SET occurred_at = created_at WHERE occurred_at IS NULL")
            conn.execute("UPDATE edges SET occurred_at = created_at WHERE occurred_at IS NULL")
            # occurred_at 索引: 列已存在, 新老库都安全 (放加列后, 修复老库 init 崩)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_occurred ON nodes(occurred_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_occurred ON edges(occurred_at)")
            # 0.2.5.1 ext_id: 节点外部稳定标识 (如微信号), 治昵称变就分裂节点。
            # 老库迁移加列 (_add_column_if_missing 幂等), 老数据 ext_id NULL 不回填。
            self._add_column_if_missing(conn, "nodes", "ext_id", "TEXT")
            conn.commit()

    @staticmethod
    def _add_column_if_missing(conn, table: str, col: str, coltype: str) -> None:
        """ALTER TABLE ADD COLUMN 幂等 (老库升级加列)。"""
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if col not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

    # =================== 审计 ===================
    def _audit(self, op: str, **fields):
        """append-only 编辑审计 (edits_audit.jsonl)。"""
        try:
            entry = {"ts": _now(), "op": op, **fields}
            with open(self.audit_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[GraphStore] audit 写入失败: {e}")

    # =================== 黑名单熔断 ===================
    async def _check_blocklist(self, pattern_type: str, match: dict) -> bool:
        """查黑名单是否命中。命中返回 True (拦截)。"""
        def _do():
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT match_key FROM blocklist WHERE pattern_type=?",
                    (pattern_type,)
                ).fetchall()
            for (mk_json,) in rows:
                mk = json.loads(mk_json)
                if self._block_match(pattern_type, mk, match):
                    return True
            return False
        return await asyncio.to_thread(_do)

    @staticmethod
    def _fuzzy_match(pattern: str, value: str) -> bool:
        """模糊匹配 (0.2.5.4 连坐修复): 精确 / pattern 是 value 连续子串 /
        pattern 整体是 value 的子序列 (插字衍生, LCS==len(pattern)>=3)。

        修复前是双向子串 (pattern in value or value in pattern) + LCS>=3, 两连坐:
        - 雷1: 删长名节点 ("胖脸鱼作息不规律") 后 blocklist 长模式匹配短名正主
          ("胖脸鱼"), 正主 upsert 被封锁 (blocklist 检查在 upsert 之前)。
        - 雷2: 单字 blocklist ("你"/"恁") 连坐所有含该字节点 ("你妈"/"拿豆橛子...")。
        现只保留防反弹方向 (blocklist 片段拦包含/衍生它的新节点), 禁正主方向
        (短名命中长模式); pattern 单字时只精确匹配。
        """
        if not pattern or not value:
            return False
        if pattern == value:
            return True
        if len(pattern) < 2:
            return False
        if pattern in value:
            return True
        # pattern 整体是否为 value 的子序列 (插字衍生): 真 LCS == len(pattern)
        # (标准 LCS DP; 旧实现是最长公共子串, 抓不到插字衍生如 "胖脸鱼作息[真]不规律")
        a, b = pattern, value
        prev = [0] * (len(b) + 1)
        for i in range(1, len(a) + 1):
            cur = [0] * (len(b) + 1)
            for j in range(1, len(b) + 1):
                if a[i-1] == b[j-1]:
                    cur[j] = prev[j-1] + 1
                else:
                    cur[j] = prev[j] if prev[j] >= cur[j-1] else cur[j-1]
            prev = cur
        lcs = prev[len(b)]
        return lcs >= 3 and lcs == len(a)

    def _block_match(self, pattern_type: str, mk: dict, match: dict) -> bool:
        """判定写入项是否命中黑名单条目。"""
        if pattern_type == "node":
            if mk.get("kind") and mk["kind"] != match.get("kind"):
                return False
            name_pat = mk.get("name_fuzzy", "")
            return self._fuzzy_match(name_pat, match.get("name", ""))
        elif pattern_type == "edge":
            if mk.get("relation") and mk["relation"] != match.get("relation"):
                return False
            src_pat = mk.get("src_fuzzy", "")
            dst_pat = mk.get("dst_fuzzy", "")
            src_ok = self._fuzzy_match(src_pat, match.get("src_name", "")) if src_pat else True
            dst_ok = self._fuzzy_match(dst_pat, match.get("dst_name", "")) if dst_pat else True
            return src_ok and dst_ok
        return False

    async def add_block(self, pattern_type: str, match_key: dict, reason: str, actor: str) -> dict:
        """加黑名单 (immutable, 只增不删)。actor 必为 boss (权限闸门在 main.py)。"""
        def _do():
            bid = _gen_id()
            with self._conn() as conn:
                conn.execute(
                    "INSERT INTO blocklist (id, pattern_type, match_key, reason, actor, created_at) VALUES (?,?,?,?,?,?)",
                    (bid, pattern_type, json.dumps(match_key, ensure_ascii=False), reason, actor, _now())
                )
                conn.commit()
            return bid
        bid = await asyncio.to_thread(_do)
        self._audit("add_block", pattern_type=pattern_type, match_key=match_key, reason=reason, actor=actor, bid=bid)
        logger.info(f"[GraphStore] 黑名单新增 [{pattern_type}] by {actor}: {reason}")
        return {"ok": True, "id": bid}

    # =================== 权重分级 ===================
    @staticmethod
    def _grade(source: str, is_core: bool, existing: Optional[dict]) -> Tuple[float, int, int]:
        """返回 (weight, hit_count, is_active)。机制1: 权重频次分级。"""
        if source == "master":
            return 1.0, (existing or {}).get("hit_count", 0) + 1, 1
        if existing:
            hit = existing["hit_count"] + 1
            weight = min(1.0, existing["weight"] + 0.1)
        else:
            hit = 1
            weight = 0.3
        if is_core:
            weight = max(weight, 0.8)
        is_active = 1 if (weight >= 0.6 or hit >= 3 or is_core) else 0
        return weight, hit, is_active

    # =================== 节点 ===================
    async def add_node(self, kind: str, graph: str, name: str, properties: dict = None,
                       source: str = "memory", is_core: bool = False,
                       occurred_at: Optional[float] = None,
                       ext_id: Optional[str] = None) -> dict:
        """加节点 (查黑名单 + 权重分级)。命中黑名单返回 {blocked: True}。

        occurred_at: 事件发生时间 (unix ts)。None 用当前时间。upsert 已存在取 max (最新事件时间),
        created_at 保留首次入库时间不动。0.2.4.0 时间检索用 occurred_at 切片。
        ext_id: 外部稳定标识 (如微信号)。0.2.5.1 微信号锚点: 有 ext_id 时按 ext_id+graph
        upsert 认人, 即使 name (昵称) 变了也合并到同一节点 (治昵称变就分裂)。None 走老
        name+graph 逻辑 (向后兼容)。昵称变合并时, 旧 name 追加进 properties.aliases 历史。
        """
        blocked = await self._check_blocklist("node", {"kind": kind, "name": name})
        if blocked:
            self._audit("block_intercept_node", kind=kind, name=name, source=source)
            logger.info(f"[GraphStore] 节点被黑名单拦截: {kind}/{name}")
            return {"ok": False, "blocked": True}

        def _do():
            with self._conn() as conn:
                # 0.2.5.1: 有 ext_id 优先按 ext_id+graph 认人 (昵称变也合并); 无 ext_id 走老逻辑
                if ext_id:
                    row = conn.execute(
                        "SELECT * FROM nodes WHERE ext_id=? AND graph=?", (ext_id, graph)
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM nodes WHERE name=? AND graph=?", (name, graph)
                    ).fetchone()
                existing = dict(row) if row else None
                weight, hit, is_active = self._grade(source, is_core, existing)
                # 昵称变合并: 旧 name (与当前 name 不同) 追加进 properties.aliases 历史认人
                merged_props = dict(properties or {})
                if existing and existing["name"] and existing["name"] != name:
                    try:
                        old_props = json.loads(existing.get("properties") or "{}")
                        aliases = set(old_props.get("aliases") or [])
                        aliases.add(existing["name"])
                        merged_props["aliases"] = sorted(aliases)
                        # 保留旧 properties 里非冲突字段 (display_name 等)
                        for k, v in old_props.items():
                            if k != "aliases" and k not in merged_props:
                                merged_props[k] = v
                    except Exception:
                        pass
                props = json.dumps(merged_props, ensure_ascii=False)
                now = _now()
                occ_ts = occurred_at if occurred_at is not None else now
                if existing:
                    nid = existing["id"]
                    # occurred_at 取 max (最新事件时间), created_at 不动 (保首次入库)
                    old_occ = existing.get("occurred_at") or existing.get("created_at") or now
                    new_occ = max(old_occ, occ_ts)
                    # ext_id 已存在不覆盖 (保首次锚定), name 更新为最新昵称
                    conn.execute(
                        "UPDATE nodes SET name=?, weight=?, hit_count=?, is_active=?, is_core=?, properties=?, source=?, occurred_at=?, updated_at=? WHERE id=?",
                        (name, weight, hit, is_active, 1 if is_core else existing["is_core"], props, source, new_occ, now, nid)
                    )
                else:
                    nid = _gen_id()
                    conn.execute(
                        "INSERT INTO nodes (id, kind, graph, name, ext_id, properties, source, weight, hit_count, is_active, is_core, created_at, updated_at, occurred_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (nid, kind, graph, name, ext_id, props, source, weight, hit, is_active, 1 if is_core else 0, now, now, occ_ts)
                    )
                conn.commit()
                return nid, weight, hit, is_active
        nid, weight, hit, is_active = await asyncio.to_thread(_do)
        self._audit("add_node", nid=nid, kind=kind, graph=graph, name=name, ext_id=ext_id, source=source, weight=weight, is_active=is_active)
        return {"ok": True, "id": nid, "weight": weight, "hit_count": hit, "is_active": is_active, "blocked": False}

    async def get_node(self, node_id: str) -> Optional[dict]:
        def _do():
            with self._conn() as conn:
                row = conn.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
                if not row:
                    return None
                d = dict(row)
                d["properties"] = json.loads(d.get("properties") or "{}")
                return d
        return await asyncio.to_thread(_do)

    async def query_nodes(self, kind: str = None, graph: str = None, keyword: str = None,
                          active_only: bool = False, ext_id: str = None) -> list:
        def _do():
            sql = "SELECT * FROM nodes WHERE 1=1"
            args: list = []
            if kind:
                sql += " AND kind=?"; args.append(kind)
            if graph:
                sql += " AND graph=?"; args.append(graph)
            if keyword:
                sql += " AND name LIKE ?"; args.append(f"%{keyword}%")
            if active_only:
                sql += " AND is_active=1"
            if ext_id:
                sql += " AND ext_id=?"; args.append(ext_id)
            with self._conn() as conn:
                rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(_do)

    async def set_node_ext_id(self, node_id: str, ext_id: str, force: bool = False) -> bool:
        """给已存在节点补 ext_id 锚点 (0.2.5.3 老记忆迁移用)。

        只补锚点, 不动 weight/hit/properties (区别于 add_node 的 upsert, 那会改权重)。
        幂等: 节点已有 ext_id 时 force=False 不覆盖 (保首次锚定), force=True 覆盖 (人工纠错)。
        返回 True=已更新, False=未更新 (已有 ext_id 且 force=False / 节点不存在)。
        """
        def _do():
            with self._conn() as conn:
                row = conn.execute("SELECT ext_id FROM nodes WHERE id=?", (node_id,)).fetchone()
                if not row:
                    return False
                existing = row["ext_id"]
                if existing and not force:
                    return False
                conn.execute("UPDATE nodes SET ext_id=?, updated_at=? WHERE id=?",
                             (ext_id, _now(), node_id))
                conn.commit()
                return True
        updated = await asyncio.to_thread(_do)
        if updated:
            self._audit("set_ext_id", nid=node_id, ext_id=ext_id, force=force)
        return updated

    # =================== 边 ===================
    async def add_edge(self, src_id: str, dst_id: str, relation: str, properties: dict = None,
                       source: str = "memory", is_core: bool = False,
                       occurred_at: Optional[float] = None) -> dict:
        """加边 (查黑名单 + 权重分级)。

        occurred_at: 事件发生时间。None 用当前时间。upsert 已存在取 max, created_at 不动。
        """
        src_node = await self.get_node(src_id)
        dst_node = await self.get_node(dst_id)
        if not src_node or not dst_node:
            return {"ok": False, "error": "src or dst node not found"}
        blocked = await self._check_blocklist("edge", {
            "relation": relation, "src_name": src_node["name"], "dst_name": dst_node["name"]
        })
        if blocked:
            self._audit("block_intercept_edge", src=src_id, dst=dst_id, relation=relation, source=source)
            return {"ok": False, "blocked": True}

        def _do():
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT * FROM edges WHERE src=? AND dst=? AND relation=?", (src_id, dst_id, relation)
                ).fetchone()
                existing = dict(row) if row else None
                weight, hit, _ = self._grade(source, is_core, existing)
                props = json.dumps(properties or {}, ensure_ascii=False)
                now = _now()
                occ_ts = occurred_at if occurred_at is not None else now
                if existing:
                    eid = existing["id"]
                    old_occ = existing.get("occurred_at") or existing.get("created_at") or now
                    new_occ = max(old_occ, occ_ts)
                    conn.execute(
                        "UPDATE edges SET weight=?, hit_count=?, properties=?, source=?, occurred_at=? WHERE id=?",
                        (weight, hit, props, source, new_occ, eid)
                    )
                else:
                    eid = _gen_id()
                    conn.execute(
                        "INSERT INTO edges (id, src, dst, relation, properties, source, weight, hit_count, created_at, occurred_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (eid, src_id, dst_id, relation, props, source, weight, hit, now, occ_ts)
                    )
                conn.commit()
                return eid, weight, hit
        eid, weight, hit = await asyncio.to_thread(_do)
        self._audit("add_edge", eid=eid, src=src_id, dst=dst_id, relation=relation, source=source, weight=weight)
        return {"ok": True, "id": eid, "weight": weight, "hit_count": hit, "blocked": False}

    async def query_edges(self, src: str = None, dst: str = None, relation: str = None) -> list:
        def _do():
            sql = "SELECT * FROM edges WHERE 1=1"
            args: list = []
            if src:
                sql += " AND src=?"; args.append(src)
            if dst:
                sql += " AND dst=?"; args.append(dst)
            if relation:
                sql += " AND relation=?"; args.append(relation)
            with self._conn() as conn:
                rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(_do)

    async def query_edges_by_graph(self, graph: str) -> list:
        """查指定 graph 域的边 (src/dst 都属该 graph, 严格不跨域)。

        0.2.3.0 图谱分桶: 点击情感/知识 tab 只加载对应域边, 跨域边 (如阿露玩游戏
        ->三国杀, src=social dst=knowledge) 不被任一域返回, 保证图谱干净不混淆。
        """
        def _do():
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT e.* FROM edges e "
                    "JOIN nodes ns ON e.src = ns.id "
                    "JOIN nodes nd ON e.dst = nd.id "
                    "WHERE ns.graph = ? AND nd.graph = ?",
                    (graph, graph)
                ).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(_do)

    # =================== 时间检索 (0.2.4.0) ===================
    async def query_nodes_by_time(self, start_ts: float, end_ts: float,
                                  kind: str = None, graph: str = None,
                                  active_only: bool = False) -> list:
        """查 occurred_at 在 [start_ts, end_ts) 半开区间的节点。"""
        def _do():
            sql = "SELECT * FROM nodes WHERE occurred_at>=? AND occurred_at<?"
            args: list = [start_ts, end_ts]
            if kind:
                sql += " AND kind=?"; args.append(kind)
            if graph:
                sql += " AND graph=?"; args.append(graph)
            if active_only:
                sql += " AND is_active=1"
            with self._conn() as conn:
                rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(_do)

    async def query_edges_by_time(self, start_ts: float, end_ts: float,
                                  graph: str = None) -> list:
        """查 occurred_at 在区间的边。graph 过滤时 src/dst 都属该 graph (同 query_edges_by_graph 语义)。"""
        def _do():
            if graph:
                sql = (
                    "SELECT e.* FROM edges e "
                    "JOIN nodes ns ON e.src = ns.id "
                    "JOIN nodes nd ON e.dst = nd.id "
                    "WHERE e.occurred_at>=? AND e.occurred_at<? "
                    "AND ns.graph = ? AND nd.graph = ?"
                )
                args = [start_ts, end_ts, graph, graph]
            else:
                sql = "SELECT * FROM edges WHERE occurred_at>=? AND occurred_at<?"
                args = [start_ts, end_ts]
            with self._conn() as conn:
                rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(_do)

    # =================== 修剪 (老板 UI) ===================
    async def prune_node(self, node_id: str, reason: str, actor: str,
                         block: bool = True) -> dict:
        """删节点 + 可选写黑名单 (防反弹) + audit。权限闸门在 main.py (is_master)。

        block=False 只删不封锁 (0.2.5.4 解耦): 批次修剪误识别节点用 (描述短语被
        误当人等), 这些名字不是"永远禁止"只是"当前不该是节点", 避免 blocklist
        膨胀; 真垃圾 (单字母/引号实体) 仍用默认 block=True 防反弹。
        """
        node = await self.get_node(node_id)
        if not node:
            return {"ok": False, "error": "node not found"}
        if block:
            await self.add_block("node", {"kind": node["kind"], "name_fuzzy": node["name"]},
                                 f"prune: {reason}", actor)

        def _do():
            with self._conn() as conn:
                conn.execute("DELETE FROM edges WHERE src=? OR dst=?", (node_id, node_id))
                conn.execute("DELETE FROM nodes WHERE id=?", (node_id,))
                conn.commit()
        await asyncio.to_thread(_do)
        self._audit("prune_node", nid=node_id, name=node["name"], reason=reason, actor=actor)
        logger.info(f"[GraphStore] 节点修剪 [{node['name']}] by {actor}: {reason}")
        return {"ok": True}

    async def prune_edge(self, edge_id: str, reason: str, actor: str) -> dict:
        def _do():
            with self._conn() as conn:
                row = conn.execute("SELECT * FROM edges WHERE id=?", (edge_id,)).fetchone()
                return dict(row) if row else None
        edge = await asyncio.to_thread(_do)
        if not edge:
            return {"ok": False, "error": "edge not found"}
        src = await self.get_node(edge["src"])
        dst = await self.get_node(edge["dst"])
        await self.add_block("edge", {
            "relation": edge["relation"],
            "src_fuzzy": src["name"] if src else "",
            "dst_fuzzy": dst["name"] if dst else "",
        }, f"prune: {reason}", actor)

        def _do2():
            with self._conn() as conn:
                conn.execute("DELETE FROM edges WHERE id=?", (edge_id,))
                conn.commit()
        await asyncio.to_thread(_do2)
        self._audit("prune_edge", eid=edge_id, reason=reason, actor=actor)
        return {"ok": True}

    # =================== NetworkX 加载 ===================
    async def load_to_networkx(self, active_only: bool = True) -> nx.DiGraph:
        """加载到 NetworkX 内存图 (供路径推理/社区检测)。默认只加载 is_active 节点。"""
        def _do():
            G = nx.DiGraph()
            with self._conn() as conn:
                node_sql = "SELECT * FROM nodes" + (" WHERE is_active=1" if active_only else "")
                for row in conn.execute(node_sql).fetchall():
                    d = dict(row)
                    G.add_node(d["id"], kind=d["kind"], graph=d["graph"], name=d["name"],
                               weight=d["weight"], hit_count=d["hit_count"], is_core=d["is_core"])
                for row in conn.execute("SELECT * FROM edges").fetchall():
                    d = dict(row)
                    if d["src"] in G and d["dst"] in G:
                        G.add_edge(d["src"], d["dst"], relation=d["relation"], weight=d["weight"])
            return G
        return await asyncio.to_thread(_do)

    # =================== 训练数据导出 ===================
    async def export_training_pairs(self, path: str) -> int:
        """导出 training_pairs.jsonl (节点属性/关系 QA)。返回条数。"""
        G = await self.load_to_networkx(active_only=True)
        pairs: list = []
        for nid, attrs in G.nodes(data=True):
            name = attrs.get("name", "")
            kind = attrs.get("kind", "")
            pairs.append({"instruction": f"{name}是什么?", "output": f"{name}是{kind}节点"})
            if attrs.get("is_core"):
                pairs.append({"instruction": f"{name}的核心属性?",
                              "output": json.dumps(attrs, ensure_ascii=False)})
        for src, dst, eattrs in G.edges(data=True):
            src_name = G.nodes[src].get("name", "")
            dst_name = G.nodes[dst].get("name", "")
            rel = eattrs.get("relation", "")
            pairs.append({"instruction": f"{src_name}和{dst_name}什么关系?", "output": rel})

        def _do():
            with open(path, "w", encoding="utf-8") as f:
                for p in pairs:
                    f.write(json.dumps(p, ensure_ascii=False) + "\n")
            return len(pairs)
        n = await asyncio.to_thread(_do)
        logger.info(f"[GraphStore] 训练数据导出 {n} 条 -> {path}")
        return n

    # =================== 统计 ===================
    async def stats(self) -> dict:
        def _do():
            with self._conn() as conn:
                nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
                edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
                active = conn.execute("SELECT COUNT(*) FROM nodes WHERE is_active=1").fetchone()[0]
                blocked = conn.execute("SELECT COUNT(*) FROM blocklist").fetchone()[0]
            return {"nodes": nodes, "edges": edges, "active_nodes": active, "blocklist": blocked}
        return await asyncio.to_thread(_do)

    async def time_range(self, graph: str = None) -> dict:
        """返 occurred_at 的 [min, max] (供前端 QDateEdit 定日期区间)。空库返 {min:0,max:0}。"""
        def _do():
            with self._conn() as conn:
                if graph:
                    sql = "SELECT MIN(occurred_at), MAX(occurred_at) FROM nodes WHERE graph=?"
                    row = conn.execute(sql, (graph,)).fetchone()
                else:
                    row = conn.execute("SELECT MIN(occurred_at), MAX(occurred_at) FROM nodes").fetchone()
            mn, mx = row[0], row[1]
            return {"min_ts": mn or 0.0, "max_ts": mx or 0.0}
        return await asyncio.to_thread(_do)
