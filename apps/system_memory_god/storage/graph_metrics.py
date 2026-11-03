# -*- coding: utf-8 -*-
"""apps/system_memory_god/storage/graph_metrics.py -- 图谱可视化富化纯函数 (0.2.21.0)。

无 IO / 确定性 / 高覆盖。供 graph_query 在 top-N 剪枝后、序列化前调用:
- enrich_graph: networkx 现算 degree + 社群 (label_propagation, 回退 connected_components),
  并产出 communities 摘要 (id/label/count; label=簇内度数最高节点, 并列取名字典序最小)。
- merge_social_axes: 把 entity socials 的 favorability/attention 按 name 合进节点 (键 fav/att)。

守不可变: 一律返回新 dict, 不就地改入参。守 capability-generic-not-coupled: 仅做图指标计算,
不碰存储/总线。networkx 已在 runtime_env (3.4.2), 零新增依赖。
"""
from __future__ import annotations

from collections import defaultdict

import networkx as nx


def enrich_graph(nodes: list, edges: list) -> tuple:
    """返回 (enriched_nodes, communities_summary)。

    enriched_nodes = [{**n, "degree": int, "community": int}, ...] (顺序同入参)
    communities_summary = [{"id","label","count"}, ...] 按 count 降序。
    空入参 -> ([], [])。
    """
    if not nodes:
        return [], []
    ids = [n["id"] for n in nodes]
    kept = set(ids)
    g = nx.Graph()
    g.add_nodes_from(sorted(kept))  # 确定序, 稳定社群编号
    for e in edges:
        s, d = e.get("src"), e.get("dst")
        if s in kept and d in kept:
            g.add_edge(s, d)
    degree = {nid: int(g.degree(nid)) for nid in ids}
    try:
        comps = [set(c) for c in nx.community.label_propagation_communities(g)]
    except Exception:
        comps = [set(c) for c in nx.connected_components(g)]
    if not comps:
        comps = [{nid} for nid in ids]
    id2cid: dict = {}
    for cid, members in enumerate(comps):
        for m in members:
            id2cid[m] = cid
    for nid in ids:  # 兜底: 漏网节点自成社群
        id2cid.setdefault(nid, len(id2cid))
    enriched = [{**n, "degree": degree[n["id"]], "community": id2cid[n["id"]]} for n in nodes]
    by_cid: dict = defaultdict(list)
    for n in enriched:
        by_cid[n["community"]].append(n)
    summary = []
    for cid, members in by_cid.items():
        top = sorted(members, key=lambda n: (-n["degree"], n.get("name", "")))[0]
        summary.append({"id": cid, "label": top.get("name", "?"), "count": len(members)})
    summary.sort(key=lambda c: (-c["count"], c["id"]))
    return enriched, summary


def merge_social_axes(nodes: list, socials: list) -> list:
    """按 name 把 socials 的 favorability/attention 合进节点 (键 fav/att); 未命中不加键。

    返回新 list, 不就地改入参。socials 空 -> 返回入参节点的浅拷贝 (无 fav/att)。
    """
    axes = {
        s.get("name"): (s.get("favorability"), s.get("attention"))
        for s in (socials or [])
        if s.get("name")
    }
    out: list = []
    for n in nodes:
        nd = {**n}
        pair = axes.get(n.get("name"))
        if pair is not None and pair[0] is not None:
            nd["fav"] = pair[0]
            nd["att"] = pair[1] if pair[1] is not None else 50
        out.append(nd)
    return out
