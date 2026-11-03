# kernel/memory_view.py
"""
[0.3.23.0 记忆中心多智能体适配] 只读记忆窗口 (记忆选择的后端读侧)。

老板拍板: 记忆中心加智能体记忆选择器, 只读浏览任意智能体记忆。活动智能体
走现有活体 system_memory_god 链路 (行为零变化); 非活动智能体经本模块开
只读窗口。与 kernel/memory_snapshot.py (写/拷贝侧) 成读侧姊妹, 同层同范式
(kernel 直读记忆根, 不 import apps, 守 capability-generic-not-coupled)。

铁律 (只读窗口严禁写副作用):
- record_hit (实体/规则/向量 hit_count)、journal、edits_audit 一律不碰;
- 全程零 makedirs/零文件创建 (chroma 走 isdir 守卫, fresh 智能体无库返空);
- graph.db 走 sqlite ro URI 连接 (绝不走 GraphStore 构造, 其 _ensure_schema
  会写库迁移);
- 全方法 fail-safe: 读取异常 logger.warning + 返空值, 永不抛。

镜像源 (改动须同步, 防语义漂移):
- matches_entity <- apps/system_memory_god/main.py SystemMemory._matches_entity
  (活体将薄委托到本函数, 同源一份实现)。
- top_rules_text <- storage/rule_store.py get_active_rules_text 的 _score
  (饱和频次 hits/(hits+5) + 0.5*exp(-dt/7d) 时效衰减)。
- spotlight 行格式 <- main.py spotlight_search ("- [注意] 关于【X】的k：v",
  core 事实优先, 规则段 Top-8)。
- archival <- storage/file_io.py deep_archive_search (journal_*.txt 流式 grep)。

调用方: web/routes_console.py (search/overview 非活体分流, IO 经
asyncio.to_thread); 测试见 tests/test_memory_view.py。
"""
import glob
import json
import math
import os
import re
import sqlite3
import threading
import time
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

COLLECTION_NAME = "memory_stream"
_HALF_LIFE_SECONDS = 7 * 24 * 3600  # 规则时效衰减半衰期, 镜像 rule_store

# [窗口缓存] normcase(root) -> ReadonlyMemoryView; 本地单用户智能体数量个位数,
# 无淘汰策略 (chroma 自身亦按 path 缓存实例)。
_VIEW_CACHE: dict = {}
_VIEW_LOCK = threading.Lock()


def matches_entity(entity_name: str, text: str) -> bool:
    """[同源镜像] SystemMemory._matches_entity 词边界匹配 (活体薄委托到此)。

    - 单字实体 (len<2) 一律拒匹配, 阻断单字污染全局记忆;
    - 拉丁/数字实体名: 强制词边界, 防匹配进更长单词内部;
    - CJK/混合实体名: 子串匹配 (中文无词概念, 子串是合理语义匹配)。
    """
    if not entity_name or len(entity_name) < 2:
        return False
    if re.search(r'[A-Za-z0-9]', entity_name):
        return bool(re.search(rf'\b{re.escape(entity_name)}\b', text))
    return entity_name in text


def top_rules_text(rules: list, limit: int = 8) -> str:
    """[镜像 rule_store.get_active_rules_text] 评分取 Top-N, 返回 "- text" 行。

    评分公式与活体完全一致: hit_freq = hits/(hits+5) 饱和归一 (Smallville),
    recency = exp(-(now-last_active_at)/7d), score = hit_freq + 0.5*recency。
    只读侧不 record_hit, 纯读排序。
    """
    if not rules:
        return ""
    # [审查整改 L3] 非 dict 元素过滤, 防 AttributeError 连累整段
    rules = [r for r in rules if isinstance(r, dict)]
    if not rules:
        return ""
    now = time.time()

    def _score(r: dict) -> float:
        hits = float(r.get("hit_count", 0) or 0)
        hit_freq = hits / (hits + 5.0)
        recency = 0.0
        last = r.get("last_active_at")
        if last:
            try:
                recency = math.exp(
                    -(now - time.mktime(time.strptime(last, "%Y-%m-%d %H:%M:%S")))
                    / _HALF_LIFE_SECONDS)
            # [审查整改 L2] OSError: 超范围日期 mktime 在 Windows 抛
            except (ValueError, TypeError, OverflowError, OSError):
                recency = 0.0  # 旧数据/格式异常回退, 不报错
        return hit_freq + 0.5 * recency

    sorted_rules = sorted(rules, key=_score, reverse=True)
    top = sorted_rules[:limit] if limit and limit > 0 else sorted_rules
    return "\n".join(f"- {r.get('text', '')}" for r in top)


def _open_collection(db_path: str):
    """打开休眠库 chroma 集合 (只读用途)。测试可 monkeypatch 本函数。

    chromadb/嵌入服务延迟导入: 仅在真有库要开时才碰重依赖。
    """
    import chromadb
    from chromadb.config import Settings
    from kernel.embedding_service import LazyBgeM3EmbeddingFunction
    client = chromadb.PersistentClient(
        path=db_path,
        settings=Settings(anonymized_telemetry=False),
    )
    return client.get_collection(
        name=COLLECTION_NAME,
        embedding_function=LazyBgeM3EmbeddingFunction(),
    )


class ReadonlyMemoryView:
    """一个非活动智能体记忆根的只读窗口。构造不触盘 (懒连接), 全方法 fail-safe。"""

    def __init__(self, root: str):
        self.root = root
        # [审查整改 C1] 真实记忆根两层布局 (镜像活体 main.py:140-141):
        # graph.db 在 root 层 (GraphStore 用 graph_data_dir=root);
        # entities/rules/journal/smallville_db 全在 root/soul (data_dir=root/soul)。
        self.soul = os.path.join(root, "soul")

    # ---------- 基础读取 ----------

    def _path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def _soul_path(self, name: str) -> str:
        return os.path.join(self.soul, name)

    def _load_json(self, name: str) -> dict:
        # entities.json / rules.json 均在 soul 层 (审查整改 C1)
        path = self._soul_path(name)
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"[memory_view] {name} 读取失败 (非致命): {e}")
            return {}

    # ---------- 图谱统计 (sqlite ro) ----------

    def graph_stats(self) -> dict:
        """镜像 GraphStore.stats 四计数; 缺库/坏库全零 (键名一致)。"""
        zeros = {"nodes": 0, "edges": 0, "active_nodes": 0, "blocklist": 0}
        path = self._path("graph.db")
        if not os.path.isfile(path):
            return zeros
        try:
            # ro URI: Windows 盘符路径转正斜杠; 只读连接绝不触发 schema 迁移。
            uri = "file:///" + os.path.abspath(path).replace("\\", "/") + "?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            try:
                nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
                edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
                active = conn.execute(
                    "SELECT COUNT(*) FROM nodes WHERE is_active=1").fetchone()[0]
                blocked = conn.execute("SELECT COUNT(*) FROM blocklist").fetchone()[0]
            finally:
                conn.close()
            return {"nodes": nodes, "edges": edges,
                    "active_nodes": active, "blocklist": blocked}
        except Exception as e:
            logger.warning(f"[memory_view] graph.db 只读统计失败 (非致命): {e}")
            return zeros

    # ---------- spotlight (entities.json + rules Top-8) ----------

    def spotlight(self, text: str) -> str:
        """镜像活体 spotlight_search 输出, record_hit 归零。"""
        try:
            entities = self._load_json("entities.json")
            core_lines = []
            normal_lines = []
            for entity_name, attributes in entities.items():
                if not matches_entity(entity_name, text):
                    continue
                if not isinstance(attributes, dict):
                    continue
                for k, v in attributes.items():
                    if isinstance(v, dict):
                        value_text = v.get("value", str(v))
                        is_core = bool(v.get("is_core_fact", False))
                    else:
                        value_text = str(v)
                        is_core = False
                    line = f"- [注意] 关于【{entity_name}】的{k}：{value_text}"
                    (core_lines if is_core else normal_lines).append(line)
            lines = core_lines + normal_lines
            rules = self._load_json("rules.json").get("rules", [])
            rules_text = top_rules_text(rules, limit=8)
            if rules_text:
                lines.append(
                    "- 潜意识演化出的性格规矩 (按重要性取高频 Top-8)：\n"
                    f"{rules_text}")
            return "\n".join(lines) if lines else ""
        except Exception as e:
            logger.warning(f"[memory_view] spotlight 失败 (非致命): {e}")
            return ""

    # ---------- archival (journal_*.txt 流式 grep) ----------

    def archival(self, keyword: str, limit: int = 10) -> str:
        """镜像 file_io.deep_archive_search: 倒序扫 journal, 命中行带日期前缀。"""
        if not keyword:
            return ""
        try:
            # [审查整改 C1] journal 在 soul 层
            files = sorted(
                glob.glob(os.path.join(self.soul, "journal_*.txt")), reverse=True)
            results = []
            for file_path in files:
                date_str = os.path.basename(file_path).replace(
                    "journal_", "").replace(".txt", "")
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if keyword in line:
                                results.append(f"[{date_str}] {line.strip()}")
                                if len(results) >= limit:
                                    return "冷数据档案:\n" + "\n".join(results)
                except (UnicodeDecodeError, PermissionError, OSError) as e:
                    logger.warning(f"[memory_view] 读取 {file_path} 失败: {e}")
                    continue
            return ("冷数据档案:\n" + "\n".join(results)) if results else ""
        except Exception as e:
            logger.warning(f"[memory_view] archival 失败 (非致命): {e}")
            return ""

    # ---------- vector_search (chroma cosine top-K 直出) ----------

    def vector_search(self, query: str, n: int = 10) -> list:
        """只读向量检索: cosine top-K 直出文档列表。

        刻意不做: Smallville 重排 (rerank 在 apps 层, kernel 禁反向 import;
        UI 浏览场景排序差异可接受) / ACL 过滤 (老板控制台全览, 等价
        asker_is_master=True + prefer_chat="" 放行语义) / hit 递增。
        fresh 智能体无 smallville_db 目录 -> 直接返空, 绝不建目录。

        [审查整改 H1 如实注明] chroma PersistentClient 以 RW 打开 chroma.sqlite3:
        迁移表齐时 no-op, 但旧版本生成的休眠库打开可能触发迁移写 -- 严格意义
        非纯只读。守卫收紧为 isdir + chroma.sqlite3 存在才开; 当前生产路由未接
        本方法 (spotlight/archival/graph/rules/interests 已覆盖 UI), 接前须重审。
        """
        if not query:
            return []
        # [审查整改 C1] smallville_db 在 soul 层; [H1] 守卫收紧防迁移写
        db_path = self._soul_path("smallville_db")
        if not (os.path.isdir(db_path)
                and os.path.isfile(os.path.join(db_path, "chroma.sqlite3"))):
            return []
        try:
            collection = _open_collection(db_path)
            res = collection.query(
                query_texts=[query], n_results=n,
                include=["documents", "metadatas"])
            docs = (res.get("documents") or [[]])[0] if res else []
            return [str(d) for d in docs]
        except Exception as e:
            logger.warning(f"[memory_view] vector_search 失败 (非致命): {e}")
            return []

    # ---------- interests / rules ----------

    def interests_text(self, top_n: int = 8) -> str:
        """兴趣 top-N; 文案通用化 ("兴趣 top%d", 不焊阿露名, 多角色复用)。"""
        try:
            entities = self._load_json("entities.json")
            entity = entities.get("阿露兴趣") or {}
            rows = []
            for topic, rec in entity.items():
                if not isinstance(rec, dict):
                    continue
                try:
                    rows.append((float(rec.get("value", 0)), topic))
                except (TypeError, ValueError):
                    continue
            rows.sort(key=lambda r: r[0], reverse=True)
            top = rows[:top_n]
            if not top:
                # [审查整改 L1] 空态文案对齐活体 list_interests
                return "暂无兴趣记录。"
            return "兴趣 top%d: %s" % (
                len(top), ", ".join(f"{t}({v:g})" for v, t in top))
        except Exception as e:
            logger.warning(f"[memory_view] interests_text 失败 (非致命): {e}")
            return ""

    def rules_list(self) -> list:
        """rules.json 原样 list (镜像 rule_store.get_all_rules 返 data["rules"])。"""
        rules = self._load_json("rules.json").get("rules", [])
        return rules if isinstance(rules, list) else []


def get_view(root: str) -> ReadonlyMemoryView:
    """按 normcase 绝对路径缓存窗口实例; 构造不触盘。"""
    key = os.path.normcase(os.path.abspath(root))
    with _VIEW_LOCK:
        view = _VIEW_CACHE.get(key)
        if view is None:
            view = ReadonlyMemoryView(root)
            _VIEW_CACHE[key] = view
        return view
