import os
# 强制从系统底层屏蔽 ChromaDB 遥测
os.environ["ANONYMIZED_TELEMETRY"] = "False"
os.environ["CHROMA_TELEMETRY_IMPL"] = "None"
os.environ["CHROMA_DISABLE_TELEMETRY"] = "True"
import logging
import time
import math
import threading
import uuid
import chromadb
from chromadb.config import Settings

from kernel.logger import get_logger

logger = get_logger(__name__)


# =================== Smallville 复合重排: 纯函数 (可离线工程化测试) ===================
# 抽出到模块级, 让 eval harness 无需 ChromaDB/bge-m3 即可验证重排质量。
def compute_smallville_score(
    meta: dict, now: float,
    alpha: float = 1.0, beta: float = 1.5, gamma: float = 0.8,
    half_life_sec: float = 7 * 24 * 3600,
    delta_fav: float = 0.01, delta_att: float = 0.005,
    social_baseline: float = 50.0,
    cosine_sim: float | None = None,
    social_relevance_gate: float = 0.5,
    non_master_social_zero: bool = True,
    chat_match: bool = False,
    chat_boost: float = 0.3,
    breakdown_out: dict | None = None,
) -> float:
    """
    单条记忆的复合得分 (v0.6.0 加双轴社交项; F1 加相关性门控):
      Score = alpha*recency + beta*importance + gamma*hit_freq + social_term
      - recency:    created_at 距 now 的指数衰减 (半衰期 half_life_sec)
      - importance: 0~1 重要性
      - hit_freq:   饱和归一化的历史命中频次 hits/(hits+5)
      - social_term: 双轴社交项 delta_fav*favorability + delta_att*attention
        [F1 相关性门控] 仅当 cosine_sim >= social_relevance_gate 时才享受社交浮顶;
        cosine_sim < gate (与 query 话题无关) -> social_term=0, 防胖脸鱼无关记忆凭社交
        权重压过路人相关记忆 (吃饭->胖脸鱼 bug). cosine_sim=None (无 distances) -> 不门控,
        保 0.1.7.0 胖脸鱼相关记忆浮顶设计向后兼容。
      胖脸鱼(85/90) 相关记忆 -> +1.3 浮顶; 无关记忆 -> 门控归零沉底; 路人(50/50)中性。
    纯函数, 无副作用, 供 eval harness 离线断言排序质量。
    """
    elapsed = max(now - float(meta.get("created_at", now)), 0.0)
    recency = math.exp(-elapsed / half_life_sec)
    importance = float(meta.get("importance", 0.5))
    hits = float(meta.get("hit_count", 0))
    hit_freq = hits / (hits + 5.0)
    # [F1] 相关性门控社交加权: 无关候选 (cosine_sim<gate) 社交项归零, 防跨话题浮顶.
    if cosine_sim is not None and cosine_sim < social_relevance_gate:
        social_term = 0.0
    else:
        fav = float(meta.get("favorability", social_baseline))
        att = float(meta.get("attention", social_baseline))
        social_term = delta_fav * fav + delta_att * att
    # [阶段3-B] 第三人 obs 降权: is_master=False 候选 social_term 归零, 防第三人写的
    # obs 凭社交浮顶压过老板真实记忆 (豆橛子复发: 豆橛子是好哥们男覆盖原pj女)。
    # 老数据无 is_master 字段默认 True 不降权 (不误伤历史老板记忆); importance/recency/
    # hit 不变 (语义相关性还在, 只不社交浮顶). non_master_social_zero=False 可回退。
    if non_master_social_zero and not meta.get("is_master", True):
        social_term = 0.0
    # [0.2.10.4 P1 串台隔离] 同群提权: 召回时 prefer_chat 与 obs 存储的 chat_name
    # 匹配 -> 加性 boost, 让本群近期流水账优先浮顶, 跨群旧话题自然降序。
    # 加性 (非乘性): cosine 距离差 >chat_boost 才能翻转, 不会把无关本群记忆硬抬过
    # 高相关跨群记忆。老数据无 chat_name -> chat_match=False, 中性不影响。
    chat_term = chat_boost if chat_match else 0.0
    # [0.3.30.3 G17] 分项透出 (加权贡献值): 供 RecallPlan/检查器展示
    # "为什么想起这条" 的真实分数分解, 不再显示恒 0 假分数。
    if breakdown_out is not None:
        breakdown_out.update({
            "recency": alpha * recency,
            "importance": beta * importance,
            "hit_freq": gamma * hit_freq,
            "social": social_term,
            "same_chat": chat_term,
        })
    return alpha * recency + beta * importance + gamma * hit_freq + social_term + chat_term


def rerank_candidates(
    ids: list, docs: list, metadatas: list, now: float,
    n_results: int = 5,
    alpha: float = 1.0, beta: float = 1.5, gamma: float = 0.8,
    half_life_sec: float = 7 * 24 * 3600,
    delta_fav: float = 0.01, delta_att: float = 0.005,
    social_baseline: float = 50.0,
    distances: list | None = None,
    social_relevance_gate: float = 0.5,
    query: str | None = None,
    non_master_social_zero: bool = True,
    prefer_chat: str = "",
    attach_breakdown: bool = False,
):
    """
    对已召回的候选集执行 Smallville 复合重排, 返回 (top_ids, top_docs, top_metas)。
    不触碰向量库, 纯计算 -> 可在 eval harness 中用合成语料验证「近期+重要+高频」优先。

    [F1] distances (可选): ChromaDB cosine 距离列表 (1 - cosine_sim), 供相关性门控社交加权.
    传入则对 cosine_sim < social_relevance_gate 的候选社交项归零; 不传 (None) 则不门控,
    保向后兼容 (query_memory 之外的调用方/老 eval 用例).

    [HITL v0.1.9.10] query (可选): 仅用于 DEBUG 日志标注当前检索词, 不参与打分;
    传入则 data/logs/aoao.log 可见每条候选的 score/cosine/是否被门控, 供 F1 真机验证.
    """
    if not ids:
        return [], [], []
    scored = []
    for i in range(len(ids)):
        meta = metadatas[i] if i < len(metadatas) else {}
        # [F1] cosine_sim = 1 - cosine_distance; distances 缺失/越界/异常 -> None (不门控)
        cosine_sim = None
        if distances is not None and i < len(distances):
            try:
                cosine_sim = 1.0 - float(distances[i])
            except (TypeError, ValueError):
                cosine_sim = None
        # [0.2.10.4 P1] 同群提权: obs 存储的 chat_name 与当前召回 prefer_chat 匹配
        # -> chat_match=True -> compute_smallville_score 加 chat_boost。
        # 老数据无 chat_name (.get 默认"") != prefer_chat -> 不 boost, 中性。
        _obs_chat = str(meta.get("chat_name", "") or "")
        _chat_match = bool(prefer_chat) and _obs_chat == prefer_chat
        # [0.3.30.3 G17] attach_breakdown: 收集分项加权贡献 (semantic=cosine_sim),
        # 随 top_metas 以 _score_breakdown 键透出, 供检查器展示真实分数分解。
        _bd: dict | None = {} if attach_breakdown else None
        score = compute_smallville_score(
            meta, now, alpha=alpha, beta=beta, gamma=gamma, half_life_sec=half_life_sec,
            delta_fav=delta_fav, delta_att=delta_att, social_baseline=social_baseline,
            cosine_sim=cosine_sim, social_relevance_gate=social_relevance_gate,
            non_master_social_zero=non_master_social_zero,
            chat_match=_chat_match,
            breakdown_out=_bd,
        )
        if _bd is not None:
            _bd["semantic"] = cosine_sim if cosine_sim is not None else 0.0
            _bd["total"] = score
        scored.append((score, i, cosine_sim, _bd))
    scored.sort(key=lambda x: x[0], reverse=True)
    # [HITL 可观测性 v0.1.9.10] 重排候选打分明细, 供 F1 相关性门控真机验证:
    # data/logs/aoao.log (DEBUG) 可见 cosine<gate 的胖脸鱼无关记忆社交项归零沉底 (gated),
    # cosine>=gate 的相关记忆仍享社交浮顶 (social). 纯函数加日志, 不触向量库不改返回值.
    if logger.isEnabledFor(logging.DEBUG):
        _lines = [
            f"[rerank] query={query!r} n={n_results} gate={social_relevance_gate:.2f} cand={len(scored)}"
        ]
        for rank, (sc, i, cos, _bd0) in enumerate(scored):
            meta = metadatas[i] if i < len(metadatas) else {}
            doc = (docs[i] if i < len(docs) else "")[:60].replace("\n", " ")
            cos_s = f"{cos:.2f}" if cos is not None else "n/a"
            gated = "gated" if (cos is not None and cos < social_relevance_gate) else "social"
            speaker = meta.get("speaker", "n/a")
            fav = meta.get("favorability", "-")
            att = meta.get("attention", "-")
            _lines.append(
                f"  #{rank + 1} score={sc:.2f} cos={cos_s} {gated} {speaker} fav{fav}/att{att} | {doc}"
            )
        logger.debug("\n".join(_lines))
    top_slice = scored[:n_results]
    top_ids = [ids[i] for _, i, _, _ in top_slice]
    top_docs = [docs[i] if i < len(docs) else "" for _, i, _, _ in top_slice]
    top_metas = []
    for _, i, _, _bd in top_slice:
        m = dict(metadatas[i]) if i < len(metadatas) else {}
        if attach_breakdown and _bd is not None:
            m["_score_breakdown"] = dict(_bd)
        top_metas.append(m)
    return top_ids, top_docs, top_metas


class VectorStore:
    # 语义去重阈值：余弦相似度 > 0.85 视为重复，跳过插入
    DEDUP_COSINE_THRESHOLD = 0.85

    # =================== Smallville 复合重排参数 ===================
    # Score = ALPHA * recency + BETA * importance + GAMMA * hit_freq
    # 底层 ChromaDB cosine 相似度已作为「相关性召回门槛」负责筛出候选集,
    # 此重排公式在此基础上叠加 时效/重要/频次 三个维度, 实现多维度加权召回。
    RERANK_ALPHA = 1.0                # 时效衰减权重
    RERANK_BETA = 1.5                 # 重要度权重 (记忆重要性 metadata, 0~1)
    RERANK_GAMMA = 0.8                # 历史命中频次权重
    RERANK_CANDIDATE_FACTOR = 4       # 候选召回倍数 (最终 n_results * 此倍数)
    RECENCY_HALF_LIFE_SEC = 7 * 24 * 3600  # 时效指数衰减半衰期: 7 天
    # v0.6.0 双轴社交重排权重: fav*0.01 + att*0.005 (胖脸鱼85/90 -> +1.3, 占比~28%, 可调)
    RERANK_DELTA_FAV = 0.01
    RERANK_DELTA_ATT = 0.005
    SOCIAL_BASELINE = 50  # 老数据/无 speaker 双轴默认值 (中性, 不浮顶也不沉底)
    # [F1] 社交浮顶相关性门控阈值: cosine_sim >= 此值的候选才享受双轴社交加权
    # (胖脸鱼相关记忆仍浮顶, 保 0.1.7.0 设计); 低于此值的无关候选社交项归零,
    # 防"吃饭"召回时胖脸鱼无关记忆凭社交权重压过路人相关记忆. bge-m3 cosine 0.5 约为
    # "话题相关"下限, 可调. query_memory 透传 distances 时生效.
    RERANK_SOCIAL_RELEVANCE_GATE = 0.5
    # [阶段3-B] 第三人 obs 社交降权开关: is_master=False 候选 social_term 归零,
    # 防豆橛子复发 (第三人写的 obs 凭社交浮顶压过老板真记忆). False 可回退保 0.1.12.5 行为.
    NON_MASTER_SOCIAL_ZERO = True

    # 白名单前缀：包含系统消息、外部感知等不应被去重拦截的内容
    DEDUP_WHITELIST_PREFIXES = ["[外部感知", "向你发送了一张图片", "向你发送了一个文件", "[系统提醒触发]", "阿露，我刚给你发了一个文件"]

    def __init__(self, data_dir: str):
        self.db_path = os.path.join(data_dir, "smallville_db")
        self.client = None
        self.collection = None
        self._embedding_function = None
        # 串行化 metadata 读改写 (R8), 防 promote 与 _increment_hits 并发覆盖丢更新。
        # VectorStore 方法是同步的 (经 asyncio.to_thread 调), 故用 threading 锁而非 asyncio.Lock。
        # [0.2.23.7 review H2] Lock -> RLock: add/query 整段持锁与 _probe_and_migrate
        # 的集合换绑互斥 (治迁移竞态丢数据), _increment_hits 锁内再入锁需可重入。
        self._meta_lock = threading.RLock()

    def _is_whitelisted(self, text: str) -> bool:
        """命中白名单的文本跳过查重，直接放行。"""
        for prefix in self.DEDUP_WHITELIST_PREFIXES:
            if prefix in text:
                return True
        return False

    def connect(self):
        # 实例化时再次从 Settings 层面彻底关闭遥测
        os.makedirs(self.db_path, exist_ok=True)
        self.client = chromadb.PersistentClient(
            path=self.db_path,
            settings=Settings(anonymized_telemetry=False)
        )
        # [0.2.23.3 性能刀3] 惰性嵌入: 旧实现此处同步加载 bge-m3 ~560MB (实测 ~10s)
        # + probe 首嵌 (~3s), 卡死 boot 13s。chroma 建集合不需要模型 (add/query 才嵌),
        # 改挂 LazyBgeM3EmbeddingFunction 毫秒级就绪, 真模型由 warmup_embeddings()
        # 后台预热; 预热未完的首次向量调用阻塞至就绪 (一次性栅栏, 不丢记忆)。
        from kernel.embedding_service import LazyBgeM3EmbeddingFunction
        self._lazy_ef = LazyBgeM3EmbeddingFunction()
        self._embedding_function = self._lazy_ef

        self.collection = self._get_or_create_collection()

    def warmup_embeddings(self) -> None:
        """[0.2.23.3 刀3] 后台预热 (同步方法, 经 asyncio.to_thread 调):

        加载全局 bge-m3 单例 (与 knowledge_base 共享) -> probe 首嵌验证 ->
        维度不匹配则迁移重建。on_connect 侧 create_task 调度, 不阻塞 boot。
        """
        self._lazy_ef.warmup()
        self._probe_and_migrate()

    def embeddings_ready(self) -> bool:
        """[0.2.23.7 review H1] 嵌入模型是否已就绪 (非阻塞, 供调用方快速判定)。"""
        return self._lazy_ef.warmed()

    def wait_embeddings_ready(self, timeout: float | None = None) -> bool:
        """[0.2.23.7 review H1] 同步阻塞至就绪 (仅限非事件循环线程, 经 to_thread)。"""
        return self._lazy_ef.wait_until_ready(timeout)

    def _get_or_create_collection(self):
        """获取或创建集合 (无 probe, 秒回); 维度迁移推迟到 warmup 探针。"""
        try:
            return self.client.get_collection(
                name="memory_stream",
                embedding_function=self._embedding_function,
            )
        except Exception as e:
            err = str(e).lower()
            # 全新空库: 集合不存在, 正常创建
            if "does not exist" in err or "not exist" in err:
                logger.info("[VectorStore] memory_stream 集合不存在, 首次创建。")
                return self.client.create_collection(
                    name="memory_stream",
                    embedding_function=self._embedding_function
                )
            raise

    def _probe_and_migrate(self) -> None:
        """[0.2.23.3 刀3] 模型就绪后首嵌探针: 维度不匹配删旧重建 (罕见路径)。

        旧实现在 connect 同步做, 卡 boot; 现移到后台预热。集合换绑过 _meta_lock
        防与并发 metadata 更新竞态。探针失败 (非维度) 仅告警不致命。
        """
        try:
            self.collection.query(query_texts=["test_probe"], n_results=1)
        except Exception as e:
            err = str(e).lower()
            if "dimension" in err or "dimensionality" in err:
                # [0.3.13.0 P0 架构审查整改] 维度不匹配绝不删库: 记忆是不可再生数据,
                # 运行时降级不得触发数据销毁 (旧实现 delete+create 重建, 审查报告列为
                # 不可接受的数据生命周期策略)。保留旧集合原样, 后续 add/query 各自报错
                # 降级; ERROR 告警待人工介入 (备份 chroma 目录 -> 新维度集合重嵌入,
                # 版本化迁移留 P4)。
                logger.error(
                    f"[VectorStore] 维度不匹配 (旧集合 vs 当前嵌入模型): {e}。"
                    "已禁止自动删库重建 (记忆不可再生)。向量召回降级, 需人工迁移: "
                    "备份 data 下 chroma 目录后用新维度集合重嵌入, 勿直接删除旧集合。"
                )
            else:
                logger.warning(f"[VectorStore] 预热探针失败 (非致命): {e}")

    def _is_duplicate(self, text: str) -> bool:
        """
        插入前执行 top-1 语义相似度检查。
        ChromaDB 默认使用 cosine 距离，distance = 1 - cosine_similarity。
        当 cosine_similarity > 0.85 时视为语义重复，返回 True。
        """
        if not self.collection:
            return False
        try:
            result = self.collection.query(
                query_texts=[text],
                n_results=1,
                include=["documents", "distances"]
            )
            if result.get("ids") and result["ids"][0]:
                distances = result["distances"][0]
                if distances and len(distances) > 0:
                    distance = distances[0]
                    # cosine_distance = 1 - cosine_similarity
                    cosine_similarity = 1 - distance
                    return cosine_similarity > self.DEDUP_COSINE_THRESHOLD
        except Exception:
            # 去重检查失败不影响正常插入，静默跳过
            pass
        return False

    def add_memory(self, text: str, prefix: str = "obs", importance: float = 0.5,
                   speaker: str = "", favorability: float = 50, attention: float = 50,
                   is_master: bool = True, chat_name: str = "",
                   visibility: str = "", doc_id: str | None = None,
                   source_ref: str = "", created_at: float | None = None) -> bool:
        if not self.collection:
            return False
        # [0.3.26.0 Phase 0 M0-10] doc_id 弃毫秒时间戳改 UUID: 并发写碰撞窗口
        # 归零 (同毫秒双写互覆)。旧毫秒 ID 不迁移, 读取侧 (prune/GC) 已优先
        # metadata.created_at, doc_id 毫秒仅兜底, 双格式共存安全。
        # [M0-03] 可选外部 doc_id: remember 命令预生成返回给调用方, forget 按
        # 该 ID 精确删除, 记住/忘记闭环; 缺省生成新 UUID (老调用方零变化)。
        doc_id = doc_id if doc_id else f"{prefix}_{uuid.uuid4().hex}"
        # v0.6.0 双轴社交快照: speaker/favorability/attention 注入 metadata, 供社交重排加权。
        # 老数据无此字段时 compute_smallville_score 用 .get 默认 50 (不迁移, 过渡期中性)。
        # [阶段3-B] is_master 标来源可信度: 第三人发言 False / 老板+阿露 True。
        # rerank 对 is_master=False 候选 social_term 归零防豆橛子复发; 默认 True 保老数据中性。
        metadata = {
            "prefix": prefix,
            "importance": importance,     # 0.0~1.0, 记忆重要性评分
            "hit_count": 0,               # 被 smart_recall 命中的次数
            "promoted": False,            # True = 已晋升为长期记忆，GC 不删
            # 缺省仍取真实写入时刻。隔离历史评测可显式回放来源时间，不影响
            # 生产调用；显式 0 也按调用值保留，便于边界测试。
            "created_at": float(time.time() if created_at is None else created_at),
            "speaker": speaker or "",
            "favorability": float(favorability),
            "attention": float(attention),
            "is_master": bool(is_master),
            "chat_name": chat_name or "",  # [0.2.10.4 P1] 来源对话名, 供召回同群提权/跨群标注
            # [0.3.41.0] 可选外部来源引用。LongMemEval 等隔离评测用它把
            # 召回结果映射回官方 corpus_id；普通记忆缺省空串，行为不变。
            "source_ref": source_ref or "",
            # [0.3.16.0 P4] 隐私桶: private(老板1v1)/group(群聊)/internal(阿露自述)
            # /public(console 当面等)。空串 = 老数据或来源不明, 召回侧按 public 放
            # 行 (宁泄勿失记忆连续性, 收紧靠写入侧打桶)。
            "visibility": visibility or "",
        }
        # [0.2.23.7 review H2] 写入整段持锁: 与 _probe_and_migrate 的 delete+重建
        # 换绑互斥, 治迁移瞬间在途 add 打在已删集合上的丢数据窗口。
        with self._meta_lock:
            # 白名单内容直接放行，跳过查重
            if self._is_whitelisted(text):
                self.collection.add(documents=[text], ids=[doc_id], metadatas=[metadata])
                return True
            # 语义去重：如果存在高度相似的已有记录，跳过插入
            if self._is_duplicate(text):
                logger.info(f"[VectorStore] 去重拦截: 跳过高度相似的记忆 -> {text[:50]}...")
                return False
            self.collection.add(documents=[text], ids=[doc_id], metadatas=[metadata])
            return True

    def query_memory_with_meta(self, query: str, n_results: int = 5,
                               prefer_chat: str = "",
                               asker_is_master: bool = True,
                               acl_enabled: bool = None,
                               commit_hits: bool = True,
                               start_ts: float | None = None,
                               end_ts: float | None = None,
                               reject_out: list | None = None,
                               attach_breakdown: bool = False) -> list:
        """[阶段3-B] 召回 + 元数据, 供 smart_recall 按 is_master 标注来源。

        返回 [(doc, meta)] 列表 (按重排得分降序), meta 含 is_master/speaker/
        favorability/attention/chat_name 等。重排逻辑: 底层 cosine 召回 Top-K 候选 ->
        Smallville 复合重排 (recency+importance+hit_freq+社交+同群提权, 第三人 obs
        social_term 归零降权)。对最终命中记忆递增 hit_count (副作用)。

        [0.2.10.4 P1] prefer_chat: 非空时同群 obs 在 rerank 中获 chat_boost 加性
        提权, 本群近期流水账优先浮顶, 跨群旧话题自然降序 (治串台)。

        [0.3.26.0 Phase 0 M0-05] commit_hits: 浏览型请求 (Web UI 搜索/预览/
        召回检查器) 必须传 False, 零写不改记忆排序; 真实 Agent 召回默认 True。
        [0.3.26.0 Phase 0 M0-04] start_ts/end_ts: 时间过滤 (秒级), 与 when_filter
        规范化解析配合。
        """
        if not self.collection:
            return []
        # [0.2.23.7 review H2] 召回整段持锁 (RLock, _increment_hits 再入不死锁):
        # 防 _probe_and_migrate 换绑 collection 时查询打在已删集合上。
        self._meta_lock.acquire()
        try:
            return self._query_memory_locked(
                query, n_results, prefer_chat,
                asker_is_master=asker_is_master, acl_enabled=acl_enabled,
                commit_hits=commit_hits,
                start_ts=start_ts, end_ts=end_ts,
                reject_out=reject_out, attach_breakdown=attach_breakdown,
            )
        finally:
            self._meta_lock.release()

    def query_memory_exact_with_meta(self, query: str, n_results: int = 5,
                                     prefer_chat: str = "",
                                     asker_is_master: bool = True,
                                     acl_enabled: bool = None,
                                     commit_hits: bool = False,
                                     start_ts: float | None = None,
                                     end_ts: float | None = None,
                                     reject_out: list | None = None,
                                     attach_breakdown: bool = False) -> list:
        """精确全候选召回，供需要完整排名的隔离评测使用。

        HNSW 只保证近似 Top-K，不保证能返回图中全部节点。本入口读取集合中
        已存 embedding 计算精确距离，再复用同一 ACL/遗忘/时间过滤和
        Smallville 重排。日常 Agent 召回仍走 ``query_memory_with_meta``。
        """
        if not self.collection:
            return []
        with self._meta_lock:
            return self._query_memory_locked(
                query, n_results, prefer_chat,
                asker_is_master=asker_is_master, acl_enabled=acl_enabled,
                commit_hits=commit_hits,
                start_ts=start_ts, end_ts=end_ts,
                reject_out=reject_out, attach_breakdown=attach_breakdown,
                exact_all=True,
            )

    # [0.3.16.0 P4] 隐私 ACL 硬过滤总开关 (宁泄勿失默认开过滤; 出偏斜改 False
    # 全量回退 0.3.15.x 软排序行为, 下步接 config 旋钮)。
    ACL_ENABLED = True

    @staticmethod
    def _acl_verdict(meta: dict, prefer_chat: str,
                     asker_is_master: bool) -> tuple:
        """[0.3.16.0 P4 + 0.3.30.3 G19] 硬 ACL 裁决 + 机器可读 reason code。

        返回 (allowed, reason_code), 放行时 reason 空串。规则与 0.3.16.0 P4
        完全一致 (报告 P4 验收: 主人私聊记忆无法在群聊召回), 仅增原因透出
        (规格 6.7 reason code):
        - 无 visibility (老数据/来源不明) -> 放行 (public, 零行为变化);
        - private: 仅同私聊窗口 (prefer_chat==obs chat) 或系统直连 (prefer_chat
          空) 可见; 群聊一律不可见 -> private_other_chat;
        - group: 同群可见; 跨群仅主人上下文可见 -> group_cross_chat_guest;
        - internal: 仅主人上下文或系统直连可见 -> internal_guest;
        - 未知桶 fail-safe 放行 (宁泄勿失记忆连续性)。
        """
        vis = str(meta.get("visibility", "") or "")
        if not vis or vis == "public":
            return True, ""
        obs_chat = str(meta.get("chat_name", "") or "")
        if vis == "private":
            if not prefer_chat or prefer_chat == obs_chat:
                return True, ""
            return False, "private_other_chat"
        if vis == "group":
            if not prefer_chat or asker_is_master or prefer_chat == obs_chat:
                return True, ""
            return False, "group_cross_chat_guest"
        if vis == "internal":
            if bool(asker_is_master) or not prefer_chat:
                return True, ""
            return False, "internal_guest"
        return True, ""

    @staticmethod
    def _acl_allow(meta: dict, prefer_chat: str, asker_is_master: bool) -> bool:
        """[0.3.16.0 P4] 硬 ACL 裁决布尔入口 (委托 _acl_verdict, 语义不变)。"""
        return VectorStore._acl_verdict(meta, prefer_chat, asker_is_master)[0]

    def _exact_candidates_locked(self, query: str) -> tuple[list, list, list, list]:
        """读取全量 embedding 并按 Chroma collection 距离精确排序。"""
        import numpy as np

        results = self.collection.get(
            include=["documents", "metadatas", "embeddings"],
        )
        ids = list(results.get("ids", []) or [])
        docs = list(results.get("documents", []) or [])
        metas = list(results.get("metadatas", []) or [])
        raw_embeddings = results.get("embeddings")
        if not ids:
            return [], [], [], []
        if raw_embeddings is None or len(raw_embeddings) != len(ids):
            raise RuntimeError("精确召回无法读取完整 embedding")
        if self._embedding_function is None:
            raise RuntimeError("精确召回缺少 embedding function")

        matrix = np.asarray(raw_embeddings, dtype=np.float32)
        query_vectors = np.asarray(
            self._embedding_function([query]), dtype=np.float32,
        )
        if query_vectors.ndim != 2 or query_vectors.shape[0] != 1:
            raise RuntimeError("精确召回 query embedding 形状非法")
        query_vector = query_vectors[0]
        if matrix.ndim != 2 or matrix.shape[1] != query_vector.shape[0]:
            raise RuntimeError("精确召回 embedding 维度不一致")

        metadata = getattr(self.collection, "metadata", None) or {}
        space = str(metadata.get("hnsw:space", "l2"))
        if space == "cosine":
            denominator = (
                np.linalg.norm(matrix, axis=1) * np.linalg.norm(query_vector)
                + 1e-30
            )
            distance_values = 1.0 - (matrix @ query_vector) / denominator
        elif space == "ip":
            distance_values = 1.0 - matrix @ query_vector
        else:
            delta = matrix - query_vector
            distance_values = np.sum(delta * delta, axis=1)

        order = np.argsort(distance_values, kind="stable").tolist()
        return (
            [ids[index] for index in order],
            [docs[index] for index in order],
            [metas[index] for index in order],
            [float(distance_values[index]) for index in order],
        )

    def _query_memory_locked(self, query: str, n_results: int, prefer_chat: str,
                             asker_is_master: bool = True,
                             acl_enabled: bool = None,
                             commit_hits: bool = True,
                             start_ts: float | None = None,
                             end_ts: float | None = None,
                             reject_out: list | None = None,
                             attach_breakdown: bool = False,
                             exact_all: bool = False) -> list:
        """query_memory_with_meta 锁内实现 (调用方已持 _meta_lock)。

        [0.3.30.3 G19] reject_out: 传入列表时, ACL/时间过滤淘汰的候选以
        (doc, meta副本, reason_code) 三元组追加, 供 RecallPlan 产出机器可读
        淘汰原因 (规格 6.7); 不传零开销零行为变化。
        [0.3.30.3 G17] attach_breakdown: True 时返回 meta 带 _score_breakdown
        分项分 (semantic/recency/importance/hit_freq/social/same_chat/total)。
        """
        if acl_enabled is None:
            acl_enabled = self.ACL_ENABLED
        if exact_all:
            ids, docs, metas, distances = self._exact_candidates_locked(query)
        else:
            candidate_k = max(
                n_results * self.RERANK_CANDIDATE_FACTOR, n_results + 5,
            )
            # [F1] include "distances": 把 cosine 距离透传给 rerank 做相关性门控社交加权.
            # 旧逻辑召回完即丢 distances -> rerank 盲排 -> 胖脸鱼无关记忆凭社交浮顶.
            results = self.collection.query(
                query_texts=[query],
                n_results=candidate_k,
                include=["documents", "metadatas", "distances"],
            )
            ids = results.get("ids", [[]])[0]
            docs = results.get("documents", [[]])[0]
            metas = results.get("metadatas", [[]])[0]
            distances = results.get("distances", [[]])[0]
        if not ids:
            return []

        # [0.3.16.0 P4] 硬 ACL: 候选先按 visibility 桶过滤, 再进 rerank (ACL 先行
        # 召回在后, 治 0.2.10.4 软排序治标: 降权不等于不可见, 私聊 obs 仍漏进群)。
        if acl_enabled:
            _kept = []
            for i, m in enumerate(metas):
                _ok, _reason = VectorStore._acl_verdict(
                    m, prefer_chat, asker_is_master)
                if _ok:
                    _kept.append(i)
                elif reject_out is not None:
                    reject_out.append((docs[i], dict(m or {}), _reason))
            if len(_kept) < len(metas):
                logger.debug(
                    f"[VectorStore] [acl] 过滤 {len(metas) - len(_kept)}/{len(metas)} "
                    f"候选 (prefer_chat={prefer_chat!r}, asker_is_master={asker_is_master})"
                )
            if not _kept:
                return []
            ids = [ids[i] for i in _kept]
            docs = [docs[i] for i in _kept]
            metas = [metas[i] for i in _kept]
            distances = [distances[i] for i in _kept]

        # [0.3.38.1 Batch 1] 软删除过滤: status=forgotten 候选不进召回
        # (老数据无 status 字段一律放行, 与 ACL 老数据放行同方向)。
        _kept_f = []
        for i, m in enumerate(metas):
            if VectorStore._forget_allow(m):
                _kept_f.append(i)
            elif reject_out is not None:
                reject_out.append((docs[i], dict(m or {}), "forgotten"))
        if len(_kept_f) < len(metas):
            logger.debug(
                f"[VectorStore] [forget] 过滤 {len(metas) - len(_kept_f)}/{len(metas)} "
                f"候选 (软删除不进召回)"
            )
        if not _kept_f:
            return []
        ids = [ids[i] for i in _kept_f]
        docs = [docs[i] for i in _kept_f]
        metas = [metas[i] for i in _kept_f]
        distances = [distances[i] for i in _kept_f]

        # [0.3.26.0 Phase 0 M0-04] 时间过滤 (when_filter 规范化的 start_ts/end_ts):
        # 按 metadata.created_at (秒级) 裁候选; 无 created_at 的老数据不参与过滤
        # (宁泄勿失, 与 ACL 老数据放行同方向)。
        if start_ts is not None or end_ts is not None:
            _kept_t = []
            for i, m in enumerate(metas):
                created = float((m or {}).get("created_at", 0) or 0)
                if created <= 0:
                    # 老数据无 created_at 不参与过滤 (宁泄勿失, 同 ACL 老数据放行
                    # 方向; 旧实现此处误丢, 与 M0-04 注释意图相反, 0.3.30.3 修正)。
                    _kept_t.append(i)
                    continue
                if ((start_ts is not None and created < start_ts)
                        or (end_ts is not None and created >= end_ts)):
                    if reject_out is not None:
                        reject_out.append(
                            (docs[i], dict(m or {}), "time_out_of_range"))
                    continue
                _kept_t.append(i)
            if len(_kept_t) < len(metas):
                logger.debug(
                    f"[VectorStore] [time] 过滤 {len(metas) - len(_kept_t)}/{len(metas)} "
                    f"候选 (start={start_ts}, end={end_ts})"
                )
            if not _kept_t:
                return []
            ids = [ids[i] for i in _kept_t]
            docs = [docs[i] for i in _kept_t]
            metas = [metas[i] for i in _kept_t]
            distances = [distances[i] for i in _kept_t]

        now = time.time()
        # Smallville 复合重排下沉为纯函数 (见模块级), 这里仅传入类常量参数
        top_ids, top_docs, top_metas = rerank_candidates(
            ids, docs, metas, now,
            n_results=n_results,
            alpha=self.RERANK_ALPHA, beta=self.RERANK_BETA, gamma=self.RERANK_GAMMA,
            half_life_sec=self.RECENCY_HALF_LIFE_SEC,
            delta_fav=self.RERANK_DELTA_FAV, delta_att=self.RERANK_DELTA_ATT,
            social_baseline=self.SOCIAL_BASELINE,
            distances=distances,
            social_relevance_gate=self.RERANK_SOCIAL_RELEVANCE_GATE,
            query=query,
            non_master_social_zero=self.NON_MASTER_SOCIAL_ZERO,
            prefer_chat=prefer_chat,
            attach_breakdown=attach_breakdown,
        )

        # 渐进式记忆: 对最终命中的记忆递增 hit_count (副作用)。
        # [0.3.26.0 Phase 0 M0-05] 仅真实 Agent 召回 (commit_hits=True) 提交命中,
        # 浏览型请求 (Web UI 搜索/预览/召回检查器) 零写, 不改记忆排序。
        if commit_hits and top_ids and top_metas:
            self._increment_hits(top_ids, top_metas)
        return list(zip(top_docs, top_metas))

    def query_memory(self, query: str, n_results: int = 5) -> list:
        """次世代检索 (保签名不变, 调 query_memory_with_meta 取 docs)。

        返回 Top-n_results 文本列表。老调用方 (main recall / reflection / gc /
        老 eval) 行为不变; smart_recall 改用 query_memory_with_meta 取元数据做来源标注。
        """
        pairs = self.query_memory_with_meta(query, n_results=n_results)
        return [doc for doc, _ in pairs]

    def projection_sample(self, limit: int = 400) -> list:
        """[0.2.13.0 向量星图] 取 embedding+文档+元数据样本并 PCA 压到 2D。

        供 UI 画真实向量散点星图 (语义近的挨得近)。numpy 惰性 import, 无 numpy
        或向量不足 3 条返 [] (UI 显示空态)。纯读, 不触写。
        """
        if not self.collection:
            return []
        try:
            res = self.collection.get(
                include=["embeddings", "documents", "metadatas"],
                limit=max(limit * 2, 50),
            )
        except Exception as e:
            logger.warning(f"[VectorStore] projection_sample get 失败: {e}")
            return []
        docs = res.get("documents", []) or []
        metas = res.get("metadatas", []) or []
        emb = res.get("embeddings", []) or []
        triples = [(emb[i], docs[i], metas[i]) for i in range(len(docs))
                   if i < len(emb) and emb[i] is not None]
        if len(triples) < 3:
            return []
        import random
        if len(triples) > limit:
            triples = random.sample(triples, limit)
        try:
            import numpy as np
            x_mat = np.asarray([t[0] for t in triples], dtype=float)
            if x_mat.ndim != 2 or x_mat.shape[0] < 3:
                return []
            centered = x_mat - x_mat.mean(axis=0)
            u_mat, s_vec, _vt = np.linalg.svd(centered, full_matrices=False)
            coords = (u_mat[:, :2] * s_vec[:2]).tolist()
        except Exception as e:
            logger.warning(f"[VectorStore] PCA 投影失败: {e}")
            return []
        pts = []
        for i, (x, y) in enumerate(coords):
            m = triples[i][2] or {}
            pts.append({
                "x": float(x), "y": float(y),
                "text": (triples[i][1] or "")[:80],
                "prefix": str(m.get("prefix", "")),
                "is_master": bool(m.get("is_master", True)),
            })
        return pts

    def list_by_prefix(self, prefix: str = "episode", limit: int = 20) -> list:
        """按 prefix 列出最近 N 条记忆 (供入梦审查 UI)。

        返回 [{text, created_at, hit_count, promoted}] 按 created_at 降序。
        同步方法 (collection.get), 调用方用 asyncio.to_thread 包。
        """
        if not self.collection:
            return []
        try:
            results = self.collection.get(
                where={"prefix": prefix},
                limit=limit,
                include=["documents", "metadatas"],
            )
        except Exception as e:
            logger.warning(f"[VectorStore] list_by_prefix 失败: {e}")
            return []
        docs = results.get("documents", [])
        metas = results.get("metadatas", [])
        items = []
        for doc, meta in zip(docs, metas):
            items.append({
                "text": doc,
                "created_at": meta.get("created_at", 0),
                "hit_count": meta.get("hit_count", 0),
                "promoted": meta.get("promoted", False),
            })
        items.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return items

    def _increment_hits(self, ids: list, current_metadatas: list):
        """批量递增记忆的命中计数。"""
        with self._meta_lock:
            try:
                updated = []
                for meta in current_metadatas:
                    m = dict(meta) if meta else {}
                    m["hit_count"] = m.get("hit_count", 0) + 1
                    updated.append(m)
                self.collection.update(ids=ids, metadatas=updated)
            except Exception as e:
                logger.warning(f"[VectorStore] 递增命中计数失败 (非致命): {e}")

    def delete_by_id(self, doc_id: str) -> bool:
        """[0.3.26.0 Phase 0 M0-03] 按 doc_id 精确删除单条向量记忆。

        forget 闭环: remember 写入 Chroma fact 后, forget 必须能真正抹除向量,
        否则"记住"与"忘记"不闭环 (遗忘后仍被召回)。返回是否确实删除。
        """
        if not self.collection:
            return False
        with self._meta_lock:
            try:
                existing = self.collection.get(ids=[doc_id], include=["metadatas"])
                if not existing.get("ids"):
                    return False
                self.collection.delete(ids=[doc_id])
                logger.info(f"[VectorStore] 已按 doc_id 删除记忆: {doc_id}")
                return True
            except Exception as e:
                logger.warning(f"[VectorStore] 按 doc_id 删除失败 (非致命): {doc_id} - {e}")
                return False

    # ---- [0.3.38.1 Batch 1] 软删除 + 恢复 (forget 不物理抹除) ----

    @staticmethod
    def _forget_allow(meta) -> bool:
        """召回过滤谓词: 只滤显式 forgotten。无 status 的老数据一律放行
        (与 ACL 老数据放行同方向, 宁泄勿失防误伤)。"""
        return str((meta or {}).get("status", "") or "") != "forgotten"

    def _set_status(self, doc_id: str, status: str) -> bool:
        """读-合并-写 metadata status (chroma update 覆盖式, 镜像 promote_memory)。"""
        if not self.collection:
            return False
        with self._meta_lock:
            try:
                existing = self.collection.get(ids=[doc_id], include=["metadatas"])
                if not existing.get("ids"):
                    return False
                meta = dict(existing["metadatas"][0]) if existing["metadatas"][0] else {}
                meta["status"] = status
                self.collection.update(ids=[doc_id], metadatas=[meta])
                logger.info(f"[VectorStore] 记忆 {doc_id} 状态 -> {status}")
                return True
            except Exception as e:
                logger.warning(f"[VectorStore] 状态置位失败 (非致命): {doc_id} - {e}")
                return False

    def soft_forget(self, doc_id: str) -> bool:
        """软删除: 置 status=forgotten, 物理保留, 召回过滤。可 restore 恢复。"""
        return self._set_status(doc_id, "forgotten")

    def restore_memory(self, doc_id: str) -> bool:
        """恢复软删除的记忆: status 置回 active (重新可召回)。"""
        return self._set_status(doc_id, "active")

    def promote_memory(self, doc_id: str) -> bool:
        """将一条 obs 记忆标记为已晋升（promoted=True），GC 不再清理它。"""
        with self._meta_lock:
            try:
                # 先读取现有元数据，合并 promoted 标记（ChromaDB update 是覆盖式，不是合并式）
                existing = self.collection.get(ids=[doc_id], include=["metadatas"])
                if not existing["ids"]:
                    return False
                meta = dict(existing["metadatas"][0]) if existing["metadatas"][0] else {}
                meta["promoted"] = True
                self.collection.update(ids=[doc_id], metadatas=[meta])
                return True
            except Exception as e:
                logger.warning(f"[VectorStore] 记忆晋升失败: {doc_id} - {e}")
                return False

    def get_promotable_memories(self, min_hits: int = 3, min_importance: float = 0.8,
                                min_age_days: float = 1.0) -> list:
        """
        获取满足渐进式晋升条件的 obs 记忆。
        晋升条件: (命中>=3次 OR 重要性>0.8) AND 存在超过1天 AND 未晋升
        """
        try:
            all_data = self.collection.get(include=["metadatas", "documents"])
        except Exception as e:
            logger.warning(f"[VectorStore] 获取候选记忆失败: {e}")
            return []
        candidates = []
        now = time.time()
        for i, doc_id in enumerate(all_data.get("ids", [])):
            if not doc_id.startswith("obs_"):
                continue
            meta = all_data.get("metadatas", [{}])[i] or {}
            if meta.get("promoted"):
                continue
            hits = meta.get("hit_count", 0)
            imp = meta.get("importance", 0.5)
            age_days = (now - meta.get("created_at", now)) / 86400
            if (hits >= min_hits or imp > min_importance) and age_days > min_age_days:
                candidates.append({
                    "id": doc_id,
                    "content": all_data.get("documents", [""])[i],
                })
        return candidates

    # ================= [神经元修剪] =================
    def prune_old_memories(self, prefix: str = "obs", max_age_seconds: int = 86400 * 3):
        """
        物理删除过期记忆。
        默认清理 3 天前 (86400 * 3 秒) 的流水账碎片 (obs)，绝对不会误删情景摘要 (episode)。
        渐进式记忆: 已晋升 (promoted=True) 的记忆即使过期也不会被清理。
        """
        if not self.collection:
            return 0
        try:
            # 提取当前所有神经元的 ID 和元数据
            data = self.collection.get(include=["metadatas"])
            ids = data.get("ids", [])
            metadatas = data.get("metadatas", [])
            current_time_ms = time.time() * 1000
            now_sec = current_time_ms / 1000.0

            ids_to_delete = []
            for i, doc_id in enumerate(ids):
                if not doc_id.startswith(f"{prefix}_"):
                    continue
                # 跳过已晋升为长期记忆的条目
                meta = metadatas[i] if i < len(metadatas) else {}
                if meta and meta.get("promoted"):
                    continue
                # R3: 优先读 metadata.created_at (秒级), 旧条目无 created_at -> doc_id 毫秒兜底;
                # 解析失败显式 warn 不静默 pass, 防前缀格式变化致 GC 静默失效 (永不删)。
                try:
                    created = float(meta.get("created_at", 0)) if meta else 0.0
                    if created <= 0:
                        created = int(doc_id.split("_")[1]) / 1000.0
                    if (now_sec - created) > max_age_seconds:
                        ids_to_delete.append(doc_id)
                except (ValueError, IndexError, TypeError) as e:
                    logger.warning(f"[VectorStore] 记忆年龄解析失败, 跳过删除 (doc_id={doc_id}): {e}")

            if ids_to_delete:
                self.collection.delete(ids=ids_to_delete)
            return len(ids_to_delete)
        except Exception as e:
            logger.error(f"[VectorStore] 神经元修剪异常: {e}")
            return 0
