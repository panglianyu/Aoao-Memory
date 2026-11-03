import asyncio
import json
import os
import time
import re
import threading
import uuid
from typing import Optional
from system_memory_god.storage.vector_store import VectorStore
from system_memory_god.storage.file_io import FileIO
from system_memory_god.storage.rule_store import RuleStore
from system_memory_god.utils.llm_client import LLMClient
from kernel.logger import get_logger

logger = get_logger(__name__)


class ReflectionDaemon:
    def __init__(self, vector_store: VectorStore, file_io: FileIO, rule_store: RuleStore,
                 llm_client: LLMClient, graph_store=None, entity_store=None,
                 buffer_path: Optional[str] = None):
        self.vector_store = vector_store
        self.file_io = file_io
        self.rule_store = rule_store
        self.llm_client = llm_client
        # [Phase 1 后续] GraphStore 自动构图。None 时跳过三元组抽取 (向后兼容老调用方/测试)。
        self.graph_store = graph_store
        # [0.2.9.0] 兴趣值演化: 反思副产品 <interest> 增量写实体存储。None 时跳过。
        self.entity_store = entity_store
        # buffer 存 (text, is_master, sender_id, sender_name, doc_id) 元组:
        # is_master 决定三元组写入 source 分级 (老板 -> master 激活 weight=1.0; 水友 -> memory 潜伏 0.3);
        # sender_id (微信号) + sender_name (昵称) 供 0.2.5.2 图谱按 ext_id 认人 (昵称变也合并);
        # [0.3.30.0 G16a] doc_id = obs 向量 doc ID, 反思 episode 派生链写真来源。
        # 老调用 push_log(text, is_master) 不传后续参数, 默认 None 向后兼容。
        self._buffer_lock = threading.RLock()
        self._reflection_lock = asyncio.Lock()
        self.buffer: list = []
        # [0.2.10.0] 阈值 20->10: 老板短会话用法, 高阈值攒不满反思永远不触发
        self.threshold = 10
        self.idle_timeout_seconds = 300
        self.last_active_time = time.time()
        self.task = None
        # [0.2.10.0] 持久化缓冲文件: 闪退/强退/关窗口不丢未反思对话 (路径由 main 接线)
        self._buffer_path: Optional[str] = buffer_path
        self._replay_persisted_buffer()

    def push_log(self, text: str, is_master: bool = False,
                 sender_id: Optional[str] = None, sender_name: Optional[str] = None,
                 doc_id: Optional[str] = None):
        """记录对话流水账。is_master 标说话人可信度, 决定图谱三元组 source 分级。
        sender_id (微信号) + sender_name (昵称): 0.2.5.2 图谱按 ext_id 认人用, 可选。
        [0.3.30.0 G16a] doc_id: obs 向量 doc ID (录制侧预生成 obs_<uuid>), 反思
        episode 沉淀时写真实来源进 ledger 派生链; 老调用方不传 -> None (派生链
        如实记 legacy_obs_unknown)。
        """
        with self._buffer_lock:
            self.buffer.append((text, is_master, sender_id, sender_name, doc_id))
            self.last_active_time = time.time()
            # [0.2.10.0] 同步落盘一行: 进程在反思前死掉也能开机续上
            self._persist_entry({"text": text, "is_master": is_master,
                                 "sender_id": sender_id,
                                 "sender_name": sender_name,
                                 "doc_id": doc_id})

    async def _apply_interest_deltas(self, interest_block: str) -> None:
        """[0.2.9.0] 解析 <interest> 块 TOPIC/DELTA 行, read-modify-write 落实体存储。

        野生兴趣: prompt 明示不限老板参与, 群里别人聊得有意思的话题也涨。
        非致命: 解析/写入失败只记日志, 不影响反思主流程 (同 _extract_graph_triples 范式)。
        """
        if self.entity_store is None:
            return
        text = (interest_block or "").strip()
        if not text or text == "无":
            return
        applied = 0
        for line in text.splitlines():
            m = re.match(r"\s*TOPIC:\s*(.+?)\s*\|\s*DELTA:\s*([0-9.]+)\s*$", line)
            if not m:
                continue
            topic = m.group(1).strip()
            try:
                delta = float(m.group(2))
            except ValueError:
                continue
            if not topic or delta <= 0:
                continue
            try:
                new_value = await self.entity_store.boost_interest(topic, delta)
                applied += 1
                logger.info(f"[Memory Daemon] 兴趣上涨: 【{topic}】+{delta} -> {new_value}")
            except Exception as e:
                logger.warning(f"[Memory Daemon] 兴趣写入失败(非致命): {topic} +{delta}: {e}")
        if applied:
            logger.info(f"[Memory Daemon] 本批反思沉淀 {applied} 个兴趣增量。")

    def start(self):
        self.task = asyncio.create_task(self._dream_loop())

    def stop(self):
        if self.task:
            self.task.cancel()

    async def _dream_loop(self):
        while True:
            await asyncio.sleep(30)

            current_time = time.time()
            with self._buffer_lock:
                buffer_length = len(self.buffer)

            is_full = buffer_length >= self.threshold
            is_idle = (buffer_length > 0) and ((current_time - self.last_active_time) > self.idle_timeout_seconds)
            needs_checkup = await self.rule_store.should_trigger_checkup()

            if is_full or is_idle or needs_checkup:
                if needs_checkup and not is_full and not is_idle:
                    trigger_reason = "性格体检周期到达"
                else:
                    trigger_reason = "触发阈值" if is_full else "系统空闲"
                await self._reflect_once(trigger_reason)

    async def _reflect_once(self, trigger_reason: str, final: bool = False) -> None:
        """串行执行一次反思，避免循环与关机 flush 重复消费同一批缓冲。"""
        async with self._reflection_lock:
            await self._reflect_once_locked(trigger_reason, final=final)

    async def _reflect_once_locked(self, trigger_reason: str,
                                   final: bool = False) -> None:
        """[0.2.10.0] 单次反思核心 (循环触发与临终反思共用)。

        final=True 临终轻路径: 只产摘要/顿悟/兴趣, 跳过三元组抽取/记忆晋升/
        性格体检等重阶段, LLM 超时缩至 45s。流程: 缓冲切片 -> LLM 反思 -> 沉淀 ->
        重写持久文件 (留未反思残量, 全处理则清空)。反思超时/异常时持久文件不动,
        残量开机续反, 对话绝不蒸发。
        """
        logger.info(f"[Memory Daemon] 检测到 {trigger_reason}，阿露进入潜意识深度反思...")

        # [风暴熔断前移] 体检判定成立即登记冷却, 防摘要超时致冷却漏登 -> 30s 死循环烧 API。
        needs_checkup = False if final else await self.rule_store.should_trigger_checkup()
        if needs_checkup:
            await self.rule_store.mark_checkup_attempt("pending")

        with self._buffer_lock:
            buffer_length = len(self.buffer)
            process_count = (buffer_length if (final or buffer_length < self.threshold)
                             else self.threshold)
            logs_to_process = self.buffer[:process_count]
        # [0.3.26.0 Phase 0 M0-07] 失败保留: 批次只在成功提交后出队。先切片但
        # 不立即出队, LLM 成功沉淀后才切走; 超时/异常时批次留在 buffer 头部,
        # 下轮循环续反 (旧实现先出队, 失败批次丢失, 规格: 反思失败不丢内容)。
        _batch_committed = False

        # logs_to_process 是 (text, is_master, sender_id, sender_name) 元组列表
        memories_text = "\n".join(text for text, *_ in logs_to_process)
        # [Phase 1 后续] 图谱 source 分级: 老板参与对话 -> master (激活); 纯水友 -> memory (潜伏)。
        graph_source = "master" if any(is_m for _, is_m, *_ in logs_to_process) else "memory"
        # 0.2.5.2 昵称->微信号 映射: 三元组解析时 person 节点按 name 查 ext_id 认人。
        nick_to_wxid: dict = {}
        # [0.3.30.0 G16a] 星号解包: 五元组 (第 5 位 doc_id) 不参与认人映射。
        for _, _, sid, sname, *_ in logs_to_process:
            if sid and sname:
                nick_to_wxid[sname] = sid
        current_time = time.time()

        prompt = f"""
        你现在是阿露的潜意识（后台顿悟引擎）。请深度分析以下近期的对话流水账，并严格按要求提取信息。

        要求：
        必须严格使用 <summary> 和 <epiphany> XML标签包裹你的输出内容。

        <summary>
        在这里写下第三视角的客观事件情景摘要（只保留客观发生的事情，用于事后回忆）。
        </summary>

        <epiphany>
        仔细体察对话中胖脸鱼的情绪反馈、态度变化，或者阿露自身的感悟。
        如果这段经历足以让阿露"学到教训"、"改变行为模式"或"产生新的情感羁绊"，请用第一人称写下一条极简的【自我规矩或性格烙印】（例如："胖脸鱼今天很累，我以后晚上要尽量温柔安静一些" 或 "胖脸鱼对某个话题很反感，我绝不再提"）。
        如果这段对话只是普通闲聊，没有对性格产生深远影响，请在这里只写一个字："无"。
        </epiphany>

        <interest>
        列出阿露自己觉得有意思的话题 0~3 个 (野生兴趣: 不限胖脸鱼参与, 群聊里别人聊得有趣的也算)。
        【话题名必须是 2~6 个字的短词】(如: 三国杀/直播/Dota2/猫罐/表情包), 严禁写成长句
        或事件描述 -- 兴趣词要能命中日后的群聊只言片语, 长句永远匹配不上等于白记。
        每行严格写: TOPIC: 短话题词 | DELTA: 涨幅 (0.3~1.0, 越感兴趣越大)。一个都没有就只写: 无
        </interest>

        近期对话日志：
        {memories_text}
        """

        try:
            insight = await asyncio.wait_for(
                self.llm_client.generate_async(prompt),
                timeout=45.0 if final else 90.0
            )

            # 错误类型化: generate_async 失败一律返回 None, 显式判空即可
            if insight:
                summary_match = re.search(r'<summary>(.*?)</summary>', insight, re.DOTALL)
                if summary_match:
                    summary_text = summary_match.group(1).strip()
                    if summary_text:
                        final_summary = f"[情景记忆摘要] {summary_text}"
                        logger.info(f"[Memory Daemon] 沉淀新记忆: {final_summary}")
                        # [0.3.30.0 G16a] 预生成 episode doc_id: 向量存储与 ledger
                        # store_ref 同 ID, 派生链可定位 episode 自身向量 doc。
                        _ep_doc_id = f"episode_{uuid.uuid4().hex}"
                        self.vector_store.add_memory(final_summary, prefix="episode",
                                                     doc_id=_ep_doc_id)
                        # [0.3.26.0 Phase 1] ledger 记 episode 来源事件 + 与入池
                        # observation 的派生链 (规格 4.1: 反思 episode 写派生关系;
                        # ledger 失败进 backlog 不阻断反思主链)。
                        try:
                            from system_memory_god.models import MemoryDerivation, MemoryItem
                            from system_memory_god.storage.memory_ledger import new_memory_id
                            ledger = getattr(self, "_ledger", None)
                            if ledger is not None:
                                _ep_mid = new_memory_id()
                                ledger.record_item(MemoryItem(
                                    memory_id=_ep_mid, kind="episode",
                                    store="vector", store_ref=_ep_doc_id,
                                    content_preview=final_summary,
                                    actor="assistant", speaker="",
                                    chat_name="", visibility="",
                                    created_at=current_time,
                                ))
                                # 派生链: episode summarized_from 入池 observation。
                                # [0.3.30.0 G16a] parent_id 取真实 obs doc_id
                                # (录制侧预生成透传); 无 ID 的老数据如实标 unknown。
                                for _src in logs_to_process[:3]:
                                    _src_doc = (_src[4]
                                                if len(_src) > 4 and _src[4] else None)
                                    ledger.record_derivation(MemoryDerivation(
                                        parent_id=_src_doc or "legacy_obs_unknown",
                                        child_id=_ep_mid,
                                        relation="summarized_from",
                                        run_id="reflection",
                                    ))
                        except Exception as _ledger_e:
                            logger.warning(f"[Memory Daemon] ledger episode 记账失败(非致命): {_ledger_e}")
                    else:
                        logger.warning("[Memory Daemon] 反思 <summary> 为空, 情景记忆摘要未沉淀")
                else:
                    logger.warning("[Memory Daemon] 反思输出未含 <summary> 标签, 情景记忆摘要未沉淀")

                epiphany_match = re.search(r'<epiphany>(.*?)</epiphany>', insight, re.DOTALL)
                if epiphany_match:
                    epiphany_text = epiphany_match.group(1).strip()
                    # 过滤 LLM 错误泄漏: 防 "Error:/API_RESPONSE_" 哨兵被当有效顿悟写入
                    _ep_lower = epiphany_text.lower()
                    if (epiphany_text and epiphany_text != "无"
                            and len(epiphany_text) > 1
                            and not _ep_lower.startswith("error")
                            and not _ep_lower.startswith("api_response")):
                        added = await self.rule_store.add_rule(epiphany_text)
                        if added:
                            logger.info(f"[Memory Daemon] 触发潜意识顿悟，自动演化性格规矩: {epiphany_text}")
                            logger.info(f"[Memory Daemon] 当前规则总数: {await self.rule_store.get_rule_count()}")
                            # [0.3.30.4 G16] 顿悟规则进账本 (kind=procedural_rule,
                            # store_ref=规则文本; 体检批量重写暂不逐条记账, 留 2c)。
                            try:
                                from system_memory_god.models import MemoryItem
                                from system_memory_god.storage.memory_ledger import new_memory_id
                                _lg = getattr(self, "_ledger", None)
                                if _lg is not None:
                                    _lg.record_item(MemoryItem(
                                        memory_id=new_memory_id(),
                                        kind="procedural_rule", store="rule",
                                        store_ref=epiphany_text[:120],
                                        content_preview=epiphany_text[:200],
                                        actor="assistant", speaker="顿悟",
                                        created_at=current_time,
                                    ))
                            except Exception as _le:
                                logger.debug(
                                    f"[Memory Daemon] ledger 顿悟记账失败(非致命): {_le}")
                        else:
                            logger.info(f"[Memory Daemon] 跳过重复顿悟: {epiphany_text}")

                # [0.2.9.0] 兴趣值上涨: 反思副产品 <interest> 块 (野生兴趣也涨); 非致命。
                interest_match = re.search(r'<interest>(.*?)</interest>', insight, re.DOTALL)
                if interest_match:
                    await self._apply_interest_deltas(interest_match.group(1))

                logger.info("[Memory Daemon] 潜意识深度反思完毕，逻辑链闭环。")
                _batch_committed = True  # [M0-07] LLM 成功 -> 批次可出队

                if not final:
                    # 渐进式记忆晋升: 高频命中的 obs 记忆升级为长期记忆 (GC 不再清理)
                    await self._promote_important_memories()
                    # [Phase 1 后续] 自动构图: 抽实体关系三元组写 GraphStore
                    await self._extract_graph_triples(memories_text, graph_source,
                                                      occurred_at=current_time,
                                                      nick_to_wxid=nick_to_wxid)
                    # 反思后执行性格体检 (M0-08: 冷却登记已在入口判定成立那一刻完成,
                    # 这里直接用判定结果驱动, 不再二次 should_trigger_checkup -- 二次
                    # 判定会因刚登记的冷却恒 False, 使正常路径不可达)。
                    if needs_checkup:
                        await self._personality_checkup()

        except asyncio.TimeoutError:
            logger.error(f"[Memory Daemon] 大模型 API 请求超时 ({45 if final else 90}秒)，本次反思已强制取消防卡死！")
        except Exception as e:
            logger.error(f"[Memory Daemon] 反思过程中发生网络或解析异常: {e}")

        # [M0-07] 成功才出队: 已提交批次切走, 失败批次留在 buffer 头部下轮续反。
        if _batch_committed:
            with self._buffer_lock:
                self.buffer = self.buffer[process_count:]

        self.last_active_time = time.time()
        # [0.2.10.0] 收尾重写持久文件: 只留未反思残量 (全处理则清空); 超时/异常时
        # 残量仍在盘上, 开机续反 -- 对话绝不蒸发的最后一道闸。
        self._rewrite_persisted_buffer()

    async def flush_final(self, timeout: float = 60.0) -> None:
        """[0.2.10.0] 临终反思: 优雅关机路径关机前跑一次轻量反思 (final=True)。

        空缓冲 no-op; 超时/异常有持久文件兜底 (开机续反)。老板短会话用法下,
        这是"聊完关机 -> 记忆照样沉淀"的主路径。
        """
        with self._buffer_lock:
            buffer_length = len(self.buffer)
        if not buffer_length:
            return
        logger.info(f"[Memory Daemon] 关机临终反思: {buffer_length} 条未反思 (上限 {timeout}s)...")
        try:
            await asyncio.wait_for(self._reflect_once("关机临终反思", final=True), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[Memory Daemon] 临终反思超时, 残量已落盘, 开机续反")
        except Exception as e:
            logger.warning(f"[Memory Daemon] 临终反思异常 (非致命, 已落盘): {e}")

    # ---- [0.2.10.0] 反思缓冲持久化 (闪退/强退/关窗口不丢未反思对话) ----

    def _persist_entry(self, entry: dict) -> None:
        """追加一行 JSONL 到持久缓冲 (写入失败只记日志不阻塞对话流)。"""
        if not self._buffer_path:
            return
        try:
            with open(self._buffer_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[Memory Daemon] 缓冲落盘失败(非致命): {e}")

    def _replay_persisted_buffer(self) -> None:
        """开机回放: 上次未反思残量回填内存缓冲并清盘, 置 last_active_time=0 使
        空闲触发在循环首轮即反思残局。坏行跳过不中断。"""
        if not self._buffer_path or not os.path.exists(self._buffer_path):
            return
        entries = []
        try:
            with open(self._buffer_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                        text = str(e.get("text", ""))
                        if text:
                            # [0.3.30.0 G16a] 老行无 doc_id 键 -> None, 派生链
                            # 如实记 legacy_obs_unknown (兼容回放)。
                            entries.append((text, bool(e.get("is_master", False)),
                                            e.get("sender_id"), e.get("sender_name"),
                                            e.get("doc_id")))
                    except (json.JSONDecodeError, TypeError):
                        continue
        except Exception as e:
            logger.warning(f"[Memory Daemon] 持久缓冲读取失败 (非致命): {e}")
            return
        if not entries:
            return
        with self._buffer_lock:
            self.buffer.extend(entries)
        try:
            os.remove(self._buffer_path)
        except OSError as e:
            logger.warning(f"[Memory Daemon] 持久缓冲清盘失败 (非致命): {e}")
        self.last_active_time = 0
        logger.info(f"[Memory Daemon] 回放下线前未反思残局 {len(entries)} 条, 启动后即反思")

    def _rewrite_persisted_buffer(self) -> None:
        """反思收尾: 按内存缓冲残量重写持久文件 (部分反思只留未反思部分)。"""
        if not self._buffer_path:
            return
        try:
            with self._buffer_lock:
                tmp = self._buffer_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    for entry in self.buffer:
                        # [0.3.30.0 G16a] 切片解包: 五元组为主, 防御历史四元组残留。
                        text, is_master, sender_id, sender_name = entry[:4]
                        doc_id = entry[4] if len(entry) > 4 else None
                        f.write(json.dumps(
                            {"text": text, "is_master": is_master,
                             "sender_id": sender_id,
                             "sender_name": sender_name,
                             "doc_id": doc_id},
                            ensure_ascii=False) + "\n")
                os.replace(tmp, self._buffer_path)
        except Exception as e:
            logger.warning(f"[Memory Daemon] 持久缓冲重写失败 (非致命): {e}")

    async def _promote_important_memories(self):
        """
        渐进式记忆晋升: 扫描 VectorStore 中高频命中或高重要性的 obs 记忆，
        将它们标记为 promoted=True，使 GC 不再清理。
        保留即时记忆路径不受影响（EntityStore 的即时存储不经过此逻辑）。
        """
        try:
            candidates = await asyncio.to_thread(self.vector_store.get_promotable_memories)
            if not candidates:
                return
            promoted_count = 0
            for mem in candidates:
                success = await asyncio.to_thread(self.vector_store.promote_memory, mem["id"])
                if success:
                    promoted_count += 1
                    # [0.3.30.4 G16b] 晋升生命周期事件 (老数据无账本行如实跳过)。
                    try:
                        _lg = getattr(self, "_ledger", None)
                        if _lg is not None:
                            _row = _lg.find_by_store_ref("vector", str(mem["id"]))
                            if _row:
                                _lg.record_event(
                                    _row["memory_id"], "promoted",
                                    actor="system", reason="高频命中晋升长期")
                    except Exception as _le:
                        logger.debug(
                            f"[Memory Daemon] ledger 晋升记账失败(非致命): {_le}")
            if promoted_count > 0:
                logger.info(
                    f"[Memory Daemon] 渐进式晋升: {promoted_count} 条高频记忆升级为长期记忆"
                )
        except Exception as e:
            logger.warning(f"[Memory Daemon] 渐进式晋升扫描失败 (非致命): {e}")

    async def _extract_graph_triples(self, memories_text: str, source: str,
                                     occurred_at: Optional[float] = None,
                                     nick_to_wxid: Optional[dict] = None):
        """[Phase 1 后续] 自动构图: LLM 抽实体关系三元组写 GraphStore。

        从同批对话流水账抽 (实体A, 关系, 实体B) 三元组, GraphStore.add_node/add_edge
        内部已查 blocklist + 权重分级, 此处只解析 + 调用。source 决定分级:
        master=1.0 激活 / memory=0.3 潜伏。非致命, 失败/超时不阻塞反思主链。
        occurred_at: 事件发生时间 (unix ts), 传给 add_node/add_edge。None 用当前时间。
        0.2.4.0 时间检索: reflection 传反思触发 current_time, A 脚本传 obs.created_at。
        """
        if not self.graph_store or not memories_text:
            return
        prompt = f"""你是阿露潜意识的【图谱关系提取器】。
任务：从对话中提取【实体关系三元组】, 构建社交/知识双图谱, 必须为每条三元组判定归属域。

格式 (每行一条, 多条换行):
TRIPLE: 实体A | 关系 | 实体B | graph_domain | kind_A | kind_B

graph_domain 归属判定 (强制):
- social: 涉及人/性格/爱好/禁忌/关系/情感/事件/社交 (如 朋友/老板/喜欢/讨厌/承诺)
- knowledge: 涉及游戏规则/营销政策/代码技术/业务逻辑/项目/skill/知识结论 (如 包含规则/依赖/属于/技术栈)
- tool: 涉及工具调用/技能使用/API/命令执行/发消息 (如 调用use_wechat/搜图/生图/发消息)

kind 取值: person(人) / skill(技能) / knowledge(知识结论) / thing(事物) / project(项目) / tool(工具)
关系示例: 朋友/老板/同事/参与/提及/喜欢/讨厌/承诺/属于/依赖/认识/家人/包含规则/技术栈

只提取对话中明确提及的关系, 不要臆测。无明确关系只输出 NONE。

对话:
{memories_text}
"""
        try:
            result = await asyncio.wait_for(
                self.llm_client.generate_async(prompt),
                timeout=180.0
            )
        except asyncio.TimeoutError:
            logger.warning("[Memory Daemon] 图谱三元组抽取超时 (非致命)")
            return
        except Exception as e:
            logger.warning(f"[Memory Daemon] 图谱三元组抽取异常 (非致命): {e}")
            return

        if not result or result.strip() == "NONE":
            return

        count = 0
        for line in result.split("\n"):
            line = line.strip()
            if not line.startswith("TRIPLE:"):
                continue
            body = line[len("TRIPLE:"):].strip()
            parts = [p.strip() for p in body.split("|")]
            if len(parts) < 3:
                continue
            a, rel, b = parts[0], parts[1], parts[2]
            if not a or not rel or not b:
                continue
            # 0.2.3.0 graph_domain 分桶: parts[3]=domain, parts[4]=kind_A, parts[5]=kind_B
            # 老格式 3 字段 (无 domain/kind) 向后兼容默认 social/person
            graph_domain = parts[3].lower() if len(parts) > 3 and parts[3].lower() in ("social", "knowledge", "tool") else "social"
            kind_a = parts[4].lower() if len(parts) > 4 and parts[4] else "person"
            kind_b = parts[5].lower() if len(parts) > 5 and parts[5] else "person"
            # 0.2.5.2 person 节点按昵称查微信号传 ext_id (认人锚点, 昵称变也合并)
            nick_map = nick_to_wxid or {}
            ext_a = nick_map.get(a) if kind_a == "person" else None
            ext_b = nick_map.get(b) if kind_b == "person" else None
            try:
                na = await self.graph_store.add_node(kind_a, graph_domain, a, source=source,
                                                     occurred_at=occurred_at, ext_id=ext_a)
                nb = await self.graph_store.add_node(kind_b, graph_domain, b, source=source,
                                                     occurred_at=occurred_at, ext_id=ext_b)
                if (na.get("ok") and nb.get("ok")
                        and not na.get("blocked") and not nb.get("blocked")):
                    _edge_res = await self.graph_store.add_edge(
                        na["id"], nb["id"], rel, source=source,
                        occurred_at=occurred_at)
                    count += 1
                    # [0.3.30.4 G16] 图谱关系边进账本 (kind=relationship,
                    # store_ref=边 ID; ledger 失败不阻断构图)。
                    try:
                        from system_memory_god.models import MemoryItem
                        from system_memory_god.storage.memory_ledger import new_memory_id
                        _lg = getattr(self, "_ledger", None)
                        _eid = str((_edge_res or {}).get("id") or "")
                        if _lg is not None and _eid:
                            _lg.record_item(MemoryItem(
                                memory_id=new_memory_id(),
                                kind="relationship", store="graph",
                                store_ref=_eid,
                                content_preview=f"{a} --[{rel}]--> {b}",
                                actor="assistant", speaker="三元组抽取",
                                created_at=occurred_at or time.time(),
                            ))
                    except Exception as _le:
                        logger.debug(
                            f"[Memory Daemon] ledger 关系边记账失败(非致命): {_le}")
            except Exception as e:
                logger.debug(f"[Memory Daemon] 三元组写入异常 (非致命): {e}")

        if count:
            logger.info(f"[Memory Daemon] 图谱自动构图: 抽出 {count} 条关系 (source={source})")

    # 体检输出哨兵黑名单: 这些前缀/取值视为 LLM 异常泄漏, 绝不当作规则
    _CHECKUP_ERROR_SENTINELS = ("error", "api_response", "none", "null", "nan", "exception")
    # 体检安全阀: 解析出的规则数低于原规则库此比例时, 视为输出异常, 拒绝覆盖
    _CHECKUP_MIN_KEEP_RATIO = 0.3

    async def _personality_checkup(self):
        """性格体检：让 LLM 对现有规则进行去重、冲突合并、淘汰不活跃规则。"""
        rules = await self.rule_store.get_all_rules()
        if len(rules) < 5:
            logger.info("[性格体检] 规则数量较少 (不足5条)，跳过体检。")
            await self.rule_store.mark_checkup_attempt("skipped")
            return

        # 关键: 体检发起第一时间即登记冷却, 防止超时/失败后下轮循环立即重试造成 API 风暴
        await self.rule_store.mark_checkup_attempt("started")
        logger.info(f"[性格体检] 启动深度人格体检，当前 {len(rules)} 条规则...")

        rules_text = "\n".join([f"[{r['id']}] {r['text']} (创建: {r['created_at']}, 命中: {r['hit_count']}次)" for r in rules])

        prompt = f"""
你是阿露的潜意识深层整理引擎。请对以下性格规矩列表进行全面体检：

【任务要求】
1. **去重**：合并表述相同/高度相似规则
2. **冲突解决**：如果存在矛盾规则（如"要叮嘱休息"和"不要老妈子语气"），保留更符合最近行为的那条
3. **淘汰**：标记长期不活跃（hit_count为0且创建时间久远）的规则为"已归档"

【输出格式】
请输出精简后的规则列表，每行一条，用序号标注：
1. 规则内容1
2. 规则内容2
...

如果没有需要修改的地方，只输出"无变化"三个字。

当前规则库：
{rules_text}
"""

        try:
            result = await asyncio.wait_for(
                self.llm_client.generate_async(prompt),
                timeout=120.0
            )
        except asyncio.TimeoutError:
            # [数据防护] 超时分支强制中断, 绝对不调用 replace_all_rules, 守护现有规则不被毒化
            logger.error("[性格体检] API 请求超时，体检取消。规则库未改动，绝不覆盖。")
            await self.rule_store.mark_checkup_attempt("timeout")
            return
        except Exception as e:
            # [数据防护] 任何网络/异常分支: 立即中断退出, 绝不触碰 replace_all_rules
            logger.error(f"[性格体检] 异常，规则库未改动，绝不覆盖: {e}")
            await self.rule_store.mark_checkup_attempt("error")
            return

        new_rules = self._parse_checkup_output(result)

        # [数据防护重申] 以下唯一允许写规则库的路径: 必须先通过解析 + 安全阀双重校验
        if not new_rules:
            logger.info("[性格体检] 输出无效或无变化，规则库未改动。")
            await self.rule_store.mark_checkup_attempt("noop")
            return

        # 安全阀: 解析结果远少于原规则库时, 极可能是错误泄漏被当成规则, 拒绝覆盖防毒化
        if len(rules) >= 10 and len(new_rules) < int(len(rules) * self._CHECKUP_MIN_KEEP_RATIO):
            logger.warning(
                f"[性格体检] 解析规则数 {len(new_rules)} 远少于现有 {len(rules)} 条，"
                f"疑似输出异常，拒绝覆盖以防毒化规则库。候选被丢弃: {new_rules}"
            )
            await self.rule_store.mark_checkup_attempt("rejected_degenerate")
            return

        old_count = len(rules)
        await self.rule_store.replace_all_rules(new_rules)
        await self.rule_store.mark_checkup_attempt("applied")
        logger.info(f"[性格体检] 完成！规则从 {old_count} 条精简为 {len(new_rules)} 条。")

    def _parse_checkup_output(self, result: str) -> list:
        """
        严格解析体检输出。只接受编号行 (1./2./3. 或 1)、2)、3)),
        自动跳过前言/解释/哨兵泄漏行。
        """
        result = (result or "").strip()
        if not result:
            return []
        low = result.lower()
        if low.startswith(self._CHECKUP_ERROR_SENTINELS) or result in ("无变化", "无"):
            return []

        new_rules = []
        for line in result.split("\n"):
            line = line.strip()
            if not line:
                continue
            # 仅接受编号前缀行; 前言/解释行 (如 "好的，以下是精简后的规则：") 不匹配, 自动跳过
            m = re.match(r"^\d+[\.)、]\s*(.+)", line)
            if not m:
                continue
            text = m.group(1).strip()
            tlow = text.lower()
            if len(text) < 4:
                continue
            if tlow.startswith(self._CHECKUP_ERROR_SENTINELS):
                continue
            new_rules.append(text)
        return new_rules
