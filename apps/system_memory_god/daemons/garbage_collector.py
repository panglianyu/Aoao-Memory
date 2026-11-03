import asyncio
from system_memory_god.storage.vector_store import VectorStore
from kernel.logger import get_logger

logger = get_logger(__name__)


class GarbageCollectorDaemon:
    # R4: 情景摘要 (episode) 保留 90 天。旧摘要已由 Smallville recency 重排压低得分,
    # 超长周期清理约束 ChromaDB 尺寸单调增长。TTL 设为常量便于调。
    EPISODE_TTL_SECONDS = 90 * 24 * 3600

    def __init__(self, vector_store: VectorStore, entity_store=None):
        self.vector_store = vector_store
        self.entity_store = entity_store  # v0.6.0 双轴: attention 衰减用
        self.task = None
        # 设定短期记忆的存活周期：默认 3 天 (72小时)。
        # 超过 3 天的日常寒暄碎片会被彻底抹除，让大脑保持清爽。
        self.obs_ttl_seconds = 3 * 24 * 3600

    def start(self):
        self.task = asyncio.create_task(self._gc_loop())

    def stop(self):
        if self.task:
            self.task.cancel()

    async def _gc_loop(self):
        while True:
            # 每天检查一次 (86400秒)，此处为了防止干扰你的前台对话，设为极其缓慢的频次
            # 如果你想立刻测试看效果，可以改成 await asyncio.sleep(60)，并把上面的 ttl 改成 60 秒
            await asyncio.sleep(86400) 
            
            logger.info("[Memory GC] 启动深度睡眠神经清理机制 (Memory Decay)...")
            # 强行剥离异步总线，进入 IO 线程进行数据库清理
            deleted_obs = await asyncio.to_thread(
                self.vector_store.prune_old_memories, "obs", self.obs_ttl_seconds
            )
            # R4: episode 情景摘要长周期清理, 约束向量库单调增长
            deleted_ep = await asyncio.to_thread(
                self.vector_store.prune_old_memories, "episode", self.EPISODE_TTL_SECONDS
            )

            if (deleted_obs + deleted_ep) > 0:
                logger.info(f"[Memory GC] 修剪 obs {deleted_obs} 条 + episode {deleted_ep} 条过期记忆。")

            # v0.6.0 双轴: attention 缓衰减 (向 baseline 50 回归), favorability 不动
            if self.entity_store is not None:
                try:
                    decayed = await self.entity_store.decay_all_attention(factor=0.95)
                    if decayed > 0:
                        logger.info(f"[Memory GC] attention 衰减回归 {decayed} 个 person。")
                except Exception as e:
                    logger.warning(f"[Memory GC] attention 衰减异常(非致命): {e}")

            # [0.2.9.0] 兴趣衰减 pass: 一律衰减 (阻尼表自动读), 触底遗忘 (死兴趣掉出闸门 top-N)
            if self.entity_store is not None:
                try:
                    decayed_interest = await self.entity_store.decay_interests(factor=0.9, floor=0.3)
                    if decayed_interest > 0:
                        logger.info(f"[Memory GC] 兴趣衰减 {decayed_interest} 个话题。")
                except Exception as e:
                    logger.warning(f"[Memory GC] 兴趣衰减异常(非致命): {e}")