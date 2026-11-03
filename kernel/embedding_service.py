# G:\Aoao_OS\kernel\embedding_service.py
"""
统一嵌入模型服务 -- 全局单例加载 BAAI/bge-m3

修复: 旧版 system_memory 和 knowledge_base 各自独立加载 bge-m3，
      导致内存中驻留两份 ~560MB 的模型权重。
      现改为全局单例，两个 VectorStore 共享同一份模型实例。
"""
import os
import threading

from kernel.logger import get_logger

logger = get_logger(__name__)

_bge_m3_instance = None
_lock = threading.Lock()

# [0.2.23.7 review M4] ONNX 兜底也走模块级单例: bge-m3 加载失败降级时,
# 双 VectorStore 的 LazyEF 各建一份会驻留两份 ONNX 权重 (还各发一次下载)。
_fallback_instance = None
_fallback_lock = threading.Lock()


def _get_fallback_embedding():
    """bge-m3 不可用时的兜底嵌入 (chroma 默认 ONNX 小模型, 单例双检锁)。"""
    global _fallback_instance
    if _fallback_instance is None:
        with _fallback_lock:
            if _fallback_instance is None:
                import chromadb.utils.embedding_functions as ef
                _fallback_instance = ef.ONNXMiniLM_L6_V2()
    return _fallback_instance


def get_bge_m3_embedding():
    """
    获取全局共享的 bge-m3 嵌入模型实例。
    线程安全的双重检查锁定单例模式。
    首次调用时加载模型，后续调用直接返回缓存实例。
    """
    global _bge_m3_instance
    if _bge_m3_instance is None:
        with _lock:
            if _bge_m3_instance is None:
                os.environ.setdefault("HF_HUB_OFFLINE", "1")
                try:
                    import chromadb.utils.embedding_functions as ef
                    _bge_m3_instance = ef.SentenceTransformerEmbeddingFunction(
                        model_name="BAAI/bge-m3"
                    )
                    logger.info("[EmbeddingService] BAAI/bge-m3 全局单例已加载 (共享实例)。")
                except Exception as e:
                    logger.warning(
                        f"[EmbeddingService] bge-m3 加载失败 ({e})，返回 None。"
                        "中文检索效果可能受损。"
                    )
                    return None
    return _bge_m3_instance


class LazyBgeM3EmbeddingFunction:
    """[0.2.23.3 性能刀3] 惰性嵌入函数包装器 (chromadb EF 协议: __call__(input))。

    旧实现: VectorStore.connect() 同步加载 bge-m3 (~560MB, 实测 ~10s) + probe
    首嵌 (~3s), 整体卡 boot 关键路径 13s。chroma 建集合本身不需要模型 (add/query
    才嵌), 故本包装器构造即就绪 (毫秒级), 真模型由调用方在后台线程 warmup()
    加载; 预热未完成前的首次向量调用阻塞等待至就绪 (只发生一次, 之后直通)。
    加载失败降级 chroma 默认 ONNX 小模型 (等价旧实现 EF=None 回退, 中文受损但
    功能在)。线程安全: warmup 单线程调, __call__ 多线程经 Event 栅栏。
    """

    LOAD_TIMEOUT_SEC = 120.0

    def __init__(self) -> None:
        self._ready = threading.Event()
        self._real = None

    def warmed(self) -> bool:
        """模型是否已就绪 (供日志/诊断, 不参与向量路径)。"""
        return self._ready.is_set()

    def warmup(self) -> None:
        """后台线程执行: 加载全局 bge-m3 单例 -> 置就绪栅栏。

        经 asyncio.to_thread 调度。加载失败装 chroma 默认 ONNX 小模型兜底。
        """
        try:
            real = get_bge_m3_embedding()
        except Exception as e:
            logger.warning(f"[EmbeddingService] bge-m3 预热异常: {e}, 尝试默认兜底")
            real = None
        if real is None:
            try:
                real = _get_fallback_embedding()
                logger.warning(
                    "[EmbeddingService] 降级 ONNXMiniLM 默认嵌入 (中文检索效果受损)。"
                )
            except Exception as e:
                logger.error(f"[EmbeddingService] 兜底嵌入构造失败: {e}, 向量功能不可用")
        self._real = real
        self._ready.set()
        logger.info("[EmbeddingService] 嵌入模型预热完成。")

    def wait_until_ready(self, timeout: float | None = None) -> bool:
        """[0.2.23.7 review H1] 同步阻塞至就绪, 返回是否就绪。

        只可在非事件循环线程调 (经 asyncio.to_thread), 供调用方把栅栏等待
        挪出事件循环。timeout 缺省用 LOAD_TIMEOUT_SEC。
        """
        return self._ready.wait(
            self.LOAD_TIMEOUT_SEC if timeout is None else timeout
        )

    def __call__(self, input) -> list:
        """chromadb EF 协议入口。预热未完阻塞等待 (一次性栅栏)。"""
        if not self._ready.wait(self.LOAD_TIMEOUT_SEC):
            logger.error(
                "[EmbeddingService] 嵌入模型预热超时 (%ss), 本次向量调用失败",
                self.LOAD_TIMEOUT_SEC,
            )
            raise TimeoutError("bge-m3 嵌入模型预热超时")
        if self._real is None:
            raise RuntimeError("嵌入模型不可用 (预热失败且无兜底)")
        return self._real(input)
