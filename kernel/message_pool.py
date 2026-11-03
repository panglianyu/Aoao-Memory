# G:\Aoao_OS\kernel\message_pool.py
"""
轻量消息池 (0.3.25.1 Phase 2) -- 消息聚合页数据基础。

职责:
  - 全渠道入站消息统一入池 (通道侧全量进池不回不过滤, 镜像微信"蓄水池"语义)。
  - 四节点状态打标: 入池 pooled -> 交 Brain -> 回复出站 replied -> 记忆写入 memory。
  - 聚合查询: 分页 + 筛选 (平台/聊天对象/是否回复/是否提及/是否写记忆/时间范围)。
  - 落盘: data/message_pool/YYYY-MM-DD.jsonl 按天 append-only; 内存 FIFO 上限 5000 条;
    保留 7 天, gc 清理旧文件。

铁律:
  - 不改 Message (frozen + metadata 不可变), 状态用池内独立 id -> status map。
  - 不调 LLM, 不写记忆库, 不决定回不回 (那是 speak_gate 的职责)。
  - 打标点全部 fire-and-forget, 绝不影响消息主路径。

数据文件行结构 (data/message_pool/2026-08-04.jsonl):
  {"id": "msg_xxx", "ts": 1722700000.0, "source": "wechat", "sender": "胖脸鱼",
   "chat_name": "学习交流群", "content": "...", "msg_type": "user_input",
   "is_master": false, "mentioned": true}
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

# 内存容量与保留天数
_MEM_CAPACITY = 5000
_KEEP_DAYS = 7
# 内容截断长度 (聚合页展示用, 防长文撑爆 jsonl)
_CONTENT_LIMIT = 500

_MENTION_KEYWORDS = ("@阿露", "阿露", "Ailu", "Ailu")

# 打标阶段
STAGE_POOLED = "pooled"
STAGE_BRAIN = "brain"
STAGE_REPLIED = "replied"
STAGE_MEMORY = "memory"

# 用户输入类 msg_type 才入池 (系统帧/语音状态等不进聚合流)
_POOLABLE_TYPES = frozenset({"user_input", "user_speak"})


def _mentioned(content: str) -> bool:
    return any(kw in content for kw in _MENTION_KEYWORDS)


class MessagePool:
    """消息池: 入池 + 打标 + 查询 + 落盘。单例挂 bus.pool (app_bus 热路径 hook)。"""

    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        self._entries: "OrderedDict[str, dict]" = OrderedDict()
        self._status: Dict[str, Dict[str, Optional[float]]] = {}
        self._lock = asyncio.Lock()
        self._pending_writes: List[dict] = []
        self._flush_task: Optional[asyncio.Task] = None

    # ---------------------------------------------------------- 入池

    def record(self, msg: Any) -> None:
        """入池 (同步, 热路径可安全调用): 内存记录 + 异步批量落盘。"""
        try:
            msg_type = getattr(msg, "msg_type", None)
            type_value = getattr(msg_type, "value", None) or str(msg_type)
            if type_value not in _POOLABLE_TYPES:
                return
            msg_id = getattr(msg, "id", "")
            if not msg_id or msg_id in self._entries:
                return
            content = str(getattr(msg, "content", "") or "")
            metadata = getattr(msg, "metadata", {}) or {}
            ts = float(getattr(msg, "timestamp", 0) or time.time())
            entry = {
                "id": msg_id,
                "ts": ts,
                "source": str(getattr(msg, "source", "") or ""),
                "sender": str(metadata.get("sender", "") or ""),
                "chat_name": str(metadata.get("chat_name", "") or ""),
                "content": content[:_CONTENT_LIMIT],
                "msg_type": type_value,
                "is_master": bool(metadata.get("is_master", False)),
                "mentioned": _mentioned(content),
            }
            self._entries[msg_id] = entry
            self._status[msg_id] = {STAGE_POOLED: ts, STAGE_BRAIN: None,
                                    STAGE_REPLIED: None, STAGE_MEMORY: None}
            self._pending_writes.append(entry)
            self._trim()
            # 惰性建 flush 任务: 无运行中 loop (纯测试环境) 不建, 靠 close() 落盘
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None and (self._flush_task is None or self._flush_task.done()):
                self._flush_task = loop.create_task(self._flush_loop())
        except Exception as e:  # 热路径: 任何异常都不得影响消息主流程
            logger.debug(f"[message_pool] 入池失败(非致命): {e}")

    def mark(self, msg_id: str, stage: str) -> None:
        """打标 (同步, fire-and-forget 语义): 状态只存内存, 重启后重置为未处理。"""
        try:
            status = self._status.get(msg_id)
            if status is not None and status.get(stage) is None:
                status[stage] = time.time()
        except Exception as e:
            logger.debug(f"[message_pool] 打标失败(非致命): {e}")

    def mark_batch(self, msg_ids: List[str], stage: str) -> None:
        for mid in msg_ids:
            self.mark(mid, stage)

    def _trim(self) -> None:
        while len(self._entries) > _MEM_CAPACITY:
            oldest_id, _ = self._entries.popitem(last=False)
            self._status.pop(oldest_id, None)

    # ---------------------------------------------------------- 落盘

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(3.0)
            if not self._pending_writes:
                continue
            await self._flush_now()

    async def _flush_now(self) -> None:
        async with self._lock:
            batch = self._pending_writes
            self._pending_writes = []
            if not batch:
                return
            try:
                await asyncio.to_thread(self._append_batch, batch)
            except Exception as e:
                logger.warning(f"[message_pool] 落盘失败(非致命), 回塞重试: {e}")
                self._pending_writes = batch + self._pending_writes

    def _append_batch(self, batch: List[dict]) -> None:
        os.makedirs(self.dir_path, exist_ok=True)
        day = time.strftime("%Y-%m-%d", time.localtime(batch[0]["ts"]))
        path = os.path.join(self.dir_path, f"{day}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            for entry in batch:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    async def close(self) -> None:
        if self._flush_task is not None and not self._flush_task.done():
            self._flush_task.cancel()
        await self._flush_now()

    # ---------------------------------------------------------- 查询

    def query(
        self,
        *,
        platform: str = "",
        target: str = "",
        replied: Optional[bool] = None,
        mentioned: Optional[bool] = None,
        memory: Optional[bool] = None,
        since: float = 0.0,
        until: float = 0.0,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """聚合查询: 最新在前。返回 {"total", "items"}。

        replied/mentioned/memory 为 None 时不筛该维度; True/False 严格匹配。
        """
        rows = []
        for msg_id, entry in self._entries.items():
            if platform and entry["source"] != platform:
                continue
            if target and entry["chat_name"] != target:
                continue
            if since and entry["ts"] < since:
                continue
            if until and entry["ts"] > until:
                continue
            status = self._status.get(msg_id, {})
            if replied is not None and bool(status.get(STAGE_REPLIED)) is not replied:
                continue
            if mentioned is not None and entry["mentioned"] is not mentioned:
                continue
            if memory is not None and bool(status.get(STAGE_MEMORY)) is not memory:
                continue
            row = {**entry, "status": status}
            rows.append(row)
        rows.sort(key=lambda r: r["ts"], reverse=True)
        total = len(rows)
        return {"total": total, "items": rows[offset : offset + limit]}

    def get(self, msg_id: str) -> Optional[dict]:
        """按 id 取单条入池记录 (聚合页直接回复用); 不存在返回 None。"""
        entry = self._entries.get(msg_id)
        if entry is None:
            return None
        return {**entry, "status": self._status.get(msg_id, {})}

    def snapshot(self, limit: int = 100) -> List[dict]:
        rows = list(self._entries.values())
        rows.sort(key=lambda r: r["ts"], reverse=True)
        return [{**r, "status": self._status.get(r["id"], {})} for r in rows[:limit]]

    # ---------------------------------------------------------- gc

    async def gc(self) -> int:
        """删除超过保留天数的 jsonl; 返回删除文件数。"""

        def _do_gc() -> int:
            if not os.path.isdir(self.dir_path):
                return 0
            removed = 0
            cutoff = time.time() - _KEEP_DAYS * 86400
            for name in os.listdir(self.dir_path):
                if not name.endswith(".jsonl"):
                    continue
                path = os.path.join(self.dir_path, name)
                try:
                    if os.path.getmtime(path) < cutoff:
                        os.remove(path)
                        removed += 1
                except OSError as e:
                    logger.warning(f"[message_pool] gc 删除失败: {e}")
            return removed

        return await asyncio.to_thread(_do_gc)
