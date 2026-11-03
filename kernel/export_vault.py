# G:\Aoao_OS\kernel\export_vault.py
"""
文件归档保险库 (ExportVault)

所有由 doc_gen 生成的文件的物理落盘与索引管理中枢。
文件按日期分区存储于 data/exports/%Y-%m-%d/ 目录下，
并在 data/exports/manifest.json 中维护结构化索引表。

设计原则:
  - 路径锁定在项目母文件夹内，绝不依赖系统盘全局路径
  - 写入前强制执行 os.makedirs(..., exist_ok=True)
  - manifest.json 采用追加模式，保证历史记录不可覆盖
  - 文件读写通过 asyncio.to_thread 包装，不阻塞事件循环
"""
import os
import json
import time
import uuid
import asyncio
import datetime
from typing import Any, Dict, List, Optional, Tuple

from kernel.logger import get_logger

logger = get_logger(__name__)


class ExportVault:
    """
    本地文件归档保险库。

    职责:
      - 为新文件分配日期分区路径并确保目录存在
      - 将文件元数据登记到 manifest.json 索引表
      - 提供按 file_id / 日期查询 manifest 记录的能力
    """

    def __init__(self, base_dir: str):
        """
        Args:
            base_dir: 项目根目录的绝对路径 (包含 data/ 子目录的那一层)
        """
        self.vault_dir = os.path.join(base_dir, "data", "exports")
        self.manifest_path = os.path.join(self.vault_dir, "manifest.json")
        self._write_lock = asyncio.Lock()

    def _ensure_vault_dir(self) -> None:
        """确保保险库根目录存在。"""
        os.makedirs(self.vault_dir, exist_ok=True)

    def _today_subdir(self) -> str:
        """返回今天的日期分区目录名 (%Y-%m-%d)。"""
        return datetime.datetime.now().strftime("%Y-%m-%d")

    def _generate_file_id(self) -> str:
        """
        生成唯一文件标识。

        格式: {YYYYMMDD}_{HHmmss}_{uuid4_hex[:8]}
        示例: 20260602_143025_a1b2c3d4
        """
        now = datetime.datetime.now()
        date_part = now.strftime("%Y%m%d")
        time_part = now.strftime("%H%M%S")
        rand_part = uuid.uuid4().hex[:8]
        return f"{date_part}_{time_part}_{rand_part}"

    def allocate_path(self, filename: str) -> Tuple[str, str]:
        """
        为新文件分配日期分区路径，确保目录存在。

        Args:
            filename: 原始文件名 (含扩展名，如 "报表.xlsx")

        Returns:
            (file_id, abs_path) 元组
            - file_id: 唯一文件标识字符串
            - abs_path: 文件的绝对路径 (目录已创建)
        """
        self._ensure_vault_dir()

        file_id = self._generate_file_id()
        today = self._today_subdir()
        day_dir = os.path.join(self.vault_dir, today)
        os.makedirs(day_dir, exist_ok=True)

        abs_path = os.path.join(day_dir, f"{file_id}_{filename}")

        # [0.2.18.3] 最后一道闸: 最终路径必须落在 vault 目录内
        # (防御 filename 参数携带 .. 的穿越, 审计 CRITICAL-2 纵深)
        vault_root = os.path.abspath(self.vault_dir)
        if os.path.commonpath([vault_root, os.path.abspath(abs_path)]) != vault_root:
            raise ValueError(f"[ExportVault] 非法文件名 (路径逃逸): {filename!r}")

        logger.info(
            f"[ExportVault] 路径分配: file_id={file_id}, "
            f"path={abs_path}"
        )
        return file_id, abs_path

    async def register(
        self,
        file_id: str,
        local_path: str,
        original_name: str = "",
        mime_type: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        将文件登记到 manifest.json 索引表。

        采用追加模式: 读取现有 manifest -> 追加新记录 -> 原子写入。
        使用 asyncio.Lock 保证并发安全。

        Args:
            file_id: 文件唯一标识 (由 allocate_path 生成)
            local_path: 文件的绝对路径
            original_name: 原始文件名
            mime_type: MIME 类型 (如 application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)
            extra: 附加元数据字典

        Returns:
            登记记录字典
        """
        self._ensure_vault_dir()

        file_size = 0
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)

        record = {
            "file_id": file_id,
            "local_path": local_path,
            "original_name": original_name,
            "mime_type": mime_type,
            "file_size": file_size,
            "timestamp": time.time(),
            "iso_timestamp": datetime.datetime.now().isoformat(),
        }
        if extra:
            record.update(extra)

        async with self._write_lock:
            manifest = await self._load_manifest()
            manifest.append(record)
            await self._save_manifest(manifest)

        logger.info(
            f"[ExportVault] 已登记: file_id={file_id}, "
            f"size={file_size}B, path={local_path}"
        )
        return record

    async def query(
        self,
        file_id: str = "",
        date: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        按条件查询 manifest 记录。

        Args:
            file_id: 按 file_id 精确查询
            date: 按日期过滤 (%Y-%m-%d 格式)
            limit: 最大返回条数

        Returns:
            匹配的记录列表 (按时间倒序)
        """
        manifest = await self._load_manifest()

        results = manifest
        if file_id:
            results = [r for r in results if r.get("file_id") == file_id]
        if date:
            results = [
                r for r in results
                if r.get("local_path", "").replace("\\", "/").find(f"/{date}/") != -1
            ]

        # 按时间倒序，取前 limit 条
        results.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
        return results[:limit]

    async def _load_manifest(self) -> List[Dict[str, Any]]:
        """从磁盘加载 manifest.json。文件不存在则返回空列表。"""
        if not os.path.exists(self.manifest_path):
            return []
        try:
            data = await asyncio.to_thread(self._read_manifest_sync)
            if isinstance(data, list):
                return data
            return []
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"[ExportVault] manifest.json 读取失败: {e}")
            return []

    def _read_manifest_sync(self) -> List[Dict[str, Any]]:
        """同步读取 manifest.json (供 asyncio.to_thread 调用)。"""
        with open(self.manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)

    async def _save_manifest(self, manifest: List[Dict[str, Any]]) -> None:
        """原子写入 manifest.json 到磁盘 (不阻塞事件循环)。"""
        try:
            await asyncio.to_thread(self._write_manifest_sync, manifest)
        except OSError as e:
            logger.error(f"[ExportVault] manifest.json 写入失败: {e}")
            raise

    def _write_manifest_sync(self, manifest: List[Dict[str, Any]]) -> None:
        """同步写入 manifest.json (供 asyncio.to_thread 调用)。"""
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
