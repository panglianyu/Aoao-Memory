# -*- coding: utf-8 -*-
"""kernel/data_layout.py -- [0.3.37.0] data/ 目录规范与启动自愈迁移。

背景: data/ 顶层散落文件混杂死残留 (journal/实体副本等, 由
scripts/clean_data_root.py 清理) 与现役机器状态件。规范: 顶层只放
目录 + README.md, 现役散落三件归位:

    behavior_log.jsonl       -> data/logs/   (行为自查日志, kernel/behavior_log.py)
    mcp_market_cache.json    -> data/tmp/    (24h 缓存, kernel/mcp_market.py)
    manual_disconnected.json -> data/state/  (手动断开标记, kernel/app_bus.py)

migrate_once() 在 boot() 首行自愈 (console/web 同走单口):
旧存新无 -> 移; 新旧同在 -> 保新, 旧改名 .orphan 留证; 旧无 -> 无操作
(不凭空建目录, 写者按需 makedirs)。幂等, 可重复执行。

目录规范全文见 data/README.md (权威文档)。
"""
import os

from kernel.logger import get_logger

logger = get_logger(__name__)

# 项目根 (本文件在 kernel/ 下, 上跳一层)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(DATA_DIR, "logs")
TMP_DIR = os.path.join(DATA_DIR, "tmp")
STATE_DIR = os.path.join(DATA_DIR, "state")

# 现役散落三件迁移映射: (旧相对路径, 新相对路径)
MIGRATIONS = (
    ("behavior_log.jsonl", os.path.join("logs", "behavior_log.jsonl")),
    ("mcp_market_cache.json", os.path.join("tmp", "mcp_market_cache.json")),
    ("manual_disconnected.json", os.path.join("state", "manual_disconnected.json")),
)


def migrate_once(data_dir: str = DATA_DIR) -> list[str]:
    """启动自愈迁移: data/ 顶层现役散落件归位 (幂等)。

    四态: 旧存新无 -> 移; 新旧同在 -> 保新, 旧改名 <旧名>.orphan 留证;
    旧无 -> 无操作; 迁移异常 -> 告警不挡启动 (逐件隔离, 不串扰)。
    返回迁移报告行列表 (空列表 = 零操作)。
    """
    report: list[str] = []
    for old_rel, new_rel in MIGRATIONS:
        old_path = os.path.join(data_dir, old_rel)
        if not os.path.exists(old_path):
            continue
        new_path = os.path.join(data_dir, new_rel)
        try:
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            if os.path.exists(new_path):
                orphan = old_path + ".orphan"
                os.replace(old_path, orphan)
                report.append(f"keep-new-orphan-old: {old_rel}")
                logger.warning(
                    f"[DataLayout] 新旧路径同在, 保新隔旧: {old_rel} -> .orphan"
                )
            else:
                os.replace(old_path, new_path)
                report.append(f"migrated: {old_rel} -> {new_rel}")
                logger.info(f"[DataLayout] 自愈迁移: {old_rel} -> {new_rel}")
        except OSError as e:
            logger.warning(f"[DataLayout] 迁移失败(非致命): {old_rel}: {e}")
    return report
