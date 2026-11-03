# kernel/memory_snapshot.py
"""[0.3.17.0 P4-b] 记忆命名空间 snapshot 一致性热拷贝。

创建 snapshot 模式智能体时, 把源智能体的记忆库一致性拷贝到新命名空间
(此后各自独立演化)。拷贝发生在运行时 (源库在线), 铁律: 禁文件系统级
cp (运行中的 chroma/sqlite 直接拷文件必坏), 全部走数据级一致拷贝:

  - 向量库: chroma get(分页, include embeddings/documents/metadatas) ->
    目标新集合 add 原样灌入。embedding 继承不重嵌入 (省 bge-m3 重算),
    doc_id/metadata 全保留 (P4-a visibility 隐私桶 + hit_count/promoted
    渐进状态无缝迁移)。
  - entities.json / rules.json / checkup_state.json: 读出写入 (JSON 无锁)。
  - graph.db: sqlite3 connection.backup() 官方热备 (WAL 一致快照)。
  - 不拷贝清单 (新库清白起步): reflection_buffer.jsonl (在途未反思) /
    reminders.json (日程属老板不属角色) / edits_audit.jsonl /
    training_pairs.jsonl (审计与导出产物)。

失败语义: 任何一步抛 RuntimeError (带上下文), 由 character_registry.
create_agent 负责整体回滚 (删半成品目录 + 不落 registry)。
"""
import json
import os
import sqlite3
import time

import chromadb
from chromadb.config import Settings

from kernel.logger import get_logger

logger = get_logger(__name__)

COLLECTION_NAME = "memory_stream"
_SNAPSHOT_JSON_FILES = ("entities.json", "rules.json", "checkup_state.json")
_BATCH_SIZE = 500   # chroma get 分页批大小, 防大库一次性灌爆内存


def _chroma_client(db_path: str) -> chromadb.PersistentClient:
    return chromadb.PersistentClient(
        path=db_path,
        settings=Settings(anonymized_telemetry=False),
    )


def _copy_vectors(src_db: str, dst_db: str) -> int:
    """向量数据级拷贝, 返回条数。源库/集合不存在 -> 0 (跳过不报错)。"""
    if not os.path.exists(src_db):
        logger.warning(f"[Snapshot] 源向量库不存在, 跳过向量段: {src_db}")
        return 0
    src_client = _chroma_client(src_db)
    try:
        src_col = src_client.get_collection(name=COLLECTION_NAME)
    except Exception as e:
        logger.warning(
            f"[Snapshot] 源库无 {COLLECTION_NAME} 集合, 跳过向量段: {e}")
        return 0

    dst_client = _chroma_client(dst_db)
    dst_col = dst_client.get_or_create_collection(name=COLLECTION_NAME)
    # [review 整改] 先取全量 id 再按 id 分批 get: 旧 offset 分页在源库在线
    # 被反思/观察守护并发写入时会偏移漂移 -> 静默漏拷; 按 id 取不受插入影响。
    all_ids = src_col.get(include=["metadatas"]).get("ids") or []
    total = 0
    for i in range(0, len(all_ids), _BATCH_SIZE):
        batch_ids = all_ids[i:i + _BATCH_SIZE]
        batch = src_col.get(
            ids=batch_ids,
            include=["documents", "metadatas", "embeddings"],
        )
        ids = batch.get("ids") or []
        if not ids:
            continue
        # add 同时带 embeddings: chroma 不调嵌入函数, 原样落库 (不重嵌入)。
        dst_col.add(
            ids=ids,
            documents=batch.get("documents"),
            metadatas=batch.get("metadatas"),
            embeddings=batch.get("embeddings"),
        )
        total += len(ids)
        logger.info(f"[Snapshot] 向量拷贝进度: {total}/{len(all_ids)} 条")
    return total


def _copy_json_trio(src_soul: str, dst_soul: str) -> int:
    """白名单 JSON 三件内容级拷贝, 返回实际拷贝件数 (缺件跳过)。"""
    copied = 0
    for fname in _SNAPSHOT_JSON_FILES:
        src_path = os.path.join(src_soul, fname)
        # [review 整改] EntityStore 原子写有"主文件转 .bak 与 tmp 上位"之间
        # 的瞬时空窗: 缺件短重试 (50ms x3) 再回退读 .bak, 防整件静默缺失。
        payload = None
        for _attempt in range(3):
            if os.path.exists(src_path):
                try:
                    with open(src_path, "r", encoding="utf-8") as f:
                        payload = json.load(f)
                    break
                except Exception as e:
                    logger.warning(
                        f"[Snapshot] {fname} 读取失败 (将重试): {e}")
            time.sleep(0.05)
        if payload is None:
            bak = src_path + ".bak"
            if os.path.exists(bak):
                try:
                    with open(bak, "r", encoding="utf-8") as f:
                        payload = json.load(f)
                    logger.warning(f"[Snapshot] {fname} 主件缺失, 已用 .bak")
                except Exception as e:
                    logger.warning(f"[Snapshot] {fname} .bak 也不可读: {e}")
        if payload is None:
            continue
        with open(os.path.join(dst_soul, fname), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        copied += 1
    return copied


def _backup_graph_db(src_root: str, dst_root: str) -> bool:
    """graph.db sqlite 官方热备 (一致快照, WAL 安全)。无源库 -> False。"""
    src_db = os.path.join(src_root, "graph.db")
    if not os.path.exists(src_db):
        return False
    src_conn = sqlite3.connect(src_db)
    try:
        dst_conn = sqlite3.connect(os.path.join(dst_root, "graph.db"))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()
    return True


def _ledger_classes():
    """[0.3.39.1] 惰性取账本类 (apps 路径不在 sys.path 时自补, 测试场景兜底)。"""
    try:
        from system_memory_god.storage.memory_ledger import (
            MemoryLedger, new_memory_id)
        from system_memory_god.models import MemoryItem
    except ImportError:
        import sys as _sys
        _sys.path.insert(0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "apps"))
        from system_memory_god.storage.memory_ledger import (
            MemoryLedger, new_memory_id)
        from system_memory_god.models import MemoryItem
    return MemoryLedger, new_memory_id, MemoryItem


def _record_snapshot_ledger(dst_root: str, src_root: str, stats: dict) -> None:
    """[0.3.39.1 Batch 3] snapshot 生命周期入账: 目标根账本记锚点行
    (store=snapshot) + snapshot_created 事件。失败非致命, 不挡快照主流程。"""
    try:
        MemoryLedger, new_memory_id, MemoryItem = _ledger_classes()
        ledger = MemoryLedger(os.path.join(dst_root, "soul", "memory_ledger.db"))
        try:
            mid = new_memory_id()
            ledger.record_item(MemoryItem(
                memory_id=mid, kind="observation", store="snapshot",
                store_ref=os.path.basename(src_root.rstrip(os.sep)) or src_root,
                content_preview=(
                    f"snapshot from {os.path.basename(src_root.rstrip(os.sep))} "
                    f"(vectors={stats.get('vectors')}, "
                    f"json_files={stats.get('json_files')}, "
                    f"graph={stats.get('graph')})"),
                actor="system", status="active",
            ))
            ledger.record_event(mid, "snapshot_created", actor="system",
                                reason=f"源={src_root}")
        finally:
            ledger.close()
    except Exception as e:
        logger.warning(f"[Snapshot] ledger 入账失败(非致命): {e}")


def snapshot_memory(src_root: str, dst_root: str) -> dict:
    """把 src_root 记忆树一致性拷贝到 dst_root (两目录均为 memory/ 根,
    内含 soul/ 与 graph.db, 对齐 SystemMemory 的 data_dir/graph_data_dir)。

    返回统计 {"vectors": int, "json_files": int, "graph": bool}。
    失败抛 RuntimeError, 调用方 (create_agent) 负责回滚半成品。
    """
    try:
        src_soul = os.path.join(src_root, "soul")
        dst_soul = os.path.join(dst_root, "soul")
        os.makedirs(dst_soul, exist_ok=True)
        stats = {
            "vectors": _copy_vectors(
                os.path.join(src_soul, "smallville_db"),
                os.path.join(dst_soul, "smallville_db"),
            ),
            "json_files": _copy_json_trio(src_soul, dst_soul),
            "graph": _backup_graph_db(src_root, dst_root),
        }
    except Exception as e:
        raise RuntimeError(
            f"[Snapshot] {src_root} -> {dst_root} 拷贝失败: {e}") from e
    finally:
        # [review 整改] 释放 chroma 共享实例缓存: Windows 下缓存的 client
        # 持 sqlite 句柄锁住 dst 文件, 失败回滚的 rmtree 会静默删不掉。
        # 运行中的 VectorStore 自持 client 引用, 逐出缓存不影响它。
        try:
            from chromadb import SharedSystemClient
            SharedSystemClient.clear_system_cache()
        except Exception:
            pass
    # [0.3.39.1 Batch 3] 生命周期入账 (双层非致命: 调用点+函数内各一道)
    try:
        _record_snapshot_ledger(dst_root, src_root, stats)
    except Exception as e:
        logger.warning(f"[Snapshot] ledger 入账失败(非致命): {e}")
    logger.info(f"[Snapshot] 完成 {src_root} -> {dst_root}: {stats}")
    return stats
