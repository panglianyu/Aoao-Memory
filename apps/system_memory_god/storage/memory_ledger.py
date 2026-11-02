# -*- coding: utf-8 -*-
"""记忆升级工程 Phase 1 -- Memory Ledger 来源账本 (SQLite)。

规格 4.1:
- 为新写入生成稳定 memory_id (mem_ + UUID4), 不再用纯毫秒时间戳;
- 记录来源事件和派生关系 (memory_derivations);
- 记录生命周期变化 (memory_events, 只追加不覆盖);
- 记录召回检查和治理任务审计 (recall_runs / recall_candidates, 默认只存脱敏摘要);
- 为 UI 提供 provenance 查询, 不承担向量检索或事实真值存储。

设计约束 (规格):
- 新数据必须写 ledger; 老数据允许 memory_id=null, 懒解析 legacy_* 只读标识;
- ledger 写失败不得阻断现有主消息链, 但必须记告警和 reconciliation backlog;
- 显式 schema_version + 幂等迁移; 单文件 SQLite 便于 snapshot/备份;
- 进程重启后任务和审计仍存在 (可重连游标)。

写路径全部经 _try_write 兜底: 失败落 backlog (内存 + 可选落盘), 不抛给调用方。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Callable, Optional

from kernel.memory_pipeline_models import PipelineScope, SourceRevision, canonical, derived_memory_id, identity
from .pipeline_database import PipelineDatabase, paged_rows

from kernel.logger import get_logger

from system_memory_god.models import (
    MemoryDerivation,
    MemoryEvent,
    MemoryItem,
    RecallCandidate,
    RecallRun,
)

logger = get_logger(__name__)

_SCHEMA_VERSION = 2

_SOURCE_DDL = """
CREATE TABLE memory_items (
 memory_id TEXT PRIMARY KEY, store TEXT NOT NULL, store_ref TEXT NOT NULL,
 item_json TEXT NOT NULL, UNIQUE(store,store_ref));
CREATE TABLE memory_sources (
 memory_id TEXT NOT NULL REFERENCES memory_items(memory_id), raw_message_id TEXT NOT NULL,
 revision INTEGER NOT NULL, source_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(memory_id,source_json));
CREATE INDEX sources_raw ON memory_sources(raw_message_id,revision);
CREATE TABLE memory_events (
 memory_id TEXT NOT NULL REFERENCES memory_items(memory_id), event TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(memory_id,event));
"""


def validate_ledger_ids(memory_ids: tuple[str, ...]) -> None:
    if type(memory_ids) is not tuple or not 1 <= len(memory_ids) <= 100:
        raise ValueError("invalid ledger inspection")
    for memory_id in memory_ids:
        identity(memory_id)


def query_ledger_ids(db: sqlite3.Connection,
                     memory_ids: tuple[str, ...]) -> dict[str, dict]:
    """Shared provenance query; connection selection remains with the caller."""
    validate_ledger_ids(memory_ids)
    marks = ",".join("?" for _ in memory_ids)
    items = set()
    for memory_id, store, store_ref, encoded in db.execute(
            "SELECT memory_id,store,store_ref,item_json FROM memory_items WHERE memory_id IN ("
            + marks + ")", memory_ids):
        item = MemoryItem(**json.loads(encoded))
        if (item.memory_id != memory_id or item.store != store or item.store_ref != store_ref):
            raise ValueError("ledger item column conflict")
        if item.status == "active" or store == "rule" and item.status == "candidate":
            items.add(memory_id)
    rows = db.execute("SELECT memory_id,raw_message_id,revision,source_json "
                      "FROM memory_sources WHERE memory_id IN ("
                      + marks + ")", memory_ids).fetchall()
    sources: dict[str, list[SourceRevision]] = {}
    for memory_id, raw_message_id, revision, encoded in rows:
        source = SourceRevision(**json.loads(encoded))
        if source.raw_message_id != raw_message_id or source.revision != revision:
            raise ValueError("ledger source column conflict")
        sources.setdefault(memory_id, []).append(source)
    return {memory_id: {"has_item": memory_id in items,
                        "sources": tuple(sorted(sources.get(memory_id, ())))}
            for memory_id in memory_ids if memory_id in items or memory_id in sources}


class SourceLedger:
    """Strict new-partition ledger; does not change legacy MemoryLedger behavior.

    A successful commit attests only this SQLite transaction, not that a domain
    result or Raw revision exists. The later worker validates those independently.
    No backlog, model calls, publication, or production activation here.
    """

    def __init__(self, root: Path, scope: PipelineScope):
        self._db = PipelineDatabase(root, scope, "source_ledger.db", _SOURCE_DDL, 0x414C534C,
                                    version=2)

    def validate_existing(self) -> None:
        self._db.validate_existing()

    def purge_inventory(self, raw_message_id: str,
                        validate_sources: Callable[[tuple[tuple[SourceRevision, str], ...]], None]
                        ) -> dict[str, tuple[MemoryItem, tuple[SourceRevision, ...]]]:
        """Inspect the entire isolated ledger, including hidden and orphan sources."""
        identity(raw_message_id)
        target: dict[str, tuple[MemoryItem, tuple[SourceRevision, ...]]] = {}
        with self._db.readonly_snapshot() as db:
            if db.execute(
                "SELECT 1 FROM memory_sources s LEFT JOIN memory_items m "
                "ON m.memory_id=s.memory_id WHERE m.memory_id IS NULL LIMIT 1"
            ).fetchone() is not None:
                raise ValueError("ledger orphan source")
            if db.execute(
                "SELECT 1 FROM memory_items m LEFT JOIN memory_sources s "
                "ON m.memory_id=s.memory_id WHERE s.memory_id IS NULL LIMIT 1"
            ).fetchone() is not None:
                raise ValueError("ledger item missing source")
            if db.execute(
                "SELECT 1 FROM memory_sources GROUP BY memory_id HAVING count(*)<>1 LIMIT 1"
            ).fetchone() is not None:
                raise ValueError("ledger shared source inventory conflict")
            batch: list[tuple[SourceRevision, str]] = []
            prior_encoded = None
            stable_ids: frozenset[str] = frozenset()

            for row in paged_rows(db,
                                  "SELECT m.memory_id,m.store,m.store_ref,m.item_json,"
                                  "s.raw_message_id,s.revision,s.source_json "
                                  "FROM memory_sources s JOIN memory_items m "
                                  "ON m.memory_id=s.memory_id "
                                  "ORDER BY s.raw_message_id,s.revision"):
                memory_id, store, store_ref, encoded, column_raw, revision, source_json = row
                item = MemoryItem(**json.loads(encoded))
                if (item.memory_id != memory_id or item.store != store
                        or item.store_ref != store_ref or canonical(asdict(item)) != encoded
                        or store_ref != "evidence:" + memory_id
                        or store not in {"vector", "entity", "rule", "graph", "journal", "schedule"}):
                    raise ValueError("ledger item inventory conflict")
                source = SourceRevision(**json.loads(source_json))
                if source_json != prior_encoded:
                    stable_ids = frozenset(derived_memory_id(source.task_id, index)
                                           for index in range(100))
                    prior_encoded = source_json
                if (source.raw_message_id != column_raw or source.revision != revision
                        or canonical(asdict(source)) != source_json
                        or memory_id not in stable_ids):
                    raise ValueError("ledger source inventory conflict")
                batch.append((source, store))
                if len(batch) == 256:
                    validate_sources(tuple(batch))
                    batch.clear()
                if source.raw_message_id == raw_message_id:
                    target[memory_id] = (item, (source,))
            if batch:
                validate_sources(tuple(batch))
            if db.execute(
                "SELECT 1 FROM memory_events e LEFT JOIN memory_items m "
                "ON m.memory_id=e.memory_id WHERE m.memory_id IS NULL "
                "OR e.event<>'created' LIMIT 1"
            ).fetchone() is not None:
                raise ValueError("ledger event inventory conflict")
            if db.execute(
                "SELECT 1 FROM memory_items m LEFT JOIN memory_events e "
                "ON m.memory_id=e.memory_id WHERE e.memory_id IS NULL LIMIT 1"
            ).fetchone() is not None:
                raise ValueError("ledger item missing event")
        return target

    def commit_source(self, item: MemoryItem, sources: tuple[SourceRevision, ...]) -> str:
        if type(item) is not MemoryItem or type(sources) is not tuple or not 1 <= len(sources) <= 100:
            raise ValueError("invalid provenance submission")
        if any(type(source) is not SourceRevision for source in sources) or len(set(sources)) != len(sources):
            raise ValueError("invalid provenance sources")
        if not isinstance(item.memory_id, str):
            raise ValueError("explicit memory identity required")
        identity(item.memory_id)
        identity(item.store_ref)
        if item.store not in {"vector", "entity", "rule", "graph", "journal", "schedule"}:
            raise ValueError("invalid provenance domain")
        if type(item.content_preview) is not str or len(item.content_preview) > 160:
            raise ValueError("invalid provenance preview")
        payload = canonical(asdict(item))
        source_payloads = tuple(canonical(asdict(source)) for source in sorted(sources))
        with self._db.transaction() as db:
            existing = db.execute("SELECT item_json FROM memory_items WHERE memory_id=?",
                                  (item.memory_id,)).fetchone()
            if existing is not None:
                previous = tuple(row[0] for row in db.execute(
                    "SELECT source_json FROM memory_sources WHERE memory_id=? ORDER BY source_json",
                    (item.memory_id,)))
                if existing[0] != payload or previous != tuple(sorted(source_payloads)):
                    raise ValueError("provenance conflict")
                return "existing"
            committed_at = datetime.now(timezone.utc).isoformat()
            db.execute("INSERT INTO memory_items VALUES (?,?,?,?)",
                       (item.memory_id, item.store, item.store_ref, payload))
            db.executemany("INSERT INTO memory_sources VALUES (?,?,?,?,?)", [
                (item.memory_id, source.raw_message_id, source.revision, encoded, committed_at)
                for source, encoded in zip(sorted(sources), source_payloads)
            ])
            db.execute("INSERT INTO memory_events VALUES (?, 'created', ?)",
                       (item.memory_id, committed_at))
        return "inserted"

    def sources_of(self, memory_id: str) -> tuple[SourceRevision, ...]:
        identity(memory_id)
        with self._db.transaction() as db:
            rows = db.execute("SELECT source_json FROM memory_sources WHERE memory_id=?",
                              (memory_id,)).fetchall()
        return tuple(sorted(SourceRevision(**json.loads(row[0])) for row in rows))

    def inspect_ids(self, memory_ids: tuple[str, ...]) -> dict[str, dict]:
        validate_ledger_ids(memory_ids)
        with self._db.transaction() as db:
            return query_ledger_ids(db, memory_ids)

    def contains_item(self, memory_id: str) -> bool:
        """Strict source-ledger presence check for purge verification."""
        identity(memory_id)
        with self._db.transaction() as db:
            return db.execute("SELECT 1 FROM memory_items WHERE memory_id=?",
                              (memory_id,)).fetchone() is not None

    def purge_one(self, memory_id: str, raw_message_id: str) -> None:
        """Clear a source-granular item only when every source is this message."""
        identity(memory_id)
        identity(raw_message_id)
        with self._db.transaction() as db:
            rows = db.execute("SELECT raw_message_id FROM memory_sources WHERE memory_id=?",
                              (memory_id,)).fetchall()
            if not rows:
                if db.execute("SELECT 1 FROM memory_items WHERE memory_id=?",
                              (memory_id,)).fetchone():
                    raise ValueError("ledger source missing")
                return
            if any(row[0] != raw_message_id for row in rows):
                raise ValueError("ledger purge source conflict")
            db.execute("DELETE FROM memory_sources WHERE memory_id=?", (memory_id,))
            db.execute("DELETE FROM memory_events WHERE memory_id=?", (memory_id,))
            db.execute("DELETE FROM memory_items WHERE memory_id=?", (memory_id,))

    def memories_from(self, raw_message_id: str, revision: int) -> tuple[MemoryItem, ...]:
        identity(raw_message_id)
        if type(revision) is not int or not 1 <= revision <= 2**63 - 1:
            raise ValueError("invalid source revision")
        with self._db.transaction() as db:
            rows = db.execute(
                "SELECT DISTINCT m.memory_id,m.item_json FROM memory_items m JOIN memory_sources s "
                "ON m.memory_id=s.memory_id WHERE s.raw_message_id=? AND s.revision=? ORDER BY m.memory_id",
                (raw_message_id, revision)).fetchall()
        return tuple(MemoryItem(**json.loads(row[1])) for row in rows)

    def items_from_raw(self, raw_message_id: str) -> tuple[MemoryItem, ...]:
        """Find every live item sourced from one raw message, across revisions."""
        identity(raw_message_id)
        with self._db.transaction() as db:
            rows = db.execute(
                "SELECT DISTINCT m.item_json FROM memory_items m JOIN memory_sources s "
                "ON m.memory_id=s.memory_id WHERE s.raw_message_id=? ORDER BY m.memory_id",
                (raw_message_id,)).fetchall()
        return tuple(MemoryItem(**json.loads(row[0])) for row in rows)

    def events_of(self, memory_id: str) -> tuple[str, ...]:
        identity(memory_id)
        with self._db.transaction() as db:
            return tuple(row[0] for row in db.execute(
                "SELECT event FROM memory_events WHERE memory_id=? ORDER BY event", (memory_id,)))

    def audit_of(self, memory_id: str) -> dict[str, list[dict]]:
        """First-commit timestamps, not original message or model extraction time."""
        identity(memory_id)
        with self._db.transaction() as db:
            sources = db.execute(
                "SELECT source_json,created_at FROM memory_sources "
                "WHERE memory_id=? ORDER BY source_json", (memory_id,)).fetchall()
            events = db.execute(
                "SELECT event,created_at FROM memory_events WHERE memory_id=? ORDER BY event",
                (memory_id,)).fetchall()
        return {
            "sources": [dict(json.loads(row[0]), created_at=row[1]) for row in sources],
            "events": [{"event": row[0], "created_at": row[1]} for row in events],
        }

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_items (
    memory_id      TEXT PRIMARY KEY,
    kind           TEXT NOT NULL,
    store          TEXT NOT NULL DEFAULT '',
    store_ref      TEXT NOT NULL DEFAULT '',
    content_preview TEXT NOT NULL DEFAULT '',
    actor          TEXT NOT NULL DEFAULT 'unknown',
    speaker        TEXT NOT NULL DEFAULT '',
    chat_name      TEXT NOT NULL DEFAULT '',
    visibility     TEXT NOT NULL DEFAULT '',
    is_master_source INTEGER NOT NULL DEFAULT 0,
    created_at     REAL,
    entity_revision REAL,
    valid_from     REAL,
    valid_to       REAL,
    status         TEXT NOT NULL DEFAULT 'active',
    confidence     REAL,
    importance     REAL,
    hit_count      INTEGER NOT NULL DEFAULT 0,
    promoted       INTEGER NOT NULL DEFAULT 0,
    source_count   INTEGER NOT NULL DEFAULT 0,
    has_history    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_items_kind ON memory_items(kind);
CREATE INDEX IF NOT EXISTS idx_items_chat ON memory_items(chat_name);
CREATE INDEX IF NOT EXISTS idx_items_created ON memory_items(created_at);

CREATE TABLE IF NOT EXISTS memory_derivations (
    parent_id  TEXT NOT NULL,
    child_id   TEXT NOT NULL,
    relation   TEXT NOT NULL,
    run_id     TEXT NOT NULL DEFAULT '',
    created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_deriv_child ON memory_derivations(child_id);
CREATE INDEX IF NOT EXISTS idx_deriv_parent ON memory_derivations(parent_id);

CREATE TABLE IF NOT EXISTS memory_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id  TEXT NOT NULL,
    event      TEXT NOT NULL,
    actor      TEXT NOT NULL DEFAULT 'unknown',
    reason     TEXT NOT NULL DEFAULT '',
    created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_events_mid ON memory_events(memory_id);

CREATE TABLE IF NOT EXISTS recall_runs (
    run_id           TEXT PRIMARY KEY,
    agent_id         TEXT NOT NULL DEFAULT '',
    query            TEXT NOT NULL DEFAULT '',
    created_at       REAL,
    total_candidates INTEGER NOT NULL DEFAULT 0,
    acl_filtered     INTEGER NOT NULL DEFAULT 0,
    injected         INTEGER NOT NULL DEFAULT 0,
    duration_ms      INTEGER NOT NULL DEFAULT 0,
    reason_codes     TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS recall_candidates (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    memory_id  TEXT NOT NULL,
    source     TEXT NOT NULL DEFAULT '',
    score      REAL NOT NULL DEFAULT 0,
    rank       INTEGER NOT NULL DEFAULT 0,
    reason_code TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_cand_run ON recall_candidates(run_id);
"""


def new_memory_id() -> str:
    """稳定记忆 ID: mem_ + UUID4 (规格: 不再用纯毫秒时间戳)。"""
    return f"mem_{uuid.uuid4().hex}"


def _redact(text: str, limit: int = 160) -> str:
    """脱敏截断展示文本: 只保留结构, 供 UI 预览。"""
    if not text:
        return ""
    return text.strip()[:limit]


class MemoryLedger:
    """来源账本。构造不触盘, 首次写/读时惰性建库 (幂等迁移)。

    fail-safe 契约: 任何写操作失败只记告警 + 入 backlog, 绝不抛给上层
    (规格: ledger 写失败不得阻断现有主消息链)。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.outbox_path = db_path + ".outbox.jsonl"
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._backlog: list[dict] = []
        self._backlog_loaded = False
        self._replaying = False

    # ---------- 连接与迁移 ----------

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            with conn:
                conn.executescript(_SCHEMA_SQL)
                row = conn.execute("SELECT version FROM schema_version").fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO schema_version(version) VALUES (?)",
                        (_SCHEMA_VERSION,),
                    )
                elif int(row["version"] or 0) < 2:
                    columns = {
                        str(info["name"])
                        for info in conn.execute(
                            "PRAGMA table_info(memory_items)",
                        ).fetchall()
                    }
                    if "entity_revision" not in columns:
                        conn.execute(
                            "ALTER TABLE memory_items "
                            "ADD COLUMN entity_revision REAL",
                        )
                    conn.execute("UPDATE schema_version SET version=2")
            self._conn = conn
        return self._conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception as e:  # noqa: BLE001
                    logger.debug(f"[MemoryLedger] close 失败(非致命): {e}")
                self._conn = None

    # ---------- 写路径 (fail-safe) ----------

    def _try_write(self, fn, what: str, payload: Optional[dict] = None) -> bool:
        """统一写兜底: 失败记告警 + 持久化 outbox, 返回 False 不抛。"""
        try:
            with self._lock:
                conn = self._connect()
                with conn:
                    fn(conn)
            return True
        except Exception as e:
            logger.error(f"[MemoryLedger] {what} 写入失败, 已入 backlog: {e}")
            if not self._replaying:
                self._push_backlog({
                    "what": what,
                    "payload": payload or {},
                    "error": str(e),
                    "ts": time.time(),
                })
            return False

    def _push_backlog(self, entry: dict) -> None:
        with self._lock:
            loaded = self._ensure_backlog_loaded()
            if loaded:
                self._backlog.append(entry)
            try:
                os.makedirs(os.path.dirname(self.outbox_path), exist_ok=True)
                with open(self.outbox_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as outbox_error:
                logger.error(
                    f"[MemoryLedger] outbox 落盘失败，操作仅保留在内存: {outbox_error}"
                )

    def _ensure_backlog_loaded(self) -> bool:
        if self._backlog_loaded:
            return True
        if not os.path.exists(self.outbox_path):
            self._backlog_loaded = True
            return True
        loaded: list[dict] = []
        try:
            with open(self.outbox_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except (json.JSONDecodeError, TypeError) as e:
                        # 保留损坏行并继续读取后续条目，避免一次局部损坏让
                        # 后半段合法操作在确认写回时被静默抹掉。
                        loaded.append({
                            "what": "__corrupt__",
                            "payload": {"line": line_no, "raw": line},
                            "error": str(e),
                            "ts": time.time(),
                        })
                        logger.error(
                            f"[MemoryLedger] outbox 第 {line_no} 行损坏，已隔离保留"
                        )
                        continue
                    if isinstance(entry, dict):
                        loaded.append(entry)
        except OSError as e:
            logger.error(f"[MemoryLedger] outbox 读取失败，保留原文件待人工处理: {e}")
            return False
        self._backlog.extend(loaded)
        self._backlog_loaded = True
        return True

    def _rewrite_outbox(self) -> None:
        if not self._backlog:
            if os.path.exists(self.outbox_path):
                os.remove(self.outbox_path)
            return
        os.makedirs(os.path.dirname(self.outbox_path), exist_ok=True)
        tmp_path = self.outbox_path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                for entry in self._backlog:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.outbox_path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def backlog_size(self) -> int:
        with self._lock:
            self._ensure_backlog_loaded()
            return len(self._backlog)

    def drain_backlog(self) -> list[dict]:
        """返回 backlog 快照；不删除，成功重放后才确认移除。"""
        with self._lock:
            self._ensure_backlog_loaded()
            return list(self._backlog)

    def replay_backlog(self) -> dict:
        """幂等重放持久化 outbox，返回成功及剩余数量。"""
        with self._lock:
            if not self._ensure_backlog_loaded():
                return {"succeeded": 0, "remaining": len(self._backlog),
                        "read_error": True}
            pending = list(self._backlog)
            remaining = []
            succeeded = 0
            self._replaying = True
            try:
                for entry in pending:
                    if self._replay_entry(entry):
                        succeeded += 1
                    else:
                        remaining.append(entry)
            finally:
                self._replaying = False
            self._backlog = remaining
            try:
                self._rewrite_outbox()
            except Exception as e:
                logger.error(f"[MemoryLedger] outbox 确认写回失败: {e}")
            return {"succeeded": succeeded, "remaining": len(remaining)}

    def _replay_entry(self, entry: dict) -> bool:
        what = str(entry.get("what") or "")
        payload = entry.get("payload") or {}
        try:
            if what == "memory_item":
                return self.record_item(MemoryItem(**payload))
            if what == "memory_item_unique":
                return self.record_item_if_store_ref_absent(
                    MemoryItem(**payload), queue_on_error=False,
                ) in ("inserted", "conflict")
            if what == "entity_item_upsert":
                status, _ = self.upsert_entity_item(
                    MemoryItem(**payload), queue_on_error=False,
                )
                return status != "error"
            if what == "status":
                return self.mark_status(payload["memory_id"], payload["status"])
            if what == "store_ref":
                return self.update_store_ref(
                    payload["memory_id"], payload["store"], payload["store_ref"],
                )
            if what == "derivation":
                return self.record_derivation(MemoryDerivation(**payload))
            if what == "event":
                return self.record_event(**payload)
            if what == "recall_run":
                return self.record_recall_run(RecallRun(**payload))
            if what == "recall_candidate":
                return self.record_recall_candidate(RecallCandidate(**payload))
        except Exception as e:
            logger.error(f"[MemoryLedger] outbox 条目无法重放({what}): {e}")
        return False

    # ---------- memory_items ----------

    def record_item(self, item: MemoryItem) -> bool:
        """新写入生成稳定 ID 并记录来源事件 (created)。

        entity 的 ``store_ref`` 是实体当前事实的唯一身份，必须与治理回填
        共用条件插入；其历史由 EntityStore history 与 Ledger events 表达。
        其他 store 保留多版本记录语义。
        """
        if item.store == "entity":
            status, _ = self.upsert_entity_item(
                item, queue_on_error=True,
            )
            return status != "error"

        def _do(conn: sqlite3.Connection) -> None:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO memory_items
                   (memory_id, kind, store, store_ref, content_preview, actor,
                    speaker, chat_name, visibility, is_master_source, created_at,
                    entity_revision, valid_from, valid_to, status, confidence, importance,
                    hit_count, promoted, source_count, has_history)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (item.memory_id, item.kind, item.store, item.store_ref,
                 _redact(item.content_preview), item.actor, item.speaker,
                 item.chat_name, item.visibility,
                 1 if item.is_master_source else 0, item.created_at,
                 item.entity_revision, item.valid_from, item.valid_to, item.status,
                 item.confidence, item.importance, item.hit_count,
                 1 if item.promoted else 0, item.source_count,
                 1 if item.has_history else 0),
            )
            if cursor.rowcount:
                conn.execute(
                    """INSERT INTO memory_events
                       (memory_id, event, actor, reason, created_at)
                       VALUES (?,?,?,?,?)""",
                    (item.memory_id, "created", item.actor, "",
                     item.created_at or time.time()),
                )
        return self._try_write(_do, "memory_item", asdict(item))

    def record_item_if_store_ref_absent(
            self, item: MemoryItem, queue_on_error: bool = False) -> str:
        """在单个 SQLite 写事务内完成 store_ref 条件插入。

        返回 ``inserted`` / ``conflict`` / ``error``。治理首次回填不能使用
        fail-soft outbox，因为“查询后、插入前”出现同 ref 在线写入时，延迟重放
        会制造第二条 active 行；这里用 BEGIN IMMEDIATE 跨 Ledger 实例串行化。
        """
        try:
            with self._lock:
                conn = self._connect()
                conn.execute("BEGIN IMMEDIATE")
                try:
                    same_id = conn.execute(
                        "SELECT store, store_ref FROM memory_items "
                        "WHERE memory_id=? LIMIT 1", (item.memory_id,),
                    ).fetchone()
                    if same_id is not None:
                        conn.rollback()
                        return "conflict"
                    existing = conn.execute(
                        "SELECT memory_id FROM memory_items "
                        "WHERE store=? AND store_ref=? LIMIT 1",
                        (item.store, item.store_ref),
                    ).fetchone()
                    if existing is not None:
                        conn.rollback()
                        return "conflict"
                    conn.execute(
                        """INSERT INTO memory_items
                           (memory_id, kind, store, store_ref, content_preview,
                            actor, speaker, chat_name, visibility,
                            is_master_source, created_at, entity_revision,
                            valid_from, valid_to,
                            status, confidence, importance, hit_count, promoted,
                            source_count, has_history)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (item.memory_id, item.kind, item.store, item.store_ref,
                         _redact(item.content_preview), item.actor, item.speaker,
                         item.chat_name, item.visibility,
                         1 if item.is_master_source else 0, item.created_at,
                         item.entity_revision, item.valid_from, item.valid_to,
                         item.status,
                         item.confidence, item.importance, item.hit_count,
                         1 if item.promoted else 0, item.source_count,
                         1 if item.has_history else 0),
                    )
                    conn.execute(
                        """INSERT INTO memory_events
                           (memory_id, event, actor, reason, created_at)
                           VALUES (?,?,?,?,?)""",
                        (item.memory_id, "created", item.actor, "",
                         item.created_at or time.time()),
                    )
                    conn.commit()
                    return "inserted"
                except Exception:
                    conn.rollback()
                    raise
        except Exception as e:
            logger.error(f"[MemoryLedger] 条件插入失败: {e}")
            if queue_on_error and not self._replaying:
                self._push_backlog({
                    "what": "memory_item_unique",
                    "payload": asdict(item),
                    "error": str(e),
                    "ts": time.time(),
                })
            return "error"

    def upsert_entity_item(
            self, item: MemoryItem,
            queue_on_error: bool = False) -> tuple[str, str]:
        """原子写入实体当前事实，并保留版本/恢复事件。

        ``inserted`` 新建 canonical 行；``updated`` 原子刷新已有事实；
        ``reactivated`` 表示在线新事实重新激活已归档身份；``unchanged``
        为完全幂等重放。返回值第二项始终是 canonical ``memory_id``。
        repair 使用条件插入而不是本方法，因此在线写与 repair 竞争时既不会
        双户口，也不会把合法的新事实版本静默丢弃。
        """
        if item.store != "entity":
            return "error", ""
        try:
            with self._lock:
                conn = self._connect()
                conn.execute("BEGIN IMMEDIATE")
                try:
                    row = conn.execute(
                        "SELECT * FROM memory_items "
                        "WHERE store='entity' AND store_ref=? "
                        "ORDER BY created_at DESC LIMIT 1",
                        (item.store_ref,),
                    ).fetchone()
                    if row is None:
                        same_id = conn.execute(
                            "SELECT memory_id FROM memory_items "
                            "WHERE memory_id=? LIMIT 1", (item.memory_id,),
                        ).fetchone()
                        if same_id is not None:
                            conn.rollback()
                            return "error", ""
                        conn.execute(
                            """INSERT INTO memory_items
                               (memory_id, kind, store, store_ref,
                                content_preview, actor, speaker, chat_name,
                                visibility, is_master_source, created_at,
                                entity_revision,
                                valid_from, valid_to, status, confidence,
                                importance, hit_count, promoted, source_count,
                                has_history)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (item.memory_id, item.kind, item.store,
                             item.store_ref, _redact(item.content_preview),
                             item.actor, item.speaker, item.chat_name,
                             item.visibility,
                             1 if item.is_master_source else 0,
                             item.created_at, item.entity_revision,
                             item.valid_from, item.valid_to,
                             item.status, item.confidence, item.importance,
                             item.hit_count, 1 if item.promoted else 0,
                             item.source_count, 1 if item.has_history else 0),
                        )
                        conn.execute(
                            """INSERT INTO memory_events
                               (memory_id, event, actor, reason, created_at)
                               VALUES (?,?,?,?,?)""",
                            (item.memory_id, "created", item.actor, "",
                             item.created_at or time.time()),
                        )
                        conn.commit()
                        return "inserted", item.memory_id

                    current = dict(row)
                    memory_id = str(current["memory_id"])
                    desired = {
                        "kind": item.kind,
                        "content_preview": _redact(item.content_preview),
                        "actor": item.actor,
                        "speaker": item.speaker,
                        "chat_name": item.chat_name,
                        "visibility": item.visibility,
                        "is_master_source": 1 if item.is_master_source else 0,
                        "created_at": item.created_at,
                        "entity_revision": item.entity_revision,
                        "valid_from": (item.valid_from if item.valid_from is not None
                                       else current.get("valid_from")),
                        "valid_to": (item.valid_to if item.valid_to is not None
                                     else current.get("valid_to")),
                        "status": item.status,
                        "confidence": (
                            item.confidence if item.confidence is not None
                            else current.get("confidence")
                        ),
                        "importance": (
                            item.importance if item.importance is not None
                            else current.get("importance")
                        ),
                        "hit_count": max(
                            int(current.get("hit_count") or 0),
                            int(item.hit_count or 0),
                        ),
                        "promoted": max(
                            int(current.get("promoted") or 0),
                            1 if item.promoted else 0,
                        ),
                        "source_count": max(
                            int(current.get("source_count") or 0),
                            int(item.source_count or 0),
                        ),
                        "has_history": max(
                            int(current.get("has_history") or 0),
                            1 if item.has_history else 0,
                        ),
                    }
                    if all(current.get(key) == value
                           for key, value in desired.items()):
                        conn.rollback()
                        return "unchanged", memory_id
                    incoming_revision = item.entity_revision
                    current_revision = current.get("entity_revision")
                    incoming_created = float(item.created_at or 0.0)
                    current_created = float(current.get("created_at") or 0.0)
                    if current_revision is not None and incoming_revision is None:
                        # 已进入带版本协议的 canonical 行绝不被无版本旧载荷降级。
                        stale = True
                    elif (incoming_revision is not None
                          and current_revision is not None):
                        incoming_revision = float(incoming_revision)
                        current_revision = float(current_revision)
                        stale = (incoming_revision < current_revision
                                 or (incoming_revision == current_revision
                                     and incoming_created <= current_created))
                    else:
                        stale = incoming_created <= current_created
                    if stale:
                        conn.rollback()
                        return "stale", memory_id
                    prior_status = str(current.get("status") or "")
                    assignments = ", ".join(f"{key}=?" for key in desired)
                    conn.execute(
                        f"UPDATE memory_items SET {assignments} "
                        "WHERE memory_id=?",
                        [*desired.values(), memory_id],
                    )
                    event = ("reactivated" if prior_status in (
                        "pruned", "forgotten",
                    ) and item.status == "active" else "updated")
                    conn.execute(
                        """INSERT INTO memory_events
                           (memory_id, event, actor, reason, created_at)
                           VALUES (?,?,?,?,?)""",
                        (memory_id, event, item.actor,
                         "entity canonical provenance refreshed", time.time()),
                    )
                    conn.commit()
                    return event, memory_id
                except Exception:
                    conn.rollback()
                    raise
        except Exception as e:
            logger.error(f"[MemoryLedger] 实体原子 upsert 失败: {e}")
            if queue_on_error and not self._replaying:
                self._push_backlog({
                    "what": "entity_item_upsert",
                    "payload": asdict(item),
                    "error": str(e),
                    "ts": time.time(),
                })
            return "error", ""

    def get_item(self, memory_id: str) -> Optional[dict]:
        try:
            with self._lock:
                conn = self._connect()
                row = conn.execute(
                    "SELECT * FROM memory_items WHERE memory_id=?", (memory_id,),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"[MemoryLedger] get_item 失败(非致命): {e}")
            return None

    def find_by_store_ref(self, store: str, store_ref: str) -> Optional[dict]:
        """[0.3.30.4 G16b] 按底层存储标识反查账本行 (forget/promote 等生命
        周期事件定位用)。同 ref 多行取最新。"""
        try:
            with self._lock:
                conn = self._connect()
                row = conn.execute(
                    "SELECT * FROM memory_items WHERE store=? AND store_ref=? "
                    "ORDER BY created_at DESC LIMIT 1", (store, store_ref),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"[MemoryLedger] find_by_store_ref 失败(非致命): {e}")
            return None

    def find_by_store_ref_strict(self, store: str,
                                 store_ref: str) -> Optional[dict]:
        """治理写路径使用的严格查询。

        与普通 fail-soft 查询不同，I/O 失败会向上传播，使调用方能区分
        “确实不存在”和“当前无法证明不存在”，从而安全拒绝 mutation。
        """
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT * FROM memory_items WHERE store=? AND store_ref=? "
                "ORDER BY created_at DESC LIMIT 1", (store, store_ref),
            ).fetchone()
            return dict(row) if row else None

    def store_ref_set(self, store: str) -> set[str]:
        """返回某底层存储已入账引用，用于只读 reconciliation。"""
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    "SELECT store_ref FROM memory_items WHERE store=?",
                    (store,),
                ).fetchall()
                return {str(row["store_ref"]) for row in rows}
        except Exception as e:
            logger.warning(f"[MemoryLedger] store_ref_set 失败(非致命): {e}")
            return set()

    def update_store_ref(self, memory_id: str, store: str, store_ref: str) -> bool:
        """将旧账本行绑定到规范底层引用，供幂等迁移修正使用。"""
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE memory_items SET store=?, store_ref=? WHERE memory_id=?",
                (store, store_ref, memory_id),
            )
        return self._try_write(_do, "store_ref", {
            "memory_id": memory_id, "store": store, "store_ref": store_ref,
        })

    def mark_status(self, memory_id: str, status: str) -> bool:
        """[0.3.30.4 G16b] 更新生命周期状态 (forgotten/pruned 等), 事件另记。"""
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE memory_items SET status=? WHERE memory_id=?",
                (status, memory_id),
            )
        return self._try_write(
            _do, "status", {"memory_id": memory_id, "status": status},
        )

    def list_items(self, kind: str = "", status: str = "", chat_name: str = "",
                   speaker: str = "", visibility: str = "", query: str = "",
                   start_ts: float = None, end_ts: float = None,
                   exclude_visibilities: tuple[str, ...] = (),
                   limit: int = 50, offset: int = 0) -> list[dict]:
        """分页筛选 (规格: 不使用无界全量数组)。

        [0.3.32.0 三梯队 G03] 过滤面: speaker 模糊 / visibility 精确 /
        query 内容模糊 (content_preview) / created_at 时间范围。
        """
        try:
            with self._lock:
                conn = self._connect()
                where, params = [], []
                if kind:
                    where.append("kind=?"); params.append(kind)
                if status:
                    where.append("status=?"); params.append(status)
                if chat_name:
                    where.append("chat_name=?"); params.append(chat_name)
                if speaker:
                    where.append("speaker LIKE ?")
                    params.append(f"%{speaker}%")
                if visibility:
                    where.append("visibility=?"); params.append(visibility)
                if exclude_visibilities:
                    marks = ",".join("?" for _ in exclude_visibilities)
                    where.append(f"visibility NOT IN ({marks})")
                    params.extend(exclude_visibilities)
                if query:
                    where.append("content_preview LIKE ?")
                    params.append(f"%{query}%")
                if start_ts is not None:
                    where.append("created_at >= ?"); params.append(float(start_ts))
                if end_ts is not None:
                    where.append("created_at < ?"); params.append(float(end_ts))
                sql = "SELECT * FROM memory_items"
                if where:
                    sql += " WHERE " + " AND ".join(where)
                sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
                params += [max(1, min(limit, 200)), max(0, offset)]
                rows = conn.execute(sql, params).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[MemoryLedger] list_items 失败(非致命): {e}")
            return []

    def count_items(self) -> int:
        try:
            with self._lock:
                conn = self._connect()
                return conn.execute("SELECT COUNT(*) FROM memory_items").fetchone()[0]
        except Exception as e:
            logger.warning(f"[MemoryLedger] count_items 失败(非致命): {e}")
            return 0

    # ---------- 派生关系 ----------

    def record_derivation(self, d: MemoryDerivation) -> bool:
        """记录来源 -> 派生链 (summarized_from/extracted_from/inferred_from/supersedes)。"""
        created_at = d.created_at or time.time()
        payload = asdict(d)
        payload["created_at"] = created_at

        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO memory_derivations
                   (parent_id, child_id, relation, run_id, created_at)
                   SELECT ?,?,?,?,?
                   WHERE NOT EXISTS (
                     SELECT 1 FROM memory_derivations
                     WHERE parent_id=? AND child_id=? AND relation=?
                       AND run_id=? AND created_at=?
                   )""",
                (d.parent_id, d.child_id, d.relation, d.run_id, created_at,
                 d.parent_id, d.child_id, d.relation, d.run_id, created_at),
            )
        return self._try_write(_do, "derivation", payload)

    def derivations_of(self, memory_id: str) -> list[dict]:
        """该记忆的派生链: 谁派生它 (parent) + 它派生谁 (child)。"""
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    """SELECT * FROM memory_derivations
                       WHERE child_id=? OR parent_id=?
                       ORDER BY created_at DESC LIMIT 200""",
                    (memory_id, memory_id),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[MemoryLedger] derivations_of 失败(非致命): {e}")
            return []

    # ---------- 生命周期事件 ----------

    def record_event(self, memory_id: str, event: str, actor: str = "unknown",
                     reason: str = "", created_at: float = None) -> bool:
        event_ts = created_at or time.time()

        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO memory_events
                   (memory_id, event, actor, reason, created_at)
                   SELECT ?,?,?,?,?
                   WHERE NOT EXISTS (
                     SELECT 1 FROM memory_events
                     WHERE memory_id=? AND event=? AND actor=? AND reason=?
                       AND created_at=?
                   )""",
                (memory_id, event, actor, reason, event_ts,
                 memory_id, event, actor, reason, event_ts),
            )
        return self._try_write(_do, "event", {
            "memory_id": memory_id,
            "event": event,
            "actor": actor,
            "reason": reason,
            "created_at": event_ts,
        })

    def events_of(self, memory_id: str, limit: int = 100) -> list[dict]:
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    """SELECT * FROM memory_events WHERE memory_id=?
                       ORDER BY id DESC LIMIT ?""",
                    (memory_id, limit),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[MemoryLedger] events_of 失败(非致命): {e}")
            return []

    # ---------- 召回审计 (脱敏摘要) ----------

    def record_recall_run(self, run: RecallRun) -> bool:
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT OR REPLACE INTO recall_runs
                   (run_id, agent_id, query, created_at, total_candidates,
                    acl_filtered, injected, duration_ms, reason_codes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (run.run_id, run.agent_id, run.query[:200], run.created_at,
                 run.total_candidates, run.acl_filtered, run.injected,
                 run.duration_ms, json.dumps(run.reason_codes, ensure_ascii=False)),
            )
        return self._try_write(_do, "recall_run", asdict(run))

    def list_recall_runs(self, limit: int = 20) -> list[dict]:
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    "SELECT * FROM recall_runs ORDER BY created_at DESC LIMIT ?",
                    (max(1, min(limit, 100)),),
                ).fetchall()
                out = []
                for r in rows:
                    d = dict(r)
                    try:
                        d["reason_codes"] = json.loads(d.get("reason_codes") or "{}")
                    except (json.JSONDecodeError, TypeError):
                        d["reason_codes"] = {}
                    out.append(d)
                return out
        except Exception as e:
            logger.warning(f"[MemoryLedger] list_recall_runs 失败(非致命): {e}")
            return []

    def record_recall_candidate(self, c: RecallCandidate) -> bool:
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO recall_candidates
                   (run_id, memory_id, source, score, rank, reason_code)
                   SELECT ?,?,?,?,?,?
                   WHERE NOT EXISTS (
                     SELECT 1 FROM recall_candidates
                     WHERE run_id=? AND memory_id=? AND source=?
                       AND score=? AND rank=? AND reason_code=?
                   )""",
                (c.run_id, c.memory_id, c.source, c.score, c.rank,
                 c.reason_code, c.run_id, c.memory_id, c.source, c.score,
                 c.rank, c.reason_code),
            )
        return self._try_write(_do, "recall_candidate", asdict(c))

    def candidates_of(self, run_id: str) -> list[dict]:
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    "SELECT * FROM recall_candidates WHERE run_id=? ORDER BY rank",
                    (run_id,),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[MemoryLedger] candidates_of 失败(非致命): {e}")
            return []

    # ---------- 统计 ----------

    def overview_counts(self) -> dict:
        """分类型/分状态数量 (UI 总览, 单查询聚合)。"""
        try:
            with self._lock:
                conn = self._connect()
                by_kind = {r["kind"]: r["n"] for r in conn.execute(
                    "SELECT kind, COUNT(*) n FROM memory_items GROUP BY kind")}
                by_status = {r["status"]: r["n"] for r in conn.execute(
                    "SELECT status, COUNT(*) n FROM memory_items GROUP BY status")}
                by_store = {r["store"]: r["n"] for r in conn.execute(
                    "SELECT store, COUNT(*) n FROM memory_items GROUP BY store")}
                provenance = conn.execute(
                    "SELECT COUNT(*) n FROM memory_items WHERE memory_id LIKE 'mem_%'"
                ).fetchone()["n"]
                return {
                    "by_kind": by_kind,
                    "by_status": by_status,
                    "by_store": by_store,
                    "total": sum(by_kind.values()),
                    "provenance_covered": provenance,
                }
        except Exception as e:
            logger.warning(f"[MemoryLedger] overview_counts 失败(非致命): {e}")
            return {
                "by_kind": {}, "by_status": {}, "by_store": {},
                "total": 0, "provenance_covered": 0,
            }
