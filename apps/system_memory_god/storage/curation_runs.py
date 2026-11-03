# -*- coding: utf-8 -*-
"""记忆升级工程 Phase 3 -- Curation Runs 持久任务与事件流 (SQLite)。

规格 4.4:
- 持久 run (queued -> running -> awaiting_approval -> applying -> completed
  | cancelled | failed | rollback_pending), 进程重启后任务和审计仍存在;
- run 事件需要可重连游标 (after_cursor);
- 每次 apply 创建 undo checkpoint (只回滚本次任务触碰的对象);
- 写路径 fail-safe (同 memory_ledger): 失败记告警 + backlog, 不抛。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS curation_runs (
    run_id       TEXT PRIMARY KEY,
    mode         TEXT NOT NULL,          -- audit/dedup/conflict/merge/prune/repair_provenance
    agent_id     TEXT NOT NULL DEFAULT '',
    scope        TEXT NOT NULL DEFAULT '',  -- 目标范围 (kind/chat 等 JSON)
    status       TEXT NOT NULL DEFAULT 'queued',
    created_at   REAL,
    updated_at   REAL,
    summary      TEXT NOT NULL DEFAULT '',
    meta         TEXT NOT NULL DEFAULT '{}'  -- 附加信息 (proposal 数等)
);
CREATE INDEX IF NOT EXISTS idx_curation_status ON curation_runs(status);

CREATE TABLE IF NOT EXISTS curation_proposals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    action       TEXT NOT NULL,          -- merge/dedup/supersede/archive/prune
    target_id    TEXT NOT NULL,          -- 目标 memory_id / store_ref
    target_kind  TEXT NOT NULL DEFAULT '',
    content      TEXT NOT NULL DEFAULT '',
    reason       TEXT NOT NULL DEFAULT '',
    evidence     TEXT NOT NULL DEFAULT '',   -- 证据摘要 (脱敏)
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending/applied/rejected
    version      INTEGER NOT NULL DEFAULT 0,      -- 目标版本 (apply 前校验)
    undo_snapshot TEXT NOT NULL DEFAULT ''       -- apply 前快照 (JSON)
);
CREATE INDEX IF NOT EXISTS idx_prop_run ON curation_proposals(run_id);

CREATE TABLE IF NOT EXISTS curation_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    event      TEXT NOT NULL,            -- created/started/proposed/applied/undo/cancelled/failed
    actor      TEXT NOT NULL DEFAULT 'unknown',
    detail     TEXT NOT NULL DEFAULT '',
    created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_cur_events ON curation_events(run_id);
"""


def new_run_id() -> str:
    return f"cur_{uuid.uuid4().hex}"


class CurationRuns:
    """治理任务持久存储。构造不触盘, 首次读写惰性建库 (幂等迁移)。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            with conn:
                conn.executescript(_SCHEMA_SQL)
            self._conn = conn
        return self._conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception as e:  # noqa: BLE001
                    logger.debug(f"[CurationRuns] close 失败(非致命): {e}")
                self._conn = None

    # ---------- run 生命周期 ----------

    def create_run(self, mode: str, agent_id: str = "", scope: str = "") -> str:
        """新建 run (queued)。返回 run_id。"""
        run_id = new_run_id()
        now = time.time()

        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO curation_runs
                   (run_id, mode, agent_id, scope, status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (run_id, mode, agent_id, scope, "queued", now, now),
            )
            conn.execute(
                """INSERT INTO curation_events(run_id, event, actor, detail, created_at)
                   VALUES (?,?,?,?,?)""",
                (run_id, "created", agent_id or "unknown", mode, now),
            )
        self._try_write(_do)
        return run_id

    def set_status(self, run_id: str, status: str, actor: str = "system",
                   detail: str = "") -> bool:
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE curation_runs SET status=?, updated_at=? WHERE run_id=?",
                (status, time.time(), run_id),
            )
            conn.execute(
                """INSERT INTO curation_events(run_id, event, actor, detail, created_at)
                   VALUES (?,?,?,?,?)""",
                (run_id, status, actor, detail, time.time()),
            )
        return self._try_write(_do)

    def get_run(self, run_id: str) -> Optional[dict]:
        try:
            with self._lock:
                conn = self._connect()
                row = conn.execute(
                    "SELECT * FROM curation_runs WHERE run_id=?", (run_id,),
                ).fetchone()
                if row is None:
                    return None
                d = dict(row)
                try:
                    d["meta"] = json.loads(d.get("meta") or "{}")
                except (json.JSONDecodeError, TypeError):
                    d["meta"] = {}
                return d
        except Exception as e:
            logger.warning(f"[CurationRuns] get_run 失败(非致命): {e}")
            return None

    def list_runs(self, status: str = "", limit: int = 20,
                  agent_id: str = "") -> list[dict]:
        """[0.3.34.0 B0b] agent_id 过滤: 观察台切换浏览对象时只看该
        智能体的运行历史 (runs 表建表即有 agent_id 列)。"""
        try:
            with self._lock:
                conn = self._connect()
                sql = "SELECT * FROM curation_runs"
                params: list = []
                where: list = []
                if status:
                    where.append("status=?")
                    params.append(status)
                if agent_id:
                    where.append("agent_id=?")
                    params.append(agent_id)
                if where:
                    sql += " WHERE " + " AND ".join(where)
                sql += " ORDER BY created_at DESC LIMIT ?"
                params.append(max(1, min(limit, 100)))
                result = []
                for row in conn.execute(sql, params).fetchall():
                    item = dict(row)
                    try:
                        item["meta"] = json.loads(item.get("meta") or "{}")
                    except (json.JSONDecodeError, TypeError):
                        item["meta"] = {}
                    result.append(item)
                return result
        except Exception as e:
            logger.warning(f"[CurationRuns] list_runs 失败(非致命): {e}")
            return []

    # ---------- proposals ----------

    def add_proposal(self, run_id: str, action: str, target_id: str,
                     target_kind: str = "", content: str = "", reason: str = "",
                     evidence: str = "", version: int = 0) -> int:
        """追加 proposal (pending)。返回 proposal id。"""
        proposal_id = [0]

        def _do(conn: sqlite3.Connection) -> None:
            cur = conn.execute(
                """INSERT INTO curation_proposals
                   (run_id, action, target_id, target_kind, content, reason,
                    evidence, status, version, undo_snapshot)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (run_id, action, target_id, target_kind, content, reason,
                 evidence, "pending", version, ""),
            )
            proposal_id[0] = cur.lastrowid
        self._try_write(_do)
        return proposal_id[0]

    def proposals_of(self, run_id: str, limit: int = 100, offset: int = 0,
                     status: str = "") -> list[dict]:
        """分页读取 proposal，避免大 run 一次性加载和渲染。"""
        try:
            with self._lock:
                conn = self._connect()
                sql = "SELECT * FROM curation_proposals WHERE run_id=?"
                params: list = [run_id]
                if status:
                    sql += " AND status=?"
                    params.append(status)
                sql += " ORDER BY id LIMIT ? OFFSET ?"
                params.extend([
                    max(1, min(int(limit), 200)), max(0, int(offset)),
                ])
                rows = conn.execute(sql, params).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[CurationRuns] proposals_of 失败(非致命): {e}")
            return []

    def proposal_counts(self, run_id: str) -> dict[str, int]:
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    """SELECT status, COUNT(*) AS n FROM curation_proposals
                       WHERE run_id=? GROUP BY status""",
                    (run_id,),
                ).fetchall()
                counts = {str(row["status"]): int(row["n"]) for row in rows}
                counts["total"] = sum(counts.values())
                return counts
        except Exception as e:
            logger.warning(f"[CurationRuns] proposal_counts 失败(非致命): {e}")
            return {"total": 0}

    def update_proposal(self, proposal_id: int, status: str,
                        undo_snapshot: str = "") -> bool:
        def _do(conn: sqlite3.Connection) -> None:
            if undo_snapshot:
                conn.execute(
                    """UPDATE curation_proposals SET status=?, undo_snapshot=?
                       WHERE id=?""",
                    (status, undo_snapshot, proposal_id),
                )
            else:
                conn.execute(
                    "UPDATE curation_proposals SET status=? WHERE id=?",
                    (status, proposal_id),
                )
        return self._try_write(_do)

    def compare_and_set_proposal(self, proposal_id: int, expected_status: str,
                                 new_status: str,
                                 undo_snapshot: str | None = None) -> bool:
        """原子状态 claim；仅当前状态匹配时更新并返回 True。"""
        try:
            with self._lock:
                conn = self._connect()
                with conn:
                    if undo_snapshot is None:
                        cursor = conn.execute(
                            "UPDATE curation_proposals SET status=? "
                            "WHERE id=? AND status=?",
                            (new_status, proposal_id, expected_status),
                        )
                    else:
                        cursor = conn.execute(
                            "UPDATE curation_proposals "
                            "SET status=?, undo_snapshot=? "
                            "WHERE id=? AND status=?",
                            (new_status, undo_snapshot, proposal_id,
                             expected_status),
                        )
                return cursor.rowcount == 1
        except Exception as e:
            logger.error(f"[CurationRuns] proposal CAS 失败: {e}")
            return False

    def get_proposal(self, proposal_id: int) -> Optional[dict]:
        try:
            with self._lock:
                conn = self._connect()
                row = conn.execute(
                    "SELECT * FROM curation_proposals WHERE id=?", (proposal_id,),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"[CurationRuns] get_proposal 失败(非致命): {e}")
            return None

    def proposals_in_statuses(self, statuses: tuple[str, ...],
                              limit: int = 500) -> list[dict]:
        """启动恢复扫描：只取显式中间态，避免无界加载全部历史。"""
        if not statuses:
            return []
        try:
            with self._lock:
                conn = self._connect()
                marks = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"SELECT * FROM curation_proposals WHERE status IN ({marks}) "
                    "ORDER BY id LIMIT ?",
                    [*statuses, max(1, min(limit, 2000))],
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"[CurationRuns] 中间态扫描失败(非致命): {e}")
            return []

    def nonterminal_runs(self, limit: int = 500) -> list[dict]:
        """启动恢复使用；终态之外的 run 有界扫描。"""
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    "SELECT * FROM curation_runs WHERE status NOT IN "
                    "('completed','cancelled','failed') "
                    "ORDER BY created_at LIMIT ?",
                    (max(1, min(limit, 2000)),),
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"[CurationRuns] 非终态扫描失败(非致命): {e}")
            return []

    # ---------- 事件流 (可重连游标) ----------

    def events_after(self, run_id: str, after_cursor: int = 0, limit: int = 100) -> list[dict]:
        """按事件 id 游标分页 (规格: 可重连游标)。"""
        try:
            with self._lock:
                conn = self._connect()
                rows = conn.execute(
                    """SELECT * FROM curation_events
                       WHERE run_id=? AND id>? ORDER BY id LIMIT ?""",
                    (run_id, after_cursor, max(1, min(limit, 200))),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[CurationRuns] events_after 失败(非致命): {e}")
            return []

    def _try_write(self, fn) -> bool:
        try:
            with self._lock:
                conn = self._connect()
                with conn:
                    fn(conn)
            return True
        except Exception as e:
            logger.error(f"[CurationRuns] 写入失败: {e}")
            return False
