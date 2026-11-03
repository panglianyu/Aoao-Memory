# -*- coding: utf-8 -*-
"""记忆升级工程 Phase 3 -- Memory Curation 安全治理 (规格 4.4)。

任务模式: audit/dedup/prune/repair_provenance。
规则:
- LLM 只生成 proposal, 不直接写存储 (本模块内置确定性规则生成 proposal,
  LLM 增强留给后续; 确定性保证可测);
- proposal 必须经过确定性校验;
- apply 前再次校验目标版本, 防止过期提议覆盖新事实 (409);
- 每次 apply 创建 undo checkpoint (只回滚本次任务触碰的对象);
- 状态机: queued -> running -> awaiting_approval -> applying -> completed
  | cancelled | failed | rollback_pending (规格 4.4);
- 所有 mutation 经 require_owner 校验 (REST 层) + 明确 agent_id;
- 对 inactive snapshot 默认拒绝写。
"""
from __future__ import annotations

import asyncio
import time
import hashlib
import json
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

# 只暴露具备确定性生成器和真实执行语义的模式。
MODES = ("audit", "dedup", "prune", "repair_provenance")
SUPPORTED_ACTIONS = frozenset({"dedup", "prune", "repair_provenance"})


class CurationEngine:
    """治理引擎: proposal 生成 + 校验 + apply/undo。

    构造持存储句柄 (entity_store/rule_store/vector_store/ledger 可缺省)
    与持久任务存储 (CurationRuns 必需)。
    """

    def __init__(self, runs, entity_store=None, rule_store=None,
                 vector_store=None, ledger=None):
        self.runs = runs
        self.entity_store = entity_store
        self.rule_store = rule_store
        self.vector_store = vector_store
        self.ledger = ledger

    async def _runs_io(self, method: str, *args, **kwargs):
        return await asyncio.to_thread(
            getattr(self.runs, method), *args, **kwargs,
        )

    async def _ledger_io(self, method: str, *args, **kwargs):
        return await asyncio.to_thread(
            getattr(self.ledger, method), *args, **kwargs,
        )

    @staticmethod
    def _version_token(value) -> int:
        encoded = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        return int(hashlib.sha256(encoded).hexdigest()[:15], 16)

    @classmethod
    def _ledger_row_token(cls, row: dict) -> int:
        """治理 undo 使用的 canonical 来源版本指纹。"""
        fields = (
            "kind", "store", "store_ref", "content_preview", "actor",
            "speaker", "chat_name", "visibility", "is_master_source",
            "created_at", "entity_revision",
        )
        return cls._version_token({key: row.get(key) for key in fields})

    # ---------- 任务生命周期 ----------

    def create_run(self, mode: str, agent_id: str = "", scope: str = "") -> str:
        if mode not in MODES:
            raise ValueError(f"未知任务模式: {mode}")
        return self.runs.create_run(mode, agent_id=agent_id, scope=scope)

    def get_run(self, run_id: str) -> Optional[dict]:
        return self.runs.get_run(run_id)

    def list_runs(self, status: str = "", limit: int = 20,
                  agent_id: str = "") -> list[dict]:
        return self.runs.list_runs(status=status, limit=limit,
                                   agent_id=agent_id)

    def events_after(self, run_id: str, after_cursor: int = 0) -> list[dict]:
        return self.runs.events_after(run_id, after_cursor=after_cursor)

    async def recover_interrupted(self) -> dict:
        """恢复崩溃遗留的 applying/undoing，无法证明安全时进入待协调态。"""
        recovered = 0
        pending_review = 0
        runs_reconciled = 0
        touched_run_ids: set[str] = set()
        proposals = await self._runs_io(
            "proposals_in_statuses", ("applying", "undoing"),
        )
        for proposal in proposals:
            touched_run_ids.add(str(proposal["run_id"]))
            status = proposal["status"]
            safe = await self._matches_undo_snapshot(proposal)
            if not safe:
                safe = await self._restore_undo(proposal)
            if safe and await self._runs_io(
                    "compare_and_set_proposal",
                    proposal["id"], status, "pending"):
                recovered += 1
                continue
            await self._runs_io(
                "compare_and_set_proposal",
                proposal["id"], status, "rollback_pending",
            )
            await self._runs_io(
                "set_status",
                proposal["run_id"], "rollback_pending", actor="engine",
                detail=f"proposal {proposal['id']} 启动恢复需人工核验",
            )
            pending_review += 1
        # proposal 已提交但 run 尚未收敛的独立崩溃窗口。恢复 undoing 时，
        # 原 run 可能已是 completed，因此本轮触及的 run 必须强制加入重算。
        runs_to_reconcile = {
            str(run["run_id"]): run
            for run in await self._runs_io("nonterminal_runs")
        }
        for touched_run_id in touched_run_ids:
            if touched_run_id not in runs_to_reconcile:
                run = await self._runs_io("get_run", touched_run_id)
                if run is not None:
                    runs_to_reconcile[touched_run_id] = run
        for run in runs_to_reconcile.values():
            counts = await self._runs_io("proposal_counts", run["run_id"])
            current = str(run.get("status") or "")
            if any(counts.get(state, 0) for state in (
                    "applying", "undoing", "rollback_pending")):
                target = "rollback_pending"
            elif counts.get("total", 0) and not counts.get("pending", 0):
                target = "completed"
            elif counts.get("pending", 0):
                target = "awaiting_approval"
            else:
                continue
            if target != current and await self._runs_io(
                    "set_status",
                    run["run_id"], target, actor="engine",
                    detail="启动时重算治理状态"):
                runs_reconciled += 1
        return {"recovered": recovered, "pending_review": pending_review,
                "runs_reconciled": runs_reconciled}

    def cancel(self, run_id: str, actor: str = "owner") -> bool:
        """取消: 仅 queued/running 可取消 (awaiting_approval 也允许取消)。"""
        run = self.runs.get_run(run_id)
        if run is None:
            return False
        if run["status"] not in ("queued", "running", "awaiting_approval"):
            return False
        return self.runs.set_status(run_id, "cancelled", actor=actor, detail="owner_cancel")

    # ---------- proposal 生成 (确定性规则) ----------

    async def generate_proposals(self, run_id: str, mode: str,
                                 scope: str = "") -> int:
        """按模式生成 proposal (pending), 返回条数。确定性规则, 无 LLM。

        dedup: 实体事实 value 相同 -> 建议去重 (保留 id 小者);
        conflict: 同实体同 key 多版本 alive -> 建议 supersede 旧版;
        audit: 无来源事实 (legacy 无 ledger) -> 建议 repair_provenance 标注;
        prune: hit_count=0 且 importance<0.5 的规则 -> 建议归档。
        """
        run = await self._runs_io("get_run", run_id)
        if run is None:
            raise ValueError("run 不存在")
        if mode not in MODES or run.get("mode") != mode:
            raise ValueError("run 模式不匹配或尚未实现")
        await self._runs_io(
            "set_status", run_id, "running", actor="engine", detail=mode,
        )
        count = 0
        try:
            if mode == "dedup" and self.entity_store is not None:
                count = await self._gen_dedup(run_id)
            elif (mode == "audit" and self.entity_store is not None
                  and self.ledger is not None):
                count = await self._gen_audit(run_id)
            elif mode == "prune" and self.rule_store is not None:
                count = await self._gen_prune(run_id)
            elif (mode == "repair_provenance" and self.ledger is not None
                  and self.entity_store is not None):
                count = await self._gen_repair_provenance(run_id)
            else:
                raise RuntimeError(f"{mode} 所需存储不可用")
        except Exception as e:
            logger.error(f"[Curation] {mode} proposal 生成失败: {e}")
            await self._runs_io(
                "set_status", run_id, "failed", actor="engine", detail=str(e),
            )
            return 0
        terminal = "awaiting_approval" if count else "completed"
        detail = f"{count} 条 proposal" if count else "no_op: 无需处理"
        await self._runs_io(
            "set_status", run_id, terminal, actor="engine", detail=detail,
        )
        return count

    async def _gen_dedup(self, run_id: str) -> int:
        entities = await self.entity_store._load()
        groups: dict[tuple[str, str, str], list[tuple[str, str, object]]] = {}
        resolver = getattr(self.entity_store, "_resolve_alias", None)
        for name, attrs in entities.items():
            if not isinstance(attrs, dict):
                continue
            canonical = resolver(name) if callable(resolver) else name
            for key, value in attrs.items():
                if str(key).startswith("__"):
                    continue
                value_text = (value.get("value", str(value))
                              if isinstance(value, dict) else str(value))
                groups.setdefault(
                    (canonical, key, str(value_text)), [],
                ).append((name, key, value))
        count = 0
        for (canonical, key, value_text), entries in groups.items():
            if len(entries) < 2:
                continue
            keep = next((entry for entry in entries if entry[0] == canonical),
                        entries[0])
            for name, _, _ in entries:
                if name == keep[0]:
                    continue
                try:
                    version = len(await self.entity_store.entity_history(
                        name, key) or [])
                except Exception:
                    version = 0
                await self._runs_io(
                    "add_proposal",
                    run_id, "dedup", target_id=f"legacy_entity_{name}.{key}",
                    target_kind="semantic_fact",
                    content=(f"事实「{name}.{key}」= 「{value_text[:80]}」"
                             f" 与「{keep[0]}.{key}」重复"),
                    reason="已配置别名的同一实体存在重复事实",
                    evidence=f"canonical={canonical}; key/value 完全一致",
                    version=version,
                )
                count += 1
        return count

    async def _gen_audit(self, run_id: str) -> int:
        entities = await self.entity_store._load()
        count = 0
        for name, attrs in entities.items():
            if not isinstance(attrs, dict):
                continue
            for k, v in attrs.items():
                if str(k).startswith("__"):
                    continue
                ref = f"{name}.{k}"
                existing = (await self._ledger_io(
                    "find_by_store_ref", "entity", ref,
                ) if self.ledger is not None else None)
                if existing and existing.get("status") not in ("pruned", "forgotten"):
                    continue
                # [0.3.34.0 B1b] 携带事实当前值: 老板核验要看得到内容,
                # 不是一句"无来源记录"。长值截断防卡片爆炸。
                value_text = v.get("value", str(v)) if isinstance(v, dict) else str(v)
                value_preview = str(value_text).strip()[:80]
                try:
                    version = len(await self.entity_store.entity_history(
                        name, k) or [])
                except Exception:
                    version = 0
                await self._runs_io(
                    "add_proposal",
                    run_id, "repair_provenance",
                    target_id=f"legacy_entity_{ref}",
                    target_kind="semantic_fact",
                    content=(f"事实「{name}.{k}」= 「{value_preview}」"
                             f" (无 ledger 来源记录)"),
                    reason="老数据未回填来源",
                    evidence=f"ledger 缺少 store=entity, store_ref={ref}",
                    version=version,
                )
                count += 1
        return count

    async def _gen_prune(self, run_id: str) -> int:
        rules = await self.rule_store.get_all_rules()
        count = 0
        for r in rules:
            if not isinstance(r, dict):
                continue
            hits = int(r.get("hit_count", 0) or 0)
            if "importance" not in r:
                continue
            importance = float(r.get("importance") or 0.0)
            if hits == 0 and importance < 0.5:
                await self._runs_io(
                    "add_proposal",
                    run_id, "prune", target_id=f"legacy_rule_{r.get('id', '')}",
                    target_kind="procedural_rule",
                    content=f"规则「{r.get('text', '')}」长期不活跃",
                    reason="hit_count=0 且 importance<0.5",
                    evidence=f"hits={hits}, importance={importance:.2f}",
                    version=self._version_token(r),
                )
                count += 1
        return count

    async def _gen_repair_provenance(self, run_id: str) -> int:
        # 来源修复与 audit 使用同一精确 store_ref 缺口，绝不猜测来源。
        return await self._gen_audit(run_id)

    # ---------- apply / undo ----------

    async def apply_proposal(self, run_id: str, proposal_id: int,
                             actor: str = "owner") -> dict:
        """apply 单条 proposal: 校验目标版本 -> 执行 -> undo checkpoint。

        返回 {"ok": True} 或 {"ok": False, "error": "...", "status": 409/403}。
        """
        proposal = await self._runs_io("get_proposal", proposal_id)
        if proposal is None:
            return {"ok": False, "error": "proposal 不存在", "status": 404}
        if proposal["status"] != "pending":
            return {"ok": False, "error": "proposal 已处理", "status": 409}
        if proposal["run_id"] != run_id:
            return {"ok": False, "error": "proposal 不属于该 run", "status": 409}
        if proposal["action"] not in SUPPORTED_ACTIONS:
            return {"ok": False, "error": "该治理动作尚未实现，已禁用",
                    "status": 400}

        # apply 前版本校验 (规格: 防止过期提议覆盖新事实)
        current_version = await self._current_version(proposal)
        if current_version is None:
            return {"ok": False, "error": "无法验证目标版本, 请重新生成",
                    "status": 409}
        if current_version != proposal["version"]:
            return {"ok": False, "error": "目标已变化, 请重新生成",
                    "status": 409}

        # 先以 CAS 原子 claim，阻止双击或多进程重复执行同一 proposal。
        undo_snapshot = await self._capture_undo(proposal)
        if not self._undo_snapshot_complete(proposal, undo_snapshot):
            return {"ok": False, "error": "无法创建完整 undo 快照",
                    "status": 409}
        if not await self._runs_io(
                "compare_and_set_proposal",
                proposal_id, "pending", "applying", undo_snapshot):
            return {"ok": False, "error": "proposal 已被其他请求处理",
                    "status": 409}
        proposal_with_undo = {**proposal, "undo_snapshot": undo_snapshot}
        try:
            claimed_version = await self._current_version(proposal)
            if claimed_version is None or claimed_version != proposal["version"]:
                await self._runs_io(
                    "compare_and_set_proposal",
                    proposal_id, "applying", "pending",
                )
                return {"ok": False, "error": "目标已变化, 请重新生成",
                        "status": 409}
            ok = await self._execute_action(proposal_with_undo)
            if not ok:
                await self._runs_io(
                    "compare_and_set_proposal",
                    proposal_id, "applying", "pending",
                )
                return {"ok": False, "error": "目标不存在或执行失败",
                        "status": 404}
            updated = await self._runs_io(
                "compare_and_set_proposal",
                proposal_id, "applying", "applied",
            )
            if not updated:
                restored = await self._restore_undo(proposal_with_undo)
                if restored:
                    await self._runs_io(
                        "compare_and_set_proposal",
                        proposal_id, "applying", "pending",
                    )
                else:
                    await self._runs_io(
                        "set_status",
                        run_id, "rollback_pending", actor="engine",
                        detail=f"proposal {proposal_id} 状态确认失败",
                    )
                return {"ok": False, "error": "状态写入失败，已进入回滚处理",
                        "status": 500}
            await self._runs_io(
                "set_status", run_id, "applying", actor=actor,
                detail=f"proposal {proposal_id} applied",
            )
            # [0.3.39.2 G11a] 全处理完 -> 收敛 completed (治停 applying)
            await self._maybe_finalize(run_id)
            return {"ok": True, "proposal_id": proposal_id,
                    "undo_available": True}
        except Exception as e:
            logger.error(f"[Curation] apply 失败: {e}")
            await self._runs_io(
                "set_status",
                run_id, "rollback_pending", actor="engine",
                detail=f"proposal {proposal_id} apply 异常",
            )
            return {"ok": False, "error": str(e), "status": 500}

    async def undo_proposal(self, run_id: str, proposal_id: int,
                            actor: str = "owner") -> dict:
        """undo: 只回滚本次 proposal 触碰的对象 (规格: undo 只回滚本 run)。"""
        proposal = await self._runs_io("get_proposal", proposal_id)
        if proposal is None:
            return {"ok": False, "error": "proposal 不存在", "status": 404}
        if proposal["run_id"] != run_id:
            return {"ok": False, "error": "proposal 不属于该 run", "status": 409}
        if proposal["status"] != "applied" or not proposal["undo_snapshot"]:
            return {"ok": False, "error": "无 undo checkpoint", "status": 409}
        if not await self._runs_io(
                "compare_and_set_proposal",
                proposal_id, "applied", "undoing"):
            return {"ok": False, "error": "proposal 已被其他请求处理",
                    "status": 409}
        try:
            restored = await self._restore_undo(proposal)
            if restored is not True:
                await self._runs_io(
                    "compare_and_set_proposal",
                    proposal_id, "undoing", "applied",
                )
                return {"ok": False, "error": "底层数据恢复失败",
                        "status": 409}
            if not await self._runs_io(
                    "compare_and_set_proposal",
                    proposal_id, "undoing", "pending"):
                await self._runs_io(
                    "set_status",
                    run_id, "rollback_pending", actor="engine",
                    detail=f"proposal {proposal_id} undo 状态确认失败",
                )
                return {"ok": False, "error": "proposal 状态恢复失败",
                        "status": 500}
            await self._runs_io(
                "set_status", run_id, "rollback_pending", actor=actor,
                detail=f"proposal {proposal_id} undone",
            )
            return {"ok": True, "restored": restored}
        except Exception as e:
            logger.error(f"[Curation] undo 失败: {e}")
            await self._runs_io(
                "compare_and_set_proposal",
                proposal_id, "undoing", "applied",
            )
            return {"ok": False, "error": str(e), "status": 500}

    async def reject_proposal(self, run_id: str, proposal_id: int,
                              actor: str = "owner") -> dict:
        """[0.3.34.0 B1a] 单条否定: pending -> rejected。

        不动底层数据 (否定本身零副作用), 仅状态机 + 审计事件,
        老板的否决意见留痕。幂等语义同 apply: 已处理 409。
        """
        proposal = await self._runs_io("get_proposal", proposal_id)
        if proposal is None:
            return {"ok": False, "error": "proposal 不存在", "status": 404}
        if proposal["status"] != "pending":
            return {"ok": False, "error": "proposal 已处理", "status": 409}
        if proposal["run_id"] != run_id:
            return {"ok": False, "error": "proposal 不属于该 run",
                    "status": 409}
        if not await self._runs_io(
                "compare_and_set_proposal",
                proposal_id, "pending", "rejected"):
            return {"ok": False, "error": "proposal 已被其他请求处理",
                    "status": 409}
        await self._runs_io(
            "set_status", run_id, "awaiting_approval", actor=actor,
            detail=f"proposal {proposal_id} rejected",
        )
        # [0.3.39.2 G11a] 全部否定同样收敛 completed
        await self._maybe_finalize(run_id)
        return {"ok": True, "proposal_id": proposal_id}

    async def _maybe_finalize(self, run_id: str) -> None:
        """[0.3.39.2 G11a] 终态收敛: run 的 proposal 全部处理完 (无 pending)
        时, 把 applying/awaiting_approval 收敛到 completed。

        治病灶: 旧状态机进 applying 后无任何路径到 completed, run 永停
        applying (G11a 审计实锤)。收敛失败非致命, 不挡 apply 主链。
        """
        try:
            run = await self._runs_io("get_run", run_id)
            if run is None or run["status"] not in (
                    "applying", "awaiting_approval", "rollback_pending"):
                return
            counts = await self._runs_io("proposal_counts", run_id)
            if any(counts.get(status, 0) for status in (
                    "pending", "applying", "undoing", "rollback_pending")):
                return
            applied = counts.get("applied", 0)
            rejected = counts.get("rejected", 0)
            await self._runs_io(
                "set_status",
                run_id, "completed", actor="engine",
                detail=f"全部处理完: applied {applied} / rejected {rejected}")
        except Exception as e:
            logger.warning(f"[Curation] 终态收敛失败(非致命): {e}")

    # ---------- 内部辅助 ----------

    async def _current_version(self, proposal: dict) -> Optional[int]:
        """目标当前版本 (供 apply 前校验)。无版本概念返回 None (跳过校验)。"""
        if proposal["target_kind"] == "procedural_rule":
            if self.rule_store is None:
                return None
            try:
                rules = await self.rule_store.get_all_rules()
                target = next((
                    row for row in rules if isinstance(row, dict)
                    and f"legacy_rule_{row.get('id', '')}"
                    == proposal["target_id"]
                ), None)
                return self._version_token(target) if target is not None else None
            except Exception:
                return None
        # 实体事实: history 长度作版本 (conflict 场景)
        if self.entity_store is not None and proposal["target_id"].startswith("legacy_entity_"):
            ent_key = proposal["target_id"][len("legacy_entity_"):]
            ent, _, key = ent_key.rpartition(".")
            try:
                history = await self.entity_store.entity_history(ent, key)
                return len(history or [])
            except Exception:
                return None
        return None

    async def _capture_undo(self, proposal: dict) -> str:
        """apply 前快照 (JSON 脱敏): 保存精确恢复所需的最小状态。"""
        import json
        snapshot = {"target_id": proposal["target_id"], "action": proposal["action"]}
        if proposal["target_kind"] == "procedural_rule" and self.rule_store is not None:
            try:
                rules = await self.rule_store.get_all_rules()
                for r in rules:
                    if isinstance(r, dict) and f"legacy_rule_{r.get('id', '')}" == proposal["target_id"]:
                        snapshot["rule_text"] = r.get("text", "")
                        snapshot["rule"] = r
                        if self.ledger is not None:
                            row = await self._ledger_io(
                                "find_by_store_ref", "rule",
                                f"rule_{r.get('id', '')}",
                            )
                            if row:
                                snapshot["ledger_memory_id"] = row["memory_id"]
                                snapshot["ledger_prior_status"] = row.get("status")
                        break
            except Exception:
                pass
        # [0.3.34.0 B1d] dedup 快照被并事实全量 (含 history 等结构), undo 原样还原
        if (proposal["action"] == "dedup" and self.entity_store is not None
                and proposal["target_id"].startswith("legacy_entity_")):
            try:
                ent_key = proposal["target_id"][len("legacy_entity_"):]
                ent, _, key = ent_key.rpartition(".")
                entities = await self.entity_store._load()
                attrs = entities.get(ent, {})
                if isinstance(attrs, dict) and key in attrs:
                    snapshot["entity"] = ent
                    snapshot["key"] = key
                    snapshot["value"] = attrs[key]
                    if self.ledger is not None:
                        row = await self._ledger_io(
                            "find_by_store_ref", "entity", f"{ent}.{key}",
                        )
                        if row:
                            snapshot["ledger_memory_id"] = row["memory_id"]
                            snapshot["ledger_prior_status"] = row.get("status")
            except Exception:
                pass
        if proposal["action"] == "repair_provenance" and self.ledger is not None:
            from system_memory_god.storage.memory_ledger import new_memory_id
            ent_key = proposal["target_id"].removeprefix("legacy_entity_")
            try:
                existing = await self._ledger_io(
                    "find_by_store_ref_strict", "entity", ent_key,
                )
                snapshot["ledger_lookup_verified"] = True
                snapshot["ledger_memory_id"] = (
                    existing["memory_id"] if existing else new_memory_id()
                )
                snapshot["ledger_prior_status"] = (
                    existing.get("status") if existing else None
                )
                snapshot["repair_created_at"] = time.time()
                ent, _, key = ent_key.rpartition(".")
                entities = await self.entity_store._load()
                value = (entities.get(ent) or {}).get(key)
                value_text = (value.get("value", "")
                              if isinstance(value, dict) else value)
                expected = existing or {
                    "kind": "semantic_fact", "store": "entity",
                    "store_ref": ent_key,
                    "content_preview": f"{ent} {key}: {value_text}",
                    "actor": "unknown", "speaker": ent,
                    "chat_name": "", "visibility": "",
                    "is_master_source": 0,
                    "created_at": snapshot["repair_created_at"],
                    "entity_revision": (
                        value.get("tx_time") if isinstance(value, dict)
                        else None
                    ),
                }
                snapshot["ledger_expected_token"] = self._ledger_row_token(
                    expected,
                )
            except Exception as e:
                logger.warning(
                    f"[Curation] 来源快照账本查询失败，拒绝执行: {e}"
                )
        return json.dumps(snapshot, ensure_ascii=False)

    @staticmethod
    def _undo_snapshot_complete(proposal: dict, snapshot_text: str) -> bool:
        try:
            snap = json.loads(snapshot_text or "{}")
        except (json.JSONDecodeError, TypeError):
            return False
        action = proposal.get("action")
        if action == "prune":
            rule = snap.get("rule")
            return (isinstance(rule, dict) and rule.get("id") is not None
                    and bool(str(rule.get("text") or "").strip()))
        if action == "dedup":
            return (bool(snap.get("entity")) and bool(snap.get("key"))
                    and "value" in snap)
        if action == "repair_provenance":
            return (snap.get("ledger_lookup_verified") is True
                    and bool(snap.get("ledger_memory_id"))
                    and "ledger_prior_status" in snap
                    and bool(snap.get("ledger_expected_token"))
                    and isinstance(snap.get("repair_created_at"), (int, float))
                    and snap.get("ledger_prior_status") in (
                        None, "pruned", "forgotten",
                    ))
        return False

    async def _ledger_matches_snapshot(self, snap: dict) -> bool:
        memory_id = str(snap.get("ledger_memory_id") or "")
        if not memory_id or self.ledger is None:
            return True
        row = await self._ledger_io("get_item", memory_id)
        prior = snap.get("ledger_prior_status")
        return ((row is None and prior is None)
                or (row is not None and row.get("status") == prior))

    async def _matches_undo_snapshot(self, proposal: dict) -> bool:
        """底层当前值是否已经等于 apply 前快照。"""
        try:
            snap = json.loads(proposal.get("undo_snapshot") or "{}")
            action = proposal.get("action")
            if action == "prune" and self.rule_store is not None:
                expected = snap.get("rule")
                if not isinstance(expected, dict):
                    return False
                rules = await self.rule_store.get_all_rules()
                return (any(row == expected for row in rules)
                        and await self._ledger_matches_snapshot(snap))
            if action == "dedup" and self.entity_store is not None:
                ent, key = snap.get("entity"), snap.get("key")
                if not ent or not key:
                    return False
                data = await self.entity_store._load()
                return (isinstance(data.get(ent), dict)
                        and data[ent].get(key) == snap.get("value")
                        and await self._ledger_matches_snapshot(snap))
            if action == "repair_provenance" and self.ledger is not None:
                memory_id = str(snap.get("ledger_memory_id") or "")
                ent_key = str(proposal.get("target_id") or "").removeprefix(
                    "legacy_entity_",
                )
                row = (await self._ledger_io(
                    "find_by_store_ref_strict", "entity", ent_key,
                ) if memory_id and ent_key else None)
                if row is not None and row.get("memory_id") != memory_id:
                    return False
                if (row is not None and self._ledger_row_token(row)
                        != snap.get("ledger_expected_token")):
                    return False
                prior = snap.get("ledger_prior_status")
                # 新建来源行的 undo 契约是保留审计行并标记 pruned；因此
                # row 不存在与目标行已 pruned 都代表 undo 已完成。
                if prior is None:
                    return row is None or row.get("status") == "pruned"
                return row is not None and row.get("status") == prior
        except Exception as e:
            logger.warning(f"[Curation] 中间态快照核验失败(非致命): {e}")
        return False

    async def _execute_action(self, proposal: dict) -> bool:
        """按 action 执行。prune 按规则文本删除 (undo 可还原)。"""
        if proposal["action"] == "prune" and self.rule_store is not None:
            try:
                # proposal 以稳定规则 ID 定位，删除也必须沿用同一 ID。
                rules = await self.rule_store.get_all_rules()
                for r in rules:
                    if isinstance(r, dict) and f"legacy_rule_{r.get('id', '')}" == proposal["target_id"]:
                        forget_by_id = getattr(
                            self.rule_store, "forget_rule_by_id", None,
                        )
                        if callable(forget_by_id):
                            ok = await forget_by_id(int(r.get("id")))
                        else:
                            ok = await self.rule_store.forget_rule_by_text(
                                str(r.get("text", "")),
                            )
                        if ok and self.ledger is not None:
                            row = await self._ledger_io(
                                "find_by_store_ref", "rule",
                                f"rule_{r.get('id', '')}",
                            )
                            if row:
                                await self._ledger_io(
                                    "mark_status", row["memory_id"], "pruned",
                                )
                                await self._ledger_io(
                                    "record_event",
                                    row["memory_id"], "pruned", actor="owner",
                                    reason="Curation 低价值规矩归档",
                                )
                        return bool(ok)
                return False
            except Exception as e:
                logger.warning(f"[Curation] prune 执行失败(非致命): {e}")
                return False
        # [0.3.34.0 B1d] dedup 真合并: 删除被并事实 (保留首条), 事件入账。
        # 老板 2026-08-18 验收: apply 空操作 = 摆设。undo 经快照还原。
        if proposal["action"] == "dedup" and self.entity_store is not None:
            if not proposal["target_id"].startswith("legacy_entity_"):
                return False
            ent_key = proposal["target_id"][len("legacy_entity_"):]
            ent, _, key = ent_key.rpartition(".")
            try:
                ok = await self.entity_store.forget_fact(ent, key)
            except Exception as e:
                logger.warning(f"[Curation] dedup 执行失败(非致命): {e}")
                return False
            if ok and self.ledger is not None:
                try:
                    row = await self._ledger_io(
                        "find_by_store_ref", "entity", f"{ent}.{key}",
                    )
                    if row:
                        await self._ledger_io(
                            "record_event",
                            row["memory_id"], "curation_dedup_merged",
                            actor="owner",
                            reason=f"重复事实合并: 删除别名事实 {ent}.{key}")
                        await self._ledger_io(
                            "mark_status", row["memory_id"], "pruned",
                        )
                except Exception as e:
                    logger.warning(f"[Curation] dedup 事件入账失败(非致命): {e}")
            return bool(ok)
        if (proposal["action"] == "repair_provenance"
                and self.entity_store is not None and self.ledger is not None):
            if not proposal["target_id"].startswith("legacy_entity_"):
                return False
            ent_key = proposal["target_id"][len("legacy_entity_"):]
            ent, _, key = ent_key.rpartition(".")
            if not ent or not key:
                return False
            entities = await self.entity_store._load()
            value = (entities.get(ent) or {}).get(key)
            if value is None:
                return False
            value_text = (value.get("value", "")
                          if isinstance(value, dict) else value)
            from system_memory_god.models import MemoryItem
            import json
            snapshot = json.loads(proposal.get("undo_snapshot") or "{}")
            memory_id = str(snapshot.get("ledger_memory_id") or "")
            if not memory_id:
                return False
            try:
                existing = await self._ledger_io(
                    "find_by_store_ref_strict", "entity", ent_key,
                )
            except Exception as e:
                logger.warning(f"[Curation] 来源执行账本核验失败: {e}")
                return False
            prior_status = snapshot.get("ledger_prior_status")
            if prior_status is not None:
                if prior_status not in ("pruned", "forgotten"):
                    return False
                if (not existing or existing.get("memory_id") != memory_id
                        or existing.get("status") != prior_status):
                    return False
                # Ledger 写失败会进入持久 outbox，因此“已接受等待重放”也算
                # 本治理动作成功，避免底层已变而 proposal 回到 pending。
                await self._ledger_io("mark_status", memory_id, "active")
                await self._ledger_io(
                    "record_event",
                    memory_id, "provenance_repaired", actor="owner",
                    reason="Curation 恢复精确 store_ref 来源",
                )
                return True
            if existing is not None:
                return False
            item = MemoryItem(
                memory_id=memory_id, kind="semantic_fact",
                store="entity", store_ref=ent_key,
                content_preview=f"{ent} {key}: {value_text}",
                actor="unknown", speaker=ent,
                created_at=float(snapshot["repair_created_at"]),
                entity_revision=(
                    value.get("tx_time") if isinstance(value, dict) else None
                ),
                status="active",
                has_history=bool(value.get("history"))
                if isinstance(value, dict) else False,
            )
            insert_result = await self._ledger_io(
                "record_item_if_store_ref_absent", item,
            )
            if insert_result != "inserted":
                return False
            await self._ledger_io(
                "record_event",
                item.memory_id, "provenance_repaired", actor="owner",
                reason="Curation 精确 store_ref 回填",
            )
            return True
        return False

    async def _restore_undo(self, proposal: dict) -> bool:
        """undo 恢复 (prune 还原规则文本 / [B1d] dedup 还原被并事实)。"""
        if proposal["action"] == "prune" and self.rule_store is not None:
            import json
            try:
                snap = json.loads(proposal["undo_snapshot"])
                rule = snap.get("rule")
                restore = getattr(self.rule_store, "restore_rule", None)
                if isinstance(rule, dict) and callable(restore):
                    rules = await self.rule_store.get_all_rules()
                    exact_present = any(row == rule for row in rules)
                    restored = exact_present or await restore(rule)
                else:
                    text = snap.get("rule_text", "")
                    restored = bool(text and await self.rule_store.add_rule(text))
                if not restored:
                    return False
                if self.ledger is not None and isinstance(rule, dict):
                    row = await self._ledger_io(
                        "find_by_store_ref", "rule",
                        f"rule_{rule.get('id', '')}",
                    )
                    if row:
                        await self._ledger_io(
                            "mark_status",
                            row["memory_id"], str(
                                snap.get("ledger_prior_status") or "active"
                            ),
                        )
                        await self._ledger_io(
                            "record_event",
                            row["memory_id"], "restored", actor="owner",
                            reason="撤销 Curation 规矩归档",
                        )
                        return True
                return True
            except Exception as e:
                logger.warning(f"[Curation] undo 恢复失败(非致命): {e}")
                return False
        # [0.3.34.0 B1d] dedup undo: 快照里的被并事实原样写回实体存储。
        if proposal["action"] == "dedup" and self.entity_store is not None:
            import json
            try:
                snap = json.loads(proposal["undo_snapshot"])
                ent, key, value = snap.get("entity"), snap.get("key"), snap.get("value")
                if ent and key and value is not None:
                    data = await self.entity_store._load()
                    attrs = data.get(ent)
                    if not isinstance(attrs, dict):
                        attrs = {}
                        data[ent] = attrs
                    if key in attrs:
                        # apply 后出现不同新写入时不得覆盖；完全等于快照则是
                        # 崩溃留下的半恢复，继续补齐 Ledger 即可。
                        if attrs[key] != value:
                            return False
                    else:
                        attrs[key] = value
                        await self.entity_store._save(data)
                    if self.ledger is not None:
                        row = await self._ledger_io(
                            "find_by_store_ref", "entity", f"{ent}.{key}",
                        )
                        if row:
                            await self._ledger_io(
                                "mark_status",
                                row["memory_id"], str(
                                    snap.get("ledger_prior_status") or "active"
                                ),
                            )
                            await self._ledger_io(
                                "record_event",
                                row["memory_id"], "restored", actor="owner",
                                reason="撤销 Curation dedup",
                            )
                    return True
            except Exception as e:
                logger.warning(f"[Curation] dedup undo 恢复失败(非致命): {e}")
                return False
        if proposal["action"] == "repair_provenance" and self.ledger is not None:
            import json
            snap = json.loads(proposal.get("undo_snapshot") or "{}")
            memory_id = str(snap.get("ledger_memory_id") or "")
            ent_key = str(proposal.get("target_id") or "").removeprefix(
                "legacy_entity_",
            )
            try:
                row = (await self._ledger_io(
                    "find_by_store_ref_strict", "entity", ent_key,
                ) if memory_id and ent_key else None)
            except Exception as e:
                logger.warning(f"[Curation] 来源 undo 账本核验失败: {e}")
                return False
            prior_status = snap.get("ledger_prior_status")
            if row is not None and row.get("memory_id") != memory_id:
                return False
            if (row is not None and self._ledger_row_token(row)
                    != snap.get("ledger_expected_token")):
                return False
            if prior_status is None:
                if row is None or row.get("status") == "pruned":
                    return True
                if row.get("status") != "active":
                    return False
            else:
                if row is None:
                    return False
                if row.get("status") == prior_status:
                    return True
                if row.get("status") != "active":
                    return False
            await self._ledger_io(
                "mark_status",
                memory_id, str(prior_status or "pruned"),
            )
            await self._ledger_io(
                "record_event",
                memory_id, "provenance_repair_undone", actor="owner",
                reason="撤销 Curation 来源回填",
            )
            return True
        return False
