"""Scoped daily recall. Candidate indexes never grant visibility."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import stat
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import time
from collections.abc import Sequence
from typing import Awaitable, TypeVar

from kernel.memory_pipeline_models import PipelineScope, PurgeInventory, SourceRevision, derived_memory_id
from kernel.raw_message_models import (
    RawMessageEvidence, RawMessageError, Scope, ScopeError, StoredMessage,
)
from kernel.raw_message_store import _PurgeProof
from kernel.daily_memory_contracts import DailyRecallOutcome, RecallStatus, RecallPurpose, recall_deadline
from kernel.daily_memory_contracts import (
    MEMORY_DOMAINS, MAX_MEMORY_QUERY_CHARS, validate_memory_query_options, memory_query_terms,
)
from contextvars import ContextVar

_QUERY_TRACE: ContextVar[dict | None] = ContextVar("memory_query_trace", default=None)

logger = logging.getLogger(__name__)
_T = TypeVar("_T")
_MAX_LEXICAL_CANDIDATES = 5000
_MAX_RAW_CANDIDATES = 500
_CANDIDATE_SCAN_SECONDS = 30.0
_EVIDENCE_BYTES = 1024 * 1024 - 4096  # Reserve the bounded query and wire envelope.


def _integrity_valid(message: StoredMessage | RawMessageEvidence) -> bool:
    # Evidence factories verify the complete original inside the read process.
    return (isinstance(message, RawMessageEvidence)
            or hashlib.sha256(message.content.encode()).hexdigest() == message.content_hash)


def _excerpt_metadata(message, *, prefix: str = "") -> dict:
    if not isinstance(message, RawMessageEvidence):
        return {prefix + "content_hash": message.content_hash,
                prefix + "content_is_excerpt": False,
                prefix + "excerpt_start_char": 0,
                prefix + "excerpt_end_char": len(message.content),
                prefix + "total_content_chars": len(message.content),
                prefix + "total_content_bytes": len(message.content.encode())}
    return {prefix + key: getattr(message, key) for key in (
        "content_hash", "content_is_excerpt", "excerpt_start_char", "excerpt_end_char",
        "total_content_chars", "total_content_bytes")}


def _database_present(path: Path) -> bool:
    """An absent optional DB is safe only when none of its sidefiles remain."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        info = None
    if info is not None and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                             or getattr(info, "st_file_attributes", 0)
                             & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise RuntimeError("pipeline database unavailable for purge")
    if info is None:
        for suffix in ("-journal", "-wal", "-shm"):
            try:
                path.with_name(path.name + suffix).lstat()
            except FileNotFoundError:
                continue
            raise RuntimeError("pipeline database unavailable for purge")
    return info is not None


async def _settle_write(operation: Awaitable[_T]) -> _T:
    pending = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError:
        while not pending.done():
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            pending.result()
        except BaseException:
            pass
        raise

_SCOPE_DOMAINS = {
    "auto": frozenset({"raw", "vector", "entity", "rule", "graph", "journal", "schedule"}),
    "raw": frozenset({"raw"}),
    "recents": frozenset({"raw", "vector", "journal"}),
    "portrait": frozenset({"entity"}),
    "relations": frozenset({"vector", "graph"}),
    "rules": frozenset({"rule"}),
    "disabled": frozenset(),
}


def daily_retrieval_enabled(bus) -> bool:
    """Inspect explicit assembly state, not a test double's dynamic attribute."""
    return getattr(bus, "__dict__", {}).get("_daily_retrieval") is not None


def _partition(scope: Scope) -> PipelineScope:
    if (not isinstance(scope, Scope) or scope.connector_id is None
            or scope.conversation_id is None):
        raise ScopeError("complete conversation scope required")
    return PipelineScope(scope.user_id, scope.agent_id,
                         scope.connector_id, scope.conversation_id)


def _in_window(created_at: str, start_ts: float | None,
               end_ts: float | None) -> bool:
    stamp = datetime.fromisoformat(created_at).timestamp()
    return ((start_ts is None or stamp >= start_ts)
            and (end_ts is None or stamp < end_ts))


def render_daily_evidence(items: list[dict], *, max_chars: int = 4000) -> str:
    """Budget complete evidence blocks; every shown original range is exact."""
    if type(max_chars) is not int or max_chars < 1:
        return ""
    parts: list[str] = []
    used = 0
    for item in items:
        kind = item.get("domain", "")
        identifier = item.get("message_id") or item.get("memory_id") or ""
        # (header, content, start, label, revision, complete-original hash, cap)
        blocks = [(f"[{kind} {identifier} {item.get('source_time', '')}]",
                   str(item.get("content", "")), item.get("excerpt_start_char", 0),
                   "原文片段" if kind == "raw" else "", item.get("revision", 0),
                   item.get("content_hash", ""), 1000)]
        if kind not in {"raw", "legacy_source"}:
            blocks.append((f"[原文 {item.get('source_message_id', '')} {item.get('source_time', '')}]",
                           str(item.get("source_content", "")),
                           item.get("source_excerpt_start_char", 0), "来源片段",
                           item.get("revision", 0), item.get("source_content_hash", ""), 500))
        for neighbor in item.get("neighbors", []):
            blocks.append((f"[邻文 {neighbor['message_id']} {neighbor['created_at']}]",
                           str(neighbor["content"]), neighbor.get("excerpt_start_char", 0),
                           "邻文片段", neighbor["revision"], neighbor.get("content_hash", ""), 300))
        if item.get("contributing_domains") and kind != "raw":
            blocks = [blocks[1], *blocks[2:], blocks[0]]
        for fact in item.get("structured_facts", ()):
            blocks.append((f"[结构证据 {fact['domain']} {fact.get('fact_status', '')}]",
                           json.dumps(fact.get("payload", {}), ensure_ascii=False),
                           0, "", item.get("revision", 0), "", 300))
        caps = [min(len(block[1]), block[6]) for block in blocks]

        def render() -> str:
            lines = []
            for (header, content, start, label, revision, checksum, _), cap in zip(blocks, caps):
                if label:
                    # Legacy full-original DTOs may predate excerpt metadata.
                    if not checksum and start == 0:
                        checksum = hashlib.sha256(content.encode()).hexdigest()
                    header += f" [{label} {start}:{start + cap} revision={revision} sha256={checksum}]"
                lines.append(header + " " + content[:cap])
            return "\n".join(lines)

        remaining = max_chars - used - bool(parts)
        line = render()
        while len(line) > remaining and any(caps):
            index = max(range(len(caps)), key=caps.__getitem__)
            caps[index] = max(0, caps[index] - (len(line) - remaining))
            line = render()
        if len(line) > remaining:
            break
        parts.append(line)
        used += len(line) + (len(parts) > 1)
    return "\n".join(parts)


class RawMessageRetrieval:
    """Read only current Raw originals and their published, sourced evidence."""

    def __init__(self, raw, stores, vector_index=None, *,
                 request_timeout_seconds: float = 30.0, collect_only: bool = False,
                 candidate_byte_budget: int = _EVIDENCE_BYTES, query_options=None):
        if (type(request_timeout_seconds) not in {int, float}
                or not math.isfinite(request_timeout_seconds)
                or not 0 < request_timeout_seconds <= 300):
            raise ValueError("invalid recall timeout")
        self.raw = raw
        self.stores = stores
        self.vector_index = vector_index
        self.request_timeout_seconds = float(request_timeout_seconds)
        if type(collect_only) is not bool:
            raise ValueError("invalid candidate mode")
        self.collect_only = collect_only
        if type(candidate_byte_budget) is not int or not 1 <= candidate_byte_budget <= _EVIDENCE_BYTES:
            raise ValueError("invalid evidence byte budget")
        self._candidate_byte_budget = candidate_byte_budget
        self.query_options = validate_memory_query_options(query_options)

    def _begin_trace(self):
        value = None
        if self.query_options is not None and self.query_options["trace"]:
            value = {"schema": 1, "domains": {domain: {
                "attempted": False, "status": "disabled", "reason": "not_selected",
                "candidate_count": 0, "selected_count": 0, "source_ids": [],
                "processor_versions": [], "elapsed_ms": 0.0, "limited": False,
            } for domain in MEMORY_DOMAINS}}
            for domain in self.query_options["enabled"]:
                value["domains"][domain].update(status="not_applicable", reason="scope_filtered")
        _QUERY_TRACE.set(value)

    @staticmethod
    def _trace_candidates(items, kinds, elapsed_ms):
        trace = _QUERY_TRACE.get()
        if trace is None:
            return
        for domain in kinds:
            entry = trace["domains"][domain]
            rows = list({item.get("memory_id", item["source_message_id"]): item
                         for item in items if item["domain"] == domain}.values())[:100]
            entry.update(attempted=True, status="ready" if rows else "empty", reason="none",
                         candidate_count=len(rows), elapsed_ms=round(
                             elapsed_ms.get(domain, 0.0) if type(elapsed_ms) is dict else elapsed_ms, 3),
                         source_ids=list(dict.fromkeys(item["source_message_id"] for item in rows))[:100],
                         processor_versions=sorted({item["source"]["processor_version"]
                             for item in rows if "source" in item}))

    @staticmethod
    def _trace_visible_candidates(items):
        trace = _QUERY_TRACE.get()
        if trace is not None:
            RawMessageRetrieval._trace_candidates(items,
                [domain for domain, entry in trace["domains"].items() if entry["attempted"]],
                {domain: entry["elapsed_ms"] for domain, entry in trace["domains"].items()})

    @staticmethod
    def _trace_result(items, reasons):
        trace = _QUERY_TRACE.get()
        if trace is not None:
            for domain, entry in trace["domains"].items():
                entry["selected_count"] = sum(domain in item.get("contributing_domains", [item["domain"]])
                                              for item in items)
                entry["limited"] = any("budget" in reason or "deadline" in reason for reason in reasons)
                if entry["attempted"] and ("raw_unavailable" in reasons or
                        domain != "raw" and "derived_unavailable" in reasons):
                    entry.update(status="failed", reason=("raw_unavailable" if "raw_unavailable" in reasons
                                                          else "derived_unavailable"))
                elif domain != "raw" and entry["status"] != "disabled" and "derivation_pending" in reasons:
                    entry.update(status="pending", reason="derivation_pending")
        return trace

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RawMessageError("recall budget exceeded")
        return remaining

    @staticmethod
    def _merge_candidates(originals: Sequence[_T], derived: Sequence[_T]) -> list[_T]:
        """Give both untrusted candidate streams a chance before visibility checks."""
        merged = []
        for offset in range(max(len(originals), len(derived))):
            if offset < len(originals):
                merged.append(originals[offset])
            if offset < len(derived):
                merged.append(derived[offset])
        return merged

    async def _source_snapshots(
        self, scope: Scope, message_ids: tuple[str, ...], deadline: float | None,
        proofs: dict[str, tuple[tuple[str, str], ...]] | None = None,
    ) -> dict[str, tuple[StoredMessage, tuple[dict, ...]]]:
        snapshots = {}
        unique_ids = tuple(dict.fromkeys(message_ids))
        evidence_mode = getattr(self.raw, "evidence_mode", False) is True
        if evidence_mode and len(unique_ids) > 500:
            raise RawMessageError("publication source budget exceeded")
        batch_size = 500 if evidence_mode else 256
        for offset in range(0, len(unique_ids), batch_size):
            if deadline is not None:
                self._remaining(deadline)
            chunk = unique_ids[offset:offset + batch_size]
            page = await asyncio.to_thread(
                self.raw.published_snapshots, scope, chunk,
                proofs={key: proofs[key] for key in chunk if key in proofs}
                if proofs is not None else None, deadline=deadline,
            )
            snapshots.update(page)
        return snapshots

    @staticmethod
    def _candidate_proofs(items: Sequence[dict]) -> dict[str, tuple[tuple[str, str], ...]]:
        selected: dict[str, list[tuple[str, str]]] = {}
        for item in items:
            if item["domain"] != "raw":
                selected.setdefault(item["source_message_id"], []).append(
                    (item["source"]["task_id"], item["memory_id"]))
        return {key: tuple(dict.fromkeys(pairs)) for key, pairs in selected.items()}

    @staticmethod
    def _record_proofs(records: dict) -> dict[str, tuple[tuple[str, str], ...]]:
        selected: dict[str, list[tuple[str, str]]] = {}
        for memory_id, record in records.items():
            source = record["source"]
            selected.setdefault(source["raw_message_id"], []).append(
                (source["task_id"], memory_id))
        return {key: tuple(dict.fromkeys(pairs)) for key, pairs in selected.items()}

    async def _visible_candidate(self, scope: Scope, item: dict,
                                 start_ts: float | None = None,
                                 end_ts: float | None = None,
                                 deadline: float | None = None, *,
                                 snapshots: dict[str, tuple[StoredMessage,
                                                            tuple[dict, ...]]] | None = None
                                 ) -> dict | None:
        if deadline is not None:
            self._remaining(deadline)
        if snapshots is not None:
            snapshot = snapshots.get(item["source_message_id"])
            if snapshot is None:
                return None
            current = snapshot[0]
        else:
            snapshot = None
            current = await asyncio.to_thread(self.raw.get, scope,
                                              item["source_message_id"])
        if current is None:
            return None
        if (current.revision != item["revision"]
                or current.content != item["source_content"]
                or current.created_at != item["source_time"]
                or not _in_window(current.created_at, start_ts, end_ts)
                or not _integrity_valid(current)
                or isinstance(current, RawMessageEvidence) and (
                    _excerpt_metadata(current, prefix="source_") != {
                        key: item.get(key) for key in _excerpt_metadata(current, prefix="source_")})):
            return None
        if "source_role" in item and (item["source_role"] != current.role
                or item["session_id"] != current.session_id or item["ordinal"] != current.ordinal):
            return None
        if item["domain"] != "raw":
            if deadline is not None:
                self._remaining(deadline)
            anchors = (snapshot[1] if snapshot is not None else
                       await asyncio.to_thread(self.raw.results, scope,
                                               current.message_id))
            source = item["source"]
            if not any(anchor["memory_id"] == item["memory_id"]
                       and anchor["store_ref"] == item["store_ref"]
                       and anchor["target_domain"] == item["domain"]
                       and anchor["task_id"] == source["task_id"]
                       and anchor["revision"] == current.revision
                       and source["raw_message_id"] == current.message_id
                       and source["revision"] == current.revision
                       and all(anchor[key] == source[key] for key in (
                           "processor", "processor_version", "model", "prompt_hash"))
                       for anchor in anchors):
                return None
            if "neighbors" not in item:
                return item
        safe_neighbors = []
        for neighbor in item["neighbors"]:
            if deadline is not None:
                self._remaining(deadline)
            if snapshots is not None:
                nearby_snapshot = snapshots.get(neighbor["message_id"])
                nearby = nearby_snapshot[0] if nearby_snapshot is not None else None
            else:
                nearby = await asyncio.to_thread(self.raw.get, scope,
                                                 neighbor["message_id"])
            if (nearby is not None and nearby.session_id == current.session_id
                    and nearby.content == neighbor["content"]
                    and nearby.created_at == neighbor["created_at"]
                    and _in_window(nearby.created_at, start_ts, end_ts)
                    and nearby.revision == neighbor["revision"]
                    and _integrity_valid(nearby)
                    and (not isinstance(nearby, RawMessageEvidence)
                         or _excerpt_metadata(nearby) == {
                             key: neighbor.get(key) for key in _excerpt_metadata(nearby)})):
                safe_neighbors.append(neighbor)
        return {**item, "neighbors": safe_neighbors}

    def _bounded_candidates(self, candidates: list[dict], reasons: list[str] | None,
                            deadline: float | None) -> list[dict]:
        ranked = sorted(candidates, key=lambda item: (
            item.get("_rank_score", 0.5), datetime.fromisoformat(item["source_time"]).timestamp(),
            (1 if item["domain"] not in {"raw", "vector"} else
             0 if item["domain"] == "raw" else -1), item["source_message_id"]), reverse=True)
        selected: list[dict] = []
        sources: set[str] = set()
        size = 2
        for item in ranked:
            if deadline is not None:
                RawMessageRetrieval._remaining(deadline)
            ids = {item["source_message_id"], *(
                row["message_id"] for row in item.get("neighbors", ()))}
            item_bytes = len(json.dumps(item, ensure_ascii=False, separators=(",", ":"),
                                       allow_nan=False).encode()) + bool(selected)
            if (len(selected) >= 100 or len(sources | ids) > 500
                    or size + item_bytes > self._candidate_byte_budget):
                if reasons is not None and "result_budget_limited" not in reasons:
                    reasons.append("result_budget_limited")
                continue
            selected.append(item)
            sources.update(ids)
            size += item_bytes
        return selected

    async def _finalize(self, scope: Scope, candidates: list[dict], top_k: int,
                        start_ts: float | None = None,
                        end_ts: float | None = None,
                        deadline: float | None = None, *,
                        reasons: list[str] | None = None) -> list[dict]:
        """Rank only visible evidence, then recheck selected items before return."""
        try:
            if self.collect_only:
                # Internal IPC candidates carry no publication authority.
                return self._bounded_candidates(candidates, reasons, deadline)
            identities: dict[str, tuple] = {}
            for item in candidates:
                copies = [(item["source_message_id"], item["revision"], item["source_time"],
                           item["source_content"], item.get("source_content_hash"))]
                copies.extend((row["message_id"], row["revision"], row["created_at"],
                               row["content"], row.get("content_hash"))
                              for row in item.get("neighbors", ()))
                for message_id, *fingerprint in copies:
                    value = tuple(fingerprint)
                    if message_id in identities and identities[message_id] != value:
                        raise RawMessageError("candidate original conflict")
                    identities[message_id] = value
            if getattr(self.raw, "evidence_mode", False) is True:
                # Bound publication inputs before the one final Raw snapshot.
                # No keyword rescan or cross-request snapshot cache is needed.
                candidates = self._bounded_candidates(candidates, reasons, deadline)
            source_ids = tuple(dict.fromkeys(
                source_id for item in candidates
                for source_id in (item["source_message_id"], *(
                    neighbor["message_id"] for neighbor in item.get("neighbors", ())
                ))
            ))
            snapshots = await self._source_snapshots(
                scope, source_ids, deadline, self._candidate_proofs(candidates))
            visible = []
            for item in candidates:
                checked = await self._visible_candidate(scope, item, start_ts,
                                                        end_ts, deadline,
                                                        snapshots=snapshots)
                if checked is not None:
                    visible.append(checked)
            if self.query_options is not None and self.query_options["full"]:
                # Fact versions are labelled only after scope/revision/anchors
                # are proven. An invisible record cannot supersede a visible one.
                facts: dict[tuple[object, object], list[dict]] = {}
                for item in visible:
                    if item["domain"] == "entity":
                        payload = item.get("payload", {})
                        facts.setdefault((payload.get("subject"), payload.get("attribute")), []).append(item)
                for versions in facts.values():
                    latest = max(datetime.fromisoformat(item["source_time"]).timestamp() for item in versions)
                    current = [item for item in versions if datetime.fromisoformat(item["source_time"]).timestamp() == latest]
                    conflict = len({item.get("payload", {}).get("value") for item in current}) > 1
                    for item in versions:
                        item["fact_status"] = "conflict" if item in current and conflict else (
                            "latest" if item in current else "historical")
                        if item["fact_status"] == "historical":
                            item["_rank_score"] = min(item.get("_rank_score", .5), .49)
            visible.sort(key=lambda item: (
                item.get("_rank_score", 0.5),
                datetime.fromisoformat(item["source_time"]).timestamp(),
                (1 if item["domain"] not in {"raw", "vector"}
                 else 0 if item["domain"] == "raw" else -1),
                item["source_message_id"],
            ), reverse=True)
            shortlist = []
            seen_sources: set[str] = set()
            snapshot_ids: set[str] = set()
            for item in visible:
                if item["source_message_id"] not in seen_sources:
                    ids = {item["source_message_id"], *(
                        neighbor["message_id"] for neighbor in item.get("neighbors", ()))}
                    if len(snapshot_ids | ids) > 500:
                        continue
                    shortlist.append(item)
                    seen_sources.add(item["source_message_id"])
                    snapshot_ids.update(ids)
            # One transaction is the visibility point, including all neighbors.
            final_ids = tuple(dict.fromkeys(
                key for item in shortlist for key in (item["source_message_id"], *(
                    neighbor["message_id"] for neighbor in item.get("neighbors", ())
                ))
            ))
            evidence_mode = getattr(self.raw, "evidence_mode", False) is True
            final_snapshot = (snapshots if evidence_mode else await asyncio.to_thread(
                self.raw.published_snapshots, scope, final_ids,
                proofs=self._candidate_proofs(visible), deadline=deadline))
            final_visible = visible
            if self.query_options is not None and (self.query_options["full"] or self.query_options["trace"]):
                final_visible = []
                for item in visible:
                    checked = await self._visible_candidate(scope, item, start_ts, end_ts,
                                                            deadline, snapshots=final_snapshot)
                    if checked is not None:
                        final_visible.append(checked)
                self._trace_visible_candidates(final_visible)
            if self.query_options is not None and self.query_options["full"]:
                for item in shortlist:
                    same_source = [row for row in final_visible
                        if row["source_message_id"] == item["source_message_id"] and row["revision"] == item["revision"]]
                    item["contributing_domains"] = list(dict.fromkeys(row["domain"] for row in same_source))
                    item["structured_facts"] = [{key: row[key] for key in (
                        "domain", "memory_id", "payload", "source", "fact_status") if key in row}
                        for row in same_source if row["domain"] not in {"raw", "vector"}][:50]
            selected: list[dict] = []
            for item in shortlist:
                checked = await self._visible_candidate(scope, item, start_ts,
                                                        end_ts, deadline,
                                                        snapshots=final_snapshot)
                if checked is not None:
                    row = {key: value for key, value in checked.items() if key != "_rank_score"}
                    if evidence_mode and len(json.dumps([*selected, row], ensure_ascii=False,
                                separators=(",", ":"), allow_nan=False).encode()) > self._candidate_byte_budget:
                        if reasons is not None:
                            reasons.append("result_budget_limited")
                        continue
                    selected.append(row)
                if len(selected) >= top_k:
                    break
            return selected
        except Exception as exc:
            logger.warning("daily evidence validation unavailable: %s", type(exc).__name__)
            self._trace_visible_candidates([])
            if reasons is not None:
                reasons.append("raw_unavailable")
            return []

    async def purge(self, scope: Scope, message_id: str, *, expected_revision: int) -> dict:
        """Clear isolated live stores; backup and external attachment policy is separate."""
        partition = _partition(scope)
        prior = await asyncio.to_thread(self.raw.purge_receipt, scope, message_id)
        if prior is not None:
            return prior
        pending = await asyncio.to_thread(self.raw.begin_purge, scope, message_id,
                                          expected_revision=expected_revision)
        async with self.stores.gate(partition) as gate:
            return await self._purge_fenced(scope, message_id, partition, pending, gate)

    async def _purge_fenced(self, scope: Scope, message_id: str,
                            partition: PipelineScope, pending: dict, gate) -> dict:
        path = self.stores.partition(partition)
        task_domains = dict(pending["task_domains"])
        task_sources = dict(pending["task_sources"])
        for memory_id, task_id in pending["result_tasks"]:
            if (task_id not in task_domains
                    or not any(derived_memory_id(task_id, index) == memory_id
                               for index in range(100))):
                raise RuntimeError("raw purge anchor source conflict")
        def validate_sources(entries: tuple[tuple[SourceRevision, str], ...]) -> None:
            task_ids = tuple(dict.fromkeys(source.task_id for source, _ in entries))
            actual = self.raw.derivation_sources(scope, task_ids)
            for source, domain in entries:
                if actual.get(source.task_id) != (source, domain):
                    raise RuntimeError("derived source inventory conflict")
                if (source.raw_message_id == message_id
                        and (task_sources.get(source.task_id) != source
                             or task_domains.get(source.task_id) != domain)):
                    raise RuntimeError("target source inventory conflict")

        runs_path = path / "derivation_runs.db"
        runs = self.stores.runs(partition) if _database_present(runs_path) else None
        snapshots = (await asyncio.to_thread(runs.purge_inventory, message_id,
                                              validate_sources) if runs is not None else {})
        evidence_path = path / "derived_evidence.db"
        evidence = self.stores.evidence(partition) if _database_present(evidence_path) else None
        records = (await asyncio.to_thread(evidence.purge_inventory, message_id,
                                           validate_sources) if evidence is not None else {})
        anchored = {memory_id: (store_ref, domain)
                    for memory_id, store_ref, domain in pending["results"]}
        ledger_path = path / "source_ledger.db"
        ledger = self.stores.ledger(partition) if _database_present(ledger_path) else None
        ledger_items = {}
        if ledger is not None:
            ledger_inventory = await asyncio.to_thread(ledger.purge_inventory, message_id,
                                                       validate_sources)
            ledger_items = {memory_id: item for memory_id, (item, _) in ledger_inventory.items()}
            for memory_id, (_, sources) in ledger_inventory.items():
                if any(source.raw_message_id != message_id for source in sources):
                    raise RuntimeError("ledger purge source conflict")
                for source in sources:
                    if (source.task_id not in task_domains
                            or source != task_sources[source.task_id]):
                        raise RuntimeError("ledger purge source conflict")
        for memory_id, record in records.items():
            previous = anchored.get(memory_id)
            if previous is not None and previous != (record["store_ref"], record["domain"]):
                raise RuntimeError("evidence purge anchor conflict")
        for memory_id, item in ledger_items.items():
            previous = ((records[memory_id]["store_ref"], records[memory_id]["domain"])
                        if memory_id in records else anchored.get(memory_id))
            if previous is not None and previous != (item.store_ref, item.store):
                raise RuntimeError("ledger purge anchor conflict")
        ids = tuple(dict.fromkeys((*anchored, *records, *ledger_items)))
        if ids:
            if evidence is None and anchored:
                raise RuntimeError("derived evidence unavailable for purge")
            if ledger is None and (anchored or any(
                    snapshot["state"] == "ledger_written" and snapshot["result_ids"]
                    for snapshot in snapshots.values())):
                raise RuntimeError("ledger unavailable for purge")
            if runs is None and anchored:
                raise RuntimeError("derivation run unavailable for purge")
        vector_candidates = [memory_id for memory_id in ids
                             if (records[memory_id]["domain"] if memory_id in records
                                 else anchored[memory_id][1] if memory_id in anchored
                                 else ledger_items[memory_id].store) == "vector"]
        for task_id, domain in pending["task_domains"]:
            if domain == "vector":
                vector_candidates.extend(derived_memory_id(task_id, index)
                                         for index in range(100))
        vector_ids = tuple(dict.fromkeys(vector_candidates))
        vector_authority = _database_present(path / "vector_index.db")
        vector_tree = path / "vectors"
        try:
            tree_info = vector_tree.lstat()
        except FileNotFoundError:
            tree_info = None
        if tree_info is not None and (not stat.S_ISDIR(tree_info.st_mode)
                                      or getattr(tree_info, "st_file_attributes", 0)
                                      & stat.FILE_ATTRIBUTE_REPARSE_POINT):
            raise RuntimeError("vector index unavailable for purge")
        if vector_authority != (tree_info is not None):
            raise RuntimeError("vector index unavailable for purge")
        if vector_authority and not (vector_tree / "chroma.sqlite3").is_file():
            raise RuntimeError("vector index unavailable for purge")
        if vector_authority:
            await asyncio.to_thread(self.stores.validate_vector_index, partition,
                                    vector_ids)
        if vector_ids and not vector_authority and any(
                memory_id in anchored for memory_id in vector_ids):
            raise RuntimeError("vector index unavailable for purge")
        if vector_authority:
            raise RuntimeError("vector partition rebuild required for purge")
        inventory = PurgeInventory(partition, message_id, pending["revision"],
                                   pending["task_ids"], ids, vector_ids)
        if evidence is not None:
            for memory_id in inventory.memory_ids:
                await _settle_write(asyncio.to_thread(evidence.purge_one, memory_id, message_id))
        if ledger is not None:
            for memory_id in inventory.memory_ids:
                await _settle_write(asyncio.to_thread(ledger.purge_one, memory_id, message_id))
        if runs is not None:
            for task_id in inventory.task_ids:
                await _settle_write(asyncio.to_thread(runs.purge_task, task_id, message_id))
        extractions = self.stores.extractions(partition)
        if await asyncio.to_thread(extractions.exists):
            await asyncio.to_thread(extractions.audit, validate_sources)
            if not await _settle_write(asyncio.to_thread(extractions.purge, message_id)):
                raise RuntimeError("extraction snapshot cleanup failed")
        if (evidence is not None and await asyncio.to_thread(
                evidence.purge_inventory, message_id, validate_sources)):
            raise RuntimeError("cleanup verification failed")
        if (ledger is not None and await asyncio.to_thread(
                ledger.purge_inventory, message_id, validate_sources)):
            raise RuntimeError("cleanup verification failed")
        if (runs is not None and await asyncio.to_thread(
                runs.purge_inventory, message_id, validate_sources)):
            raise RuntimeError("cleanup verification failed")
        proof = _PurgeProof(self.raw._purge_key(scope, message_id), inventory.revision,
                            inventory.task_ids, pending["results"], gate)
        return await _settle_write(asyncio.to_thread(
            self.raw._finish_purge_verified, scope, message_id, proof=proof))

    async def search(self, scope: Scope, query: str, *, top_k: int = 10,
                     neighbors: int = 0, start_ts: float | None = None,
                     end_ts: float | None = None,
                     memory_scope: str = "auto", deadline: float | None = None,
                     purpose: RecallPurpose = "warm") -> list[dict]:
        return (await self.search_with_status(
            scope, query, top_k=top_k, neighbors=neighbors, start_ts=start_ts,
            end_ts=end_ts, memory_scope=memory_scope, deadline=deadline, purpose=purpose)).items

    async def publish_candidates(self, scope: Scope, query: str, candidates: list[dict], *,
                                  top_k: int, neighbors: int, start_ts, end_ts,
                                  memory_scope: str, deadline: float) -> DailyRecallOutcome:
        self._begin_trace()
        if type(candidates) is not list or len(candidates) > 100:
            raise RawMessageError("invalid publication candidates")
        reasons: list[str] = []
        items = await self._search_impl(scope, query, top_k=top_k, neighbors=neighbors,
            start_ts=start_ts, end_ts=end_ts, memory_scope=memory_scope, deadline=deadline,
            reasons=reasons, candidate_items=candidates)
        status: RecallStatus = ("unavailable" if "raw_unavailable" in reasons else
                                "degraded" if reasons else "ready")
        return DailyRecallOutcome(items, status, tuple(dict.fromkeys(reasons)), self._trace_result(items, reasons))

    async def search_with_status(self, scope: Scope, query: str, *, top_k: int = 10,
                                  neighbors: int = 0, start_ts: float | None = None,
                                  end_ts: float | None = None,
                                  memory_scope: str = "auto",
                                  deadline: float | None = None,
                                  purpose: RecallPurpose = "warm") -> DailyRecallOutcome:
        self._begin_trace()
        if purpose not in {"warm", "interactive"}:
            raise RawMessageError("invalid recall purpose")
        if purpose == "interactive":
            deadline = recall_deadline(purpose=purpose, deadline=deadline)
        _partition(scope)
        if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(top_k) is not int or not 1 <= top_k <= 100
                or type(neighbors) is not int or not 0 <= neighbors <= 2
                or type(memory_scope) is not str
                or memory_scope.strip().lower() not in _SCOPE_DOMAINS
                and memory_scope.strip() != ""
                 or any(value is not None and (
                     type(value) not in {int, float} or not math.isfinite(value)
                     or value < 0) for value in (start_ts, end_ts))
                 or deadline is not None and (
                     type(deadline) not in {int, float} or not math.isfinite(deadline))):
            raise RawMessageError("invalid recall request")
        if deadline is not None and deadline <= time.monotonic():
            return DailyRecallOutcome([], "unavailable", ("deadline_exceeded",))
        try:
            reasons: list[str] = []
            items = await self._search_impl(
                scope, query, top_k=top_k, neighbors=neighbors,
                start_ts=start_ts, end_ts=end_ts, memory_scope=memory_scope,
                reasons=reasons, deadline=deadline,
            )
            status: RecallStatus = ("unavailable" if "raw_unavailable" in reasons else
                      "degraded" if reasons else "ready")
            return DailyRecallOutcome(items, status, tuple(dict.fromkeys(reasons)), self._trace_result(items, reasons))
        except (RawMessageError, ValueError):
            raise
        except Exception as exc:
            logger.warning("daily Raw generation unavailable: %s", type(exc).__name__)
            return DailyRecallOutcome([], "unavailable", ("raw_unavailable",))

    async def _search_impl(self, scope: Scope, query: str, *, top_k: int,
                           neighbors: int, start_ts: float | None,
                           end_ts: float | None, memory_scope: str,
                           reasons: list[str], deadline: float | None = None,
                           candidate_items: list[dict] | None = None) -> list[dict]:
        partition = _partition(scope)
        if (type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(top_k) is not int or not 1 <= top_k <= 100
                or type(neighbors) is not int or not 0 <= neighbors <= 2
                or type(memory_scope) is not str
                or any(value is not None and (
                    type(value) not in {int, float} or not math.isfinite(value)
                    or value < 0) for value in (start_ts, end_ts))):
            raise RawMessageError("invalid recall request")
        selected_scope = memory_scope.strip().lower()
        if selected_scope == "":
            selected_scope = "auto"
        if selected_scope not in _SCOPE_DOMAINS:
            raise RawMessageError("invalid recall request")
        kinds = _SCOPE_DOMAINS[selected_scope]
        if self.query_options is not None:
            kinds = kinds.intersection(self.query_options["enabled"])
        if not kinds:
            return []
        local_deadline = time.monotonic() + self.request_timeout_seconds
        request_deadline = min(deadline, local_deadline) if deadline is not None else local_deadline
        if selected_scope == "recents":
            recent_start = time.time() - 7 * 86400
            start_ts = max(start_ts or 0, recent_start)
        began = time.monotonic()
        phase_times: dict[str, float] = {}
        if self.query_options is not None and self.query_options["full"] and kinds - {"raw"}:
            try:
                _, resolved = await asyncio.to_thread(self.raw.partition_state, scope)
                if not resolved:
                    reasons.append("derivation_pending")
            except Exception:
                reasons.append("derived_unavailable")
        originals = []
        if "raw" in kinds and candidate_items is None:
            try:
                page_size = min(50, top_k * 3)
                page = await asyncio.to_thread(self.raw.search, scope, query,
                                               limit=page_size,
                                               start_ts=start_ts, end_ts=end_ts,
                                               deadline=request_deadline)
                scanned_raw = 0
                while page:
                    scanned_raw += len(page)
                    evidence_mode = getattr(self.raw, "evidence_mode", False) is True
                    current_page = ({} if evidence_mode else await self._source_snapshots(
                        scope, tuple(message.message_id for message in page),
                        request_deadline, {}))
                    for message in page:
                        self._remaining(request_deadline)
                        if evidence_mode:
                            originals.append(message)
                            continue
                        source_snapshot = current_page.get(message.message_id)
                        current = source_snapshot[0] if source_snapshot is not None else None
                        if (current is not None and current.revision == message.revision
                                and current.content == message.content
                                and current.created_at == message.created_at
                                and _integrity_valid(current)):
                            originals.append(message)
                    if len(page) < page_size:
                        break
                    if scanned_raw >= _MAX_RAW_CANDIDATES:
                        last = page[-1]
                        more = await asyncio.to_thread(
                            self.raw.search_page, scope, query, limit=1,
                            start_ts=start_ts, end_ts=end_ts,
                            deadline=request_deadline,
                            before=(last.ordinal, last.session_id, last.message_id),
                        )
                        if more:
                            raise RawMessageError("raw candidate scan budget exceeded")
                        break
                    last = page[-1]
                    page = await asyncio.to_thread(
                        self.raw.search_page, scope, query, limit=page_size,
                        start_ts=start_ts, end_ts=end_ts,
                        deadline=request_deadline,
                        before=(last.ordinal, last.session_id, last.message_id),
                    )
            except Exception as exc:
                logger.warning("daily raw recall unavailable: %s", type(exc).__name__)
                reasons.append("raw_unavailable")
                return []
        results: list[dict] = list(candidate_items) if candidate_items is not None else []
        for message in originals:
            try:
                self._remaining(request_deadline)
                context = (await asyncio.to_thread(self.raw.neighbors, scope,
                                                   message.message_id, before=neighbors,
                                                   after=neighbors,
                                                   deadline=request_deadline)) if neighbors else [message]
            except Exception as exc:
                logger.warning("daily raw context unavailable: %s", type(exc).__name__)
                reasons.append("raw_unavailable")
                return []
            if not context or not any(item.message_id == message.message_id
                                      for item in context):
                continue
            results.append({
                "domain": "raw", "message_id": message.message_id,
                "content": message.content, "source_message_id": message.message_id,
                "source_content": message.content, "source_time": message.created_at,
                "revision": message.revision,
                **_excerpt_metadata(message),
                **_excerpt_metadata(message, prefix="source_"),
                "neighbors": [
                    {"message_id": item.message_id, "content": item.content,
                     "created_at": item.created_at, "role": item.role,
                     "revision": item.revision}
                    | _excerpt_metadata(item)
                    for item in context if item.message_id != message.message_id
                ],
            })
            if self.query_options is not None:
                results[-1].update(source_role=message.role, session_id=message.session_id,
                                   ordinal=message.ordinal)
        async def raw_fallback() -> list[dict]:
            reasons.append("derived_unavailable")
            self._trace_candidates(results, kinds - {"raw"},
                max(0.0, (time.monotonic() - began) * 1000 - phase_times.get("raw", 0.0)))
            return await self._finalize(scope, results, top_k, start_ts, end_ts,
                                        request_deadline, reasons=reasons)

        phase_times["raw"] = (time.monotonic() - began) * 1000
        self._trace_candidates(results, kinds.intersection({"raw"}), phase_times)
        if kinds == frozenset({"raw"}) or (
                candidate_items is not None and not getattr(self.vector_index, "hits", ())):
            self._trace_candidates(results, kinds, (time.monotonic() - began) * 1000)
            return await self._finalize(scope, results, top_k, start_ts, end_ts,
                                        request_deadline, reasons=reasons)

        try:
            path = self.stores.partition(partition)
            evidence_present = _database_present(path / "derived_evidence.db")
            ledger_present = _database_present(path / "source_ledger.db")
            vector_authority = _database_present(path / "vector_index.db")
            vector_tree = (path / "vectors").is_dir()
            snapshot = await asyncio.to_thread(
                self.stores.partition_registry().snapshot, partition,
            )
        except Exception as exc:
            logger.warning("daily derived recall registry unavailable: %s",
                           type(exc).__name__)
            return await raw_fallback()
        if snapshot is not None:
            if (snapshot["state"] not in {"READY", "INITIALIZING"}
                    or snapshot["evidence"] and not evidence_present
                    or snapshot["ledger"] and not ledger_present
                    or snapshot["vector"] and not vector_authority):
                logger.warning("daily derived recall incomplete: partition state")
                return await raw_fallback()
        elif (evidence_present or ledger_present or vector_authority
              or vector_tree or (path / "derivation_runs.db").exists()):
            logger.warning("daily derived recall incomplete: unregistered partition")
            return await raw_fallback()
        if vector_authority != vector_tree:
            logger.warning("daily derived recall incomplete: vector files unpaired")
            return await raw_fallback()
        if evidence_present != ledger_present and (
                snapshot is None or snapshot["ledger"]):
            logger.warning("daily derived recall incomplete: missing paired store")
            return await raw_fallback()
        if not evidence_present:
            if (await asyncio.to_thread(self.raw.has_derived_results, scope)
                    or snapshot is not None):
                logger.warning("daily derived recall incomplete: missing stores")
                return await raw_fallback()
            return await self._finalize(scope, results, top_k, start_ts, end_ts,
                                        request_deadline, reasons=reasons)
        try:
            evidence = self.stores.evidence(partition)
            ledger = self.stores.ledger(partition)
            deadline = min(request_deadline,
                           time.monotonic() + _CANDIDATE_SCAN_SECONDS)
            derived_results: list[dict] = []
            vector_ids: tuple[str, ...] = ()
            vector_scores: dict[str, float] = {}
            if ("vector" in kinds and self.vector_index is not None
                    and vector_authority and vector_tree):
                cosine_hits = getattr(self.vector_index, "search_cosine_hits", None)
                search_hits = getattr(self.vector_index, "search_hits", None)
                if callable(cosine_hits):
                    hits = await asyncio.wait_for(
                        cosine_hits(partition, query, limit=100),
                        timeout=self._remaining(deadline),
                    )
                    if type(hits) is not tuple or len(hits) > 100:
                        raise ValueError("invalid vector candidate count")
                    scored_ids = []
                    for key, similarity in hits:
                        if (type(key) is not str or type(similarity) not in {int, float}
                                or not math.isfinite(similarity)
                                or not -1 <= similarity <= 1):
                            raise ValueError("invalid vector candidate score")
                        scored_ids.append(key)
                        vector_scores.setdefault(key, (1 + similarity) / 2)
                    vector_ids = tuple(scored_ids)
                elif callable(search_hits):
                    hits = await asyncio.wait_for(
                        search_hits(partition, query, limit=100),
                        timeout=self._remaining(deadline),
                    )
                    if type(hits) is not tuple or len(hits) > 100:
                        raise ValueError("invalid vector candidate count")
                    scored_ids = []
                    for key, distance in hits:
                        if (type(key) is not str or type(distance) not in {int, float}
                                or not math.isfinite(distance) or distance < 0):
                            raise ValueError("invalid vector candidate score")
                        scored_ids.append(key)
                    vector_ids = tuple(scored_ids)
                else:
                    vector_ids = await asyncio.wait_for(
                        self.vector_index.search_ids(partition, query, limit=100),
                        timeout=self._remaining(deadline),
                    )
                    if type(vector_ids) is not tuple or len(vector_ids) > 100:
                        raise ValueError("invalid vector candidate count")
            by_memory_id: dict[str, dict] = {}

            async def consider(memory_id: str, *, lexical: bool,
                               records: dict[str, dict],
                               ledger_items: dict[str, dict],
                               snapshots: dict[str, tuple[StoredMessage,
                                                          tuple[dict, ...]]]) -> None:
                if time.monotonic() > deadline:
                    raise RawMessageError("derived candidate scan budget exceeded")
                score = max(0.5 if lexical else 0.0,
                            vector_scores.get(memory_id, 0.0))
                existing = by_memory_id.get(memory_id)
                if existing is not None:
                    existing["_rank_score"] = max(existing["_rank_score"], score)
                    return
                record = records.get(memory_id)
                if record is None or record["domain"] not in kinds:
                    return
                source = SourceRevision(**record["source"])
                ledger_item = ledger_items.get(memory_id)
                if (ledger_item is None or not ledger_item["has_item"]
                        or ledger_item["sources"] != (source,)):
                    return
                source_snapshot = snapshots.get(source.raw_message_id)
                if source_snapshot is None:
                    return
                message = source_snapshot[0]
                if message.revision != source.revision:
                    return
                stamp = datetime.fromisoformat(message.created_at).timestamp()
                if ((start_ts is not None and stamp < start_ts)
                        or (end_ts is not None and stamp >= end_ts)):
                    return
                anchors = source_snapshot[1]
                if not any(anchor["memory_id"] == memory_id
                           and anchor["store_ref"] == record["store_ref"]
                           and anchor["target_domain"] == record["domain"]
                           and anchor["task_id"] == source.task_id
                           and anchor["revision"] == source.revision for anchor in anchors):
                    return
                payload = record["payload"]
                content = " ".join(str(value) for value in payload.values() if value is not None)
                candidate = {
                    "domain": record["domain"], "memory_id": memory_id,
                    "content": content, "source_message_id": message.message_id,
                    "source_content": message.content, "source_time": message.created_at,
                    "revision": message.revision, "store_ref": record["store_ref"],
                    "source": asdict(source),
                    "_rank_score": score,
                    **_excerpt_metadata(message, prefix="source_"),
                }
                if self.query_options is not None:
                    candidate.update(source_role=message.role, session_id=message.session_id,
                                     ordinal=message.ordinal)
                if self.query_options is not None and self.query_options["full"]:
                    candidate["payload"] = payload
                    terms = memory_query_terms(query)
                    matches = sum(term in content.lower() for term in terms)
                    candidate["_rank_score"] = max(score, .5 + matches / (2 * max(1, len(terms))))
                    candidate["neighbors"] = []
                    if neighbors:
                        context = await asyncio.to_thread(self.raw.neighbors, scope, message.message_id,
                                                          before=neighbors, after=neighbors, deadline=deadline)
                        candidate["neighbors"] = [{"message_id": row.message_id, "content": row.content,
                            "created_at": row.created_at, "role": row.role, "revision": row.revision,
                            **_excerpt_metadata(row)} for row in context if row.message_id != message.message_id]
                nearby = candidate.pop("neighbors", None)
                checked = await self._visible_candidate(scope, candidate,
                                                        start_ts, end_ts, deadline,
                                                        snapshots=snapshots)
                if checked is not None:
                    if nearby is not None:
                        checked["neighbors"] = nearby
                    derived_results.append(checked)
                    by_memory_id[memory_id] = checked

            if vector_ids:
                vector_records = await asyncio.to_thread(evidence.get_many, vector_ids)
                vector_ledger = await asyncio.to_thread(ledger.inspect_ids, vector_ids)
                vector_snapshots = await self._source_snapshots(
                    scope, tuple(vector_records[memory_id]["source"]["raw_message_id"]
                                 for memory_id in vector_ids if memory_id in vector_records),
                    deadline, self._record_proofs(vector_records),
                )
                for memory_id in vector_ids:
                    await consider(memory_id, lexical=False,
                                   records=vector_records, ledger_items=vector_ledger,
                                   snapshots=vector_snapshots)

            after = None
            scanned = 0
            if candidate_items is None and self.query_options is not None and self.query_options["full"]:
                page = await asyncio.to_thread(evidence.domain_ids, query,
                    domains=tuple(domain for domain in MEMORY_DOMAINS if domain != "raw" and domain in kinds),
                    limit=100, deadline=deadline, event_start_ts=self.query_options["event_start_ts"],
                    event_end_ts=self.query_options["event_end_ts"], timings=phase_times)
                if any(value is True for key, value in phase_times.items() if key.endswith("_limited")):
                    reasons.append("scan_budget_limited")
                if page:
                    records = await asyncio.to_thread(evidence.get_many, page)
                    inspected = await asyncio.to_thread(ledger.inspect_ids, page)
                    snapshots = await self._source_snapshots(scope, tuple(
                        record["source"]["raw_message_id"] for record in records.values()),
                        deadline, self._record_proofs(records))
                    for memory_id in page:
                        await consider(memory_id, lexical=True, records=records,
                                       ledger_items=inspected, snapshots=snapshots)
            while candidate_items is None:
                if self.query_options is not None and self.query_options["full"]:
                    break
                if time.monotonic() > deadline:
                    raise RawMessageError("derived candidate scan budget exceeded")
                remaining = _MAX_LEXICAL_CANDIDATES - scanned
                if remaining <= 0:
                    if await asyncio.to_thread(evidence.search_ids_page, query,
                                               limit=1, after=after, deadline=deadline):
                        raise RawMessageError("lexical candidate scan budget exceeded")
                    break
                page_size = min(100, remaining)
                page = await asyncio.to_thread(evidence.search_ids_page, query,
                                               limit=page_size, after=after,
                                               deadline=deadline)
                if not page:
                    break
                scanned += len(page)
                page_records = await asyncio.to_thread(evidence.get_many, page)
                page_ledger = await asyncio.to_thread(ledger.inspect_ids, page)
                page_snapshots = await self._source_snapshots(
                    scope, tuple(page_records[memory_id]["source"]["raw_message_id"]
                                 for memory_id in page if memory_id in page_records),
                    deadline, self._record_proofs(page_records),
                )
                for memory_id in page:
                    await consider(memory_id, lexical=True,
                                   records=page_records, ledger_items=page_ledger,
                                   snapshots=page_snapshots)
                if len(page) < page_size:
                    break
                after = page[-1]
        except RawMessageError as exc:
            logger.warning("daily derived recall incomplete: %s", type(exc).__name__)
            return await raw_fallback()
        except Exception as exc:
            logger.warning("daily derived recall unavailable: %s", type(exc).__name__)
            return await raw_fallback()
        self._trace_candidates([*derived_results, *results], kinds, phase_times if self.query_options
                               else (time.monotonic() - began) * 1000)
        return await self._finalize(
            scope, self._merge_candidates(derived_results, results), top_k,
            start_ts, end_ts, deadline, reasons=reasons,
        )
