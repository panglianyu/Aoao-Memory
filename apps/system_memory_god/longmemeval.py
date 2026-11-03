# -*- coding: utf-8 -*-
"""LongMemEval V1 公开基准适配层。

职责边界:
- 读取并严格校验官方 cleaned/oracle 数据；
- 把单题 haystack 隔离写入阿露真实 VectorStore；
- 按官方证据标签计算检索指标；
- 产出官方生成/裁判脚本可消费的 JSONL。

本模块不读取生产记忆目录，也不默认调用在线模型。调用方必须为每道题提供
独立索引，避免不同题目的合成用户历史互相污染。
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence


QUESTION_TYPES = frozenset({
    "single-session-user",
    "single-session-assistant",
    "single-session-preference",
    "temporal-reasoning",
    "knowledge-update",
    "multi-session",
})
DEFAULT_KS = (1, 3, 5, 10, 30, 50)
OFFICIAL_FIELDS = (
    "question_id",
    "question_type",
    "question",
    "answer",
    "question_date",
    "haystack_session_ids",
    "haystack_dates",
    "haystack_sessions",
    "answer_session_ids",
)
BENCHMARK_CHAT = "LongMemEval"


class LongMemEvalFormatError(ValueError):
    """官方数据不满足预期契约。"""


@dataclass(frozen=True)
class CorpusItem:
    """单个可检索记忆单元。"""

    corpus_id: str
    text: str
    timestamp: str
    session_id: str
    has_answer: bool = False
    source_ref: str = ""


def parse_longmemeval_timestamp(value: str) -> float:
    """解析官方 ``YYYY/MM/DD (Day) HH:MM``，不依赖系统 weekday locale。"""
    parts = value.split()
    if len(parts) != 3 or not parts[1].startswith("(") or not parts[1].endswith(")"):
        raise ValueError(f"LongMemEval 时间格式非法: {value!r}")
    parsed = datetime.strptime(f"{parts[0]} {parts[2]}", "%Y/%m/%d %H:%M")
    return parsed.replace(tzinfo=timezone.utc).timestamp()


def _load_json_or_jsonl(raw: bytes, path: str) -> list:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            payload = [
                json.loads(line)
                for line in raw.decode("utf-8").splitlines()
                if line.strip()
            ]
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LongMemEvalFormatError(f"无法解析 LongMemEval 数据: {path}") from exc
    if not isinstance(payload, list):
        raise LongMemEvalFormatError("LongMemEval 顶层必须是 JSON 数组或 JSONL")
    return payload


def _require_text(entry: dict, field: str, index: int) -> None:
    if not isinstance(entry.get(field), str) or not entry[field].strip():
        raise LongMemEvalFormatError(
            f"第 {index} 条的 {field} 必须是非空字符串"
        )


def _require_answer(entry: dict, index: int) -> None:
    """cleaned 数据答案为字符串或数值；多会话计数题会直接使用整数。"""
    answer = entry.get("answer")
    if isinstance(answer, str):
        valid = bool(answer.strip())
    elif isinstance(answer, (int, float)) and not isinstance(answer, bool):
        valid = True
    else:
        valid = False
    if not valid:
        raise LongMemEvalFormatError(
            f"第 {index} 条的 answer 必须是非空字符串或数值"
        )


def validate_longmemeval_entry(entry: object, index: int = 0) -> None:
    """校验一条官方实例；允许 cleaned 版本增加额外字段。"""
    if not isinstance(entry, dict):
        raise LongMemEvalFormatError(f"第 {index} 条必须是对象")
    missing = [field for field in OFFICIAL_FIELDS if field not in entry]
    if missing:
        raise LongMemEvalFormatError(
            f"第 {index} 条缺少字段: {', '.join(missing)}"
        )
    for field in ("question_id", "question", "question_date"):
        _require_text(entry, field, index)
    _require_answer(entry, index)
    try:
        parse_longmemeval_timestamp(entry["question_date"])
    except ValueError as exc:
        raise LongMemEvalFormatError(
            f"第 {index} 条 question_date 格式非法"
        ) from exc
    qtype = entry.get("question_type")
    if qtype not in QUESTION_TYPES:
        raise LongMemEvalFormatError(
            f"第 {index} 条 question_type 非法: {qtype!r}"
        )

    session_ids = entry.get("haystack_session_ids")
    dates = entry.get("haystack_dates")
    sessions = entry.get("haystack_sessions")
    if not all(isinstance(value, list) for value in (session_ids, dates, sessions)):
        raise LongMemEvalFormatError(
            f"第 {index} 条 haystack 三字段必须都是数组"
        )
    if not (len(session_ids) == len(dates) == len(sessions)):
        raise LongMemEvalFormatError(
            f"第 {index} 条 haystack id/date/session 数量不一致"
        )
    # cleaned S 中有少量同一 filler 会话在不同日期重复出现。官方检索脚本
    # 允许 corpus_id 重复；这里只允许内容完全相同的重复，避免同 ID 异内容
    # 使官方 generation 阶段的 session 映射产生歧义。
    session_payloads: dict[str, str] = {}
    for session_id, turns in zip(session_ids, sessions):
        payload = json.dumps(turns, ensure_ascii=False, sort_keys=True)
        previous = session_payloads.setdefault(session_id, payload)
        if previous != payload:
            raise LongMemEvalFormatError(
                f"第 {index} 条 session id {session_id!r} 对应不同内容"
            )

    for session_pos, (session_id, date, turns) in enumerate(
        zip(session_ids, dates, sessions)
    ):
        if not isinstance(session_id, str) or not session_id:
            raise LongMemEvalFormatError(
                f"第 {index} 条第 {session_pos} 个 session id 非法"
            )
        if not isinstance(date, str) or not date:
            raise LongMemEvalFormatError(
                f"第 {index} 条第 {session_pos} 个 session date 非法"
            )
        try:
            parse_longmemeval_timestamp(date)
        except ValueError as exc:
            raise LongMemEvalFormatError(
                f"第 {index} 条第 {session_pos} 个 session date 格式非法"
            ) from exc
        if not isinstance(turns, list):
            raise LongMemEvalFormatError(
                f"第 {index} 条第 {session_pos} 个 session 必须是数组"
            )
        for turn_pos, turn in enumerate(turns):
            if not isinstance(turn, dict):
                raise LongMemEvalFormatError(
                    f"第 {index} 条 session {session_pos} turn {turn_pos} 必须是对象"
                )
            if turn.get("role") not in {"user", "assistant"}:
                raise LongMemEvalFormatError(
                    f"第 {index} 条 session {session_pos} turn {turn_pos} role 非法"
                )
            if not isinstance(turn.get("content"), str):
                raise LongMemEvalFormatError(
                    f"第 {index} 条 session {session_pos} turn {turn_pos} content 非法"
                )
            if "has_answer" in turn and not isinstance(turn["has_answer"], bool):
                raise LongMemEvalFormatError(
                    f"第 {index} 条 session {session_pos} turn {turn_pos} "
                    "has_answer 必须是布尔值"
                )

    answer_session_ids = entry.get("answer_session_ids")
    if not isinstance(answer_session_ids, list) or not all(
        isinstance(value, str) for value in answer_session_ids
    ):
        raise LongMemEvalFormatError(
            f"第 {index} 条 answer_session_ids 必须是字符串数组"
        )
    unknown = set(answer_session_ids) - set(session_ids)
    if unknown:
        raise LongMemEvalFormatError(
            f"第 {index} 条 answer_session_ids 不在 haystack 中: {sorted(unknown)}"
        )


def load_longmemeval(path: str, limit: int | None = None) -> tuple[list[dict], str]:
    """加载官方 JSON/JSONL，返回校验后的实例和原文件 SHA-256。"""
    if limit is not None and limit <= 0:
        raise ValueError("limit 必须大于 0")
    with open(path, "rb") as handle:
        raw = handle.read()
    entries = _load_json_or_jsonl(raw, path)
    seen_question_ids: set[str] = set()
    for index, entry in enumerate(entries):
        validate_longmemeval_entry(entry, index=index)
        question_id = entry["question_id"]
        if question_id in seen_question_ids:
            raise LongMemEvalFormatError(f"重复 question_id: {question_id}")
        seen_question_ids.add(question_id)
    selected = entries if limit is None else entries[:limit]
    return copy.deepcopy(selected), hashlib.sha256(raw).hexdigest()


def build_corpus(entry: dict, granularity: str = "turn") -> list[CorpusItem]:
    """按官方 baseline 语义构造检索语料：只索引 user 侧内容。"""
    if granularity not in {"session", "turn"}:
        raise ValueError("granularity 只能是 session 或 turn")
    corpus: list[CorpusItem] = []
    session_counts: dict[str, int] = {}
    for session_id in entry["haystack_session_ids"]:
        session_counts[session_id] = session_counts.get(session_id, 0) + 1
    occurrence: dict[str, int] = {}
    answer_session_ids = set(entry["answer_session_ids"])
    for session_id, timestamp, turns in zip(
        entry["haystack_session_ids"],
        entry["haystack_dates"],
        entry["haystack_sessions"],
    ):
        occurrence[session_id] = occurrence.get(session_id, 0) + 1
        source_suffix = (
            f"#occ{occurrence[session_id]}"
            if session_counts[session_id] > 1
            else ""
        )
        if granularity == "session":
            user_turns = [turn for turn in turns if turn["role"] == "user"]
            corpus.append(CorpusItem(
                corpus_id=session_id,
                text=" ".join(turn["content"] for turn in user_turns),
                timestamp=timestamp,
                session_id=session_id,
                has_answer=session_id in answer_session_ids,
                source_ref=f"{session_id}{source_suffix}",
            ))
            continue
        for turn_index, turn in enumerate(turns, start=1):
            if turn["role"] != "user":
                continue
            corpus.append(CorpusItem(
                corpus_id=f"{session_id}_{turn_index}",
                text=turn["content"],
                timestamp=timestamp,
                session_id=session_id,
                has_answer=bool(turn.get("has_answer", False)),
                source_ref=f"{session_id}_{turn_index}{source_suffix}",
            ))
    return corpus


def _dcg(relevances: Sequence[int], k: int) -> float:
    values = list(relevances[:k])
    total = 0.0
    for index, relevance in enumerate(values):
        if index == 0:
            total += float(relevance)
        else:
            total += float(relevance) / math.log2(index + 1)
    return total


def evaluate_ranking(
    ranked_ids: Sequence[str],
    correct_ids: Iterable[str],
    corpus_ids: Sequence[str],
    ks: Sequence[int] = DEFAULT_KS,
) -> dict[str, float]:
    """复刻官方 evaluate_retrieval 的 recall_any/all 与 nDCG。"""
    correct = set(correct_ids)
    corpus_relevance = [1 if corpus_id in correct else 0 for corpus_id in corpus_ids]
    ideal = sorted(corpus_relevance, reverse=True)
    metrics: dict[str, float] = {}
    for k in ks:
        recalled = set(ranked_ids[:k])
        recall_any = float(any(doc_id in recalled for doc_id in correct))
        recall_all = float(all(doc_id in recalled for doc_id in correct))
        ranked_relevance = [1 if doc_id in correct else 0 for doc_id in ranked_ids[:k]]
        ideal_dcg = _dcg(ideal, k)
        ndcg = _dcg(ranked_relevance, k) / ideal_dcg if ideal_dcg else 0.0
        metrics[f"recall_any@{k}"] = recall_any
        metrics[f"recall_all@{k}"] = recall_all
        metrics[f"ndcg_any@{k}"] = ndcg
    return metrics


def evaluate_turn_ranking_as_sessions(
    ranked_turn_ids: Sequence[str],
    corpus: Sequence[CorpusItem],
    correct_session_ids: Iterable[str],
    ks: Sequence[int] = DEFAULT_KS,
) -> dict[str, float]:
    """复刻官方 turn2session：为取得 k 个不同会话自动扩展 turn 截断。"""
    corpus_by_id = {item.corpus_id: item for item in corpus}
    ranked_sessions = [
        corpus_by_id[corpus_id].session_id
        for corpus_id in ranked_turn_ids
        if corpus_id in corpus_by_id
    ]
    corpus_sessions = [item.session_id for item in corpus]
    metrics: dict[str, float] = {}
    for k in ks:
        effective_k = min(k, len(ranked_sessions))
        while (
            effective_k < len(ranked_sessions)
            and len(set(ranked_sessions[:effective_k])) < k
        ):
            effective_k += 1
        current = evaluate_ranking(
            ranked_sessions[:effective_k],
            correct_session_ids,
            corpus_sessions,
            ks=(effective_k,),
        )
        for name in ("recall_any", "recall_all", "ndcg_any"):
            metrics[f"{name}@{k}"] = current[f"{name}@{effective_k}"]
    return metrics


class LongMemEvalVectorIndex:
    """把 LongMemEval 语料接到阿露 VectorStore 的薄适配器。"""

    def __init__(self, vector_store, close_callback: Callable[[], None] | None = None):
        self._vector_store = vector_store
        self._close_callback = close_callback
        self._items: dict[str, CorpusItem] = {}
        self.indexed = 0
        self.skipped = 0
        self.retrieval_mode = "ann"

    def add(self, corpus: Sequence[CorpusItem]) -> None:
        for item in corpus:
            source_ref = item.source_ref or item.corpus_id
            digest = hashlib.sha256(source_ref.encode("utf-8")).hexdigest()
            added = self._vector_store.add_memory(
                item.text,
                prefix="obs",
                doc_id=f"lme_{digest}",
                source_ref=source_ref,
                created_at=parse_longmemeval_timestamp(item.timestamp),
                is_master=True,
                chat_name=BENCHMARK_CHAT,
                visibility="private",
            )
            if added:
                self._items[source_ref] = item
                self.indexed += 1
            else:
                self.skipped += 1

    def retrieve(self, query: str, top_k: int) -> list[CorpusItem]:
        query_kwargs = dict(
            n_results=top_k,
            prefer_chat=BENCHMARK_CHAT,
            asker_is_master=True,
            commit_hits=False,
        )
        exact_query = getattr(
            self._vector_store, "query_memory_exact_with_meta", None,
        )
        if callable(exact_query) and top_k >= len(self._items):
            # HNSW 是近似 Top-K 索引，不保证能返回图中的全部节点。官方
            # turn2session 需要完整 turn 排名时改走精确距离，避免 k==全库触发
            # hnswlib 固定二维数组错误；普通 Top-K 仍沿用生产 ANN 路径。
            self.retrieval_mode = "exact_full_rank"
            pairs = exact_query(query, **query_kwargs)
        else:
            self.retrieval_mode = "ann"
            pairs = self._vector_store.query_memory_with_meta(
                query,
                **query_kwargs,
            )
        ranked: list[CorpusItem] = []
        seen: set[str] = set()
        for pair in pairs:
            if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                continue
            metadata = pair[1] if isinstance(pair[1], dict) else {}
            corpus_id = str(metadata.get("source_ref", "") or "")
            if corpus_id in seen or corpus_id not in self._items:
                continue
            seen.add(corpus_id)
            ranked.append(self._items[corpus_id])
        return ranked

    def close(self) -> None:
        if self._close_callback is not None:
            self._close_callback()


def _average_metrics(rows: Sequence[dict[str, float]]) -> dict[str, float]:
    if not rows:
        return {}
    keys = sorted(set.intersection(*(set(row) for row in rows)))
    return {key: sum(row[key] for row in rows) / len(rows) for key in keys}


def _entry_result(entry: dict) -> dict:
    return {field: copy.deepcopy(entry[field]) for field in OFFICIAL_FIELDS}


def run_longmemeval_retrieval(
    entries: Sequence[dict],
    make_index: Callable[[dict], object],
    *,
    dataset_sha256: str,
    context_label: str,
    granularity: str = "turn",
    top_k: int = 50,
    ks: Sequence[int] = DEFAULT_KS,
    progress_fn: Callable[[int, int, str], None] | None = None,
    result_fn: Callable[[int, dict], None] | None = None,
    resume_results: Sequence[dict] = (),
    ranking_mode: str = "full",
    source_sha256: str | None = None,
) -> tuple[dict, list[dict]]:
    """逐题隔离索引并评测，返回摘要报告与官方兼容检索记录。"""
    if granularity not in {"session", "turn"}:
        raise ValueError("granularity 只能是 session 或 turn")
    if top_k <= 0:
        raise ValueError("top_k 必须大于 0")
    if ranking_mode not in {"full", "online"}:
        raise ValueError("invalid ranking mode")
    if ranking_mode == "online" and (
            granularity != "turn" or top_k > 100 or type(source_sha256) is not str
            or len(source_sha256) != 64 or any(char not in "0123456789abcdef" for char in source_sha256)):
        raise ValueError("online turn ranking requires a frozen source SHA-256 and top_k <= 100")
    selected_ks = tuple(k for k in ks if 0 < k <= top_k)
    if not selected_ks:
        raise ValueError("至少需要一个不大于 top_k 的 k")

    started = time.time()
    results: list[dict] = []
    aggregate_session: list[dict[str, float]] = []
    aggregate_turn: list[dict[str, float]] = []
    by_type: dict[str, dict[str, list[dict[str, float]]]] = {}
    skipped_abstention: list[str] = []
    skipped_no_target: list[str] = []
    indexed_items = 0
    dedup_skipped_items = 0
    resumed_entries = 0
    retrieval_modes: dict[str, int] = {}

    entry_ids = {entry["question_id"] for entry in entries}
    resumed_by_id: dict[str, dict] = {}
    for result in resume_results:
        question_id = result.get("question_id") if isinstance(result, dict) else None
        if question_id not in entry_ids:
            raise ValueError(f"断点包含未知 question_id: {question_id!r}")
        if question_id in resumed_by_id:
            raise ValueError(f"断点包含重复 question_id: {question_id}")
        retrieval = result.get("retrieval_results")
        if not isinstance(retrieval, dict) or not isinstance(
            retrieval.get("metrics"), dict,
        ):
            raise ValueError(f"断点结果结构非法: {question_id}")
        if ranking_mode == "online" and (
                retrieval.get("ranking_mode") != ranking_mode
                or retrieval.get("source_sha256") != source_sha256
                or retrieval.get("candidate_limit") != 100
                or retrieval.get("query_char_limit") != 200
                or "session" in retrieval["metrics"]):
            raise ValueError("online checkpoint profile or source differs")
        resumed_by_id[question_id] = copy.deepcopy(result)

    def _accumulate(
        entry: dict,
        result: dict,
        *,
        has_target: bool,
    ) -> None:
        nonlocal indexed_items, dedup_skipped_items
        retrieval = result["retrieval_results"]
        metrics = retrieval["metrics"]
        session_metrics = metrics.get("session", {})
        turn_metrics = metrics.get("turn", {})
        indexed_items += int(retrieval.get("indexed_items", 0))
        dedup_skipped_items += int(retrieval.get("dedup_skipped_items", 0))
        mode = str(retrieval.get("retrieval_mode", "unknown") or "unknown")
        retrieval_modes[mode] = retrieval_modes.get(mode, 0) + 1
        results.append(copy.deepcopy(result))

        question_id = entry["question_id"]
        if question_id.endswith("_abs"):
            skipped_abstention.append(question_id)
            return
        if not has_target:
            skipped_no_target.append(question_id)
            return
        aggregate_session.append(session_metrics)
        if turn_metrics:
            aggregate_turn.append(turn_metrics)
        bucket = by_type.setdefault(entry["question_type"], {
            "session": [], "turn": [],
        })
        bucket["session"].append(session_metrics)
        if turn_metrics:
            bucket["turn"].append(turn_metrics)

    for entry_position, entry in enumerate(entries, start=1):
        corpus = build_corpus(entry, granularity=granularity)
        correct_turn_ids = {item.corpus_id for item in corpus if item.has_answer}
        correct_session_ids = set(entry["answer_session_ids"])
        has_target = bool(
            correct_turn_ids if granularity == "turn" else correct_session_ids
        )
        question_id = entry["question_id"]
        resumed = resumed_by_id.get(question_id)
        if resumed is not None:
            resumed_entries += 1
            _accumulate(entry, resumed, has_target=has_target)
            if progress_fn is not None:
                progress_fn(entry_position, len(entries), question_id)
            continue

        index = make_index({"question_id": question_id} if ranking_mode == "online" else entry)
        try:
            index.add([replace(item, has_answer=False) for item in corpus]
                      if ranking_mode == "online" else corpus)
            # 官方 turn2session 指标会越过前 k 个 turn，直到获得 k 个不同
            # session 或耗尽排名。因此 turn 粒度必须保留完整排名；session
            # 粒度只需保留最大指标截断 top_k。
            retrieval_count = 100 if ranking_mode == "online" else (
                len(corpus) if granularity == "turn" else top_k)
            query = entry["question"][:200] if ranking_mode == "online" else entry["question"]
            query_started = time.monotonic()
            ranked_items = index.retrieve(
                query, top_k=retrieval_count,
            )
            query_ms = (time.monotonic() - query_started) * 1000
            case_indexed = int(getattr(index, "indexed", len(corpus)))
            case_skipped = int(getattr(index, "skipped", 0))
            retrieval_mode = str(getattr(index, "retrieval_mode", "unknown"))
        finally:
            close = getattr(index, "close", None)
            if callable(close):
                close()

        ranked_ids = [item.corpus_id for item in ranked_items]
        if granularity == "turn":
            turn_metrics = evaluate_ranking(
                ranked_ids, correct_turn_ids,
                [item.corpus_id for item in corpus], selected_ks,
            )
            session_metrics = {} if ranking_mode == "online" else evaluate_turn_ranking_as_sessions(
                ranked_ids, corpus, correct_session_ids, selected_ks)
        else:
            turn_metrics = {}
            session_metrics = evaluate_ranking(
                ranked_ids, correct_session_ids,
                [item.corpus_id for item in corpus], selected_ks,
            )

        result = _entry_result(entry)
        result["retrieval_results"] = {
            "query": entry["question"],
            "ranked_items": [
                {
                    "corpus_id": item.corpus_id,
                    "text": item.text,
                    "timestamp": item.timestamp,
                }
                for item in ranked_items
            ],
            "metrics": {
                "session": session_metrics,
                "turn": turn_metrics,
            },
            "indexed_items": case_indexed,
            "dedup_skipped_items": case_skipped,
            "retrieval_mode": retrieval_mode,
        }
        if ranking_mode == "online":
            retrieval = result["retrieval_results"]
            retrieval["metrics"].pop("session")
            retrieval.update(ranking_mode=ranking_mode, source_sha256=source_sha256,
                             candidate_limit=100, query_char_limit=200, query_ms=query_ms,
                             query_truncated=len(entry["question"]) > 200)
            outcome = getattr(index, "last_outcome", None)
            if type(outcome) is dict:
                retrieval["outcome"] = dict(outcome)
        if result_fn is not None:
            result_fn(entry_position, copy.deepcopy(result))
        _accumulate(entry, result, has_target=has_target)
        if progress_fn is not None:
            progress_fn(entry_position, len(entries), question_id)

    report: dict = {
        "benchmark": "LongMemEval-V1-cleaned",
        "generated_at": time.time(),
        "dataset_sha256": dataset_sha256,
        "context": context_label,
        "granularity": granularity,
        "top_k": top_k,
        "ks": list(selected_ks),
        "total_entries": len(entries),
        "evaluated_entries": len(aggregate_session),
        "skipped_abstention": skipped_abstention,
        "skipped_no_target": skipped_no_target,
        "indexed_items": indexed_items,
        "dedup_skipped_items": dedup_skipped_items,
        "resumed_entries": resumed_entries,
        "retrieval_modes": retrieval_modes,
        "metrics": {
            "session": _average_metrics(aggregate_session),
            "turn": _average_metrics(aggregate_turn),
        },
        "by_question_type": {
            question_type: {
                "count": len(metrics["session"]),
                "session": _average_metrics(metrics["session"]),
                "turn": _average_metrics(metrics["turn"]),
            }
            for question_type, metrics in sorted(by_type.items())
        },
        "elapsed_seconds": time.time() - started,
    }
    if ranking_mode == "online":
        latencies = sorted(row["retrieval_results"]["query_ms"] for row in results)
        statuses: dict[str, int] = {}
        reasons: dict[str, int] = {}
        proof_failures = 0
        for row in results:
            outcome = row["retrieval_results"].get("outcome", {})
            status = outcome.get("status", "unobserved")
            statuses[status] = statuses.get(status, 0) + 1
            for reason in outcome.get("reason_codes", []):
                reasons[reason] = reasons.get(reason, 0) + 1
            proof_failures += outcome.get("source_proof_failures", 0)
        report["query_summary"] = {
            "queries": len(latencies),
            "truncated_queries": sum(len(entry["question"]) > 200 for entry in entries),
            "mean_ms": sum(latencies) / len(latencies) if latencies else 0,
            "p95_ms": latencies[max(0, math.ceil(len(latencies) * .95) - 1)] if latencies else 0,
            "max_ms": max(latencies, default=0), "statuses": statuses, "reason_counts": reasons,
            "source_proof_failures": proof_failures,
            "latency_policy": "daily: interactive 2s; legacy: synchronous query, measured without 2s cancellation",
        }
        report.update(ranking_mode=ranking_mode, source_sha256=source_sha256,
                      candidate_limit=100, query_char_limit=200,
                      session_metrics_omitted="online rank is truncated; not official full turn2session",
                      evaluation_scope="retrieval only; no six-domain LLM extraction or answer judge")
        report["metrics"].pop("session")
        for bucket in report["by_question_type"].values():
            bucket.pop("session")
    return report, results


def build_answer_prompt(result: dict, top_k: int = 10) -> str:
    """把检索记录转换为官方推荐的带日期结构化问答提示。"""
    ranked = list(result.get("retrieval_results", {}).get("ranked_items", []))[:top_k]
    ranked.sort(key=lambda item: str(item.get("timestamp", "")))
    history_parts = []
    for index, item in enumerate(ranked, start=1):
        history_parts.append(
            f"### Session {index}:\n"
            f"Session Date: {item.get('timestamp', '')}\n"
            f"Session Content:\n{item.get('text', '')}"
        )
    history = "\n\n".join(history_parts) or "No relevant history was retrieved."
    return (
        "I will give you relevant history chats between you and a user. "
        "Answer the question using only the history. First extract the relevant "
        "information, then reason to the answer. If the history does not contain "
        "enough evidence, explicitly say the question cannot be answered.\n\n"
        f"History Chats:\n{history}\n\n"
        f"Current Date: {result['question_date']}\n"
        f"Question: {result['question']}\n"
        "Answer:"
    )


def generate_hypotheses(
    results: Sequence[dict],
    answer_fn: Callable[[str], str | None],
    *,
    top_k: int = 10,
) -> tuple[list[dict], list[str]]:
    """生成官方 evaluate_qa.py 可消费的 question_id/hypothesis JSONL 数据。"""
    hypotheses = []
    failed = []
    for result in results:
        question_id = result["question_id"]
        try:
            answer = answer_fn(build_answer_prompt(result, top_k=top_k))
        except Exception:
            answer = None
        if not isinstance(answer, str) or not answer.strip():
            failed.append(question_id)
            answer = "I cannot answer from the retrieved history."
        hypotheses.append({
            "question_id": question_id,
            "hypothesis": answer.strip(),
        })
    return hypotheses, failed


def _atomic_write(path: str, text: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".longmemeval_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


class LongMemEvalCheckpoint:
    """逐题原子断点；配置不同的运行绝不互相复用。"""

    def __init__(
        self,
        data_dir: str,
        *,
        dataset_sha256: str,
        context_label: str,
        granularity: str,
        top_k: int,
        total_entries: int,
        ranking_mode: str = "full",
        source_sha256: str | None = None,
    ) -> None:
        config = {
            "checkpoint_schema": 1,
            "retrieval_strategy": "exact-full-rank-v1",
            "dataset_sha256": dataset_sha256,
            "context": context_label,
            "granularity": granularity,
            "top_k": int(top_k),
            "total_entries": int(total_entries),
        }
        if ranking_mode not in {"full", "online"}:
            raise ValueError("invalid checkpoint ranking mode")
        if ranking_mode == "online":
            if (granularity != "turn" or not 1 <= top_k <= 100 or type(source_sha256) is not str
                    or len(source_sha256) != 64 or any(char not in "0123456789abcdef" for char in source_sha256)):
                raise ValueError("online checkpoint requires frozen source SHA-256")
            config.update(checkpoint_schema=2, retrieval_strategy="online-top100-v1",
                          source_sha256=source_sha256, query_char_limit=200)
        run_key = hashlib.sha256(
            json.dumps(config, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        self._data_dir = Path(data_dir).resolve()
        self.path = self._data_dir / f".longmemeval_checkpoint_{run_key}"
        self._meta_path = self.path / "meta.json"
        self._config = config
        self.path.mkdir(parents=True, exist_ok=True)
        if self._meta_path.exists():
            try:
                existing = json.loads(self._meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"LongMemEval 断点元数据损坏: {self._meta_path}") from exc
            if existing != config:
                raise ValueError("LongMemEval 断点配置不匹配")
        else:
            _atomic_write(
                str(self._meta_path),
                json.dumps(config, ensure_ascii=False, indent=2) + "\n",
            )

    def _result_path(self, position: int) -> Path:
        if position <= 0 or position > self._config["total_entries"]:
            raise ValueError(f"断点题号越界: {position}")
        return self.path / f"{position:06d}.json"

    def save(self, position: int, result: dict) -> None:
        question_id = result.get("question_id") if isinstance(result, dict) else None
        if not isinstance(question_id, str) or not question_id:
            raise ValueError("断点结果缺少 question_id")
        _atomic_write(
            str(self._result_path(position)),
            json.dumps(result, ensure_ascii=False) + "\n",
        )

    def load(self, entries: Sequence[dict]) -> list[dict]:
        total_entries = self._config["total_entries"]
        if type(total_entries) is not int:
            raise ValueError("invalid checkpoint total")
        if (len(entries) > total_entries or
                self._config["checkpoint_schema"] == 1 and len(entries) != total_entries):
            raise ValueError("LongMemEval 断点题目总数不匹配")
        results: list[dict] = []
        for position, entry in enumerate(entries, start=1):
            result_path = self._result_path(position)
            if not result_path.exists():
                continue
            try:
                result = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"LongMemEval 断点结果损坏: {result_path}") from exc
            if result.get("question_id") != entry["question_id"]:
                raise ValueError(f"LongMemEval 断点题号错位: {result_path}")
            results.append(result)
        return results

    def clear(self) -> None:
        resolved = self.path.resolve()
        if resolved.parent != self._data_dir:
            raise RuntimeError("拒绝清理断点目录：父目录不匹配")
        if not resolved.name.startswith(".longmemeval_checkpoint_"):
            raise RuntimeError("拒绝清理断点目录：名称不匹配")
        shutil.rmtree(resolved, ignore_errors=False)


def save_longmemeval_outputs(
    report: dict,
    results: Sequence[dict],
    data_dir: str,
    *,
    hypotheses: Sequence[dict] | None = None,
) -> dict[str, str]:
    """原子保存摘要、检索 JSONL，以及可选官方问答 JSONL。"""
    stamp = int(time.time() * 1000)
    base = os.path.join(data_dir, f"longmemeval_{stamp}")
    paths = {
        "report": f"{base}.report.json",
        "retrieval": f"{base}.retrieval.jsonl",
    }
    _atomic_write(
        paths["report"],
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
    )
    _atomic_write(
        paths["retrieval"],
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in results),
    )
    if hypotheses is not None:
        paths["hypotheses"] = f"{base}.hypotheses.jsonl"
        _atomic_write(
            paths["hypotheses"],
            "".join(
                json.dumps(row, ensure_ascii=False) + "\n"
                for row in hypotheses
            ),
        )
    return paths
