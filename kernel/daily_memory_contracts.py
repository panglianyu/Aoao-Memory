"""Daily runtime value contracts, without importing plugin implementations."""

from dataclasses import dataclass
import math
import re
import time
from typing import Literal, Protocol, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from kernel.raw_message_models import StoredMessage
    from kernel.raw_message_service import RawMessageService
    from kernel.raw_message_profile import RawMessageProfile
    from kernel.app_bus import AppBus

RecallStatus = Literal["ready", "degraded", "recovering", "unavailable", "stopped"]
SNAPSHOT_QUERY_TIMEOUT = .25
WARM_RECALL_TIMEOUT = .3
INTERACTIVE_RECALL_TIMEOUT = 2.0
RecallPurpose = Literal["warm", "interactive"]


def recall_deadline(*, purpose: RecallPurpose = "warm", deadline: float | None = None) -> float:
    """One absolute caller deadline; explicit queries never extend shorter limits."""
    if type(purpose) is not str or purpose not in {"warm", "interactive"}:
        raise ValueError("invalid recall purpose")
    if deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
        raise ValueError("invalid recall deadline")
    limit = WARM_RECALL_TIMEOUT if purpose == "warm" else INTERACTIVE_RECALL_TIMEOUT
    capped = time.monotonic() + limit
    return min(deadline, capped) if deadline is not None else capped


_PUBLIC_STATES = frozenset({"disabled", "preparing", "ready", "degraded", "unavailable"})
_PUBLIC_REASONS = frozenset({
    "authority_denied", "deadline_exceeded", "invalid_query", "retrieval_error", "disabled",
    "reader_recovering", "reader_unavailable", "reader_preparing", "process_exit", "timeout",
    "protocol_error", "not_prepared", "stopped", "raw_unavailable", "derived_unavailable",
    "vector_unavailable", "vector_online_disabled", "result_budget_limited", "scan_budget_limited",
    "publication_pending", "metadata_deadline", "metadata_unavailable", "metadata_capacity",
    "vector_capacity_exceeded", "partition_unavailable", "component_error",
    "legacy_unmapped", "legacy_unavailable", "legacy_timeout", "derived_recovering",
    "derived_recovery_incomplete", "startup_incomplete", "semantic_publication_incomplete",
    "semantic_publisher_failed",
})


def public_daily_health(value: object) -> dict:
    """Finite allowlist only: no exception strings, paths, ids, messages or keys."""
    if type(value) is not dict:
        return {"state": "unavailable", "reason_codes": ["component_error"]}
    state = value.get("state")
    if type(state) is str and state in {"created", "initializing", "recovering"}:
        state = "preparing"
    if type(state) is not str or state not in _PUBLIC_STATES:
        state = "unavailable"
    reasons = value.get("reason_codes", ())
    if type(reasons) not in {list, tuple}:
        reasons = ("component_error",)
    codes = [code if type(code) is str and code in _PUBLIC_REASONS else "component_error"
             for code in reasons[:16]]
    semantic_reason = value.get("semantic_reason")
    if semantic_reason is not None and not (type(semantic_reason) is str and semantic_reason == ""):
        codes.append(semantic_reason if type(semantic_reason) is str
                     and semantic_reason in _PUBLIC_REASONS else "component_error")
    if value.get("worker_error"):
        codes.append("component_error")
    result: dict[str, object] = {"state": state, "reason_codes": list(dict.fromkeys(codes))}
    for name in ("reader_recovery_attempts", "vector_recovery_attempts", "pending", "result_count"):
        number = value.get(name)
        if type(number) is int and 0 <= number <= 1_000_000_000:
            result[name] = number
    semantic = value.get("semantic_state")
    if type(semantic) is str and semantic in _PUBLIC_STATES:
        result["semantic_state"] = semantic
    return result


@dataclass(frozen=True)
class DailyRecallOutcome:
    items: list[dict]
    status: RecallStatus
    reason_codes: tuple[str, ...] = ()
    trace: dict | None = None


MEMORY_DOMAINS = ("raw", "vector", "entity", "graph", "rule", "journal", "schedule")
MAX_MEMORY_QUERY_CHARS = 32768
_QUERY_STOP = frozenset("a an the is are was were be who which what when where how does do did of to in on at for with and or my me please tell prefer".split())


def memory_query_terms(query: str) -> tuple[str, ...]:
    """At most eight literal terms; never an FTS/SQL expression."""
    if type(query) is not str or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS:
        raise ValueError("invalid domain query")
    terms = []
    for token in re.findall(r"[a-zA-Z0-9_]+|[\u3400-\u9fff]+", query.lower()):
        if token in _QUERY_STOP:
            continue
        parts = [token] if not re.search(r"[\u3400-\u9fff]", token) or len(token) <= 2 else [
            token[i:i + 2] for i in range(len(token) - 1)]
        terms.extend(parts)
    return tuple(dict.fromkeys(terms))[:8] or (query.strip().lower(),)


def validate_retrieval_hints(value: object) -> tuple[str, ...]:
    """Finite literal words only; these never carry authority or instructions."""
    if type(value) not in {tuple, list}:
        raise ValueError("invalid retrieval hints")
    values = cast("list[str] | tuple[str, ...]", value)
    if (len(values) > 16
            or any(type(term) is not str or not term or term != term.lower()
                   or re.fullmatch(r"[a-z0-9_]+|[\u3400-\u9fff]{1,2}", term) is None
                   for term in values)
            or sum(map(len, values)) > 512 or len(set(values)) != len(values)):
        raise ValueError("invalid retrieval hints")
    return tuple(values)


def memory_retrieval_hints(options: object) -> tuple[str, ...]:
    """Select a stable round-robin set without changing the original query."""
    if type(options) not in {list, tuple}:
        raise ValueError("invalid options")
    values = cast("list[str] | tuple[str, ...]", options)
    if (len(values) > 256
            or any(type(option) is not str or not option.strip() or len(option) > 32768
                   for option in values)):
        raise ValueError("invalid options")
    try:
        for option in values:
            option.encode("utf-8")
    except UnicodeError:
        raise ValueError("invalid options") from None
    normalized = sorted({" ".join(option.lower().split()) for option in values})
    queues = [tuple(term for term in memory_query_terms(option)
                    if re.fullmatch(r"[a-z0-9_]+|[\u3400-\u9fff]{1,2}", term)
                    and term not in _QUERY_STOP) for option in normalized]
    selected: list[str] = []
    size = 0
    for offset in range(max(map(len, queues), default=0)):
        for terms in queues:
            if offset >= len(terms):
                continue
            term = terms[offset]
            if term in selected or size + len(term) > 512:
                continue
            selected.append(term)
            size += len(term)
            if len(selected) == 16:
                return tuple(selected)
    return tuple(selected)


def validate_memory_query_options(value: object) -> dict | None:
    """Strict transport values, never a source of caller authorization."""
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {
            "full", "enabled", "trace", "event_start_ts", "event_end_ts"}
            or type(value["full"]) is not bool or type(value["trace"]) is not bool
            or type(value["enabled"]) not in {list, tuple}
            or any(type(domain) is not str or domain not in MEMORY_DOMAINS for domain in value["enabled"])
            or len(set(value["enabled"])) != len(value["enabled"])
            or not value["full"] and set(value["enabled"]) - {"raw", "vector"}
            or any(stamp is not None and (type(stamp) not in {int, float}
                or not math.isfinite(stamp) or stamp < 0)
                for stamp in (value["event_start_ts"], value["event_end_ts"]))):
        raise ValueError("invalid memory query options")
    if (value["event_start_ts"] is not None and value["event_end_ts"] is not None
            and value["event_start_ts"] >= value["event_end_ts"]):
        raise ValueError("invalid event window")
    return {**value, "enabled": list(value["enabled"])}


@dataclass(frozen=True)
class MemorySaveReceipt:
    """Durable original acceptance does not promise published derivations."""

    message: "StoredMessage"
    saved: bool = True
    derivation_status: str = "pending"


class DailyRuntime(Protocol):
    raw: "RawMessageService"
    derivation: object
    retrieval: object

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def health(self) -> dict: ...


class DailyRuntimeFactory(Protocol):
    def __call__(self, raw: "RawMessageService", profile: "RawMessageProfile",
                 bus: "AppBus") -> DailyRuntime: ...
