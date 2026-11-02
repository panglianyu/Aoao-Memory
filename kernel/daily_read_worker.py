"""Read-only child for bounded daily recall; no production configuration."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import asdict
from multiprocessing.connection import Connection
from pathlib import Path

from kernel.memory_pipeline_models import PipelineScope
from kernel.raw_message_models import RawMessageError, Scope, ScopeError
from kernel.raw_message_reader import RawReadLifetime, RawReadView
from kernel.raw_message_retrieval import RawMessageRetrieval
from kernel.daily_memory_contracts import DailyRecallOutcome, validate_retrieval_hints
from system_memory_god.storage.derived_read_view import (
    DerivedEvidenceReadView, SourceLedgerReadView,
)
from system_memory_god.storage.partition_registry import PartitionRegistry
from system_memory_god.storage.pipeline_database import (
    reject_directory_link, resolved_pipeline_path,
)

MAX_READ_FRAME = 2 * 1024 * 1024
MAX_READ_RESULT = 1024 * 1024
READ_PROTOCOL_VERSION = 6


class _EvidenceReadView:
    """Per-request slice projection; storage originals are never overwritten."""

    evidence_mode = True

    def __init__(self, view: RawReadView, query: str, hints: tuple[str, ...] = ()):
        self.view, self.query = view, query
        self.window_query = (query, *hints) if hints else query

    def __getattr__(self, name):
        return getattr(self.view, name)

    def search(self, scope, query, **kwargs):
        return self.search_page(scope, query, **kwargs)

    def search_page(self, scope, query, **kwargs):
        if type(self.window_query) is tuple:
            kwargs["evidence_query"] = self.window_query
        return self.view.search_evidence_page(scope, query, **kwargs)

    def hint_search(self, scope, hints, **kwargs):
        return self.view.hint_search(scope, hints, evidence_query=self.window_query, **kwargs)

    def published_snapshots(self, scope, message_ids, **kwargs):
        return self.view.published_evidence_snapshots(scope, message_ids,
                                                     query=self.window_query, **kwargs)

    def neighbors(self, scope, message_id, **kwargs):
        return self.view.evidence_neighbors(scope, message_id, query=self.window_query, **kwargs)


class _ReadRegistry:
    def __init__(self, root: Path):
        self._root = root

    def snapshot(self, scope: PipelineScope) -> dict | None:
        return PartitionRegistry(self._root).snapshot(scope)


class _ReadStores:
    def __init__(self, root: Path):
        self.root = resolved_pipeline_path(root)

    def partition(self, scope: PipelineScope) -> Path:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        path = self.root / scope.partition_id
        if resolved_pipeline_path(path).parent != self.root:
            raise ValueError("pipeline path changed")
        reject_directory_link(path)
        return path

    def partition_registry(self) -> _ReadRegistry:
        return _ReadRegistry(self.root)

    def evidence(self, scope: PipelineScope) -> DerivedEvidenceReadView:
        return DerivedEvidenceReadView(self.root, scope)

    def ledger(self, scope: PipelineScope) -> SourceLedgerReadView:
        return SourceLedgerReadView(self.root, scope)


class _VectorHits:
    def __init__(self, hits: tuple[tuple[str, float], ...]):
        self.hits = hits

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *,
                                 limit: int) -> tuple[tuple[str, float], ...]:
        return self.hits[:limit]


def _search(raw_path: Path, derived_root: Path, request: dict, *,
            lifetime: RawReadLifetime | None = None) -> dict:
    operation = request.get("op", "search") if type(request) is dict else "search"
    if operation not in {"search", "collect", "publish"}:
        raise ValueError("invalid read operation")
    candidate_items = request.get("candidates") if type(request) is dict else None
    if operation == "publish" and type(candidate_items) is not list:
        raise ValueError("invalid candidate request")
    if operation in {"collect", "publish"}:
        expected = {"op"} | ({"candidates"} if operation == "publish" else set())
        if not expected.issubset(request):
            raise ValueError("invalid candidate request")
        request = {key: value for key, value in request.items() if key not in expected}
    if (type(request) is not dict or set(request) - {"query_options", "retrieval_hints"} != {
            "version", "id", "scope", "query", "top_k", "neighbors", "start_ts",
            "end_ts", "memory_scope", "vector_hits", "deadline"}
            or request["version"] != READ_PROTOCOL_VERSION or type(request["id"]) is not int
            or type(request["scope"]) is not list or len(request["scope"]) != 4
            or type(request["vector_hits"]) is not list
            or len(request["vector_hits"]) > 100
            or type(request["deadline"]) not in {int, float}
            or not math.isfinite(request["deadline"])):
        raise ValueError("invalid read request")
    hints = validate_retrieval_hints(request.get("retrieval_hints", ()))
    remaining = request["deadline"] - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("read request expired")
    scope = Scope(*request["scope"])
    if scope.connector_id is None or scope.conversation_id is None:
        raise ValueError("complete read scope required")
    hits = []
    for pair in request["vector_hits"]:
        if (type(pair) is not list or len(pair) != 2
                or type(pair[0]) is not str or type(pair[1]) not in {int, float}):
            raise ValueError("invalid vector candidate")
        hits.append((pair[0], pair[1]))
    raw = _EvidenceReadView(RawReadView(raw_path, trusted_root=raw_path.parent, scope=scope,
                                     lifetime=lifetime),
                            request["query"], hints)
    stores = _ReadStores(derived_root)
    # Account for actual scope/query/envelope bytes before the candidate array.
    # collect cannot know the next vector hits yet: reserve their validated
    # maximum (100 IDs of <=256 Unicode chars and finite numeric scores).
    envelope = {**request, "op": "publish", "candidates": [], "vector_hits": []}
    overhead = len(json.dumps(envelope, ensure_ascii=False, separators=(",", ":"),
                              allow_nan=False).encode()) + 128
    vector_reserve = 100 * (256 * 4 + 32 + 8) if operation == "collect" else 0
    retrieval = RawMessageRetrieval(raw, stores, _VectorHits(tuple(hits)),
                                    request_timeout_seconds=min(0.3, remaining),
                                    collect_only=operation == "collect",
                                    query_options=request.get("query_options"),
                                    retrieval_hints=hints,
                                    candidate_byte_budget=min(1024 * 1024 - 4096,
                                        MAX_READ_RESULT - overhead - vector_reserve))
    if operation == "publish":
        result = asdict(asyncio.run(retrieval.publish_candidates(
            scope, request["query"], candidate_items or [], top_k=request["top_k"],
            neighbors=request["neighbors"], start_ts=request["start_ts"],
            end_ts=request["end_ts"], memory_scope=request["memory_scope"],
            deadline=request["deadline"])))
    else:
        async def collect():
            outcome = await retrieval.search_with_status(
                scope, request["query"], top_k=request["top_k"],
                neighbors=request["neighbors"], start_ts=request["start_ts"],
                end_ts=request["end_ts"], memory_scope=request["memory_scope"],
                deadline=request["deadline"])
            if operation != "collect" or not hints or outcome.status == "unavailable":
                return outcome
            reasons = list(outcome.reason_codes)
            items = await retrieval.collect_hint_candidates(scope, outcome.items,
                start_ts=request["start_ts"], end_ts=request["end_ts"],
                deadline=request["deadline"], reasons=reasons,
                memory_scope=request["memory_scope"])
            return DailyRecallOutcome(items, "degraded" if reasons else "ready",
                                      tuple(dict.fromkeys(reasons)), outcome.trace)

        result = asdict(asyncio.run(collect()))
    if result.get("trace") is None:
        result.pop("trace", None)
    if operation == "collect":
        result["kind"] = "candidates"
    return result


def _prepare_scope(request: dict) -> Scope:
    if (type(request) is not dict or set(request) != {
            "version", "id", "op", "scope"}
            or request["version"] != READ_PROTOCOL_VERSION or type(request["id"]) is not int
            or request["op"] not in {"prepare", "prepare_online"}
            or type(request["scope"]) is not list
            or len(request["scope"]) != 4):
        raise ValueError("invalid read preparation")
    scope = Scope(*request["scope"])
    if scope.connector_id is None or scope.conversation_id is None:
        raise ValueError("complete read scope required")
    return scope


def _prepare(raw_path: Path, derived_root: Path, request: dict, *,
             lifetime: RawReadLifetime | None = None) -> dict:
    scope = _prepare_scope(request)
    if scope.connector_id is None or scope.conversation_id is None:
        raise ValueError("complete read scope required")
    raw = RawReadView(raw_path, trusted_root=raw_path.parent, scope=scope, lifetime=lifetime)
    if not raw.conversation_registered():
        return {"ready": False, "vector": False, "raw_generation": 0}
    generation, resolved = raw.partition_state(scope)
    partition = PipelineScope(scope.user_id, scope.agent_id,
                              scope.connector_id, scope.conversation_id)
    stores = _ReadStores(derived_root)
    if request["op"] == "prepare_online":
        # Raw is independently usable during ongoing derivation. Full source
        # proofs, not this preparation hint, grant visibility during each search.
        raw.published_snapshots(scope, ())
        raw.search(scope, "__daily_read_warmup__", limit=1)
        try:
            snapshot = stores.partition_registry().snapshot(partition)
            path = stores.partition(partition)
            vector = bool(snapshot and snapshot["state"] in {"READY", "INITIALIZING"}
                          and (path / "vector_index.db").is_file()
                          and (path / "vectors").is_dir())
        except (RuntimeError, ValueError, OSError):
            vector = False
        return {"ready": True, "vector": vector, "raw_generation": generation}
    snapshot = stores.partition_registry().snapshot(partition)
    path = stores.partition(partition)
    if not resolved:
        ready = False
        vector = False
    elif snapshot is None:
        ready = not raw.has_derived_results(scope) and not path.exists()
        vector = False
    else:
        evidence_present = (path / "derived_evidence.db").is_file()
        ledger_present = (path / "source_ledger.db").is_file()
        vector_authority = (path / "vector_index.db").is_file()
        vector_tree = (path / "vectors").is_dir()
        vector = snapshot["vector"]
        ready = (snapshot["state"] == "READY"
                 and snapshot["raw_generation"] == generation
                 and snapshot["evidence"] and snapshot["ledger"]
                 and evidence_present and ledger_present
                 and vector == vector_authority == vector_tree)
        if ready:
            stores.evidence(partition).validate_existing()
            stores.ledger(partition).validate_existing()
    return {"ready": ready, "vector": vector if ready else False,
            "raw_generation": generation}


def _content_chunk(raw_path: Path, request: dict, *,
                   lifetime: RawReadLifetime | None = None) -> dict:
    if (type(request) is not dict or set(request) != {
            "version", "id", "op", "scope", "message_id", "expected_revision",
            "expected_content_hash", "offset_chars", "limit_chars", "deadline"}
            or request["version"] != READ_PROTOCOL_VERSION or type(request["id"]) is not int
            or type(request["scope"]) is not list or len(request["scope"]) != 4
            or type(request["deadline"]) not in {int, float}
            or not math.isfinite(request["deadline"])):
        raise ValueError("invalid content request")
    scope = Scope(*request["scope"])
    raw = RawReadView(raw_path, trusted_root=raw_path.parent, scope=scope, lifetime=lifetime)
    return asdict(raw.read_content_chunk(scope, request["message_id"],
        expected_revision=request["expected_revision"],
        expected_content_hash=request["expected_content_hash"],
        offset_chars=request["offset_chars"], limit_chars=request["limit_chars"],
        deadline=request["deadline"]))


def serve(connection: Connection, raw_text: str, derived_text: str) -> None:
    """One process owns only finite read requests and never accepts write verbs."""
    raw_path, derived_root = Path(raw_text), Path(derived_text)
    lifetime: RawReadLifetime | None = None
    try:
        connection.send_bytes(json.dumps({"version": READ_PROTOCOL_VERSION, "ready": True}).encode())
        while True:
            try:
                data = connection.recv_bytes(MAX_READ_FRAME)
            except EOFError:
                return
            request = None
            try:
                request = json.loads(data)
                if type(request) is dict and request.get("op") in {"prepare", "prepare_online"}:
                    scope = _prepare_scope(request)
                    if lifetime is None:
                        candidate = RawReadLifetime(raw_path, trusted_root=raw_path.parent)
                        candidate.start(RawReadView(
                            raw_path, trusted_root=raw_path.parent, scope=scope))
                        lifetime = candidate
                    result = _prepare(raw_path, derived_root, request, lifetime=lifetime)
                elif type(request) is dict and request.get("op") == "content_chunk":
                    result = _content_chunk(raw_path, request, lifetime=lifetime)
                else:
                    result = (_search(raw_path, derived_root, request, lifetime=lifetime)
                              if lifetime is not None else _search(raw_path, derived_root, request))
                response = {"version": READ_PROTOCOL_VERSION, "id": request["id"], "ok": True,
                            "result": result}
            except Exception as exc:
                if isinstance(exc, TimeoutError):
                    error = "TimeoutError"
                elif isinstance(exc, ScopeError):
                    error = "ScopeError"
                elif isinstance(exc, RawMessageError):
                    error = "RawMessageError"
                elif isinstance(exc, ValueError):
                    error = "ValueError"
                else:
                    error = "WorkerFailure"
                response = {"version": READ_PROTOCOL_VERSION,
                            "id": request.get("id") if type(request) is dict else None,
                            "ok": False, "error": error}
            encoded = json.dumps(response, ensure_ascii=False,
                                 separators=(",", ":"), allow_nan=False).encode()
            if len(encoded) > MAX_READ_RESULT:
                encoded = json.dumps({"version": READ_PROTOCOL_VERSION, "id": response["id"],
                                      "ok": False, "error": "ResponseTooLarge"}).encode()
            try:
                connection.send_bytes(encoded)
            except (OSError, EOFError):
                return
    finally:
        try:
            if lifetime is not None:
                lifetime.close()
        finally:
            connection.close()
