"""Numerical read capability only: no Chroma client, writes or original text."""

from __future__ import annotations

import json
import math
from multiprocessing.connection import Connection
from pathlib import Path
import time

from kernel.memory_pipeline_models import PipelineScope
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from .daily_vector_snapshot import SnapshotData, load_snapshot
from .daily_vector_worker import (MAX_FRAME_BYTES, _bind_shared_deadline, _shared_command,
                                  _synthetic_embed, _validate_shared_embedding)


DIAGNOSTIC_STAGES = frozenset({"input", "preparation", "embedding", "numerical", "complete"})
DIAGNOSTIC_CODES = frozenset({"none", "child_input_rejected", "snapshot_preparation_failed",
                              "embedding_failed", "numerical_failed"})
ERROR_TYPES = frozenset({"ValueError", "RuntimeError", "OSError", "TimeoutError",
                         "TypeError", "IndexError", "MemoryError", "OtherError", "none"})


def validate_diagnostic(value: object, request_id: int) -> dict:
    """Closed child schema; arbitrary exception bodies never cross this boundary."""
    if (type(value) is not dict or set(value) != {"request_id", "stage", "error_code",
            "error_type", "embedding_ms", "numerical_ms", "complete"}
            or type(value["request_id"]) is not int or value["request_id"] != request_id
            or value["stage"] not in DIAGNOSTIC_STAGES or value["error_code"] not in DIAGNOSTIC_CODES
            or value["error_type"] not in ERROR_TYPES or type(value["complete"]) is not bool):
        raise ValueError("invalid snapshot diagnostic")
    for key in ("embedding_ms", "numerical_ms"):
        elapsed = value[key]
        if elapsed is not None and (type(elapsed) not in {int, float}
                or not math.isfinite(elapsed) or not 0 <= elapsed <= 120000):
            raise ValueError("invalid snapshot diagnostic timing")
    if len(json.dumps(value, allow_nan=False).encode()) > 1024:
        raise ValueError("snapshot diagnostic too large")
    if (value["complete"] != (value["stage"] == "complete")
            or (value["error_code"] == "none") != value["complete"]
            or (value["error_type"] == "none") != value["complete"]):
        raise ValueError("inconsistent snapshot diagnostic")
    return dict(value)


def serve_snapshot(connection: Connection, root_text: str, embedding: str, diagnostics: bool = False,
                   *, formal_embedding=None, embedding_client=None, embedding_deadline_ns=None) -> None:
    _validate_shared_embedding(embedding, formal_embedding, embedding_client, embedding_deadline_ns)
    if type(diagnostics) is not bool:
        raise ValueError("invalid snapshot diagnostics mode")
    if formal_embedding is not None:
        from kernel.raw_message_profile import FormalEmbeddingBinding

        if embedding != "bge-m3":
            raise ValueError("formal binding requires bge-m3")
        formal_embedding = FormalEmbeddingBinding.from_payload(formal_embedding)
    prepared: dict[PipelineScope, SnapshotData] = {}
    paged_prepared: dict[PipelineScope, int] = {}
    paged_session = None
    root = Path(root_text)
    if embedding == "bge-m3":
        try:
            if embedding_client is not None:
                _bind_shared_deadline(embedding_client, embedding_deadline_ns)
                embed = embedding_client
            else:
                from .daily_embedding_adapter import embed_bge_m3, FormalEmbeddingAdapter

                embed = FormalEmbeddingAdapter(formal_embedding) if formal_embedding is not None else embed_bge_m3
            embed(["阿露语义只读查询预热"])
            actual = embed.actual() if formal_embedding is not None else None
        except Exception:
            connection.send_bytes(b'{"version":2,"ready":false,"error":"embedding"}' if formal_embedding is not None else b'{"version":1,"ready":false,"error":"embedding"}')
            connection.close()
            if embedding_client is not None:
                embedding_client.close()
            return
    elif embedding in {"synthetic", "synthetic-stall", "synthetic-1024"}:
        from .daily_vector_worker import _synthetic_embed_1024
        embed = _synthetic_embed_1024 if embedding == "synthetic-1024" else _synthetic_embed
    else:
        raise ValueError("unsupported snapshot embedding")
    try:
        connection.send_bytes(json.dumps({"version": 2, "ready": True, "binding_sha256": formal_embedding.binding_sha256,
                              "actual": actual}, separators=(",", ":")).encode() if formal_embedding is not None else b'{"version":1,"ready":true}')
        while True:
            request = None
            result: object
            diagnostic: dict = {"request_id": 0, "stage": "input", "error_code": "child_input_rejected",
                          "error_type": "none", "embedding_ms": None, "numerical_ms": None,
                          "complete": False}
            try:
                request = _shared_command(json.loads(connection.recv_bytes(MAX_FRAME_BYTES)), embedding_client)
                base = {"version", "id", "scope", "op"}
                if (type(request) is not dict or type(request.get("version")) is not int
                        or request["version"] != 1 or type(request.get("id")) is not int
                        or request["id"] < 1 or type(request.get("scope")) is not list
                        or len(request["scope"]) != 4):
                    raise ValueError("invalid snapshot request")
                scope = PipelineScope(*request["scope"])
                diagnostic["request_id"] = request["id"]
                if request["op"] in {"warm_paged", "page_score_begin", "page_score", "page_score_end"}:
                    from .daily_vector_pages import BINDING_KEYS, PagedCosine, binding, check_deadline
                    if set(request) != base | {"page"} or type(request["page"]) is not dict:
                        raise ValueError("invalid paged score command")
                    page = request["page"]
                    if request["op"] == "warm_paged":
                        if (set(page) != {"generation"} or type(page["generation"]) is not int
                                or page["generation"] < 0 or scope not in paged_prepared and len(paged_prepared) >= 8
                                or paged_session is not None):
                            raise ValueError("invalid paged preparation")
                        paged_prepared[scope] = page["generation"]
                        result = True
                    elif request["op"] == "page_score_begin":
                        if (set(page) != BINDING_KEYS | {"query", "limit"}
                                or type(page["query"]) is not str or not page["query"].strip()
                                or len(page["query"]) > MAX_MEMORY_QUERY_CHARS
                                or paged_session is not None or paged_prepared.get(scope) != page["generation"]):
                            raise ValueError("invalid paged score start")
                        session_binding = binding(page)
                        if embedding_client is not None:
                            _bind_shared_deadline(embedding_client, page["deadline_ns"])
                        query = embed([page["query"].strip()])[0]
                        check_deadline(page["deadline_ns"])
                        scorer = PagedCosine(session_binding, query, page["limit"])
                        paged_session = (scope, scorer)
                        result = scorer.cursor.ack()
                    else:
                        if paged_session is None or paged_session[0] != scope:
                            raise ValueError("paged score session unavailable")
                        if request["op"] == "page_score":
                            result = paged_session[1].feed(page)
                        else:
                            result = paged_session[1].finish(page)
                            paged_session = None
                elif request["op"] == "warm_snapshot":
                    if set(request) != base | {"generation"}:
                        raise ValueError("invalid snapshot preparation")
                    if scope not in prepared and len(prepared) >= 8:
                        raise ValueError("snapshot scope capacity exceeded")
                    diagnostic.update(stage="preparation", error_code="snapshot_preparation_failed")
                    prepared[scope] = load_snapshot(root, scope, request["generation"],
                        embedding="synthetic" if embedding == "synthetic-stall" else embedding,
                        **({"binding_sha256": formal_embedding.binding_sha256} if formal_embedding is not None else {}))
                    result = True
                elif request["op"] == "search_cosine_hits":
                    if (set(request) != base | {"query", "limit"}
                            or type(request["query"]) is not str
                            or not request["query"].strip() or len(request["query"]) > MAX_MEMORY_QUERY_CHARS
                            or type(request["limit"]) is not int or not 1 <= request["limit"] <= 100
                            or scope not in prepared):
                        raise ValueError("invalid snapshot query")
                    data = prepared[scope]
                    diagnostic.update(stage="embedding", error_code="embedding_failed")
                    began = time.perf_counter()
                    if embedding == "synthetic-stall":
                        time.sleep(60)
                    if data.ids:
                        query = embed([request["query"].strip()])[0]
                        diagnostic["embedding_ms"] = (time.perf_counter() - began) * 1000
                        diagnostic.update(stage="numerical", error_code="numerical_failed")
                        began = time.perf_counter()
                        result = data.cosine_hits(query, limit=request["limit"])
                        diagnostic["numerical_ms"] = (time.perf_counter() - began) * 1000
                    else:
                        result = ()
                else:
                    raise ValueError("snapshot capability rejects writes")
                response = {"version": 1, "id": request["id"], "ok": True, "result": result}
                diagnostic.update(stage="complete", error_code="none", complete=True)
            except EOFError:
                break
            except Exception as exc:
                name = type(exc).__name__
                diagnostic["error_type"] = name if name in ERROR_TYPES else "OtherError"
                response = {"version": 1, "id": request.get("id") if type(request) is dict else None,
                            "ok": False, "error": type(exc).__name__}
            if diagnostics:
                response["diagnostic"] = validate_diagnostic(diagnostic, diagnostic["request_id"])
            if formal_embedding is not None:
                response["formal"] = {"encode_calls": embed.encode_calls if response["ok"] else None}
            body = json.dumps(response, allow_nan=False, separators=(",", ":")).encode()
            if len(body) > MAX_FRAME_BYTES:
                raise ValueError("snapshot response too large")
            connection.send_bytes(body)
    finally:
        connection.close()
        if embedding_client is not None:
            embedding_client.close()
