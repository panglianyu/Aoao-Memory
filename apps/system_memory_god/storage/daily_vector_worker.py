"""Isolated owner of the daily Chroma index."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from multiprocessing.connection import Connection
import os
from pathlib import Path
import stat
import time

from kernel.memory_pipeline_models import PipelineScope, identity
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from .daily_vector_index import DailyVectorIndex
from .pipeline_database import reject_directory_link
from .scoped_stores import ScopedStores


MAX_FRAME_BYTES = 2 * 1024 * 1024


def _embedding_deadline(value) -> int:
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError("invalid shared embedding deadline")
    return value


def _validate_shared_embedding(embedding, formal_embedding, client, deadline_ns) -> None:
    if client is None and deadline_ns is None:
        return
    if formal_embedding is not None:
        raise ValueError("formal and shared embedding cannot be combined")
    if (embedding != "bge-m3" or not callable(client)
            or any(not callable(getattr(client, method, None)) for method in
                   ("bind_deadline", "close_parent_copy", "close"))):
        raise ValueError("explicit shared embedding client required")
    if _embedding_deadline(deadline_ns) <= time.monotonic_ns():
        raise ValueError("expired shared embedding configuration")


def _bind_shared_deadline(client, deadline_ns) -> None:
    if _embedding_deadline(deadline_ns) <= time.monotonic_ns():
        raise TimeoutError("shared embedding operation deadline exceeded")
    client.bind_deadline(deadline_ns)


def _shared_command(request, client):
    if client is None:
        return request
    if type(request) is not dict:
        raise ValueError("invalid shared embedding command")
    request = dict(request)
    absolute = request.pop("embedding_deadline_ns", None)
    _bind_shared_deadline(client, absolute)
    return request


def _synthetic_embed(texts: list[str]) -> list[list[float]]:
    vectors = []
    for value in texts:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        vectors.append([(digest[offset] + 1) / 256 for offset in range(16)])
    return vectors


def _synthetic_stall(texts: list[str]) -> list[list[float]]:
    time.sleep(60)
    return _synthetic_embed(texts)


def _execute(index: DailyVectorIndex, request: dict) -> object:
    scope = PipelineScope(*request["scope"])
    operation = request["op"]
    if operation == "warm_existing":
        asyncio.run(index.warm_existing(scope))
        return True
    if operation in {"put", "put_snapshot"}:
        asyncio.run(index.put(scope, request["records"]))
        return asyncio.run(index.receipts(scope, request["records"], snapshot=operation == "put_snapshot"))
    if operation in {"receipts", "receipts_snapshot"}:
        return asyncio.run(index.receipts(scope, request["records"], snapshot=operation == "receipts_snapshot"))
    if operation == "contains":
        return asyncio.run(index.contains(scope, tuple(request["ids"])))
    if operation == "present_ids":
        return asyncio.run(index.present_ids(scope, tuple(request["ids"])))
    if operation == "search_cosine_hits":
        return asyncio.run(index.search_cosine_hits(
            scope, request["query"], limit=request["limit"]
        ))
    raise ValueError("unknown vector operation")


def _reject_constant(value: str) -> None:
    raise ValueError("invalid vector number")


def _validate_request(request: object) -> dict:
    if (type(request) is not dict or request.get("version") != 1
            or type(request.get("id")) is not int or request["id"] < 1
            or type(request.get("scope")) is not list
            or len(request["scope"]) != 4):
        raise ValueError("invalid vector request")
    PipelineScope(*request["scope"])
    operation = request.get("op")
    shared = {"version", "id", "scope", "op"}
    if operation == "warm_existing":
        if set(request) != shared:
            raise ValueError("invalid vector warm request")
    elif operation in {"put", "receipts", "put_snapshot", "receipts_snapshot"}:
        if (set(request) != shared | {"records"}
                or type(request["records"]) is not list
                or not 1 <= len(request["records"]) <= 100):
            raise ValueError("invalid vector write")
        for record in request["records"]:
            if (type(record) is not dict or set(record) != {
                    "memory_id", "payload", "payload_hash"}
                    or type(record.get("payload")) is not dict
                    or type(record["payload"].get("text")) is not str
                    or type(record.get("payload_hash")) is not str):
                raise ValueError("invalid vector write")
            identity(record["memory_id"])
    elif operation in {"contains", "present_ids"}:
        if (set(request) != shared | {"ids"}
                or type(request["ids"]) is not list
                or not 1 <= len(request["ids"]) <= 100):
            raise ValueError("invalid vector lookup")
        for key in request["ids"]:
            identity(key)
    elif operation == "search_cosine_hits":
        if (set(request) != shared | {"query", "limit"}
                or type(request["query"]) is not str
                or not request["query"].strip() or len(request["query"]) > MAX_MEMORY_QUERY_CHARS
                or type(request["limit"]) is not int
                or not 1 <= request["limit"] <= 100):
            raise ValueError("invalid vector search")
    else:
        raise ValueError("unknown vector operation")
    return request


def _owner_file(root: Path):
    reject_directory_link(root)
    root.mkdir(parents=True, exist_ok=True)
    reject_directory_link(root)
    path = root / "daily_vector_owner.lock"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_BINARY)
    except FileExistsError:
        before = path.lstat()
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or getattr(before, "st_file_attributes", 0)
                & stat.FILE_ATTRIBUTE_REPARSE_POINT):
            raise RuntimeError("vector owner file unavailable") from None
        descriptor = os.open(path, os.O_RDWR | os.O_BINARY)
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
            os.close(descriptor)
            raise RuntimeError("vector owner file changed")
    handle = os.fdopen(descriptor, "r+b", buffering=0)
    try:
        if os.fstat(descriptor).st_size == 0:
            handle.write(b"\0")
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            posix_lock = importlib.import_module("fcntl")
            posix_lock.flock(descriptor, posix_lock.LOCK_EX | posix_lock.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("vector owner unavailable") from None
    return handle


def serve(connection: Connection, root_text: str, embedding: str, *, formal_embedding=None,
          embedding_client=None, embedding_deadline_ns=None) -> None:
    """Process entry point; never import or read production model configuration."""
    _validate_shared_embedding(embedding, formal_embedding, embedding_client, embedding_deadline_ns)
    if formal_embedding is not None:
        from kernel.raw_message_profile import FormalEmbeddingBinding

        if embedding != "bge-m3":
            raise ValueError("formal binding requires bge-m3")
        formal_embedding = FormalEmbeddingBinding.from_payload(formal_embedding)
    if embedding not in {"bge-m3", "synthetic", "synthetic-stall", "synthetic-drop-ack",
                         "synthetic-wrong-id", "synthetic-bad-ready"}:
        raise ValueError("unsupported vector embedding")
    stores = ScopedStores(Path(root_text))
    try:
        owner = _owner_file(stores.root)
    except (OSError, RuntimeError, ValueError):
        connection.send_bytes(b'{"version":2,"ready":false,"error":"owner"}' if formal_embedding is not None else b'{"version":1,"ready":false,"error":"owner"}')
        connection.close()
        if embedding_client is not None:
            embedding_client.close()
        return
    if embedding == "bge-m3":
        try:
            if embedding_client is not None:
                _bind_shared_deadline(embedding_client, embedding_deadline_ns)
                embed = embedding_client
            else:
                from .daily_embedding_adapter import embed_bge_m3, FormalEmbeddingAdapter

                embed = FormalEmbeddingAdapter(formal_embedding) if formal_embedding is not None else embed_bge_m3
            embed(["阿露记忆向量预热"])
            actual = embed.actual() if formal_embedding is not None else None
        except (RuntimeError, ValueError, OSError):
            connection.send_bytes(b'{"version":2,"ready":false,"error":"embedding"}' if formal_embedding is not None else b'{"version":1,"ready":false,"error":"embedding"}')
            connection.close()
            owner.close()
            if embedding_client is not None:
                embedding_client.close()
            return
    else:
        embed = _synthetic_stall if embedding == "synthetic-stall" else _synthetic_embed
    try:
        index = DailyVectorIndex(stores, embed)
        if embedding == "synthetic-bad-ready":
            connection.send_bytes(b"[]")
            time.sleep(60)
            return
        connection.send_bytes(json.dumps({"version": 2, "ready": True, "binding_sha256": formal_embedding.binding_sha256,
                              "actual": actual}, separators=(",", ":")).encode() if formal_embedding is not None else b'{"version":1,"ready":true}')
        while True:
            try:
                data = connection.recv_bytes(MAX_FRAME_BYTES)
            except EOFError:
                break
            request = None
            try:
                request = _validate_request(_shared_command(
                    json.loads(data, parse_constant=_reject_constant), embedding_client))
                result = _execute(index, request)
                if embedding == "synthetic-drop-ack" and request["op"] == "put":
                    return
                response = {"version": 1, "id": request["id"] + (1 if embedding == "synthetic-wrong-id" else 0), "ok": True,
                            "result": result}
            except Exception as exc:
                response = {"version": 1, "id": request.get("id") if isinstance(request, dict) else None,
                            "ok": False, "error": type(exc).__name__}
            if formal_embedding is not None:
                response["formal"] = {"encode_calls": embed.encode_calls if response["ok"] else None}
            connection.send_bytes(json.dumps(response, separators=(",", ":")).encode())
    finally:
        connection.close()
        owner.close()
        if embedding_client is not None:
            embedding_client.close()
