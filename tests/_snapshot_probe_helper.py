"""TEMP-only R9 feasibility probe. Not a production cache or authority.

The probe exports through the original in-process Chroma owner solely to test
the proposed receipt extension. No second PersistentClient or directory clone
is used. Production publication and provenance approval are not implemented.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import numpy as np

_PREFLIGHT_OWNERS: list[object] = []


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _save_numbers(root: Path, ids: list[str], vectors: np.ndarray) -> dict:
    root.mkdir()
    body = np.asarray(vectors, dtype="<f4").tobytes()
    directory = json.dumps(ids, separators=(",", ":")).encode()
    (root / "vectors.f32").write_bytes(body)
    (root / "ids.json").write_bytes(directory)
    return {"rows": len(ids), "dimension": vectors.shape[1],
            "vector_sha256": _digest(body), "ids_sha256": _digest(directory),
            "bytes": len(body) + len(directory)}


def _query_numbers(root: str, manifest: dict, query: list[float]) -> list:
    """Only numerical files are opened in the spawned query process."""
    path = Path(root)
    body = (path / "vectors.f32").read_bytes()
    directory = (path / "ids.json").read_bytes()
    if (_digest(body) != manifest["vector_sha256"]
            or _digest(directory) != manifest["ids_sha256"]):
        raise ValueError("probe snapshot hash mismatch")
    matrix = np.frombuffer(body, dtype="<f4").reshape(
        manifest["rows"], manifest["dimension"])
    ids = json.loads(directory)
    q = np.asarray(query, dtype=np.float64)
    norms = np.sqrt(np.einsum("ij,ij->i", matrix, matrix, dtype=np.float64))
    qnorm = np.linalg.norm(q)
    if (not np.isfinite(matrix).all() or not np.isfinite(q).all()
            or not np.isfinite(norms).all() or (norms <= 0).any() or qnorm <= 0):
        raise ValueError("invalid probe vectors")
    scores = np.einsum("ij,j->i", matrix, q, dtype=np.float64) / (norms * qnorm)
    order = sorted(range(len(ids)), key=lambda row: (-scores[row], ids[row]))[:100]
    return [[ids[row], float(np.clip(scores[row], -1, 1))] for row in order]


def _query_child(connection: Any, root: str, manifest: dict, query: list[float]) -> None:
    try:
        start = time.perf_counter()
        hits = _query_numbers(root, manifest, query)
        connection.send({"ok": True, "hits": hits,
                         "read_query_ms": (time.perf_counter() - start) * 1000,
                         "pid": os.getpid()})
    except Exception as exc:
        connection.send({"ok": False, "error": type(exc).__name__})
    finally:
        connection.close()


def _spawn_query(root: Path, manifest: dict, query: list[float]) -> dict:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=_query_child,
                              args=(child, str(root), manifest, query))
    try:
        process.start()
        child.close()
        if not parent.poll(15):
            raise TimeoutError("snapshot query child exceeded probe budget")
        response = parent.recv()
        if not response["ok"]:
            raise RuntimeError("snapshot query child failed: " + response["error"])
        process.join(3)
        if process.is_alive() or process.exitcode != 0:
            raise RuntimeError("snapshot query child not reaped")
        response["reaped"] = True
        return response
    finally:
        child.close()
        parent.close()
        if process.pid is not None:
            if process.is_alive():
                process.terminate()
                process.join(3)
            if process.is_alive():
                process.kill()
                process.join(3)
            if not process.is_alive():
                process.close()


def probe_committed_vectors(root: Path) -> dict:
    """100 actual Chroma writes and receipts; two fresh snapshot readers."""
    from kernel.memory_pipeline_models import PipelineScope
    from apps.system_memory_god.storage.daily_vector_index import DailyVectorIndex
    from apps.system_memory_god.storage.scoped_stores import ScopedStores

    root = root.resolve()
    if not root.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError("probe requires a system TEMP directory")
    scope = PipelineScope("r9-probe", "isolated", "probe", "committed")
    vectors = np.random.default_rng(902).normal(size=(100, 16)).astype("<f4")
    ids = [f"probe-{row:04}" for row in range(100)]
    records = [{"memory_id": key, "payload": {"text": str(row)},
                "payload_hash": _digest(str(row).encode())}
               for row, key in enumerate(ids)]
    index = DailyVectorIndex(ScopedStores(root / "derived"),
                             lambda texts: [vectors[int(text)].tolist() for text in texts])
    asyncio.run(index.put(scope, records))
    receipts = asyncio.run(index.receipts(scope, records))
    # Investigation-only export from that same live owner, never a second client.
    collection = index._collections[scope.partition_id]
    checked = collection.get(ids=ids, include=["documents", "metadatas", "embeddings"])
    stored = {key: (text, metadata, vector) for key, text, metadata, vector in zip(
        checked["ids"], checked["documents"], checked["metadatas"], checked["embeddings"])}
    normalized_rows = 0
    for record, receipt in zip(records, receipts):
        text, metadata, vector = stored[record["memory_id"]]
        if (text != record["payload"]["text"]
                or metadata != {"payload_hash": record["payload_hash"]}
                or receipt != {"memory_id": record["memory_id"],
                               "payload_hash": record["payload_hash"],
                               "text_hash": _digest(text.encode()), "embedding_dim": 16}):
            raise ValueError("original persisted receipt disagrees")
        persisted = np.asarray(vector, dtype="<f4")
        original = vectors[int(text)]
        cosine = np.dot(persisted, original) / (
            np.linalg.norm(persisted) * np.linalg.norm(original))
        if not np.isfinite(cosine) or cosine < 1 - 1e-6:
            raise ValueError("persisted vector direction changed")
        normalized_rows += int(not np.array_equal(persisted, original))
    matrix = np.asarray([stored[key][2] for key in ids], dtype="<f4")
    manifest = _save_numbers(root / "snapshot", ids, matrix)
    reopened = np.frombuffer((root / "snapshot" / "vectors.f32").read_bytes(),
                             dtype="<f4").reshape(matrix.shape)
    if not np.array_equal(reopened, matrix):
        raise ValueError("persisted snapshot vector changed")
    first = _spawn_query(root / "snapshot", manifest, matrix[17].tolist())
    second = _spawn_query(root / "snapshot", manifest, matrix[17].tolist())
    if first["hits"][0][0] != ids[17] or first["hits"] != second["hits"]:
        raise ValueError("snapshot restart query disagrees")
    return {"verified_rows": len(receipts), "top_k": len(first["hits"]),
            "snapshot_fields": ["ids", "vectors"],
            "reopened_same_hits": first["hits"] == second["hits"],
            "query_children_reaped": first["reaped"] and second["reaped"],
            "read_query_ms": [first["read_query_ms"], second["read_query_ms"]],
            "snapshot_bytes": manifest["bytes"], "chroma_normalized_rows": normalized_rows,
            "vector_sha256": manifest["vector_sha256"]}


def _capacity_child(connection: Any, entries: list[tuple[str, dict]]) -> None:
    """Prepare eight numeric caches, then serve bounded measurement requests."""
    try:
        prepared = []
        for root, manifest in entries:
            body = (Path(root) / "vectors.f32").read_bytes()
            directory = (Path(root) / "ids.json").read_bytes()
            if (_digest(body) != manifest["vector_sha256"]
                    or _digest(directory) != manifest["ids_sha256"]):
                raise ValueError("capacity snapshot hash mismatch")
            matrix = np.frombuffer(body, dtype="<f4").reshape(
                manifest["rows"], manifest["dimension"])
            norms = np.sqrt(np.einsum("ij,ij->i", matrix, matrix, dtype=np.float64))
            if (not np.isfinite(matrix).all() or not np.isfinite(norms).all()
                    or (norms <= 0).any()):
                raise ValueError("invalid capacity vectors")
            prepared.append((matrix, norms, json.loads(directory)))
        connection.send_bytes(b'{"ready":true}')
        while True:
            request = json.loads(connection.recv_bytes(128 * 1024))
            if request == {"close": True}:
                break
            matrix, norms, ids = prepared[request["scope"]]
            query = np.asarray(request["query"], dtype=np.float64)
            scores = np.einsum("ij,j->i", matrix, query, dtype=np.float64) / (
                norms * np.linalg.norm(query))
            order = sorted(range(len(ids)), key=lambda row: (-scores[row], ids[row]))[:100]
            connection.send_bytes(json.dumps([ids[row] for row in order]).encode())
    except EOFError:
        pass
    except Exception as exc:
        connection.send_bytes(json.dumps({"error": type(exc).__name__}).encode())
    finally:
        connection.close()


def probe_capacity(root: Path) -> dict:
    """Fixed 8-scope, 2048-dimension capacity profiles; no model or Chroma."""
    root = root.resolve()
    if not root.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError("probe requires a system TEMP directory")
    cases = []
    total_bytes = 0
    for rows in (100, 1000, 5000):
        entries, queries, publishing_ms = [], [], []
        for scope in range(8):
            vectors = np.random.default_rng(902 + scope).normal(
                size=(rows, 2048)).astype("<f4")
            ids = [f"scope-{scope}-row-{row:04}" for row in range(rows)]
            target = root / f"capacity-{rows}-{scope}"
            start = time.perf_counter()
            manifest = _save_numbers(target, ids, vectors)
            publishing_ms.append((time.perf_counter() - start) * 1000)
            total_bytes += manifest["bytes"]
            entries.append((str(target), manifest))
            queries.append(vectors[17].tolist())
        del vectors
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=_capacity_child, args=(child, entries))
        latencies = []
        start = time.perf_counter()
        try:
            process.start()
            child.close()
            if not parent.poll(15) or json.loads(parent.recv_bytes()) != {"ready": True}:
                raise RuntimeError("capacity reader preparation failed")
            ready_ms = (time.perf_counter() - start) * 1000
            for repeat in range(3):
                for scope, query in enumerate(queries):
                    request = json.dumps({"scope": scope, "query": query}).encode()
                    start = time.perf_counter()
                    parent.send_bytes(request)
                    if not parent.poll(2):
                        raise TimeoutError("capacity query budget exceeded")
                    hits = json.loads(parent.recv_bytes(128 * 1024))
                    latency = (time.perf_counter() - start) * 1000
                    if len(hits) != 100 or hits[0] != f"scope-{scope}-row-0017":
                        raise ValueError("capacity scope or TopK changed")
                    if repeat:
                        latencies.append(latency)
            parent.send_bytes(b'{"close":true}')
            process.join(3)
            if process.is_alive() or process.exitcode != 0:
                raise RuntimeError("capacity child not reaped")
            cases.append({"rows_per_scope": rows, "scopes": 8, "dimension": 2048,
                          "top_k": 100, "publish_max_ms": max(publishing_ms),
                          "query_p95_ms": float(np.percentile(latencies, 95)),
                          "query_max_ms": max(latencies), "ready_ms": ready_ms})
        finally:
            child.close()
            parent.close()
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                    process.join(3)
                if process.is_alive():
                    process.kill()
                    process.join(3)
                if not process.is_alive():
                    process.close()
    return {"cases": cases, "total_bytes": total_bytes, "query_children_reaped": True,
            "go": (total_bytes <= 768 * 1024**2
                   and all(case["publish_max_ms"] < 2000
                           and case["query_max_ms"] < 150 for case in cases))}


async def probe_model_preflight(root: Path, *, early: bool = False, synthetic: bool = False,
                                work_deadline: float | None = None,
                                cleanup_deadline: float | None = None,
                                fault: str | None = None, permit=None) -> dict:
    """Finite CPU latency/resource preflight, not answer accuracy or API scoring."""
    import psutil
    from kernel.memory_pipeline_models import PipelineScope
    from apps.system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from apps.system_memory_god.storage.scoped_stores import ScopedStores
    from apps.system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector
    from apps.system_memory_god.storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot

    if type(early) is not bool or type(synthetic) is not bool:
        raise ValueError("invalid preflight profile")
    if not synthetic:
        from Tools.daily_semantic_probe_guard import WorkerPermit
        if type(permit) is not WorkerPermit or not early:
            raise ValueError("real preflight requires the fixed Job controller")
        permit.begin_model(root, work_deadline, cleanup_deadline)
    if fault not in {None, "publish-stall", "cache-close-stall", "reader-close-stall"} or fault and not synthetic:
        raise ValueError("invalid synthetic fault")
    if _PREFLIGHT_OWNERS:
        raise RuntimeError("previous preflight resources unsettled")
    work_deadline = time.monotonic() + 120 if work_deadline is None else work_deadline
    root = root.resolve()
    if not root.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError("model preflight requires system TEMP")
    report: dict = {"profile": "feasibility-early" if early else "model-preflight", "available_bytes": None,
                    "api_calls": 0, "local_embedding_samples_reserved": 0,
                    "peak_children_rss_bytes": 0, "latencies_ms": [],
                    "queries_completed": 0, "blocked": None, "go": False,
                    "started_roles": [], "cleanup_failures": [], "stage": "preflight",
                    "role_rss_bytes": {}, "stage_ms": {}, "child_pids": [],
                    "cache_removed": False, "admissions": [], "query_diagnostics": [],
                    "failed_query_ms": None, "file_work_settled": True, "report_io_ms": 0.0,
                    "cleanup_status": dict.fromkeys(("reader", "writer", "monitor", "cache"), "not_attempted")}
    def checkpoint() -> None:
        if permit is not None:
            from Tools.daily_semantic_probe_guard import write_report
            began = time.perf_counter()
            try:
                write_report(permit.owner, "stage.json", os.environ["AOAO_DAILY_PROBE_PROFILE"],
                             os.environ["AOAO_DAILY_PROBE_TOKEN"], report)
            finally:
                report["report_io_ms"] += (time.perf_counter() - began) * 1000
    def admit(role: str) -> bool:
        try:
            available = psutil.virtual_memory().available
            if type(available) is not int or available < 0:
                raise ValueError("invalid available memory")
        except (psutil.Error, OSError, ValueError):
            report["admissions"].append({"role": role, "available_bytes": None, "allowed": False})
            report["blocked"] = report["blocked"] or "resource_measurement_failed"
            return False
        allowed = available >= 8 * 1024**3
        report["admissions"].append({"role": role, "available_bytes": available, "allowed": allowed})
        if role == "writer":
            report["available_bytes"] = available
        if not allowed:
            report["blocked"] = report["blocked"] or "insufficient_available_memory"
        return allowed
    if not admit("writer"):
        report["query_children_reaped"] = True
        checkpoint()
        return report
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    scope = PipelineScope("r9-model-preflight", "isolated", "probe", "latency")
    embedding = "synthetic" if synthetic else "bge-m3"
    vector = SupervisedDailyVector(ScopedStores(root / "derived"), embedding=embedding,
                                   request_timeout=30, enable_snapshot=True)
    cache = DailyVectorSnapshot(root / "cache", (scope,), embedding=embedding)
    reader = SupervisedDailyVectorSnapshot(cache, diagnostics=True, auto_recover=False, query_timeout=.15)
    texts = ["【隔离合成】小岚喜欢喝茉莉茶，不喜欢红茶。",
             "【隔离合成】小蓝是另一个人，他住在哈尔滨。",
             "【隔离合成】小岚周六计划参观海洋馆。"]
    questions = ["小岚偏爱什么饮品", "小岚喜欢哪一种茶", "小岚不喜欢什么茶",
                 "周末的出游安排是什么", "小蓝居住在哪个城市", "小岚计划去哪里参观",
                 "他周六打算做什么", "茉莉茶是谁喜欢的", "谁住在哈尔滨", "小岚周末去哪儿",
                 "红茶是小岚的喜好吗", "小蓝和小岚是同一个人吗", "海洋馆什么时候去",
                 "小岚的饮料偏好", "小蓝的住址是什么", "有哪些还没完成的计划",
                 "谁打算参观海洋馆", "小岚喜欢咖啡吗", "小蓝喜欢什么饮料", "小岚养宠物吗"]
    if early:
        questions = questions[:4]
    records = [{"memory_id": f"model-{index}", "payload": {"text": text},
                "payload_hash": _digest(text.encode())} for index, text in enumerate(texts)]
    stop = asyncio.Event()
    file_tasks: list[asyncio.Task] = []
    operation_tasks: list[asyncio.Task] = []
    def block(reason):
        if report["blocked"] is None:
            report["blocked"] = reason

    def sample_resources():
        total = 0
        for role, pid in (("writer", vector.worker_pid), ("reader", reader.worker_pid)):
            if pid:
                if pid not in report["child_pids"]:
                    report["child_pids"].append(pid)
                try:
                    rss = psutil.Process(pid).memory_info().rss
                except psutil.NoSuchProcess:
                    continue
                except psutil.Error:
                    block("resource_measurement_failed")
                    stop.set()
                    return
                total += rss
                report["role_rss_bytes"][role] = max(report["role_rss_bytes"].get(role, 0), rss)
        report["peak_children_rss_bytes"] = max(report["peak_children_rss_bytes"], total)
        if total > 6 * 1024**3:
            block("model_memory_capacity_exceeded")
            stop.set()

    async def monitor():
        while not stop.is_set():
            sample_resources()
            await asyncio.sleep(.02)

    async def stage(name, operation, *, reserve=0):
        sample_resources()
        if stop.is_set():
            raise RuntimeError("preflight stopped")
        if report["local_embedding_samples_reserved"] + reserve > (9 if early else 40):
            block("embedding_budget_exhausted")
            raise RuntimeError("preflight stopped")
        report["local_embedding_samples_reserved"] += reserve
        report["stage"] = name
        checkpoint()
        began = time.perf_counter()
        task = asyncio.create_task(operation())
        operation_tasks.append(task)
        if name == "cache_publish":
            file_tasks.append(task)
            report["file_work_settled"] = False
        try:
            return await asyncio.shield(task)
        finally:
            report["stage_ms"][name] = (time.perf_counter() - began) * 1000
            sample_resources()
            if name == "cache_publish":
                report["file_work_settled"] = task.done() and not task.cancelled()
            checkpoint()

    def publish(captured: dict) -> str:
        if fault == "publish-stall":
            with cache._lock:
                time.sleep(60)
        return cache.publish(scope, {key: base64.b64decode(row["snapshot"]["f32"], validate=True)
                                     for key, row in captured.items()})

    async def run_steps():
        report["started_roles"].append("writer")
        await stage("writer_put", lambda: vector.put(scope, records), reserve=1 + len(texts))
        captured = vector.snapshot_vectors(scope)
        await stage("cache_publish", lambda: asyncio.to_thread(publish, captured))
        sample_resources()
        if stop.is_set():
            return
        if not admit("reader"):
            return
        report["started_roles"].append("reader")
        await stage("reader_warm", lambda: reader.warm_existing(scope), reserve=1)
        for number, question in enumerate(questions):
            if report["blocked"]:
                break
            try:
                hits = await stage(f"query_{number}",
                    lambda: reader.search_cosine_hits(scope, question, limit=3), reserve=1)
            except (RuntimeError, TimeoutError, ValueError, OSError):
                report["failed_query_ms"] = report["stage_ms"].get(f"query_{number}")
                diagnostic = reader.diagnostic()
                from Tools.daily_semantic_probe_guard import BLOCKS
                if diagnostic.get("error_code") not in BLOCKS:
                    diagnostic = {"request_id": 0, "stage": "parent",
                        "error_code": "unknown_query_failure", "error_type": "RuntimeError",
                        "embedding_ms": None, "numerical_ms": None, "queue_ms": None,
                        "roundtrip_ms": None, "complete": False}
                report["query_diagnostics"].append(diagnostic)
                block(diagnostic.get("error_code", "unknown_query_failure"))
                report["failed_query_index"] = number
                break
            report["latencies_ms"].append(report["stage_ms"][f"query_{number}"])
            report["query_diagnostics"].append(reader.diagnostic())
            if report["latencies_ms"][-1] > 150:
                block("semantic_query_deadline_exceeded")
                break
            if not hits or any(key not in captured for key, score in hits):
                block("semantic_candidates_invalid")
                break
            report["queries_completed"] += 1

    async def close_resource(role: str, resource: Any) -> None:
        report["cleanup_status"][role] = "running"
        checkpoint()
        try:
            if role == "reader" and fault == "reader-close-stall":
                await asyncio.sleep(60)
            await resource.close()
        except (RuntimeError, ValueError, OSError, TimeoutError):
            report["cleanup_status"][role] = "failed"
            report["cleanup_failures"].append(role)
            block("cleanup_failed")
        else:
            report["cleanup_status"][role] = "settled"
        checkpoint()

    def close_cache() -> None:
        if fault == "cache-close-stall":
            with cache._lock:
                time.sleep(60)
        cache.close()

    async def cleanup() -> None:
        settlement = min(time.monotonic() + 5, cleanup_deadline or float("inf"))
        # Create both owners before waiting on either. Waiting cancellation is
        # not cancellation/completion of the actual close or file executor.
        closing = {role: asyncio.create_task(close_resource(role, resource))
                   for role, resource in (("reader", reader), ("writer", vector))}
        stop.set()
        if not work.done():
            work.cancel()
        pending = [work, watching, *closing.values(), *operation_tasks, *file_tasks]
        await asyncio.wait(pending, timeout=max(0, settlement - time.monotonic()))
        for role, task in closing.items():
            if not task.done():
                if report["cleanup_status"][role] == "running":
                    report["cleanup_status"][role] = "unsettled"
                block("cleanup_unsettled")
        if watching.done() and not watching.cancelled() and watching.exception() is None:
            report["cleanup_status"]["monitor"] = "settled"
        else:
            report["cleanup_status"]["monitor"] = "unsettled"
            block("resource_measurement_failed")
        report["query_children_reaped"] = reader.worker_pid is None and vector.worker_pid is None
        report["file_work_settled"] = all(task.done() and not task.cancelled() for task in file_tasks)
        operations_settled = all(task.done() and not task.cancelled() for task in operation_tasks)
        if not work.done() or not report["file_work_settled"] or not operations_settled:
            block("file_work_unsettled")
        if not report["query_children_reaped"]:
            block("child_retirement_incomplete")
        if (work.done() and operations_settled and report["file_work_settled"] and report["query_children_reaped"]
                and all(task.done() for task in closing.values()) and time.monotonic() < settlement):
            report["cleanup_status"]["cache"] = "running"
            checkpoint()
            cache_task = asyncio.create_task(asyncio.to_thread(close_cache))
            file_tasks.append(cache_task)
            await asyncio.wait((cache_task,), timeout=max(0, settlement - time.monotonic()))
            if not cache_task.done():
                report["cleanup_status"]["cache"] = "unsettled"
                block("cleanup_unsettled")
            elif cache_task.exception() is not None:
                report["cleanup_status"]["cache"] = "failed"
                report["cleanup_failures"].append("cache")
                block("cleanup_failed")
            else:
                report["cleanup_status"]["cache"] = "settled"
                report["cache_removed"] = True
        report["file_work_settled"] = all(task.done() and not task.cancelled() for task in file_tasks)
        pending = [task for task in [work, watching, *closing.values(), *operation_tasks, *file_tasks] if not task.done()]
        if pending:
            _PREFLIGHT_OWNERS.append((cache, reader, vector, pending))
        for task in [work, watching, *closing.values(), *operation_tasks, *file_tasks]:
            if task.done() and not task.cancelled():
                task.exception()
        checkpoint()

    watching = asyncio.create_task(monitor())
    report["cleanup_status"]["monitor"] = "running"
    work = asyncio.create_task(run_steps())
    cancelled = False
    try:
        done, _ = await asyncio.wait((work, watching), timeout=max(0, work_deadline - time.monotonic()),
                                     return_when=asyncio.FIRST_COMPLETED)
        if work not in done:
            block("preflight_deadline_exceeded" if watching not in done else "resource_monitor_stopped")
            work.cancel()
        if work in done:
            await work
    except asyncio.CancelledError:
        cancelled = True
        block("parent_cancelled")
    except (RuntimeError, ValueError, OSError, TimeoutError) as exc:
        block("model_preparation_failed")
        report["error_type"] = type(exc).__name__
        # Closed diagnostic labels only; never copy an arbitrary exception body.
        known = {"vector operation failed", "vector write receipt mismatch",
                 "vector operation rejected", "snapshot reader not prepared"}
        report["error_code"] = str(exc) if str(exc) in known else "preparation_error"
    finally:
        closing_owner = asyncio.create_task(cleanup())
        while not closing_owner.done():
            try:
                await asyncio.shield(closing_owner)
            except asyncio.CancelledError:
                cancelled = True
                block("parent_cancelled")
        closing_owner.result()
    if cancelled:
        raise asyncio.CancelledError
    report["go"] = report["blocked"] is None and report["queries_completed"] == len(questions)
    return report


def safety_selftest() -> dict:
    """Actual system TEMP capability check, using numerical synthetic data only."""
    from kernel.memory_pipeline_models import PipelineScope
    from apps.system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    directory = Path(tempfile.mkdtemp(prefix="aoao-r10-v03-safety-"))
    scope = PipelineScope("safety", "isolated", "probe", "cache")
    cache = DailyVectorSnapshot(directory / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    cache.publish(scope, {"two": np.asarray([0, 1], dtype="<f4").tobytes()})
    cache.close()
    from Tools.daily_semantic_probe_guard import run_guarded
    tree = run_guarded("synthetic-tree")
    if not tree["tree_empty"] or not tree["forced"]:
        raise RuntimeError("synthetic Job tree barrier failed")
    import psutil
    from Tools.daily_semantic_probe_guard import parent_death_selftest
    checks = {name: run_guarded(name) for name in (
        "synthetic", "synthetic-publish-stall", "synthetic-cache-close-stall", "synthetic-reader-close-stall")}
    if not checks["synthetic"]["go"]:
        raise RuntimeError("synthetic normal completion failed")
    for name, result in checks.items():
        if not result["tree_empty"] or not result["handles_closed"]:
            raise RuntimeError("synthetic tree did not settle")
        if name.endswith("-stall") and (result["go"] or not result["forced"]):
            raise RuntimeError("forced synthetic stop misreported")
        evidence = result.get("report") or result.get("last_complete_stage", {})
        if any(psutil.pid_exists(pid) for pid in evidence.get("child_pids", [])):
            raise RuntimeError("synthetic descendant still alive")
    if any(psutil.pid_exists(pid) for pid in tree["last_complete_stage"]["child_pids"]):
        raise RuntimeError("synthetic nested descendant still alive")
    return {"cache_removed": not cache.root.exists(), "temp_root": str(directory),
            "tree": tree, "checks": checks, "parent_death": parent_death_selftest(), "api_calls": 0}


def _safety_descendant() -> None:
    time.sleep(60)


def _safety_tree(root: str) -> None:
    child = multiprocessing.get_context("spawn").Process(target=_safety_descendant)
    child.start()
    from Tools.daily_semantic_probe_guard import write_report
    from apps.system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory
    with OwnedDirectory(Path(root)) as directory:
        write_report(directory, "stage.json", "synthetic-tree", os.environ["AOAO_DAILY_PROBE_TOKEN"],
                     {"stage": "synthetic_tree", "child_pids": [os.getpid(), child.pid]})
        time.sleep(60)


def _guard_worker(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise ValueError("invalid fixed worker arguments")
    profile, root_text, work, cleanup = arguments
    from Tools.daily_semantic_probe_guard import PROFILES
    if profile not in PROFILES:
        raise ValueError("invalid fixed worker profile")
    root = Path(root_text)
    if (not root.is_absolute() or not root.is_relative_to(Path(tempfile.gettempdir()).resolve())
            or not (root.name.startswith("aoao-r10-v03-guard-") or profile == "synthetic-tree"
                    and root.name.startswith("aoao-r10-v03-parent-death-"))):
        raise ValueError("fixed probe requires system TEMP")
    from apps.system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory
    with OwnedDirectory(root) as directory:
        _run_permitted_worker(directory, profile, float(work), float(cleanup))


def _run_permitted_worker(directory, profile: str, work: float, cleanup: float) -> None:
    from Tools.daily_semantic_probe_guard import accept_worker, write_report, ADMISSION_CODES
    try:
        permit = accept_worker(directory, profile, work, cleanup)
    except (OSError, ValueError, RuntimeError) as error:
        code = str(error) if type(error) is ValueError and str(error) in ADMISSION_CODES else "worker_admission_failed"
        admission = {"schema": 1, "error_code": code}
        directory.publish("worker-admission.json", json.dumps(admission).encode())
        raise
    root = directory.root
    if profile == "synthetic-tree":
        child = multiprocessing.get_context("spawn").Process(target=_safety_tree, args=(str(root),))
        child.start()
        time.sleep(60)
        return
    if profile != "feasibility-early":
        import psutil
        from types import SimpleNamespace
        # Fixed synthetic fixture only; the real profile always reads actual RAM.
        psutil.virtual_memory = lambda: SimpleNamespace(available=16 * 1024**3)
    fault = profile.removeprefix("synthetic-") if profile.endswith("-stall") else None
    report = asyncio.run(probe_model_preflight(root, early=True, synthetic=profile != "feasibility-early",
        work_deadline=work, cleanup_deadline=cleanup, fault=fault, permit=permit))
    write_report(directory, "report.json", profile, os.environ["AOAO_DAILY_PROBE_TOKEN"], report)


def main() -> None:
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "--safety-owner":
        from Tools.daily_semantic_probe_guard import safety_owner
        safety_owner(Path(sys.argv[2]))
        return
    if len(sys.argv) > 1 and sys.argv[1] == "--guard-worker":
        _guard_worker(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["committed", "capacity", "model-preflight", "feasibility-early", "safety-selftest"], default="committed")
    args = parser.parse_args()
    if args.profile == "safety-selftest":
        print(json.dumps(safety_selftest(), sort_keys=True))
        return
    if args.profile in {"model-preflight", "feasibility-early"}:
        if args.profile != "feasibility-early":
            raise ValueError("only the approved four-query real profile is enabled")
        from Tools.daily_semantic_probe_guard import run_guarded
        report = run_guarded("feasibility-early")
        print(json.dumps(report, sort_keys=True))
        return
    with tempfile.TemporaryDirectory(prefix="aoao-r9-probe-") as directory:
        if args.profile == "committed":
            report = probe_committed_vectors(Path(directory))
        elif args.profile == "capacity":
            report = probe_capacity(Path(directory))
        print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    main()

