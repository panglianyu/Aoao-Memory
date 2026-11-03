"""Recover the frozen failed-task set once in a private, quiescent clone only."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _path in (PROJECT_ROOT, PROJECT_ROOT / "apps"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from kernel.challenge_diagnostics import (  # noqa: E402
    DiagnosticSink, check_private_path, ensure_private_directory, open_private_file,
)
from kernel.raw_message_recovery import (  # noqa: E402
    RecoveryCallBudget, RecoveryReceipt, build_manifest, preflight, readonly, recovery_extractor,
)
from Tools.serve_memory_challenge import _api_settings, _DataRootLease  # noqa: E402

_RETAINED: list = []


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="One-shot recovery of the approved failed challenge tasks")
    value.add_argument("--data-root", type=Path, required=True)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--clone-proof-sha256", required=True)
    mode = value.add_mutually_exclusive_group()
    mode.add_argument("--manifest-only", action="store_true")
    mode.add_argument("--inspect-only", action="store_true")
    value.add_argument("--receipt", type=Path)
    value.add_argument("--api-config", type=Path)
    value.add_argument("--diagnostics-file", type=Path)
    return value


def _private_write(path: Path, value: dict) -> None:
    ensure_private_directory(path.parent)
    descriptor = open_private_file(path, exclusive=True)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _load(path: Path) -> dict:
    check_private_path(path)
    if not path.is_file() or path.stat().st_size > 128 * 1024:
        raise ValueError("bounded recovery manifest required")
    # A supervisor holds this manifest against modification throughout recovery.
    # Reading must not request write/DACL access and conflict with that pin.
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    try:
        os.set_inheritable(descriptor, False)
        opened, named = os.fstat(descriptor), path.lstat()
        check_private_path(path)
        if (opened.st_nlink != 1 or opened.st_size > 128 * 1024
                or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
            raise ValueError("stable bounded recovery manifest required")
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            descriptor = -1
            return json.load(stream)
    finally:
        if descriptor != -1:
            os.close(descriptor)


async def _bounded(awaitable, deadline: float):
    if time.monotonic() >= deadline:
        if asyncio.iscoroutine(awaitable):
            awaitable.close()
        raise TimeoutError("recovery owner deadline exceeded")
    task = asyncio.create_task(awaitable)
    done, _ = await asyncio.wait({task}, timeout=max(0, deadline - time.monotonic()))
    if task not in done:
        # Keep the task strongly held. Cancelling does not stop its SQL thread.
        _RETAINED.append(task)
        raise TimeoutError("recovery owner deadline exceeded")
    result = task.result()
    if time.monotonic() > deadline:
        raise TimeoutError("recovery owner deadline exceeded")
    return result


async def _published_or_abort(runtime, service, deadline: float, terminal_event=None):
    """A selected derivation failure ends this run without waiting for its deadline."""
    if terminal_event is None:
        return await runtime._published(service, deadline)
    if terminal_event.is_set():
        raise RuntimeError("recovery derivation stopped")
    published = asyncio.create_task(runtime._published(service, deadline))
    stopped = asyncio.create_task(terminal_event.wait())
    try:
        await asyncio.wait({published, stopped}, return_when=asyncio.FIRST_COMPLETED)
        if terminal_event.is_set():
            raise RuntimeError("recovery derivation stopped")
        await published
        if terminal_event.is_set():
            raise RuntimeError("recovery derivation stopped")
    finally:
        for task in (published, stopped):
            if not task.done():
                task.cancel()
        await asyncio.gather(published, stopped, return_exceptions=True)


def _terminal(root: Path, manifest: dict) -> dict:
    succeeded = failed = changed_attempts = 0
    for group in manifest["groups"]:
        with readonly(root / group["database"]) as db:
            for expected in group["rows"]:
                row = db.execute("SELECT status,attempts FROM derivation_outbox WHERE task_id=?",
                                 (expected["task_id"],)).fetchone()
                succeeded += row["status"] == "succeeded"
                failed += row["status"] == "failed"
                changed_attempts += row["attempts"] == expected["attempts"] + 1
    with readonly(root / "challenge.sqlite3") as db:
        requests = [dict(row) for row in db.execute("SELECT * FROM challenge_requests WHERE complete=0 LIMIT 3")]
    if requests != manifest["incomplete_requests"]:
        raise RuntimeError("recovery changed challenge completion")
    return {"succeeded": succeeded, "failed": failed, "additional_attempts": changed_attempts,
            "original_incomplete": len(requests)}


async def execute(args, manifest: dict, receipt: RecoveryReceipt, started: float, *,
                  budget_factory=None, terminal=None, expected_additional_attempts: int = 18) -> dict:
    if (type(expected_additional_attempts) is not int or expected_additional_attempts not in {15, 18}
            or budget_factory is not None and not callable(budget_factory)
            or terminal is not None and not callable(terminal)):
        raise ValueError("invalid finite recovery execution boundary")
    from openai import AsyncOpenAI
    from system_memory_god.challenge_runtime import ChallengeRuntime
    from system_memory_god.challenge_store import UserBinding
    from system_memory_god.memory_factory import create_memory_service
    from system_memory_god.storage.scoped_stores import ScopedStores
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner
    from kernel.raw_message_service import RawMessageService

    business_deadline, absolute_deadline = started + 540, started + 600
    api = _api_settings(args.api_config)
    if api["model_name"] != manifest["model"]:
        raise ValueError("recovery configured model differs")
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    owners: dict[str, Any] = {"runtime": None, "embedding": None, "client": None, "sink": None}
    _RETAINED.append(owners)
    client: Any = None
    sink: Any = None
    runtime: Any = None
    embedding: Any = None
    budget: Any = None
    terminal_read = terminal or _terminal
    selected = {str(args.data_root / group["database"]):
                {row["task_id"]: row["attempts"] for row in group["rows"]} for group in manifest["groups"]}
    sources = {str(args.data_root / group["database"]):
               {row["task_id"]: row for row in group["rows"]} for group in manifest["groups"]}

    def factory(profile, **options):
        claims = selected.get(str(profile.database))
        if claims is None:
            raise ValueError("unselected recovery profile denied")
        options.pop("client", None)
        options.pop("model", None)
        raw = RawMessageService(profile, selected_claims=claims, selected_sources=sources[str(profile.database)],
                                 on_failure=getattr(budget, "abort", None))
        extractor = recovery_extractor(ScopedStores(profile.derived_root), manifest["model"], budget)
        return create_memory_service(profile, raw=raw, extractor=extractor, **options)

    report: dict[str, Any] = {"state": "failed", "selected": 18, "closed": False, "calls": 0}

    async def work():
        runtime._remaining(business_deadline)
        await sink.start()
        # Receipt authorization precedes every CAS; all DBs must commit before
        # runtime creation can start a worker or a guarded model call.
        for group in manifest["groups"]:
            runtime._remaining(business_deadline)
            await asyncio.to_thread(receipt.commit_group, args.data_root, group)
        if not receipt.all_committed:
            raise RuntimeError("recovery database commits incomplete")
        runtime._remaining(business_deadline)
        await embedding.start(min(int(business_deadline * 1_000_000_000),
                                  time.monotonic_ns() + 120_000_000_000))
        for group in manifest["groups"]:
            binding = UserBinding(group["user_id"], group["user_key"])
            service = await runtime._ensure_service(binding, business_deadline)
            await _published_or_abort(runtime, service, business_deadline, getattr(budget, "terminal_event", None))
        report.update(await asyncio.to_thread(terminal_read, args.data_root, manifest))
        if report["succeeded"] != 18 or report["additional_attempts"] != expected_additional_attempts:
            raise RuntimeError("recovery task set incomplete")

    try:
        client = AsyncOpenAI(base_url=api["base_url"], api_key=api["api_key"], timeout=60, max_retries=0)
        owners["client"] = client
        sink = DiagnosticSink(args.diagnostics_file)
        owners["sink"] = sink
        origin = getattr(client, "base_url", None)
        budget = (budget_factory or RecoveryCallBudget)(manifest, receipt, client, business_deadline,
            **({"provider_base_url": str(origin)} if origin is not None else {}))
        embedding = SharedEmbeddingOwner(max_generations=1)
        owners["embedding"] = embedding
        runtime = ChallengeRuntime(args.data_root, client=client, model=manifest["model"], service_factory=factory,
                                   embedding_owner=embedding)
        owners["runtime"] = runtime
        await _bounded(work(), business_deadline)
        report["state"] = "recovered"
        cleanup_deadline = min(absolute_deadline, time.monotonic() + 60)
    except BaseException:
        cleanup_deadline = min(absolute_deadline, time.monotonic() + 60)
        report["state"] = "unknown" if _RETAINED[-1] is not owners else "failed"
        try:
            report.update(await _bounded(asyncio.to_thread(terminal_read, args.data_root, manifest),
                                         min(cleanup_deadline, time.monotonic() + 2)))
        except BaseException:
            report["state"] = "unknown"

    async def cleanup():
        if runtime is not None:
            await runtime.close()
            if runtime._owns_cache:
                raise RuntimeError("recovery runtime ownership remains")
            owners["runtime"] = None
        if embedding is not None:
            await embedding.close()
            stats = embedding.stats
            if (not stats["clean_close"] or stats["active_generation_count"] != 0
                    or stats["ipc_inflight"] != 0 or stats["model_owner_count"] != 0):
                raise RuntimeError("recovery embedding ownership remains")
            report.update({"embedding_closed": True, "active_generation_count": stats["active_generation_count"],
                           "ipc_inflight": stats["ipc_inflight"], "model_owner_count": stats["model_owner_count"]})
            owners["embedding"] = None
        if client is not None:
            await client.close()
            owners["client"] = None
        if sink is not None:
            await sink.close()
            if sink.settlement_status != "settled":
                raise RuntimeError("recovery diagnostic ownership remains")
            owners["sink"] = None

    try:
        await _bounded(cleanup(), cleanup_deadline)
        if any(value is not None for value in owners.values()):
            raise RuntimeError("recovery resource ownership remains")
        report["closed"] = True
    except BaseException:
        report["state"] = "unknown"
    if report["closed"] and report["state"] != "recovered":
        try:
            report.update(await _bounded(asyncio.to_thread(terminal_read, args.data_root, manifest),
                                         min(cleanup_deadline, time.monotonic() + 2)))
        except BaseException:
            report["state"] = "unknown"
    report["calls"] = budget.calls if budget is not None else 0
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    if (time.monotonic() > absolute_deadline
            or any(isinstance(value, asyncio.Task) and not value.done() for value in _RETAINED)):
        report["state"], report["closed"] = "unknown", False
    try:
        await _bounded(asyncio.to_thread(receipt.append, report["state"], report=dict(report)), absolute_deadline)
    except BaseException:
        report["state"] = "unknown"
        report["closed"] = False
    if report["closed"]:
        _RETAINED.remove(owners)
    return report


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    started = time.monotonic()
    lease = _DataRootLease.acquire(args.data_root)
    retained = False
    receipt = None
    loop = None
    try:
        from kernel.raw_message_clone import validate_clone_proof
        validate_clone_proof(args.data_root, expected_sha256=args.clone_proof_sha256, runtime_lease=lease)
        if args.manifest_only:
            manifest = build_manifest(args.data_root)
            report = preflight(args.data_root, manifest)
            validate_clone_proof(args.data_root, expected_sha256=args.clone_proof_sha256,
                                 manifest=manifest, runtime_lease=lease)
            _private_write(args.manifest, manifest)
            print(json.dumps({"state": "manifest_frozen", **report}, sort_keys=True))
            return 0
        manifest = _load(args.manifest)
        report = preflight(args.data_root, manifest)
        if args.inspect_only:
            validate_clone_proof(args.data_root, expected_sha256=args.clone_proof_sha256,
                                 manifest=manifest, runtime_lease=lease)
            print(json.dumps({"state": "preflight_passed", **report}, sort_keys=True))
            return 0
        if any(value is None for value in (args.receipt, args.api_config, args.diagnostics_file)):
            raise ValueError("explicit recovery output and API paths required")
        for path in (args.receipt, args.diagnostics_file):
            ensure_private_directory(path.parent)
            check_private_path(path)
        # Validate settings before consuming the irreversible authorization.
        if _api_settings(args.api_config)["model_name"] != manifest["model"]:
            raise ValueError("recovery configured model differs")
        validate_clone_proof(args.data_root, expected_sha256=args.clone_proof_sha256,
                             manifest=manifest, runtime_lease=lease)
        receipt = RecoveryReceipt(args.receipt, manifest)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            report = loop.run_until_complete(execute(args, manifest, receipt, started))
        except BaseException:
            if not _RETAINED:
                raise
            report = {"state": "unknown", "closed": False}
        print(json.dumps(report, sort_keys=True), flush=True)
        if not report["closed"]:
            # The external exact-PID supervisor owns the 600s hard stop. Keep
            # the real lease and unresolved tasks alive for its termination.
            retained = True
            _RETAINED.extend((lease, receipt, loop))
            loop.run_forever()
        return 0 if report["state"] == "recovered" else 3
    finally:
        if not retained:
            if loop is not None:
                loop.close()
            if receipt is not None:
                receipt.close()
            lease.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt):
        print('{"state":"rejected","closed":false}', flush=True)
        raise SystemExit(2) from None
