"""One private synthetic batch gate; no official requests or HTTP protocol claims."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import sqlite3
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT), str(PROJECT_ROOT / "apps")]


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _source_manifest() -> dict:
    paths = sorted(path for base in (PROJECT_ROOT / "kernel", PROJECT_ROOT / "apps",
                                    PROJECT_ROOT / "protocols", PROJECT_ROOT / "Tools")
                   for path in base.rglob("*.py"))
    entries = {path.relative_to(PROJECT_ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
               for path in paths}
    return {"count": len(entries), "sha256": hashlib.sha256(json.dumps(
        entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}


def _stored_counts(root: Path) -> dict:
    def read(path: Path):
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        connection.execute("PRAGMA query_only=ON")
        return connection

    connection = read(root / "challenge.sqlite3")
    try:
        rows = connection.execute(
            "SELECT user_key,message_count,progress,complete FROM challenge_requests").fetchall()
    finally:
        connection.close()
    if len(rows) != 1:
        raise ValueError("one synthetic request required")
    key, count, progress, complete = rows[0]
    connection = read(root / "users" / key / "raw" / "messages.db")
    try:
        groups = connection.execute("SELECT target_domain,status,count(*) "
                                    "FROM derivation_outbox GROUP BY target_domain,status").fetchall()
        retries = connection.execute("SELECT count(*) FROM derivation_events "
                                     "WHERE action='retry'").fetchone()[0]
    finally:
        connection.close()
    return {"message_count": count, "progress": progress, "complete": bool(complete),
            "derivations": [{"domain": domain, "state": state, "count": amount}
                            for domain, state, amount in groups], "retry_events": retries}


class _Samples:
    def __init__(self):
        self.samples = 0
        self.peak_rss = 0
        self.min_available = None
        self.error_type = None
        self._observed = {}
        self.stop_new_work = asyncio.Event()
        self.stop_reason = None

    def capture(self) -> None:
        import psutil

        process = psutil.Process()
        try:
            children = process.children(recursive=True)
            rss = process.memory_info().rss
            for child in children:
                try:
                    self._observed[child.pid] = child.create_time()
                    rss += child.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            available = psutil.virtual_memory().available
            self.samples += 1
            self.peak_rss = max(self.peak_rss, rss)
            self.min_available = (available if self.min_available is None
                                  else min(self.min_available, available))
            if available < 2 * 1024**3:
                self.stop_reason = "sampled_available_below_2gib"
            elif rss > 6 * 1024**3:
                self.stop_reason = "sampled_runner_rss_above_6gib"
        except psutil.Error as error:
            self.error_type = type(error).__name__
            self.stop_reason = "sample_unavailable"
        if self.stop_reason is not None:
            self.stop_new_work.set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.capture()
            try:
                await asyncio.wait_for(stop.wait(), timeout=.5)
            except asyncio.TimeoutError:
                pass

    def summary(self) -> dict:
        return {"scope": "sampled runner and descendants; no hard memory bound",
                "samples": self.samples, "peak_rss_bytes": self.peak_rss,
                "min_machine_available_bytes": self.min_available, "error_type": self.error_type,
                "stop_reason": self.stop_reason}

    def require_admission(self) -> None:
        if self.stop_new_work.is_set():
            raise _SampledResourceStop("no new work after sampled resource stop")

    def remaining_processes(self) -> dict:
        import psutil

        live = unknown = 0
        for pid, created in self._observed.items():
            try:
                process = psutil.Process(pid)
                live += process.create_time() == created and process.is_running()
            except psutil.NoSuchProcess:
                pass
            except psutil.Error:
                unknown += 1
        return {"observed_count": len(self._observed), "still_live_count": live,
                "unknown_count": unknown, "scope": "only observed runner descendants"}


class _RuntimeSetupOwner:
    """Keep the store owned if job coordinator construction fails partway."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.store = None

    async def close(self) -> None:
        await self.runtime.close()
        if self.store is not None:
            self.store.close()
            self.store = None


class _SampledResourceStop(RuntimeError):
    pass


async def _observe_job(jobs, job_id: str, timeout: float, samples: _Samples):
    terminal = asyncio.create_task(jobs.await_terminal(job_id, timeout=timeout))
    stopped = asyncio.create_task(samples.stop_new_work.wait())
    try:
        await asyncio.wait((terminal, stopped), return_when=asyncio.FIRST_COMPLETED)
        samples.require_admission()
        return terminal.result()
    finally:
        # Only the observation is cancelled. The coordinator retains the job owner
        # and finally stops admission and waits for its actual settlement.
        terminal.cancel()
        stopped.cancel()
        await asyncio.gather(terminal, stopped, return_exceptions=True)


async def _run(args) -> dict:
    from openai import AsyncOpenAI
    from Tools.serve_memory_challenge import _DataRootLease, _StartupOwner, _api_settings, _prepare_root
    from system_memory_god.challenge_job_store import ChallengeJobStore
    from system_memory_god.challenge_jobs import ChallengeJobs
    from system_memory_god.challenge_lanes import ChallengeLanes
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner

    if args.data_root.exists():
        raise ValueError("the one real gate requires a fresh state directory")
    root = args.data_root
    lease = _DataRootLease.acquire(root)
    owner = _StartupOwner(lease)
    report: dict[str, Any] = {"scope": "internal durable job; not HTTP, official Smoke or Full",
                              "started_utc": _utc(), "passed": False}
    started = time.monotonic()
    stop = asyncio.Event()
    samples = _Samples()
    sampler = None
    jobs = None
    try:
        _prepare_root(root)
        report["source_before"] = _source_manifest()
        samples.capture()
        samples.require_admission()
        sampler = asyncio.create_task(samples.run(stop))
        api = _api_settings(args.api_config)
        owner.client = AsyncOpenAI(api_key=api["api_key"], base_url=api["base_url"],
                                   max_retries=0, timeout=60.0)
        owner.embedding_owner = SharedEmbeddingOwner(max_generations=2)
        await owner.embedding_owner.start(time.monotonic_ns() + 120_000_000_000)
        samples.require_admission()
        runtime = ChallengeLanes(root, client=owner.client, model=api["model_name"],
            embedding_owner=owner.embedding_owner, add_timeout=100.0)
        setup = _RuntimeSetupOwner(runtime)
        owner.runtime = setup
        store = ChallengeJobStore(root / "jobs", model=api["model_name"], runtime_lease=lease)
        setup.store = store
        jobs = ChallengeJobs(runtime, store, runtime_lease=lease, job_timeout=args.job_seconds)
        owner.runtime = jobs
        setup.store = None
        report["startup_seconds"] = round(time.monotonic() - started, 6)
        nonce = secrets.token_hex(8)
        markers = [f"sample{nonce}item{index:02d}" for index in range(20)]
        messages = []
        for index, marker in enumerate(markers):
            prefix = f"我在这次虚构收藏记录中把第{index + 1}号物品命名为{marker}，它是一枚蓝色书签，存放在书桌左侧抽屉。"
            content = (prefix + "这是一条合成记忆，只描述这枚书签的名称、颜色与位置，内容需要完整保留。" * 6)[:164]
            if len(content) != 164 or marker not in content:
                raise ValueError("invalid synthetic fixture")
            messages.append({"role": "user", "content": content})
        values = {"user_id": "batch-user-" + nonce, "session_id": "batch-session-" + nonce,
                  "request_id": "batch-request-" + nonce, "messages": messages}
        began = time.monotonic()
        samples.require_admission()
        receipt = await jobs.submit_add(**values)
        report["accepted_seconds"] = round(time.monotonic() - began, 6)
        terminal = await _observe_job(jobs, receipt.job_id, args.job_seconds + 10, samples)
        report["job_seconds"] = round(time.monotonic() - began, 6)
        report["job_state"], report["job_error_code"] = terminal.state, terminal.error_code
        report["journal"] = await asyncio.to_thread(_stored_counts, root)
        if terminal.state != "SUCCEEDED":
            raise RuntimeError("synthetic batch did not succeed")
        expected = {"success": True, "request_id": values["request_id"],
                    "user_id": values["user_id"], "session_id": values["session_id"]}
        report["result_valid"] = terminal.result == expected
        report["search"] = []
        for marker in (markers[0], markers[-1]):
            samples.require_admission()
            began_search = time.monotonic()
            results = await jobs.search(user_id=values["user_id"], query=marker, top_k=20)
            report["search"].append({"seconds": round(time.monotonic() - began_search, 6),
                "marker_found": any(marker in item.get("content", "") for item in results)})
        await runtime.wait_idle()
        report["owner_idle_observed"] = True
        journal = report["journal"]
        report["counts_valid"] = (journal["message_count"] == journal["progress"] == 20
            and journal["complete"] and len(journal["derivations"]) == 6
            and all(item["state"] == "succeeded" and item["count"] == 20
                    for item in journal["derivations"]))
        report["passed"] = (report["counts_valid"] and report["result_valid"]
                            and all(item["marker_found"] for item in report["search"]))
    except Exception as error:
        report["error_type"] = type(error).__name__
    finally:
        shared = owner.embedding_owner
        try:
            await owner.close()
            report["closed"] = True
        except BaseException as error:
            report["close_error_type"] = type(error).__name__
            report["closed"] = False
            report["passed"] = False
        if shared is not None:
            raw_stats = shared.stats
            report["shared"] = {key: raw_stats.get(key) for key in (
                "load_count", "generation_count", "model_owner_count", "model_owner_peak",
                "active_generation_count", "active_generation_peak", "ipc_inflight", "ipc_peak",
                "clean_close", "failed")}
            report["passed"] &= (raw_stats.get("load_count") == 1
                and raw_stats.get("model_owner_count") == raw_stats.get("active_generation_count")
                == raw_stats.get("ipc_inflight") == 0
                and raw_stats.get("model_owner_peak") == 1
                and raw_stats.get("active_generation_peak", 3) <= 2
                and raw_stats.get("ipc_peak", 3) <= 2
                and raw_stats.get("clean_close") is True and raw_stats.get("failed") is False)
        stop.set()
        if sampler is not None:
            try:
                await sampler
            except Exception as error:
                samples.error_type = type(error).__name__
        report["resources"] = samples.summary()
        report["passed"] &= (samples.samples >= 2 and samples.error_type is None
            and samples.stop_reason is None
            and samples.peak_rss <= 6 * 1024**3 and samples.min_available is not None
            and samples.min_available >= 2 * 1024**3)
        report["processes"] = samples.remaining_processes()
        report["passed"] &= (report["processes"]["observed_count"] > 0
            and report["processes"]["still_live_count"] == report["processes"]["unknown_count"] == 0)
        report["source_after"] = _source_manifest()
        report["passed"] &= report.get("source_before") == report["source_after"]
        report["finished_utc"] = _utc()
        report["runner_seconds"] = round(time.monotonic() - started, 6)
        with (root / "batch-job-report.json").open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--api-config", required=True, type=Path)
    parser.add_argument("--job-seconds", type=float, default=1700.0)
    args = parser.parse_args()
    if (not math.isfinite(args.job_seconds) or not 0 < args.job_seconds <= 1700
            or not args.data_root.is_absolute()):
        raise ValueError("invalid finite gate limits")
    # Each gate owns a separate ordinary Temp root, including every spawned worker.
    temp = args.data_root.with_name(args.data_root.name + "-temp")
    temp.mkdir(exist_ok=False)
    os.environ["TEMP"] = os.environ["TMP"] = str(temp)
    os.environ["HF_HUB_OFFLINE"] = os.environ["TRANSFORMERS_OFFLINE"] = "1"
    report = asyncio.run(_run(args))
    print(json.dumps({key: report.get(key) for key in (
        "scope", "passed", "job_state", "job_error_code", "job_seconds", "runner_seconds",
        "error_type", "close_error_type", "closed")}, ensure_ascii=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
