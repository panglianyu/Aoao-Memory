"""Fixed-root, zero-cloud replay of the saved R2 first recall; never formal GO."""

from __future__ import annotations

import json
import asyncio
from contextlib import contextmanager
from dataclasses import asdict, replace
import math
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import sys
import time
import re
import subprocess
import uuid
from typing import Any, TYPE_CHECKING, cast, Iterator

REPO = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    sys.path[:0] = [str(REPO), str(REPO / "apps")]

from kernel.raw_message_service import RawMessageService  # noqa: E402
from Tools.probe_daily_memory_real import QUESTIONS, digest, encoded  # noqa: E402
from kernel.daily_memory_contracts import (  # noqa: E402
    INTERACTIVE_RECALL_TIMEOUT, SNAPSHOT_QUERY_TIMEOUT, RecallPurpose,
)

if TYPE_CHECKING:
    from kernel.app_bus import AppBus
    from kernel.message import Message
    from kernel.memory_pipeline_models import PipelineScope, SourceRevision
    from kernel.raw_message_models import StoredMessage
    from kernel.raw_message_profile import RawMessageProfile
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory

ROOT = REPO / "runtime/upgrade_validation/memory-daily-recall-diagnosis-20261009"
INPUT = REPO / "runtime/upgrade_validation/memory-daily-real-acceptance-r2-20261009"
OLD = REPO / "runtime/upgrade_validation/memory-daily-real-acceptance-20261008"
PLAN = "MEM-DAILY-RECALL-DIAG-v0.8-closure-r2"
PROOF_NAMES = (
    "python-review.md",
    "security-review.md",
    "main-review.md",
    "matrix-finish.json",
    "comparison.json",
)
RETAINED: list[Any] = []
FILES = {
    "apps/system_memory_god/daily_runtime.py",
    "tests/test_daily_memory_runtime.py",
    "Tools/probe_daily_memory_recall_diag.py",
    "tests/test_daily_memory_recall_diag.py",
    "tests/test_daily_memory_early_acceptance.py",
    "tests/test_web_research_process_guard.py",
    "tests/aoao_wechat/test_native_ocr.py",
}
V06_FILES = FILES - {"tests/aoao_wechat/test_native_ocr.py"}
OLD_FILES = V06_FILES - {"tests/test_daily_memory_early_acceptance.py", "tests/test_web_research_process_guard.py"}
EDITED_FILES = {
    "Tools/probe_daily_memory_recall_diag.py", "tests/test_daily_memory_recall_diag.py",
}
WECHAT_TRANSITION = {
    "apps/wechat/linux_runtime.py": (
        "895B7F33B613FED422BB0F2B4140CE0A7F6909D90DA19178A13A8B88E61110BC",
        "FA134BE372EE75FF321A8E484C12C883D11C4F2F5BE9B13C44008E77BB050B68"),
    "apps/wechat/linux_transport.py": (
        "BA980EF0953A878B419096E07EE1C30C456531DF42AC974F1ABA50760AE34E01",
        "E1E54C2FB9DCA7A6EAAEB9FF2EC7CDF2D830BB64AA411D8DE9ED9A662F0962AC"),
}
EXCEPTIONS = {
    "apps/system_memory_god/daily_runtime.py",
    "tests/test_daily_memory_runtime.py",
}
DOMAINS = frozenset({"raw", "entity", "graph", "rule", "vector", "journal", "schedule"})
STATES = frozenset(
    {
        "created",
        "initializing",
        "ready",
        "degraded",
        "recovering",
        "unavailable",
        "stopping",
        "stopped",
        "disabled",
        "preparing",
    }
)
REASONS = frozenset(
    {
        "not_prepared",
        "process_exit",
        "stopped",
        "vector_online_disabled",
        "vector_unavailable",
        "deadline_exceeded",
        "reader_recovering",
        "reader_preparing",
        "reader_unavailable",
        "timeout",
        "protocol_error",
        "raw_unavailable",
        "derived_unavailable",
        "source_unavailable",
        "derived_recovering",
        "startup_incomplete",
        "metadata_unavailable",
        "source_changed",
        "scope_unavailable",
        "registry_unavailable",
    }
)
REASONS = REASONS | {"result_budget_limited", "derived_recovery_incomplete"}
ERROR_TYPES = frozenset(
    {
        "none",
        "ValueError",
        "RuntimeError",
        "OSError",
        "TimeoutError",
        "TypeError",
        "IndexError",
        "MemoryError",
        "OtherError",
        "RawMessageError",
        "CancelledError",
    }
)
DIAG_CODES = frozenset(
    {
        "none",
        "unknown",
        "child_input_rejected",
        "snapshot_preparation_failed",
        "embedding_failed",
        "numerical_failed",
        "parent_deadline",
        "not_prepared",
        "queue_rejected",
        "parent_cancelled",
        "protocol_error",
        "unknown_query_failure",
    }
)
DIAG_CODES = DIAG_CODES | {"queue_deadline", "child_exited", "transport_error"}
TIMEOUT_SOURCES = frozenset(
    {
        "none",
        "queue_deadline",
        "watchdog_deadline",
        "transport_deadline",
        "facade_postcheck",
    }
)


class DiagnosisRejected(RuntimeError):
    """Only constant finite codes are raised by this tool."""


def ordinary_directory(path: Path) -> None:
    if not path.is_absolute() or path.resolve() != path or ".." in path.parts:
        raise DiagnosisRejected("directory_invalid")
    for parent in (path, *path.parents):
        info = parent.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or getattr(info, "st_file_attributes", 0) & 0x400
        ):
            raise DiagnosisRejected("directory_invalid")


def file_hash(root: Path, path: Path) -> tuple[int, str]:
    from kernel.project_workspace_io import open_bound_read_file

    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or getattr(info, "st_file_attributes", 0) & 0x400
        or info.st_size > 64 * 1024**2
    ):
        raise DiagnosisRejected("file_invalid")
    with open_bound_read_file(root, path) as stream:
        body = stream.read(64 * 1024**2 + 1)
        after = os.fstat(stream.fileno())
    if len(body) != info.st_size or (info.st_dev, info.st_ino, info.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_mtime_ns,
    ):
        raise DiagnosisRejected("file_changed")
    return len(body), digest(body).upper()


def input_inventory(root: Path) -> list[dict]:
    ordinary_directory(root)
    result: list[dict] = []
    pending = [root / "isolated-raw", root / "isolated-derived"]
    directories = total = 0
    while pending:
        directory = pending.pop()
        ordinary_directory(directory)
        directories += 1
        if directories > 64:
            raise DiagnosisRejected("input_invalid")
        for entry in directory.iterdir():
            info = entry.lstat()
            if getattr(info, "st_file_attributes", 0) & 0x400 or entry.is_symlink():
                raise DiagnosisRejected("input_invalid")
            if stat.S_ISDIR(info.st_mode):
                pending.append(entry)
            else:
                size, sha256 = file_hash(root, entry)
                total += size
                result.append(
                    {
                        "relative": entry.relative_to(root).as_posix(),
                        "size": size,
                        "sha256": sha256,
                    }
                )
                if len(result) > 64 or total > 64 * 1024**2:
                    raise DiagnosisRejected("input_invalid")
    return sorted(result, key=lambda row: row["relative"])


def prepare_clone(
    owner: OwnedDirectory, group: str, scope: PipelineScope, vector_ids: tuple[str, ...]
) -> dict:
    from kernel.memory_pipeline_models import PipelineScope, identity
    from kernel.project_workspace_io import hold_directory_chain, open_bound_read_file
    from system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory
    from system_memory_god.storage.daily_vector_index import (
        DailyVectorIndex,
        _INDEX_DDL,
    )
    from system_memory_god.storage.pipeline_database import PipelineDatabase
    from system_memory_god.storage.scoped_stores import ScopedStores

    if (
        type(owner) is not OwnedDirectory
        or owner.root != ROOT
        or type(scope) is not PipelineScope
        or type(group) is not str
        or group not in {"baseline", "instrumented"}
        or type(vector_ids) is not tuple
        or not 1 <= len(vector_ids) <= 100
    ):
        raise DiagnosisRejected("clone_boundary_invalid")
    for key in vector_ids:
        identity(key)
    expected = json.loads((ROOT / "proof/input.json").read_bytes().decode("utf-8-sig"))
    expected = sorted(expected, key=lambda row: row["relative"])
    if input_inventory(INPUT) != expected:
        raise DiagnosisRejected("input_changed")
    owner.publish(
        group + "-copy-start.json",
        encoded(
            {
                "plan": PLAN,
                "group": group,
                "input_hash": digest(encoded(expected)),
                "root_identity": owner.identity,
            }
        ),
    )
    clone = ROOT / group
    clone.mkdir()
    with OwnedDirectory(clone):
        for name in ("isolated-raw", "isolated-derived"):
            (clone / name).mkdir()
        for row in expected:
            source, target = INPUT / row["relative"], clone / row["relative"]
            target.parent.mkdir(parents=True, exist_ok=True)
            with (
                hold_directory_chain(INPUT, source.parent),
                hold_directory_chain(clone, target.parent),
            ):
                with open_bound_read_file(INPUT, source) as stream:
                    # A fresh owned copy needs no rename staging or replacement path.
                    with target.open("xb") as output:
                        shutil.copyfileobj(stream, output, 64 * 1024)
                        output.flush()
                        os.fsync(output.fileno())
        if input_inventory(clone) != expected or input_inventory(INPUT) != expected:
            raise DiagnosisRejected("copy_changed")
        partition = clone / "isolated-derived" / scope.partition_id
        source_vector = (
            INPUT / "isolated-derived" / scope.partition_id / "vectors/chroma.sqlite3"
        )
        target_vector = partition / "vectors/chroma.sqlite3"
        source_info, target_info = source_vector.stat(), target_vector.stat()
        old = (str(source_info.st_dev), str(source_info.st_ino))
        new = (str(target_info.st_dev), str(target_info.st_ino))
        authority = PipelineDatabase(
            clone / "isolated-derived", scope, "vector_index.db", _INDEX_DDL, 0x414C5649
        )

        def check(db: sqlite3.Connection) -> None:
            if db.execute("SELECT device,inode FROM vector_identity").fetchall() != [
                old
            ]:
                raise DiagnosisRejected("clone_identity_invalid")

        with hold_directory_chain(clone, target_vector.parent):
            authority.validate_existing(check)
            identity_before = authority.path.stat()
            db = sqlite3.connect(
                authority.path.as_uri() + "?mode=rw", uri=True, isolation_level=None
            )
            try:
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                # Reuse the original exact recognizer, explicitly forbidding initialization.
                authority._recognize(db, allow_initialize=False)
                check(db)
                if (authority.path.stat().st_dev, authority.path.stat().st_ino) != (
                    identity_before.st_dev,
                    identity_before.st_ino,
                ) or (
                    str(target_vector.stat().st_dev),
                    str(target_vector.stat().st_ino),
                ) != new:
                    raise DiagnosisRejected("clone_identity_changed")
                cursor = db.execute(
                    "UPDATE vector_identity SET device=?,inode=? WHERE device=? AND inode=?",
                    (*new, *old),
                )
                if cursor.rowcount != 1:
                    raise DiagnosisRejected("clone_identity_invalid")
                db.commit()
            finally:
                if db.in_transaction:
                    db.rollback()
                db.close()
        after = input_inventory(clone)
        changed = [row["relative"] for row in after if row not in expected]
        identity_relative = authority.path.relative_to(clone).as_posix()
        if (
            changed != [identity_relative]
            or input_inventory(INPUT) != expected
            or {row["relative"] for row in after}
            != {row["relative"] for row in expected}
        ):
            raise DiagnosisRejected("copy_changed")
        DailyVectorIndex.validate_existing(
            ScopedStores(clone / "isolated-derived"), scope, vector_ids
        )
    return {
        "group": group,
        "files": len(expected),
        "bytes": sum(row["size"] for row in expected),
        "input_hash": digest(encoded(expected)),
        "old_identity": old,
        "new_identity": new,
        "identity_restored": True,
        "only_identity_file_changed": True,
    }


def gate_observation(hits: object, elapsed_ms: float) -> dict:
    if (
        type(elapsed_ms) not in {int, float}
        or not math.isfinite(elapsed_ms)
        or elapsed_ms < 0
    ):
        raise DiagnosisRejected("diagnostic_invalid")
    if hits is None:
        empty = True
    elif type(hits) in {list, tuple, dict, set, str, bytes}:
        empty = len(cast(Any, hits)) == 0
    elif type(hits) in {bool, int, float}:
        if type(hits) is float and not math.isfinite(hits):
            raise DiagnosisRejected("diagnostic_invalid")
        empty = hits == 0
    else:
        raise DiagnosisRejected("diagnostic_invalid")
    flags = {
        "return_type_invalid": type(hits) is not list,
        "deadline_exceeded": elapsed_ms > 300,
        "required_evidence_empty": empty,
    }
    counts = {domain: 0 for domain in sorted(DOMAINS)}
    if type(hits) is list:
        if len(hits) > 100 or any(type(hit) is not dict for hit in hits):
            raise DiagnosisRejected("diagnostic_invalid")
        for hit in hits:
            if type(hit.get("domain")) is not str or hit["domain"] not in DOMAINS:
                raise DiagnosisRejected("diagnostic_invalid")
            counts[hit["domain"]] += 1
    return {
        "return_type": "list"
        if type(hits) is list
        else "none"
        if hits is None
        else "other",
        "candidate_count": len(hits) if type(hits) is list else None,
        "domain_counts": counts,
        "elapsed_ms": elapsed_ms,
        "flags": flags,
        "gate_pass": not any(flags.values()),
    }


def finite_health(value: dict) -> dict:
    if type(value) is not dict:
        raise DiagnosisRejected("diagnostic_invalid")
    result: dict[str, Any] = {}
    for key in ("state", "raw_state", "derived_state", "semantic_state"):
        state = value.get(key)
        if state is not None and type(state) is not str:
            raise DiagnosisRejected("diagnostic_invalid")
        result[key] = state if state in STATES else "unknown"
    reasons = value.get("reason_codes", ())
    if type(reasons) not in {tuple, list} or len(reasons) > 16:
        raise DiagnosisRejected("diagnostic_invalid")
    if any(type(code) is not str for code in reasons):
        raise DiagnosisRejected("diagnostic_invalid")
    result["reason_codes"] = [
        code if type(code) is str and code in REASONS else "unknown" for code in reasons
    ]
    return result


def finite_diagnostic(
    value: dict,
    previous: dict,
    *,
    purpose: RecallPurpose = "warm",
    previous_purpose: RecallPurpose = "warm",
    reader_before: tuple[int, float] | None = None,
    reader_after: tuple[int, float] | None = None,
) -> dict:
    if any(type(mode) is not str or mode not in {"warm", "interactive"}
           for mode in (purpose, previous_purpose)):
        raise DiagnosisRejected("diagnostic_invalid")
    if type(value) is not dict or type(previous) is not dict:
        raise DiagnosisRejected("diagnostic_invalid")
    prior_id = previous.get("request_id", 0)
    if type(prior_id) is not int or prior_id < 0:
        raise DiagnosisRejected("diagnostic_invalid")
    allowed_previous = {
        "request_id",
        "stage",
        "error_code",
        "error_type",
        "complete",
        "timeout_source",
        "queue_ms",
        "embedding_ms",
        "numerical_ms",
        "roundtrip_ms",
        "request_elapsed_ms",
        "budget_ms",
    }
    if set(previous) - allowed_previous:
        raise DiagnosisRejected("diagnostic_invalid")
    for key, number in previous.items():
        if (
            key in {"stage", "error_code", "error_type", "timeout_source"}
            and type(number) is not str
        ):
            raise DiagnosisRejected("diagnostic_invalid")
        if key == "complete" and type(number) is not bool:
            raise DiagnosisRejected("diagnostic_invalid")
        if (
            key.endswith("_ms")
            and number is not None
            and (
                type(number) not in {int, float}
                or not math.isfinite(number)
                or not 0 <= number <= 120000
            )
        ):
            raise DiagnosisRejected("diagnostic_invalid")
    if not value:
        return {"association": "unavailable", "values": None}
    timing = {
        "queue_ms",
        "embedding_ms",
        "numerical_ms",
        "roundtrip_ms",
        "request_elapsed_ms",
        "budget_ms",
    }
    required = {"request_id", "stage", "error_code", "error_type", "complete"}
    if any(
        type(value.get(key)) is not str for key in ("stage", "error_code", "error_type")
    ):
        raise DiagnosisRejected("diagnostic_invalid")
    if "timeout_source" in value and type(value["timeout_source"]) is not str:
        raise DiagnosisRejected("diagnostic_invalid")
    if (
        type(value) is not dict
        or not required.issubset(value)
        or set(value) - required - timing - {"timeout_source"}
        or type(value["request_id"]) is not int
        or value["request_id"] < 0
        or type(value["complete"]) is not bool
    ):
        raise DiagnosisRejected("diagnostic_invalid")
    for key in timing:
        number = value.get(key)
        if number is not None and (
            type(number) not in {int, float}
            or not math.isfinite(number)
            or not 0 <= number <= 120000
        ):
            raise DiagnosisRejected("diagnostic_invalid")
    for frame, mode in ((value, purpose), (previous, previous_purpose)):
        ceiling = SNAPSHOT_QUERY_TIMEOUT if mode == "warm" else INTERACTIVE_RECALL_TIMEOUT
        if "budget_ms" in frame and (type(frame["budget_ms"]) not in {int, float}
                or not math.isfinite(frame["budget_ms"])
                or not 0 < frame["budget_ms"] <= ceiling * 1000):
            raise DiagnosisRejected("diagnostic_invalid")
    for reader in (reader_before, reader_after):
        if reader is not None and (
            type(reader) is not tuple
            or len(reader) != 2
            or type(reader[0]) is not int
            or reader[0] <= 0
            or type(reader[1]) is not float
            or not math.isfinite(reader[1])
            or reader[1] <= 0
        ):
            raise DiagnosisRejected("diagnostic_invalid")
    safe = dict(value)
    for key, choices in (
        (
            "stage",
            {
                "queue",
                "parent",
                "input",
                "preparation",
                "embedding",
                "numerical",
                "complete",
            },
        ),
        ("error_code", DIAG_CODES),
        ("error_type", ERROR_TYPES),
        ("timeout_source", TIMEOUT_SOURCES),
    ):
        if key in safe and safe[key] not in choices:
            safe[key] = "unknown"
    query_protocol = (
        "budget_ms" in value
        and "request_elapsed_ms" in value
        and value["request_elapsed_ms"] is not None
        and value["stage"] != "preparation"
    )
    if value == previous and value["request_id"] > 0:
        association = "stale"
    elif (
        query_protocol
        and "unknown"
        not in (
            safe["stage"],
            safe["error_code"],
            safe["error_type"],
            safe.get("timeout_source", "none"),
        )
        and reader_before is not None
        and reader_after == reader_before
        and value["request_id"] > prior_id
    ):
        association = "fresh"
    else:
        association = "inconclusive"
    return {"association": association, "values": safe}


def reader_identity(runtime: DailyMemoryRuntime) -> tuple[int, float] | None:
    import psutil

    pid = runtime.snapshot_reader.worker_pid if runtime.snapshot_reader else None
    if pid is None:
        return None
    try:
        return pid, psutil.Process(pid).create_time()
    except psutil.Error:
        return None


class ObservedRaw(RawMessageService):
    def __init__(self, profile: RawMessageProfile) -> None:
        super().__init__(profile)
        self.reset_observation()

    def reset_observation(self) -> None:
        self.observation: dict[str, Any] = {
            "seen": False,
            "ok": False,
            "elapsed_ms": None,
            "error_type": "none",
        }

    async def validate_turn(
        self, original: Message, *, deadline: float | None = None
    ) -> None:
        began = time.perf_counter()
        self.observation["seen"] = True
        try:
            await super().validate_turn(original, deadline=deadline)
            self.observation["ok"] = True
        except BaseException as exc:
            name = type(exc).__name__
            self.observation["error_type"] = (
                name if name in ERROR_TYPES else "OtherError"
            )
            raise
        finally:
            self.observation["elapsed_ms"] = (time.perf_counter() - began) * 1000


class ForbiddenExtractor:
    def __init__(self) -> None:
        self.calls = 0

    async def extract(
        self,
        domain: str,
        message: StoredMessage,
        source: SourceRevision,
        max_output_tokens: int,
    ) -> tuple[dict, ...]:
        self.calls += 1
        raise DiagnosisRejected("derivation_forbidden")


class QueryBudget:
    def __init__(self, deadline: float):
        if type(deadline) not in {int, float} or not math.isfinite(deadline):
            raise DiagnosisRejected("budget_invalid")
        self.deadline, self.calls = deadline, 0
        self.groups = {"baseline": 0, "instrumented": 0}

    def charge(self, group: str | None = None) -> None:
        if type(self.deadline) not in {int, float} or not math.isfinite(self.deadline):
            raise DiagnosisRejected("budget_invalid")
        if group is not None and (type(group) is not str or group not in self.groups):
            raise DiagnosisRejected("budget_invalid")
        if time.monotonic() >= self.deadline:
            raise DiagnosisRejected("workflow_deadline")
        if self.calls >= 6:
            raise DiagnosisRejected("query_budget_exhausted")
        if group is not None and self.groups[group] >= 3:
            raise DiagnosisRejected("query_budget_exhausted")
        if group is not None:
            self.groups[group] += 1
        self.calls += 1


async def observe_query(
    bus: AppBus,
    raw: ObservedRaw,
    runtime: DailyMemoryRuntime,
    budget: QueryBudget,
    *,
    group: str | None = None,
) -> tuple[dict, object]:
    before = finite_health(runtime.health())
    previous = runtime.snapshot_reader.diagnostic() if runtime.snapshot_reader else {}
    reader_before = reader_identity(runtime)
    raw.reset_observation()
    budget.charge(group)
    remaining_ms = max(0.0, (budget.deadline - time.monotonic()) * 1000)
    began = time.perf_counter()
    call_error = "none"
    try:
        hits = await bus.recall_daily(
            QUESTIONS[0], top_k=10, neighbors=1, deadline=budget.deadline
        )
    except Exception as exc:
        name = type(exc).__name__
        call_error = name if name in ERROR_TYPES else "OtherError"
        hits = None
    finally:
        elapsed_ms = (time.perf_counter() - began) * 1000
    result = gate_observation(hits, elapsed_ms)
    result.update(
        query_hash=digest(QUESTIONS[0].encode()),
        raw_validation=dict(raw.observation),
        before=before,
        after=finite_health(runtime.health()),
        call_error=call_error,
        remaining_work_ms=remaining_ms,
        full_300ms_sample=remaining_ms >= 300,
    )
    current = runtime.snapshot_reader.diagnostic() if runtime.snapshot_reader else {}
    try:
        result["vector_diagnostic"] = finite_diagnostic(
            current,
            previous,
            reader_before=reader_before,
            reader_after=reader_identity(runtime),
        )
    except DiagnosisRejected:
        result["vector_diagnostic"] = {"association": "invalid", "values": None}
    return result, hits


def record(path: Path) -> Any:
    """Read bounded ordinary proof data, never configuration values."""
    from kernel.project_workspace_io import open_bound_read_file

    root = REPO if path.is_relative_to(REPO) else ROOT
    ordinary_directory(path.parent)
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_size > 2 * 1024**2
        or getattr(info, "st_file_attributes", 0) & 0x400
    ):
        raise DiagnosisRejected("proof_invalid")
    with open_bound_read_file(root, path) as stream:
        body = stream.read(2 * 1024**2 + 1)
    try:
        return json.loads(body.decode("utf-8-sig"))
    except (ValueError, UnicodeError):
        raise DiagnosisRejected("proof_invalid") from None


def sealed_evidence(manifest: Path, roots: tuple[Path, ...], *, legacy_size: bool = False) -> Any:
    entries = record(manifest)
    if type(entries) is not list:
        raise DiagnosisRejected("proof_invalid")
    fields = {"path", "sha256", "size"} if legacy_size else {"path", "sha256"}
    expected: dict[str, tuple[int | None, str]] = {}
    for row in entries:
        if (type(row) is not dict or set(row) != fields
                or type(row["path"]) is not str or type(row["sha256"]) is not str
                or not re.fullmatch("[A-F0-9]{64}", row["sha256"]) or row["path"] in expected
                or legacy_size and not finite_integer(row["size"])):
            raise DiagnosisRejected("proof_invalid")
        expected[row["path"]] = (row["size"] if legacy_size else None, row["sha256"])
    actual = set()
    for root in roots:
        ordinary_directory(root)
        for path in root.rglob("*"):
            info = path.lstat()
            if getattr(info, "st_file_attributes", 0) & 0x400 or path.is_symlink():
                raise DiagnosisRejected("protection_failed")
            if stat.S_ISREG(info.st_mode):
                actual.add(str(path))
    if actual != set(expected):
        raise DiagnosisRejected("protection_failed")
    for name, (size, sha256) in expected.items():
        path = Path(name)
        anchor = next((root for root in roots if path.is_relative_to(root)), None)
        if anchor is None:
            raise DiagnosisRejected("protection_failed")
        actual_size, actual_hash = file_hash(anchor, path)
        if actual_hash != sha256 or size is not None and actual_size != size:
            raise DiagnosisRejected("protection_failed")
    return entries


def source_inventory(root: Path, *, exclude: tuple[Path, ...]) -> list[dict[str, str]]:
    """Hash tested source/config identities, never credentials or runtime data."""
    ignored = {"node_modules", "__pycache__", "dist", "site-packages", "venv"}
    top_ignored = {"runtime", "runtime_env", "data", "wxauto_logs", "wxautox文件下载"}
    dependencies = {"mcp/cubism-chat", "mcp/soullink-emotion-sdk", "mcp/Open-LLM-VTuber",
                    "docker/browser_guest/vendor"}
    suffixes = {".py", ".pyi", ".ipynb", ".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", ".vue", ".css", ".html", ".ps1"}
    metadata = {"pyproject.toml", "ruff.toml", ".ruff.toml", "pytest.ini", "mypy.ini", "setup.cfg", ".gitignore"}
    result = []
    def walk_error(error: OSError) -> None:
        raise DiagnosisRejected("protection_failed") from None
    for directory, names, files in os.walk(root, followlinks=False, onerror=walk_error):
        parent = Path(directory)
        kept = []
        for name in names:
            path = parent / name
            if (name.startswith(".") or name in ignored or parent == root and name in top_ignored
                    or path.relative_to(root).as_posix() in dependencies or path in exclude):
                continue
            ordinary_directory(path)
            kept.append(name)
        names[:] = kept
        for name in files:
            path = parent / name
            relative = path.relative_to(root).as_posix()
            if (path.suffix.lower() in suffixes or name in metadata
                    or relative.startswith("web/frontend/") and path.suffix == ".json"):
                result.append({"path": relative, "sha256": file_hash(root, path)[1]})
                if len(result) > 10000:
                    raise DiagnosisRejected("proof_invalid")
    return sorted(result, key=lambda row: row["path"])


def verify_authority() -> dict[str, Any]:
    """Bind all protected sources and old evidence, including v0.1 archives."""
    ordinary_directory(ROOT)
    base = ROOT / "proof"
    frozen = record(base / "v08-closure-r2/frozen.json")
    if (
        type(frozen) is not list
        or len(frozen) != 7
        or {row.get("path") for row in frozen if type(row) is dict} != FILES
    ):
        raise DiagnosisRejected("frozen_invalid")
    protected = record(base / "source-baseline.json")
    predecessor = record(base / "frozen.json")
    old = record(base / "old-evidence.json")
    if (
        type(protected) is not list
        or len(protected) != 50
        or type(predecessor) is not list
        or len(predecessor) != 4
        or {row.get("path") for row in predecessor if type(row) is dict} != OLD_FILES
        or type(old) is not list
    ):
        raise DiagnosisRejected("proof_invalid")
    transition = record(base / "v08-closure-r2/protection-transition.json")
    if (type(transition) is not dict or transition.get("plan") != PLAN
            or type(transition.get("entries")) is not list or len(transition["entries"]) != 2
            or transition.get("source_confirmation_sha256") != file_hash(ROOT, base / "v03/parallel-source-confirmation.json")[1]):
        raise DiagnosisRejected("protection_failed")
    rows = transition["entries"]
    if {row.get("path") for row in rows if type(row) is dict} != set(WECHAT_TRANSITION):
        raise DiagnosisRejected("protection_failed")
    for row in rows:
        if (type(row) is not dict or set(row) != {"path", "before", "after"}
                or (row["before"], row["after"]) != WECHAT_TRANSITION[row["path"]]):
            raise DiagnosisRejected("protection_failed")
    for row in protected:
        expected = row["sha256"]
        if row["path"] in WECHAT_TRANSITION:
            before, expected = WECHAT_TRANSITION[row["path"]]
            if row["sha256"] != before:
                raise DiagnosisRejected("protection_failed")
        if (
            row["path"] not in EXCEPTIONS | {"VERSION"}
            and file_hash(REPO, REPO / row["path"])[1] != expected
        ):
            raise DiagnosisRejected("protection_failed")
    for row in predecessor:
        if file_hash(ROOT, base / "frozen" / row["path"])[1] != row["sha256"]:
            raise DiagnosisRejected("protection_failed")
    for row in frozen:
        for path, anchor in (
            (REPO / row["path"], REPO),
            (base / "v08-closure-r2/frozen" / row["path"], ROOT),
        ):
            if file_hash(anchor, path)[1] != row["sha256"]:
                raise DiagnosisRejected("frozen_invalid")
    v02 = sealed_evidence(base / "v03/v02-evidence.json", (base / "v02", ROOT / "validation/v02"), legacy_size=True)
    v03 = sealed_evidence(base / "v04/v03-evidence.json", (base / "v03", ROOT / "validation/v03"))
    v04 = sealed_evidence(base / "v05/v04-evidence.json", (base / "v04", ROOT / "validation/v04"))
    v05 = sealed_evidence(base / "v06/v05-evidence.json", (base / "v05", ROOT / "validation/v05"))
    v06 = sealed_evidence(base / "v07/v06-evidence.json", (base / "v06", ROOT / "validation/v06"))
    v07 = sealed_evidence(base / "v08-closure-r2/v07-evidence.json", (base / "v07", ROOT / "validation/v07"))
    v08 = sealed_evidence(base / "v08-closure-r2/v08-evidence.json", (base / "v08", ROOT / "validation/v08"))
    closure = sealed_evidence(base / "v08-closure-r2/v08-closure-evidence.json",
                              (base / "v08-closure", ROOT / "validation/v08-closure"))
    closure_r1 = sealed_evidence(base / "v08-closure-r2/v08-closure-r1-evidence.json",
                                 (base / "v08-closure-r1", ROOT / "validation/v08-closure-r1"))
    freeze = record(base / "v08-closure-r2/freeze.json")
    if (type(freeze) is not dict or freeze.get("plan") != PLAN
            or freeze.get("frozen_sources") != frozen):
        raise DiagnosisRejected("frozen_invalid")
    scripts = freeze.get("audit_scripts")
    if (type(scripts) is not list or len(scripts) != 7
            or {row.get("path") for row in scripts if type(row) is dict}
            != {"authority-smoke.ps1", "protection.ps1", "freeze.ps1", "matrix.ps1",
                "analyze.ps1", "execute.ps1", "import-smoke.ps1"}):
        raise DiagnosisRejected("proof_invalid")
    for row in scripts:
        if file_hash(ROOT, base / "v08-closure-r2" / row["path"])[1] != row["sha256"]:
            raise DiagnosisRejected("protection_failed")
    v06_frozen = record(base / "v06/frozen.json")
    if (type(v06_frozen) is not list or len(v06_frozen) != 6
            or {row.get("path") for row in v06_frozen if type(row) is dict} != V06_FILES):
        raise DiagnosisRejected("proof_invalid")
    v08_frozen = record(base / "v08/frozen.json")
    if (type(v08_frozen) is not list or len(v08_frozen) != 7
            or {row.get("path") for row in v08_frozen if type(row) is dict} != FILES):
        raise DiagnosisRejected("proof_invalid")
    old_hashes = {row["path"]: row["sha256"] for row in v08_frozen}
    if any(row["sha256"] != old_hashes[row["path"]] for row in frozen if row["path"] not in EDITED_FILES):
        raise DiagnosisRejected("frozen_invalid")
    expected_old = {row["path"] for row in old}
    actual_old = set()
    for root in (INPUT, OLD):
        ordinary_directory(root)
        for path in root.rglob("*"):
            info = path.lstat()
            if getattr(info, "st_file_attributes", 0) & 0x400 or path.is_symlink():
                raise DiagnosisRejected("protection_failed")
            if stat.S_ISREG(info.st_mode):
                actual_old.add(str(path))
    if actual_old != expected_old:
        raise DiagnosisRejected("protection_failed")
    for row in old:
        path = Path(row["path"])
        anchor = INPUT if path.is_relative_to(INPUT) else OLD
        if (
            not path.is_relative_to(anchor)
            or file_hash(anchor, path)[1] != row["sha256"]
        ):
            raise DiagnosisRejected("protection_failed")
    production = record(base / "production-baseline.json")
    for relative, key in (
        ("data/memory/soul/memory_ledger.db", "ledger_sha256"),
        ("config/api.yaml", "configuration_sha256"),
        (
            "runtime/upgrade_validation/memory-online-next-r5-20261001/p5-eight-scope.json",
            "r5_sha256",
        ),
    ):
        if file_hash(REPO, REPO / relative)[1] != production[key]:
            raise DiagnosisRejected("protection_failed")
    start = record(base / "v08-closure-r2/start.json")
    candidate = freeze.get("candidate_version")
    prior_freeze = record(base / "v08-closure-r1/freeze.json")
    if (type(start) is not dict or start.get("plan") != PLAN
            or start.get("version") != prior_freeze["candidate_version"]["value"]
            or type(candidate) is not dict or set(candidate) != {"value", "sha256"}
            or type(candidate["value"]) is not str or not candidate["value"]
            or candidate["value"] != start["version"]
            or candidate["sha256"] != start.get("version_sha256")
            or (REPO / "VERSION").read_text().strip() != candidate["value"]
            or file_hash(REPO, REPO / "VERSION")[1] != candidate["sha256"] or any(
        (REPO / name).exists()
        for name in ("data/raw_messages.db", "data/memory/raw_messages.db")
    )):
        raise DiagnosisRejected("protection_failed")
    inventory = record(base / "input.json")
    inventory = sorted(inventory, key=lambda row: row["relative"])
    if input_inventory(INPUT) != inventory:
        raise DiagnosisRejected("input_changed")
    matrix_sources = record(base / "v08-closure-r2/matrix-sources.json")
    if matrix_sources != source_inventory(REPO, exclude=(ROOT, INPUT, OLD)):
        raise DiagnosisRejected("protection_failed")
    return {
        "schema": 3,
        "plan": PLAN,
        "frozen": digest(encoded(frozen)),
        "input": digest(encoded(inventory)),
        "dataset": digest(QUESTIONS[0].encode()),
        "protection": digest(encoded([protected, predecessor, old, production, transition, v02, v03, v04, v05, v06, v07, v08, closure, closure_r1, v06_frozen, v08_frozen, start, freeze, matrix_sources])),
    }


def require_ready(owner: OwnedDirectory, authority: dict[str, Any]) -> None:
    if owner.root != ROOT:
        raise DiagnosisRejected("ready_gate_invalid")
    proofs = {
        name: file_hash(ROOT, ROOT / "proof/v08-closure-r2" / name)[1] for name in PROOF_NAMES
    }
    matrix = record(ROOT / "proof/v08-closure-r2/matrix-finish.json")
    comparison = record(ROOT / "proof/v08-closure-r2/comparison.json")
    if (
        matrix.get("admission_pass") is not True
        or type(matrix.get("commands")) is not int or matrix["commands"] != 26
        or matrix.get("production_go") is not False
        or comparison.get("admission_pass") is not True
        or comparison.get("safe_diagnosis") is not True
        or comparison.get("production_go") is not False
        or comparison.get("unresolved") != ["V1", "R2"]
        or comparison.get("diagnostic_admission") not in {"Accept", "ConditionalAccept"}
    ):
        raise DiagnosisRejected("ready_gate_invalid")
    exception = comparison.get("v1_exception")
    if comparison["diagnostic_admission"] == "ConditionalAccept":
        if exception != {"nodeid": "tests/test_daily_memory_early_acceptance.py::test_numeric_snapshot_contribution",
                         "assertion": "empty_candidates", "resolved": False}:
            raise DiagnosisRejected("ready_gate_invalid")
    elif exception is not None:
        raise DiagnosisRejected("ready_gate_invalid")
    if record(ROOT / "ready.json") != {
        **authority,
        "approvals": ["python", "security", "main"],
        "proofs": proofs,
        "purpose": "isolated_diagnosis",
        "production_go": False,
        "unresolved": ["V1", "R2"],
    }:
        raise DiagnosisRejected("ready_gate_invalid")


def finite_failure(exc: BaseException) -> str:
    allowed = {
        "diagnostic_invalid",
        "directory_invalid",
        "file_invalid",
        "file_changed",
        "input_invalid",
        "clone_boundary_invalid",
        "input_changed",
        "copy_changed",
        "clone_identity_invalid",
        "clone_identity_changed",
        "budget_invalid",
        "workflow_deadline",
        "query_budget_exhausted",
        "derivation_forbidden",
        "frozen_invalid",
        "proof_invalid",
        "protection_failed",
        "ready_gate_invalid",
        "worker_permit_invalid",
        "worker_launch_invalid",
        "worker_job_invalid",
        "source_changed",
        "runtime_not_ready",
        "numeric_changed",
        "evidence_invalid",
        "insufficient_available_memory",
        "model_cache_missing",
        "parallel_python",
        "job_close_failed",
        "directory_close_failed",
        "memory_capacity_exceeded",
        "fixed_root_required",
        "attempt_consumed",
        "worker_exit_failed",
        "controller_binding_invalid",
        "controller_signal_invalid",
        "controller_exit_failed",
    }
    return (
        str(exc)
        if type(exc) is DiagnosisRejected and str(exc) in allowed
        else "diagnosis_failed"
    )


@contextmanager
def owned(root: Path, lifecycle: dict[str, Any] | None = None) -> Iterator[OwnedDirectory]:
    from system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory

    owner = None
    try:
        owner = OwnedDirectory(root)
        if lifecycle is not None:
            lifecycle["directory_state"] = "open"
        yield owner
    except BaseException as exc:
        retained = getattr(exc, "retained_owner", None)
        if retained is not None:
            RETAINED.append(retained)
            if lifecycle is not None:
                lifecycle["directory_state"] = "unverified"
        raise
    finally:
        if owner is not None:
            try:
                owner.close()
            except BaseException:
                RETAINED.append(owner)
                if lifecycle is not None:
                    lifecycle.update(directory_state="close_failed", directory_closed=False)
                raise DiagnosisRejected("directory_close_failed") from None
            else:
                if lifecycle is not None:
                    lifecycle.update(directory_state="closed", directory_closed=True)


def publish_controller_attempt(owner: OwnedDirectory, permit: dict[str, Any]) -> None:
    """Only the validated attempt latch may classify an exclusive publish race."""
    try:
        owner.publish("attempt.json", encoded(permit))
    except FileExistsError:
        raise DiagnosisRejected("attempt_consumed") from None


def admit_worker(owner: OwnedDirectory, token: str) -> dict[str, Any]:
    import psutil
    from Tools.daily_semantic_probe_guard import belongs_to_probe_job
    from Tools.probe_daily_memory_real import validate_worker_binding

    authority = verify_authority()
    require_ready(owner, authority)
    permit = record(ROOT / "attempt.json")
    if (
        type(permit) is not dict
        or any(permit.get(key) != value for key, value in authority.items())
        or permit.get("root_identity") != list(owner.identity)
        or type(permit.get("began")) not in {int, float}
        or not math.isfinite(permit["began"])
        or permit.get("work_deadline") != permit["began"] + 360
        or permit.get("cleanup_deadline") != permit["began"] + 420
        or not time.monotonic() < permit["work_deadline"]
        or type(token) is not str
        or not re.fullmatch("[a-f0-9]{32}", token)
        or permit.get("token") != token
    ):
        raise DiagnosisRejected("worker_permit_invalid")
    until = min(permit["work_deadline"], time.monotonic() + 2)
    while not (ROOT / "launch.json").exists() and time.monotonic() < until:
        time.sleep(0.01)
    launch = record(ROOT / "launch.json")
    if type(launch) is not dict or set(launch) != {
        "worker_pid",
        "worker_generation",
        "token",
    }:
        raise DiagnosisRejected("worker_launch_invalid")
    try:
        current, launcher = psutil.Process(), psutil.Process(launch["worker_pid"])
        controller = launcher.parent()
        if (
            controller is None
            or current.pid != launcher.pid
            and current.ppid() != launcher.pid
        ):
            raise DiagnosisRejected("worker_launch_invalid")
        guardian = permit.get("guardian")
        expected_keys = {"token", "parent_pid", "parent_generation", "began", "job_name",
                         "launcher_pid", "launcher_generation", "controller_pid", "controller_generation"}
        if (type(guardian) is not dict or set(guardian) != expected_keys
                or guardian.get("token") != token or guardian.get("began") != permit["began"]
                or guardian.get("job_name") == permit["job_name"]
                or any(type(guardian.get(key)) is not int for key in ("parent_pid", "launcher_pid", "controller_pid"))
                or any(type(guardian.get(key)) not in {int, float} or not math.isfinite(guardian[key]) for key in ("parent_generation", "launcher_generation", "controller_generation"))
                or (guardian["controller_pid"], guardian["controller_generation"]) != (controller.pid, controller.create_time())
                or record(ROOT / "controller-claim.json") != guardian
                or not belongs_to_probe_job(guardian["job_name"])):
            raise DiagnosisRejected("worker_permit_invalid")
        controller_launcher = psutil.Process(guardian["launcher_pid"])
        if (controller_launcher.create_time() != guardian["launcher_generation"]
                or controller.pid != controller_launcher.pid and controller.ppid() != controller_launcher.pid):
            raise DiagnosisRejected("worker_launch_invalid")
        actual_guardian = controller_launcher.parent()
        if (actual_guardian is None or (actual_guardian.pid, actual_guardian.create_time()) !=
                (guardian["parent_pid"], guardian["parent_generation"])):
            raise DiagnosisRejected("worker_launch_invalid")
        validate_worker_binding(
            {**permit, **launch},
            token=token,
            parent=(controller.pid, controller.create_time()),
            worker=(launcher.pid, launcher.create_time()),
            inside_job=belongs_to_probe_job(permit["job_name"]),
        )
        owner.publish(
            "worker-claim.json",
            encoded(
                {
                    "pid": current.pid,
                    "generation": current.create_time(),
                    "token": token,
                }
            ),
        )
    except (psutil.Error, ValueError, OSError, RuntimeError) as exc:
        if type(exc) is DiagnosisRejected:
            raise
        raise DiagnosisRejected("worker_launch_invalid") from None
    return permit


def content_proof(
    raw: ObservedRaw, runtime: DailyMemoryRuntime, message_id: str
) -> str:
    """Hash public logical rows, not volatile SQLite page/checkpoint bytes."""
    from kernel.memory_pipeline_models import PipelineScope

    scopes = raw.configured_scopes()
    if len(scopes) != 1:
        raise DiagnosisRejected("source_changed")
    scope = scopes[0]
    if scope.connector_id is None or scope.conversation_id is None:
        raise DiagnosisRejected("source_changed")
    original = raw.store.get(scope, message_id)
    replayed = raw.store.replay(
        scope, scope.conversation_id, limit=2, include_inactive=True
    )
    if original is None or len(replayed) != 1 or replayed[0] != original:
        raise DiagnosisRejected("source_changed")
    partition = PipelineScope(
        scope.user_id, scope.agent_id, scope.connector_id, scope.conversation_id
    )
    tasks = raw.store.list_derivations(scope, message_id)
    if len(tasks) != 6 or any(
        row["status"] != "succeeded" or row["attempts"] != 1 for row in tasks
    ):
        raise DiagnosisRejected("source_changed")
    results = raw.store.results(scope, message_id)
    ledger = runtime.stores.ledger(partition)
    rows = [
        asdict(original),
        raw.store.message_events(scope, message_id),
        tasks,
        [raw.store.derivation_events(scope, row["task_id"]) for row in tasks],
        results,
        [asdict(item) for item in ledger.items_from_raw(message_id)],
        [
            [
                runtime.stores.evidence(partition).get(row["memory_id"]),
                ledger.audit_of(row["memory_id"]),
            ]
            for row in results
        ],
    ]
    return digest(encoded(rows))


def numeric_proof(clone: Path, scope: PipelineScope) -> str:
    """Hash stored Chroma logical data; unused HNSW buffer bytes are not vectors."""
    path = clone / "isolated-derived" / scope.partition_id / "vectors"
    ordinary_directory(path)
    database = path / "chroma.sqlite3"
    file_hash(clone, database)
    db = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, isolation_level=None)
    rows: list[Any] = []
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        for table in (
            "embeddings",
            "embedding_metadata",
            "embeddings_queue",
            "collections",
            "collection_metadata",
            "segments",
            "segment_metadata",
        ):
            values = db.execute('SELECT * FROM "' + table + '"').fetchmany(5001)
            if len(values) > 5000:
                raise DiagnosisRejected("numeric_changed")
            clean = [
                [
                    {"blob_hash": digest(value), "size": len(value)}
                    if type(value) is bytes
                    else value
                    for value in row
                ]
                for row in values
            ]
            rows.append([table, sorted(clean, key=encoded)])
    finally:
        db.close()
    return digest(encoded(rows))


def segment_proof(clone: Path, scope: PipelineScope) -> dict[str, str]:
    path = clone / "isolated-derived" / scope.partition_id / "vectors"
    ordinary_directory(path)
    segments: dict[str, str] = {}
    for directory in path.iterdir():
        if directory.is_dir():
            ordinary_directory(directory)
            for file in directory.iterdir():
                _, hashed = file_hash(clone, file)
                segments[file.relative_to(path).as_posix()] = hashed
    return segments


def stored_vectors(clone: Path, scope: PipelineScope) -> dict[str, str]:
    path = clone / "isolated-derived" / scope.partition_id / "vectors/chroma.sqlite3"
    file_hash(clone, path)
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT id,vector,encoding,operation FROM embeddings_queue ORDER BY seq_id"
        ).fetchmany(101)
        if not 1 <= len(rows) <= 100:
            raise DiagnosisRejected("numeric_changed")
        result = {}
        for key, vector, encoding, operation in rows:
            if (
                type(key) is not str
                or type(vector) is not bytes
                or encoding != "FLOAT32"
                or operation not in {0, 2}
                or key in result
                or len(vector) % 4
            ):
                raise DiagnosisRejected("numeric_changed")
            result[key] = digest(vector)
        return result
    finally:
        db.close()


STARTUP_STAGES = frozenset({"created", "raw_preparation", "read_preparation", "start_return",
    "partition_recovery", "vector_warm", "recovered", "snapshot_capture",
    "snapshot_publication", "snapshot_reader_warm", "ready", "publisher", "unknown",
    "not_started", "spawn", "waiting_ready", "failed"})
STARTUP_CODES = frozenset({"none", "unknown", "startup_failed", "recovery_failed",
    "recovery_unverified", "publication_failed", "publication_capacity", "source_changed",
    "publisher_failed", "spawn_failed", "handshake_error", "invalid_ready", "owner_rejected",
    "embedding_rejected", "process_exit", "ready_deadline", "cancelled"})


def finite_startup(value: dict) -> dict:
    """Whitelist the separate startup report, without expanding query contracts."""
    if type(value) is not dict:
        raise DiagnosisRejected("diagnostic_invalid")
    if not value:
        return {}

    def entry(row: Any, *, transport: bool = False) -> dict:
        if type(row) is not dict:
            raise DiagnosisRejected("diagnostic_invalid")
        result: dict[str, Any] = {}
        for key, allowed in (("stage", STARTUP_STAGES), ("error_code", STARTUP_CODES),
                             ("error_type", ERROR_TYPES | {"EOFError"})):
            item = row.get(key)
            if type(item) is not str:
                raise DiagnosisRejected("diagnostic_invalid")
            result[key] = item if item in allowed else "unknown"
        if transport:
            elapsed: Any = row.get("elapsed_ms")
            if (type(elapsed) not in {float, int} or not math.isfinite(elapsed)
                    or not 0 <= elapsed <= 86_400_000):
                raise DiagnosisRejected("diagnostic_invalid")
            result["elapsed_ms"] = float(elapsed)
        return result

    result = entry(value)
    scopes = value.get("scopes")
    if type(scopes) is not list or len(scopes) > 8:
        raise DiagnosisRejected("diagnostic_invalid")
    result["scopes"] = []
    seen = set()
    for row in scopes:
        cleaned = entry(row)
        slot = row.get("slot")
        if type(slot) is not int or not 0 <= slot < 8 or slot in seen:
            raise DiagnosisRejected("diagnostic_invalid")
        seen.add(slot)
        result["scopes"].append({"slot": slot, **cleaned})
    for role in ("writer", "reader"):
        result[role] = entry(value.get(role), transport=True)
    if len(encoded(result)) > 4096:
        raise DiagnosisRejected("diagnostic_invalid")
    return result


async def observe_startup(runtime: DailyMemoryRuntime, deadline: float, *, report: dict,
                          on_change=None, cycle: int = 1) -> bool:
    """Own a bounded, pre-query observer inside the caller's existing deadline."""
    if (type(deadline) not in {int, float} or not math.isfinite(deadline)
            or type(report) is not dict or type(cycle) is not int or cycle not in {1, 2}
            or on_change is not None and not callable(on_change)):
        raise DiagnosisRejected("diagnostic_invalid")
    changes = report.setdefault("changes", [])
    if type(changes) is not list or len(changes) > 32:
        raise DiagnosisRejected("diagnostic_invalid")
    phase, outcome, error_type = "start", "pending", "none"
    signature = None
    report["settled"] = False

    def capture(*, final=False):
        nonlocal signature
        health = finite_health(runtime.health())
        frame = {"cycle": cycle, "phase": phase, "outcome": outcome, "error_type": error_type,
                 "runtime": finite_startup(runtime.startup_diagnostic()), "health": health,
                 "health_observation": "incomplete" if "stopped" in health.values() else "observed"}
        body = encoded(frame)
        if len(body) > 4096:
            raise DiagnosisRejected("diagnostic_invalid")
        report["last"] = json.loads(body)
        stable = json.loads(body)
        for role in ("writer", "reader"):
            if stable["runtime"]:
                stable["runtime"][role].pop("elapsed_ms", None)
        current = encoded(stable)
        changed = current != signature
        signature = current
        if changed and len(changes) < 32:
            changes.append(json.loads(body))
            if on_change is not None:
                on_change(json.loads(body))
        if final and on_change is not None:
            on_change({**json.loads(body), "final": True})

    async def observe():
        while True:
            await asyncio.sleep(.05)
            capture()

    capture()
    observer = asyncio.create_task(observe())
    try:
        await runtime.start()
        phase = "wait_ready"
        capture()
        ready = await wait_ready(runtime, deadline, initial=True)
        phase, outcome = ("ready", "ready") if ready else ("wait_ready", "blocked")
        return ready
    except BaseException as exc:
        outcome = "cancelled" if isinstance(exc, asyncio.CancelledError) else "exception"
        name = type(exc).__name__
        error_type = name if name in ERROR_TYPES else "OtherError"
        raise
    finally:
        observer.cancel()
        settled = await asyncio.gather(observer, return_exceptions=True)
        report["settled"] = True
        capture(final=True)
        if any(isinstance(exc, BaseException) and not isinstance(exc, asyncio.CancelledError) for exc in settled):
            raise RuntimeError("startup observer failed") from None


async def wait_ready(
    runtime: DailyMemoryRuntime, deadline: float, *, initial: bool,
    on_change: Any = None,
) -> bool:
    if on_change is not None and (not callable(on_change) or type(initial) is not bool):
        raise DiagnosisRejected("ready_observer_invalid")
    until = deadline if initial else min(deadline, time.monotonic() + 10)
    health: dict = {}
    last: dict = {}
    changes, observer_failed = 0, False

    def publish(outcome: str) -> None:
        nonlocal last, changes, observer_failed
        if on_change is None:
            return
        reader = runtime.snapshot_reader
        reader_state = (reader.health() if reader is not None else
                        {"state": "absent", "pending": 0, "recovery_attempts": 0})
        if (type(reader_state) is not dict
                or type(reader_state.get("state")) is not str
                or reader_state["state"] not in {"absent", "ready", "recovering", "unavailable", "stopped"}
                or any(type(reader_state.get(key)) is not int or not 0 <= reader_state[key] <= limit
                       for key, limit in (("pending", 8), ("recovery_attempts", 3)))
                or type(until) not in {int, float} or not math.isfinite(until)):
            raise DiagnosisRejected("ready_observer_invalid")
        frame = {"phase": "wait_ready", "outcome": outcome, "initial": initial,
                 "deadline": until, "health": finite_health(health),
                 "reader": {key: reader_state[key] for key in ("state", "pending", "recovery_attempts")}}
        if frame == last:
            return
        if changes >= 32:
            raise DiagnosisRejected("ready_observer_capacity")
        last, changes = frame, changes + 1
        try:
            on_change(json.loads(encoded(frame)))
        except BaseException:
            observer_failed = True
            raise DiagnosisRejected("ready_observer_failed") from None

    def finish(value: bool, outcome: str) -> bool:
        publish(outcome)
        return value

    async def wait() -> bool:
        nonlocal health
        while time.monotonic() < until:
            health = runtime.health()
            publish("pending")
            if (initial and health.get("derived_state") == "degraded"
                    and "derived_recovery_incomplete" in health.get("reason_codes", ())):
                return finish(False, "blocked")
            if initial and health.get("semantic_state") in {"preparing", "recovering", "degraded", "unavailable"}:
                from kernel.memory_pipeline_models import PipelineScope
                for scope in runtime.raw.configured_scopes():
                    partition = PipelineScope(scope.user_id, scope.agent_id,
                                              scope.connector_id, scope.conversation_id)
                    publication = await runtime.semantic_publication_state(partition, deadline=until)
                    if publication.get("terminal") is True:
                        return finish(False, "blocked")
            retryable_history = (
                not initial and health["state"] == "degraded"
                and health.get("raw_state") == "degraded" and health.get("derived_state") == "ready"
                and set(health.get("reason_codes", ())) == {"vector_unavailable"}
                and not health.get("worker_error")
                and type(health.get("reader_pid")) is int and health["reader_pid"] > 0
            )
            if (health["state"] == "ready" or retryable_history) and health["semantic_state"] == "ready":
                from kernel.memory_pipeline_models import PipelineScope
                scopes = runtime.raw.configured_scopes()
                current = bool(scopes)
                for scope in scopes:
                    partition = PipelineScope(scope.user_id, scope.agent_id,
                                              scope.connector_id, scope.conversation_id)
                    if retryable_history and (runtime.snapshot_reader is None
                            or not runtime.snapshot_reader.available(partition)
                            or runtime.retrieval.worker_pid != health["reader_pid"]):
                        current = False
                        break
                    publication = await runtime.semantic_publication_state(partition, deadline=until)
                    if initial and publication.get("terminal") is True:
                        return finish(False, "blocked")
                    if publication["status"] != "current":
                        current = False
                        break
                if current:
                    return finish(True, "ready")
            if (health["state"] in {"unavailable", "stopped"}
                    or health.get("derived_state") == "unavailable"):
                return finish(False, "blocked")
            await asyncio.sleep(0.05)
        return finish(False, "deadline")

    try:
        return await wait()
    except BaseException as exc:
        if not observer_failed:
            try:
                publish("cancelled" if isinstance(exc, asyncio.CancelledError) else "exception")
            except BaseException:
                # Observation must never replace the business cancellation/error.
                pass
        raise


async def replay(
    owner: OwnedDirectory,
    permit: dict[str, Any],
    *,
    embedding: str,
    report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from kernel.app_bus import AppBus
    from kernel.memory_pipeline_models import PipelineScope
    from system_memory_god.daily_runtime import DailyMemoryRuntime
    from Tools.probe_daily_memory_real import MESSAGES, make_profile, source_proof

    saved = record(INPUT / "worker-report.json")["source_details"]
    if type(saved) is not list or len(saved) != 1 or type(saved[0]) is not dict:
        raise DiagnosisRejected("source_changed")
    detail = saved[0]
    records = detail["records"]
    model = records[0]["source"]["model"]
    if (
        type(model) is not str
        or not 1 <= len(model) <= 128
        or any(row["source"]["model"] != model for row in records)
    ):
        raise DiagnosisRejected("source_changed")
    if report is None:
        report = {}
    report.update(
        go=False,
        cloud_requests=0,
        groups=[],
        query_count=0,
        derivation_calls=0,
        diagnosis_complete=False,
    )
    budget = QueryBudget(permit["work_deadline"])
    extractor = ForbiddenExtractor()
    for group in ("baseline", "instrumented"):
        profile = replace(
            make_profile(model),
            database=ROOT / group / "isolated-raw/messages.db",
            derived_root=ROOT / group / "isolated-derived",
        )
        # Scope is derived from the same fixed binding as the original capture.
        from kernel.raw_message_service import stable_id

        conversation = stable_id(
            "real-probe-agent",
            profile.owner_id,
            "real-probe-connector",
            "real-probe-chat",
        )
        partition = PipelineScope(
            profile.owner_id, "real-probe-agent", "real-probe-connector", conversation
        )
        vector_ids = tuple(
            row["memory_id"] for row in records if row["domain"] == "vector"
        )
        clone = ROOT / group
        restored = prepare_clone(owner, group, partition, vector_ids)
        owner.publish(group + "-copy.json", encoded(restored))
        numeric_before = numeric_proof(clone, partition)
        segments_before = segment_proof(clone, partition)
        vectors_before = stored_vectors(clone, partition)
        raw = ObservedRaw(profile)
        bus = AppBus(str(clone / "no-apps"))
        bus.attach_raw_messages(raw, lambda: "real-probe-agent")
        runtime = DailyMemoryRuntime(
            raw,
            profile,
            bus,
            extractor=extractor,
            embedding=embedding,
            semantic_mode="snapshot",
            semantic_diagnostics=group == "instrumented",
        )
        bus.attach_daily_runtime(runtime)
        entry: dict[str, Any] = {
            "group": group,
            "diagnostics": group == "instrumented",
            "queries": [],
            "normal_close": False,
            "content_unchanged": False,
            "numeric_unchanged": False,
        }
        report["groups"].append(entry)
        try:
            scope = raw.configured_scopes()[0]
            if (
                source_proof(raw, runtime, scope, detail["message_id"], MESSAGES[0], 1)
                != detail
            ):
                raise DiagnosisRejected("source_changed")
            before = content_proof(raw, runtime, detail["message_id"])
            entry["content_hash_before"], entry["numeric_hash_before"] = (
                before,
                numeric_before,
            )
            await runtime.start()
            if not await wait_ready(runtime, budget.deadline, initial=True):
                raise DiagnosisRejected("runtime_not_ready")
            import base64

            captured = runtime.vector.snapshot_vectors(partition)
            if {
                key: digest(base64.b64decode(row["snapshot"]["f32"], validate=True))
                for key, row in captured.items()
            } != vectors_before or set(captured) != set(vector_ids):
                raise DiagnosisRejected("numeric_changed")
            await bus.restore_raw_turn(
                {
                    "agent_id": scope.agent_id,
                    "owner_id": scope.user_id,
                    "connector_id": scope.connector_id,
                    "conversation_id": scope.conversation_id,
                    "event_id": detail["message_id"],
                    "revision": 1,
                    "source": "web",
                    "target": "web_console",
                }
            )
            if not bus.raw_turn_snapshot():
                raise DiagnosisRejected("source_changed")
            for number in range(1, 4):
                if not await wait_ready(runtime, budget.deadline, initial=False):
                    entry["not_executed"] = 4 - number
                    break
                observation, hits = await observe_query(
                    bus, raw, runtime, budget, group=group
                )
                observation.update(group=group, number=number)
                entry["queries"].append(observation)
                report["query_count"] = budget.calls
                owner.publish(
                    group + "-query-" + str(number) + ".json", encoded(observation)
                )
                if type(hits) is list:
                    for hit in hits:
                        if (
                            hit.get("source_message_id") != detail["message_id"]
                            or hit.get("source_time") != detail["created_at"]
                            or type(hit.get("revision")) is not int
                            or hit["revision"] != 1
                            or hit.get("source_content") != MESSAGES[0]
                            or hit.get("neighbors", []) != []
                        ):
                            raise DiagnosisRejected("evidence_invalid")
                        if hit["domain"] != "raw" and not any(
                            row["memory_id"] == hit.get("memory_id")
                            and row["domain"] == hit["domain"]
                            and row["source"] == hit.get("source")
                            for row in records
                        ):
                            raise DiagnosisRejected("evidence_invalid")
                if (
                    content_proof(raw, runtime, detail["message_id"]) != before
                    or extractor.calls
                ):
                    raise DiagnosisRejected("source_changed")
        finally:
            try:
                await runtime.close()
            finally:
                # Preserve actual refused calls even if close/cancellation fails.
                report["derivation_calls"] = extractor.calls
            entry["normal_close"] = runtime.health()["state"] == "stopped"
        after = content_proof(raw, runtime, detail["message_id"])
        numeric_after = numeric_proof(clone, partition)
        entry.update(
            content_hash_after=after,
            numeric_hash_after=numeric_after,
            content_unchanged=after == before,
            numeric_unchanged=numeric_before == numeric_after,
            captured_vectors_hash=digest(encoded(vectors_before)),
            segment_hashes_before=segments_before,
            segment_hashes_after=segment_proof(clone, partition),
        )
        owner.publish(group + "-result.json", encoded(entry))
        if not entry["numeric_unchanged"]:
            raise DiagnosisRejected("numeric_changed")
        if not entry["content_unchanged"] or extractor.calls:
            raise DiagnosisRejected("source_changed")
    report["diagnosis_complete"] = all(
        len(group["queries"]) == 3 and group["normal_close"]
        for group in report["groups"]
    )
    return report


def finite_integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> bool:
    return type(value) is int and minimum <= value <= maximum


def finite_failure_value(value: Any) -> bool:
    return value is None or type(value) is str and finite_failure(DiagnosisRejected(value)) == value


def nonnegative_number(value: Any) -> bool:
    try:
        return type(value) in {int, float} and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def validate_query_report(query: Any) -> None:
    keys = {"return_type", "candidate_count", "domain_counts", "elapsed_ms", "flags", "gate_pass",
            "query_hash", "raw_validation", "before", "after", "call_error", "remaining_work_ms",
            "full_300ms_sample", "vector_diagnostic", "group", "number"}
    if (type(query) is not dict or set(query) != keys or query["return_type"] not in {"list", "none", "other"}
            or not nonnegative_number(query["elapsed_ms"]) or not nonnegative_number(query["remaining_work_ms"])
            or type(query["full_300ms_sample"]) is not bool or query["full_300ms_sample"] != (query["remaining_work_ms"] >= 300)
            or type(query["gate_pass"]) is not bool or query["call_error"] not in ERROR_TYPES
            or type(query["number"]) is not int or not 1 <= query["number"] <= 3):
        raise DiagnosisRejected("worker_permit_invalid")
    flags, counts = query["flags"], query["domain_counts"]
    if (type(flags) is not dict or set(flags) != {"return_type_invalid", "deadline_exceeded", "required_evidence_empty"}
            or any(type(value) is not bool for value in flags.values())
            or flags["deadline_exceeded"] != (query["elapsed_ms"] > 300)
            or flags["return_type_invalid"] != (query["return_type"] != "list")
            or query["gate_pass"] != (not any(flags.values()))
            or type(counts) is not dict or set(counts) != DOMAINS
            or any(type(value) is not int or not 0 <= value <= 100 for value in counts.values())):
        raise DiagnosisRejected("worker_permit_invalid")
    if query["return_type"] == "list":
        if (type(query["candidate_count"]) is not int or not 0 <= query["candidate_count"] <= 100
                or sum(counts.values()) != query["candidate_count"]
                or flags["required_evidence_empty"] != (query["candidate_count"] == 0)):
            raise DiagnosisRejected("worker_permit_invalid")
    elif query["candidate_count"] is not None or sum(counts.values()) != 0:
        raise DiagnosisRejected("worker_permit_invalid")
    raw = query["raw_validation"]
    if (type(raw) is not dict or set(raw) != {"seen", "ok", "elapsed_ms", "error_type"}
            or type(raw["seen"]) is not bool or type(raw["ok"]) is not bool
            or raw["elapsed_ms"] is not None and not nonnegative_number(raw["elapsed_ms"])
            or raw["error_type"] not in ERROR_TYPES):
        raise DiagnosisRejected("worker_permit_invalid")
    if any(finite_health(query[key]) != query[key] for key in ("before", "after")):
        raise DiagnosisRejected("worker_permit_invalid")
    diagnostic = query["vector_diagnostic"]
    if (type(diagnostic) is not dict or set(diagnostic) != {"association", "values"}
            or diagnostic["association"] not in {"fresh", "stale", "inconclusive", "unavailable", "invalid"}):
        raise DiagnosisRejected("worker_permit_invalid")
    if diagnostic["values"] is not None and finite_diagnostic(diagnostic["values"], {})["values"] != diagnostic["values"]:
        raise DiagnosisRejected("worker_permit_invalid")


def validate_replay_report(report: dict[str, Any]) -> None:
    if (type(report) is not dict or type(report.get("diagnosis_complete")) is not bool
            or not finite_integer(report.get("query_count"), 0, 6)
            or "blocked" in report and not finite_failure_value(report["blocked"])):
        raise DiagnosisRejected("worker_permit_invalid")
    if "groups" not in report or "derivation_calls" not in report:
        # Failure before replay starts has no observations or extractor counter.
        if ("groups" in report or "derivation_calls" in report or report["query_count"] != 0
                or report["diagnosis_complete"] or report.get("blocked") is None):
            raise DiagnosisRejected("worker_permit_invalid")
        return
    if not finite_integer(report["derivation_calls"]):
        raise DiagnosisRejected("worker_permit_invalid")
    groups = report.get("groups", [])
    if type(groups) is not list or len(groups) > 2:
        raise DiagnosisRejected("worker_permit_invalid")
    group_keys = {"group", "diagnostics", "queries", "normal_close", "content_unchanged", "numeric_unchanged",
                  "content_hash_before", "numeric_hash_before", "content_hash_after", "numeric_hash_after",
                  "captured_vectors_hash", "segment_hashes_before", "segment_hashes_after", "not_executed"}
    for group, name in zip(groups, ("baseline", "instrumented")):
        if (type(group) is not dict or set(group) - group_keys or group.get("group") != name
                or type(group.get("queries")) is not list or len(group["queries"]) > 3
                or "diagnostics" in group and (type(group["diagnostics"]) is not bool or group["diagnostics"] != (name == "instrumented"))
                or "not_executed" in group and (type(group["not_executed"]) is not int or not 1 <= group["not_executed"] <= 3
                                               or group["not_executed"] != 3 - len(group["queries"]))
                or any(type(group.get(key)) is not bool for key in ("normal_close", "content_unchanged", "numeric_unchanged"))):
            raise DiagnosisRejected("worker_permit_invalid")
        for key, value in group.items():
            if "hash" in key and not key.startswith("segment_"):
                if type(value) is not str or not re.fullmatch("[a-fA-F0-9]{64}", value):
                    raise DiagnosisRejected("worker_permit_invalid")
            elif key.startswith("segment_"):
                if (type(value) is not dict or len(value) > 32 or any(
                    type(path) is not str or not re.fullmatch(r"[a-f0-9-]{36}/[a-z0-9_.-]{1,64}", path)
                    or type(sha256) is not str or not re.fullmatch("[A-F0-9]{64}", sha256) for path, sha256 in value.items())):
                    raise DiagnosisRejected("worker_permit_invalid")
        for number, query in enumerate(group["queries"], 1):
            validate_query_report(query)
            if query["group"] != name or query["number"] != number or query["query_hash"] != digest(QUESTIONS[0].encode()):
                raise DiagnosisRejected("worker_permit_invalid")
    # Count retained observations, not precharged calls that failed before recording.
    if report["query_count"] != sum(len(group["queries"]) for group in groups):
        raise DiagnosisRejected("worker_permit_invalid")
    if report.get("diagnosis_complete") is not True:
        return
    groups = report.get("groups")
    if (
        type(report.get("query_count")) is not int
        or report["query_count"] != 6
        or type(report.get("derivation_calls")) is not int
        or report["derivation_calls"] != 0
        or report.get("blocked") is not None
        or type(groups) is not list
        or len(groups) != 2
    ):
        raise DiagnosisRejected("worker_permit_invalid")
    for group, name in zip(groups, ("baseline", "instrumented"), strict=True):
        if (
            type(group) is not dict
            or group.get("group") != name
            or any(
                group.get(key) is not True
                for key in ("normal_close", "content_unchanged", "numeric_unchanged")
            )
            or type(group.get("queries")) is not list
            or len(group["queries"]) != 3
        ):
            raise DiagnosisRejected("worker_permit_invalid")
        for number, query in enumerate(group["queries"], 1):
            if (
                type(query) is not dict
                or type(query.get("number")) is not int
                or query.get("number") != number
                or query.get("group") != name
                or query.get("query_hash") != digest(QUESTIONS[0].encode())
            ):
                raise DiagnosisRejected("worker_permit_invalid")


def offline_environment() -> dict[str, Any]:
    import psutil
    from huggingface_hub.constants import HF_HUB_CACHE

    available = psutil.virtual_memory().available
    if available < 6 * 1024**3:
        raise DiagnosisRejected("insufficient_available_memory")
    cache = Path(HF_HUB_CACHE) / "models--BAAI--bge-m3" / "snapshots"
    if not cache.is_dir() or not any(cache.iterdir()):
        raise DiagnosisRejected("model_cache_missing")
    current = psutil.Process()
    allowed = {current.pid, *(parent.pid for parent in current.parents())}
    try:
        for process in psutil.process_iter(["pid", "name"]):
            if process.info["pid"] not in allowed and (
                process.info["name"] or ""
            ).lower().startswith(("python", "pypy")):
                raise DiagnosisRejected("parallel_python")
    except psutil.Error:
        raise DiagnosisRejected("parallel_python") from None
    return {
        "available_ram_bytes": available,
        "bge_cache_present": True,
        "cpu": True,
        "cloud_requests": 0,
    }


def run_worker(token: str) -> int:
    import logging

    logging.disable(logging.CRITICAL)
    report: dict[str, Any] = {
        "schema": 3,
        "plan": PLAN,
        "go": False,
        "cloud_requests": 0,
        "token": token,
        "diagnosis_complete": False,
        "query_count": 0,
        "blocked": None,
    }
    try:
        if RETAINED:
            raise DiagnosisRejected("worker_job_invalid")
        with owned(ROOT) as owner:
            permit = admit_worker(owner, token)
            try:
                remaining = permit["work_deadline"] - time.monotonic()
                if remaining <= 0:
                    raise DiagnosisRejected("workflow_deadline")
                asyncio.run(
                    asyncio.wait_for(
                        replay(owner, permit, embedding="bge-m3", report=report),
                        remaining,
                    )
                )
                validate_replay_report(report)
            except BaseException as exc:
                report["blocked"] = finite_failure(exc)
                report["diagnosis_complete"] = False
            report.update(
                authority={
                    key: permit[key]
                    for key in ("frozen", "input", "dataset", "protection")
                },
                final=False,
                directory_closed=False,
            )
            owner.publish("worker-report.json", encoded(report))
        report["directory_closed"] = True
        report["final"] = True
    except BaseException as exc:
        report.update(
            blocked=finite_failure(exc),
            diagnosis_complete=False,
            final=False,
            directory_closed=False,
        )
    if report.get("blocked") == "directory_close_failed":
        return 32
    return (
        0
        if report["diagnosis_complete"]
        and report.get("directory_closed") is True
        and not RETAINED
        else 1
    )


def controller_binding(arguments: list[str]) -> dict[str, Any]:
    """Validate the direct guardian, with only the existing venv launcher layer."""
    import psutil
    from Tools.daily_semantic_probe_guard import belongs_to_probe_job

    try:
        token, parent_pid, parent_generation, began, job_name = arguments
        binding: dict[str, Any] = {"token": token, "parent_pid": int(parent_pid),
                   "parent_generation": float(parent_generation), "began": float(began),
                   "job_name": job_name}
        if (not re.fullmatch("[a-f0-9]{32}", token)
                or not math.isfinite(binding["began"])
                or not math.isfinite(binding["parent_generation"])
                or not binding["began"] <= time.monotonic() < binding["began"] + 360
                or not belongs_to_probe_job(job_name)):
            raise DiagnosisRejected("controller_binding_invalid")
        current = psutil.Process()
        parent = current.parent()
        if parent is None:
            raise DiagnosisRejected("controller_binding_invalid")
        launcher = current
        if parent.pid != binding["parent_pid"]:
            launcher = parent
            parent = launcher.parent()
        if (parent is None or (parent.pid, parent.create_time()) !=
                (binding["parent_pid"], binding["parent_generation"])):
            raise DiagnosisRejected("controller_binding_invalid")
        return {**binding, "launcher_pid": launcher.pid,
                "launcher_generation": launcher.create_time(), "controller_pid": current.pid,
                "controller_generation": current.create_time()}
    except (ValueError, psutil.Error):
        raise DiagnosisRejected("controller_binding_invalid") from None


def run_controller(binding: dict[str, Any]) -> dict[str, Any]:
    import psutil
    from aoao_wechat_job_core import WindowsJobCore, create_job_bound_command
    from Tools.probe_daily_memory_real import sample_tree_rss

    result: dict[str, Any] = {
        "schema": 3,
        "plan": PLAN,
        "go": False,
        "cloud_requests": 0,
        "diagnosis_complete": False,
        "forced": False,
        "tree_empty": False,
        "handles_closed": False,
        "directory_closed": False,
        "directory_state": "not_opened",
        "job_state": "not_created",
        "peak_rss_bytes": 0,
        "blocked": None,
    }
    try:
        if RETAINED:
            raise DiagnosisRejected("worker_job_invalid")
        with owned(ROOT, result) as owner:
            if (ROOT / "attempt.json").exists():
                raise DiagnosisRejected("attempt_consumed")
            authority = verify_authority()
            require_ready(owner, authority)
            result["authority"] = {key: authority[key] for key in ("frozen", "input", "dataset", "protection")}
            result["root_identity"] = list(owner.identity)
            result["environment"] = offline_environment()
            began = binding["began"]
            token = binding["token"]
            if time.monotonic() >= began + 360:
                raise DiagnosisRejected("workflow_deadline")
            current = psutil.Process()
            permit = {
                **authority,
                "root_identity": list(owner.identity),
                "began": began,
                "work_deadline": began + 360,
                "cleanup_deadline": began + 420,
                "token": token,
                "parent_pid": current.pid,
                "parent_generation": current.create_time(),
                "job_name": "Local\\AoaoDailyProbe." + uuid.uuid4().hex,
                "guardian": binding,
            }
            publish_controller_attempt(owner, permit)
            owner.publish("controller-claim.json", encoded(binding))
            process, job = None, None
            final_deadline = began + 420
            try:
                result["job_state"] = "unverified"
                job = WindowsJobCore(name=permit["job_name"])
                result["job_state"] = "open"
                environment = dict(os.environ)
                environment.update(
                    HF_HUB_OFFLINE="1",
                    TRANSFORMERS_OFFLINE="1",
                    TOKENIZERS_PARALLELISM="false",
                    AOAO_DAILY_REAL_API_TEST="0",
                    AOAO_REAL_EMBEDDING_TEST="0",
                    CUDA_VISIBLE_DEVICES="",
                )
                process = create_job_bound_command(
                    [
                        str(Path(sys.executable).resolve()),
                        "-B",
                        str(Path(__file__).resolve()),
                        "--worker",
                        token,
                    ],
                    {
                        "cwd": str(REPO),
                        "env": environment,
                        "stdin": subprocess.DEVNULL,
                        "stdout": subprocess.DEVNULL,
                        "stderr": subprocess.DEVNULL,
                        "close_fds": True,
                        "creationflags": subprocess.CREATE_NO_WINDOW,
                    },
                    final_deadline,
                    job_type=lambda: job,
                )
                owner.publish(
                    "launch.json",
                    encoded(
                        {
                            "worker_pid": process.pid,
                            "worker_generation": psutil.Process(
                                process.pid
                            ).create_time(),
                            "token": token,
                        }
                    ),
                )
                while (
                    process.poll() is None
                    and time.monotonic() < permit["cleanup_deadline"]
                ):
                    rss = sample_tree_rss(process.pid)
                    result["peak_rss_bytes"] = max(result["peak_rss_bytes"], rss)
                    if rss > 6 * 1024**3:
                        raise DiagnosisRejected("memory_capacity_exceeded")
                    time.sleep(0.05)
                until = min(permit["cleanup_deadline"], time.monotonic() + 0.25)
                while job.active_processes() and time.monotonic() < until:
                    time.sleep(0.01)
                if process.poll() is None or job.active_processes():
                    raise DiagnosisRejected("workflow_deadline")
                worker = record(ROOT / "worker-report.json")
                if (
                    type(worker) is not dict
                    or type(worker.get("schema")) is not int
                    or worker.get("schema") != 3
                    or worker.get("plan") != PLAN
                    or worker.get("token") != token
                    or type(worker.get("cloud_requests")) is not int
                    or worker.get("cloud_requests") != 0
                    or type(worker.get("query_count")) is not int
                    or not 0 <= worker["query_count"] <= 6
                    or worker.get("go") is not False
                    or worker.get("final") is not False
                    or worker.get("authority")
                    != {
                        key: authority[key]
                        for key in ("frozen", "input", "dataset", "protection")
                    }
                ):
                    raise DiagnosisRejected("worker_permit_invalid")
                result["worker"] = worker
                validate_replay_report(worker)
                result["worker_exit_code"] = process.returncode
                result["diagnosis_complete"] = (
                    worker.get("diagnosis_complete") is True and process.returncode == 0
                )
                result["blocked"] = (
                    "directory_close_failed"
                    if process.returncode == 32
                    else worker.get("blocked")
                    or ("worker_exit_failed" if process.returncode != 0 else None)
                )
            except BaseException as exc:
                result.update(blocked=finite_failure(exc), diagnosis_complete=False)
                retained = getattr(exc, "retained_owner", None)
                if retained is not None:
                    RETAINED.append(retained)
            finally:
                try:
                    if process is not None:
                        result["forced"] = (
                            process.poll() is None
                            or job is not None
                            and job.active_processes() != 0
                        )
                        process.quiesce_until(final_deadline)
                        result["tree_empty"] = (
                            job is not None and job.active_processes() == 0
                        )
                        process.close_until(final_deadline)
                    elif job is not None:
                        result["forced"] = job.active_processes() != 0
                        job.quiesce_until(final_deadline)
                        result["tree_empty"] = job.active_processes() == 0
                        job.close()
                    if job is not None:
                        result["handles_closed"] = True
                        result["job_state"] = "closed"
                except BaseException:
                    RETAINED.append(process if process is not None else job)
                    result.update(blocked="job_close_failed", diagnosis_complete=False,
                                  job_state="close_failed")
                result["protection_pass"] = verify_authority() == authority
                if (
                    result["forced"]
                    or not result["tree_empty"]
                    or not result["handles_closed"]
                    or RETAINED
                    or not result["protection_pass"]
                ):
                    result["diagnosis_complete"] = False
                owner.publish("report.json", encoded({**result, "stage": "pre_close", "final": False}))
        result["final"] = True
    except BaseException as exc:
        result.update(
            blocked=finite_failure(exc), diagnosis_complete=False, final=False
        )
    result["elapsed_seconds"] = time.monotonic() - binding["began"]
    if result["elapsed_seconds"] >= 420 or RETAINED:
        result.update(blocked="workflow_deadline", diagnosis_complete=False)
    return result


def read_available(process: Any, body: bytearray) -> bool:
    """Single unbuffered reader; never wait for bytes or read beyond the cap."""
    import _winapi
    import msvcrt

    if process.stdout is None:
        raise DiagnosisRejected("controller_signal_invalid")
    handle = msvcrt.get_osfhandle(process.stdout.fileno())
    try:
        observed = _winapi.PeekNamedPipe(handle, 0)[0]
        if type(observed) is not int or observed < 0:
            raise DiagnosisRejected("controller_signal_invalid")
        available = cast(int, observed)
        if available:
            if len(body) + available > 64 * 1024:
                raise DiagnosisRejected("controller_signal_invalid")
            chunk, error = _winapi.ReadFile(handle, available)
            if error:
                raise DiagnosisRejected("controller_signal_invalid")
            body.extend(chunk)
        return False
    except OSError as exc:
        if exc.winerror in (109, 232):
            return True
        raise DiagnosisRejected("controller_signal_invalid") from None


def validate_optional_controller_fields(result: dict[str, Any]) -> None:
    for key in ("final", "protection_pass"):
        if key in result and type(result[key]) is not bool:
            raise DiagnosisRejected("controller_signal_invalid")
    if "worker_exit_code" in result and not finite_integer(result["worker_exit_code"], -(2**31), 2**32 - 1):
        raise DiagnosisRejected("controller_signal_invalid")
    validate_lifecycle(result)


def validate_lifecycle(result: dict[str, Any]) -> None:
    states = {
        "job_state": {"not_created", "unverified", "open", "closed", "close_failed"},
        "directory_state": {"not_opened", "open", "closed", "unverified", "close_failed"},
    }
    for key, allowed in states.items():
        if key not in result:
            continue
        value = result[key]
        closed = result["handles_closed" if key == "job_state" else "directory_closed"]
        if (type(value) is not str or value not in allowed
                or value == "closed" and not closed
                or value in {"open", "unverified", "close_failed", "not_opened"} and closed
                or result["diagnosis_complete"] and value != "closed"
                or key == "job_state" and value == "not_created"
                and any(name in result for name in ("worker", "worker_exit_code"))):
            raise DiagnosisRejected("controller_signal_invalid")


def decode_controller_signal(body: bytes, binding: dict[str, Any], launcher: tuple[int, float]) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise DiagnosisRejected("controller_signal_invalid")
            value[key] = item
        return value

    def invalid_number(value: str) -> Any:
        raise DiagnosisRejected("controller_signal_invalid")

    def finite_number(value: str) -> float:
        number = float(value)
        if not math.isfinite(number):
            raise DiagnosisRejected("controller_signal_invalid")
        return number

    try:
        if not 0 < len(body) <= 64 * 1024 or body.count(b"\n") != 1 or not body.endswith(b"\n"):
            raise DiagnosisRejected("controller_signal_invalid")
        frame = json.loads(body.decode("ascii"), object_pairs_hook=unique, parse_constant=invalid_number, parse_float=finite_number)
        if (type(frame) is not dict or set(frame) != {"schema", "plan", "role", "binding", "result"}
                or type(frame["schema"]) is not int or frame["schema"] != 3
                or frame["plan"] != PLAN or frame["role"] != "controller"):
            raise DiagnosisRejected("controller_signal_invalid")
        actual = frame["binding"]
        if (type(actual) is not dict or set(actual) != {*binding, "launcher_pid", "launcher_generation", "controller_pid", "controller_generation"}
                or any(actual.get(key) != value for key, value in binding.items())
                or any(type(actual.get(key)) is not int or actual[key] <= 0 for key in ("launcher_pid", "controller_pid", "parent_pid"))
                or any(type(actual.get(key)) not in {float, int} or not math.isfinite(actual[key]) for key in ("launcher_generation", "controller_generation", "parent_generation", "began"))
                or (actual["launcher_pid"], actual["launcher_generation"]) != launcher
                or actual["controller_pid"] == actual["launcher_pid"] and actual["controller_generation"] != actual["launcher_generation"]
                or actual["controller_generation"] < actual["launcher_generation"]):
            raise DiagnosisRejected("controller_signal_invalid")
        result = frame["result"]
        required = {"schema", "plan", "go", "cloud_requests", "diagnosis_complete", "forced", "tree_empty", "handles_closed", "directory_closed", "peak_rss_bytes", "blocked", "elapsed_seconds"}
        optional = {"environment", "worker", "worker_exit_code", "protection_pass", "final", "authority", "root_identity",
                    "job_state", "directory_state"}
        if (type(result) is not dict or not required <= set(result) or set(result) - required - optional
                or type(result["schema"]) is not int or result["schema"] != 3 or result["plan"] != PLAN
                or result["go"] is not False or type(result["cloud_requests"]) is not int or result["cloud_requests"] != 0
                or any(type(result[key]) is not bool for key in ("diagnosis_complete", "forced", "tree_empty", "handles_closed", "directory_closed"))
                or not finite_integer(result["peak_rss_bytes"])
                or result["peak_rss_bytes"] > 6 * 1024**3 and (result["diagnosis_complete"] or result["blocked"] is None)
                or type(result["elapsed_seconds"]) not in {float, int} or not math.isfinite(result["elapsed_seconds"]) or result["elapsed_seconds"] < 0
                or not finite_failure_value(result["blocked"])):
            raise DiagnosisRejected("controller_signal_invalid")
        validate_optional_controller_fields(result)
        if "authority" in result and (type(result["authority"]) is not dict
                or set(result["authority"]) != {"frozen", "input", "dataset", "protection"}
                or any(type(value) is not str or not re.fullmatch("[a-f0-9]{64}", value) for value in result["authority"].values())):
            raise DiagnosisRejected("controller_signal_invalid")
        if "root_identity" in result and (type(result["root_identity"]) is not list or len(result["root_identity"]) != 2
                or any(type(value) is not int or value < 0 for value in result["root_identity"])):
            raise DiagnosisRejected("controller_signal_invalid")
        if "environment" in result:
            environment = result["environment"]
            if (type(environment) is not dict or set(environment) != {"available_ram_bytes", "bge_cache_present", "cpu", "cloud_requests"}
                    or type(environment["available_ram_bytes"]) is not int or environment["available_ram_bytes"] < 6 * 1024**3
                    or environment["bge_cache_present"] is not True or environment["cpu"] is not True
                    or type(environment["cloud_requests"]) is not int or environment["cloud_requests"] != 0):
                raise DiagnosisRejected("controller_signal_invalid")
        if "worker" in result:
            worker = result["worker"]
            required_worker = {"schema", "plan", "go", "cloud_requests", "token", "diagnosis_complete",
                               "query_count", "blocked", "authority", "final", "directory_closed"}
            allowed_worker = {"schema", "plan", "go", "cloud_requests", "token", "diagnosis_complete", "query_count", "blocked",
                               "groups", "derivation_calls", "authority", "final", "directory_closed"}
            if (type(worker) is not dict or not required_worker <= set(worker) or set(worker) - allowed_worker
                    or type(worker.get("schema")) is not int or worker["schema"] != 3 or worker.get("plan") != PLAN
                    or worker.get("token") != binding["token"] or worker.get("go") is not False
                    or type(worker.get("cloud_requests")) is not int or worker["cloud_requests"] != 0
                    or type(worker.get("query_count")) is not int or not 0 <= worker["query_count"] <= 6
                    or type(worker.get("diagnosis_complete")) is not bool
                    or "authority" not in result or worker.get("authority") != result["authority"]
                    or worker.get("final") is not False or worker.get("directory_closed") is not False):
                raise DiagnosisRejected("controller_signal_invalid")
            validate_replay_report(worker)
        if result["diagnosis_complete"]:
            if (result.get("final") is not True or result["directory_closed"] is not True
                    or result["forced"] is not False or result["tree_empty"] is not True
                    or result["handles_closed"] is not True or result.get("protection_pass") is not True
                    or type(result.get("worker_exit_code")) is not int or result["worker_exit_code"] != 0
                    or result["blocked"] is not None or result["elapsed_seconds"] >= 420
                    or "authority" not in result or "root_identity" not in result):
                raise DiagnosisRejected("controller_signal_invalid")
            validate_replay_report(result["worker"])
            if result["worker"].get("diagnosis_complete") is not True:
                raise DiagnosisRejected("controller_signal_invalid")
        return result
    except (UnicodeError, ValueError, KeyError, TypeError, RecursionError, OverflowError):
        raise DiagnosisRejected("controller_signal_invalid") from None


def run_once() -> dict[str, Any]:
    """Guardian performs no proof/root IO, including after the Job is closed."""
    import psutil
    from aoao_wechat_job_core import WindowsJobCore, create_job_bound_command
    from Tools.probe_daily_memory_real import sample_tree_rss

    began = time.monotonic()
    current = psutil.Process()
    binding = {"token": uuid.uuid4().hex, "parent_pid": current.pid,
               "parent_generation": current.create_time(), "began": began,
               "job_name": "Local\\AoaoDailyProbe." + uuid.uuid4().hex}
    result: dict[str, Any] = {"schema": 3, "plan": PLAN, "role": "guardian", "go": False,
        "cloud_requests": 0, "diagnosis_complete": False, "forced": False, "tree_empty": False,
        "handles_closed": False, "directory_closed": False, "peak_rss_bytes": 0, "blocked": None,
        "final": False, "token": binding["token"]}
    process, job = None, None
    body = bytearray()
    try:
        if RETAINED:
            raise DiagnosisRejected("worker_job_invalid")
        job = WindowsJobCore(name=binding["job_name"])
        environment = dict(os.environ)
        environment.update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", TOKENIZERS_PARALLELISM="false",
                           AOAO_DAILY_REAL_API_TEST="0", AOAO_REAL_EMBEDDING_TEST="0", CUDA_VISIBLE_DEVICES="")
        process = create_job_bound_command(
            [str(Path(sys.executable).resolve()), "-B", str(Path(__file__).resolve()), "--controller",
             binding["token"], str(binding["parent_pid"]), str(binding["parent_generation"]), str(began), binding["job_name"]],
            {"cwd": str(REPO), "env": environment, "stdin": subprocess.DEVNULL,
             "stdout": subprocess.PIPE, "stderr": subprocess.DEVNULL, "bufsize": 0,
             "close_fds": True, "creationflags": subprocess.CREATE_NO_WINDOW}, began + 435,
            job_type=lambda: job)
        launcher = (process.pid, psutil.Process(process.pid).create_time())
        eof = False
        while time.monotonic() < began + 420:
            eof = read_available(process, body) or eof
            rss = sample_tree_rss(process.pid)
            result["peak_rss_bytes"] = max(result["peak_rss_bytes"], rss)
            if rss > 6 * 1024**3:
                raise DiagnosisRejected("memory_capacity_exceeded")
            if process.poll() is not None and job.active_processes() == 0 and eof:
                break
            time.sleep(.01)
        else:
            raise DiagnosisRejected("workflow_deadline")
        controller = decode_controller_signal(bytes(body), binding, launcher)
        result["controller"] = controller
        # Keep inner measurements even if the guardian missed a short-lived peak.
        result["peak_rss_bytes"] = max(result["peak_rss_bytes"], controller["peak_rss_bytes"])
        result["forced"] = controller["forced"]
        for key in ("worker", "environment", "directory_closed", "protection_pass", "blocked"):
            if key in controller:
                result[key] = controller[key]
        result["diagnosis_complete"] = controller["diagnosis_complete"] and process.returncode == 0
        if process.returncode != 0:
            result["blocked"] = result["blocked"] or "controller_exit_failed"
    except BaseException as exc:
        result.update(blocked=finite_failure(exc), diagnosis_complete=False)
        retained = getattr(exc, "retained_owner", None)
        if retained is not None:
            RETAINED.append(retained)
    finally:
        try:
            if process is not None:
                result["forced"] = result["forced"] or process.poll() is None or job is not None and job.active_processes() != 0
                process.quiesce_until(began + 435)
                result["tree_empty"] = job is not None and job.active_processes() == 0
                if process.stdout is not None:
                    process.stdout.close()
                process.close_until(began + 435)
            elif job is not None:
                result["forced"] = job.active_processes() != 0
                job.quiesce_until(began + 435)
                result["tree_empty"] = job.active_processes() == 0
                job.close()
            else:
                result["tree_empty"] = True
            result["handles_closed"] = True
        except BaseException:
            RETAINED.append(process if process is not None else job)
            result.update(blocked="job_close_failed", diagnosis_complete=False)
        result["elapsed_seconds"] = time.monotonic() - began
        result["final"] = True
        if (result["forced"] or not result["tree_empty"] or not result["handles_closed"]
                or RETAINED or result["elapsed_seconds"] >= 435):
            result["diagnosis_complete"] = False
            result["blocked"] = result["blocked"] or "workflow_deadline"
    return result


def close_authority_process(process: Any, deadline: float) -> bool:
    """Return success only after the existing Job primitive proves an empty tree."""
    try:
        job = process._worker_job
        process.quiesce_until(deadline)
        if job is None or job.active_processes() != 0:
            raise DiagnosisRejected("controller_exit_failed")
        if process.stdout is not None:
            process.stdout.close()
        process.close_until(deadline)
        return True
    except BaseException:
        if not any(owner is process for owner in RETAINED):
            RETAINED.append(process)
        return False


def authority_check_signal() -> dict[str, Any]:
    """Encode only finite authority facts, including a rejected check."""
    result: dict[str, Any] = {"schema": 1, "plan": PLAN, "checked": False,
                             "authority": None, "blocked": None, "cloud_requests": 0,
                             "ready_created": False, "attempt_consumed": False}
    try:
        result.update(authority=verify_authority(), checked=True)
    except BaseException as exc:
        result["blocked"] = finite_failure(exc)
    result.update(model_modules_loaded=any(name in sys.modules for name in (
        "torch", "transformers", "sentence_transformers", "FlagEmbedding")),
        ready_created=(ROOT / "ready.json").exists(),
        attempt_consumed=(ROOT / "attempt.json").exists())
    if result["model_modules_loaded"] or result["ready_created"] or result["attempt_consumed"]:
        result.update(checked=False, authority=None, blocked="proof_invalid")
    return result


def decode_authority_signal(body: bytes, exit_code: int) -> dict[str, Any]:
    """Accept one bounded, typed frame; no arbitrary child error text."""
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise DiagnosisRejected("proof_invalid")
            value[key] = item
        return value

    def invalid_number(value: str) -> Any:
        raise DiagnosisRejected("proof_invalid")

    try:
        if (type(body) is not bytes or not 0 < len(body) <= 64 * 1024
                or body.count(b"\n") != 1 or not body.endswith(b"\n")
                or type(exit_code) is not int):
            raise DiagnosisRejected("proof_invalid")
        signal = json.loads(body.decode("ascii"), object_pairs_hook=unique, parse_constant=invalid_number)
        fields = {"schema", "plan", "checked", "authority", "blocked", "model_modules_loaded",
                  "cloud_requests", "ready_created", "attempt_consumed"}
        if (type(signal) is not dict or set(signal) != fields
                or type(signal["schema"]) is not int or signal["schema"] != 1 or signal["plan"] != PLAN
                or type(signal["checked"]) is not bool or type(signal["model_modules_loaded"]) is not bool
                or type(signal["cloud_requests"]) is not int or signal["cloud_requests"] != 0
                or signal["ready_created"] is not False or signal["attempt_consumed"] is not False):
            raise DiagnosisRejected("proof_invalid")
        if signal["checked"]:
            authority = signal["authority"]
            if (exit_code != 0 or signal["blocked"] is not None or signal["model_modules_loaded"]
                    or type(authority) is not dict or set(authority) != {
                        "schema", "plan", "frozen", "input", "dataset", "protection"}
                    or type(authority["schema"]) is not int or authority["schema"] != 3
                    or authority["plan"] != PLAN
                    or any(type(authority[key]) is not str or re.fullmatch("[0-9a-f]{64}", authority[key]) is None
                           for key in ("frozen", "input", "dataset", "protection"))):
                raise DiagnosisRejected("proof_invalid")
        elif (exit_code != 1 or signal["authority"] is not None or signal["blocked"] is None
              or not finite_failure_value(signal["blocked"])):
            raise DiagnosisRejected("proof_invalid")
        return signal
    except (ValueError, UnicodeError, KeyError, TypeError, RecursionError, OverflowError):
        raise DiagnosisRejected("proof_invalid") from None


def run_authority_preflight(*, deadline: float | None = None) -> dict[str, Any]:
    """One fixed zero-model child, bounded by a single startup/work/cleanup deadline."""
    from aoao_wechat_job_core import create_job_bound_command

    began = time.monotonic()
    end = began + 180 if deadline is None else deadline
    if type(end) not in {int, float} or not math.isfinite(end) or end <= began:
        raise DiagnosisRejected("work_deadline")
    validation = ROOT / "validation/v08-closure-r2"
    ordinary_directory(ROOT)
    validation.mkdir(parents=True, exist_ok=True)
    ordinary_directory(validation)
    start = validation / "authority-preflight-start.json"
    # Exclusive creation is also the refusal-to-retry record after a crash.
    with start.open("xb") as stream:
        stream.write(encoded({"plan": PLAN, "deadline_seconds": end - began,
                              "cloud_requests": 0, "model_loads": 0}))
    result: dict[str, Any] = {"plan": PLAN, "checked": False, "tree_empty": False,
                             "cloud_requests": 0, "model_modules_loaded": None,
                             "ready_created": False, "attempt_consumed": False,
                             "authority": None, "blocked": None}
    code = (
        "import sys,json,logging;from pathlib import Path;"
        f"p=Path({str(REPO)!r});sys.path[:0]=[str(p),str(p/'apps')];"
        "logging.disable(logging.CRITICAL);"
        "from Tools import probe_daily_memory_recall_diag as d;"
        "r=d.authority_check_signal();"
        "print(json.dumps(r,ensure_ascii=True,allow_nan=False));"
        "sys.exit(0 if r['checked'] else 1)"
    )
    process = None
    try:
        process = create_job_bound_command(
            [str(REPO / "runtime_env/Scripts/python.exe"), "-B", "-c", code],
            {"stdin": subprocess.DEVNULL, "stdout": subprocess.PIPE,
             "stderr": subprocess.DEVNULL, "bufsize": 0, "cwd": str(REPO),
             "close_fds": True, "creationflags": subprocess.CREATE_NO_WINDOW}, end)
        work_end = end - min(10, (end - began) / 4)
        body = bytearray()
        while True:
            eof = read_available(process, body)
            if process.poll() is not None and eof:
                break
            if time.monotonic() >= work_end:
                raise DiagnosisRejected("work_deadline")
            time.sleep(min(.01, max(0, work_end - time.monotonic())))
        response = decode_authority_signal(bytes(body), process.poll())
        result["model_modules_loaded"] = response["model_modules_loaded"]
        if not response["checked"]:
            raise DiagnosisRejected(response["blocked"])
        result.update(checked=True, authority=response["authority"])
    except BaseException as exc:
        result["blocked"] = finite_failure(exc)
        retained = getattr(exc, "retained_owner", None)
        if process is None and retained is not None:
            RETAINED.append(retained)
    finally:
        if process is not None:
            result["tree_empty"] = close_authority_process(process, end)
            if not result["tree_empty"]:
                result.update(checked=False, blocked="controller_exit_failed")
        result.update(elapsed_seconds=time.monotonic() - began,
                      ready_created=(ROOT / "ready.json").exists(),
                      attempt_consumed=(ROOT / "attempt.json").exists())
        if result["ready_created"] or result["attempt_consumed"]:
            result.update(checked=False, blocked="ready_gate_invalid")
        if result["elapsed_seconds"] >= end - began:
            result.update(checked=False, blocked=result["blocked"] or "work_deadline")
        with (validation / "authority-preflight.json").open("xb") as stream:
            stream.write(encoded(result))
    return result


def main(arguments: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    try:
        if len(arguments) == 2 and arguments[0] == "--worker":
            return run_worker(arguments[1])
        if len(arguments) == 6 and arguments[0] == "--controller":
            binding = controller_binding(arguments[1:])
            result = run_controller(binding)
            frame = {"schema": 3, "plan": PLAN, "role": "controller", "binding": binding, "result": result}
            body = encoded(frame) + b"\n"
            if len(body) > 64 * 1024:
                raise DiagnosisRejected("controller_signal_invalid")
            sys.stdout.buffer.write(body)
            sys.stdout.buffer.flush()
            return 0 if result["diagnosis_complete"] else 1
        if arguments == ["--authority-preflight"]:
            result = run_authority_preflight()
        elif arguments == ["--run-once"]:
            result = run_once()
        elif arguments in ([], ["--check"]):
            result = {
                **verify_authority(),
                "cloud_requests": 0,
                "go": False,
                "checked": True,
            }
        else:
            raise DiagnosisRejected("fixed_root_required")
    except BaseException as exc:
        result = {
            "plan": PLAN,
            "cloud_requests": 0,
            "go": False,
            "blocked": finite_failure(exc),
        }
    print(json.dumps(result, ensure_ascii=True, allow_nan=False))
    return (
        0
        if result.get("checked") is True or result.get("diagnosis_complete") is True
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
