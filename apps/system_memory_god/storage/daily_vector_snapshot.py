"""Bounded numerical candidate cache. Raw and SourceLedger remain authorities."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
import uuid
from typing import Callable

import numpy as np

from kernel.memory_pipeline_models import PipelineScope, canonical, identity
from .pipeline_database import reject_directory_link, resolved_pipeline_path
from .daily_snapshot_file_ops import SnapshotFileOps

MAX_DIMENSION = 2048
MAX_BODY = 40 * 1024**2
MAX_DIRECTORY = 2 * 1024**2
MAX_TOTAL = 768 * 1024**2
_HASH = re.compile(r"[0-9a-f]{64}")
_FILES = {"vectors.f32", "ids.json", "manifest.json"}


class SnapshotCapacityError(ValueError):
    """Do not silently truncate the searchable corpus when capacity is reached."""


class SnapshotLeasePressure(SnapshotCapacityError):
    """An old, genuinely leased generation must retire before publication."""


def snapshot_capacity(body_bytes: int, directory_bytes: int | None = None) -> None:
    """Check whole-generation byte budgets before allocating its full bodies."""
    if (type(body_bytes) is not int or body_bytes < 0
            or directory_bytes is not None
            and (type(directory_bytes) is not int or directory_bytes < 2)):
        raise ValueError("invalid snapshot size")
    if (body_bytes > MAX_BODY
            or directory_bytes is not None and directory_bytes > MAX_DIRECTORY):
        raise SnapshotCapacityError("vector_capacity_exceeded")


def _cache_path(path: Path) -> Path:
    """Validate ordinary Win32 spelling before using its long-path equivalent."""
    _directory(path)
    resolved = resolved_pipeline_path(path)
    if os.name != "nt":
        return resolved
    text = str(resolved)
    return Path("\\\\?\\UNC\\" + text[2:] if text.startswith("\\\\") else "\\\\?\\" + text)


def _directory(path: Path) -> None:
    for parent in (*reversed(path.parents), path):
        reject_directory_link(parent)


def _read(path: Path, maximum: int) -> bytes:
    _directory(path.parent)
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_size > maximum
            or getattr(before, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise ValueError("unsafe snapshot file")
    with path.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ValueError("snapshot file changed")
        body = handle.read(maximum + 1)
        after = os.fstat(handle.fileno())
    if (len(body) > maximum or (opened.st_size, opened.st_mtime_ns)
            != (after.st_size, after.st_mtime_ns)):
        raise ValueError("snapshot file changed")
    _directory(path.parent)
    current = path.lstat()
    if (current.st_nlink != 1 or after.st_nlink != 1
            or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
            != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            or getattr(current, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise ValueError("snapshot file changed")
    return body


def _json(body: bytes):
    def constant(value):
        raise ValueError("invalid snapshot number")
    def pairs(items):
        output = {}
        for key, value in items:
            if key in output:
                raise ValueError("duplicate snapshot field")
            output[key] = value
        return output
    return json.loads(body, parse_constant=constant, object_pairs_hook=pairs)


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _write(path: Path, body: bytes, *, on_open: Callable[[os.stat_result], None] | None = None) -> None:
    with path.open("xb") as handle:
        if on_open is not None:
            on_open(os.fstat(handle.fileno()))
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


@dataclass(frozen=True)
class SnapshotLease:
    scope: PipelineScope
    generation: str
    token: str


@dataclass
class _RollbackGeneration:
    before: os.stat_result
    files: dict[str, tuple[os.stat_result, bytes]] = field(default_factory=dict)


@dataclass(frozen=True)
class SnapshotData:
    ids: tuple[str, ...]
    matrix: np.ndarray
    norms: np.ndarray

    def cosine_hits(self, query: list[float], *, limit: int) -> tuple[tuple[str, float], ...]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("invalid snapshot TopK")
        vector = np.asarray(query, dtype=np.float64)
        if vector.shape != (self.matrix.shape[1],) or not np.isfinite(vector).all():
            raise ValueError("invalid snapshot query")
        norm = np.linalg.norm(vector)
        if not np.isfinite(norm) or norm <= 0:
            raise ValueError("invalid snapshot query")
        scores = np.einsum("ij,j->i", self.matrix, vector, dtype=np.float64) / (self.norms * norm)
        order = sorted(range(len(self.ids)), key=lambda row: (-scores[row], self.ids[row]))[:limit]
        return tuple((self.ids[row], float(np.clip(scores[row], -1, 1))) for row in order)


def load_snapshot(root: Path, scope: PipelineScope, generation: str, *, embedding: str,
                  binding_sha256: str | None = None) -> SnapshotData:
    if binding_sha256 is not None:
        if type(binding_sha256) is not str or not _HASH.fullmatch(binding_sha256) or embedding != "bge-m3":
            raise ValueError("invalid formal snapshot binding")
    if (not isinstance(root, Path) or not root.is_absolute() or type(scope) is not PipelineScope
            or type(generation) is not str or not _HASH.fullmatch(generation)
            or embedding not in {"synthetic", "bge-m3"}):
        raise ValueError("invalid snapshot identity")
    root = _cache_path(root)
    path = root / scope.partition_id / generation
    _directory(path)
    if {entry.name for entry in path.iterdir()} != _FILES:
        raise ValueError("unknown snapshot entry")
    manifest = _json(_read(path / "manifest.json", 4096))
    if (type(manifest) is not dict or set(manifest) != {
            "schema", "scope", "embedding", "rows", "dimension", "body_sha256", "ids_sha256"} | ({"binding_sha256"} if binding_sha256 is not None else set())
            or type(manifest["schema"]) is not int or manifest["schema"] != (2 if binding_sha256 is not None else 1)
            or binding_sha256 is not None and manifest.get("binding_sha256") != binding_sha256
            or manifest["scope"] != scope.partition_id or manifest["embedding"] != embedding
            or _hash(canonical(manifest).encode()) != generation
            or type(manifest["rows"]) is not int or manifest["rows"] < 0
            or type(manifest["dimension"]) is not int or not 1 <= manifest["dimension"] <= MAX_DIMENSION
            or binding_sha256 is not None and manifest["rows"] > 0 and manifest["dimension"] != 1024):
        raise ValueError("invalid snapshot manifest")
    expected_size = manifest["rows"] * manifest["dimension"] * 4
    snapshot_capacity(expected_size)
    body = _read(path / "vectors.f32", expected_size)
    ids_body = _read(path / "ids.json", MAX_DIRECTORY)
    if (len(body) != expected_size or len(body) > MAX_BODY
            or _hash(body) != manifest["body_sha256"] or _hash(ids_body) != manifest["ids_sha256"]):
        raise ValueError("snapshot integrity failed")
    ids = _json(ids_body)
    if type(ids) is not list or len(ids) != manifest["rows"]:
        raise ValueError("invalid snapshot ID directory")
    for key in ids:
        identity(key)
    if ids != sorted(set(ids)):
        raise ValueError("invalid snapshot ID directory")
    matrix = np.frombuffer(body, dtype="<f4").reshape(len(ids), manifest["dimension"])
    norms = np.sqrt(np.einsum("ij,ij->i", matrix, matrix, dtype=np.float64))
    if not np.isfinite(matrix).all() or not np.isfinite(norms).all() or (norms <= 0).any():
        raise ValueError("invalid snapshot vector")
    norms.flags.writeable = False
    return SnapshotData(tuple(ids), matrix, norms)


class DailyVectorSnapshot:
    def __init__(self, root: Path, scopes: tuple[PipelineScope, ...], *, embedding: str,
                 binding_sha256: str | None = None):
        if binding_sha256 is not None:
            if (type(binding_sha256) is not str or not _HASH.fullmatch(binding_sha256)
                    or embedding != "bge-m3"):
                raise ValueError("invalid formal snapshot binding")
        if os.name != "nt":
            raise OSError("bound snapshot cache requires Windows")
        if (not isinstance(root, Path) or not root.is_absolute() or type(scopes) is not tuple
                or not 1 <= len(scopes) <= 8 or any(type(scope) is not PipelineScope for scope in scopes)
                or len(set(scopes)) != len(scopes) or embedding not in {"synthetic", "bge-m3"}):
            raise ValueError("invalid snapshot configuration")
        ordinary = Path(str(root).removeprefix("\\\\?\\"))
        if ordinary.parent in {Path(ordinary.anchor), Path.home()}:
            raise ValueError("explicit owned snapshot parent required")
        root = _cache_path(root)
        _directory(root)
        root.mkdir(parents=True, exist_ok=True)
        _directory(root)
        # A fresh, explicit cache root; recovery reconstructs from original receipts.
        if any(root.iterdir()):
            raise ValueError("snapshot root must be empty")
        self.root, self.scopes, self.embedding = root, scopes, embedding
        self.binding_sha256 = binding_sha256
        self.formal_snapshot = None
        self._files = SnapshotFileOps(root)
        self._current: dict[PipelineScope, str] = {}
        self._generations: dict[tuple[PipelineScope, str], int] = {}
        self._leases: dict[str, SnapshotLease] = {}
        self._pending_releases: dict[str, SnapshotLease] = {}
        self.on_lease_released: Callable[[PipelineScope], None] | None = None
        self._lock = threading.RLock()
        self._closed = False
        self._disposed = False
        self._removed_pointers: set[PipelineScope] = set()
        self._owned_partitions: dict[PipelineScope, None] = {}
        self._rollback_generations: dict[tuple[PipelineScope, str], _RollbackGeneration] = {}
        self._rollback_pointers: dict[tuple[PipelineScope, str], tuple[os.stat_result, bytes]] = {}

    def publish(self, scope: PipelineScope, vectors: dict[str, bytes]) -> str:
        with self._lock:
            if self._closed or scope not in self.scopes or type(vectors) is not dict:
                raise ValueError("snapshot scope unavailable")
            self._resume_rollbacks()
            if vectors:
                first = next(iter(vectors.values()))
                if type(first) is not bytes:
                    raise ValueError("invalid snapshot vector shape")
                dimension = len(first) // 4
            else:
                dimension = 16
            if (not 1 <= dimension <= MAX_DIMENSION
                    or self.binding_sha256 is not None and vectors and dimension != 1024):
                raise ValueError("invalid snapshot vector shape")
            expected_size = len(vectors) * dimension * 4
            snapshot_capacity(expected_size)
            directory_size = 2
            for index, (key, vector) in enumerate(vectors.items()):
                identity(key)
                if type(vector) is not bytes or len(vector) != dimension * 4:
                    raise ValueError("invalid snapshot vector shape")
                directory_size += len(canonical(key).encode("ascii")) + (1 if index else 0)
                snapshot_capacity(expected_size, directory_size)
            ids = sorted(vectors)
            body = b"".join(vectors[key] for key in ids)
            ids_body = canonical(ids).encode()
            if len(body) != expected_size or len(ids_body) != directory_size:
                raise ValueError("snapshot size changed")
            manifest = {"schema": 1, "scope": scope.partition_id, "embedding": self.embedding,
                        "rows": len(ids), "dimension": dimension,
                        "body_sha256": _hash(body), "ids_sha256": _hash(ids_body)}
            if self.binding_sha256 is not None:
                manifest.update(schema=2, binding_sha256=self.binding_sha256)
            manifest_body = canonical(manifest).encode()
            generation = _hash(manifest_body)
            if self._current.get(scope) == generation:
                load_snapshot(self.root, scope, generation, embedding=self.embedding, binding_sha256=self.binding_sha256)
                return generation
            if sum(key[0] == scope for key in self._generations) >= 2:
                raise SnapshotLeasePressure("snapshot_lease_pressure")
            if sum(self._generations.values()) + len(body) + len(ids_body) + len(manifest_body) > MAX_TOTAL:
                raise SnapshotCapacityError("vector_capacity_exceeded")
            parent = self.root / scope.partition_id
            _directory(parent)
            parent.mkdir(exist_ok=True)
            self._owned_partitions[scope] = None
            self._files.capture_partition(scope.partition_id)
            staging = parent / ("staging-" + uuid.uuid4().hex)
            staging.mkdir()
            receipt = _RollbackGeneration(staging.lstat())
            self._rollback_generations[scope, staging.name] = receipt
            target = parent / generation
            pointer = parent / ("pointer-" + uuid.uuid4().hex)
            try:
                if target.exists():
                    raise ValueError("unexpected snapshot generation")
                for name, contents in (("vectors.f32", body), ("ids.json", ids_body),
                                       ("manifest.json", manifest_body)):
                    _write(staging / name, contents,
                           on_open=lambda info: receipt.files.__setitem__(name, (info, contents)))
                staging.rename(target)
                self._rollback_generations[scope, generation] = self._rollback_generations.pop((scope, staging.name))
                load_snapshot(self.root, scope, generation, embedding=self.embedding, binding_sha256=self.binding_sha256)
                pointer_body = canonical({"schema": 1, "generation": generation}).encode()
                _write(pointer, pointer_body, on_open=lambda info: self._rollback_pointers.__setitem__(
                    (scope, pointer.name), (info, pointer_body)))
                pointer.replace(parent / "current.json")
                del self._rollback_pointers[scope, pointer.name]
            except BaseException:
                self._resume_rollbacks()
                raise
            del self._rollback_generations[scope, generation]
            self._generations[scope, generation] = len(body) + len(ids_body) + len(manifest_body)
            self._current[scope] = generation
            if self.binding_sha256 is not None:
                self.formal_snapshot = {"rows": len(ids), "dimension": dimension} if ids else None
            self._collect()
            return generation

    def acquire(self, scope: PipelineScope) -> SnapshotLease:
        with self._lock:
            if self._closed or scope not in self._current:
                raise RuntimeError("snapshot not prepared")
            pointer = _json(_read(self.root / scope.partition_id / "current.json", 256))
            if pointer != {"schema": 1, "generation": self._current[scope]}:
                raise ValueError("snapshot pointer changed")
            lease = SnapshotLease(scope, self._current[scope], uuid.uuid4().hex)
            self._leases[lease.token] = lease
            return lease

    def release(self, lease: SnapshotLease) -> None:
        with self._lock:
            if type(lease) is not SnapshotLease or (
                    self._leases.get(lease.token) != lease
                    and self._pending_releases.get(lease.token) != lease):
                raise ValueError("invalid snapshot lease")
            self._leases.pop(lease.token, None)
            self._pending_releases[lease.token] = lease
            self._collect()
            del self._pending_releases[lease.token]
            if self.on_lease_released is not None:
                self.on_lease_released(lease.scope)

    def lease_pressure(self, scope: PipelineScope) -> bool:
        with self._lock:
            return sum(key[0] == scope for key in self._generations) >= 2

    def _collect(self) -> None:
        occupied = {(lease.scope, lease.generation) for lease in self._leases.values()}
        for scope, generation in tuple(self._generations):
            if (scope, generation) in occupied or self._current.get(scope) == generation and not self._closed:
                continue
            self._files.remove_generation(scope.partition_id, generation,
                lambda: load_snapshot(self.root, scope, generation, embedding=self.embedding, binding_sha256=self.binding_sha256))
            del self._generations[scope, generation]

    def _resume_rollbacks(self) -> None:
        # Pin all produced files before the first delete can fail and skip
        # later objects. First proofs must match identities/content we wrote.
        for (scope, generation), receipt in tuple(self._rollback_generations.items()):
            self._files.capture_generation(scope.partition_id, generation, receipt.before, receipt.files)
        for (scope, name), (before, body) in tuple(self._rollback_pointers.items()):
            self._files.remove_pointer(scope.partition_id, name,
                lambda: _read(self.root / scope.partition_id / name, 256), before=before, contents=body)
            del self._rollback_pointers[scope, name]
        for (scope, generation), receipt in tuple(self._rollback_generations.items()):
            self._files.remove_generation(scope.partition_id, generation, before=receipt.before)
            del self._rollback_generations[scope, generation]

    def close(self) -> None:
        with self._lock:
            if self._disposed:
                return
            self._closed = True
            if self._leases:
                raise RuntimeError("snapshot leases still active")
            self._resume_rollbacks()
            self._collect()
            for scope in tuple(self._owned_partitions):
                parent = self.root / scope.partition_id
                if scope in self._current and scope not in self._removed_pointers:
                    self._files.remove_pointer(scope.partition_id, "current.json",
                                               lambda: _read(parent / "current.json", 256))
                    self._removed_pointers.add(scope)
                self._files.remove_empty(scope.partition_id)
                self._current.pop(scope, None)
                self._removed_pointers.discard(scope)
                del self._owned_partitions[scope]
            self._files.remove_empty()
            self._pending_releases.clear()
            self._disposed = True
