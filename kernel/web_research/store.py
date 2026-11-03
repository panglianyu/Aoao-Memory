"""Task-scoped immutable snapshots; never read or write the knowledge database."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import threading
import time

from protocols.web_research import ID_PATTERN, SourceSnapshot

_SCOPE = re.compile(r"^[0-9a-f]{64}$")
_MAX_FILE_BYTES = 2 * 1024 * 1024
_STORE_LOCK = threading.RLock()


class SnapshotStore:
    def __init__(self, root: Path, *, retention_days: int = 7,
                 storage_limit_bytes: int = 256 * 1024 * 1024):
        self.root = Path(root).absolute()
        if self.root.resolve() != self.root or any(p.is_symlink() for p in (self.root, *self.root.parents)):
            raise ValueError("invalid_artifact_path")
        self.retention_seconds = retention_days * 86400
        self.storage_limit = storage_limit_bytes
        self._lock = _STORE_LOCK
        self._active: set[str] = set()

    def _path(self, scope: str, filename: str) -> Path:
        if not _SCOPE.fullmatch(scope) or not re.fullmatch(r"(?:snap_[0-9a-f]{32}|state|display|history)\.json", filename):
            raise ValueError("invalid_artifact_id")
        path = self.root / "runs" / scope / filename
        if any(p.is_symlink() for p in (self.root, path.parent.parent, path.parent, path)):
            raise ValueError("invalid_artifact_path")
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("invalid_artifact_path")
        return path

    def pin(self, scope: str) -> None:
        with self._lock:
            self._path(scope, "state.json")
            self._active.add(scope)

    def unpin(self, scope: str) -> None:
        with self._lock:
            self._active.discard(scope)

    def _write(self, scope: str, filename: str, data: dict) -> None:
        path = self._path(scope, filename)
        encoded = json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(encoded) > _MAX_FILE_BYTES:
            raise ValueError("response_too_large")
        with self._lock:
            self.cleanup()
            total = sum(self._path(p.parent.name, p.name).stat().st_size
                        for p in self.root.glob("runs/*/*.json") if not p.is_symlink())
            existing_size = path.stat().st_size if path.exists() else 0
            if total - existing_size + len(encoded) > self.storage_limit:
                raise ValueError("storage_limit")
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, name = tempfile.mkstemp(prefix=".snapshot-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, path)
            finally:
                Path(name).unlink(missing_ok=True)

    def save(self, scope: str, snapshot: SourceSnapshot) -> None:
        self._write(scope, snapshot.snapshot_id + ".json", snapshot.to_dict())

    def save_state(self, scope: str, state: dict) -> None:
        self._write(scope, "state.json", state)

    def _read(self, scope: str, filename: str) -> dict:
        path = self._path(scope, filename)
        with self._lock:
            try:
                with path.open("rb") as stream:
                    if time.time() - os.fstat(stream.fileno()).st_mtime > self.retention_seconds:
                        raise ValueError("artifact_expired")
                    content = stream.read(_MAX_FILE_BYTES + 1)
            except FileNotFoundError as exc:
                raise ValueError("artifact_expired") from exc
        if len(content) > _MAX_FILE_BYTES:
            raise ValueError("response_too_large")
        data = json.loads(content)
        if type(data) is not dict:
            raise ValueError("invalid_artifact")
        return data

    def load(self, scope: str, snapshot_id: str) -> SourceSnapshot:
        if not ID_PATTERN.fullmatch(snapshot_id) or not snapshot_id.startswith("snap_"):
            raise ValueError("invalid_artifact_id")
        snapshot = SourceSnapshot.from_dict(self._read(scope, snapshot_id + ".json"))
        if snapshot.snapshot_id != snapshot_id:
            raise ValueError("invalid_artifact")
        return snapshot

    def load_state(self, scope: str) -> dict:
        return self._read(scope, "state.json")

    def cleanup(self) -> None:
        with self._lock:
            cutoff = time.time() - self.retention_seconds
            for directory in (self.root / "runs").glob("*"):
                if directory.is_symlink() or not _SCOPE.fullmatch(directory.name) or directory.name in self._active:
                    continue
                for path in directory.glob("*.json"):
                    safe = self._path(directory.name, path.name)
                    if safe.stat().st_mtime < cutoff:
                        safe.unlink()
