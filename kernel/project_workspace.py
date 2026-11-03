"""宿主项目目录授权、任务副本与结果回存服务。"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import secrets
import stat
import threading
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, BinaryIO, Iterable, Iterator

from kernel.principal import Principal
from kernel.project_workspace_io import (
    WorkspaceIOError,
    final_open_path as _bound_final_open_path,
    hold_directory_chain as _bound_directory_chain,
    open_bound_read_file,
    publish_opened_file,
)

if TYPE_CHECKING:
    from kernel.project_results import ProjectResultFile


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATE_FILE = PROJECT_ROOT / "runtime" / "project_workspaces" / "grants.json"
DEFAULT_TASK_ROOT = PROJECT_ROOT / "runtime" / "project_workspaces" / "tasks"
DEFAULT_MANAGED_DATA_ROOT = PROJECT_ROOT / "data"
MAX_TASK_FILES = 16
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_TASK_BYTES = 128 * 1024 * 1024
_WORKSPACE_ID_RE = re.compile(r"^project_[0-9a-f]{16}$")
_TASK_ID_RE = re.compile(r"^projecttask_[0-9a-f]{16}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_STORE_LOCK = threading.RLock()


class ProjectWorkspaceError(ValueError):
    """项目授权或任务文件不满足安全边界。"""


@dataclass(frozen=True, slots=True, repr=False)
class ProjectTaskInput:
    """已按任务清单复核的内存副本，不携带任何宿主路径。"""

    relative_path: str
    data: bytes
    size_bytes: int
    sha256: str


def _require_owner(principal: Principal | None) -> None:
    if principal is None or not principal.is_owner:
        raise ProjectWorkspaceError("项目工作区只允许主人使用")


def _relative_path(value: str | os.PathLike[str]) -> PurePosixPath:
    text = str(value or "").strip().replace("\\", "/")
    path = PurePosixPath(text)
    if (
        not text
        or len(text) > 512
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(":" in part or _CONTROL_RE.search(part) for part in path.parts)
    ):
        raise ProjectWorkspaceError("必须提供项目内的安全相对路径")
    return path


def _reparse_or_link(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & 0x400
    )


def _reject_reparse_chain(path: Path, stop: Path) -> None:
    current = path
    while True:
        if _reparse_or_link(current):
            raise ProjectWorkspaceError("授权路径不能经过符号链接或重解析点")
        if current == stop:
            return
        if current.parent == current:
            raise ProjectWorkspaceError("授权路径边界非法")
        current = current.parent


def _authorized_root(value: str | os.PathLike[str]) -> Path:
    raw = Path(str(value or "").strip())
    if not raw.is_absolute() or str(raw).startswith("\\\\"):
        raise ProjectWorkspaceError("项目目录必须是本机绝对路径")
    try:
        resolved = raw.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ProjectWorkspaceError("项目目录不存在或无法读取") from exc
    if not resolved.is_dir() or resolved == Path(resolved.anchor):
        raise ProjectWorkspaceError("只能授权具体项目目录，不能授权文件或整盘")
    _reject_reparse_chain(raw, Path(raw.anchor))
    return resolved


def _json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            raw = handle.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ProjectWorkspaceError("项目工作区记录损坏") from exc
    if not isinstance(value, dict):
        raise ProjectWorkspaceError("项目工作区记录损坏")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        publish_opened_file(
            io.BytesIO(payload),
            path,
            root=path.parent,
            max_bytes=1024 * 1024,
            replace=True,
        )
    except WorkspaceIOError as exc:
        raise ProjectWorkspaceError(str(exc)) from exc


def _final_open_path(handle: BinaryIO) -> Path:
    """从已打开句柄取真实路径，避免检查后按旧路径重新打开。"""
    return _bound_final_open_path(handle)


@contextmanager
def _open_checked_file(root: Path, lexical: Path) -> Iterator[tuple[BinaryIO, os.stat_result]]:
    """打开一次后用句柄核对边界，后续读取始终复用该句柄。"""
    handle: BinaryIO | None = None
    try:
        handle = open_bound_read_file(root, lexical)
        final = _final_open_path(handle)
        final.relative_to(root)
        _reject_reparse_chain(lexical, root)
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_FILE_BYTES:
            raise ProjectWorkspaceError("所选文件类型或大小不受支持")
    except ProjectWorkspaceError:
        if handle is not None:
            handle.close()
        raise
    except (OSError, RuntimeError, ValueError, WorkspaceIOError) as exc:
        if handle is not None:
            handle.close()
        raise ProjectWorkspaceError("文件不存在、越过授权边界或无法安全打开") from exc

    try:
        yield handle, info
    finally:
        handle.close()


def _hash_opened(handle: BinaryIO) -> tuple[str, int]:
    handle.seek(0)
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = handle.read(min(1024 * 1024, MAX_FILE_BYTES + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_FILE_BYTES:
            raise ProjectWorkspaceError("文件读取量超过 32 MiB 上限")
        digest.update(chunk)
    handle.seek(0)
    if total <= 0:
        raise ProjectWorkspaceError("空文件不受支持")
    return digest.hexdigest(), total


def _ensure_safe_directory(path: Path, stop: Path, *, create: bool) -> Path:
    try:
        if create:
            path.mkdir(exist_ok=True)
        _reject_reparse_chain(path, stop)
        resolved = path.resolve(strict=True)
        resolved.relative_to(stop)
    except ProjectWorkspaceError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise ProjectWorkspaceError("任务目录不存在、越界或包含重解析点") from exc
    if not resolved.is_dir():
        raise ProjectWorkspaceError("任务目录类型非法")
    return resolved


def _ensure_safe_directory_chain(root: Path, parts: Iterable[str]) -> Path:
    current = _ensure_safe_directory(root, root, create=False)
    for part in parts:
        with _hold_directory_chain(root, current):
            current = current / part
            _ensure_safe_directory(current, root, create=True)
    return current


@contextmanager
def _hold_directory_chain(root: Path, directory: Path) -> Iterator[Path]:
    """固定目标目录链，阻止发布期间目录被移动或替换成重解析点。"""
    _ensure_safe_directory(root, root, create=False)
    _ensure_safe_directory(directory, root, create=False)
    manager = _bound_directory_chain(root, directory)
    try:
        safe_directory = manager.__enter__()
    except WorkspaceIOError as exc:
        raise ProjectWorkspaceError(str(exc)) from exc

    try:
        yield safe_directory
    finally:
        manager.__exit__(None, None, None)


def _operation_id(task_id: str, output: str, destination: str, digest: str) -> str:
    raw = "\0".join((task_id, output, destination, digest)).encode("utf-8")
    return f"commit_{hashlib.sha256(raw).hexdigest()[:24]}"


@contextmanager
def _hold_unchanged_task_inputs(
    manifest: dict[str, Any],
    root: Path,
) -> Iterator[None]:
    """核对任务输入，并保持拒绝写入的句柄直到结果发布结束。"""
    with ExitStack() as stack:
        for item in manifest["inputs"]:
            if not isinstance(item, dict):
                raise ProjectWorkspaceError("项目任务输入记录损坏")
            relative = _relative_path(item.get("relative_path", ""))
            source = root.joinpath(*relative.parts)
            source_handle, _source_info = stack.enter_context(
                _open_checked_file(root, source)
            )
            current_hash, current_size = _hash_opened(source_handle)
            if (
                current_size != item.get("size_bytes")
                or current_hash != item.get("sha256")
            ):
                raise ProjectWorkspaceError(
                    "任务源文件已变化，拒绝回存旧结果"
                )
        yield


class ProjectWorkspaceStore:
    """保存精确目录授权，并只把显式选择的文件复制到 Aoao 任务区。"""

    def __init__(
        self,
        *,
        state_file: Path = DEFAULT_STATE_FILE,
        task_root: Path = DEFAULT_TASK_ROOT,
        managed_data_root: Path = DEFAULT_MANAGED_DATA_ROOT,
    ) -> None:
        self.state_file = Path(state_file).resolve()
        self.task_root = Path(task_root).resolve()
        self.managed_data_root = Path(managed_data_root).resolve()

    def _load_grants(self) -> list[dict[str, Any]]:
        if not self.state_file.exists():
            return []
        value = _json_object(self.state_file)
        if set(value) != {"version", "workspaces"} or value["version"] != 1:
            raise ProjectWorkspaceError("项目工作区授权记录版本不兼容")
        rows = value["workspaces"]
        if not isinstance(rows, list) or len(rows) > 64:
            raise ProjectWorkspaceError("项目工作区授权记录损坏")
        checked: list[dict[str, Any]] = []
        for row in rows:
            if (
                not isinstance(row, dict)
                or set(row) != {
                    "workspace_id", "label", "root_path", "owner_scope", "created_at",
                }
                or not _WORKSPACE_ID_RE.fullmatch(str(row.get("workspace_id", "")))
                or row.get("owner_scope") != "owner"
                or not isinstance(row.get("label"), str)
                or not isinstance(row.get("root_path"), str)
                or type(row.get("created_at")) not in {int, float}
            ):
                raise ProjectWorkspaceError("项目工作区授权记录损坏")
            checked.append(dict(row))
        if len({row["workspace_id"] for row in checked}) != len(checked):
            raise ProjectWorkspaceError("项目工作区授权记录损坏")
        return checked

    def _save_grants(self, grants: list[dict[str, Any]]) -> None:
        _write_json(self.state_file, {"version": 1, "workspaces": grants})

    @staticmethod
    def _label(value: str, root: Path) -> str:
        label = str(value or "").strip() or root.name
        if len(label) > 80 or _CONTROL_RE.search(label):
            raise ProjectWorkspaceError("项目工作区名称非法")
        return label

    def authorize(
        self,
        root_path: str | os.PathLike[str],
        *,
        label: str,
        principal: Principal | None,
    ) -> dict[str, Any]:
        _require_owner(principal)
        root = _authorized_root(root_path)
        safe_label = self._label(label, root)
        with _STORE_LOCK:
            grants = self._load_grants()
            existing = next(
                (row for row in grants if Path(row["root_path"]) == root),
                None,
            )
            if existing is not None:
                existing["label"] = safe_label
                self._save_grants(grants)
                return dict(existing)
            if len(grants) >= 64:
                raise ProjectWorkspaceError("项目工作区授权数量已达上限")
            row = {
                "workspace_id": f"project_{secrets.token_hex(8)}",
                "label": safe_label,
                "root_path": str(root),
                "owner_scope": "owner",
                "created_at": time.time(),
            }
            grants.append(row)
            self._save_grants(grants)
            return dict(row)

    def list_workspaces(self, principal: Principal | None) -> list[dict[str, Any]]:
        _require_owner(principal)
        with _STORE_LOCK:
            return [dict(row) for row in self._load_grants()]

    def _grant(self, workspace_id: str) -> dict[str, Any]:
        if not _WORKSPACE_ID_RE.fullmatch(str(workspace_id or "")):
            raise ProjectWorkspaceError("项目工作区标识非法")
        grant = next(
            (row for row in self._load_grants() if row["workspace_id"] == workspace_id),
            None,
        )
        if grant is None:
            raise ProjectWorkspaceError("项目工作区未授权或已撤销")
        root = _authorized_root(grant["root_path"])
        if str(root) != grant["root_path"]:
            raise ProjectWorkspaceError("项目工作区根目录已变化")
        return grant

    def revoke(self, workspace_id: str, *, principal: Principal | None) -> dict[str, Any]:
        _require_owner(principal)
        if not _WORKSPACE_ID_RE.fullmatch(str(workspace_id or "")):
            raise ProjectWorkspaceError("项目工作区标识非法")
        with _STORE_LOCK:
            grants = self._load_grants()
            remaining = [row for row in grants if row["workspace_id"] != workspace_id]
            if len(remaining) == len(grants):
                raise ProjectWorkspaceError("项目工作区未授权或已撤销")
            self._save_grants(remaining)
            return {"workspace_id": workspace_id, "revoked": True}

    def prepare_task(
        self,
        workspace_id: str,
        relative_paths: Iterable[str],
        *,
        principal: Principal | None,
    ) -> dict[str, Any]:
        _require_owner(principal)
        paths = [_relative_path(item) for item in relative_paths]
        if not 1 <= len(paths) <= MAX_TASK_FILES or len(set(paths)) != len(paths):
            raise ProjectWorkspaceError("每个任务需选择 1 到 16 个不重复文件")
        with _STORE_LOCK:
            grant = self._grant(workspace_id)
            root = Path(grant["root_path"])
            task_id = f"projecttask_{secrets.token_hex(8)}"
            _ensure_safe_directory(self.task_root, self.task_root, create=True)
            task_dir = self.task_root / task_id
            inputs: list[dict[str, Any]] = []
            with _hold_directory_chain(self.task_root, self.task_root):
                task_dir.mkdir(parents=False, exist_ok=False)
            _ensure_safe_directory(task_dir, self.task_root, create=False)
            input_root = task_dir / "input"
            with _hold_directory_chain(task_dir, task_dir):
                input_root.mkdir(parents=False, exist_ok=False)
            _ensure_safe_directory(input_root, task_dir, create=False)
            with ExitStack() as copied_inputs:
                total_bytes = 0
                for relative in paths:
                    source = root.joinpath(*relative.parts)
                    parent = _ensure_safe_directory_chain(
                        input_root,
                        relative.parts[:-1],
                    )
                    destination = parent / relative.name
                    with _open_checked_file(root, source) as (handle, before):
                        try:
                            source_hash, copied = publish_opened_file(
                                handle,
                                destination,
                                root=task_dir,
                                max_bytes=MAX_FILE_BYTES,
                            )
                        except WorkspaceIOError as exc:
                            raise ProjectWorkspaceError(str(exc)) from exc
                        after = os.fstat(handle.fileno())
                    copied_handle, _copied_info = copied_inputs.enter_context(
                        _open_checked_file(task_dir, destination)
                    )
                    copied_hash, copied_size = _hash_opened(copied_handle)
                    if copied_hash != source_hash or copied_size != copied:
                        raise ProjectWorkspaceError("任务文件副本校验失败")
                    total_bytes += copied
                    if total_bytes > MAX_TASK_BYTES:
                        raise ProjectWorkspaceError("任务文件总大小超过 128 MiB")
                    if (
                        before.st_size != after.st_size
                        or before.st_mtime_ns != after.st_mtime_ns
                    ):
                        raise ProjectWorkspaceError("复制期间源文件发生变化")
                    inputs.append({
                        "relative_path": relative.as_posix(),
                        "size_bytes": copied,
                        "sha256": source_hash,
                    })
                manifest = {
                    "version": 1,
                    "task_id": task_id,
                    "workspace_id": workspace_id,
                    "created_at": time.time(),
                    "inputs": inputs,
                    "outputs": {},
                    "commits": [],
                }
                _write_json(task_dir / "manifest.json", manifest)
            return manifest

    def _load_task(self, task_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        if not _TASK_ID_RE.fullmatch(str(task_id or "")):
            raise ProjectWorkspaceError("项目任务标识非法")
        _ensure_safe_directory(self.task_root, self.task_root, create=False)
        task_dir = self.task_root / task_id
        _ensure_safe_directory(task_dir, self.task_root, create=False)
        manifest_path = task_dir / "manifest.json"
        try:
            _reject_reparse_chain(manifest_path, self.task_root)
            manifest_path.resolve(strict=True).relative_to(self.task_root)
        except ProjectWorkspaceError:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            raise ProjectWorkspaceError("项目任务记录不存在或越界") from exc
        manifest = _json_object(manifest_path)
        if (
            manifest.get("version") != 1
            or manifest.get("task_id") != task_id
            or not _WORKSPACE_ID_RE.fullmatch(str(manifest.get("workspace_id", "")))
            or not isinstance(manifest.get("inputs"), list)
            or not isinstance(manifest.get("outputs"), dict)
            or not isinstance(manifest.get("commits"), list)
            or (
                "pending_commit" in manifest
                and not isinstance(manifest.get("pending_commit"), dict)
            )
        ):
            raise ProjectWorkspaceError("项目任务记录损坏")
        grant = self._grant(manifest["workspace_id"])
        return task_dir, manifest, grant

    def task_status(self, task_id: str, *, principal: Principal | None) -> dict[str, Any]:
        _require_owner(principal)
        with _STORE_LOCK:
            _, manifest, _ = self._load_task(task_id)
            return manifest

    def list_results(self, *, principal: Principal | None) -> dict[str, Any]:
        """有界查找持久任务成果；不可用记录和截断显式标记，不扩大目录授权。"""
        from kernel.project_results import result_rows

        _require_owner(principal)
        with _STORE_LOCK:
            if not self.task_root.exists():
                return {"results": [], "partial": False}
            _ensure_safe_directory(self.task_root, self.task_root, create=False)
            rows: list[dict[str, Any]] = []
            partial = False
            with _hold_directory_chain(self.task_root, self.task_root):
                with os.scandir(self.task_root) as entries:
                    for index, entry in enumerate(entries):
                        if index >= 200:
                            partial = True
                            break
                        if not _TASK_ID_RE.fullmatch(entry.name):
                            continue
                        try:
                            _, manifest, grant = self._load_task(entry.name)
                            rows.extend(result_rows(manifest, grant))
                        except (ProjectWorkspaceError, OSError):
                            partial = True
            rows.sort(key=lambda row: (row["created_at"], row["task_id"], row["output_path"]),
                      reverse=True)
            return {"results": rows[:50], "partial": partial or len(rows) > 50}

    def read_task_input(
        self,
        task_id: str,
        input_path: str,
        *,
        principal: Principal | None,
    ) -> ProjectTaskInput:
        """读取一份已复制并与任务清单哈希绑定的输入，不返回宿主路径。"""
        _require_owner(principal)
        relative = _relative_path(input_path)
        with _STORE_LOCK:
            task_dir, manifest, _ = self._load_task(task_id)
            record = next(
                (
                    item
                    for item in manifest["inputs"]
                    if isinstance(item, dict)
                    and item.get("relative_path") == relative.as_posix()
                ),
                None,
            )
            if record is None:
                raise ProjectWorkspaceError("任务输入不存在或未被本任务选择")
            input_root = task_dir / "input"
            _ensure_safe_directory(input_root, task_dir, create=False)
            source = input_root.joinpath(*relative.parts)
            with _open_checked_file(input_root, source) as (handle, _source_info):
                digest, size = _hash_opened(handle)
                if (
                    size != record.get("size_bytes")
                    or digest != record.get("sha256")
                ):
                    raise ProjectWorkspaceError("任务输入副本已变化或记录损坏")
                data = handle.read(MAX_FILE_BYTES + 1)
            if len(data) != size:
                raise ProjectWorkspaceError("任务输入读取不完整")
            return ProjectTaskInput(relative.as_posix(), data, size, digest)

    def read_task_output(
        self, task_id: str, output_path: str, *, principal: Principal | None,
    ) -> ProjectResultFile:
        """只读已登记的暂存成果，逐次复核授权、路径和内容，不打开项目原件。"""
        from kernel.project_results import ProjectResultFile, output_record

        _require_owner(principal)
        relative = _relative_path(output_path)
        with _STORE_LOCK:
            task_dir, manifest, _ = self._load_task(task_id)
            size, digest = output_record(manifest["outputs"].get(relative.as_posix()))
            output_root = task_dir / "output"
            _ensure_safe_directory(output_root, task_dir, create=False)
            source = output_root.joinpath(*relative.parts)
            with _open_checked_file(output_root, source) as (handle, _info):
                data = handle.read(MAX_FILE_BYTES + 1)
            if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                raise ProjectWorkspaceError("项目成果已变化或校验失败")
            return ProjectResultFile(relative.as_posix(), data, size, digest)

    def _stage_opened_result(
        self,
        task_id: str,
        task_dir: Path,
        manifest: dict[str, Any],
        source_handle: BinaryIO,
        relative: PurePosixPath,
        *,
        source_hash: str,
        source_size: int,
    ) -> tuple[dict[str, Any], Path]:
        output_root = task_dir / "output"
        if not output_root.exists():
            with _hold_directory_chain(task_dir, task_dir):
                output_root.mkdir(parents=False, exist_ok=False)
        _ensure_safe_directory(output_root, task_dir, create=False)
        parent = _ensure_safe_directory_chain(output_root, relative.parts[:-1])
        destination = parent / relative.name
        record = manifest["outputs"].get(relative.as_posix())
        if record is not None and (
            not isinstance(record, dict)
            or record.get("size_bytes") != source_size
            or record.get("sha256") != source_hash
        ):
            raise ProjectWorkspaceError("任务结果登记已存在且内容不同")

        def record_output(copied_hash: str, copied_size: int) -> None:
            if copied_hash != source_hash or copied_size != source_size:
                raise ProjectWorkspaceError("结果暂存校验失败")
            manifest["outputs"][relative.as_posix()] = {
                "size_bytes": source_size,
                "sha256": source_hash,
            }
            _write_json(task_dir / "manifest.json", manifest)

        if destination.exists():
            with _open_checked_file(output_root, destination) as (
                staged_handle,
                _staged_info,
            ):
                staged_hash, staged_size = _hash_opened(staged_handle)
                if staged_hash != source_hash or staged_size != source_size:
                    raise ProjectWorkspaceError("任务结果路径已存在，拒绝覆盖")
                record_output(staged_hash, staged_size)
        else:
            try:
                publish_opened_file(
                    source_handle,
                    destination,
                    root=output_root,
                    max_bytes=MAX_FILE_BYTES,
                    on_published=record_output,
                )
            except WorkspaceIOError as exc:
                raise ProjectWorkspaceError(str(exc)) from exc
        return (
            {
                "task_id": task_id,
                "output_path": relative.as_posix(),
                "sha256": source_hash,
                "size_bytes": source_size,
            },
            destination,
        )

    def stage_result(
        self,
        task_id: str,
        source_path: str | os.PathLike[str],
        output_path: str,
        *,
        principal: Principal | None,
    ) -> dict[str, Any]:
        _require_owner(principal)
        relative = _relative_path(output_path)
        with _STORE_LOCK:
            task_dir, manifest, _ = self._load_task(task_id)
            source = Path(source_path)
            if not source.is_absolute():
                raise ProjectWorkspaceError(
                    "结果来源必须是 Aoao OS 管理的数据文件"
                )
            try:
                source.relative_to(self.managed_data_root)
            except ValueError as exc:
                raise ProjectWorkspaceError(
                    "结果来源必须是 Aoao OS 管理的数据文件"
                ) from exc
            with _open_checked_file(self.managed_data_root, source) as (
                source_handle,
                _source_info,
            ):
                source_hash, source_size = _hash_opened(source_handle)
                result, destination = self._stage_opened_result(
                    task_id,
                    task_dir,
                    manifest,
                    source_handle,
                    relative,
                    source_hash=source_hash,
                    source_size=source_size,
                )
            return {**result, "staged_path": str(destination)}

    def stage_result_stream(
        self,
        task_id: str,
        source: BinaryIO,
        output_path: str,
        *,
        expected_size: int,
        expected_sha256: str,
        principal: Principal | None,
    ) -> dict[str, Any]:
        """把可信桥接层给出的受限字节流登记为结果，不接收宿主路径。"""
        _require_owner(principal)
        relative = _relative_path(output_path)
        if (
            type(expected_size) is not int
            or not 0 < expected_size <= MAX_FILE_BYTES
            or not isinstance(expected_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
        ):
            raise ProjectWorkspaceError("结果流元数据非法")
        with _STORE_LOCK:
            task_dir, manifest, _ = self._load_task(task_id)
            try:
                source_hash, source_size = _hash_opened(source)
            except (OSError, ValueError, TypeError) as exc:
                raise ProjectWorkspaceError("结果流无法安全读取") from exc
            if source_size != expected_size or source_hash != expected_sha256:
                raise ProjectWorkspaceError("结果流与声明的大小或哈希不一致")
            result, _destination = self._stage_opened_result(
                task_id,
                task_dir,
                manifest,
                source,
                relative,
                source_hash=source_hash,
                source_size=source_size,
            )
            return result

    def commit_result(
        self,
        task_id: str,
        output_path: str,
        destination_path: str,
        *,
        principal: Principal | None,
    ) -> dict[str, Any]:
        _require_owner(principal)
        output_relative = _relative_path(output_path)
        destination_relative = _relative_path(destination_path)
        with _STORE_LOCK:
            task_dir, manifest, grant = self._load_task(task_id)
            root = Path(grant["root_path"])
            output_key = output_relative.as_posix()
            output_record = manifest["outputs"].get(output_key)
            if not isinstance(output_record, dict):
                raise ProjectWorkspaceError("任务结果未登记，拒绝回存")
            recorded_size = output_record.get("size_bytes")
            recorded_hash = output_record.get("sha256")
            if (
                type(recorded_size) is not int
                or not 0 < recorded_size <= MAX_FILE_BYTES
                or not isinstance(recorded_hash, str)
                or not re.fullmatch(r"[0-9a-f]{64}", recorded_hash)
            ):
                raise ProjectWorkspaceError("任务结果登记损坏")

            output_root = task_dir / "output"
            _ensure_safe_directory(output_root, task_dir, create=False)
            staged = output_root.joinpath(*output_relative.parts)
            with _open_checked_file(output_root, staged) as (
                staged_handle,
                _staged_info,
            ):
                result_hash, result_size = _hash_opened(staged_handle)
                if result_hash != recorded_hash or result_size != recorded_size:
                    raise ProjectWorkspaceError("任务结果校验失败，拒绝回存")

                operation_id = _operation_id(
                    task_id,
                    output_key,
                    destination_relative.as_posix(),
                    result_hash,
                )
                existing = next(
                    (
                        item for item in manifest["commits"]
                        if isinstance(item, dict)
                        and item.get("operation_id") == operation_id
                    ),
                    None,
                )
                if existing is not None:
                    return dict(existing)

                desired = root.joinpath(*destination_relative.parts)
                parent = desired.parent
                _ensure_safe_directory(parent, root, create=False)
                pending = manifest.get("pending_commit")
                if pending is None:
                    committed = None
                elif (
                    pending.get("operation_id") != operation_id
                    or pending.get("output_path") != output_key
                    or pending.get("destination_path")
                    != destination_relative.as_posix()
                    or pending.get("sha256") != result_hash
                    or pending.get("size_bytes") != result_size
                ):
                    raise ProjectWorkspaceError("另一个结果回存操作仍待核对")
                else:
                    committed_relative = _relative_path(
                        pending.get("committed_relative_path", "")
                    )
                    committed = root.joinpath(*committed_relative.parts)
                    if committed.parent != parent:
                        raise ProjectWorkspaceError("待核对的结果目标记录损坏")

                receipt_box: dict[str, dict[str, Any]] = {}

                def finalize_commit(
                    published_hash: str,
                    published_size: int,
                ) -> None:
                    if (
                        published_hash != result_hash
                        or published_size != result_size
                    ):
                        raise ProjectWorkspaceError("结果回存校验失败")
                    receipt = {
                        "operation_id": operation_id,
                        "task_id": task_id,
                        "output_path": output_key,
                        "destination_path": destination_relative.as_posix(),
                        "committed_relative_path": committed.relative_to(root).as_posix(),
                        "committed_path": str(committed),
                        "sha256": result_hash,
                        "committed_at": time.time(),
                    }
                    manifest.pop("pending_commit", None)
                    manifest["commits"].append(receipt)
                    _write_json(task_dir / "manifest.json", manifest)
                    receipt_box["receipt"] = receipt

                if committed is not None and committed.exists():
                    with _hold_directory_chain(root, parent):
                        with _open_checked_file(root, committed) as (
                            committed_handle,
                            _committed_info,
                        ):
                            committed_hash, committed_size = _hash_opened(
                                committed_handle
                            )
                            if (
                                committed_hash != result_hash
                                or committed_size != result_size
                            ):
                                raise ProjectWorkspaceError(
                                    "待核对的结果目标已被其他内容占用"
                                )
                            finalize_commit(committed_hash, committed_size)
                    return receipt_box["receipt"]

                with _hold_unchanged_task_inputs(manifest, root):
                    if committed is None:
                        with _hold_directory_chain(root, parent):
                            committed = self._available_destination(desired)
                            committed_relative = committed.relative_to(
                                root
                            ).as_posix()
                            pending = {
                                "operation_id": operation_id,
                                "output_path": output_key,
                                "destination_path": (
                                    destination_relative.as_posix()
                                ),
                                "committed_relative_path": committed_relative,
                                "sha256": result_hash,
                                "size_bytes": result_size,
                            }
                            manifest["pending_commit"] = pending
                            _write_json(task_dir / "manifest.json", manifest)

                    try:
                        publish_opened_file(
                            staged_handle,
                            committed,
                            root=root,
                            max_bytes=MAX_FILE_BYTES,
                            on_published=finalize_commit,
                        )
                    except FileExistsError as exc:
                        raise ProjectWorkspaceError(
                            "结果目标刚被占用，请重新提交"
                        ) from exc
                    except WorkspaceIOError as exc:
                        raise ProjectWorkspaceError(str(exc)) from exc

                return receipt_box["receipt"]

    @staticmethod
    def _available_destination(desired: Path) -> Path:
        if not desired.exists():
            return desired
        stamp = time.strftime("%Y%m%d-%H%M%S")
        candidate = desired.with_name(
            f"{desired.stem}.alu-{stamp}{desired.suffix}"
        )
        for index in range(100):
            value = candidate if index == 0 else candidate.with_name(
                f"{desired.stem}.alu-{stamp}-{index}{desired.suffix}"
            )
            if not value.exists():
                return value
        raise ProjectWorkspaceError("结果版本名已用尽")
