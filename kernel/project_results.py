"""项目成果的闭集展示投影；回存记录不等于目标文件的实时健康证明。"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from kernel.project_workspace import MAX_FILE_BYTES, ProjectWorkspaceError, _relative_path


@dataclass(frozen=True, slots=True, repr=False)
class ProjectResultFile:
    """在同一文件句柄中校验后的成果副本，不携带宿主路径。"""

    relative_path: str
    data: bytes
    size_bytes: int
    sha256: str


def _timestamp(value: Any) -> float:
    # 比较先于 float 转换，连 JSON 合法的大整数也不能击穿坏记录隔离。
    if type(value) not in {int, float} or not 0 <= value <= 253402300799:
        raise ProjectWorkspaceError("项目成果时间记录损坏")
    return float(value)


def output_record(value: Any) -> tuple[int, str]:
    if not isinstance(value, dict):
        raise ProjectWorkspaceError("项目成果记录损坏")
    size, digest = value.get("size_bytes"), value.get("sha256")
    if (type(size) is not int or not 0 < size <= MAX_FILE_BYTES
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        raise ProjectWorkspaceError("项目成果记录损坏")
    return size, digest


def result_rows(manifest: dict[str, Any], grant: dict[str, Any]) -> list[dict[str, Any]]:
    """只投影已暂存成果、匹配哈希的回存记录，不含宿主绝对路径。"""
    if len(manifest["outputs"]) > 100 or len(manifest["commits"]) > 100:
        raise ProjectWorkspaceError("项目成果展示记录超过上限")
    created = _timestamp(manifest.get("created_at"))
    rows = []
    for path, record in manifest["outputs"].items():
        if not isinstance(path, str) or _relative_path(path).as_posix() != path:
            raise ProjectWorkspaceError("项目成果路径记录损坏")
        size, digest = output_record(record)
        commits = []
        for commit in manifest["commits"]:
            if not isinstance(commit, dict):
                raise ProjectWorkspaceError("项目回存记录损坏")
            if commit.get("output_path") != path or commit.get("sha256") != digest:
                continue
            destination = _relative_path(commit.get("committed_relative_path", "")).as_posix()
            commits.append({"destination_path": destination,
                            "committed_at": _timestamp(commit.get("committed_at"))})
        pending = manifest.get("pending_commit") or {}
        rows.append({
            "task_id": manifest["task_id"], "workspace_id": grant["workspace_id"],
            "project_label": grant["label"], "output_path": path,
            "size_bytes": size, "sha256": digest, "created_at": created,
            "commits": commits[-3:],
            "pending": pending.get("output_path") == path,
        })
    return rows
