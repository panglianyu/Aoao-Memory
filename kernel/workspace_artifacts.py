"""工作空间小型产物的受限只读解析器。

只暴露按协议命名的 PNG 快照，不提供目录浏览、任意相对路径或下载能力。
浏览器插件和后续文件/终端后端均可复用这一内核边界。
"""
from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WORKSPACE_ROOT = PROJECT_ROOT / "data" / "workspaces"
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
_WORKSPACE_ID_RE = re.compile(r"^workspace_[0-9a-f]{16}$")
_SNAPSHOT_NAME_RE = re.compile(r"^tab_[0-9a-f]{16}_[0-9]{6}\.png$")
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class WorkspaceArtifactError(ValueError):
    """产物不存在或不满足公开边界。"""


def resolve_workspace_snapshot(
    workspace_id: str,
    filename: str,
    *,
    workspace_root: Path | str = DEFAULT_WORKSPACE_ROOT,
) -> Path:
    """解析一张协议快照；任何异常统一为 WorkspaceArtifactError。"""
    if not _WORKSPACE_ID_RE.fullmatch(str(workspace_id or "")):
        raise WorkspaceArtifactError("工作空间标识非法")
    if not _SNAPSHOT_NAME_RE.fullmatch(str(filename or "")):
        raise WorkspaceArtifactError("快照名称非法")

    root = Path(workspace_root).resolve()
    candidate = root / workspace_id / "snapshots" / filename
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise WorkspaceArtifactError("快照不存在") from exc
    if resolved.parent != (root / workspace_id / "snapshots").resolve():
        raise WorkspaceArtifactError("快照路径越界")
    try:
        stat = resolved.stat()
        with resolved.open("rb") as handle:
            signature = handle.read(len(_PNG_SIGNATURE))
    except OSError as exc:
        raise WorkspaceArtifactError("快照不可读") from exc
    if not resolved.is_file() or stat.st_size <= 0 or stat.st_size > MAX_SNAPSHOT_BYTES:
        raise WorkspaceArtifactError("快照大小非法")
    if signature != _PNG_SIGNATURE:
        raise WorkspaceArtifactError("快照内容不是 PNG")
    return resolved
