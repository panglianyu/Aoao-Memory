"""项目工作区的绑定句柄文件发布边界。"""
from __future__ import annotations

import hashlib
import os
import secrets
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Callable, Iterator


class WorkspaceIOError(ValueError):
    """文件系统对象无法证明仍位于预期目录边界。"""


def _reparse_or_link(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & 0x400
    )


def _reject_reparse_chain(path: Path, stop: Path) -> None:
    current = path
    while True:
        if _reparse_or_link(current):
            raise WorkspaceIOError("目标目录不能包含符号链接或重解析点")
        if current == stop:
            return
        if current.parent == current:
            raise WorkspaceIOError("目标目录越过工作区边界")
        current = current.parent


def _safe_directory(root: Path, directory: Path) -> tuple[Path, Path]:
    try:
        safe_root = root.resolve(strict=True)
        safe_directory = directory.resolve(strict=True)
        safe_directory.relative_to(safe_root)
        _reject_reparse_chain(directory, root)
    except WorkspaceIOError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceIOError("目标目录不存在或越过工作区边界") from exc
    if not safe_root.is_dir() or not safe_directory.is_dir():
        raise WorkspaceIOError("目标目录类型非法")
    return safe_root, safe_directory


def _windows_final_path(os_handle: int) -> Path:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    getter = kernel32.GetFinalPathNameByHandleW
    getter.argtypes = [
        ctypes.c_void_p,
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    getter.restype = ctypes.c_uint32
    size = getter(os_handle, None, 0, 0)
    if not size:
        raise OSError(ctypes.get_last_error(), "无法读取文件句柄路径")
    buffer = ctypes.create_unicode_buffer(size + 1)
    written = getter(os_handle, buffer, len(buffer), 0)
    if not written or written >= len(buffer):
        raise OSError(ctypes.get_last_error(), "无法读取文件句柄路径")
    value = buffer.value
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(value)


def final_open_path(handle: BinaryIO) -> Path:
    """返回已经打开的文件对象实际绑定的路径。"""
    if os.name == "nt":
        import msvcrt

        return _windows_final_path(
            msvcrt.get_osfhandle(handle.fileno())
        ).resolve(strict=True)
    return Path(os.readlink(f"/proc/self/fd/{handle.fileno()}")).resolve(
        strict=True
    )


def open_bound_read_file(root: Path, path: Path) -> BinaryIO:
    """以拒绝并发写入和删除的共享方式打开并核对文件。"""
    root = root.resolve(strict=True)
    if os.name != "nt":
        handle = path.open("rb")
    else:
        import ctypes
        import msvcrt
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.c_void_p,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        raw_handle = create_file(
            str(path),
            0x80000000,
            0x00000001,
            None,
            3,
            0x00200000,
            None,
        )
        if raw_handle == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_last_error(), "无法安全打开文件")
        try:
            descriptor = msvcrt.open_osfhandle(
                raw_handle,
                os.O_RDONLY | os.O_BINARY,
            )
        except Exception:
            kernel32.CloseHandle(raw_handle)
            raise
        handle = os.fdopen(descriptor, "rb")
    try:
        final_open_path(handle).relative_to(root)
        _reject_reparse_chain(path, root)
    except Exception:
        handle.close()
        raise
    return handle


@contextmanager
def _directory_chain_lease(
    root: Path,
    directory: Path,
    *,
    deny_writes: bool = True,
) -> Iterator[tuple[Path, int | None]]:
    """固定目录链，并返回最终目录的操作系统句柄。"""
    safe_root, safe_directory = _safe_directory(root, directory)
    if os.name != "nt":
        yield safe_directory, None
        return

    import ctypes
    from ctypes import wintypes

    generic_read = 0x80000000
    generic_write = 0x40000000
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    open_existing = 3
    file_flag_open_reparse_point = 0x00200000
    file_flag_backup_semantics = 0x02000000
    file_attribute_reparse_point = 0x00000400
    file_attribute_tag_info = 9
    invalid_handle_value = ctypes.c_void_p(-1).value

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("reparse_tag", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    get_info = kernel32.GetFileInformationByHandleEx
    get_info.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    get_info.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    relative = safe_directory.relative_to(safe_root)
    candidates = [safe_root]
    current = safe_root
    for part in relative.parts:
        current = current / part
        candidates.append(current)

    handles: list[int] = []
    try:
        for candidate in candidates:
            desired_access = generic_read
            if candidate == safe_directory:
                desired_access |= generic_write
            handle = create_file(
                str(candidate),
                desired_access,
                file_share_read if deny_writes else (
                    file_share_read | file_share_write
                ),
                None,
                open_existing,
                file_flag_backup_semantics | file_flag_open_reparse_point,
                None,
            )
            if handle == invalid_handle_value:
                raise OSError(ctypes.get_last_error(), "无法固定目标目录")
            handles.append(handle)
            tag_info = FileAttributeTagInfo()
            if not get_info(
                handle,
                file_attribute_tag_info,
                ctypes.byref(tag_info),
                ctypes.sizeof(tag_info),
            ):
                raise OSError(ctypes.get_last_error(), "无法核对目标目录属性")
            if tag_info.file_attributes & file_attribute_reparse_point:
                raise WorkspaceIOError("目标目录不能包含重解析点")
            final = _windows_final_path(handle)
            if os.path.normcase(os.path.abspath(final)) != os.path.normcase(
                os.path.abspath(candidate)
            ):
                raise WorkspaceIOError("目录句柄与工作区路径不一致")
            final.relative_to(safe_root)
    except WorkspaceIOError:
        for handle in reversed(handles):
            close_handle(handle)
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        for handle in reversed(handles):
            close_handle(handle)
        raise WorkspaceIOError("无法固定安全的目标目录链") from exc

    try:
        yield safe_directory, handles[-1]
    finally:
        for handle in reversed(handles):
            close_handle(handle)


@contextmanager
def hold_directory_chain(root: Path, directory: Path) -> Iterator[Path]:
    """固定目录链并拒绝新的写入或删除句柄。"""
    with _directory_chain_lease(root, directory) as (safe_directory, _handle):
        yield safe_directory


def _copy_stream(
    source: BinaryIO,
    target: BinaryIO,
    *,
    max_bytes: int,
) -> tuple[str, int]:
    source.seek(0)
    digest = hashlib.sha256()
    total = 0
    try:
        while True:
            chunk = source.read(min(1024 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise WorkspaceIOError("文件复制量超过安全上限")
            target.write(chunk)
            digest.update(chunk)
        target.flush()
        os.fsync(target.fileno())
    finally:
        source.seek(0)
    if total <= 0:
        raise WorkspaceIOError("空文件不受支持")
    return digest.hexdigest(), total


def _open_bound_windows_file(path: Path, root: Path) -> BinaryIO:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    generic_read = 0x80000000
    generic_write = 0x40000000
    delete = 0x00010000
    file_share_read = 0x00000001
    create_new = 1
    file_attribute_normal = 0x00000080
    file_flag_open_reparse_point = 0x00200000
    invalid_handle_value = ctypes.c_void_p(-1).value
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    raw_handle = create_file(
        str(path),
        generic_read | generic_write | delete,
        file_share_read,
        None,
        create_new,
        file_attribute_normal | file_flag_open_reparse_point,
        None,
    )
    if raw_handle == invalid_handle_value:
        raise OSError(ctypes.get_last_error(), "无法创建绑定的临时文件")
    try:
        descriptor = msvcrt.open_osfhandle(raw_handle, os.O_RDWR | os.O_BINARY)
    except Exception:
        kernel32.CloseHandle(raw_handle)
        raise
    target = os.fdopen(descriptor, "w+b")
    try:
        final_open_path(target).relative_to(root)
    except Exception:
        target.close()
        raise
    return target


def _rename_windows_handle(
    target: BinaryIO,
    destination: Path,
    *,
    replace: bool,
    parent_handle: int,
) -> None:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class FileRenameInfo(ctypes.Structure):
        _fields_ = [
            ("flags", wintypes.DWORD),
            ("root_directory", wintypes.HANDLE),
            ("file_name_length", wintypes.DWORD),
            ("file_name", wintypes.WCHAR * 1),
        ]

    class IoStatusBlock(ctypes.Structure):
        _fields_ = [
            ("status", ctypes.c_ssize_t),
            ("information", ctypes.c_size_t),
        ]

    encoded = destination.name.encode("utf-16-le")
    buffer_size = max(
        ctypes.sizeof(FileRenameInfo),
        FileRenameInfo.file_name.offset + len(encoded),
    )
    buffer = ctypes.create_string_buffer(buffer_size)
    info = ctypes.cast(buffer, ctypes.POINTER(FileRenameInfo)).contents
    info.flags = 1 if replace else 0
    info.root_directory = parent_handle
    info.file_name_length = len(encoded)
    ctypes.memmove(
        ctypes.addressof(buffer) + FileRenameInfo.file_name.offset,
        encoded,
        len(encoded),
    )
    ntdll = ctypes.WinDLL("ntdll")
    setter = ntdll.NtSetInformationFile
    setter.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_int,
    ]
    setter.restype = ctypes.c_long
    io_status = IoStatusBlock()
    status = setter(
        msvcrt.get_osfhandle(target.fileno()),
        ctypes.byref(io_status),
        buffer,
        buffer_size,
        10,
    )
    if status < 0:
        converter = ntdll.RtlNtStatusToDosError
        converter.argtypes = [ctypes.c_long]
        converter.restype = wintypes.DWORD
        error = converter(status)
        if not replace and destination.exists():
            raise FileExistsError(error, "结果目标已存在", str(destination))
        raise OSError(error, "无法通过绑定句柄发布文件", str(destination))


def _delete_windows_handle(target: BinaryIO) -> None:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class FileDispositionInfo(ctypes.Structure):
        _fields_ = [("delete_file", wintypes.BOOL)]

    info = FileDispositionInfo(True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    setter = kernel32.SetFileInformationByHandle
    setter.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    setter.restype = wintypes.BOOL
    if not setter(
        msvcrt.get_osfhandle(target.fileno()),
        4,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        raise OSError(ctypes.get_last_error(), "无法清理绑定的临时文件")


def publish_opened_file(
    source: BinaryIO,
    destination: Path,
    *,
    root: Path,
    max_bytes: int,
    replace: bool = False,
    on_published: Callable[[str, int], None] | None = None,
) -> tuple[str, int]:
    """保持临时文件排他句柄，写入并由同一对象完成最终发布。"""
    root = root.resolve(strict=True)
    parent = destination.parent
    temporary = parent / f".{destination.name}.{secrets.token_hex(8)}.tmp"
    renamed = False
    target: BinaryIO | None = None
    try:
        with _directory_chain_lease(root, parent):
            if os.name == "nt":
                target = _open_bound_windows_file(temporary, root)
            else:
                target = temporary.open("x+b")
                final_open_path(target).relative_to(root)
            copied = _copy_stream(source, target, max_bytes=max_bytes)

        if os.name == "nt":
            # 复制阶段的严格目录 lease 已释放；发布阶段依靠临时文件排他句柄，
            # 并使用当前父目录句柄作相对重命名，不把这段视为全目录写入封锁。
            with _directory_chain_lease(
                root,
                parent,
                deny_writes=False,
            ) as (_safe_parent, parent_handle):
                if parent_handle is None:
                    raise WorkspaceIOError("Windows 目录句柄不可用")
                _rename_windows_handle(
                    target,
                    destination,
                    replace=replace,
                    parent_handle=parent_handle,
                )
        elif replace:
            os.replace(temporary, destination)
        else:
            os.link(temporary, destination, follow_symlinks=False)
            temporary.unlink()
        renamed = True
        if os.path.normcase(os.path.abspath(final_open_path(target))) != (
            os.path.normcase(os.path.abspath(destination))
        ):
            raise WorkspaceIOError("发布后的文件句柄路径不一致")
        if on_published is not None:
            on_published(*copied)
        return copied
    finally:
        if target is not None:
            try:
                if os.name == "nt" and not renamed:
                    _delete_windows_handle(target)
            finally:
                target.close()
        if os.name != "nt" and not renamed:
            temporary.unlink(missing_ok=True)
