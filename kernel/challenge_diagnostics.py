"""Private bounded failure codes. No exception messages or business identifiers."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
import json
import os
from pathlib import Path
import stat
from typing import Iterator

_STAGES = frozenset({"finish", "content", "json", "group_shape", "domain_payload",
                     "cache_integrity", "source_budget", "prepare", "published", "retirement", "unknown"})
_REASONS = _STAGES
_DOMAINS = frozenset({"vector", "entity", "graph", "rule", "journal", "schedule"})
_ERROR_TYPES: dict[type[BaseException], str] = {ValueError: "ValueError", RuntimeError: "RuntimeError", OSError: "OSError",
                TimeoutError: "TimeoutError", PermissionError: "PermissionError",
                TypeError: "TypeError", KeyError: "KeyError", IndexError: "IndexError"}
_FINISH_REASONS = frozenset({"stop", "length", "content_filter", "tool_calls",
                            "insufficient_system_resource", "aborted", "unknown"})
_PROJECT = Path(__file__).absolute().parent.parent
_STATIC_FILES = frozenset({"kernel/raw_message_worker.py", "kernel/challenge_diagnostics.py",
    "apps/system_memory_god/grouped_extractor.py", "apps/system_memory_god/derivation.py",
    "apps/system_memory_god/storage/extraction_snapshots.py",
    "apps/system_memory_god/storage/derived_evidence.py",
    "apps/system_memory_god/challenge_runtime.py", "apps/system_memory_god/challenge_lanes.py",
    "apps/system_memory_god/challenge_admission.py"})
_ACTIVE: DiagnosticSink | None = None


class DiagnosticError(ValueError):
    """ValueError-compatible closed classification; never accepts diagnostic text."""

    def __init__(self, reason: str, message: str | None = None, *, finish_reason: object = None):
        self.safe_reason = reason if type(reason) is str and reason in _REASONS else "unknown"
        self.safe_finish_reason = (finish_reason if type(finish_reason) is str and finish_reason in _FINISH_REASONS
                                   else "unknown")
        # Existing static validation messages remain useful to ordinary callers.
        # Only closed reason and finish fields are eligible for diagnostics.
        super().__init__(message if message is not None else self.safe_reason)


def _encode_record(stage: object, error: BaseException, domain: object, attempt: object) -> bytes:
    reason = error.safe_reason if type(error) is DiagnosticError else "unknown"
    frames: list[dict] = []
    trace = error.__traceback__
    # Frame names and code text are deliberately excluded: only literal project
    # file names from this allowlist and integer source locations may persist.
    traversed = 0
    while trace is not None and traversed < 64:
        traversed += 1
        filename = trace.tb_frame.f_code.co_filename.replace("\\", "/")
        prefix = _PROJECT.as_posix() + "/"
        relative = filename[len(prefix):] if filename.startswith(prefix) else filename
        if relative in _STATIC_FILES:
            frames.append({"file": relative, "line": trace.tb_lineno})
            frames = frames[-8:]
        trace = trace.tb_next
    record = {"utc": datetime.now(timezone.utc).isoformat(),
        "stage": stage if type(stage) is str and stage in _STAGES else "unknown",
        "reason": reason if type(reason) is str and reason in _REASONS else "unknown",
        "error_type": "DiagnosticError" if type(error) is DiagnosticError else _ERROR_TYPES.get(type(error), "other"),
        "domain": domain if type(domain) is str and domain in _DOMAINS else None,
        "attempt": attempt if type(attempt) is int and 0 <= attempt <= 100 else None,
        "frames": frames}
    if type(error) is DiagnosticError and error.safe_reason == "finish":
        finish = error.safe_finish_reason
        record["finish_reason"] = finish if type(finish) is str and finish in _FINISH_REASONS else "unknown"
    return (json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def emit_failure(stage: str, error: BaseException, domain: str | None = None,
                 attempt: int | None = None) -> None:
    """Best-effort synchronous enqueue only: never IO, await, or acquire a SQL gate."""
    try:
        sink = _ACTIVE
        if sink is None or sink._closing or not sink._accepting:
            return
        record = _encode_record(stage, error, domain, attempt)
        if len(record) > 2048:
            sink.dropped += 1
            return
        try:
            sink._queue.put_nowait(record)
        except asyncio.QueueFull:
            sink.dropped += 1
    except BaseException:
        # Diagnostics must not change the original failure or ownership path.
        return


def check_private_path(path: Path, *, directory: bool = False) -> None:
    """Require a lexical absolute ordinary path, including its existing ancestors."""
    if not path.is_absolute() or Path(os.path.abspath(path)) != path:
        raise ValueError("ordinary absolute diagnostic path required")
    for current in (path, *path.parents):
        try:
            info = current.lstat()
        except FileNotFoundError:
            if current == path:
                continue
            raise ValueError("ordinary diagnostic parent required") from None
        expected = stat.S_ISDIR if current != path or directory else stat.S_ISREG
        if (not expected(info.st_mode) or current.is_symlink()
                or getattr(info, "st_file_attributes", 0) & 1024
                or (not stat.S_ISDIR(info.st_mode) or os.name == "nt") and info.st_nlink != 1):
            raise ValueError("ordinary single-link diagnostic path required")


@lru_cache(maxsize=1)
def _windows_api():
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    security = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    security.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    security.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                             wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    security.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.LPWSTR)]
    security.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.DWORD)]
    security.GetSecurityDescriptorDacl.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.BOOL),
                                                 ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.BOOL)]
    security.SetSecurityInfo.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.DWORD,
                                       ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    security.SetSecurityInfo.restype = wintypes.DWORD
    return ctypes, wintypes, kernel, security


@lru_cache(maxsize=1)
def _current_sid() -> str:
    ctypes, wintypes, kernel, security = _windows_api()
    token, size = wintypes.HANDLE(), wintypes.DWORD()
    if not security.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        security.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        buffer = ctypes.create_string_buffer(size.value)
        if not security.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        sid = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))[0]
        encoded = wintypes.LPWSTR()
        if not security.ConvertSidToStringSidW(sid, ctypes.byref(encoded)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return encoded.value
        finally:
            kernel.LocalFree(encoded)
    finally:
        kernel.CloseHandle(token)


def _protect_handle(handle: int, *, directory: bool) -> None:
    ctypes, wintypes, kernel, security = _windows_api()
    inheritance = "OICI" if directory else ""
    descriptor = ctypes.c_void_p()
    sddl = "D:P" + "".join(f"(A;{inheritance};FA;;;{sid})" for sid in (_current_sid(), "SY", "BA"))
    if not security.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1, ctypes.byref(descriptor), None):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        present, defaulted, acl = wintypes.BOOL(), wintypes.BOOL(), ctypes.c_void_p()
        if not security.GetSecurityDescriptorDacl(descriptor, ctypes.byref(present), ctypes.byref(acl),
                                                  ctypes.byref(defaulted)) or not present.value:
            raise ValueError("private diagnostic ACL unavailable")
        result = security.SetSecurityInfo(handle, 1, 0x80000004, None, None, acl, None)
        if result:
            raise ctypes.WinError(result)
    finally:
        kernel.LocalFree(descriptor)


def _directory_handle(path: Path, *, protect: bool = False) -> int:
    ctypes, _, kernel, _ = _windows_api()
    # Deny deletion while descendants are checked/opened. OPEN_REPARSE_POINT
    # prevents the final component from following a newly introduced junction.
    handle = kernel.CreateFileW(str(path), 0x80 | (0x60000 if protect else 0), 3,
                                None, 3, 0x02200000, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    return handle


@contextmanager
def _pinned_directories(path: Path) -> Iterator[None]:
    handles = []
    try:
        check_private_path(path, directory=True)
        if os.name == "nt":
            for current in reversed((path, *path.parents)):
                handles.append(_directory_handle(current))
            check_private_path(path, directory=True)
        yield
    finally:
        if handles:
            kernel = _windows_api()[2]
            for handle in reversed(handles):
                kernel.CloseHandle(handle)


def ensure_private_directory(path: Path) -> None:
    """Create an empty private directory before any diagnostic bytes are written."""
    if not path.is_absolute():
        raise ValueError("ordinary absolute diagnostic path required")
    if not path.exists():
        with _pinned_directories(path.parent):
            path.mkdir(mode=0o700)
    with _pinned_directories(path):
        if os.name == "nt":
            handle = _directory_handle(path, protect=True)
            try:
                _protect_handle(handle, directory=True)
            finally:
                _windows_api()[2].CloseHandle(handle)
        else:
            os.chmod(path, 0o700)


def open_private_file(path: Path, *, exclusive: bool = False, append: bool = False) -> int:
    """Return an owned noninheritable descriptor with private ACL before first write."""
    with _pinned_directories(path.parent):
        check_private_path(path)
        if os.name == "nt":
            import msvcrt

            ctypes, _, kernel, _ = _windows_api()
            handle = kernel.CreateFileW(str(path), 0xC0060080, 1, None,
                                        1 if exclusive else 4, 0x00200080, None)
            if handle == ctypes.c_void_p(-1).value:
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                descriptor = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)
            except BaseException:
                kernel.CloseHandle(handle)
                raise
        else:
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                                 | (os.O_EXCL if exclusive else 0), 0o600)
        try:
            os.set_inheritable(descriptor, False)
            opened, named = os.fstat(descriptor), path.lstat()
            check_private_path(path)
            if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                    or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
                raise ValueError("stable ordinary diagnostic file required")
            if os.name == "nt":
                _protect_handle(msvcrt.get_osfhandle(descriptor), directory=False)
            else:
                getattr(os, "fchmod")(descriptor, 0o600)
            if append:
                os.lseek(descriptor, 0, os.SEEK_END)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise


class DiagnosticSink:
    """One process-local bounded queue and strongly owned asynchronous writer."""

    def __init__(self, path: Path, *, close_timeout: float = 5.0):
        if type(close_timeout) not in {int, float} or not 0 < close_timeout <= 60:
            raise ValueError("invalid diagnostic shutdown budget")
        self.path = Path(path)
        self.close_timeout = close_timeout
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=64)
        self._starter: asyncio.Task | None = None
        self._writer: asyncio.Task | None = None
        self._closer: asyncio.Task | None = None
        self._io: asyncio.Task | None = None
        self._descriptor: int | None = None
        self._closing = self._accepting = self._failed = False
        self.dropped = 0

    def _prepare(self) -> None:
        ensure_private_directory(self.path.parent)
        for path in (self.path, self.path.with_name(self.path.name + ".1")):
            check_private_path(path)
            if path.exists() and path.stat().st_size > 256 * 1024:
                raise ValueError("bounded diagnostic file required")
        self._descriptor = open_private_file(self.path, append=True)

    async def start(self) -> None:
        global _ACTIVE
        if self._starter is not None or _ACTIVE is not None:
            raise RuntimeError("diagnostic sink already owned")
        _ACTIVE = self
        self._starter = asyncio.create_task(asyncio.to_thread(self._prepare))
        try:
            await asyncio.shield(self._starter)
        except asyncio.CancelledError:
            # The owner must close this object before releasing its root lease.
            raise
        except BaseException:
            self._failed = True
            _ACTIVE = None
            raise
        self._writer = asyncio.create_task(self._run())
        self._accepting = True

    def _write_record(self, record: bytes) -> None:
        with _pinned_directories(self.path.parent):
            backup = self.path.with_name(self.path.name + ".1")
            for path in (self.path, backup):
                check_private_path(path)
            if self._descriptor is None:
                raise RuntimeError("diagnostic descriptor unavailable")
            opened, named = os.fstat(self._descriptor), self.path.lstat()
            if (opened.st_nlink != 1 or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
                raise ValueError("diagnostic file changed")
            if opened.st_size + len(record) > 256 * 1024:
                os.close(self._descriptor)
                self._descriptor = None
                os.replace(self.path, backup)
                self._descriptor = open_private_file(self.path, exclusive=True)
            view = memoryview(record)
            while view:
                written = os.write(self._descriptor, view)
                if written <= 0:
                    raise OSError("diagnostic write failed")
                view = view[written:]
            os.fsync(self._descriptor)

    async def _run(self) -> None:
        while True:
            record = await self._queue.get()
            try:
                if record is None:
                    return
                if self._failed:
                    self.dropped += 1
                    continue
                self._io = asyncio.create_task(asyncio.to_thread(self._write_record, record))
                try:
                    await asyncio.shield(self._io)
                except Exception:
                    self._failed = True
                    self.dropped += 1
            finally:
                self._queue.task_done()

    def _close_descriptor(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None

    async def _finish(self) -> None:
        if self._starter is not None:
            try:
                await asyncio.shield(self._starter)
            except Exception:
                self._failed = True
        if self._writer is not None:
            await self._queue.put(None)
            await asyncio.shield(self._writer)
        self._io = asyncio.create_task(asyncio.to_thread(self._close_descriptor))
        await asyncio.shield(self._io)

    async def close(self) -> None:
        global _ACTIVE
        self._closing, self._accepting = True, False
        if self._closer is None:
            self._closer = asyncio.create_task(self._finish())
        await asyncio.wait_for(asyncio.shield(self._closer), timeout=self.close_timeout)
        if _ACTIVE is self:
            _ACTIVE = None
        if self._failed:
            raise RuntimeError("diagnostic writer failed")

    @property
    def settlement_status(self) -> str:
        if (self._descriptor is not None or any(task is not None and not task.done()
                for task in (self._starter, self._writer, self._closer, self._io))):
            return "unsettled"
        return "settled_failed" if self._failed else "settled"
