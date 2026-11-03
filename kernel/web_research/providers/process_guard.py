"""Supervise the fixed DDGS parser under a parent-owned Windows Job Object."""
from __future__ import annotations

import asyncio
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Protocol

_MAX_INPUT = 1024 * 1024
_MAX_OUTPUT = 512 * 1024
_PROCESS_MEMORY_LIMIT = 512 * 1024 * 1024
_CREATE_SUSPENDED = 0x00000004
_CREATE_UNICODE_ENVIRONMENT = 0x00000400
_CREATE_NO_WINDOW = 0x08000000


class GuardedProcess(Protocol):
    async def wait(self, timeout: float) -> int: ...
    def terminate(self) -> None: ...
    def close(self) -> None: ...


class ProcessLauncher(Protocol):
    def spawn(self, argv: list[str], *, cwd: Path, env: dict[str, str]) -> GuardedProcess: ...


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
    )]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _Win32Process:
    def __init__(self, process_handle: int, job_handle: int, pid: int) -> None:
        self.process_handle = process_handle
        self.job_handle = job_handle
        self.pid = pid
        self._closed = False
        self._waiters: set[asyncio.Task] = set()

    async def wait(self, timeout: float) -> int:
        import _winapi

        milliseconds = max(1, min(int(timeout * 1000), 0xFFFFFFFE))
        waiter = asyncio.create_task(
            asyncio.to_thread(_winapi.WaitForSingleObject, self.process_handle, milliseconds),
        )
        self._waiters.add(waiter)
        try:
            result = await asyncio.shield(waiter)
        finally:
            if waiter.done():
                self._waiters.discard(waiter)
        if result == _winapi.WAIT_TIMEOUT:
            raise asyncio.TimeoutError
        if result != _winapi.WAIT_OBJECT_0:
            raise ValueError("parser_unavailable")
        return int(_winapi.GetExitCodeProcess(self.process_handle))

    async def drain(self) -> None:
        if self._waiters:
            await asyncio.gather(*tuple(self._waiters), return_exceptions=True)
            self._waiters.clear()

    def terminate(self) -> None:
        if not self._closed:
            _kernel32().TerminateJobObject(self.job_handle, 1)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        import _winapi

        _winapi.CloseHandle(self.process_handle)
        _kernel32().CloseHandle(self.job_handle)


def _kernel32():
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    api.CreateJobObjectW.restype = wintypes.HANDLE
    api.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    api.SetInformationJobObject.restype = wintypes.BOOL
    api.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    api.AssignProcessToJobObject.restype = wintypes.BOOL
    api.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    api.TerminateJobObject.restype = wintypes.BOOL
    api.ResumeThread.argtypes = [wintypes.HANDLE]
    api.ResumeThread.restype = wintypes.DWORD
    api.CloseHandle.argtypes = [wintypes.HANDLE]
    api.CloseHandle.restype = wintypes.BOOL
    return api


class _Win32Launcher:
    def spawn(self, argv: list[str], *, cwd: Path, env: dict[str, str]):
        if os.name != "nt":
            raise ValueError("parser_unavailable")
        import _winapi

        api = _kernel32()
        job = api.CreateJobObjectW(None, None)
        if not job:
            raise ValueError("parser_unavailable")
        info = _ExtendedLimitInformation()
        info.BasicLimitInformation.LimitFlags = 0x100 | 0x2000
        info.ProcessMemoryLimit = _PROCESS_MEMORY_LIMIT
        if not api.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
            api.CloseHandle(job)
            raise ValueError("parser_unavailable")
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = subprocess.SW_HIDE
        process = thread = None
        try:
            process, thread, pid, _ = _winapi.CreateProcess(
                argv[0], subprocess.list2cmdline(argv), None, None, False,
                _CREATE_SUSPENDED | _CREATE_NO_WINDOW | _CREATE_UNICODE_ENVIRONMENT,
                env, str(cwd), startup,
            )
            if not api.AssignProcessToJobObject(job, process):
                _winapi.TerminateProcess(process, 1)
                raise ValueError("parser_unavailable")
            if api.ResumeThread(thread) == 0xFFFFFFFF:
                api.TerminateJobObject(job, 1)
                raise ValueError("parser_unavailable")
            return _Win32Process(process, job, pid)
        except Exception:
            if process is not None:
                _winapi.CloseHandle(process)
            api.CloseHandle(job)
            raise
        finally:
            if thread is not None:
                _winapi.CloseHandle(thread)


class ParserSupervisor:
    def __init__(self, root: Path, *, launcher: ProcessLauncher | None = None,
                 temporary_root: Path | None = None) -> None:
        self.root = root.resolve()
        self.executable = self.root / "runtime/local_search/ddgs_env/Scripts/python.exe"
        self.worker = self.root / "kernel/web_research/providers/ddgs_parser_worker.py"
        self.lock = self.root / "deploy/local_search/ddgs-requirements.lock"
        self.temporary_root = (temporary_root or self.root / "runtime/local_search/parser").resolve()
        self.launcher = launcher or _Win32Launcher()
        self._slots = asyncio.Semaphore(2)
        self._active: set[GuardedProcess] = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self._closed = False

    async def parse(self, body: bytes, *, timeout: float = 15) -> list[dict[str, str]]:
        if self._closed:
            raise ValueError("parser_unavailable")
        if type(body) is not bytes or not body or len(body) > _MAX_INPUT or timeout <= 0:
            raise ValueError("parse_failed")
        deadline = time.monotonic() + timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("parse_timeout")
        try:
            await asyncio.wait_for(self._slots.acquire(), remaining)
        except asyncio.TimeoutError as exc:
            raise ValueError("parse_timeout") from exc
        try:
            if self._closed:
                raise ValueError("parser_unavailable")
            if deadline - time.monotonic() <= 0:
                raise ValueError("parse_timeout")
            return await self._parse(body, deadline)
        finally:
            self._slots.release()

    async def _parse(self, body: bytes, deadline: float) -> list[dict[str, str]]:
        for path in (self.executable, self.worker, self.lock):
            if not path.is_file():
                raise ValueError("parser_unavailable")
        self.temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="google-parse-", dir=self.temporary_root) as directory:
            source = Path(directory) / "input.html"
            output = Path(directory) / "result.json"
            source.write_bytes(body)
            argv = [
                str(self.executable), "-I", "-B", str(self.worker),
                "--input", str(source), "--output", str(output), "--lock", str(self.lock),
            ]
            if deadline - time.monotonic() <= 0:
                raise ValueError("parse_timeout")
            process = self.launcher.spawn(argv, cwd=self.root, env=_minimal_environment(directory))
            self._active.add(process)
            self._idle.clear()
            finished = False
            try:
                try:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    exit_code = await process.wait(remaining)
                    finished = True
                except asyncio.TimeoutError as exc:
                    raise ValueError("parse_timeout") from exc
                return _read_worker_result(output, exit_code)
            except (json.JSONDecodeError, OSError, UnicodeError) as exc:
                raise ValueError("parse_failed") from exc
            finally:
                if not finished:
                    process.terminate()
                drain = getattr(process, "drain", None)
                if drain is not None:
                    await drain()
                process.close()
                self._active.discard(process)
                if not self._active:
                    self._idle.set()

    async def close(self) -> None:
        self._closed = True
        for process in tuple(self._active):
            process.terminate()
        if self._active:
            try:
                await asyncio.wait_for(self._idle.wait(), 2)
            except asyncio.TimeoutError as exc:
                raise RuntimeError("parser_cleanup_failed") from exc


def _read_worker_result(output: Path, exit_code: int) -> list[dict[str, str]]:
    if not output.exists() or output.stat().st_size > _MAX_OUTPUT:
        raise ValueError("parse_failed")
    result = json.loads(output.read_text(encoding="utf-8"))
    if type(result) is dict and result.get("error") in {
        "parse_failed", "parser_unavailable", "response_too_large",
    }:
        raise ValueError(result["error"])
    if exit_code != 0:
        raise ValueError("parse_failed")
    return _validate_result(result)


def _minimal_environment(temporary: str) -> dict[str, str]:
    allowed = {key: value for key, value in os.environ.items() if key.upper() in {"SYSTEMROOT", "WINDIR"}}
    allowed.update({"TEMP": temporary, "TMP": temporary, "PYTHONDONTWRITEBYTECODE": "1"})
    return allowed


def _validate_result(result: object) -> list[dict[str, str]]:
    if type(result) is not dict or set(result) != {"results"} or type(result["results"]) is not list:
        raise ValueError("parse_failed")
    rows = result["results"]
    if len(rows) > 8:
        raise ValueError("response_too_large")
    for row in rows:
        if (type(row) is not dict or set(row) != {"title", "href", "body"}
                or any(not isinstance(value, str) for value in row.values())
                or len(row["title"]) > 512 or len(row["href"]) > 4096 or len(row["body"]) > 1600):
            raise ValueError("parse_failed")
    return rows
