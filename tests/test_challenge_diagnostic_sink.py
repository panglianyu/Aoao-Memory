"""Bounded private diagnostics never serialize business data or own its locks."""

import asyncio
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from kernel import challenge_diagnostics as diagnostics


@pytest.mark.asyncio
async def test_closed_record_does_not_touch_error_text_or_unknown_fields(tmp_path):
    class SensitiveError(Exception):
        def __str__(self):
            raise AssertionError("must never stringify")

        def __repr__(self):
            raise AssertionError("must never represent")

    path = tmp_path / "private" / "failure.jsonl"
    sink = diagnostics.DiagnosticSink(path)
    await sink.start()
    try:
        try:
            raise SensitiveError("secret-key private-user raw-id prompt answer")
        except SensitiveError as error:
            diagnostics.emit_failure("secret-stage", error, domain="private-user", attempt="raw-id")
        diagnostics.emit_failure("prepare", diagnostics.DiagnosticError("json"), domain="rule", attempt=2)
    finally:
        await sink.close()
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows[0]["stage"] == rows[0]["reason"] == "unknown"
    assert rows[0]["error_type"] == "other"
    assert rows[0]["domain"] is None and rows[0]["attempt"] is None
    assert rows[1]["reason"] == "json" and rows[1]["domain"] == "rule"
    encoded = path.read_text()
    assert all(value not in encoded for value in ("secret", "private-user", "raw-id", str(tmp_path)))
    assert all(len(line.encode()) <= 2048 for line in encoded.splitlines())
    assert all(len(row["frames"]) <= 8 for row in rows)
    assert sink.settlement_status == "settled"


@pytest.mark.asyncio
async def test_overflow_is_bounded_and_exception_path_never_does_io(tmp_path, monkeypatch):
    sink = diagnostics.DiagnosticSink(tmp_path / "failures.jsonl")
    await sink.start()
    original = sink._write_record

    def forbidden(*args):
        raise AssertionError("synchronous diagnostic IO")

    monkeypatch.setattr(sink, "_write_record", forbidden)
    # No await: writer cannot run while the exception path is enqueueing.
    for _ in range(100):
        diagnostics.emit_failure("prepare", ValueError("private"))
    assert sink._queue.qsize() == 64 and sink.dropped == 36
    monkeypatch.setattr(sink, "_write_record", original)
    await sink.close()
    assert len(sink.path.read_text().splitlines()) == 64


@pytest.mark.asyncio
async def test_oversized_record_is_dropped_whole(tmp_path, monkeypatch):
    sink = diagnostics.DiagnosticSink(tmp_path / "failures.jsonl")
    await sink.start()
    monkeypatch.setattr(diagnostics, "_encode_record", lambda *args: b"x" * 2049)
    diagnostics.emit_failure("prepare", ValueError("private"))
    assert sink.dropped == 1 and sink._queue.empty()
    await sink.close()
    assert sink.path.read_bytes() == b""


@pytest.mark.asyncio
async def test_disk_failure_never_masks_business_failure_and_retains_failure_proof(tmp_path, monkeypatch):
    sink = diagnostics.DiagnosticSink(tmp_path / "failures.jsonl")
    await sink.start()

    def broken(record):
        raise OSError("private-disk-location")

    monkeypatch.setattr(sink, "_write_record", broken)
    diagnostics.emit_failure("prepare", ValueError("business-error"))
    with pytest.raises(RuntimeError, match="diagnostic writer failed"):
        await sink.close()
    assert sink.settlement_status == "settled_failed"
    assert sink._writer is not None and sink._writer.done()


@pytest.mark.asyncio
async def test_close_timeout_keeps_thread_owner_until_it_really_settles(tmp_path, monkeypatch):
    sink = diagnostics.DiagnosticSink(tmp_path / "failures.jsonl", close_timeout=0.02)
    await sink.start()
    entered, release = threading.Event(), threading.Event()
    original = sink._write_record

    def delayed(record):
        entered.set()
        release.wait(2)
        original(record)

    monkeypatch.setattr(sink, "_write_record", delayed)
    diagnostics.emit_failure("retirement", TimeoutError())
    assert await asyncio.to_thread(entered.wait, 1)
    try:
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await sink.close()
        assert sink.settlement_status == "unsettled"
        assert sink._writer is not None and not sink._writer.done()
        assert sink._io is not None and not sink._io.done()
    finally:
        release.set()
    await asyncio.sleep(0.05)
    await sink.close()
    assert sink.settlement_status == "settled"


@pytest.mark.asyncio
async def test_rotation_is_exactly_two_bounded_files(tmp_path):
    sink = diagnostics.DiagnosticSink(tmp_path / "failures.jsonl")
    await sink.start()
    try:
        for _ in range(32):
            for _ in range(64):
                diagnostics.emit_failure("prepare", ValueError())
            await sink._queue.join()
    finally:
        await sink.close()
    paths = list(tmp_path.iterdir())
    assert set(paths) == {sink.path, sink.path.with_name(sink.path.name + ".1")}
    for path in paths:
        assert path.stat().st_size <= 256 * 1024
        for line in path.read_bytes().splitlines():
            json.loads(line)


@pytest.mark.asyncio
async def test_hardlinked_destination_and_rotation_target_are_rejected(tmp_path):
    original = tmp_path / "original"
    original.write_text("must stay unchanged")
    path = tmp_path / "failures.jsonl"
    os.link(original, path)
    with pytest.raises(ValueError):
        await diagnostics.DiagnosticSink(path).start()
    assert original.read_text() == "must stay unchanged"
    path.unlink()
    os.link(original, tmp_path / "failures.jsonl.1")
    with pytest.raises(ValueError):
        await diagnostics.DiagnosticSink(path).start()
    assert original.read_text() == "must stay unchanged"


def test_private_helpers_reject_nonordinary_and_relative_paths(tmp_path):
    with pytest.raises(ValueError):
        diagnostics.ensure_private_directory(Path("relative"))
    directory = tmp_path / "private"
    diagnostics.ensure_private_directory(directory)
    with pytest.raises(ValueError):
        diagnostics.open_private_file(directory)
    descriptor = diagnostics.open_private_file(directory / "receipt", exclusive=True)
    os.close(descriptor)
    with pytest.raises(FileExistsError):
        diagnostics.open_private_file(directory / "receipt", exclusive=True)


def test_reparse_directory_ancestor_is_rejected_before_any_write(tmp_path, monkeypatch):
    path = tmp_path / "private"
    path.mkdir()
    original = Path.lstat

    def reparsed(target):
        result = original(target)
        if target != path:
            return result
        return SimpleNamespace(st_mode=result.st_mode, st_nlink=result.st_nlink,
                               st_file_attributes=1024)

    monkeypatch.setattr(Path, "lstat", reparsed)
    with pytest.raises(ValueError):
        diagnostics.open_private_file(path / "receipt", exclusive=True)
    assert not (path / "receipt").exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows private DACL contract")
def test_windows_exact_private_acl_is_installed_before_first_byte(tmp_path):
    import msvcrt

    ctypes, wintypes, kernel, security = diagnostics._windows_api()
    security.GetSecurityInfo.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.DWORD,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p)]
    security.ConvertSecurityDescriptorToStringSecurityDescriptorW.argtypes = [
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(wintypes.LPWSTR),
        ctypes.c_void_p]
    path = tmp_path / "private" / "receipt"
    diagnostics.ensure_private_directory(path.parent)
    descriptor = diagnostics.open_private_file(path, exclusive=True)
    try:
        assert os.fstat(descriptor).st_size == 0
        security_descriptor, text = ctypes.c_void_p(), wintypes.LPWSTR()
        assert security.GetSecurityInfo(msvcrt.get_osfhandle(descriptor), 1, 4,
            None, None, None, None, ctypes.byref(security_descriptor)) == 0
        try:
            assert security.ConvertSecurityDescriptorToStringSecurityDescriptorW(
                security_descriptor, 1, 4, ctypes.byref(text), None)
            try:
                expected = "D:P" + "".join(f"(A;;FA;;;{sid})" for sid in
                    (diagnostics._current_sid(), "SY", "BA"))
                # Windows preserves the auto-inheritance control bit while all
                # inherited ACEs are removed by the protected replacement DACL.
                assert text.value.replace("D:PAI", "D:P", 1) == expected
            finally:
                kernel.LocalFree(text)
        finally:
            kernel.LocalFree(security_descriptor)
    finally:
        os.close(descriptor)
