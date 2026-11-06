"""Real Win32 snapshot guards must coexist on a shared cache ancestor."""

import ctypes
from ctypes import wintypes
import os

import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot


pytestmark = pytest.mark.skipif(os.name != "nt", reason="real Win32 sharing contract")


def _native_codes(error):
    result, seen = [], set()
    while error is not None and id(error) not in seen and len(result) < 4:
        seen.add(id(error))
        result.append({"type": type(error).__name__, "errno": getattr(error, "errno", None),
                       "winerror": getattr(error, "winerror", None)})
        error = error.__cause__ or error.__context__
    return result


@pytest.mark.parametrize("operation", ["capture", "close"])
def test_sibling_snapshot_cleanup_coexists_with_shared_anchor_guard(tmp_path, operation):
    scope = PipelineScope("synthetic-owner", "agent", "web", "parallel-cleanup")
    first = DailyVectorSnapshot(tmp_path / "first", (scope,), embedding="synthetic")
    second = DailyVectorSnapshot(tmp_path / "second", (scope,), embedding="synthetic")
    assert first._files.anchor == second._files.anchor
    try:
        if operation == "capture":
            (second.root / scope.partition_id).mkdir()
        with first._files._hold(first._files.anchor):
            try:
                if operation == "capture":
                    second._files.capture_partition(scope.partition_id)
                else:
                    second.close()
            except BaseException as error:
                # Numeric codes only: no exception body or private path.
                print("native_error_chain:", _native_codes(error))
                raise
        if operation == "capture":
            second._files.remove_empty(scope.partition_id)
        second.close()
        assert not second.root.exists()
    finally:
        # The first guard has unwound before either cleanup attempt.
        if operation == "capture" and (second.root / scope.partition_id).exists():
            second._files.remove_empty(scope.partition_id)
        second.close()
        first.close()


@pytest.mark.parametrize("access", [0x40000000, 0x10000], ids=["write", "delete"])
def test_shared_anchor_guard_still_denies_write_and_delete_handles(tmp_path, access):
    scope = PipelineScope("synthetic-owner", "agent", "web", "denied-access")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes, close.restype = [wintypes.HANDLE], wintypes.BOOL
    try:
        with cache._files._hold(cache._files.anchor):
            handle = create(str(cache._files.anchor), access, 7, None, 3,
                            0x02000000 | 0x00200000, None)
            code = ctypes.get_last_error()
            if handle != ctypes.c_void_p(-1).value:
                assert close(handle)
                pytest.fail("snapshot guard admitted a conflicting native handle")
            assert code == 32
    finally:
        cache.close()
