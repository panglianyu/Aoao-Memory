"""Public checks for the isolated numerical snapshot feasibility probe."""

import asyncio
import base64
import hashlib
import os
from contextlib import contextmanager

import numpy as np
import pytest

from tests._snapshot_probe_helper import probe_capacity, probe_committed_vectors
from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage.daily_vector_index import DailyVectorIndex
from system_memory_god.storage.scoped_stores import ScopedStores


@contextmanager
def bound_delete_fault(monkeypatch, selected, *, close_handle=False, binding=False):
    """Fail one real Win32 delete/close; all other native calls still execute."""
    import ctypes
    from ctypes import wintypes
    import sys
    from pathlib import Path
    from system_memory_god.storage import daily_snapshot_file_ops as operations

    original = ctypes.WinDLL
    fault = {"enabled": True, "failures": 0, "deleted": []}

    def load(name, *args, **kwargs):
        kernel = original(name, *args, **kwargs)
        caller = sys._getframe(1).f_code.co_filename
        if caller != operations.__file__ and not (binding and caller.endswith("project_workspace_io.py")):
            return kernel
        creator = kernel.CreateFileW
        creator.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                            ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        creator.restype = wintypes.HANDLE
        setter = kernel.SetFileInformationByHandle
        setter.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        setter.restype = wintypes.BOOL
        closer = kernel.CloseHandle
        closer.argtypes, closer.restype = [wintypes.HANDLE], wintypes.BOOL
        getter = kernel.GetFinalPathNameByHandleW
        getter.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
        getter.restype = wintypes.DWORD

        def path_of(handle):
            length = getter(handle, None, 0, 0)
            buffer = ctypes.create_unicode_buffer(length + 1)
            assert getter(handle, buffer, len(buffer), 0)
            return Path(buffer.value)

        def delete(handle, kind, body, size):
            path = path_of(handle)
            if kind == 4 and fault["enabled"] and not close_handle and not binding and selected(path):
                fault["failures"] += 1
                raise OSError("injected native deletion failure")
            result = setter(handle, kind, body, size)
            if kind == 4 and result:
                fault["deleted"].append(path)
            return result

        def close(handle):
            if (close_handle and fault["enabled"]
                    and selected(path_of(handle)) and path_of(handle) in fault["deleted"]):
                fault["failures"] += 1
                ctypes.set_last_error(5)
                return False
            return closer(handle)

        def create(path, *args):
            if binding and fault["enabled"] and selected(Path(path)):
                fault["failures"] += 1
                raise OSError("injected native binding failure")
            return creator(path, *args)

        class NativeBoundary:
            def __getattr__(self, attribute):
                return delete if attribute == "SetFileInformationByHandle" else (
                    close if attribute == "CloseHandle" else (
                        create if attribute == "CreateFileW" else getattr(kernel, attribute)))

        return NativeBoundary()

    with monkeypatch.context() as native_fault:
        native_fault.setattr(ctypes, "WinDLL", load)
        try:
            yield fault
        finally:
            fault["enabled"] = False


def test_close_can_resume_after_pointer_deleted_but_partition_delete_failed(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "partial-partition")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    with bound_delete_fault(monkeypatch, lambda path: path.name == scope.partition_id) as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        assert fault["failures"] == 1
        assert not (cache.root / scope.partition_id / "current.json").exists()
        fault["enabled"] = False
        cache.close()
        cache.close()
        assert not cache.root.exists()


def test_failed_publication_staging_rollback_can_resume_in_close(tmp_path, monkeypatch):
    from pathlib import Path
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "rollback-staging")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    original_open = Path.open

    def fail_second_file(path, *args, **kwargs):
        if path.name == "ids.json" and path.parent.name.startswith("staging-") and args[:1] == ("xb",):
            raise OSError("injected publication write failure")
        return original_open(path, *args, **kwargs)

    with monkeypatch.context() as io_fault:
        io_fault.setattr(Path, "open", fail_second_file)
        with bound_delete_fault(monkeypatch, lambda path: path.name == "vectors.f32") as fault:
            with pytest.raises(OSError, match="injected"):
                cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
            assert fault["failures"] == 1
            fault["enabled"] = False
            cache.close()
            cache.close()
    assert not cache.root.exists()


@pytest.mark.parametrize("object_name,close_handle", [
    ("pointer", False), ("pointer", True), ("manifest.json", False), ("manifest.json", True)])
def test_failed_pointer_rollback_resumes_before_uncommitted_target(tmp_path, monkeypatch, object_name, close_handle):
    from pathlib import Path
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot, load_snapshot

    scope = PipelineScope("owner", "agent", "web", "rollback-pointer")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    original_generation = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    original_replace = Path.replace

    def fail_pointer_publication(path, *args, **kwargs):
        if path.name.startswith("pointer-"):
            raise OSError("injected pointer publication failure")
        return original_replace(path, *args, **kwargs)

    with monkeypatch.context() as io_fault:
        io_fault.setattr(Path, "replace", fail_pointer_publication)
        with bound_delete_fault(monkeypatch, lambda path: path.name.startswith("pointer-")
                                if object_name == "pointer" else path.name == object_name,
                                close_handle=close_handle) as fault:
            with pytest.raises(OSError, match="injected|cannot close bound"):
                cache.publish(scope, {"two": (np.ones(16, dtype="<f4") * 2).tobytes()})
            assert fault["failures"] == 1
            assert load_snapshot(cache.root, scope, original_generation, embedding="synthetic").ids == ("one",)
            lease = cache.acquire(scope)
            assert lease.generation == original_generation
            cache.release(lease)
            fault["enabled"] = False
            cache.close()
            cache.close()
    assert not cache.root.exists()


@pytest.mark.parametrize("tamper", ["body", "extra", "missing", "hardlink", "replacement"])
def test_uncommitted_rollback_refuses_changed_objects(tmp_path, monkeypatch, tamper):
    from pathlib import Path
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "rollback-proof")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    original_replace = Path.replace

    def fail_pointer(path, *args, **kwargs):
        if path.name.startswith("pointer-"):
            raise OSError("injected pointer publication failure")
        return original_replace(path, *args, **kwargs)

    with monkeypatch.context() as io_fault:
        io_fault.setattr(Path, "replace", fail_pointer)
        with bound_delete_fault(monkeypatch, lambda path: path.name.startswith("pointer-")) as fault:
            with pytest.raises(OSError, match="injected"):
                cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
            assert fault["failures"] == 1
            fault["enabled"] = False
            parent = cache.root / scope.partition_id
            directory = next(path for path in parent.iterdir() if path.is_dir())
            body = directory / "vectors.f32"
            survivor = body
            if tamper == "body":
                body.write_bytes(b"new-owner-body")
            elif tamper == "extra":
                survivor = directory / "new-owner-sentinel"
                survivor.write_bytes(b"must-survive")
            elif tamper == "missing":
                (directory / "ids.json").unlink()
            elif tamper == "hardlink":
                survivor = tmp_path / "external-link"
                os.link(body, survivor)
            else:
                directory.rename(tmp_path / "parked-generation")
                directory.mkdir()
                survivor = directory / "new-owner-sentinel"
                survivor.write_bytes(b"must-survive")
            expected = survivor.read_bytes()
            for _ in range(2):
                with pytest.raises((ValueError, OSError)):
                    cache.close()
                assert survivor.read_bytes() == expected
                assert cache.root.exists()


def test_partial_file_write_rollback_keeps_its_original_prefix_proof(tmp_path, monkeypatch):
    from pathlib import Path
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "partial-write")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    original_open = Path.open

    class InterruptedFile:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def fileno(self):
            return self.handle.fileno()

        def write(self, body):
            self.handle.write(body[:7])
            self.handle.flush()
            raise OSError("injected partial file write")

    def partial_file(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.name == "vectors.f32" and args[:1] == ("xb",):
            return InterruptedFile(handle)
        return handle

    with monkeypatch.context() as io_fault:
        io_fault.setattr(Path, "open", partial_file)
        with bound_delete_fault(monkeypatch, lambda path: path.name == "vectors.f32") as fault:
            with pytest.raises(OSError, match="injected"):
                cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
            assert fault["failures"] == 1
            assert next((cache.root / scope.partition_id).glob("staging-*/vectors.f32")).stat().st_size == 7
            fault["enabled"] = False
            cache.close()
            cache.close()
    assert not cache.root.exists()


@pytest.mark.parametrize("replacement", [False, True])
def test_first_partition_binding_failure_does_not_adopt_replacement(tmp_path, monkeypatch, replacement):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "first-partition")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    directory = cache.root / scope.partition_id
    with bound_delete_fault(monkeypatch, lambda path: path.name == scope.partition_id,
                            binding=True) as fault:
        with pytest.raises(Exception, match="目标目录链|injected native binding"):
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
        assert fault["failures"] == 1
        fault["enabled"] = False
        if replacement:
            directory.rename(tmp_path / "parked-original")
            directory.mkdir()
            with pytest.raises(ValueError, match="identity changed|ownership unknown"):
                cache.close()
            assert directory.is_dir() and not tuple(directory.iterdir())
        else:
            cache.close()
            cache.close()
            assert not cache.root.exists()


@pytest.mark.parametrize("retry_publication", [False, True])
def test_partition_without_initial_identity_remains_fail_closed(tmp_path, monkeypatch, retry_publication):
    from pathlib import Path
    import sys
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "unknown-partition")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    original_stat = Path.lstat

    def fail_initial_proof(path, *args, **kwargs):
        if path.name == scope.partition_id and sys._getframe(1).f_code.co_name == "capture_partition":
            raise OSError("injected initial identity failure")
        return original_stat(path, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(Path, "lstat", fail_initial_proof)
        with pytest.raises(OSError, match="initial identity"):
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    if retry_publication:
        directory = cache.root / scope.partition_id
        directory.rename(tmp_path / "parked-unknown")
        directory.mkdir()
        with pytest.raises(ValueError, match="ownership unknown"):
            cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
        assert not tuple(directory.iterdir())
    for _ in range(2):
        with pytest.raises(ValueError, match="ownership unknown"):
            cache.close()
        assert (cache.root / scope.partition_id).is_dir()


def test_close_resumes_only_its_remaining_generation_files(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "partial-files")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    directory = cache.root / scope.partition_id / generation
    with bound_delete_fault(monkeypatch, lambda path: path.name == "manifest.json") as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        assert not (directory / "ids.json").exists()
        assert (directory / "vectors.f32").exists()
        fault["enabled"] = False
        cache.close()
        assert not cache.root.exists()


def test_release_can_retry_retired_generation_partial_cleanup(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "release-retry")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    lease = cache.acquire(scope)
    cache.publish(scope, {"two": (np.ones(16, dtype="<f4") * 2).tobytes()})
    with bound_delete_fault(monkeypatch, lambda path: path.name == "manifest.json") as fault:
        with pytest.raises(OSError, match="injected"):
            cache.release(lease)
        fault["enabled"] = False
        cache.release(lease)
        assert not (cache.root / scope.partition_id / lease.generation).exists()
        with pytest.raises(ValueError, match="invalid snapshot lease"):
            cache.release(lease)
        cache.close()


def test_close_resumes_remaining_partition_and_then_ignores_a_new_owner(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scopes = tuple(PipelineScope("owner", "agent", "web", name) for name in ("first", "second"))
    cache = DailyVectorSnapshot(tmp_path / "cache", scopes, embedding="synthetic")
    for scope in scopes:
        cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    with bound_delete_fault(monkeypatch, lambda path: path.name == scopes[1].partition_id) as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        assert not (cache.root / scopes[0].partition_id).exists()
        fault["enabled"] = False
        cache.close()
    cache.root.mkdir()
    sentinel = cache.root / "new-owner"
    sentinel.write_bytes(b"must-survive")
    cache.close()
    assert sentinel.read_bytes() == b"must-survive"


@pytest.mark.parametrize("close_handle", [False, True])
@pytest.mark.parametrize("object_name", ["ids.json", "cache"])
def test_close_can_settle_native_failure_at_file_or_final_root(
        tmp_path, monkeypatch, close_handle, object_name):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "native-failure")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    with bound_delete_fault(monkeypatch, lambda path: path.name == object_name,
                            close_handle=close_handle) as fault:
        with pytest.raises(OSError) as failure:
            cache.close()
        assert fault["failures"] == 1
        if close_handle:
            owner = getattr(failure.value, "retained_owner", None)
            assert owner is not None
        fault["enabled"] = False
        if close_handle:
            owner.close()
        cache.close()
        cache.close()
        assert not cache.root.exists()


@pytest.mark.parametrize("change", ["content", "replacement", "missing", "extra", "hardlink"])
def test_partial_cleanup_still_refuses_changed_remaining_files(tmp_path, monkeypatch, change):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "partial-tamper")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    directory = cache.root / scope.partition_id / generation
    with bound_delete_fault(monkeypatch, lambda path: path.name == "manifest.json") as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        assert not (directory / "ids.json").exists()
        fault["enabled"] = False
        target = directory / "vectors.f32"
        original = target.read_bytes()
        if change == "content":
            target.write_bytes(b"x" * len(original))
        elif change == "replacement":
            target.rename(tmp_path / "parked-vector")
            target.write_bytes(original)
        elif change == "missing":
            target.unlink()
        elif change == "extra":
            (directory / "new-owner").write_bytes(b"must-survive")
        else:
            os.link(target, tmp_path / "external-link")
        with pytest.raises((ValueError, OSError)):
            cache.close()
        if change != "missing":
            assert target.read_bytes() == (b"x" * len(original) if change == "content" else original)
        if change == "extra":
            assert (directory / "new-owner").read_bytes() == b"must-survive"
        if change == "hardlink":
            assert (tmp_path / "external-link").read_bytes() == original


def test_partial_partition_cleanup_refuses_a_replaced_partition(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "partition-replacement")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    directory = cache.root / scope.partition_id
    with bound_delete_fault(monkeypatch, lambda path: path.name == scope.partition_id) as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        fault["enabled"] = False
        directory.rename(tmp_path / "parked-partition")
        directory.mkdir()
        sentinel = directory / "new-owner"
        sentinel.write_bytes(b"must-survive")
        with pytest.raises(ValueError, match="identity changed"):
            cache.close()
        assert sentinel.read_bytes() == b"must-survive"


def test_partial_pointer_cleanup_refuses_changed_pointer_content(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "pointer-content")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    pointer = cache.root / scope.partition_id / "current.json"
    with bound_delete_fault(monkeypatch, lambda path: path.name == "current.json") as fault:
        with pytest.raises(OSError, match="injected"):
            cache.close()
        fault["enabled"] = False
        pointer.write_bytes(b"{\"new_owner\":true}")
        with pytest.raises(ValueError, match="content changed"):
            cache.close()
        assert pointer.read_bytes() == b"{\"new_owner\":true}"


def test_committed_vectors_can_be_queried_without_the_chroma_writer(tmp_path):
    report = probe_committed_vectors(tmp_path)
    assert report["verified_rows"] == 100
    assert report["top_k"] == 100
    assert report["reopened_same_hits"] is True
    assert report["query_children_reaped"] is True
    assert report["snapshot_fields"] == ["ids", "vectors"]


def test_persisted_receipt_snapshot_is_opt_in_and_keeps_original_receipt(tmp_path):
    index = DailyVectorIndex(ScopedStores(tmp_path / "derived"), lambda texts: [[1.0, 2.0]])
    scope = PipelineScope("owner", "agent", "web", "chat")
    records = [{"memory_id": "one", "payload": {"text": "synthetic"},
                "payload_hash": "a" * 64}]
    asyncio.run(index.put(scope, records))
    original = asyncio.run(index.receipts(scope, records))
    expanded = asyncio.run(index.receipts(scope, records, snapshot=True))
    assert {key: value for key, value in expanded[0].items() if key != "snapshot"} == original[0]
    numerical = expanded[0]["snapshot"]
    body = base64.b64decode(numerical["f32"], validate=True)
    assert numerical == {"schema": 1, "f32": numerical["f32"],
                         "sha256": hashlib.sha256(body).hexdigest()}
    assert np.frombuffer(body, dtype="<f4").tolist() == [1.0, 2.0]


def test_numerical_cache_fits_eight_scope_capacity_and_query_budget(tmp_path):
    report = probe_capacity(tmp_path)
    assert report["go"] is True
    assert [case["rows_per_scope"] for case in report["cases"]] == [100, 1000, 5000]
    assert all(case["scopes"] == 8 and case["dimension"] == 2048
               and case["top_k"] == 100 for case in report["cases"])
    assert report["query_children_reaped"] is True


def test_cache_publishes_exact_numbers_and_reopens_scoped_top_k(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot, load_snapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    vectors = {"one": np.asarray([1, 0], dtype="<f4").tobytes(),
               "two": np.asarray([0, 1], dtype="<f4").tobytes()}
    generation = cache.publish(scope, vectors)
    lease = cache.acquire(scope)
    try:
        loaded = load_snapshot(cache.root, scope, lease.generation, embedding="synthetic")
        assert lease.generation == generation
        assert loaded.cosine_hits([1, 0], limit=1) == (("one", 1.0),)
        assert loaded.matrix.tobytes() == b"".join(vectors.values())
    finally:
        cache.release(lease)
        cache.close()


def test_retired_generation_remains_until_lease_is_returned(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot, load_snapshot, SnapshotCapacityError

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    first = {"one": np.asarray([1, 0], dtype="<f4").tobytes()}
    second = {"two": np.asarray([0, 1], dtype="<f4").tobytes()}
    cache.publish(scope, first)
    lease = cache.acquire(scope)
    cache.publish(scope, second)
    assert load_snapshot(cache.root, scope, lease.generation, embedding="synthetic").ids == ("one",)
    with pytest.raises(SnapshotCapacityError):
        cache.publish(scope, {**first, **second})
    cache.release(lease)
    assert not (cache.root / scope.partition_id / lease.generation).exists()
    cache.publish(scope, {**first, **second})
    cache.close()


@pytest.mark.parametrize("fault", ["hash", "extra", "hardlink", "scope", "generation"])
def test_snapshot_read_rejects_tampering_and_wrong_identity(tmp_path, fault):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot, load_snapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    directory = cache.root / scope.partition_id / generation
    if fault == "hash":
        (directory / "vectors.f32").write_bytes(b"\0" * 8)
    elif fault == "extra":
        (directory / "unrecognized").write_bytes(b"x")
    elif fault == "hardlink":
        os.link(directory / "vectors.f32", tmp_path / "external-link")
    elif fault == "scope":
        scope = PipelineScope("another-owner", "agent", "web", "chat")
    else:
        generation = "../escape"
    with pytest.raises((ValueError, FileNotFoundError)):
        load_snapshot(cache.root, scope, generation, embedding="synthetic")


def test_nonfinite_snapshot_does_not_replace_previous_generation(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    with pytest.raises(ValueError):
        cache.publish(scope, {"two": np.asarray([float("nan"), 0], dtype="<f4").tobytes()})
    lease = cache.acquire(scope)
    assert lease.generation == generation
    cache.release(lease)
    cache.close()


def test_snapshot_capacity_failure_does_not_change_original_vector_receipt(tmp_path):
    index = DailyVectorIndex(ScopedStores(tmp_path / "derived"), lambda texts: [[1.0] * 2049])
    scope = PipelineScope("owner", "agent", "web", "chat")
    records = [{"memory_id": "one", "payload": {"text": "synthetic"}, "payload_hash": "a" * 64}]
    asyncio.run(index.put(scope, records))
    original = asyncio.run(index.receipts(scope, records))
    expanded = asyncio.run(index.receipts(scope, records, snapshot=True))
    assert {key: value for key, value in expanded[0].items() if key != "snapshot"} == original[0]
    assert expanded[0]["snapshot"] == {"schema": 1, "unavailable": "vector_capacity_exceeded"}


@pytest.mark.skipif(os.name != "nt", reason="Windows directory sharing contract")
def test_cache_collection_cannot_redirect_deletion_after_validation(tmp_path, monkeypatch):
    import _winapi
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    lease = cache.acquire(scope)
    cache.publish(scope, {"two": np.asarray([0, 1], dtype="<f4").tobytes()})
    directory = cache.root / scope.partition_id / generation
    parked = directory.with_name("parked")
    external = tmp_path / "external-sentinel"
    external.mkdir()
    sentinels = {name: b"must-survive" for name in ("vectors.f32", "ids.json", "manifest.json")}
    for name, body in sentinels.items():
        (external / name).write_bytes(body)
    finite = np.isfinite
    attempts = []

    def finiteness_with_directory_replacement(value, *args, **kwargs):
        result = finite(value, *args, **kwargs)
        if getattr(value, "ndim", None) == 1 and not attempts:
            attempts.append(True)
            try:
                directory.rename(parked)
            except OSError:
                pass
            else:
                _winapi.CreateJunction(str(external), str(directory))
        return result

    monkeypatch.setattr(np, "isfinite", finiteness_with_directory_replacement)
    try:
        cache.release(lease)
    except (OSError, ValueError):
        pass
    assert attempts == [True]
    assert {name: (external / name).read_bytes() for name in sentinels} == sentinels


@pytest.mark.skipif(os.name != "nt", reason="Windows long path contract")
def test_cache_bound_cleanup_supports_long_roots(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot

    scope = PipelineScope("owner", "agent", "web", "chat")
    root = tmp_path / ("a" * 80) / ("b" * 80) / ("c" * 80) / "cache"
    assert len(str(root)) > 260
    cache = DailyVectorSnapshot(root, (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    cache.close()
    assert not cache.root.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows bound file contract")
def test_pointer_cleanup_holds_parent_until_bound_deletion(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    from system_memory_god.storage.daily_snapshot_file_ops import SnapshotFileOps

    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.asarray([1, 0], dtype="<f4").tobytes()})
    parent = cache.root / scope.partition_id
    attempted = []

    def validate():
        attempted.append(True)
        with pytest.raises(OSError):
            parent.rename(cache.root / "replacement")

    SnapshotFileOps(cache.root).remove_pointer(scope.partition_id, "current.json", validate)
    assert attempted == [True]
    assert not (parent / "current.json").exists()


@pytest.mark.parametrize("replaced", ["root", "anchor"])
def test_cache_cleanup_rejects_replaced_owner_and_keeps_external_sentinel(tmp_path, replaced):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    owned = tmp_path / "owned"
    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(owned / "cache", (scope,), embedding="synthetic")
    cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    target = cache.root if replaced == "root" else owned
    target.rename(target.with_name("parked-" + replaced))
    target.mkdir()
    sentinel = target / "external-sentinel"
    sentinel.write_bytes(b"must-survive")
    with pytest.raises((ValueError, OSError)):
        cache.close()
    assert sentinel.read_bytes() == b"must-survive"


def test_cleanup_refuses_hardlinked_generation_body(tmp_path):
    from system_memory_god.storage.daily_vector_snapshot import DailyVectorSnapshot
    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    generation = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    linked = tmp_path / "external-sentinel"
    os.link(cache.root / scope.partition_id / generation / "vectors.f32", linked)
    before = linked.read_bytes()
    with pytest.raises(ValueError):
        cache.close()
    assert linked.read_bytes() == before


def test_final_empty_root_deletion_rejects_replacement_after_identity_sample(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_snapshot_file_ops import SnapshotFileOps
    from pathlib import Path
    root = tmp_path / "cache"
    root.mkdir()
    operations = SnapshotFileOps(root)
    old_sample = root.lstat()
    root.rename(tmp_path / "parked")
    root.mkdir()
    original = Path.lstat
    samples = []
    def earlier_sample(path, *args, **kwargs):
        # Supply the earlier check's OS sample; actual deletion still uses
        # a real Windows handle to the replacement, not a mocked delete.
        if path.name == "cache" and not samples:
            samples.append(True)
            return old_sample
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "lstat", earlier_sample)
    with pytest.raises((ValueError, OSError)):
        operations.remove_empty()
    assert samples == [True]
    assert root.is_dir() and (tmp_path / "parked").is_dir()


def test_report_owner_rejects_replaced_root_before_acquisition_and_after_close(tmp_path):
    from system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory
    root = tmp_path / "reports"
    root.mkdir()
    captured = root.stat()
    root.rename(tmp_path / "parked-reports")
    root.mkdir()
    sentinel = root / "sentinel"
    sentinel.write_bytes(b"external")
    with pytest.raises(ValueError):
        OwnedDirectory(root, expected=(captured.st_dev, captured.st_ino))
    owner = OwnedDirectory(root)
    owner.close()
    with pytest.raises(ValueError):
        owner.publish("report.json", b"{}")
    assert sentinel.read_bytes() == b"external"
    assert not (root / "report.json").exists()


def test_report_publication_keeps_root_pinned_through_actual_fsync(tmp_path, monkeypatch):
    from system_memory_god.storage.daily_snapshot_file_ops import OwnedDirectory
    root = tmp_path / "reports"
    root.mkdir()
    sync = os.fsync
    attempted = []
    def competing_rename(fd):
        attempted.append(True)
        with pytest.raises(OSError):
            root.rename(tmp_path / "replacement")
        sync(fd)
    monkeypatch.setattr(os, "fsync", competing_rename)
    with OwnedDirectory(root) as owner:
        owner.publish("stage.json", b'{"stage":1}', replace=True)
        owner.publish("report.json", b'{"done":true}', replace=True)
    assert len(attempted) == 2
    assert (root / "stage.json").read_bytes() == b'{"stage":1}'
    assert (root / "report.json").read_bytes() == b'{"done":true}'


def test_staging_failure_does_not_replace_previous_source(tmp_path, monkeypatch):
    from system_memory_god.storage import daily_vector_snapshot as module
    from pathlib import Path
    scope = PipelineScope("owner", "agent", "web", "chat")
    cache = module.DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    first = cache.publish(scope, {"one": np.ones(16, dtype="<f4").tobytes()})
    original = Path.open
    def failed_write(path, *args, **kwargs):
        if path.name == "ids.json" and args[:1] == ("xb",):
            raise OSError("synthetic file failure")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", failed_write)
    with pytest.raises(OSError):
        cache.publish(scope, {"two": np.ones(16, dtype="<f4").tobytes()})
    lease = cache.acquire(scope)
    assert lease.generation == first
    assert not any(entry.name.startswith("staging-") for entry in (cache.root / scope.partition_id).iterdir())
    cache.release(lease)
    cache.close()
