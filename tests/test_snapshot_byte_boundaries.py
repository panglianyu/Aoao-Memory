"""Byte gates and unchanged snapshot ABI, with numerical fixtures only."""

import base64
import hashlib
import json

import numpy as np
import pytest

from kernel.memory_pipeline_models import PipelineScope, canonical
from system_memory_god.storage import daily_vector_snapshot as snap
from system_memory_god.storage.scoped_stores import ScopedStores
from system_memory_god.storage.supervised_daily_vector import SupervisedDailyVector


def scope():
    return PipelineScope("test-owner", "agent", "web", "byte-boundary")


@pytest.mark.parametrize("dimension,count", [(2048, 5120), (1, 11000)])
def test_dimension_drives_body_boundary_not_fixed_row_cap(tmp_path, dimension, count):
    vector = np.ones(dimension, dtype="<f4").tobytes()
    cache = snap.DailyVectorSnapshot(tmp_path / "cache", (scope(),), embedding="synthetic")
    try:
        generation = cache.publish(scope(), {f"m-{i:05d}": vector for i in range(count)})
        data = snap.load_snapshot(cache.root, scope(), generation, embedding="synthetic")
        assert data.matrix.shape == (count, dimension)
        assert len(data.cosine_hits([1.0] * dimension, limit=100)) == 100
    finally:
        cache.close()


@pytest.mark.parametrize("dimension,count", [(1024, 10241), (2048, 5121)])
def test_publish_over_body_budget_rejects_before_writing(tmp_path, dimension, count):
    vector = np.ones(dimension, dtype="<f4").tobytes()
    cache = snap.DailyVectorSnapshot(tmp_path / "cache", (scope(),), embedding="synthetic")
    try:
        with pytest.raises(snap.SnapshotCapacityError):
            cache.publish(scope(), {f"m-{i:05d}": vector for i in range(count)})
        assert not list(cache.root.iterdir())
    finally:
        cache.close()


@pytest.mark.parametrize("dimension,count", [(1024, 10241), (2048, 5121)])
def test_load_over_body_budget_never_reads_vector_file(tmp_path, monkeypatch, dimension, count):
    root = tmp_path / "fixture"
    manifest = {"schema": 1, "scope": scope().partition_id, "embedding": "synthetic",
                "rows": count, "dimension": dimension,
                "body_sha256": "a" * 64, "ids_sha256": "b" * 64}
    body = canonical(manifest).encode()
    generation = hashlib.sha256(body).hexdigest()
    path = root / scope().partition_id / generation
    path.mkdir(parents=True)
    (path / "manifest.json").write_bytes(body)
    (path / "vectors.f32").write_bytes(b"")
    (path / "ids.json").write_bytes(b"[]")
    calls = []
    original = snap._read

    def read_only_manifest(path, maximum):
        calls.append(path.name)
        assert path.name == "manifest.json", "over-budget body was read before rejection"
        return original(path, maximum)

    monkeypatch.setattr(snap, "_read", read_only_manifest)
    with pytest.raises(snap.SnapshotCapacityError):
        snap.load_snapshot(root, scope(), generation, embedding="synthetic")
    assert calls == ["manifest.json"]


@pytest.mark.parametrize("schema", [1, 2])
def test_legacy_three_file_schema_generation_and_bytes_unchanged(tmp_path, schema):
    binding = "f" * 64 if schema == 2 else None
    embedding = "bge-m3" if schema == 2 else "synthetic"
    cache = snap.DailyVectorSnapshot(tmp_path / "cache", (scope(),), embedding=embedding,
                                     binding_sha256=binding)
    vectors = {"a": np.ones(1024, dtype="<f4").tobytes()}
    try:
        generation = cache.publish(scope(), vectors)
        path = cache.root / scope().partition_id / generation
        before = {p.name: p.read_bytes() for p in path.iterdir()}
        manifest = json.loads(before["manifest.json"])
        assert manifest["schema"] == schema
        assert hashlib.sha256(canonical(manifest).encode()).hexdigest() == generation
        snap.load_snapshot(cache.root, scope(), generation, embedding=embedding,
                           binding_sha256=binding)
        assert {p.name: p.read_bytes() for p in path.iterdir()} == before
        assert set(before) == {"vectors.f32", "ids.json", "manifest.json"}
    finally:
        cache.close()


def receipt(key, values):
    body = np.asarray(values, dtype="<f4").tobytes()
    return {"memory_id": key, "embedding_dim": len(values), "payload_hash": "a" * 64,
            "text_hash": "b" * 64, "snapshot": {"schema": 1,
            "f32": base64.b64encode(body).decode("ascii"),
            "sha256": hashlib.sha256(body).hexdigest()}}


def test_duplicate_receipt_replacement_and_copy_ownership(tmp_path, monkeypatch):
    monkeypatch.setattr(snap, "MAX_BODY", 12)
    owner = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                 embedding="synthetic", enable_snapshot=True)
    row = receipt("one", [1])
    owner._capture_snapshot(scope(), [row] * 50)
    row["snapshot"]["f32"] = "external mutation"
    owner._capture_snapshot(scope(), [receipt("one", [1, 2]), receipt("two", [1])])
    assert len(owner.snapshot_vectors(scope())) == 2
    owner._capture_snapshot(scope(), [receipt("three", [1])])
    with pytest.raises(snap.SnapshotCapacityError):
        owner.snapshot_vectors(scope())
    assert set(owner._snapshot_rows[scope()]) == {"one", "two"}


@pytest.mark.parametrize("fault", ["sha", "nan", "base64", "dimension"])
def test_invalid_numerical_receipt_cannot_enter_capture(tmp_path, fault):
    owner = SupervisedDailyVector(ScopedStores(tmp_path / "derived"),
                                 embedding="synthetic", enable_snapshot=True)
    row = receipt("bad", [float("nan")] if fault == "nan" else [1])
    if fault == "sha":
        row["snapshot"]["sha256"] = "0" * 64
    elif fault == "base64":
        row["snapshot"]["f32"] = "!"
    elif fault == "dimension":
        row["embedding_dim"] = 2049
    with pytest.raises((ValueError, RuntimeError)):
        owner._capture_snapshot(scope(), [row])
    assert not owner._snapshot_rows[scope()]


@pytest.mark.parametrize("invalid", [True, -1, 1.5])
def test_shared_byte_gate_rejects_non_exact_nonnegative_int(invalid):
    with pytest.raises(ValueError):
        snap.snapshot_capacity(invalid)


def test_escaped_id_directory_exact_two_mib_then_one_byte_over(tmp_path):
    ids = [f"k{i:05d}" + "\U00020000" * 250 for i in range(696)]
    used = len(canonical(ids).encode("ascii"))
    wanted = snap.MAX_DIRECTORY - used - 1
    last = None
    for astral in range(251):
        ascii_count = wanted - 8 - astral * 12
        if 0 <= ascii_count and 6 + astral + ascii_count <= 256:
            last = "z99999" + "\U00020000" * astral + "x" * ascii_count
            break
    assert last is not None
    ids.append(last)
    assert len(canonical(ids).encode("ascii")) == snap.MAX_DIRECTORY
    vector = np.ones(1, dtype="<f4").tobytes()
    cache = snap.DailyVectorSnapshot(tmp_path / "cache", (scope(),), embedding="synthetic")
    try:
        generation = cache.publish(scope(), dict.fromkeys(ids, vector))
        assert len(snap.load_snapshot(cache.root, scope(), generation,
                                     embedding="synthetic").ids) == len(ids)
        larger = list(ids)
        larger[-1] = larger[-1] + "x"
        assert len(canonical(larger).encode("ascii")) == snap.MAX_DIRECTORY + 1
        with pytest.raises(snap.SnapshotCapacityError):
            cache.publish(scope(), dict.fromkeys(larger, vector))
        assert cache.acquire(scope()).generation == generation
        for lease in tuple(cache._leases.values()):
            cache.release(lease)
    finally:
        cache.close()
