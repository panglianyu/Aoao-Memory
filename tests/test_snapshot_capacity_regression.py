"""Real 1024D numerical regression; no model, SDK or production storage."""
import base64
import hashlib
from pathlib import Path
import time

import numpy as np
import psutil
import pytest

from kernel.memory_pipeline_models import PipelineScope
from system_memory_god.storage import daily_vector_snapshot as snapshots
from system_memory_god.storage import supervised_daily_vector as supervised
from system_memory_god.storage.scoped_stores import ScopedStores


@pytest.mark.parametrize("count", [5000, 5001, 5002, 10240])
def test_complete_numerical_capture_publish_load_and_tail_hit(tmp_path, count, record_property):
    started = time.perf_counter()
    candidate = Path(__file__).resolve().parents[1]
    for module in (snapshots, supervised):
        assert Path(module.__file__).resolve().is_relative_to(candidate)
    scope = PipelineScope("owner", "agent", "web", "capacity")
    owner = supervised.SupervisedDailyVector(
        ScopedStores(tmp_path / "derived"), embedding="synthetic", enable_snapshot=True)
    ordinary = np.zeros(1024, dtype="<f4")
    ordinary[1] = 1
    best = np.zeros(1024, dtype="<f4")
    best[0] = 1
    for offset in range(0, count, 100):
        rows = []
        for number in range(offset, min(offset + 100, count)):
            body = (best if number == count - 1 else ordinary).tobytes()
            rows.append({"memory_id": f"memory-{number:05d}",
                         "payload_hash": "a" * 64, "text_hash": "b" * 64,
                         "embedding_dim": 1024,
                         "snapshot": {"schema": 1,
                                      "f32": base64.b64encode(body).decode("ascii"),
                                      "sha256": hashlib.sha256(body).hexdigest()}})
        owner._capture_snapshot(scope, rows)
    captured = owner.snapshot_vectors(scope)
    assert len(captured) == count
    vectors = {key: base64.b64decode(row["snapshot"]["f32"], validate=True)
               for key, row in captured.items()}
    cache = snapshots.DailyVectorSnapshot(tmp_path / "cache", (scope,), embedding="synthetic")
    try:
        generation = cache.publish(scope, vectors)
        loaded = snapshots.load_snapshot(cache.root, scope, generation, embedding="synthetic")
        assert len(loaded.ids) == count and loaded.matrix.shape == (count, 1024)
        assert loaded.cosine_hits(best.tolist(), limit=1)[0][0] == f"memory-{count - 1:05d}"
        record_property("rows", count)
        record_property("dimension", 1024)
        record_property("elapsed_seconds", time.perf_counter() - started)
        memory = psutil.Process().memory_info()
        record_property("process_rss_bytes", memory.rss)
        record_property("process_peak_wset_bytes", memory.peak_wset)
    finally:
        cache.close()
