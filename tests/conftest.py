"""Import setup and default guard for offline memory regression tests."""
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'apps'))


@pytest.fixture(autouse=True)
def deny_unplanned_model_loads(monkeypatch):
    if os.environ.get('AMC_RUN_BOUNDED_LIVE') == '1':
        yield
        return
    from system_memory_god.storage import daily_embedding_adapter
    import sentence_transformers

    loader = daily_embedding_adapter._local_bge_model
    loader.cache_clear()

    def denied(*args, **kwargs):
        raise RuntimeError('real BGE loading is disabled in offline tests')

    monkeypatch.setattr(sentence_transformers, 'SentenceTransformer', denied)
    try:
        yield
    finally:
        loader.cache_clear()
