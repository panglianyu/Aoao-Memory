"""Token coverage and model binding, without loading embedding weights."""

import hashlib
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from system_memory_god.memory_factory import RawOriginalExtractor
from system_memory_god.grouped_extractor import memory_specs
from system_memory_god.storage import daily_embedding_adapter, token_chunking


class CharacterTokenizer:
    is_fast = True

    def __call__(self, text, **kwargs):
        assert kwargs["truncation"] is False
        offsets = [(0, 0)]
        for index, char in enumerate(text):
            if not char.isspace():
                offsets.extend([(index, index + 1)] * (3 if char == "龘" else 1))
        offsets.append((0, 0))
        return {"input_ids": list(range(len(offsets))), "offset_mapping": offsets}


def assert_coverage(text, chunks, tokenizer):
    assert "".join(chunks) == text
    assert len(chunks) <= 10
    for chunk in chunks:
        assert chunk.strip()
        assert len(chunk) <= 8192
        assert token_chunking.token_count(chunk, tokenizer) <= 8192


@pytest.mark.parametrize("text", [
    "a" * 8190,
    "a" * 8192 + " tail-fact",
    "龘" * 5000 + "\nThe final fact: Ada is in Oslo.\n",
    "  中文 English\n" * 1600 + "tail",
], ids=["special-boundary", "character-boundary-tail", "rare-character-tail", "mixed-whitespace"])
def test_token_chunks_cover_original_tail_and_special_tokens(text):
    tokenizer = CharacterTokenizer()
    chunks = token_chunking.chunk_original(text, tokenizer=tokenizer)
    assert_coverage(text, chunks, tokenizer)


def test_chunk_capacity_failure_is_atomic_and_whitespace_is_not_dropped():
    tokenizer = CharacterTokenizer()
    with pytest.raises(ValueError, match="capacity"):
        token_chunking.chunk_original("龘" * 30000, tokenizer=tokenizer)
    with pytest.raises(ValueError, match="whitespace"):
        token_chunking.chunk_original(" " * 8192 + "tail", tokenizer=tokenizer)
    with pytest.raises(ValueError):
        token_chunking.chunk_original("a" * 81921, tokenizer=tokenizer)


@pytest.mark.parametrize("tokenizer", [
    SimpleNamespace(is_fast=False),
    lambda text, **kwargs: {"input_ids": [1], "offset_mapping": [(5, 99999)]},
])
def test_missing_or_invalid_offsets_do_not_fall_back(tokenizer):
    with pytest.raises(ValueError):
        token_chunking.chunk_original("original", tokenizer=tokenizer)


@pytest.mark.asyncio
async def test_raw_extractor_uses_token_chunks_without_rewriting_original():
    original = "龘" * 5000 + "\n end"
    extractor = RawOriginalExtractor(tokenizer=CharacterTokenizer())
    outputs = await extractor.extract("vector", SimpleNamespace(content=original), None, 2048)
    assert_coverage(original, [item["text"] for item in outputs], CharacterTokenizer())


def test_vector_processor_version_changes_without_structured_versions():
    specs = memory_specs("memory-full-v1", "test-model")
    assert specs[0].processor_version == "v2"
    assert specs[0].prompt_hash != hashlib.sha256(
        b"exact original slices of 8192 characters; no LLM").hexdigest()
    assert all(item.processor_version == "v1" for item in specs[1:])


def test_embedding_uses_local_binding_and_refuses_truncation(monkeypatch):
    calls = []
    model = SimpleNamespace(tokenizer=CharacterTokenizer(), max_seq_length=8192)

    def encode(texts, **kwargs):
        calls.append((texts, kwargs))
        return [[1.0, 0.0] for _ in texts]

    model.encode = encode
    monkeypatch.setattr(daily_embedding_adapter, "_local_bge_model", lambda: model)
    assert daily_embedding_adapter.embed_bge_m3(["safe text"]) == [[1.0, 0.0]]
    with pytest.raises(ValueError, match="token"):
        daily_embedding_adapter.embed_bge_m3(["龘" * 3000])
    assert len(calls) == 1
    assert calls[0][1]["prompt"] == ""


def test_local_snapshot_resolution_cannot_download(monkeypatch, tmp_path):
    import huggingface_hub

    snapshot = tmp_path / token_chunking.BGE_REVISION
    snapshot.mkdir()
    calls = []

    def local_only(**kwargs):
        calls.append(kwargs)
        return str(snapshot)

    monkeypatch.setattr(huggingface_hub, "snapshot_download", local_only)
    token_chunking.bge_snapshot.cache_clear()
    try:
        assert token_chunking.bge_snapshot() == snapshot
        assert calls == [{"repo_id": "BAAI/bge-m3", "revision": token_chunking.BGE_REVISION,
                          "local_files_only": True}]
    finally:
        token_chunking.bge_snapshot.cache_clear()


def test_model_loader_uses_same_snapshot_and_rejects_shorter_limit(monkeypatch, tmp_path):
    calls = []
    tokenizer = CharacterTokenizer()
    tokenizer.name_or_path = str(tmp_path)
    model = SimpleNamespace(tokenizer=tokenizer, max_seq_length=8192)

    def constructor(path, **kwargs):
        calls.append((path, kwargs))
        return model

    monkeypatch.setitem(sys.modules, "sentence_transformers",
                        SimpleNamespace(SentenceTransformer=constructor))
    monkeypatch.setattr(token_chunking, "bge_snapshot", lambda: tmp_path)
    daily_embedding_adapter._local_bge_model.cache_clear()
    try:
        assert daily_embedding_adapter._local_bge_model() is model
        path, kwargs = calls[0]
        assert path == str(tmp_path)
        assert kwargs["device"] == "cpu"
        assert kwargs["local_files_only"] is True and kwargs["trust_remote_code"] is False
        assert kwargs["tokenizer_kwargs"]["local_files_only"] is True
        model.max_seq_length = 512
        daily_embedding_adapter._local_bge_model.cache_clear()
        with pytest.raises(RuntimeError, match="binding"):
            daily_embedding_adapter._local_bge_model()
    finally:
        daily_embedding_adapter._local_bge_model.cache_clear()


def test_existing_local_bge_fast_tokenizer_covers_boundaries_without_weights():
    tokenizer = token_chunking.bge_tokenizer()
    assert tokenizer.is_fast
    assert Path(tokenizer.name_or_path).resolve() == token_chunking.bge_snapshot()
    texts = ["Hello 中文\n" * 1800 + " final-fact: 2042-03-06",
             "龘𠮷\U00020000" * 2400 + "尾部事实",
             " \n\t" + "x" * 8190 + "\nTAIL", "a" * 81920]
    for text in texts:
        assert_coverage(text, token_chunking.chunk_original(text, tokenizer=tokenizer), tokenizer)
    with pytest.raises(ValueError, match="capacity"):
        token_chunking.chunk_original("中" * 81920, tokenizer=tokenizer)
