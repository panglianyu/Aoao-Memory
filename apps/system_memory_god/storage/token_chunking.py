"""Complete original slices using the exact, offline BGE tokenizer snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path


BGE_MODEL_ID = "BAAI/bge-m3"
BGE_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
MAX_CHUNK_CHARS = 8192
MAX_CHUNK_TOKENS = 8192
MAX_ORIGINAL_CHUNKS = 10


@lru_cache(maxsize=1)
def bge_snapshot() -> Path:
    from huggingface_hub import snapshot_download

    resolved = Path(snapshot_download(repo_id=BGE_MODEL_ID, revision=BGE_REVISION,
                                      local_files_only=True)).resolve(strict=True)
    if not resolved.is_dir() or resolved.name != BGE_REVISION:
        raise ValueError("BGE local snapshot binding unavailable")
    return resolved


@lru_cache(maxsize=1)
def bge_tokenizer():
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(str(bge_snapshot()), use_fast=True,
        local_files_only=True, trust_remote_code=False)
    if not getattr(tokenizer, "is_fast", False):
        raise ValueError("BGE fast tokenizer required")
    return tokenizer


def _encoding(text: str, tokenizer) -> tuple[list[int], tuple[int, ...]]:
    if getattr(tokenizer, "is_fast", False) is not True:
        raise ValueError("BGE fast tokenizer offsets required")
    encoded = tokenizer(text, add_special_tokens=True, truncation=False,
        return_offsets_mapping=True, return_attention_mask=False, return_token_type_ids=False)
    if not isinstance(encoded, Mapping):
        raise ValueError("invalid tokenizer result")
    ids, offsets = encoded.get("input_ids"), encoded.get("offset_mapping")
    if (type(ids) is not list or not ids or any(type(token) is not int for token in ids)
            or type(offsets) is not list or len(offsets) != len(ids)):
        raise ValueError("invalid tokenizer offsets")
    boundaries, previous = set(), 0
    for pair in offsets:
        if (type(pair) not in {list, tuple} or len(pair) != 2
                or any(type(value) is not int for value in pair)):
            raise ValueError("invalid tokenizer offsets")
        start, end = pair
        if not 0 <= start <= end <= len(text):
            raise ValueError("invalid tokenizer offsets")
        if start == end == 0:
            continue
        if start < previous:
            raise ValueError("unordered tokenizer offsets")
        previous = start
        if end:
            boundaries.add(end)
    return ids, tuple(sorted(boundaries))


def token_count(text: str, tokenizer=None) -> int:
    if type(text) is not str or not text:
        raise ValueError("nonempty tokenizer input required")
    ids, _ = _encoding(text, bge_tokenizer() if tokenizer is None else tokenizer)
    return len(ids)


def chunk_original(text: str, *, tokenizer=None) -> tuple[str, ...]:
    if (type(text) is not str or not text.strip()
            or len(text) > MAX_CHUNK_CHARS * MAX_ORIGINAL_CHUNKS):
        raise ValueError("original exceeds vector input capacity")
    tokenizer = bge_tokenizer() if tokenizer is None else tokenizer
    chunks: list[str] = []
    position = 0
    while position < len(text):
        if len(chunks) == MAX_ORIGINAL_CHUNKS:
            raise ValueError("original exceeds vector output capacity")
        candidate = text[position:position + MAX_CHUNK_CHARS]
        ids, boundaries = _encoding(candidate, tokenizer)
        end = len(candidate)
        if len(ids) > MAX_CHUNK_TOKENS:
            # Offsets select original character boundaries, never decoded text.
            # Re-tokenize each tested prefix: token merges can change at its end.
            lower, upper, end = 0, len(boundaries) - 1, 0
            while lower <= upper:
                middle = (lower + upper) // 2
                boundary = boundaries[middle]
                if token_count(candidate[:boundary], tokenizer) <= MAX_CHUNK_TOKENS:
                    end = boundary
                    lower = middle + 1
                else:
                    upper = middle - 1
            if not end:
                raise ValueError("original token boundary unavailable")
        chunk = candidate[:end]
        if not chunk.strip():
            raise ValueError("whitespace-only vector chunk cannot preserve original")
        if token_count(chunk, tokenizer) > MAX_CHUNK_TOKENS:
            raise ValueError("vector token capacity exceeded")
        chunks.append(chunk)
        position += len(chunk)
    if "".join(chunks) != text:
        raise ValueError("original token coverage mismatch")
    return tuple(chunks)
