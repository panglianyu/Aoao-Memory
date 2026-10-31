"""Use the existing local bge-m3 service inside the isolated vector owner."""

from __future__ import annotations

import math
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import stat


_FORMAL_FILES = frozenset({"1_Pooling/config.json", "README.md", "config.json",
    "config_sentence_transformers.json", "modules.json", "pytorch_model.bin", "sentence_bert_config.json",
    "sentencepiece.bpe.model", "special_tokens_map.json", "tokenizer.json", "tokenizer_config.json"})
_FORMAL_MODULES = [
    {"idx": 0, "name": "0", "path": "", "type": "sentence_transformers.models.Transformer"},
    {"idx": 1, "name": "1", "path": "1_Pooling", "type": "sentence_transformers.models.Pooling"},
    {"idx": 2, "name": "2", "path": "2_Normalize", "type": "sentence_transformers.models.Normalize"}]
_FORMAL_VENDOR = (
    "sentence_transformers/__init__.py", "sentence_transformers/SentenceTransformer.py",
    "sentence_transformers/util/misc.py", "sentence_transformers/util/file_io.py",
    "sentence_transformers/models/Module.py", "sentence_transformers/models/Transformer.py",
    "sentence_transformers/models/Pooling.py", "sentence_transformers/models/Normalize.py",
    "transformers/__init__.py", "transformers/modeling_utils.py", "transformers/dynamic_module_utils.py",
    "transformers/utils/hub.py", "transformers/utils/import_utils.py", "transformers/utils/peft_utils.py",
    "transformers/integrations/peft.py", "transformers/models/auto/auto_factory.py",
    "transformers/models/auto/configuration_auto.py", "transformers/models/auto/modeling_auto.py",
    "transformers/models/auto/tokenization_auto.py", "transformers/models/xlm_roberta/configuration_xlm_roberta.py",
    "transformers/models/xlm_roberta/modeling_xlm_roberta.py", "transformers/models/xlm_roberta/tokenization_xlm_roberta.py",
    "transformers/configuration_utils.py", "transformers/tokenization_utils_base.py", "transformers/tokenization_utils_tokenizers.py",
    "torch/version.py", "torch/serialization.py", "torch/_weights_only_unpickler.py", "huggingface_hub/__init__.py",
    "huggingface_hub/file_download.py", "huggingface_hub/utils/_validators.py", "huggingface_hub/_snapshot_download.py",
    "huggingface_hub/constants.py")
_FORMAL_VERSIONS = {"sentence_transformers": "5.2.3", "transformers": "5.3.0", "torch": "2.9.1+cpu", "huggingface_hub": "1.3.3"}


def _formal_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate formal metadata key")
            result[key] = value
        return result
    def constant(_):
        raise ValueError("invalid formal metadata number")
    return json.loads(body, object_pairs_hook=pairs, parse_constant=constant)


def _formal_canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


def _formal_ordinary(path):
    if not path.is_absolute():
        raise ValueError("formal material native rejected")
    for ancestor in (path, *path.parents):
        value = ancestor.lstat()
        if stat.S_ISLNK(value.st_mode) or getattr(value, "st_file_attributes", 0) & 0x400:
            raise ValueError("formal material native rejected")


def _formal_file(path, limit=None):
    """Metadata-only for large material; bounded same-handle reads for small sources."""
    _formal_ordinary(path)
    class FileId(ctypes.Structure):
        _fields_ = [("volume", ctypes.c_ulonglong), ("identity", ctypes.c_ubyte * 16)]
    class ByHandle(ctypes.Structure):
        _pack_ = 4
        _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME), ("accessed", wintypes.FILETIME),
                    ("written", wintypes.FILETIME), ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD),
                    ("size_low", wintypes.DWORD), ("links", wintypes.DWORD), ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.CreateFileW.argtypes, api.CreateFileW.restype = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE], wintypes.HANDLE
    api.GetFileInformationByHandle.argtypes, api.GetFileInformationByHandle.restype = [wintypes.HANDLE, ctypes.POINTER(ByHandle)], wintypes.BOOL
    api.GetFileInformationByHandleEx.argtypes, api.GetFileInformationByHandleEx.restype = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL
    api.GetFinalPathNameByHandleW.argtypes, api.GetFinalPathNameByHandleW.restype = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD], wintypes.DWORD
    api.ReadFile.argtypes, api.ReadFile.restype = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p], wintypes.BOOL
    api.CloseHandle.argtypes, api.CloseHandle.restype = [wintypes.HANDLE], wintypes.BOOL
    handle = api.CreateFileW(str(path), 0x80000080 if limit is not None else 0x80, 1, None, 3, 0x00200000, None)
    if handle in (None, ctypes.c_void_p(-1).value):
        raise ValueError("formal material native rejected")
    def info():
        ordinary, identity = ByHandle(), FileId()
        if not api.GetFileInformationByHandle(handle, ctypes.byref(ordinary)) or not api.GetFileInformationByHandleEx(handle, 18, ctypes.byref(identity), ctypes.sizeof(identity)):
            raise ValueError("formal material native rejected")
        name = ctypes.create_unicode_buffer(32768)
        count = api.GetFinalPathNameByHandleW(handle, name, len(name), 0)
        if (ordinary.attributes & (0x10 | 0x400) or ordinary.links != 1 or not 0 < count < len(name)
                or os.path.normcase(os.path.normpath(name.value.removeprefix("\\\\?\\"))) != os.path.normcase(os.path.normpath(str(path)))):
            raise ValueError("formal material native rejected")
        return {"volume_serial": str(identity.volume), "file_id_128": bytes(identity.identity).hex(),
            "last_write_time": str((ordinary.written.dwHighDateTime << 32) | ordinary.written.dwLowDateTime),
            "links": int(ordinary.links), "creation_time": str((ordinary.created.dwHighDateTime << 32) | ordinary.created.dwLowDateTime),
            "attributes": int(ordinary.attributes), "size": (ordinary.size_high << 32) | ordinary.size_low}
    try:
        before = info()
        body = None
        if limit is not None:
            if before["size"] > limit:
                raise ValueError("formal material integrity rejected")
            chunks, total = [], 0
            buffer = ctypes.create_string_buffer(65536)
            while True:
                count = wintypes.DWORD()
                if not api.ReadFile(handle, buffer, len(buffer), ctypes.byref(count), None) or count.value > len(buffer):
                    raise ValueError("formal material integrity rejected")
                if not count.value:
                    break
                total += count.value
                if total > limit:
                    raise ValueError("formal material integrity rejected")
                chunks.append(buffer.raw[:count.value])
            body = b"".join(chunks)
            if len(body) != before["size"]:
                raise ValueError("formal material integrity rejected")
        if info() != before:
            raise ValueError("formal material native rejected")
    finally:
        if not api.CloseHandle(handle):
            raise ValueError("formal material native rejected")
    _formal_ordinary(path)
    return body, before


def embed_bge_m3(texts: list[str]) -> list[list[float]]:
    if (type(texts) is not list or not 1 <= len(texts) <= 100
            or any(type(text) is not str or not text.strip() for text in texts)):
        raise ValueError("invalid embedding input")

    from kernel import embedding_service

    model = embedding_service.get_bge_m3_embedding()
    if model is None:
        raise RuntimeError("bge-m3 unavailable")
    generated = model(texts)
    if len(generated) != len(texts):
        raise RuntimeError("embedding count mismatch")
    vectors = [[float(value) for value in row] for row in generated]
    dimension = len(vectors[0])
    if (dimension == 0 or any(len(row) != dimension or not all(
            math.isfinite(value) for value in row) for row in vectors)):
        raise RuntimeError("invalid embedding values")
    return vectors


class FormalEmbeddingAdapter:
    """Explicit worker-local formal model boundary; never the kernel singleton."""

    def __init__(self, binding):
        from kernel.raw_message_profile import FormalEmbeddingBinding

        if type(binding) is not FormalEmbeddingBinding:
            raise ValueError("explicit formal embedding binding required")
        payload = binding.as_payload()
        material_body, _ = _formal_file(Path(binding.material_path), 65536)
        if hashlib.sha256(material_body).hexdigest() != binding.material_sha256:
            raise ValueError("formal material integrity rejected")
        material = _formal_json(material_body)
        if (type(material) is not dict or set(material) != {"schema", "model_id", "revision", "root", "files", "module_policy_sha256", "loader_policy_sha256"}
                or type(material["schema"]) is not int or material["schema"] != 1 or material["root"] != binding.model_root
                or material["model_id"] != binding.model_id or material["revision"] != binding.revision
                or material["module_policy_sha256"] != binding.module_policy_sha256 or material["loader_policy_sha256"] != binding.loader_policy_sha256):
            raise ValueError("formal material integrity rejected")
        root = Path(binding.model_root)
        _formal_ordinary(root)
        _formal_ordinary(Path(binding.cache_root))
        if not root.is_dir() or not Path(binding.cache_root).is_dir():
            raise ValueError("formal material native rejected")
        actual = set()
        for child in root.iterdir():
            _formal_ordinary(child)
            if child.name == "1_Pooling" and child.is_dir():
                for item in child.iterdir():
                    _formal_ordinary(item)
                    if item.name != "config.json":
                        raise ValueError("formal material inventory rejected")
                    actual.add("1_Pooling/" + item.name)
            else:
                actual.add(child.name)
            if len(actual) > 11:
                raise ValueError("formal material inventory rejected")
        rows = material["files"]
        if (actual != _FORMAL_FILES or type(rows) is not list or len(rows) != 11 or
                any(type(row) is not dict or set(row) != {"relative_path", "bytes", "sha256", "native"} for row in rows)
                or [row["relative_path"] for row in rows] != sorted(_FORMAL_FILES)):
            raise ValueError("formal material inventory rejected")
        configs = {}
        small = {"modules.json", "config.json", "sentence_bert_config.json", "1_Pooling/config.json"}
        for row in rows:
            relative = row["relative_path"]
            body, native = _formal_file(root / relative, 65536 if relative in small else None)
            expected = row["native"]
            if (type(expected) is not dict or set(expected) != {"volume_serial", "file_id_128", "last_write_time", "links"}
                    or any(type(expected[key]) is not type(native[key]) or expected[key] != native[key] for key in expected)):
                raise ValueError("formal material native rejected")
            if (type(row["bytes"]) is not int or native["size"] != row["bytes"] or type(row["sha256"]) is not str
                    or re.fullmatch("[0-9a-f]{64}", row["sha256"]) is None):
                raise ValueError("formal material integrity rejected")
            if body is not None:
                if hashlib.sha256(body).hexdigest() != row["sha256"]:
                    raise ValueError("formal material integrity rejected")
                configs[relative] = _formal_json(body)
        config, sentence, pooling = (configs[name] for name in ("config.json", "sentence_bert_config.json", "1_Pooling/config.json"))
        expected_config = {"model_type": "xlm-roberta", "architectures": ["XLMRobertaModel"], "hidden_size": 1024,
                           "max_position_embeddings": 8194, "torch_dtype": "float32"}
        modes = ("pooling_mode_mean_tokens", "pooling_mode_max_tokens", "pooling_mode_mean_sqrt_len_tokens", "pooling_mode_weightedmean_tokens", "pooling_mode_lasttoken")
        if (configs["modules.json"] != _FORMAL_MODULES or any(config.get(key) != value for key, value in expected_config.items())
                or config.get("auto_map") is not None or sentence.get("max_seq_length") != 8192 or sentence.get("do_lower_case") is not False
                or pooling.get("word_embedding_dimension") != 1024 or pooling.get("pooling_mode_cls_token") is not True
                or any(pooling.get(key, False) is not False for key in modes)):
            raise ValueError("formal module policy rejected")
        module = {"schema": 1, "modules": _FORMAL_MODULES, **expected_config, "auto_map": None,
                  "max_seq_length": 8192, "do_lower_case": False, "pooling": {"dimension": 1024, "mode": "cls"}}
        shared = {"local_files_only": True, "trust_remote_code": False, "token": False}
        constructor = {"model_name_or_path": binding.model_root, "device": "cpu", "cache_folder": binding.cache_root,
            "revision": binding.revision, **shared, "backend": "torch", "model_kwargs": {"weights_only": True,
            "use_safetensors": False, "torch_dtype": "torch.float32", **shared}, "tokenizer_kwargs": dict(shared),
            "config_kwargs": dict(shared), "encode": {"normalize_embeddings": True, "convert_to_numpy": True, "show_progress_bar": False}}
        files, versions, total = [], {}, 0
        repo = Path(__file__).resolve().parents[3]
        for relative in sorted(_FORMAL_VENDOR):
            body, native = _formal_file(repo / "runtime_env/Lib/site-packages" / relative, 16777216)
            total += len(body)
            if total > 67108864:
                raise ValueError("formal module policy rejected")
            files.append({"path": "runtime_env/Lib/site-packages/" + relative, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest().upper(), "native": native})
            package = relative.split("/", 1)[0]
            if relative == package + ("/version.py" if package == "torch" else "/__init__.py"):
                found = re.findall(r"^__version__(?:\s*:\s*str)?\s*=\s*['\"]([^'\"]+)['\"]", body.decode("utf-8-sig"), re.MULTILINE)
                if found != [_FORMAL_VERSIONS[package]]:
                    raise ValueError("formal module policy rejected")
                versions[package] = found[0]
        environment = {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "DISABLE_SAFETENSORS_CONVERSION": "1"}
        loader = {"schema": 1, "versions": versions, "files": files, "constructor": constructor, "environment": environment,
            "modules": _FORMAL_MODULES, "forbidden": ["network", "download", "auto_conversion", "remote_code", "custom_module", "safe_globals", "material_rehash_in_worker"]}
        if hashlib.sha256(_formal_canonical(module)).hexdigest() != binding.module_policy_sha256 or hashlib.sha256(_formal_canonical(loader)).hexdigest() != binding.loader_policy_sha256:
            raise ValueError("formal module policy rejected")
        os.environ.update(environment)
        import torch
        from sentence_transformers import SentenceTransformer
        kwargs = {key: value for key, value in constructor.items() if key not in {"model_name_or_path", "encode"}}
        kwargs["model_kwargs"] = dict(kwargs["model_kwargs"], torch_dtype=torch.float32)
        self._model = SentenceTransformer(binding.model_root, **kwargs)
        self._binding = payload
        self._calls, self._baseline = 0, None
        self._facts = {"model_id": material["model_id"], "revision": material["revision"],
            "material_sha256": hashlib.sha256(material_body).hexdigest(), "module_policy_sha256": hashlib.sha256(_formal_canonical(module)).hexdigest(),
            "loader_policy_sha256": hashlib.sha256(_formal_canonical(loader)).hexdigest(), "device": str(self._model.device),
            "dtype": str(self._model.dtype).removeprefix("torch."), "dimension": self._model.get_sentence_embedding_dimension(),
            "normalize": constructor["encode"]["normalize_embeddings"], "backend": "torch", "max_seq_length": self._model.max_seq_length}
        if any(type(self._facts[key]) is not type(payload[key]) or self._facts[key] != payload[key] for key in self._facts):
            raise ValueError("formal actual embedding rejected")

    def __call__(self, texts: list[str]) -> list[list[float]]:
        if type(texts) is not list or not 1 <= len(texts) <= 100 or any(type(text) is not str or not text.strip() for text in texts):
            raise ValueError("invalid embedding input")
        rows = self._model.encode(texts, normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False)
        vectors = [[float(value) for value in row] for row in rows]
        if len(vectors) != len(texts) or any(len(row) != 1024 or not all(math.isfinite(value) for value in row) for row in vectors):
            raise ValueError("invalid embedding values")
        self._calls += 1
        return vectors

    def actual(self) -> dict:
        if self._baseline is None:
            if self._calls != 1:
                raise ValueError("formal warm count rejected")
            self._baseline = self._calls
        return {**self._facts, "warm_calls": self._baseline}

    @property
    def encode_calls(self):
        if self._baseline is None:
            raise ValueError("formal warm unavailable")
        return self._calls - self._baseline
