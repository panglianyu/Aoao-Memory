"""Explicit trusted daily-capture configuration; no defaults or file loading."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from kernel.raw_message_models import DerivationSpec, RawMessageError
from kernel.raw_message_validation import _id, _specs


@dataclass(frozen=True)
class ConversationBinding:
    local_key: str
    provider_id: str
    kind: str
    display_name: str
    proactive_send: bool = False


@dataclass(frozen=True)
class ConnectorBinding:
    agent_id: str
    source: str
    connector_id: str
    account_ref: str
    conversations: tuple[ConversationBinding, ...]


def _formal_path(value: object) -> None:
    """Validate path spelling only; the loading boundary owns native checks."""
    if type(value) is not str or not value or len(value) > 32767:
        raise RawMessageError("invalid formal embedding path")
    path = Path(value)
    if (not path.is_absolute() or path.as_posix() != value.replace("\\", "/")
            or (path.drive and (len(path.drive) != 2 or path.drive[1] != ":"
                                or path.drive[0] not in
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"))):
        raise RawMessageError("canonical absolute formal embedding path required")
    reserved = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
                *(f"LPT{i}" for i in range(1, 10))}
    for part in path.parts[1:]:
        if (part in {".", ".."} or part != part.rstrip(" .")
                or any(ord(char) < 32 or char in '<>:"|?*' for char in part)
                or part.split(".", 1)[0].upper() in reserved):
            raise RawMessageError("ordinary formal embedding path required")


@dataclass(frozen=True)
class FormalEmbeddingBinding:
    """Passive, closed model descriptor; not proof that materials were loaded."""

    schema: int
    model_id: str
    revision: str
    model_root: str
    cache_root: str
    material_path: str
    material_sha256: str
    module_policy_sha256: str
    loader_policy_sha256: str
    device: str
    dtype: str
    normalize: bool
    dimension: int
    backend: str
    max_seq_length: int

    def __post_init__(self) -> None:
        for value, expected in ((self.schema, 1), (self.dimension, 1024),
                                (self.max_seq_length, 8192)):
            if type(value) is not int or value != expected:
                raise RawMessageError("invalid formal embedding numeric identity")
        for value, expected in ((self.model_id, "BAAI/bge-m3"), (self.device, "cpu"),
                                (self.dtype, "float32"), (self.backend, "torch")):
            if type(value) is not str or value != expected:
                raise RawMessageError("invalid formal embedding identity")
        if type(self.normalize) is not bool or self.normalize is not True:
            raise RawMessageError("formal embedding normalization required")
        for value, length in ((self.revision, 40), (self.material_sha256, 64),
                              (self.module_policy_sha256, 64),
                              (self.loader_policy_sha256, 64)):
            if (type(value) is not str or len(value) != length
                    or any(char not in "0123456789abcdef" for char in value)):
                raise RawMessageError("invalid formal embedding digest")
        for value in (self.model_root, self.cache_root, self.material_path):
            _formal_path(value)

    @classmethod
    def from_payload(cls, payload: object) -> FormalEmbeddingBinding:
        expected = {"schema", "model_id", "revision", "model_root", "cache_root",
                    "material_path", "material_sha256", "module_policy_sha256",
                    "loader_policy_sha256", "device", "dtype", "normalize",
                    "dimension", "backend", "max_seq_length"}
        if (type(payload) is not dict or any(type(key) is not str for key in payload)
                or set(payload) != expected):
            raise RawMessageError("invalid formal embedding descriptor fields")
        return cls(**payload)

    def as_payload(self) -> dict[str, object]:
        return {"schema": self.schema, "model_id": self.model_id,
                "revision": self.revision, "model_root": self.model_root,
                "cache_root": self.cache_root, "material_path": self.material_path,
                "material_sha256": self.material_sha256,
                "module_policy_sha256": self.module_policy_sha256,
                "loader_policy_sha256": self.loader_policy_sha256,
                "device": self.device, "dtype": self.dtype, "normalize": self.normalize,
                "dimension": self.dimension, "backend": self.backend,
                "max_seq_length": self.max_seq_length}

    @property
    def binding_sha256(self) -> str:
        body = json.dumps(self.as_payload(), ensure_ascii=True, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
        return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class RawMessageProfile:
    database: Path
    owner_id: str
    connectors: tuple[ConnectorBinding, ...]
    derivations: tuple[DerivationSpec, ...]
    # Passive configuration only; neither capture nor boot activates a worker.
    derived_root: Path | None = None
    derivation_input_chars: int = 32000
    formal_embedding: FormalEmbeddingBinding | None = None

    def __post_init__(self) -> None:
        if (type(self.derivation_input_chars) is not int
                or not 1 <= self.derivation_input_chars <= 128000):
            raise RawMessageError("invalid derivation input capacity")
        if (self.formal_embedding is not None
                and type(self.formal_embedding) is not FormalEmbeddingBinding):
            raise RawMessageError("invalid formal embedding binding")
        if not isinstance(self.database, Path) or not self.database.is_absolute():
            raise RawMessageError("explicit absolute database path required")
        if self.derived_root is not None:
            if not isinstance(self.derived_root, Path) or not self.derived_root.is_absolute():
                raise RawMessageError("explicit absolute derived root required")
            raw_dir, derived = self.database.parent.resolve(), self.derived_root.resolve()
            if derived.is_relative_to(raw_dir) or raw_dir.is_relative_to(derived):
                raise RawMessageError("raw and derived directories must be disjoint")
        _id(self.owner_id)
        if type(self.connectors) is not tuple or not self.connectors:
            raise RawMessageError("explicit connectors required")
        if type(self.derivations) is not tuple or not self.derivations:
            raise RawMessageError("daily derivation profile required")
        _specs(self.derivations)
        keys: set[tuple[str, str]] = set()
        ids: set[str] = set()
        for binding in self.connectors:
            self._validate_connector(binding)
            key = (binding.agent_id, binding.source)
            if key in keys or binding.connector_id in ids:
                raise RawMessageError("ambiguous connector profile")
            keys.add(key)
            ids.add(binding.connector_id)

    @staticmethod
    def _validate_connector(binding: ConnectorBinding) -> None:
        if type(binding) is not ConnectorBinding:
            raise RawMessageError("invalid connector profile")
        for value in (binding.agent_id, binding.source, binding.connector_id, binding.account_ref):
            _id(value)
        if type(binding.conversations) is not tuple or not binding.conversations:
            raise RawMessageError("explicit conversations required")
        keys: set[str] = set()
        providers: set[str] = set()
        for chat in binding.conversations:
            if type(chat) is not ConversationBinding:
                raise RawMessageError("invalid conversation profile")
            for value in (chat.local_key, chat.provider_id, chat.display_name):
                _id(value)
            if chat.kind not in {"group", "dm", "web", "system"}:
                raise RawMessageError("invalid conversation kind")
            if type(chat.proactive_send) is not bool or (chat.proactive_send and chat.kind != "dm"):
                raise RawMessageError("invalid proactive target")
            if chat.local_key in keys or chat.provider_id in providers:
                raise RawMessageError("ambiguous conversation profile")
            keys.add(chat.local_key)
            providers.add(chat.provider_id)
