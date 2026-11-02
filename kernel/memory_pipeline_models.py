"""Daily derivation identities. Values only; importing never activates memory."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re


def identity(value: str) -> None:
    if type(value) is not str or not value.strip() or len(value) > 256:
        raise ValueError("invalid pipeline identity")
    if any(ord(char) < 32 for char in value):
        raise ValueError("invalid pipeline identity")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ValueError("invalid pipeline identity") from None


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False)


def derived_memory_id(task_id: str, output_index: int) -> str:
    identity(task_id)
    if type(output_index) is not int or not 0 <= output_index < 100:
        raise ValueError("invalid evidence index")
    return "mem_" + hashlib.sha256(canonical([task_id, output_index]).encode()).hexdigest()


@dataclass(frozen=True)
class PipelineScope:
    """Owner boundary, not the group member who sent a message; never optional."""

    user_id: str
    agent_id: str
    connector_id: str
    conversation_id: str

    def __post_init__(self) -> None:
        for value in self.key:
            identity(value)

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.user_id, self.agent_id, self.connector_id, self.conversation_id)

    @property
    def partition_id(self) -> str:
        return hashlib.sha256(canonical(self.key).encode("utf-8")).hexdigest()


@dataclass(frozen=True, order=True)
class SourceRevision:
    """Provenance only. The worker must verify RawStore existence and access."""

    raw_message_id: str
    revision: int
    task_id: str
    processor: str
    processor_version: str
    model: str
    prompt_hash: str

    def __post_init__(self) -> None:
        for value in (self.raw_message_id, self.task_id, self.processor,
                      self.processor_version, self.model):
            identity(value)
        if type(self.revision) is not int or not 1 <= self.revision <= 2**63 - 1:
            raise ValueError("invalid source revision")
        if type(self.prompt_hash) is not str or not re.fullmatch("[0-9a-f]{64}", self.prompt_hash):
            raise ValueError("invalid prompt hash")


@dataclass(frozen=True)
class PurgeInventory:
    """Immutable deletion plan after the partition's read-only inventory succeeds."""

    partition: PipelineScope
    raw_message_id: str
    revision: int
    task_ids: tuple[str, ...]
    memory_ids: tuple[str, ...]
    vector_ids: tuple[str, ...]
