"""Explicit identities and immutable transfer objects; no runtime activation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class Scope:
    """Access owner, not the individual group member who authored a message."""

    agent_id: str
    user_id: str
    connector_id: str | None = None
    conversation_id: str | None = None


@dataclass(frozen=True)
class DerivationSpec:
    processor: str
    processor_version: str
    target_domain: str
    model: str = ""
    prompt_hash: str = ""


@dataclass(frozen=True)
class ResultAnchor:
    """Opaque target identifiers, not memory text or a Ledger write."""

    memory_id: str
    store_ref: str


@dataclass(frozen=True)
class AttachmentRef:
    """Opaque asset ID only: resolving files requires a later scoped Adapter."""

    attachment_id: str
    content_type: str
    sha256: str
    size_bytes: int
    description: str = ""


@dataclass(frozen=True)
class MessageDraft:
    message_id: str
    conversation_id: str
    session_id: str
    role: str
    content: str
    content_type: str
    created_at: str
    source: str
    sender_id: str
    idempotency_key: str | None = None
    reply_to_message_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    derivations: tuple[DerivationSpec, ...] = ()
    attachment_refs: tuple[AttachmentRef, ...] = ()


@dataclass(frozen=True)
class StoredMessage:
    message_id: str
    agent_id: str
    user_id: str
    connector_id: str
    conversation_id: str
    session_id: str
    ordinal: int
    role: str
    content: str
    content_type: str
    created_at: str
    received_at: str
    edited_at: str | None
    source: str
    sender_id: str
    status: str
    revision: int
    content_hash: str
    reply_to_message_id: str | None
    metadata: Mapping[str, Any]
    attachment_refs: tuple[AttachmentRef, ...]


@dataclass(frozen=True)
class RawMessageEvidence:
    """A located original slice, never a complete StoredMessage."""

    message_id: str
    agent_id: str
    user_id: str
    connector_id: str
    conversation_id: str
    session_id: str
    ordinal: int
    role: str
    content: str
    created_at: str
    status: str
    revision: int
    content_hash: str
    content_is_excerpt: bool
    excerpt_start_char: int
    excerpt_end_char: int
    total_content_chars: int
    total_content_bytes: int


@dataclass(frozen=True)
class RawContentChunk:
    message_id: str
    revision: int
    content_hash: str
    content: str
    start_char: int
    next_char: int
    total_chars: int
    total_bytes: int
    eof: bool


class RawMessageError(ValueError):
    """Errors deliberately contain no private payloads or identifiers."""


class RawPersistenceError(RawMessageError):
    """A storage/IO failure whose outcome must not be acknowledged as saved."""


class ConflictError(RawMessageError):
    pass


class ScopeError(RawMessageError):
    pass
