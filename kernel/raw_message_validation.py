"""Private validation and canonicalization behind RawMessageStore."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import time

from kernel.raw_message_models import (
    AttachmentRef,
    DerivationSpec,
    MessageDraft,
    RawMessageError,
    Scope,
    ScopeError,
)

_METADATA_KEYS = frozenset(
    {
        "provider_message_id",
        "provider_message_type",
        "provider_source_type",
        "provider_limited",
        "media_limited",
        "provider_sequence",
        "origin_message_id",
        "turn_id",
        "run_id",
        "sender_display_name",
        "transport",
        "provider_timestamp",
        "provider_timestamp_limited",
        "delivery_status",
    }
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _content(value: str) -> str:
    if not isinstance(value, str):
        raise RawMessageError("text content required")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise RawMessageError("invalid content encoding") from None
    if len(encoded) > 2 * 1024 * 1024:
        raise RawMessageError("content too large")
    return hashlib.sha256(encoded).hexdigest()


def _id(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 256
        or "\x00" in value
    ):
        raise RawMessageError("invalid identity")
    return value


def _provider_type(value: object) -> str:
    """Keep only a bounded SDK type label, never arbitrary provider metadata."""
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,63}", value):
        return value
    return "invalid"


def _scope(scope: Scope) -> None:
    if not isinstance(scope, Scope):
        raise ScopeError("explicit scope required")
    _id(scope.agent_id)
    _id(scope.user_id)
    for value in (scope.connector_id, scope.conversation_id):
        if value is not None:
            _id(value)


def _predicate(scope: Scope, alias: str = "") -> tuple[str, list[str]]:
    """Apply the same scope restriction to every raw/conversation projection."""
    prefix = alias + "." if alias else ""
    fields = {"agent_id": scope.agent_id, "user_id": scope.user_id}
    if scope.connector_id is not None:
        fields["connector_id"] = scope.connector_id
    if scope.conversation_id is not None:
        fields["conversation_id"] = scope.conversation_id
    return " AND ".join(prefix + key + "=?" for key in fields), list(fields.values())


def _limits(
    scope: Scope, connector_id: str, conversation_id: str | None = None
) -> bool:
    return (scope.connector_id is None or scope.connector_id == connector_id) and (
        conversation_id is None
        or scope.conversation_id is None
        or scope.conversation_id == conversation_id
    )


def _timestamp(value: str) -> datetime:
    try:
        if not isinstance(value, str):
            raise TypeError
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
        if parsed.utcoffset() is None:
            raise ValueError
        return parsed
    except (ValueError, TypeError):
        raise RawMessageError("timezone-aware ISO timestamp required") from None


def _metadata(value) -> str:
    if not isinstance(value, dict) and not hasattr(value, "items"):
        raise RawMessageError("invalid metadata")
    if set(value) - _METADATA_KEYS:
        raise RawMessageError("metadata key not allowed")
    for item in value.values():
        if type(item) not in {str, int, bool, type(None)}:
            raise RawMessageError("invalid metadata value")
        if isinstance(item, str) and len(item) > 256:
            raise RawMessageError("metadata value too large")
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True)


def _specs(specs: tuple[DerivationSpec, ...]) -> str:
    if len(specs) > 32:
        raise RawMessageError("too many derivations")
    keys = set()
    payload = []
    for spec in specs:
        if not isinstance(spec, DerivationSpec):
            raise RawMessageError("invalid derivation specification")
        _id(spec.processor)
        _id(spec.processor_version)
        if spec.target_domain not in {
            "vector",
            "entity",
            "rule",
            "graph",
            "journal",
            "schedule",
        }:
            raise RawMessageError("invalid target domain")
        for value in (spec.model, spec.prompt_hash):
            if not isinstance(value, str) or len(value) > 256:
                raise RawMessageError("invalid processor provenance")
        key = (spec.processor, spec.processor_version, spec.target_domain)
        if key in keys:
            raise RawMessageError("duplicate derivation specification")
        keys.add(key)
        payload.append(asdict(spec))
    return json.dumps(
        sorted(
            payload,
            key=lambda row: (
                row["processor"],
                row["processor_version"],
                row["target_domain"],
            ),
        ),
        sort_keys=True,
    )


def _clock(value: float | None) -> float:
    value = time.time() if value is None else value
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise RawMessageError("invalid clock")
    return float(value)


def _attachments(refs: tuple[AttachmentRef, ...]) -> str:
    if len(refs) > 32:
        raise RawMessageError("too many attachments")
    seen = set()
    for ref in refs:
        if not isinstance(ref, AttachmentRef):
            raise RawMessageError("invalid attachment reference")
        _id(ref.attachment_id)
        _id(ref.content_type)
        if not isinstance(ref.sha256, str) or not re.fullmatch(
            r"[a-f0-9]{64}", ref.sha256
        ):
            raise RawMessageError("invalid attachment digest")
        if (
            type(ref.size_bytes) is not int
            or not 0 <= ref.size_bytes <= 100 * 1024 * 1024
        ):
            raise RawMessageError("invalid attachment size")
        if not isinstance(ref.description, str) or len(ref.description) > 1024:
            raise RawMessageError("invalid attachment description")
        if ref.attachment_id in seen:
            raise RawMessageError("duplicate attachment reference")
        seen.add(ref.attachment_id)
    return json.dumps([asdict(ref) for ref in refs], ensure_ascii=False, sort_keys=True)


def _draft_payload(scope: Scope, draft: MessageDraft) -> tuple[str, str, str, str]:
    _scope(scope)
    for value in (
        draft.message_id,
        draft.conversation_id,
        draft.session_id,
        draft.source,
        draft.sender_id,
    ):
        _id(value)
    _timestamp(draft.created_at)
    if draft.role not in {"user", "assistant", "system", "tool"}:
        raise RawMessageError("invalid role")
    if draft.content_type not in {"text", "image", "audio", "tool"}:
        raise RawMessageError("invalid content type")
    content_hash = _content(draft.content)
    if draft.idempotency_key is not None:
        _id(draft.idempotency_key)
    metadata = _metadata(draft.metadata)
    specifications = _specs(draft.derivations)
    attachment_refs = _attachments(draft.attachment_refs)
    payload_hash = _hash(
        json.dumps(
            [
                scope.agent_id,
                scope.user_id,
                draft.conversation_id,
                draft.session_id,
                draft.role,
                draft.content,
                draft.content_type,
                draft.created_at,
                draft.source,
                draft.sender_id,
                draft.reply_to_message_id,
                metadata,
                specifications,
                attachment_refs,
            ],
            ensure_ascii=False,
        )
    )
    return content_hash, metadata, attachment_refs, payload_hash
