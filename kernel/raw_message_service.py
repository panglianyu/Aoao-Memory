"""Durable daily capture with optional explicitly injected background derivation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import hmac
import json
import secrets
import sqlite3
from typing import TYPE_CHECKING, Literal, Callable

if TYPE_CHECKING:
    from protocols.memory_provider import MemoryServiceContract
    from kernel.memory_pipeline_models import PipelineScope
    from kernel.raw_message_worker import RawMessageWorker

from kernel.message import Message, MessageType
from kernel.raw_message_models import (
    AttachmentRef, MessageDraft, RawMessageError, RawPersistenceError, Scope, StoredMessage,
)
from kernel.raw_message_profile import ConnectorBinding, ConversationBinding, RawMessageProfile
from kernel.raw_message_store import RawMessageStore
from kernel.raw_message_validation import _provider_type


def stable_id(*parts: str) -> str:
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return "raw_" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ChannelOriginal:
    """Trusted adapter input, distinct from untrusted model-facing envelopes."""

    content: str
    content_type: str = "text"
    provider_message_id: str = ""
    created_at: str = ""
    sender_id: str = ""
    provider_conversation_id: str = ""
    sequence: int | None = None
    attachment_refs: tuple[AttachmentRef, ...] = ()
    provider_message_type: str = "text"
    provider_source_type: str = ""
    provider_limited: bool = False
    provider_timestamp_limited: bool = False
    media_limited: bool = False


@dataclass(frozen=True)
class PreparedReply:
    agent_id: str
    source: str
    target: str
    conversation_id: str
    message_id: str
    fresh: bool
    seal: str
    connector_id: str = ""
    provider_id: str = ""
    parent_id: str = ""
    parent_revision: int = 0


@dataclass(frozen=True)
class RawDeliveryReceipt:
    status: str
    connector_id: str
    provider_id: str
    text: str


@dataclass(frozen=True)
class ReplyIdentity:
    """In-process reply provenance; never accepted from channel metadata."""
    original: Message
    turn_id: str


class TurnReply(str):
    """String-compatible output that keeps its own immutable reply identity."""
    identity: ReplyIdentity

    def __new__(cls, text: str, identity: ReplyIdentity):
        value = super().__new__(cls, text)
        object.__setattr__(value, "identity", identity)
        return value

    def __setattr__(self, name, value):
        raise AttributeError("reply identity is immutable")


class RawMessageService:
    def __init__(self, profile: RawMessageProfile, *, selected_claims=None, selected_sources=None, on_failure=None):
        self.profile = profile
        self.store = RawMessageStore(profile.database)
        self._closed = False
        self._close_failure: Literal["write_failed", "settlement_failed"] | None = None
        self._secret = secrets.token_bytes(32)
        self._lock = asyncio.Lock()
        self._inflight: set[asyncio.Task] = set()
        self.worker: RawMessageWorker | None = None
        self._selected_claims = dict(selected_claims) if selected_claims is not None else None
        self._selected_sources = ({key: dict(row) for key, row in selected_sources.items()}
                                  if selected_sources is not None else None)
        self._on_failure = on_failure
        self.memory_service: MemoryServiceContract | None = None

    def start_worker(self, bus, *, blocked_scopes: frozenset[Scope] = frozenset(),
                     on_settled: Callable[[PipelineScope, str], None] | None = None) -> None:
        from kernel.raw_message_worker import RawMessageWorker

        if self._closed or self.worker is not None or self.profile.derived_root is None:
            raise RawMessageError("worker assembly unavailable")
        if getattr(bus, "raw_messages", None) is not self or getattr(bus, "_daily_derivation", None) is None:
            raise RawMessageError("worker processor unavailable")
        self.worker = RawMessageWorker(self.store, bus, self.configured_scopes(),
                                       max_input_chars=self.profile.derivation_input_chars,
                                       blocked_scopes=blocked_scopes, on_settled=on_settled,
                                       **({"selected_claims": self._selected_claims}
                                          if self._selected_claims is not None else {}),
                                       **({"selected_sources": self._selected_sources}
                                          if self._selected_sources is not None else {}),
                                       **({"on_failure": self._on_failure} if self._on_failure is not None else {}))
        self.worker.start()

    def configured_scopes(self) -> tuple[Scope, ...]:
        return tuple(Scope(binding.agent_id, self.profile.owner_id, binding.connector_id,
                             stable_id(binding.agent_id, self.profile.owner_id,
                                       binding.connector_id, chat.provider_id))
                       for binding in self.profile.connectors for chat in binding.conversations)

    async def initialize_profile(self) -> None:
        """Register only the trusted configured identities before runtime startup."""
        if self._closed:
            raise RawMessageError("capture service closed")

        def register() -> None:
            for binding in self.profile.connectors:
                scope = Scope(binding.agent_id, self.profile.owner_id)
                self.store.register_connector(scope, binding.connector_id, binding.source,
                                              binding.account_ref, binding.source)
                for chat in binding.conversations:
                    self.store.register_conversation(
                        scope, stable_id(binding.agent_id, self.profile.owner_id,
                                         binding.connector_id, chat.provider_id),
                        binding.connector_id, chat.provider_id, chat.kind, chat.display_name)

        task = asyncio.create_task(asyncio.to_thread(register))
        self._track_write(task)
        await asyncio.shield(task)

    def _track_write(self, task: asyncio.Task) -> None:
        """Own accepted completion even when its original waiter has left."""
        self._inflight.add(task)

        def finished(done: asyncio.Task) -> None:
            try:
                error = done.exception()
                # Validation and idempotency rejection are not storage damage.
                failed = (error is not None and (
                    isinstance(error, RawPersistenceError)
                    or not isinstance(error, RawMessageError)))
            except asyncio.CancelledError:
                failed = True
            if failed:
                self._close_failure = self._close_failure or "write_failed"
            self._inflight.discard(done)

        task.add_done_callback(finished)

    def _binding(self, msg: Message, agent_id: str) -> tuple[ConnectorBinding, ConversationBinding]:
        binding = next((b for b in self.profile.connectors
                        if b.agent_id == agent_id and b.source == msg.source), None)
        original = msg.get("raw_original")
        provider_id = original.provider_conversation_id if type(original) is ChannelOriginal else ""
        local_key = msg.get("chat_name") or msg.source
        local = next((c for c in binding.conversations if c.local_key == local_key), None) if binding else None
        chat = next((c for c in binding.conversations if c.provider_id == provider_id), None) if binding and provider_id else local
        if local is not None and provider_id and local != chat:
            raise RawMessageError("provider conversation conflict")
        if binding is None or chat is None:
            raise RawMessageError("conversation not configured")
        return binding, chat

    def proactive_target(self, agent_id: str, source: str) -> str | None:
        """Resolve only an unambiguous, configured direct-message destination."""
        binding = next((b for b in self.profile.connectors
                        if b.agent_id == agent_id and b.source == source), None)
        matches = [chat.local_key for chat in binding.conversations
                   if chat.proactive_send] if binding else []
        return matches[0] if len(matches) == 1 else None

    async def validate_turn(self, original: Message, *,
                            deadline: float | None = None) -> None:
        if self._closed or not self.verify(original) or original.role != "user":
            raise RawMessageError("saved turn required")
        scope = Scope(original.get("_raw_agent_id"), self.profile.owner_id,
                      original.get("connector_id"), original.get("conversation_id"))
        row = await asyncio.to_thread(self.store.get, scope,
                                      original.get("origin_message_id"),
                                      deadline=deadline)
        if row is None or row.revision != original.get("_raw_revision"):
            raise RawMessageError("turn source unavailable")

    def project_saved_event(self, original: Message, event: Message) -> Message:
        if (self._closed or not self.verify(original) or original.role != "user"
                or event.event_name != "on_user_speak"):
            raise RawMessageError("saved user event required")
        return self._owned_event(event, dict(original.metadata))

    def _owned_event(self, event: Message, metadata: dict) -> Message:
        keys = ("origin_message_id", "connector_id", "conversation_id", "session_id", "_raw_agent_id")
        event = replace(event, metadata={**event.metadata, **{key: metadata[key] for key in keys},
                                         "_daily_owned": True})
        return replace(event, metadata={**event.metadata, "_raw_seal": self._seal(event)})

    async def capture_turn_event(self, original: Message, event: Message,
                                 ticket: PreparedReply | None = None) -> Message:
        """Persist trusted emitted assistant/tool events before old listeners can run."""
        if (self._closed or not self.verify(original) or original.role != "user"
                or event.event_name not in {"on_agent_speak", "on_agent_stream_end", "on_tool_finish"}):
            raise RawMessageError("saved turn event required")
        agent_id = original.get("_raw_agent_id")
        binding, chat = self._binding(original, agent_id)
        scope = Scope(agent_id, self.profile.owner_id, binding.connector_id, original.get("conversation_id"))
        parent = await asyncio.to_thread(self.store.get, scope, original.get("origin_message_id"))
        if parent is None or parent.revision != original.get("_raw_revision"):
            raise RawMessageError("turn source unavailable")
        role = "tool" if event.event_name == "on_tool_finish" else "assistant"
        message_id = stable_id(parent.conversation_id, role, event.id)
        if ticket is not None:
            if (role != "assistant" or not self._verify_reply(ticket)
                    or ticket.agent_id != scope.agent_id
                    or ticket.connector_id != scope.connector_id
                    or ticket.conversation_id != parent.conversation_id
                    or ticket.parent_id != parent.message_id or ticket.parent_revision != parent.revision
                    or ticket.source != parent.source or ticket.provider_id != chat.provider_id):
                raise RawMessageError("reply receipt scope conflict")
            message_id = ticket.message_id
            outbound = await asyncio.to_thread(self.store.get, scope, message_id)
            if outbound is None or outbound.content != event.content or outbound.role != role:
                raise RawMessageError("reply receipt content conflict")
        existing = await asyncio.to_thread(self.store.get, scope, message_id)
        if existing is None:
            draft = MessageDraft(message_id=message_id, conversation_id=parent.conversation_id,
                session_id=parent.session_id, role=role, content=event.content,
                content_type="text", created_at=datetime.fromtimestamp(event.timestamp, timezone.utc).isoformat(),
                source=parent.source, sender_id=event.source, reply_to_message_id=parent.message_id,
                idempotency_key=message_id, metadata={"origin_message_id": parent.message_id},
                derivations=self.profile.derivations)
            async with self._lock:
                await self._write_reply(scope, binding, chat, draft,
                                        parent=(parent.message_id, parent.revision))
        elif existing.content != event.content or existing.role != role:
            raise RawMessageError("turn event conflict")
        return self._owned_event(event, {**original.metadata, "origin_message_id": message_id})

    def _seal(self, msg: Message) -> str:
        metadata = {k: v for k, v in msg.metadata.items() if k != "_raw_seal"}
        values = (msg.id, msg.timestamp, msg.source, msg.target, msg.role,
                  msg.msg_type.value, msg.content, msg.event_name, metadata)
        try:
            payload = json.dumps(values, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            raise RawMessageError("invalid capture metadata") from None
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def verify(self, msg: Message) -> bool:
        token = msg.get("_raw_seal")
        try:
            return isinstance(token, str) and hmac.compare_digest(token, self._seal(msg))
        except RawMessageError:
            return False

    def reframe(self, msg: Message, content: str) -> Message:
        """Trusted adapter may format model input after committing its original."""
        if not self.verify(msg):
            raise RawMessageError("invalid saved input")
        framed = replace(msg, content=content)
        return replace(framed, metadata={**framed.metadata, "_raw_seal": self._seal(framed)})

    async def capture(self, msg: Message, agent_id: str) -> Message:
        if self._closed:
            raise RawMessageError("capture service closed")
        if not isinstance(msg, Message) or msg.msg_type != MessageType.USER_INPUT:
            raise RawMessageError("human input required")
        if "_raw_seal" in msg.metadata:
            if not self.verify(msg):
                raise RawMessageError("invalid saved input")
            return msg
        binding, chat = self._binding(msg, agent_id)
        scope = Scope(agent_id, self.profile.owner_id)
        draft = self._input_draft(msg, scope, binding, chat)
        clean_metadata = {k: v for k, v in msg.metadata.items() if k != "raw_original"}
        accepted = replace(msg, metadata={**clean_metadata, "conversation_id": draft.conversation_id,
                                         "origin_message_id": draft.message_id,
                                         "session_id": draft.session_id, "_raw_agent_id": agent_id,
                                         "connector_id": binding.connector_id})
        saved = await self._write(scope, binding, chat, draft)
        accepted = replace(accepted, metadata={**accepted.metadata, "_raw_revision": saved.revision})
        return replace(accepted, metadata={**accepted.metadata, "_raw_seal": self._seal(accepted)})

    async def restore_turn(self, saved: dict) -> Message:
        """Reissue a process-local capability from a complete saved raw reference."""
        if not isinstance(saved, dict) or self._closed or saved.get("owner_id") != self.profile.owner_id:
            raise RawMessageError("saved task owner unavailable")
        fields: dict[str, str] = {}
        for key in ("agent_id", "owner_id", "connector_id", "conversation_id", "event_id", "source", "target"):
            value = saved.get(key)
            if not isinstance(value, str) or not value:
                raise RawMessageError("saved task scope unavailable")
            fields[key] = value
        if type(saved.get("revision")) is not int:
            raise RawMessageError("saved task revision unavailable")
        scope = Scope(fields["agent_id"], fields["owner_id"], fields["connector_id"], fields["conversation_id"])
        row = await asyncio.to_thread(self.store.get, scope, fields["event_id"])
        if (row is None or row.role != "user" or row.status != "active"
                or row.revision != saved.get("revision") or row.source != saved.get("source")):
            raise RawMessageError("saved task source unavailable")
        original = Message.user_input(row.content, row.source, chat_name=saved.get("target"))
        binding, chat = self._binding(original, row.agent_id)
        if (binding.connector_id != row.connector_id
                or stable_id(row.agent_id, row.user_id, binding.connector_id, chat.provider_id) != row.conversation_id):
            raise RawMessageError("saved task target unavailable")
        original = replace(original, metadata={**original.metadata, "_raw_agent_id": row.agent_id,
            "connector_id": row.connector_id, "conversation_id": row.conversation_id,
            "session_id": row.session_id, "origin_message_id": row.message_id, "_raw_revision": row.revision})
        return replace(original, metadata={**original.metadata, "_raw_seal": self._seal(original)})

    @staticmethod
    def _channel_original(msg: Message, chat: ConversationBinding) -> ChannelOriginal:
        original = msg.get("raw_original")
        if original is None and msg.source in {"web", "console"}:
            original = ChannelOriginal(content=msg.content)
        if type(original) is not ChannelOriginal:
            raise RawMessageError("original channel content required")
        if original.provider_message_type not in {"text", "post", "image", "audio", "file", "unsupported"}:
            raise RawMessageError("unsupported provider message type")
        if (type(original.media_limited) is not bool or type(original.provider_limited) is not bool
                or type(original.provider_timestamp_limited) is not bool):
            raise RawMessageError("invalid media limitation")
        if original.provider_message_type == "unsupported" and not original.provider_limited:
            raise RawMessageError("unsupported provider type requires restriction")
        if original.provider_conversation_id and original.provider_conversation_id != chat.provider_id:
            raise RawMessageError("provider conversation conflict")
        return original

    def _input_draft(self, msg: Message, scope: Scope, binding: ConnectorBinding,
                     chat: ConversationBinding) -> MessageDraft:
        agent_id = scope.agent_id
        conversation_id = stable_id(agent_id, scope.user_id, binding.connector_id, chat.provider_id)
        original = self._channel_original(msg, chat)
        event_id = original.provider_message_id or msg.id
        created_at = original.created_at or datetime.fromtimestamp(msg.timestamp, timezone.utc).isoformat()
        metadata: dict = {"transport": msg.source}
        if original.provider_message_id:
            metadata["provider_message_id"] = original.provider_message_id
        if original.sequence is not None:
            metadata["provider_sequence"] = original.sequence
        metadata["provider_message_type"] = original.provider_message_type
        if original.provider_message_type == "unsupported":
            metadata["provider_source_type"] = _provider_type(original.provider_source_type)
        if original.provider_limited:
            metadata["provider_limited"] = True
        if original.provider_timestamp_limited:
            metadata["provider_timestamp_limited"] = True
        if original.media_limited or (
            original.provider_message_type in {"image", "audio", "file"}
            and not original.attachment_refs
        ):
            metadata["media_limited"] = True
        return MessageDraft(
            message_id=stable_id(conversation_id, "user", event_id), conversation_id=conversation_id,
            session_id=stable_id(conversation_id, "session"), role="user", content=original.content,
            content_type=original.content_type, created_at=created_at,
            source=msg.source, sender_id=original.sender_id or str(msg.get("actor_id") or "anonymous:" + msg.source),
            derivations=self.profile.derivations, metadata=metadata,
            idempotency_key=stable_id(conversation_id, "user", event_id),
            attachment_refs=original.attachment_refs,
        )

    def _append(self, scope: Scope, binding: ConnectorBinding, chat: ConversationBinding,
                draft: MessageDraft) -> StoredMessage:
        owner = Scope(scope.agent_id, scope.user_id)
        self.store.register_connector(owner, binding.connector_id, binding.source, binding.account_ref, binding.source)
        self.store.register_conversation(owner, draft.conversation_id, binding.connector_id,
                                         chat.provider_id, chat.kind, chat.display_name)
        return self.store.append(scope, draft)

    async def _write(self, scope: Scope, binding: ConnectorBinding,
                     chat: ConversationBinding, draft: MessageDraft) -> StoredMessage:
        scope = Scope(scope.agent_id, scope.user_id, binding.connector_id, draft.conversation_id)
        if self.memory_service is not None:
            return (await self.memory_service.add(scope, draft)).message
        return await self.append(scope, draft)

    async def append(self, scope: Scope, draft: MessageDraft) -> StoredMessage:
        """Trusted configured scope only; retain accepted-write cancellation ownership."""
        if type(scope) is not Scope or scope not in self.configured_scopes():
            raise RawMessageError("conversation not configured")
        binding, chat = next((binding, chat) for binding in self.profile.connectors
            for chat in binding.conversations
            if scope.agent_id == binding.agent_id and scope.connector_id == binding.connector_id
            and scope.conversation_id == stable_id(binding.agent_id, self.profile.owner_id,
                                                   binding.connector_id, chat.provider_id))
        if (type(draft) is not MessageDraft or draft.conversation_id != scope.conversation_id
                or draft.source != binding.source):
            raise RawMessageError("message scope conflict")
        draft = replace(draft, derivations=self.profile.derivations)
        if self._closed or len(self._inflight) >= 128:
            raise RawMessageError("capture unavailable")
        task = asyncio.create_task(self._serialized_write(scope, binding, chat, draft))
        self._track_write(task)
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        result = task.result()
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _serialized_write(self, scope: Scope, binding: ConnectorBinding,
                                chat: ConversationBinding, draft: MessageDraft) -> StoredMessage:
        async with self._lock:
            try:
                return await asyncio.to_thread(self._append, scope, binding, chat, draft)
            except (OSError, sqlite3.Error):
                raise RawPersistenceError("raw persistence failed") from None

    async def replay(self, scope: Scope, conversation_id: str, **kwargs) -> list[StoredMessage]:
        return await asyncio.to_thread(self.store.replay, scope, conversation_id, **kwargs)

    async def claim_input_dispatch(self, msg: Message) -> bool:
        if self._closed or not self.verify(msg):
            raise RawMessageError("durable input required")
        agent_id = msg.get("_raw_agent_id")
        message_id = msg.get("origin_message_id")
        if not isinstance(agent_id, str) or not isinstance(message_id, str):
            raise RawMessageError("durable input identity required")
        return await asyncio.to_thread(
            self.store.claim_input_dispatch, Scope(agent_id, self.profile.owner_id), message_id,
        )

    async def review_handoff_allowed(self, msg: Message, agent_id: str) -> bool:
        """Allow an in-process reviewer handoff only while its saved input is active."""
        if self._closed or not self.verify(msg) or msg.get("_raw_agent_id") != agent_id:
            return False
        message_id = msg.get("origin_message_id")
        connector_id = msg.get("connector_id")
        conversation_id = msg.get("conversation_id")
        if not all(isinstance(value, str) and value for value in
                   (message_id, connector_id, conversation_id)):
            return False
        scope = Scope(agent_id, self.profile.owner_id, connector_id, conversation_id)
        row = await asyncio.to_thread(self.store.get, scope, message_id)
        return bool(row and row.status == "active" and row.role == "user" and row.source == msg.source)

    def mark_input_dispatched(self, msg: Message) -> None:
        if not self.verify(msg):
            raise RawMessageError("durable input required")
        self.store.mark_input_dispatched(
            Scope(msg.get("_raw_agent_id"), self.profile.owner_id),
            msg.get("origin_message_id"),
        )

    async def business_input_states(self, scope: Scope) -> list[dict[str, str]]:
        """Diagnose durable Feishu text handoffs without claiming or replaying them."""
        if self._closed:
            raise RawMessageError("capture service closed")
        return await asyncio.to_thread(self.store.business_input_states, scope)

    async def prepare_reply(self, agent_id: str, source: str, target: str,
                            content: str, event_id: str, *, original: Message | None = None,
                            claim_outbound: bool = True) -> PreparedReply:
        """Capture final text before display/send; duplicates must not be sent again."""
        if self._closed:
            raise RawMessageError("capture service closed")
        content = str(content)
        msg = Message.user_input(content, source, chat_name=target)
        binding, chat = self._binding(msg, agent_id)
        scope = Scope(agent_id, self.profile.owner_id)
        cid = stable_id(agent_id, scope.user_id, binding.connector_id, chat.provider_id)
        message_id = stable_id(cid, "assistant", event_id)
        parent = None
        if original is not None:
            if (not self.verify(original) or original.role != "user"
                    or original.get("_raw_agent_id") != agent_id
                    or original.get("conversation_id") != cid):
                raise RawMessageError("outbound turn conflict")
            parent = (original.get("origin_message_id"), original.get("_raw_revision"))
        async with self._lock:
            draft = MessageDraft(
                message_id=message_id, conversation_id=cid, session_id=stable_id(cid, "session"),
                role="assistant", content=content, content_type="text",
                created_at=datetime.now(timezone.utc).isoformat(),
                source=source, sender_id=agent_id, idempotency_key=message_id,
                reply_to_message_id=parent[0] if parent else None,
                metadata={"delivery_status": "prepared", "turn_id": event_id},
                derivations=self.profile.derivations,
            )
            _, fresh = await self._write_reply(scope, binding, chat, draft, claim_outbound=claim_outbound, parent=parent)
        ticket = PreparedReply(agent_id, source, target, cid, message_id, fresh, "",
                               binding.connector_id, chat.provider_id,
                               parent[0] if parent else "", parent[1] if parent else 0)
        return replace(ticket, seal=self._reply_seal(ticket))

    def _reply_seal(self, ticket: PreparedReply) -> str:
        values = (ticket.agent_id, ticket.source, ticket.target, ticket.conversation_id,
                  ticket.message_id, ticket.fresh, ticket.connector_id, ticket.provider_id,
                  ticket.parent_id, ticket.parent_revision)
        payload = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def _verify_reply(self, ticket: PreparedReply) -> bool:
        return type(ticket) is PreparedReply and hmac.compare_digest(ticket.seal, self._reply_seal(ticket))

    async def _write_reply(self, scope: Scope, binding: ConnectorBinding,
                           chat: ConversationBinding, draft: MessageDraft,
                           *, claim_outbound: bool = False,
                           parent: tuple[str, int] | None = None) -> tuple[StoredMessage, bool]:
        """Called under the service lock; settle the transaction even on cancellation."""
        if self._closed:
            raise RawMessageError("capture unavailable")
        task = asyncio.create_task(asyncio.to_thread(
            self._append_reply, scope, binding, chat, draft, claim_outbound, parent,
        ))
        self._track_write(task)
        cancelled = False
        try:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            result = task.result()
            if cancelled:
                raise asyncio.CancelledError
            return result
        except (OSError, sqlite3.Error):
            raise RawMessageError("raw persistence failed") from None

    def _append_reply(self, scope: Scope, binding: ConnectorBinding,
                      chat: ConversationBinding, draft: MessageDraft,
                      claim_outbound: bool, parent: tuple[str, int] | None = None) -> tuple[StoredMessage, bool]:
        if parent is not None:
            return self.store.append_for_parent(scope, draft, *parent,
                outbound=draft.role == "assistant", claim_outbound=claim_outbound)
        if draft.role == "assistant" and draft.metadata.get("delivery_status") == "prepared":
            return self._append_outbound_once(scope, binding, chat, draft, claim_outbound=claim_outbound)
        return self._append(scope, binding, chat, draft), False

    def _append_outbound_once(self, scope: Scope, binding: ConnectorBinding,
                              chat: ConversationBinding, draft: MessageDraft, *,
                              claim_outbound: bool = True) -> tuple[StoredMessage, bool]:
        self.store.register_connector(scope, binding.connector_id, binding.source,
                                      binding.account_ref, binding.source)
        self.store.register_conversation(scope, draft.conversation_id, binding.connector_id,
                                         chat.provider_id, chat.kind, chat.display_name)
        return self.store.append_outbound_once(scope, draft, claim_outbound=claim_outbound)

    async def record_delivery(self, ticket: PreparedReply, status: str) -> None:
        if status not in {"confirmed", "failed", "unknown"} or not self._verify_reply(ticket):
            raise RawMessageError("invalid delivery receipt")
        msg = Message.user_input("", ticket.source, chat_name=ticket.target)
        binding, chat = self._binding(msg, ticket.agent_id)
        scope = Scope(ticket.agent_id, self.profile.owner_id)
        receipt_id = stable_id(ticket.message_id, "delivery", status)
        async with self._lock:
            existing = await asyncio.to_thread(self.store.get, scope, receipt_id, include_inactive=True)
            draft = MessageDraft(
                message_id=receipt_id, conversation_id=ticket.conversation_id,
                session_id=stable_id(ticket.conversation_id, "session"), role="tool", content=status,
                content_type="tool", source=ticket.source, sender_id="delivery-adapter",
                created_at=existing.created_at if existing else datetime.now(timezone.utc).isoformat(),
                reply_to_message_id=ticket.message_id, idempotency_key=receipt_id,
                metadata={"origin_message_id": ticket.message_id, "delivery_status": status},
            )
            await self._write_reply(scope, binding, chat, draft)

    async def diagnostics(self, scope: Scope) -> dict:
        """Counts only; no private text, identifiers, or fake worker success."""
        def counts():
            states: dict[str, int] = {}
            oldest = 0.0
            for row in self.store.export_messages(scope):
                for task in self.store.list_derivations(scope, row["message_id"]):
                    status = task["status"]
                    states[status] = states.get(status, 0) + 1
                    if status == "pending":
                        age = (datetime.now(timezone.utc) - datetime.fromisoformat(task["created_at"])).total_seconds()
                        oldest = max(oldest, age)
            return {"derivations": states, "oldest_pending_seconds": oldest,
                    "worker_enabled": self.worker is not None and not self._closed,
                    "state": "derivation_only_retrieval_disabled" if self.worker is not None
                    else "pending_worker"}
        return await asyncio.to_thread(counts)

    async def close(self) -> None:
        self._closed = True
        inflight = tuple(self._inflight)
        worker_error: BaseException | None = None
        try:
            if self.worker is not None:
                await self.worker.close()
        except BaseException as exc:
            worker_error = exc
        pending: set[asyncio.Task] = set()
        try:
            if inflight:
                _, pending = await asyncio.wait(inflight, timeout=15)
        except BaseException as exc:
            pending = {task for task in inflight if not task.done()}
            worker_error = worker_error or exc
        if pending and worker_error is None:
            worker_error = TimeoutError("raw service still writing")
        if (worker_error is not None and not isinstance(worker_error, asyncio.CancelledError)
                and self.settlement_status != "unsettled"):
            self._close_failure = self._close_failure or "settlement_failed"
        if worker_error is not None:
            raise worker_error
        # Raise a fresh finite error. Retaining a previously raised exception
        # would retain its traceback and accepted message/task locals on retries.
        if self._close_failure == "write_failed":
            raise RawMessageError("raw service write failed")
        if self._close_failure == "settlement_failed":
            raise RuntimeError("raw service settlement failure")

    @property
    def settlement_status(self) -> str:
        if any(not task.done() for task in self._inflight):
            return "unsettled"
        worker_state = (getattr(self.worker, "settlement_status", "unsettled")
                        if self.worker is not None else "settled")
        if worker_state == "unsettled":
            return worker_state
        return "settled_failed" if self._close_failure is not None else worker_state


async def accept_daily_input(bus, msg: Message) -> Message:
    """Keep legacy protocol test doubles compatible without bypassing enabled capture."""
    accept = getattr(bus, "accept_input", None)
    if callable(accept):
        return await accept(msg)
    if getattr(bus, "raw_messages", None) is not None:
        raise RawMessageError("durable input interface unavailable")
    bus.submit_input(msg)
    return msg
