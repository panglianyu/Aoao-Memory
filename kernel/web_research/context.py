"""A trusted task identity is bound by the dispatcher, never by model arguments."""
from __future__ import annotations
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json

from kernel.principal import Principal
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.policy import ResearchAdmission

_CALL_FIELDS = {"call_id", "function_name", "arguments_digest", "revision"}


@dataclass(frozen=True)
class ResearchCallIdentity:
    call_id: str
    function_name: str
    arguments_digest: str
    revision: int

    def to_dict(self) -> dict:
        return {
            "call_id": self.call_id,
            "function_name": self.function_name,
            "arguments_digest": self.arguments_digest,
            "revision": self.revision,
        }


def _bounded_identity_text(value, *, limit: int) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= limit
            or any(ord(char) < 32 for char in value)):
        raise ValueError("invalid_artifact")
    return value


def research_call_identity(call_id: str, function_name: str,
                           canonical_arguments: str, revision: int) -> ResearchCallIdentity:
    call_id = _bounded_identity_text(call_id, limit=256)
    function_name = _bounded_identity_text(function_name, limit=128)
    if not isinstance(canonical_arguments, str) or len(canonical_arguments) > 65536:
        raise ValueError("invalid_artifact")
    try:
        parsed = json.loads(canonical_arguments)
        expected = json.dumps(parsed, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid_artifact") from exc
    if type(parsed) is not dict or canonical_arguments != expected:
        raise ValueError("invalid_artifact")
    if type(revision) is not int or not 0 <= revision <= 1_000_000_000:
        raise ValueError("invalid_artifact")
    digest = hashlib.sha256(canonical_arguments.encode("utf-8")).hexdigest()
    return ResearchCallIdentity(call_id, function_name, digest, revision)


def validate_research_call_identity(value: dict) -> dict:
    if type(value) is not dict or set(value) != _CALL_FIELDS:
        raise ValueError("invalid_artifact")
    arguments_digest = value.get("arguments_digest")
    revision = value.get("revision")
    if (not isinstance(arguments_digest, str)
            or len(arguments_digest) != 64
            or any(char not in "0123456789abcdef" for char in arguments_digest)
            or type(revision) is not int
            or not 0 <= revision <= 1_000_000_000):
        raise ValueError("invalid_artifact")
    identity = ResearchCallIdentity(
        _bounded_identity_text(value.get("call_id"), limit=256),
        _bounded_identity_text(value.get("function_name"), limit=128),
        arguments_digest,
        revision,
    )
    return identity.to_dict()


@dataclass(frozen=True)
class ResearchContext:
    run_id: str
    session_key: str
    principal: Principal
    budget: ResearchBudget
    admission: ResearchAdmission | None = None

    @property
    def scope(self) -> str:
        identity = [self.run_id, self.session_key, self.principal.actor_id,
                    self.principal.channel, self.principal.authenticated_by]
        return hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()

    def validate(self, *, network: bool = True) -> None:
        if (not self.run_id or not self.session_key or not isinstance(self.principal, Principal)
                or not self.principal.is_owner or not self.principal.actor_id or not self.principal.authenticated_by):
            raise ValueError("permission_denied")
        if network:
            if not self.budget.config.enabled:
                raise ValueError("network_forbidden")
            if (not isinstance(self.admission, ResearchAdmission)
                    or self.admission.scope != self.scope
                    or self.admission.state != "allowed"):
                reason = self.admission.reason if (
                    isinstance(self.admission, ResearchAdmission)
                    and self.admission.scope == self.scope
                ) else "pending"
                raise ValueError("network_undetermined" if reason in {
                    "pending", "unclear", "model_error",
                } else "network_forbidden")
            self.budget.remaining()


_CURRENT: ContextVar[ResearchContext | None] = ContextVar("research_context", default=None)
_CURRENT_CALL: ContextVar[ResearchCallIdentity | None] = ContextVar(
    "research_call_identity", default=None,
)


def current_context(*, network: bool = True) -> ResearchContext:
    context = _CURRENT.get()
    if context is None:
        raise ValueError("permission_denied")
    context.validate(network=network)
    return context


def current_research_call() -> ResearchCallIdentity:
    identity = _CURRENT_CALL.get()
    if identity is None:
        raise ValueError("invalid_artifact")
    validate_research_call_identity(identity.to_dict())
    return identity


@contextmanager
def bind_context(context: ResearchContext, *, network: bool = True):
    context.validate(network=network)
    token = _CURRENT.set(context)
    try:
        yield context
    finally:
        _CURRENT.reset(token)


@contextmanager
def bind_research_call(identity: ResearchCallIdentity):
    validate_research_call_identity(identity.to_dict())
    token = _CURRENT_CALL.set(identity)
    try:
        yield identity
    finally:
        _CURRENT_CALL.reset(token)
