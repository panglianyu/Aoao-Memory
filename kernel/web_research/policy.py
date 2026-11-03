"""Trusted, monotonic admission state for research network access."""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json


ADMISSION_SCHEMA_VERSION = 1
_STATES = {"pending", "allowed", "blocked"}
_INTENTS = {"required", "not_needed", "forbidden", "unclear"}
_RECORD_FIELDS = {"schema_version", "state", "reason", "scope", "input_digest"}
_HEX = set("0123456789abcdef")


def admission_input_digest(text: str) -> str:
    if not isinstance(text, str):
        raise ValueError("invalid_research_admission")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def research_admission_scope(run) -> str:
    principal = getattr(run, "principal", None)
    identity = [
        getattr(run, "run_id", ""), getattr(run, "session_key", ""),
        getattr(principal, "actor_id", ""), getattr(principal, "channel", ""),
        getattr(principal, "authenticated_by", ""),
    ]
    if not all(isinstance(value, str) and value for value in identity):
        raise ValueError("invalid_research_admission")
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()


def _bounded(value: object, limit: int) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= limit
            or any(ord(char) < 32 for char in value)):
        raise ValueError("invalid_research_admission")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX for char in value):
        raise ValueError("invalid_research_admission")
    return value


@dataclass(frozen=True)
class ResearchAdmission:
    state: str
    reason: str
    scope: str
    input_digest: str
    plan: dict | None = None

    @classmethod
    def pending_for(cls, scope: str, input_digest: str) -> "ResearchAdmission":
        return cls("pending", "pending", _digest(scope), _digest(input_digest))

    @classmethod
    def allowed_for(cls, scope: str, input_digest: str,
                    plan: dict | None = None) -> "ResearchAdmission":
        return cls("allowed", "required", _digest(scope), _digest(input_digest), plan)

    @classmethod
    def blocked_for(cls, scope: str, input_digest: str, reason: str) -> "ResearchAdmission":
        return cls("blocked", _bounded(reason, 64), _digest(scope), _digest(input_digest))

    def to_record(self) -> dict:
        return {
            "schema_version": ADMISSION_SCHEMA_VERSION,
            "state": self.state,
            "reason": self.reason,
            "scope": self.scope,
            "input_digest": self.input_digest,
        }


def validate_admission_record(raw: object) -> ResearchAdmission:
    if type(raw) is not dict or set(raw) != _RECORD_FIELDS:
        raise ValueError("invalid_research_admission")
    if raw.get("schema_version") != ADMISSION_SCHEMA_VERSION or raw.get("state") not in _STATES:
        raise ValueError("invalid_research_admission")
    reason = _bounded(raw.get("reason"), 64)
    if ((raw["state"] == "pending" and reason != "pending")
            or (raw["state"] == "allowed" and reason != "required")):
        raise ValueError("invalid_research_admission")
    return ResearchAdmission(raw["state"], reason, _digest(raw.get("scope")),
                             _digest(raw.get("input_digest")))


def validate_admission_result(raw: dict, *, planned: bool = False) -> tuple[str, dict | None]:
    expected = {"intent", "subquestions", "queries", "mode", "controversial"} if planned else {"intent"}
    if type(raw) is not dict or set(raw) != expected or raw.get("intent") not in _INTENTS:
        raise ValueError("model_response_invalid")
    if not planned:
        return raw["intent"], None
    subquestions, queries = raw.get("subquestions"), raw.get("queries")
    if (not isinstance(subquestions, list) or not 1 <= len(subquestions) <= 6
            or any(type(row) is not dict or set(row) != {"id", "text"}
                   or row.get("id") != f"q{index + 1}"
                   or not isinstance(row.get("text"), str) or not row["text"].strip()
                   for index, row in enumerate(subquestions))
            or not isinstance(queries, list) or not 1 <= len(queries) <= 4
            or any(not isinstance(query, str) or not query.strip() for query in queries)
            or raw.get("mode") not in {"quick", "deep"}
            or type(raw.get("controversial")) is not bool):
        raise ValueError("model_response_invalid")
    plan = {key: raw[key] for key in ("subquestions", "queries", "mode", "controversial")}
    return raw["intent"], plan


def admission_from_result(raw: dict, *, scope: str, input_text: str,
                          planned: bool = False) -> ResearchAdmission:
    intent, plan = validate_admission_result(raw, planned=planned)
    digest = admission_input_digest(input_text)
    if intent == "required":
        return ResearchAdmission.allowed_for(scope, digest, plan)
    return ResearchAdmission.blocked_for(scope, digest, intent)


def current_research_admission(run, *, scope: str, input_text: str) -> ResearchAdmission:
    digest = admission_input_digest(input_text)
    raw = getattr(run, "research_admission", None)
    if not raw:
        return ResearchAdmission.pending_for(scope, digest)
    try:
        admission = validate_admission_record(raw)
    except ValueError:
        return ResearchAdmission.blocked_for(scope, digest, "invalid_state")
    if admission.scope != scope or admission.input_digest != digest:
        return ResearchAdmission.blocked_for(scope, digest, "identity_changed")
    plan = getattr(run, "_research_preplan", None) if admission.state == "allowed" else None
    return replace(admission, plan=plan)


def latch_research_admission(run, decision: ResearchAdmission, *, input_text: str,
                             scope: str) -> ResearchAdmission:
    current = current_research_admission(run, scope=scope, input_text=input_text)
    if current.state in {"allowed", "blocked"}:
        return current
    if decision.state not in {"allowed", "blocked"} or decision.scope != scope:
        raise ValueError("invalid_research_admission")
    if decision.input_digest != admission_input_digest(input_text):
        raise ValueError("invalid_research_admission")
    run.research_admission = decision.to_record()
    run.research_policy = "allow" if decision.state == "allowed" else "deny"
    if decision.state == "allowed" and decision.plan is not None:
        run._research_preplan = decision.plan
    return decision


def serialize_research_admission(raw: dict) -> dict:
    admission = validate_admission_record(raw)
    if admission.state != "blocked":
        admission = ResearchAdmission.blocked_for(
            admission.scope, admission.input_digest, "restore_untrusted",
        )
    return admission.to_record()


def restored_research_admission(raw: object, *, scope: str, input_text: str,
                                legacy_policy: object = None) -> dict:
    digest = admission_input_digest(input_text)
    if raw:
        try:
            admission = validate_admission_record(raw)
            if admission.scope != scope or admission.input_digest != digest:
                raise ValueError("invalid_research_admission")
            if admission.state == "blocked":
                return admission.to_record()
        except ValueError:
            pass
    reason = "legacy_denied" if legacy_policy == "deny" else "restore_untrusted"
    return ResearchAdmission.blocked_for(scope, digest, reason).to_record()


def network_policy(question: str, saved: str = "allow") -> str:
    """Compatibility projection only; this function never grants network access."""
    del question
    return "deny" if saved == "deny" else "undetermined"
