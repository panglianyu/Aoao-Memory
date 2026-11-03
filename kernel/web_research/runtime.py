"""Trusted runtime binding and terminal delivery for public research commands."""
from __future__ import annotations
import time
from dataclasses import replace
from pathlib import Path

from kernel.tool_result import ToolResult
from kernel.web_research.budget import ResearchBudget
from kernel.web_research.config import ResearchConfig
from kernel.web_research.context import (
    ResearchContext,
    bind_context,
    current_research_call,
)
from kernel.web_research.model_gateway import ResearchModelGateway
from kernel.web_research.receipt import error_receipt
from kernel.web_research.researcher import settled_io
from kernel.web_research.store import SnapshotStore
from kernel.web_research.validation import (
    validate_domains,
    validate_query,
    validate_research_id,
    validate_snapshot_id,
    validate_source_id,
)
from kernel.web_research.policy import (
    ResearchAdmission,
    admission_from_result,
    admission_input_digest,
    current_research_admission,
    latch_research_admission,
)
from kernel.web_research.artifacts import (
    bind_receipt_to_call,
    load_receipt,
    validate_receipt,
)
from kernel.web_research.display import save_display, build_display
from protocols.web_research import canonical_url


def terminal_text(brain) -> str | None:
    run = getattr(brain, "_current_run", None)
    return getattr(run, "_research_terminal_text", None)


def record_failure(brain, text: str) -> None:
    run = getattr(brain, "_current_run", None)
    if run is not None:
        run._research_terminal_text = text
        run._research_display = None
        run.research_id = ""
        run.research_status = "failed"


def is_research_call(name: str, args: dict) -> bool:
    return name == "use_web_research" or (name == "instruct_app" and args.get("app_name") == "web_research")


_COMMAND_PARAMETERS = {
    "research": {"question", "mode", "research_id"},
    "web_search": {"queries", "domains", "recency_days"},
    "web_open": {"url", "source_id", "snapshot_id", "start_line", "line_count"},
    "web_find": {"url", "source_id", "snapshot_id", "pattern"},
    "web_pdf_page": {"url", "source_id", "snapshot_id", "page"},
}


def _validate_research_request(kwargs: dict) -> None:
    if kwargs.get("mode", "auto") not in {"auto", "quick", "deep"}:
        raise ValueError("invalid_parameters")
    if "research_id" in kwargs:
        validate_research_id(kwargs["research_id"])


def _validate_search_request(kwargs: dict) -> None:
    queries = kwargs.get("queries")
    if type(queries) is not list or not 1 <= len(queries) <= 4:
        raise ValueError("invalid_parameters")
    for query in queries:
        validate_query(query)
    recency = kwargs.get("recency_days")
    if recency is not None and (type(recency) is not int or not 1 <= recency <= 3650):
        raise ValueError("invalid_parameters")
    validate_domains(kwargs.get("domains"))


def _validate_source_request(kwargs: dict) -> None:
    if not any(kwargs.get(name) for name in ("url", "source_id", "snapshot_id")):
        raise ValueError("invalid_parameters")
    if "url" in kwargs:
        canonical_url(kwargs["url"])
    if kwargs.get("source_id"):
        validate_source_id(kwargs["source_id"])
    if kwargs.get("snapshot_id"):
        validate_snapshot_id(kwargs["snapshot_id"])


def _validate_find_request(kwargs: dict) -> None:
    _validate_source_request(kwargs)
    if (not isinstance(kwargs.get("pattern"), str)
            or not 1 <= len(kwargs["pattern"]) <= 200):
        raise ValueError("invalid_parameters")


def _validate_pdf_request(kwargs: dict) -> None:
    _validate_source_request(kwargs)
    if type(kwargs.get("page")) is not int or kwargs["page"] < 1:
        raise ValueError("invalid_parameters")


def _validate_open_request(kwargs: dict) -> None:
    _validate_source_request(kwargs)
    if (type(kwargs.get("start_line", 1)) is not int
            or kwargs.get("start_line", 1) < 1
            or type(kwargs.get("line_count", 80)) is not int
            or not 1 <= kwargs.get("line_count", 80) <= 200):
        raise ValueError("invalid_parameters")


_COMMAND_VALIDATORS = {
    "research": _validate_research_request,
    "web_search": _validate_search_request,
    "web_open": _validate_open_request,
    "web_find": _validate_find_request,
    "web_pdf_page": _validate_pdf_request,
}


def validate_dispatch_request(command: str, kwargs: dict) -> None:
    if type(kwargs) is not dict or command not in _COMMAND_PARAMETERS:
        raise ValueError("invalid_parameters")
    if set(kwargs) - _COMMAND_PARAMETERS[command]:
        raise ValueError("invalid_parameters")
    _COMMAND_VALIDATORS[command](kwargs)


def _validate_configured_request(command: str, kwargs: dict,
                                 config: ResearchConfig) -> None:
    if (command == "web_search"
            and len(kwargs["queries"]) > config.max_queries_per_round):
        raise ValueError("invalid_parameters")
    if (command == "web_pdf_page"
            and kwargs["page"] > config.max_pdf_pages):
        raise ValueError("invalid_parameters")


def context_for(brain, config: ResearchConfig, *, network: bool = True) -> ResearchContext:
    run = getattr(brain, "_current_run", None)
    principal = brain._principal_for_gate()
    if (run is None or principal != run.principal or run.source in {"idle", "boot", "system"}
            or not principal or not principal.is_owner or principal.channel != run.source
            or not principal.actor_id or not principal.authenticated_by):
        raise ValueError("permission_denied")
    context = getattr(run, "_research_context", None)
    if context is None:
        timeout = float(getattr(brain, "_TURN_TIMEOUT", 180) or 180)
        remaining = max(0, timeout - max(0, time.time() - run.started_at) - 5)
        budget = ResearchBudget(config, deadline=time.monotonic() + remaining, run=run)
        context = ResearchContext(run.run_id, run.session_key, principal, budget)
    else:
        if context.budget.config != config:
            budget = ResearchBudget(config, deadline=context.budget.deadline, run=run)
            context = replace(context, budget=budget)
    trusted_input = run.user_input
    admission = current_research_admission(run, scope=context.scope, input_text=trusted_input)
    context = replace(context, admission=admission)
    run._research_context = context
    context.validate(network=network)
    return context


async def admit_context(brain, config: ResearchConfig, context: ResearchContext,
                        command: str) -> ResearchContext:
    """Resolve semantic admission once using only the trusted current user input."""
    if not config.enabled:
        raise ValueError("network_forbidden")
    run = brain._current_run
    trusted_input = run.user_input
    current = current_research_admission(run, scope=context.scope, input_text=trusted_input)
    if current.state == "pending":
        if not trusted_input.strip():
            current = latch_research_admission(
                run,
                ResearchAdmission.blocked_for(
                    context.scope, admission_input_digest(trusted_input), "unclear",
                ),
                input_text=trusted_input, scope=context.scope,
            )
            context = replace(context, admission=current)
            run._research_context = context
            context.validate()
        gateway = ResearchModelGateway()
        stage = "admit_plan" if command == "research" else "admit"
        try:
            raw = await gateway.complete(stage, {"user_input": trusted_input}, context.budget)
            decision = admission_from_result(
                raw, scope=context.scope, input_text=trusted_input,
                planned=stage == "admit_plan",
            )
            current = latch_research_admission(
                run, decision, input_text=trusted_input, scope=context.scope,
            )
        except (ValueError, TypeError, KeyError) as exc:
            decision = ResearchAdmission.blocked_for(
                context.scope, admission_input_digest(trusted_input), "model_error",
            )
            latch_research_admission(
                run, decision, input_text=trusted_input, scope=context.scope,
            )
            if str(exc) in {"budget_exhausted", "cancelled", "network_forbidden"}:
                raise
            raise ValueError("model_error") from exc
        finally:
            await gateway.close()
    context = replace(context, admission=current)
    run._research_context = context
    context.validate()
    return context


async def dispatch(brain, command: str, trigger, kwargs: dict):
    if getattr(brain, "_current_run", None) is not None:
        brain._current_run._research_display = None
    try:
        validate_dispatch_request(command, kwargs)
        config = await settled_io(ResearchConfig.load, Path("config/web_research.yaml"))
        _validate_configured_request(command, kwargs, config)
        network = command in {"research", "web_search"} or not kwargs.get("snapshot_id")
        context = context_for(brain, config, network=False)
        if network:
            if not config.enabled:
                raise ValueError("network_forbidden")
            if command in {"research", "web_search"} and not config.provider_ready():
                raise ValueError("provider_unconfigured")
            context = await admit_context(brain, config, context, command)
        dispatch_kwargs = dict(kwargs)
        if command == "research":
            dispatch_kwargs["question"] = brain._current_run.user_input
        with bind_context(context, network=network):
            result = await brain.bus.send_instruction(
                "web_research", command, trigger_msg=trigger, **dispatch_kwargs,
            )
        if not isinstance(result, ToolResult) or result.app != "web_research" or not isinstance(result.data, dict):
            raise ValueError("invalid_receipt")
        # The plugin supplies program-rendered data. Re-render rather than trust a text field.
        data = result.data
        store = SnapshotStore(Path("data/web_research"), retention_days=config.retention_days,
                              storage_limit_bytes=config.storage_limit_bytes)
        receipt = (error_receipt(data.get("error_code")) if data.get("command") == "error"
                   else await settled_io(validate_receipt, data, context, store))
        if receipt["command"] != "error":
            identity = current_research_call()
            await settled_io(bind_receipt_to_call, store, context, receipt, identity)
        brain._current_run._research_terminal_text = receipt["text"]
        if receipt["command"] != "error":
            brain._current_run._research_display = await settled_io(save_display, store, context, receipt)
        display = getattr(brain._current_run, "_research_display", None)
        brain._current_run.research_id = display.get("research_id", "") if isinstance(display, dict) else ""
        brain._current_run.research_status = receipt["status"]
        return ToolResult("web_research", result.ok, result.kind, receipt, receipt["text"])
    except (ValueError, TypeError, KeyError) as exc:
        data = error_receipt(str(exc))
        record_failure(brain, data["text"])
        return ToolResult("web_research", False, "error", data, data["text"])


async def replay(brain, entry: dict) -> ToolResult:
    """A replay reads the same run's immutable snapshots and never searches again."""
    if getattr(brain, "_current_run", None) is not None:
        brain._current_run._research_display = None
    try:
        config = await settled_io(ResearchConfig.load, Path("config/web_research.yaml"))
        context = context_for(brain, config, network=False)
        store = SnapshotStore(Path("data/web_research"), retention_days=config.retention_days,
                              storage_limit_bytes=config.storage_limit_bytes)
        identity = current_research_call()
        result = await settled_io(load_receipt, store, context, entry, identity)
        brain._current_run._research_terminal_text = result["text"]
        brain._current_run._research_display = build_display(result, research_id="research_" + context.scope[:32])
        brain._current_run.research_id = brain._current_run._research_display["research_id"]
        brain._current_run.research_status = result["status"]
        return ToolResult("web_research", entry["receipt_ok"], "text", result, result["text"])
    except (ValueError, KeyError, TypeError, OSError) as exc:
        code = str(exc) if str(exc) in {"permission_denied", "artifact_expired"} else "invalid_artifact"
        result = error_receipt(code)
        record_failure(brain, result["text"])
        return ToolResult("web_research", False, "error", result, result["text"])
