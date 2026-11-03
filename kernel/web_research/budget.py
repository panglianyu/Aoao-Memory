"""Reserve before I/O; failed and retried requests also consume task limits."""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Any

from kernel.web_research.config import ResearchConfig
from kernel.model_usage import reserve_model_call

_EMPTY_USAGE = {"search_requests": 0, "open_pages": 0, "model_calls": 0,
                "input_tokens_reserved": 0, "output_tokens_reserved": 0,
                "download_bytes": 0, "cache_hits": 0, "credits": 0,
                "input_tokens_actual": 0, "output_tokens_actual": 0,
                "model_usage_unknown": False}


@dataclass
class ModelReservation:
    input_tokens: int
    output_tokens: int
    settled: bool = False


def validate_saved_usage(raw: dict) -> dict:
    if type(raw) is not dict:
        raise ValueError("invalid_research_usage")
    if not raw:
        return {}  # Ordinary tasks created before research have no usage ledger.
    if not set(_EMPTY_USAGE) <= set(raw) <= set(_EMPTY_USAGE) | {"download_bytes_upper_bound"}:
        raise ValueError("invalid_research_usage")
    if any(not isinstance(key, str) or
           (type(value) is not bool if key == "model_usage_unknown" else type(value) is not int or value < 0)
           for key, value in raw.items()):
        raise ValueError("invalid_research_usage")
    if (raw["input_tokens_reserved"] < raw["input_tokens_actual"]
            or raw["output_tokens_reserved"] < raw["output_tokens_actual"]
            or raw.get("download_bytes_upper_bound", 0) < raw["download_bytes"]):
        raise ValueError("invalid_research_usage")
    return dict(raw)


class ResearchBudget:
    def __init__(self, config: ResearchConfig, *, deadline: float | None = None,
                 run: Any = None):
        self.config = config
        self.deadline = min(time.monotonic() + config.research_timeout_seconds,
                            deadline if deadline is not None else float("inf"))
        self.run = run
        self._pending_bytes = 0
        self._charged_bytes = 0
        self.usage = dict(_EMPTY_USAGE)
        if run is not None:
            self.usage.update(validate_saved_usage(getattr(run, "research_usage", {})))
            run.research_usage = self.usage
            self._charged_bytes = self.usage.get("download_bytes_upper_bound", self.usage["download_bytes"])

    def reserve_download(self, maximum: int) -> int:
        self.remaining()
        available = self.config.max_download_bytes - self._charged_bytes - self._pending_bytes
        if available <= 0:
            raise ValueError("budget_exhausted")
        limit = min(maximum, available)
        self._pending_bytes += limit
        return limit

    def finish_download(self, reserved: int, actual: int | None) -> None:
        self._pending_bytes -= reserved
        # Failed transfers have unknown byte counts, so charge their entire reservation.
        self._charged_bytes += reserved if actual is None else actual
        self.usage["download_bytes_upper_bound"] = self._charged_bytes
        if actual is not None:
            self.downloaded(actual)

    def remaining(self) -> float:
        if self.run is not None and (self.run.cancel_requested or self.run.preempted):
            raise ValueError("cancelled")
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("budget_exhausted")
        return remaining

    def reserve(self, kind: str, *, input_tokens: int = 0, output_tokens: int = 0) -> ModelReservation | None:
        self.remaining()
        validate_saved_usage(self.usage)
        key, limit = {"search": ("search_requests", self.config.max_search_requests),
                      "open": ("open_pages", self.config.max_open_pages),
                      "model": ("model_calls", self.config.max_model_calls)}[kind]
        if self.usage[key] >= limit:
            raise ValueError("budget_exhausted")
        if kind == "model":
            if min(input_tokens, output_tokens) < 0:
                raise ValueError("invalid_reservation")
            if (self.usage["input_tokens_reserved"] + input_tokens > self.config.max_total_input_tokens
                    or self.usage["output_tokens_reserved"] + output_tokens > self.config.max_total_output_tokens):
                raise ValueError("budget_exhausted")
            if self.run is not None:
                reserve_model_call(self.run)
            self.usage["input_tokens_reserved"] += input_tokens
            self.usage["output_tokens_reserved"] += output_tokens
        self.usage[key] += 1
        return ModelReservation(input_tokens, output_tokens) if kind == "model" else None

    def settle_model(self, ticket: ModelReservation, input_tokens: int | None,
                     output_tokens: int | None) -> None:
        if ticket.settled:
            raise ValueError("reservation_already_settled")
        ticket.settled = True
        for kind, estimate, actual in (("input", ticket.input_tokens, input_tokens),
                                       ("output", ticket.output_tokens, output_tokens)):
            if type(actual) is int and actual >= 0:
                self.usage[kind + "_tokens_reserved"] += actual - estimate
                self.usage[kind + "_tokens_actual"] += actual
            else:
                self.usage["model_usage_unknown"] = True

    def downloaded(self, count: int) -> None:
        self.usage["download_bytes"] += count
        if self.usage["download_bytes"] > self.config.max_download_bytes:
            raise ValueError("budget_exhausted")

    def snapshot(self) -> dict:
        return dict(self.usage)
