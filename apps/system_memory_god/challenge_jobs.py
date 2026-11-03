"""Internal durable Add ownership; no external async HTTP protocol is implied."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import time
from typing import Any

from kernel.raw_message_models import ConflictError
from .challenge_admission import AdmissionClosed, AdmissionFull, BoundedAdmission
from .challenge_job_store import (
    ChallengeJobStore, JobCapacityError, JobReceipt, JobStatus, MAX_PAYLOAD_BYTES, TERMINAL,
    validate_runtime_lease,
)
from .challenge_runtime import _external_id, _messages


class ChallengeJobs:
    """A receipt owner survives callers; the existing dispatchers execute jobs."""

    def __init__(self, runtime, store: ChallengeJobStore, *, runtime_lease,
                 job_timeout: float = 1700.0):
        if (type(job_timeout) not in {int, float} or not math.isfinite(job_timeout)
                or not 0 < job_timeout <= 1700 or getattr(runtime, "lane_count", None) != 2):
            raise ValueError("two lanes and a finite independent job budget required")
        root = runtime.for_lane(0).root
        validate_runtime_lease(runtime_lease, root)
        if (store.root != root / "jobs" or store.runtime_lease is not runtime_lease
                or any(runtime.for_lane(lane).root != root
                       or runtime.for_lane(lane).model != store.model for lane in range(2))):
            raise ValueError("job store and runtime ownership differ")
        self.runtime, self.store, self.runtime_lease = runtime, store, runtime_lease
        self._job_timeout = float(job_timeout)
        self.admission = BoundedAdmission(runtime.wait_idle, capacity=30, lane_count=2)
        runtime.bind_admission(self.admission)
        self._controls = asyncio.Semaphore(2)
        self._owners: set[asyncio.Task] = set()
        self._events: dict[str, asyncio.Event] = {}
        self._accept_tails: dict[str, asyncio.Future] = {}
        self._closing = self._closed = self._failed = False
        self._close_task: asyncio.Task | None = None

    @staticmethod
    def _consume(done):
        if not done.cancelled():
            done.exception()

    def _available(self):
        validate_runtime_lease(self.runtime_lease, self.store.root.parent)
        if self._closing or self._closed or self._failed or self.admission.closing:
            raise AdmissionClosed("job core unavailable")

    def _fail_closed(self, error):
        self._failed = True
        self.admission.fail_closed(error)
        for event in self._events.values():
            event.set()

    @staticmethod
    async def _owned_result(task):
        """Return a committed result even when this receipt owner is cancelled."""
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        return task.result(), cancelled

    async def _read(self, operation, *args):
        # Two short controls remain independent of the thirty work owners.
        if self._controls.locked():
            raise AdmissionFull("job status capacity reached")
        async with self._controls:
            return await operation(*args)

    async def status(self, job_id: str) -> JobStatus:
        return await self._read(self.store.status, job_id)

    async def await_terminal(self, job_id: str, *, timeout: float = 1710.0) -> JobStatus:
        if (type(timeout) not in {int, float} or not math.isfinite(timeout)
                or not 0 < timeout <= 1800):
            raise ValueError("finite job observation timeout required")
        deadline = time.monotonic() + timeout
        while True:
            state = await self.status(job_id)
            if state.state in TERMINAL:
                return state
            event = self._events.get(job_id)
            if event is None or self._failed:
                # A background owner cannot be manufactured by a status read.
                state = await self.status(job_id)
                if state.state in TERMINAL:
                    return state
                raise RuntimeError("job ownership remains unresolved")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("job observation deadline exceeded")
            # This waits only on a local Event. Cancelling it never cancels work.
            await asyncio.wait_for(event.wait(), timeout=remaining)

    async def submit_add(self, *, user_id: str, session_id: str, request_id: str,
                         messages: list[dict]) -> JobReceipt:
        self._available()
        user_id, session_id, request_id = map(_external_id, (user_id, session_id, request_id))
        values = {"user_id": user_id, "session_id": session_id, "request_id": request_id,
                  "messages": _messages(messages)}
        payload = json.dumps(values, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False)
        encoded = payload.encode("utf-8")
        if len(encoded) > MAX_PAYLOAD_BYTES:
            raise ValueError("job input exceeds acceptance budget")
        digest = hashlib.sha256(encoded).hexdigest()
        began = time.monotonic()
        received = datetime.now(timezone.utc)
        existing = await self._read(self.store.find, user_id, request_id)
        if existing is not None:
            if existing.digest != digest:
                raise ConflictError("job request content conflict")
            if existing.state not in TERMINAL:
                self._events.setdefault(existing.job_id, asyncio.Event())
            return existing.receipt()
        self._available()
        deadline = began + self._job_timeout
        accepted = asyncio.get_running_loop().create_future()
        context: dict[str, Any] = {"record": None, "created": False, "started": False,
                   "cancelled": False, "accept_error": None}

        async def before_release():
            await asyncio.shield(accepted)
            if context["accept_error"] is not None:
                raise RuntimeError("job acceptance outcome unavailable")
            record = context["record"]
            if record is not None and context["created"] and not context["started"]:
                code = ("closed" if self._closing or context["cancelled"]
                        else "expired" if time.monotonic() >= deadline else "closed")
                await self._finish(record.job_id, state="FAILED", error_code=code)

        ticket = self.admission.reserve(deadline, before_release=before_release)
        predecessor = self._accept_tails.get(user_id)
        turn = asyncio.get_running_loop().create_future()
        self._accept_tails[user_id] = turn

        def handed_off():
            if not turn.done():
                turn.set_result(None)
            if self._accept_tails.get(user_id) is turn:
                self._accept_tails.pop(user_id)

        receipt = asyncio.get_running_loop().create_future()
        receipt.add_done_callback(self._consume)
        owner = asyncio.create_task(self._accept_and_watch(
            ticket, receipt, values, payload, digest, received.isoformat(),
            (received + timedelta(seconds=self._job_timeout)).isoformat(), deadline,
            accepted, context, predecessor, handed_off))
        self._owners.add(owner)

        def finished(done):
            self._owners.discard(done)
            self._consume(done)

        owner.add_done_callback(finished)
        return await asyncio.shield(receipt)

    async def _finish(self, job_id, *, state, error_code=None, result=None):
        try:
            status = await self.store.finish(job_id, state=state, error_code=error_code, result=result)
        except BaseException as error:
            self._fail_closed(error)
            raise
        event = self._events.pop(job_id, None)
        if event is not None:
            event.set()
        return status

    @staticmethod
    def _error_code(error) -> str:
        if isinstance(error, TimeoutError):
            return "expired"
        if isinstance(error, (asyncio.CancelledError, AdmissionClosed)):
            return "closed"
        return "execution_failed"

    async def _accept_and_watch(self, ticket, receipt, values, payload, digest,
                                received_utc, expires_utc, deadline, accepted, context,
                                predecessor, handed_off):
        record = None
        created = False
        try:
            if predecessor is not None:
                _, cancelled = await self._owned_result(predecessor)
                context["cancelled"] = cancelled
            if (context["cancelled"] or self._closing or self._failed or self.admission.closing):
                accepted.set_result(None)
                self.admission.abandon(ticket, closing=True)
                await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
                receipt.set_exception(AdmissionClosed("job acceptance interrupted"))
                return
            insertion = asyncio.create_task(self.store.accept(values, payload, digest,
                received_utc=received_utc, expires_utc=expires_utc))
            (record, created), cancelled = await self._owned_result(insertion)
            context.update(record=record, created=created, cancelled=cancelled)
            accepted.set_result(None)
            if not created:
                self.admission.abandon(ticket)
                await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
                receipt.set_result(record.receipt())
                return
            self._events.setdefault(record.job_id, asyncio.Event())

            if cancelled or self._closing:
                self.admission.abandon(ticket, closing=True)
                await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
                receipt.set_exception(AdmissionClosed("job acceptance interrupted"))
                return

            async def execute(lane_id):
                context["started"] = True
                try:
                    await self.store.running(record.job_id)
                except BaseException as error:
                    self._fail_closed(error)
                    raise
                selected = self.runtime.for_lane(lane_id)
                result, failure = None, None
                try:
                    result = await selected.add_job(**values, deadline=deadline,
                                                    received_utc=record.received_utc)
                    expected = {"success": True, **{key: values[key] for key in (
                        "user_id", "session_id", "request_id")}}
                    if type(result) is not dict or result != expected or result.get("success") is not True:
                        raise RuntimeError("invalid job execution result")
                except BaseException as error:
                    failure = error
                try:
                    await selected.wait_idle()
                except BaseException as error:
                    try:
                        await self.store.settling(record.job_id)
                    finally:
                        self._fail_closed(error)
                    raise
                if failure is None and time.monotonic() >= deadline:
                    failure = TimeoutError("job execution deadline exceeded")
                if failure is None:
                    await self._finish(record.job_id, state="SUCCEEDED", result=result)
                else:
                    await self._finish(record.job_id, state="FAILED",
                                       error_code=self._error_code(failure))
                return result

            try:
                self.admission.enqueue(ticket, execute, affinity_key=values["user_id"])
            except BaseException:
                self.admission.abandon(ticket)
                await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
                if not receipt.done():
                    final = await self.store.status(record.job_id)
                    receipt.set_result(JobReceipt(final.job_id, final.state, final.received_utc, final.expires_utc))
                return
            receipt.set_result(record.receipt())
            handed_off()
            await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
        except BaseException as error:
            if not accepted.done():
                if not isinstance(error, (ConflictError, JobCapacityError)):
                    context["accept_error"] = error
                accepted.set_result(None)
            if not created and isinstance(error, (ConflictError, JobCapacityError)):
                self.admission.abandon(ticket)
                await self._owned_result(asyncio.create_task(self.admission.wait_settled(ticket)))
            else:
                self._fail_closed(error)
            if not receipt.done():
                receipt.set_exception(error if isinstance(error, (ConflictError, AdmissionClosed, TimeoutError))
                                      else RuntimeError("job acceptance unavailable"))
        finally:
            # Only accept-to-enqueue/failure is serialized, never job execution.
            handed_off()

    async def search(self, *, user_id: str, query: str, top_k: int, options=()):
        self._available()
        user_id = _external_id(user_id)
        deadline = time.monotonic() + 100.0
        ticket = self.admission.reserve(deadline)

        async def execute(lane_id):
            return await self.runtime.for_lane(lane_id).search(
                user_id=user_id, query=query, top_k=top_k, options=options, deadline=deadline)

        return await self.admission.submit(ticket, execute, affinity_key=user_id)

    def health(self) -> dict:
        value = self.runtime.health()
        state = ("unavailable" if self._failed else "stopped" if self._closed
                 else "closing" if self._closing else value["state"])
        return {"state": state, "version": value["version"]}

    async def close(self):
        if self._closed:
            return
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close_owned())
            self._close_task.add_done_callback(self._consume)
        cancelled = False
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        self._close_task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _close_owned(self):
        error = None
        try:
            await self.admission.close()
        except BaseException as caught:
            error = caught
        if self._owners:
            await asyncio.gather(*tuple(self._owners), return_exceptions=True)
        try:
            await self.runtime.close()
        except BaseException as caught:
            error = error or caught
        if error is not None or self._failed:
            # Failed persistence/ownership keeps both the job lock and root lease.
            raise RuntimeError("job cleanup remains unresolved") from None
        await self.store.aclose()
        self._closed = True
