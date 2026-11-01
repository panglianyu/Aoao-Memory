"""Bounded per-user FIFO lanes whose dispatchers outlive HTTP waiters."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
import math
import time
from typing import Any


class AdmissionFull(RuntimeError):
    """All in-flight reservations, including settling owners, are occupied."""


class AdmissionClosed(RuntimeError):
    """Admission has shut down or failed closed."""


@dataclass(eq=False)
class _Ticket:
    deadline: float
    future: asyncio.Future[object]
    phase: str = "reading"
    operation: Callable[..., Awaitable[object]] | None = None
    body_task: asyncio.Task | None = None
    lane_id: int | None = None
    affinity_key: str | None = None


@dataclass
class _UserPin:
    lane_id: int
    pending: int = 0
    cache_token: int | None = None


class BoundedAdmission:
    """One or two execution lanes, with at most 32 total reservations."""

    def __init__(self, wait_idle: Callable[..., Awaitable[None]], *,
                 capacity: int = 32, lane_count: int = 1):
        if type(capacity) is not int or not 1 <= capacity <= 32:
            raise ValueError("invalid admission capacity")
        if type(lane_count) is not int or lane_count not in {1, 2}:
            raise ValueError("invalid admission lane count")
        self._capacity = capacity
        self.lane_count = lane_count
        self._wait_idle = wait_idle
        self._tickets: set[_Ticket] = set()
        self._queues: list[deque[_Ticket]] = [deque() for _ in range(lane_count)]
        self._dispatchers: list[asyncio.Task | None] = [None] * lane_count
        self._current: list[_Ticket | None] = [None] * lane_count
        self._lane_loads = [0] * lane_count
        self._pins: dict[str, _UserPin] = {}
        self._caches: list[tuple[str, int] | None] = [None] * lane_count
        self._last_cache_token = [0] * lane_count
        self._next_lane = 0
        self._close_task: asyncio.Task | None = None
        self._closing = False
        self._failure: BaseException | None = None
        self._empty = asyncio.Event()
        self._empty.set()

    @property
    def in_flight(self) -> int:
        return len(self._tickets)

    @property
    def queued(self) -> int:
        return sum(len(queue) for queue in self._queues)

    @property
    def pin_count(self) -> int:
        return len(self._pins)

    @property
    def lane_in_flight(self) -> tuple[int, ...]:
        return tuple(self._lane_loads)

    @property
    def closing(self) -> bool:
        return self._closing

    @staticmethod
    def _consume(future: asyncio.Future) -> None:
        if not future.cancelled():
            future.exception()

    def reserve(self, deadline: float) -> _Ticket:
        # No await separates the capacity check and reservation.
        if self._closing:
            raise AdmissionClosed("admission unavailable")
        if len(self._tickets) >= self._capacity:
            raise AdmissionFull("admission capacity reached")
        if (type(deadline) not in {int, float} or not math.isfinite(deadline)
                or deadline <= time.monotonic()):
            raise TimeoutError("request deadline expired")
        ticket = _Ticket(deadline, asyncio.get_running_loop().create_future())
        ticket.future.add_done_callback(self._consume)
        self._tickets.add(ticket)
        self._empty.clear()
        return ticket

    async def read_body(self, ticket: _Ticket, body: Coroutine[Any, Any, dict],
                        timeout: float) -> dict:
        if self._closing or ticket not in self._tickets or ticket.phase != "reading":
            body.close()
            raise AdmissionClosed("admission unavailable")
        if timeout <= 0:
            body.close()
            raise TimeoutError("request deadline expired")
        task = asyncio.create_task(body)
        ticket.body_task = task

        def body_settled(done: asyncio.Task) -> None:
            if ticket.body_task is done:
                ticket.body_task = None
            if ticket.phase == "canceling_body":
                self._finish(ticket)

        task.add_done_callback(body_settled)
        task.add_done_callback(self._consume)
        try:
            return await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError("request body deadline expired") from None
        except asyncio.CancelledError:
            if self._closing:
                raise AdmissionClosed("admission closing") from None
            raise
        finally:
            # Repeated waiter cancellation can interrupt wait_for's child cleanup.
            # Keep the reservation and child reference until the child is truly done.
            if task.done() and ticket.body_task is task:
                ticket.body_task = None

    def _finish(self, ticket: _Ticket) -> None:
        if ticket not in self._tickets:
            return
        ticket.phase = "done"
        ticket.operation = None
        ticket.body_task = None
        self._tickets.discard(ticket)
        if ticket.lane_id is not None:
            self._lane_loads[ticket.lane_id] -= 1
            if ticket.affinity_key is not None:
                pin = self._pins[ticket.affinity_key]
                pin.pending -= 1
                self._release_pin(ticket.affinity_key, pin)
        if not self._tickets:
            self._empty.set()

    def _release_pin(self, key: str, pin: _UserPin) -> None:
        if pin.pending == 0 and pin.cache_token is None:
            del self._pins[key]

    def _fail_closed(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._closing = True
        for ticket in tuple(self._tickets):
            self.abandon(ticket, closing=True)

    def _cache_identity(self, lane_id: int, user_id: str, cache_token: int) -> _UserPin:
        if (self.lane_count != 2 or type(lane_id) is not int
                or not 0 <= lane_id < self.lane_count or type(user_id) is not str
                or type(cache_token) is not int or not 0 < cache_token < 2**63):
            self._fail_closed(RuntimeError("invalid cache ownership notification"))
            raise AdmissionClosed("admission cache ownership unavailable")
        pin = self._pins.get(user_id)
        if pin is None or pin.lane_id != lane_id:
            self._fail_closed(RuntimeError("cache notification does not match user pin"))
            raise AdmissionClosed("admission cache ownership unavailable")
        return pin

    def cache_enter(self, lane_id: int, user_id: str, cache_token: int) -> None:
        """Pin a lane's resource owner before construction can suspend or fail."""
        pin = self._cache_identity(lane_id, user_id, cache_token)
        current = self._current[lane_id]
        if (self._caches[lane_id] is not None or pin.cache_token is not None
                or cache_token <= self._last_cache_token[lane_id]
                or current is None or current.phase != "running"
                or current.affinity_key != user_id):
            self._fail_closed(RuntimeError("cache entered with conflicting ownership"))
            raise AdmissionClosed("admission cache ownership unavailable")
        self._caches[lane_id] = (user_id, cache_token)
        self._last_cache_token[lane_id] = cache_token
        pin.cache_token = cache_token

    def cache_retired(self, lane_id: int, user_id: str, cache_token: int) -> None:
        """Release only the exact cache whose real service has finished retiring."""
        pin = self._cache_identity(lane_id, user_id, cache_token)
        if (self._caches[lane_id] != (user_id, cache_token)
                or pin.cache_token != cache_token):
            self._fail_closed(RuntimeError("cache retirement identity mismatch"))
            raise AdmissionClosed("admission cache ownership unavailable")
        self._caches[lane_id] = None
        pin.cache_token = None
        self._release_pin(user_id, pin)

    def _assign(self, ticket: _Ticket, affinity_key: str | None) -> int:
        if self.lane_count == 1:
            lane = 0
        else:
            if (type(affinity_key) is not str or not affinity_key.strip()
                    or len(affinity_key) > 256 or "\0" in affinity_key):
                raise ValueError("explicit user affinity required")
            pin = self._pins.get(affinity_key)
            if pin is None:
                minimum = min(self._lane_loads)
                lane = next((self._next_lane + offset) % self.lane_count
                            for offset in range(self.lane_count)
                            if self._lane_loads[(self._next_lane + offset)
                                                % self.lane_count] == minimum)
                self._next_lane = (lane + 1) % self.lane_count
                pin = self._pins[affinity_key] = _UserPin(lane)
            lane = pin.lane_id
            pin.pending += 1
            ticket.affinity_key = affinity_key
        ticket.lane_id = lane
        self._lane_loads[lane] += 1
        return lane

    def _publish(self, ticket: _Ticket, *, result: object = None,
                 error: Exception | None = None) -> None:
        if ticket.future.done():
            return
        if error is not None:
            ticket.future.set_exception(error)
        else:
            ticket.future.set_result(result)

    def abandon(self, ticket: _Ticket, *, closing: bool = False) -> None:
        """Remove only unstarted work; a running ticket remains dispatcher-owned."""
        if ticket not in self._tickets:
            return
        if ticket.phase in {"running", "settling", "done", "canceling_body"}:
            return
        if closing:
            self._publish(ticket, error=AdmissionClosed("admission closing"))
        else:
            ticket.future.cancel()
        if ticket.phase == "queued":
            try:
                lane = ticket.lane_id
                if lane is None:
                    raise RuntimeError("queued ticket has no lane")
                self._queues[lane].remove(ticket)
            except ValueError:
                # The dispatcher can be waiting for an older runtime owner.
                pass
        if ticket.body_task is not None and not ticket.body_task.done():
            ticket.phase = "canceling_body"
            ticket.body_task.cancel()
        else:
            self._finish(ticket)

    async def submit(self, ticket: _Ticket,
                     operation: Callable[..., Awaitable[object]], *,
                     affinity_key: str | None = None) -> object:
        # Recheck closing after body reading and validation, before enqueueing.
        if self._closing or ticket not in self._tickets or ticket.phase != "reading":
            self.abandon(ticket, closing=True)
            raise AdmissionClosed("admission unavailable")
        if ticket.deadline <= time.monotonic():
            self.abandon(ticket)
            raise TimeoutError("request deadline expired")
        try:
            lane = self._assign(ticket, affinity_key)
        except ValueError:
            self.abandon(ticket)
            raise
        ticket.operation = operation
        ticket.phase = "queued"
        self._queues[lane].append(ticket)
        if self._dispatchers[lane] is None:
            dispatcher = asyncio.create_task(self._dispatch(lane))
            self._dispatchers[lane] = dispatcher
            dispatcher.add_done_callback(self._consume)
        try:
            return await asyncio.wait_for(asyncio.shield(ticket.future),
                                          timeout=max(0, ticket.deadline - time.monotonic()))
        except asyncio.TimeoutError:
            self.abandon(ticket)
            raise TimeoutError("request deadline expired") from None
        except (asyncio.CancelledError, TimeoutError):
            self.abandon(ticket)
            raise

    async def _idle(self, lane_id: int) -> None:
        if self.lane_count == 1:
            await self._wait_idle()
        else:
            await self._wait_idle(lane_id)

    async def _dispatch(self, lane_id: int) -> None:
        current: _Ticket | None = None
        try:
            queue = self._queues[lane_id]
            while queue:
                current = self._current[lane_id] = queue.popleft()
                await self._idle(lane_id)
                if current.phase != "queued":
                    current = None
                    continue
                if self._closing:
                    self.abandon(current, closing=True)
                    current = None
                    continue
                if current.deadline <= time.monotonic():
                    self._publish(current, error=TimeoutError("request deadline expired"))
                    self._finish(current)
                    current = None
                    continue
                operation = current.operation
                if operation is None:
                    raise RuntimeError("missing admitted operation")
                current.phase = "running"
                try:
                    result = await (operation() if self.lane_count == 1 else operation(lane_id))
                except Exception as error:
                    self._publish(current, error=error)
                else:
                    if current.deadline <= time.monotonic():
                        self._publish(current, error=TimeoutError("request deadline expired"))
                    else:
                        self._publish(current, result=result)
                current.phase = "settling"
                # The HTTP result/timeout does not release the real execution slot.
                await self._idle(lane_id)
                self._finish(current)
                current = None
        except BaseException as error:
            if current is not None:
                self._publish(current, error=RuntimeError("admission dispatcher unavailable"))
            self._fail_closed(error)
            raise
        finally:
            self._current[lane_id] = None
            self._dispatchers[lane_id] = None

    async def wait_empty(self) -> None:
        await self._empty.wait()

    async def _close(self, dispatchers: list[asyncio.Task],
                     body_tasks: list[asyncio.Task]) -> None:
        if body_tasks:
            await asyncio.gather(*body_tasks, return_exceptions=True)
        if dispatchers:
            results = await asyncio.gather(*dispatchers, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and self._failure is None:
                    self._failure = result
        if self._tickets or self._failure is not None:
            # Even an interrupted dispatcher cannot authorize owner replacement.
            results = await asyncio.gather(*(self._idle(lane) for lane in range(self.lane_count)),
                                           return_exceptions=True)
            for ticket in tuple(self._tickets):
                lane = ticket.lane_id
                if lane is not None and isinstance(results[lane], BaseException):
                    if self._failure is None:
                        self._failure = results[lane]
                else:
                    self._finish(ticket)
        if self._failure is not None:
            raise RuntimeError("admission dispatcher unavailable") from None

    async def close(self) -> None:
        if self._close_task is None:
            self._closing = True
            body_tasks = [ticket.body_task for ticket in self._tickets
                          if ticket.body_task is not None]
            for ticket in tuple(self._tickets):
                self.abandon(ticket, closing=True)
            dispatchers = [task for task in self._dispatchers if task is not None]
            self._close_task = asyncio.create_task(self._close(dispatchers, body_tasks))
            self._close_task.add_done_callback(self._consume)
        cancelled = False
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                cancelled = True
        self._close_task.result()
        if cancelled:
            raise asyncio.CancelledError
