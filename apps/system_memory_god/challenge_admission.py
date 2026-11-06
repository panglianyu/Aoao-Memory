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
    settled: asyncio.Future[None]
    owner: object
    phase: str = "reading"
    operation: Callable[..., Awaitable[object]] | None = None
    body_task: asyncio.Task | None = None
    lane_id: int | None = None
    affinity_key: str | None = None
    before_release: Callable[[], Awaitable[None]] | None = None
    release_task: asyncio.Task | None = None


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
        self._release_tasks: dict[asyncio.Task, _Ticket] = {}
        self._closing = False
        self._failure: BaseException | None = None
        self._lane_failures: list[BaseException | None] = [None] * lane_count
        self._quarantined_users: set[str] = set()
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

    def reserve(self, deadline: float, *,
                before_release: Callable[[], Awaitable[None]] | None = None) -> _Ticket:
        # No await separates the capacity check and reservation.
        if self._closing:
            raise AdmissionClosed("admission unavailable")
        if len(self._tickets) >= self._capacity:
            raise AdmissionFull("admission capacity reached")
        if (type(deadline) not in {int, float} or not math.isfinite(deadline)
                or deadline <= time.monotonic()):
            raise TimeoutError("request deadline expired")
        if before_release is not None and not callable(before_release):
            raise ValueError("invalid admission release hook")
        loop = asyncio.get_running_loop()
        ticket = _Ticket(deadline, loop.create_future(), loop.create_future(), self,
                         before_release=before_release)
        ticket.future.add_done_callback(self._consume)
        ticket.settled.add_done_callback(self._consume)
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
        if ticket.before_release is None:
            self._finish_now(ticket)
            return
        if ticket.release_task is not None:
            return
        ticket.phase = "releasing"
        task = asyncio.create_task(self._release(ticket))
        ticket.release_task = task
        self._release_tasks[task] = ticket

        def finished(done: asyncio.Task) -> None:
            self._release_tasks.pop(done, None)
            self._consume(done)

        task.add_done_callback(finished)

    async def _release(self, ticket: _Ticket) -> None:
        try:
            hook = ticket.before_release
            if hook is None:
                raise RuntimeError("missing admission release hook")
            await hook()
        except BaseException as error:
            self._fail_closed(error)
            raise
        self._finish_now(ticket)

    async def _wait_releases(self, lane_id: int | None = None) -> None:
        while True:
            tasks = [task for task, ticket in self._release_tasks.items()
                     if lane_id is None or ticket.lane_id in {None, lane_id}]
            if not tasks:
                break
            await asyncio.gather(*(asyncio.shield(task) for task in tasks),
                                 return_exceptions=True)
        if self._failure is not None:
            raise AdmissionClosed("admission settlement unavailable")

    def _finish_now(self, ticket: _Ticket) -> None:
        if ticket not in self._tickets:
            return
        ticket.phase = "done"
        ticket.operation = None
        ticket.body_task = None
        ticket.before_release = None
        ticket.release_task = None
        self._tickets.discard(ticket)
        if ticket.lane_id is not None:
            self._lane_loads[ticket.lane_id] -= 1
            if ticket.affinity_key is not None:
                pin = self._pins[ticket.affinity_key]
                pin.pending -= 1
                self._release_pin(ticket.affinity_key, pin)
        if not self._tickets:
            self._empty.set()
        if not ticket.settled.done():
            ticket.settled.set_result(None)

    def _release_pin(self, key: str, pin: _UserPin) -> None:
        if pin.pending == 0 and pin.cache_token is None:
            del self._pins[key]

    def _fail_closed(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._closing = True
        for ticket in tuple(self._tickets):
            self._publish(ticket, error=AdmissionClosed("admission unavailable"))
            if not ticket.settled.done():
                ticket.settled.set_exception(AdmissionClosed("admission settlement unavailable"))
            self.abandon(ticket, closing=True)

    def fail_closed(self, error: BaseException) -> None:
        """Retain active tickets when a durable owner cannot prove completion."""
        if not isinstance(error, BaseException):
            raise TypeError("admission failure must be an exception")
        self._fail_closed(error)

    def quarantine_lane(self, lane_id: int, error: BaseException) -> None:
        """Close one lane without releasing its unresolved ticket or cache pin."""
        if type(lane_id) is not int or not 0 <= lane_id < self.lane_count or not isinstance(error, BaseException):
            raise ValueError("invalid lane quarantine")
        if self._lane_failures[lane_id] is None:
            self._lane_failures[lane_id] = error
        self._quarantined_users.update(key for key, pin in self._pins.items() if pin.lane_id == lane_id)
        for ticket in tuple(self._queues[lane_id]):
            self._publish(ticket, error=AdmissionClosed("user lane unavailable"))
            self.abandon(ticket, closing=True)
        current = self._current[lane_id]
        if current is not None and current.phase == "queued":
            # The dispatcher may have dequeued this ticket while awaiting an
            # older cache owner. Its own operation has never been admitted.
            self._publish(current, error=AdmissionClosed("user lane unavailable"))
            self.abandon(current, closing=True)

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
            if affinity_key in self._quarantined_users:
                raise AdmissionClosed("user lane unavailable")
            pin = self._pins.get(affinity_key)
            if pin is None:
                available = tuple(index for index in range(self.lane_count) if self._lane_failures[index] is None)
                if not available:
                    raise AdmissionClosed("all user lanes unavailable")
                minimum = min(self._lane_loads[index] for index in available)
                lane = next((self._next_lane + offset) % self.lane_count
                            for offset in range(self.lane_count)
                            if self._lane_loads[(self._next_lane + offset)
                                                % self.lane_count] == minimum
                            and (self._next_lane + offset) % self.lane_count in available)
                self._next_lane = (lane + 1) % self.lane_count
                pin = self._pins[affinity_key] = _UserPin(lane)
            lane = pin.lane_id
            if self._lane_failures[lane] is not None:
                raise AdmissionClosed("user lane unavailable")
            pin.pending += 1
            ticket.affinity_key = affinity_key
        if self._lane_failures[lane] is not None:
            raise AdmissionClosed("user lane unavailable")
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
        if ticket.phase in {"running", "settling", "releasing", "done", "canceling_body"}:
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

    def enqueue(self, ticket: _Ticket,
                operation: Callable[..., Awaitable[object]], *,
                affinity_key: str | None = None,
                before_release: Callable[[], Awaitable[None]] | None = None) -> None:
        """Hand work to its dispatcher without creating or depending on a waiter."""
        if ticket in self._tickets and ticket.phase == "reading" and before_release is not None:
            if (not callable(before_release)
                    or ticket.before_release is not None and ticket.before_release is not before_release):
                self.abandon(ticket)
                raise ValueError("invalid admission release hook")
            ticket.before_release = before_release
        # Recheck closing after body reading and validation, before enqueueing.
        if self._closing or ticket not in self._tickets or ticket.phase != "reading":
            self.abandon(ticket, closing=True)
            raise AdmissionClosed("admission unavailable")
        if ticket.deadline <= time.monotonic():
            self.abandon(ticket)
            raise TimeoutError("request deadline expired")
        try:
            lane = self._assign(ticket, affinity_key)
        except (ValueError, AdmissionClosed):
            self.abandon(ticket)
            raise
        ticket.operation = operation
        ticket.phase = "queued"
        self._queues[lane].append(ticket)
        if self._dispatchers[lane] is None:
            dispatcher = asyncio.create_task(self._dispatch(lane))
            self._dispatchers[lane] = dispatcher
            dispatcher.add_done_callback(self._consume)

    async def wait_settled(self, ticket: _Ticket) -> None:
        """Wait for real ticket release; cancellation never abandons its owner."""
        if ticket.owner is not self:
            raise ValueError("ticket belongs to another admission")
        if self._failure is not None:
            raise AdmissionClosed("admission settlement unavailable")
        await asyncio.shield(ticket.settled)

    async def submit(self, ticket: _Ticket,
                     operation: Callable[..., Awaitable[object]], *,
                     affinity_key: str | None = None) -> object:
        self.enqueue(ticket, operation, affinity_key=affinity_key)
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
                # Removed queue entries can still own a durable release hook.
                await self._wait_releases(lane_id)
                if not queue:
                    break
                current = self._current[lane_id] = queue.popleft()
                await self._idle(lane_id)
                if current.phase != "queued":
                    await self.wait_settled(current)
                    current = None
                    continue
                if self._closing:
                    self.abandon(current, closing=True)
                    await self.wait_settled(current)
                    current = None
                    continue
                if current.deadline <= time.monotonic():
                    self._publish(current, error=TimeoutError("request deadline expired"))
                    self._finish(current)
                    await self.wait_settled(current)
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
                if self._failure is not None:
                    raise AdmissionClosed("admission settlement unavailable")
                current.phase = "settling"
                # The HTTP result/timeout does not release the real execution slot.
                await self._idle(lane_id)
                self._finish(current)
                await self.wait_settled(current)
                current = None
        except BaseException as error:
            if current is not None:
                self._publish(current, error=RuntimeError("admission dispatcher unavailable"))
            if self._failure is None and self._lane_failures[lane_id] is not None:
                # A classified local failure still owns its current ticket.
                # It is retired only by close after the runtime proves idle.
                self.quarantine_lane(lane_id, error)
                return
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
        try:
            await self._wait_releases()
        except AdmissionClosed:
            pass  # Preserve the failure while still checking every real owner below.
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
        try:
            await self._wait_releases()
        except AdmissionClosed:
            pass  # A failed hook keeps its ticket and the close remains failed.
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
