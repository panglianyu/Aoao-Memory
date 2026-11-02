"""Two bounded user runtimes sharing only a journal and an inference owner."""

from __future__ import annotations

import asyncio
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from .challenge_runtime import ChallengeRuntime
from .challenge_store import ChallengeStore

if TYPE_CHECKING:
    from .challenge_admission import BoundedAdmission
    from .storage.shared_embedding_owner import SharedEmbeddingOwner


class ChallengeLanes:
    """Internal HTTP facade; user affinity belongs to the admission scheduler."""

    lane_count = 2

    def __init__(self, root: Path, *, client, model: str,
                 embedding_owner: SharedEmbeddingOwner, add_timeout: float = 1700.0,
                 service_factory=None, observer: Callable | None = None):
        if embedding_owner is None or embedding_owner.max_generations != 2:
            raise ValueError("two runtimes require one explicit two-generation owner")
        self._embedding_owner = embedding_owner
        self._admission: BoundedAdmission | None = None
        self._observer = observer
        self._closed = self._closing = self._failed = False
        self._close_task: asyncio.Task | None = None
        store = ChallengeStore(root, model=model)
        self._lanes = tuple(ChallengeRuntime(root, client=client, model=model,
            embedding_owner=embedding_owner, add_timeout=add_timeout,
            service_factory=service_factory, store=store,
            cache_observer=partial(self._cache_event, lane),
            observer=partial(self._phase, lane)) for lane in range(self.lane_count))

    def bind_admission(self, admission: BoundedAdmission) -> None:
        if self._admission is not None or any(lane.busy or lane.cache_user is not None
                                               for lane in self._lanes):
            raise RuntimeError("admission must bind once before user ownership")
        self._admission = admission

    def _cache_event(self, lane: int, event: str, user: str, token: int) -> None:
        if self._admission is not None:
            if event == "enter":
                self._admission.cache_enter(lane, user, token)
            elif event == "retired":
                self._admission.cache_retired(lane, user, token)
            else:
                raise ValueError("unknown challenge cache event")

    def _phase(self, lane: int, user: str, stage: str, start: float, end: float) -> None:
        if self._observer is not None:
            self._observer(lane, user, stage, start, end)

    def for_lane(self, lane: int) -> ChallengeRuntime:
        if type(lane) is not int or not 0 <= lane < self.lane_count:
            raise ValueError("invalid challenge lane")
        return self._lanes[lane]

    def request_deadline(self, deadline: float | None = None) -> float:
        return self._lanes[0].request_deadline(deadline)

    async def wait_idle(self, lane: int | None = None) -> None:
        if lane is not None:
            await self.for_lane(lane).wait_idle()
        else:
            await asyncio.gather(*(item.wait_idle() for item in self._lanes))

    def health(self) -> dict:
        state = "ready"
        try:
            stats = self._embedding_owner.stats
            unavailable = (self._failed or stats.get("failed") is not False
                or stats.get("stats_available", True) is not True
                or any(item.health()["state"] == "unavailable" for item in self._lanes))
        except Exception:
            unavailable = True
        if unavailable:
            state = "unavailable"
        elif self._closed:
            state = "stopped"
        elif self._closing:
            state = "closing"
        return {"state": state, "version": self._lanes[0].health()["version"]}

    async def close(self) -> None:
        if self._closed:
            return
        self._closing = True
        if self._close_task is None or self._close_task.done():
            self._close_task = asyncio.create_task(self._close_owned())
        await ChallengeRuntime._settle(self._close_task)

    async def _close_owned(self) -> None:
        results = await asyncio.gather(*(lane.close() for lane in self._lanes),
                                       return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            self._failed = True
            raise RuntimeError("challenge lane cleanup incomplete")
        self._closed = True
