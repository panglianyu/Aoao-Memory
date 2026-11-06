"""Fault boundaries of the private gate without starting models or native owners."""

import asyncio
import sys
from types import SimpleNamespace

import pytest

from Tools.validate_memory_challenge_batch_job import (
    _RuntimeSetupOwner, _SampledResourceStop, _Samples, _observe_job,
)


@pytest.mark.asyncio
async def test_partial_setup_closes_runtime_before_releasing_store():
    events = []

    class Runtime:
        async def close(self):
            events.append("runtime")

    store = SimpleNamespace(close=lambda: events.append("store"))
    owner = _RuntimeSetupOwner(Runtime())
    owner.store = store
    await owner.close()
    assert events == ["runtime", "store"]
    assert owner.store is None


@pytest.mark.asyncio
async def test_partial_setup_retains_store_when_runtime_cannot_settle():
    class Runtime:
        async def close(self):
            raise RuntimeError("synthetic owner still active")

    store = SimpleNamespace(close=lambda: pytest.fail("released before owner settled"))
    owner = _RuntimeSetupOwner(Runtime())
    owner.store = store
    with pytest.raises(RuntimeError, match="still active"):
        await owner.close()
    assert owner.store is store


def test_process_check_uses_observed_identity_and_keeps_unknowns(monkeypatch):
    class Error(Exception):
        pass

    class Gone(Error):
        pass

    def process(pid):
        if pid == 1:
            raise Gone()
        if pid == 2:
            return SimpleNamespace(create_time=lambda: 99, is_running=lambda: True)
        if pid == 3:
            raise Error()
        assert pid == 4
        return SimpleNamespace(create_time=lambda: 4, is_running=lambda: True)

    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace(
        Process=process, NoSuchProcess=Gone, Error=Error))
    samples = _Samples()
    samples._observed = {1: 1, 2: 2, 3: 3, 4: 4}
    assert samples.remaining_processes() == {
        "observed_count": 4, "still_live_count": 1, "unknown_count": 1,
        "scope": "only observed runner descendants"}


@pytest.mark.parametrize("available,rss,stop", [
    (2 * 1024**3 - 1, 100, True),
    (2 * 1024**3, 6 * 1024**3, False),
    (2 * 1024**3, 6 * 1024**3 + 1, True),
])
def test_sampled_resource_boundary_stops_new_admission(monkeypatch, available, rss, stop):
    class Error(Exception):
        pass

    process = SimpleNamespace(children=lambda **kwargs: [],
                              memory_info=lambda: SimpleNamespace(rss=rss))
    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace(Process=lambda: process,
        Error=Error, virtual_memory=lambda: SimpleNamespace(available=available)))
    samples = _Samples()
    samples.capture()
    assert samples.stop_new_work.is_set() is stop
    if stop:
        with pytest.raises(_SampledResourceStop):
            samples.require_admission()
    else:
        samples.require_admission()


@pytest.mark.asyncio
async def test_resource_stop_cancels_observation_and_retains_actual_job_owner():
    release, entered = asyncio.Event(), asyncio.Event()

    async def work():
        entered.set()
        await release.wait()
        return "completed"

    owner = asyncio.create_task(work())

    class Jobs:
        async def await_terminal(self, job_id, *, timeout):
            return await asyncio.shield(owner)

    samples = _Samples()
    observation = asyncio.create_task(_observe_job(Jobs(), "opaque-id", 10, samples))
    await entered.wait()
    samples.stop_new_work.set()
    with pytest.raises(_SampledResourceStop):
        await observation
    assert not owner.done()
    release.set()
    assert await owner == "completed"
