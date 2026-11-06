"""Fake spawned inference checks for two user generations, never real models."""

import asyncio
import multiprocessing
import time

import pytest

from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingClient, SharedEmbeddingOwner
from system_memory_god.storage import shared_embedding_owner as shared


def _deadline(seconds=10):
    return time.monotonic_ns() + int(seconds * 1_000_000_000)


async def _until(condition):
    async def poll():
        while not condition():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(poll(), timeout=3)


def _embed(texts):
    if texts[0] == "slow":
        time.sleep(0.3)
    if texts[0] == "never":
        raise AssertionError("expired capacity waiter reached inference")
    return [[float(len(texts[0]))] * 1024]


def _hold_endpoint(client, ready, release):
    ready.set()
    try:
        if not release.wait(10):
            raise TimeoutError("endpoint holder not released")
    finally:
        client.close()


async def _finish(generation):
    await generation.prepare_close(deadline_ns=_deadline())
    for client in generation.clients.values():
        client.close()
    await generation.close(deadline_ns=_deadline())


@pytest.mark.asyncio
async def test_two_generations_share_one_model_and_retire_independently():
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    try:
        await owner.start(_deadline())
        first, second = await asyncio.gather(
            owner.new_generation(_deadline()), owner.new_generation(_deadline()))
        assert {first.number, second.number} == {1, 2}
        assert owner.stats["active_generation_count"] == 2
        assert owner.stats["current_generation"] is None
        with pytest.raises(RuntimeError):
            await owner.new_generation(_deadline())
        results = await asyncio.gather(
            asyncio.to_thread(first.clients["writer"], ["one"]),
            asyncio.to_thread(second.clients["writer"], ["second"]))
        assert results == [[[3.0] * 1024], [[6.0] * 1024]]
        await _finish(first)
        assert owner.stats["active_generations"] == [second.number]
        third = await owner.new_generation(_deadline())
        assert third.number == 3
        assert await asyncio.to_thread(second.clients["query"], ["still here"]) == [[10.0] * 1024]
        await _finish(second)
        await _finish(third)
        assert owner.stats["load_count"] == 1
        assert owner.stats["active_generation_count"] == 0
        assert owner.stats["ipc_peak"] <= 2
    finally:
        await owner.close()
    assert owner.stats["clean_close"]


@pytest.mark.asyncio
async def test_full_global_capacity_waits_and_unsent_expiry_is_local():
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    try:
        await owner.start(_deadline())
        first = await owner.new_generation(_deadline())
        second = await owner.new_generation(_deadline(0.09))
        calls = [asyncio.create_task(asyncio.to_thread(first.clients[role], ["slow"]))
                 for role in ("writer", "query")]
        await _until(lambda: owner.stats["ipc_inflight"] == 2)
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(second.clients["writer"], ["never"])
        assert not owner.stats["failed"]
        assert owner.stats["ipc_inflight"] == 2
        # This waiter never acquired a global slot and cannot poison its generation.
        await second.set_deadline(_deadline())
        waiting = asyncio.create_task(asyncio.to_thread(second.clients["query"], ["after wait"]))
        await asyncio.sleep(0.02)
        assert not waiting.done()
        assert await waiting == [[10.0] * 1024]
        await asyncio.gather(*calls)
        assert owner.stats["ipc_peak"] == 2
        await _finish(first)
        await _finish(second)
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_settle_wait_does_not_hold_control_pipe_or_other_generation():
    context = multiprocessing.get_context("spawn")
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    ready, release = context.Event(), context.Event()
    process = None
    try:
        await owner.start(_deadline())
        first = await owner.new_generation(_deadline())
        second = await owner.new_generation(_deadline())
        process = context.Process(target=_hold_endpoint,
            args=(first.clients["writer"], ready, release))
        process.start()
        first.clients["writer"].close_parent_copy()
        assert await asyncio.to_thread(ready.wait, 5)
        closing = asyncio.create_task(first.close(deadline_ns=_deadline()))
        await asyncio.sleep(0.03)
        assert not closing.done()
        await asyncio.wait_for(second.set_deadline(_deadline()), timeout=1)
        assert await asyncio.to_thread(second.clients["writer"], ["independent"]) == [[11.0] * 1024]
        release.set()
        await asyncio.to_thread(process.join, 5)
        assert process.exitcode == 0
        await closing
        await _finish(second)
    finally:
        release.set()
        if process is not None:
            if process.is_alive():
                process.terminate()
            process.join(5)
            process.close()
        await owner.close()


@pytest.mark.asyncio
async def test_expired_inference_stays_owned_without_blocking_other_deadline():
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    try:
        await owner.start(_deadline())
        first = await owner.new_generation(_deadline(0.07))
        second = await owner.new_generation(_deadline())
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(first.clients["writer"], ["slow"])
        assert owner.stats["ipc_inflight"] == 1
        old_deadline = first._state[shared._DEADLINE]
        with pytest.raises(shared.DeadlineHandoffNotCommitted):
            await first.set_deadline(_deadline(.03))
        assert first._state[shared._DEADLINE] == old_deadline and not owner.stats["failed"]
        await second.set_deadline(_deadline())
        assert await asyncio.to_thread(second.clients["query"], ["other"]) == [[5.0] * 1024]
        await _finish(first)
        await _finish(second)
        assert not owner.stats["failed"]
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_old_unsent_waiter_cannot_clear_replacement_slot_role():
    context = multiprocessing.get_context("spawn")
    state, global_state = context.RawArray("q", 15), context.RawArray("q", 16)
    guard = context.RLock()
    connection, peer = context.Pipe()
    absolute = _deadline()
    state[shared._GENERATION], state[shared._DEADLINE] = 1, absolute
    global_state[shared._INFLIGHT] = 2
    client = SharedEmbeddingClient(connection, state, guard, "a" * 64, 1,
                                   "writer", absolute, global_state=global_state)
    call = asyncio.create_task(asyncio.to_thread(client, ["old capacity waiter"]))
    try:
        await _until(lambda: bool(state[shared._WRITER_IO]))
        with guard:
            state[shared._GENERATION] = 2
            state[shared._WRITER_IO] = 1
        with pytest.raises(RuntimeError):
            await call
        assert state[shared._WRITER_IO] == 1
        assert global_state[shared._INFLIGHT] == 2
        assert not global_state[shared._FAILED]
    finally:
        client.close()
        peer.close()
        await asyncio.gather(call, return_exceptions=True)


def test_completion_cannot_observe_a_reused_generation_as_settled():
    context = multiprocessing.get_context("spawn")
    state, guard = context.RawArray("q", 15), context.RLock()
    connection, peer = context.Pipe()
    absolute = _deadline()
    state[shared._GENERATION] = 2
    client = SharedEmbeddingClient(connection, state, guard, "a" * 64, 1, "writer", absolute)
    try:
        with pytest.raises(ValueError):
            client._wait_completion(1, absolute)
    finally:
        client.close()
        peer.close()


@pytest.mark.asyncio
async def test_close_waits_for_the_owned_prepare_command(monkeypatch):
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    ready, release = asyncio.Event(), asyncio.Event()
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        command = owner._command

        async def delayed(action, *args, **kwargs):
            if action == "prepare":
                ready.set()
                await release.wait()
            return await command(action, *args, **kwargs)

        monkeypatch.setattr(owner, "_command", delayed)
        preparing = asyncio.create_task(generation.prepare_close(deadline_ns=_deadline()))
        await asyncio.wait_for(ready.wait(), timeout=3)
        closing = asyncio.create_task(generation.close(deadline_ns=_deadline()))
        await asyncio.sleep(0.02)
        assert not closing.done()
        assert not owner.stats["failed"]
        release.set()
        await preparing
        await closing
        assert not owner.stats["failed"]
    finally:
        release.set()
        await owner.close()


@pytest.mark.asyncio
async def test_expired_unsent_prepare_can_be_followed_by_bounded_cleanup():
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        with pytest.raises(TimeoutError):
            await generation.prepare_close(deadline_ns=time.monotonic_ns() - 1)
        assert not owner.stats["failed"]
        assert not generation._closed
        # None is explicitly a cleanup budget, not a replacement business D.
        await generation.prepare_close()
        for client in generation.clients.values():
            client.close()
        await generation.close()
        assert generation._closed
        assert not owner.stats["failed"]
    finally:
        await owner.close()
    assert owner.stats["clean_close"]


@pytest.mark.asyncio
async def test_expired_unsent_close_task_can_be_followed_by_cleanup():
    owner = SharedEmbeddingOwner(embedder=_embed)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        with pytest.raises(TimeoutError):
            await generation.close(deadline_ns=time.monotonic_ns() - 1)
        assert not owner.stats["failed"]
        await generation.close()
        assert generation._closed
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_sent_prepare_failure_stays_failed_until_confirmed_model_join(monkeypatch):
    owner = SharedEmbeddingOwner(embedder=_embed, max_generations=2)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        exchange = owner._exchange

        def lose_ack(action, *args):
            result = exchange(action, *args)
            if action == "prepare":
                raise TimeoutError("fake lost prepare receipt after actual send")
            return result

        monkeypatch.setattr(owner, "_exchange", lose_ack)
        with pytest.raises(TimeoutError):
            await generation.prepare_close(deadline_ns=_deadline())
        assert owner.stats["failed"]
        with pytest.raises((RuntimeError, TimeoutError)):
            await generation.prepare_close()
        assert not generation._closed
        # Only actual owned process exit permits endpoint-only local cleanup.
        await owner.close()
        assert owner.stats["clean_close"] and owner.stats["failed"]
        await generation.prepare_close()
        await generation.close()
        assert generation._closed
    finally:
        await owner.close()
