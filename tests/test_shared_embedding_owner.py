"""Fake-only checks for the private shared embedding process."""

import asyncio
import math
import multiprocessing
import os
import threading
import time

import pytest

from system_memory_god.storage.shared_embedding_owner import (
    SharedEmbeddingClient, SharedEmbeddingOwner, _decode_response, _encode_request,
)
from system_memory_god.storage import shared_embedding_owner as shared


def _embedding(texts):
    if texts[0] == "slow":
        time.sleep(0.25)
    if texts[0] == "bad":
        return [[math.nan] * 1024]
    return [[float(len(texts[0]))] * 1024]


def _failed_start(texts):
    raise RuntimeError("fake initialization failure")


class _CountingEmbedder:
    def __init__(self, calls):
        self.calls = calls

    def __call__(self, texts):
        with self.calls.get_lock():
            self.calls.value += 1
        if texts[0] == "never":
            raise AssertionError("expired queued inference must not run")
        return _embedding(texts)


def _use_spawned_client(client, returned, release):
    try:
        returned.send_bytes(str(client(["spawn"])[0][0]).encode("ascii"))
        if not release.wait(5):
            raise TimeoutError("fake worker close was not requested")
    finally:
        client.close()
        returned.close()


def _die_holding_guard(guard, ready):
    guard.acquire()
    ready.set()
    os._exit(0)


def _dead_guard_probe(returned):
    """No model child is started; a failed probe can be safely terminated."""
    context = multiprocessing.get_context("spawn")
    owner = SharedEmbeddingOwner(embedder=_embedding)
    ready = context.Event()
    poisoner = context.Process(target=_die_holding_guard, args=(owner._guard, ready))
    client_pipe, server_pipe = context.Pipe()
    try:
        poisoner.start()
        if not ready.wait(3):
            raise TimeoutError("fake lock holder did not start")
        poisoner.join(3)
        assert poisoner.exitcode == 0
        snapshot = owner.stats
        assert snapshot["stats_available"] is False
        assert snapshot["ipc_inflight"] is None
        client = SharedEmbeddingClient(client_pipe, owner._state, owner._guard,
                                      "a" * 64, 1, "writer", _deadline(0.05))
        with pytest.raises((RuntimeError, TimeoutError)):
            client(["locked"])
        with pytest.raises(RuntimeError):
            asyncio.run(owner.close())
        assert not owner.stats["clean_close"] and owner.stats["failed"]
        returned.send_bytes(b"bounded_unknown")
    finally:
        if poisoner.is_alive():
            poisoner.terminate()
            poisoner.join(3)
        poisoner.close()
        client_pipe.close()
        server_pipe.close()
        returned.close()


def _deadline(seconds=10):
    return time.monotonic_ns() + int(seconds * 1_000_000_000)


async def _finish(generation):
    await generation.prepare_close()
    for client in generation.clients.values():
        client.close()
    await generation.close()


@pytest.mark.asyncio
async def test_model_once_across_generations_and_exact_single_item_abi():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        for number in range(1, 4):
            generation = await owner.new_generation(_deadline())
            assert generation.number == number
            assert await asyncio.to_thread(generation.clients["writer"], ["fact"]) == [[4.0] * 1024]
            assert await asyncio.to_thread(generation.clients["query"], ["q"]) == [[1.0] * 1024]
            await _finish(generation)
        assert owner.stats["load_count"] == 1
        assert owner.stats["generation_count"] == 3
        assert owner.stats["ipc_inflight"] == 0
    finally:
        await owner.close()
    assert owner.stats["clean_close"]
    assert owner.stats["model_owner_count"] == 0


@pytest.mark.asyncio
async def test_two_reserved_before_send_and_one_outstanding_per_role():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        writer = asyncio.create_task(asyncio.to_thread(generation.clients["writer"], ["slow"]))
        query = asyncio.create_task(asyncio.to_thread(generation.clients["query"], ["slow"]))
        for _ in range(100):
            if owner.stats["ipc_inflight"] == 2:
                break
            await asyncio.sleep(0.005)
        assert owner.stats["ipc_inflight"] == 2
        with pytest.raises(RuntimeError):
            await asyncio.to_thread(generation.clients["writer"], ["third"])
        await asyncio.gather(writer, query)
        assert owner.stats["ipc_peak"] == 2
        await _finish(generation)
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_expired_waiter_keeps_capacity_and_prevents_deadline_reset():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline(0.08))
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(generation.clients["writer"], ["slow"])
        assert owner.stats["ipc_inflight"] == 1
        old_deadline = generation._state[shared._DEADLINE]
        with pytest.raises(shared.DeadlineHandoffNotCommitted):
            await generation.set_deadline(_deadline(.03))
        assert generation._state[shared._DEADLINE] == old_deadline and not owner.stats["failed"]
        with pytest.raises(RuntimeError):
            await owner.new_generation(_deadline())
        await _finish(generation)
        assert owner.stats["ipc_inflight"] == 0
        assert not owner.stats["failed"]
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_unstarted_expiry_and_shape_errors_never_reserve():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        client = generation.clients["writer"]
        for texts in ([], ["a", "b"], ("a",), [False], ["x" * 131073]):
            with pytest.raises((ValueError, TypeError)):
                await asyncio.to_thread(client, texts)
        client.bind_deadline(time.monotonic_ns() - 1)
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(client, ["a"])
        assert owner.stats["ipc_peak"] == 0
        await _finish(generation)
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_unexpected_eof_and_invalid_model_output_fail_closed_without_reload():
    for mode in ("eof", "bad"):
        owner = SharedEmbeddingOwner(embedder=_embedding)
        try:
            await owner.start(_deadline())
            generation = await owner.new_generation(_deadline())
            if mode == "eof":
                generation.clients["writer"].close()
            else:
                with pytest.raises(RuntimeError):
                    await asyncio.to_thread(generation.clients["writer"], ["bad"])
            for _ in range(100):
                if owner.stats["failed"]:
                    break
                await asyncio.sleep(0.005)
            assert owner.stats["failed"]
            with pytest.raises(RuntimeError):
                await owner.new_generation(_deadline())
            assert owner.stats["load_count"] == 1
        finally:
            await owner.close()
        assert owner.stats["clean_close"]


def test_protocol_bounds_and_identity_are_closed():
    import json

    nonce = "a" * 64
    frame = _encode_request(nonce, 1, "writer", 1, _deadline(), "x" * 131072)
    assert len(frame) <= 1024 * 1024
    identity = {"schema": "amc-shared-embedding-v1", "op": "result",
                "owner_nonce": nonce, "generation": 1, "role": "writer", "request_id": 1}
    valid = {**identity, "ok": True, "vector": [1.0] * 1024}
    assert _decode_response(json.dumps(valid).encode(), nonce, 1, "writer", 1) == [1.0] * 1024
    invalids = [dict(valid, generation=2), dict(valid, request_id=True),
                dict(valid, vector=[True] * 1024), dict(valid, extra=1),
                dict(valid, vector=[float("nan")] * 1024), dict(valid, vector=[0.0] * 1023),
                dict(valid, vector=[10 ** 1000] + [0] * 1023)]
    for payload in invalids:
        with pytest.raises((ValueError, RuntimeError)):
            _decode_response(json.dumps(payload).encode(), nonce, 1, "writer", 1)
    with pytest.raises(ValueError):
        _decode_response(b'{"ok":true,"ok":true}', nonce, 1, "writer", 1)


@pytest.mark.asyncio
async def test_spawned_client_parent_copy_closes_and_eof_is_owned():
    context = multiprocessing.get_context("spawn")
    owner = SharedEmbeddingOwner(embedder=_embedding)
    processes, receivers = [], []
    release = context.Event()
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        for client in generation.clients.values():
            receiver, returned = context.Pipe(duplex=False)
            process = context.Process(target=_use_spawned_client, args=(client, returned, release))
            processes.append(process)
            receivers.append(receiver)
            process.start()
            returned.close()
            client.close_parent_copy()
        for receiver in receivers:
            assert await asyncio.to_thread(receiver.poll, 5)
            assert receiver.recv_bytes() == b"5.0"
        await generation.prepare_close()
        release.set()
        for process in processes:
            await asyncio.to_thread(process.join, 5)
            assert process.exitcode == 0
        await generation.close()
        assert not owner.stats["failed"]
    finally:
        release.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(5)
            process.close()
        for receiver in receivers:
            receiver.close()
        await owner.close()
    assert owner.stats["clean_close"]


@pytest.mark.asyncio
async def test_queued_expiry_never_runs_and_late_response_cannot_move_generation():
    context = multiprocessing.get_context("spawn")
    calls = context.Value("i", 0)
    owner = SharedEmbeddingOwner(embedder=_CountingEmbedder(calls))
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline(0.12))
        writer = asyncio.create_task(asyncio.to_thread(generation.clients["writer"], ["slow"]))
        while owner.stats["ipc_inflight"] == 0:
            await asyncio.sleep(0.001)
        await asyncio.sleep(0.015)
        query = asyncio.create_task(asyncio.to_thread(generation.clients["query"], ["never"]))
        results = await asyncio.gather(writer, query, return_exceptions=True)
        assert all(isinstance(result, TimeoutError) for result in results)
        assert owner.stats["ipc_inflight"] == 2
        await asyncio.sleep(0.2)
        assert calls.value == 2
        assert owner.stats["ipc_inflight"] == 0
        old_deadline = generation._state[shared._DEADLINE]
        with pytest.raises(shared.DeadlineHandoffNotCommitted):
            await generation.set_deadline(_deadline(.03))
        assert generation._state[shared._DEADLINE] == old_deadline and not owner.stats["failed"]
        with pytest.raises(RuntimeError):
            await owner.new_generation(_deadline())
        await _finish(generation)
        new_generation = await owner.new_generation(_deadline())
        assert await asyncio.to_thread(new_generation.clients["writer"], ["new"]) == [[3.0] * 1024]
        await _finish(new_generation)
        assert not owner.stats["failed"]
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_startup_failure_joins_actual_process_without_retry():
    owner = SharedEmbeddingOwner(embedder=_failed_start)
    with pytest.raises((RuntimeError, EOFError, OSError)):
        await owner.start(_deadline())
    assert owner.stats["load_count"] == 1
    assert owner.stats["failed"] and owner.stats["clean_close"]
    with pytest.raises(RuntimeError):
        await owner.start(_deadline())
    await owner.close()


@pytest.mark.asyncio
async def test_cancelled_http_thread_waiter_and_repeated_close_cancel_keep_owners():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    await owner.start(_deadline())
    generation = await owner.new_generation(_deadline())
    call = asyncio.create_task(asyncio.to_thread(generation.clients["writer"], ["slow"]))
    while owner.stats["ipc_inflight"] == 0:
        await asyncio.sleep(0.001)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert owner.stats["ipc_inflight"] == 1
    close = asyncio.create_task(owner.close())
    await asyncio.sleep(0.01)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    close_again = asyncio.create_task(owner.close())
    await asyncio.sleep(0.01)
    close_again.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_again
    await owner.close()
    assert owner.stats["clean_close"]
    assert owner.stats["ipc_inflight"] == 0
    assert not owner.stats["failed"]


@pytest.mark.asyncio
async def test_only_parent_may_register_the_next_operation_deadline():
    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        client = generation.clients["writer"]
        next_deadline = client._deadline_ns + 1_000_000
        client.bind_deadline(next_deadline)
        with pytest.raises(RuntimeError):
            await asyncio.to_thread(client, ["unregistered"])
        assert owner.stats["ipc_inflight"] == 0
        await generation.set_deadline(next_deadline)
        assert await asyncio.to_thread(client, ["allowed"]) == [[7.0] * 1024]
        await _finish(generation)
    finally:
        await owner.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["pickle", "identity", "duplicate_key", "extra"])
async def test_data_pipe_rejects_non_json_or_unbound_frames_without_reload(fault):
    import json

    owner = SharedEmbeddingOwner(embedder=_embedding)
    try:
        await owner.start(_deadline())
        generation = await owner.new_generation(_deadline())
        client = generation.clients["writer"]
        payload = json.loads(_encode_request(owner._nonce, generation.number,
                            "writer", 1, client._deadline_ns, "no model call"))
        if fault == "pickle":
            frame = b"\x80\x04N."
        elif fault == "identity":
            frame = json.dumps(dict(payload, owner_nonce="b" * 64)).encode()
        elif fault == "duplicate_key":
            frame = b'{"schema":"amc-shared-embedding-v1","schema":"duplicate"}'
        else:
            frame = json.dumps(dict(payload, extra="invalid")).encode()
        client._connection.send_bytes(frame)
        for _ in range(100):
            if owner.stats["failed"]:
                break
            await asyncio.sleep(0.005)
        assert owner.stats["failed"]
        assert owner.stats["load_count"] == 1
        with pytest.raises(RuntimeError):
            await generation.set_deadline(_deadline())
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_process_start_failure_closes_both_control_copies(monkeypatch):
    owner = SharedEmbeddingOwner(embedder=_embedding)
    pipes = []
    original = owner._context.Pipe

    def capture_pipe(*args, **kwargs):
        pair = original(*args, **kwargs)
        pipes.extend(pair)
        return pair

    def fail_start(process):
        raise OSError("fake spawn failure")

    monkeypatch.setattr(owner._context, "Pipe", capture_pipe)
    monkeypatch.setattr(multiprocessing.context.SpawnProcess, "start", fail_start)
    with pytest.raises(OSError):
        await owner.start(_deadline())
    assert len(pipes) == 2 and all(pipe.closed for pipe in pipes)
    await owner.close()
    assert owner.stats["failed"] and owner.stats["clean_close"]


@pytest.mark.asyncio
async def test_valid_response_cannot_release_role_before_server_completion_commit():
    import json

    context = multiprocessing.get_context("spawn")
    client_pipe, server_pipe = context.Pipe()
    state, guard = context.RawArray("q", 15), context.RLock()
    deadline = _deadline()
    state[shared._GENERATION], state[shared._DEADLINE] = 1, deadline
    client = SharedEmbeddingClient(client_pipe, state, guard, "a" * 64, 1, "writer", deadline)
    sent, commit = threading.Event(), threading.Event()
    errors = []

    def serve_two():
        try:
            for number in (1, 2):
                frame = json.loads(server_pipe.recv_bytes())
                assert frame["request_id"] == number
                response = {key: frame[key] for key in ("schema", "owner_nonce", "generation",
                                                        "role", "request_id")}
                response.update(op="result", ok=True, vector=[float(number)] * 1024)
                server_pipe.send_bytes(json.dumps(response).encode())
                if number == 1:
                    sent.set()
                    assert commit.wait(5)
                with guard:
                    assert state[shared._WRITER_ID] == number
                    state[shared._WRITER_ID] = 0
                    state[shared._INFLIGHT] -= 1
        except BaseException as error:
            errors.append(error)
        finally:
            server_pipe.close()

    server = asyncio.create_task(asyncio.to_thread(serve_two))
    first = asyncio.create_task(asyncio.to_thread(client, ["one"]))
    try:
        assert await asyncio.to_thread(sent.wait, 3)
        await asyncio.sleep(0.02)
        assert not first.done()
        assert state[shared._WRITER_IO] == state[shared._INFLIGHT] == 1
        with pytest.raises(RuntimeError):
            await asyncio.to_thread(client, ["too early"])
        commit.set()
        assert await first == [[1.0] * 1024]
        assert await asyncio.to_thread(client, ["two"]) == [[2.0] * 1024]
        await server
        assert errors == []
        assert state[shared._WRITER_IO] == state[shared._INFLIGHT] == 0
    finally:
        commit.set()
        client.close()
        await asyncio.gather(first, server, return_exceptions=True)


def test_dead_process_guard_does_not_block_stats_client_or_final_close():
    context = multiprocessing.get_context("spawn")
    receiver, returned = context.Pipe(duplex=False)
    probe = context.Process(target=_dead_guard_probe, args=(returned,))
    probe.start()
    returned.close()
    try:
        probe.join(4)
        assert not probe.is_alive(), "dead child guard blocked its real parent owner"
        assert probe.exitcode == 0
        assert receiver.poll(0.1)
        assert receiver.recv_bytes() == b"bounded_unknown"
    finally:
        if probe.is_alive():
            probe.terminate()
            probe.join(3)
        probe.close()
        receiver.close()
