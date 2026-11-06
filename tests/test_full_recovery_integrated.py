"""Cancelled caller, durable replay and cold restart over the actual local stack."""

import asyncio
from pathlib import Path
import tempfile
import time

import pytest

from system_memory_god.challenge_lanes import ChallengeLanes
from system_memory_god.memory_factory import create_memory_service
from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner
from tests.test_memory_challenge_real_workers import _EmptyStructuredClient, _fake_1024_embedding


@pytest.mark.asyncio
async def test_lost_waiter_replay_cold_restart_keeps_all_memory_and_call_budget(record_property):
    root = Path(tempfile.mkdtemp(prefix="i-", dir=tempfile.gettempdir()))
    entered, release = asyncio.Event(), asyncio.Event()
    client = _EmptyStructuredClient()
    services = []

    def factory(profile, **options):
        service = create_memory_service(profile, **options)
        original = service.wait_until_published

        async def publication(scope, *, deadline):
            entered.set()
            await release.wait()
            return await original(scope, deadline=deadline)

        service.wait_until_published = publication
        services.append(service)
        return service

    values = {"user_id": "synthetic-integrated-user", "request_id": "request-one",
              "session_id": "session-one", "messages": [
                  {"role": "user", "content": "INTEGRATED-TAIL-ONE"},
                  {"role": "user", "content": "INTEGRATED-TAIL-TWO"}]}
    began = time.monotonic()
    for phase in range(2):
        embedding = SharedEmbeddingOwner(embedder=_fake_1024_embedding, max_generations=2)
        runtime = ChallengeLanes(root, client=client, model="synthetic-local-model",
                                 embedding_owner=embedding, add_timeout=100.0,
                                 service_factory=factory)
        lane = runtime.for_lane(0)
        waiter = entered_wait = None
        try:
            await embedding.start(time.monotonic_ns() + 60_000_000_000)
            if phase == 0:
                waiter = asyncio.create_task(lane.add(**values))
                entered_wait = asyncio.create_task(entered.wait())
                done, _ = await asyncio.wait({waiter, entered_wait}, timeout=45,
                                            return_when=asyncio.FIRST_COMPLETED)
                if waiter in done:
                    await waiter  # Propagate startup errors instead of hiding them behind an event timeout.
                assert entered_wait in done, "publication did not start within the fixture deadline"
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
                assert lane.busy
                release.set()
                await asyncio.wait_for(lane.wait_idle(), timeout=60)
                before = client.calls
                assert before == 2
            else:
                before = client.calls
            response = await lane.add(**values)
            assert response["success"] is True
            assert client.calls == before
            found = await lane.search(user_id=values["user_id"], query="INTEGRATED-TAIL", top_k=100)
            assert {item["content"] for item in found} == {
                "INTEGRATED-TAIL-ONE", "INTEGRATED-TAIL-TWO"}
            assert lane.health()["state"] == "ready"
            service = services[-1]
            assert len(service.raw.store.replay(service.scopes[0], service.scopes[0].conversation_id)) == 2
            assert service.runtime.vector._snapshot_rows == {}
        finally:
            release.set()
            if entered_wait is not None and not entered_wait.done():
                entered_wait.cancel()
            await asyncio.gather(*(task for task in (waiter, entered_wait) if task is not None),
                                 return_exceptions=True)
            await runtime.close()
            await embedding.close()
            assert embedding.stats["clean_close"]
    record_property("integrated_seconds", time.monotonic() - began)
    record_property("synthetic_sdk_calls", client.calls)
