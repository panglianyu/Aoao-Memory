"""Internal failure evidence retains code locations without retaining data."""

import json

import pytest

from system_memory_god.challenge_runtime import ChallengeRuntime
from tests.test_memory_challenge_runtime import FakeFactory, add


@pytest.mark.asyncio
async def test_close_failure_is_precise_private_and_not_overwritten_by_cleanup(tmp_path):
    factory = FakeFactory()
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model", service_factory=factory)
    await add(runtime, user="private-user-not-diagnostic")
    service = factory.services[0]
    original = service.close

    async def broken_close():
        raise ValueError("secret-message-not-diagnostic")

    service.close = broken_close
    try:
        with pytest.raises(RuntimeError):
            await add(runtime, user="next-private-user")
        diagnostic = runtime.diagnostic()
        assert diagnostic["failure"]["stage"] == "service_close"
        assert diagnostic["failure"]["error_type"] == "ValueError"
        assert any(frame["function"] == "broken_close"
                   for cause in diagnostic["failure"]["chain"] for frame in cause["frames"])
        text = json.dumps(diagnostic)
        assert "secret-message" not in text and "private-user" not in text and str(tmp_path) not in text
        diagnostic["failure"]["stage"] = "mutated"
        assert runtime.diagnostic()["failure"]["stage"] == "service_close"
        runtime._record_failure("cache_retired", PermissionError("later-private-cleanup"))
        assert runtime.diagnostic()["failure"]["stage"] == "service_close"
        assert runtime.diagnostic()["failures"][-1]["stage"] == "cache_retired"
    finally:
        service.close = original
        await runtime.close()


def test_internal_failure_history_is_bounded(tmp_path):
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model", service_factory=FakeFactory())
    for _ in range(20):
        runtime._record_failure("cache_enter", RuntimeError("not retained"))
    assert len(runtime.diagnostic()["failures"]) == 8


@pytest.mark.asyncio
async def test_published_and_search_exceptions_keep_their_stage(tmp_path):
    factory = FakeFactory()
    runtime = ChallengeRuntime(tmp_path, client=object(), model="test-model", service_factory=factory)
    await add(runtime)
    service = factory.services[0]
    original = service.wait_until_published

    async def broken(*args, **kwargs):
        raise ValueError("private-data-not-retained")

    try:
        service.wait_until_published = broken
        with pytest.raises(ValueError):
            await runtime.search(user_id="alice", query="marker", top_k=3)
        assert runtime.diagnostic()["failure"]["stage"] == "published"
        await runtime.wait_idle()
        assert service.closed
        # A failed request now retires its cache; inject the next error into
        # the new live service rather than the retired object's old method.
        await add(runtime, request="reopened-for-search")
        service = factory.services[-1]
        service.search = broken
        with pytest.raises(ValueError):
            await runtime.search(user_id="alice", query="marker", top_k=3)
        assert runtime.diagnostic()["failures"][-1]["stage"] == "search"
    finally:
        service.wait_until_published = original
        await runtime.close()
