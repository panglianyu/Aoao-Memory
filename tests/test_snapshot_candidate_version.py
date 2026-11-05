"""The candidate must identify its actual version without starting a service."""
from pathlib import Path

import httpx
import pytest

from system_memory_god import challenge_api
from Tools import serve_memory_challenge


class ReadyRuntime:
    lane_count = 1

    async def wait_idle(self):
        return None

    async def close(self):
        return None

    def health(self):
        return {"state": "ready"}


@pytest.mark.asyncio
async def test_health_and_cli_identify_the_candidate():
    label = (Path(__file__).resolve().parents[1] / "CANDIDATE_VERSION").read_text().strip()
    app = challenge_api.create_app(ReadyRuntime(), token="synthetic-credential-for-local-test-only")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test.invalid") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"state": "ready", "version": label}
    assert serve_memory_challenge.ENTRY_VERSION == label
