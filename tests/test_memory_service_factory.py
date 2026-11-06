"""Host and minimal-host adapters bind the identical service implementation."""

from dataclasses import replace

import pytest

from kernel.app_bus import AppBus
from kernel.message import Message
from tests.test_raw_message_service import profile


@pytest.mark.asyncio
async def test_host_capture_enters_the_same_service_add_and_replay(tmp_path):
    from kernel.raw_message_service import RawMessageService
    from system_memory_god.memory_factory import create_memory_service
    from system_memory_god.memory_service import MemoryService

    settings = replace(profile(tmp_path / "raw"), derived_root=tmp_path / "derived")
    raw = RawMessageService(settings)
    bus = AppBus(str(tmp_path / "apps"))
    bus.attach_raw_messages(raw, lambda: "test-agent")
    service = create_memory_service(settings, mode="raw-vector-v1", embedding="synthetic",
                                    semantic_mode="disabled", raw=raw, bus=bus)
    bus.attach_daily_runtime(service)
    assert type(service) is MemoryService and raw.memory_service is service
    try:
        saved = await raw.capture(Message.user_input("宿主原话", "web", chat_name="web_console"),
                                  "test-agent")
        assert (await service.replay(service.scopes[0]))[0].message_id == saved.get("origin_message_id")
        assert not bus.active_apps and not bus.available_apps
    finally:
        await service.close()
