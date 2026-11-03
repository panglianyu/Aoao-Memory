"""Daily capture uses real isolated SQLite and public replay interfaces."""

import pytest
import asyncio
import statistics
import time
from dataclasses import replace

from kernel.message import Message
from kernel.raw_message_models import ConflictError, DerivationSpec, RawMessageError, Scope
from kernel.raw_message_profile import ConversationBinding, ConnectorBinding, RawMessageProfile
from kernel.raw_message_service import ChannelOriginal, RawMessageService


def profile(tmp_path):
    return RawMessageProfile(
        database=tmp_path / "daily.db", owner_id="test-owner",
        connectors=(ConnectorBinding(
            agent_id="test-agent", source="web", connector_id="web-test",
            account_ref="local-test", conversations=(
                ConversationBinding("web_console", "local-chat", "web", "测试对话"),
            ),
        ),),
        derivations=(DerivationSpec("daily", "v1", "vector"),),
    )


@pytest.mark.asyncio
async def test_feishu_business_handoff_states_survive_service_reopen(tmp_path):
    settings = replace(profile(tmp_path), connectors=(ConnectorBinding(
        "test-agent", "feishu", "feishu-test", "test-account",
        (ConversationBinding("oc_states", "oc_states", "group", "测试群"),),
    ),))
    service = RawMessageService(settings)
    scope = Scope("test-agent", "test-owner", "feishu-test")
    saved = []
    for index in range(3):
        saved.append(await service.capture(Message.user_input(
            "模型信封", "feishu", chat_name="oc_states",
            raw_original=ChannelOriginal(
                content=f'{{"text":"消息{index}"}}',
                provider_message_id=f"om_state_{index}",
                provider_conversation_id="oc_states", sender_id="ou_test",
                created_at="2026-09-20T00:00:00+00:00",
            ),
        ), "test-agent"))
    assert await service.claim_input_dispatch(saved[1]) is True
    assert await service.claim_input_dispatch(saved[2]) is True
    service.mark_input_dispatched(saved[2])
    await service.close()

    reopened = RawMessageService(settings)
    try:
        assert await reopened.business_input_states(scope) == [
            {"message_id": saved[0].get("origin_message_id"), "state": "saved_unattempted"},
            {"message_id": saved[1].get("origin_message_id"), "state": "claimed_unknown"},
            {"message_id": saved[2].get("origin_message_id"), "state": "queue_dispatched"},
        ]
        assert await reopened.business_input_states(Scope("other-agent", "test-owner")) == []
        assert await reopened.business_input_states(Scope("test-agent", "other-owner")) == []
        assert await reopened.business_input_states(
            Scope("test-agent", "test-owner", "other-connector"),
        ) == []
        assert await reopened.business_input_states(
            Scope("test-agent", "test-owner", "feishu-test", "other-conversation"),
        ) == []
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_business_handoff_audit_excludes_web_and_limited_media(tmp_path):
    base = profile(tmp_path)
    feishu = ConnectorBinding(
        "test-agent", "feishu", "feishu-test", "test-account",
        (ConversationBinding("oc_states", "oc_states", "group", "测试群"),),
    )
    service = RawMessageService(replace(base, connectors=(*base.connectors, feishu)))
    try:
        await service.capture(Message.user_input(
            "网页原文", "web", chat_name="web_console",
        ), "test-agent")
        await service.capture(Message.user_input(
            "媒体信封", "feishu", chat_name="oc_states",
            raw_original=ChannelOriginal(
                content='{"image_key":"synthetic"}', content_type="image",
                provider_message_type="image", media_limited=True,
                provider_message_id="om_media", provider_conversation_id="oc_states",
                sender_id="ou_test", created_at="2026-09-20T00:00:00+00:00",
            ),
        ), "test-agent")
        for message_id, original, limited, time_limited in (
            ("om_malformed", '{"text":"片段"', False, False),
            ("om_restricted", '{"text":"受限"}', True, False),
            ("om_bad_time", '{"text":"时间无效"}', False, True),
        ):
            await service.capture(Message.user_input(
                "受限信封", "feishu", chat_name="oc_states",
                raw_original=ChannelOriginal(
                    content=original, provider_message_type="post",
                    provider_limited=limited, provider_timestamp_limited=time_limited,
                    provider_message_id=message_id, provider_conversation_id="oc_states",
                    sender_id="ou_test", created_at="2026-09-20T00:00:00+00:00",
                ),
            ), "test-agent")
        assert await service.business_input_states(Scope("test-agent", "test-owner")) == []
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_feishu_post_business_handoff_is_visible_after_reopen(tmp_path):
    settings = replace(profile(tmp_path), connectors=(ConnectorBinding(
        "test-agent", "feishu", "feishu-test", "test-account",
        (ConversationBinding("oc_post", "oc_post", "group", "测试群"),),
    ),))
    original = '{"post":{"en_us":{"content":[[{"tag":"text","text":"hello"}]]}}}'
    service = RawMessageService(settings)
    saved = await service.capture(Message.user_input(
        "模型信封", "feishu", chat_name="oc_post",
        raw_original=ChannelOriginal(
            content=original, provider_message_type="post",
            provider_message_id="om_post_audit", provider_conversation_id="oc_post",
            sender_id="ou_test", created_at="2026-09-20T00:00:00+00:00",
        ),
    ), "test-agent")
    assert await service.claim_input_dispatch(saved) is True
    await service.close()

    reopened = RawMessageService(settings)
    try:
        assert await reopened.business_input_states(Scope("test-agent", "test-owner")) == [
            {"message_id": saved.get("origin_message_id"), "state": "claimed_unknown"},
        ]
        assert await reopened.business_input_states(Scope("other-agent", "test-owner")) == []
        assert await reopened.business_input_states(Scope("test-agent", "other-owner")) == []
        assert await reopened.business_input_states(
            Scope("test-agent", "test-owner", "other-connector"),
        ) == []
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_proactive_target_requires_explicit_single_dm_binding(tmp_path):
    base = profile(tmp_path)
    connector = ConnectorBinding(
        "test-agent", "feishu", "feishu-proactive", "test-account",
        (ConversationBinding("first", "oc_first", "dm", "私聊一", proactive_send=True),
         ConversationBinding("second", "oc_second", "dm", "私聊二")),
    )
    service = RawMessageService(replace(base, connectors=(connector,)))
    assert service.proactive_target("test-agent", "feishu") == "first"
    assert service.proactive_target("other-agent", "feishu") is None
    ambiguous = replace(connector, conversations=tuple(
        replace(chat, proactive_send=True) for chat in connector.conversations
    ))
    ambiguous_service = RawMessageService(replace(base, connectors=(ambiguous,)))
    assert ambiguous_service.proactive_target("test-agent", "feishu") is None
    with pytest.raises(RawMessageError):
        replace(base, connectors=(replace(connector, conversations=(
            ConversationBinding("group", "oc_group", "group", "群", proactive_send=True),
        )),))
    await service.close()
    await ambiguous_service.close()


@pytest.mark.asyncio
async def test_unsupported_provider_type_cannot_claim_normal_processing(tmp_path):
    service = RawMessageService(profile(tmp_path))
    msg = Message.user_input(
        "模型信封", "web", chat_name="web_console",
        raw_original=ChannelOriginal(content='{"type":"interactive"}',
                                     provider_message_type="unsupported",
                                     provider_source_type="interactive"),
    )
    with pytest.raises(RawMessageError):
        await service.capture(msg, "test-agent")
    assert list(service.store.export_messages(Scope("test-agent", "test-owner"))) == []
    await service.close()


@pytest.mark.asyncio
async def test_synthetic_daily_capture_120_message_latency_probe(tmp_path):
    service = RawMessageService(profile(tmp_path))
    latencies = []
    loop_delays = []
    active = True

    async def ticker():
        while active:
            started = time.perf_counter()
            await asyncio.sleep(0.005)
            loop_delays.append(max(0.0, time.perf_counter() - started - 0.005))

    ticker_task = asyncio.create_task(ticker())
    for index in range(120):
        started = time.perf_counter()
        await service.capture(Message.user_input(
            f"synthetic-{index}", "web", chat_name="web_console",
        ), "test-agent")
        latencies.append((time.perf_counter() - started) * 1000)
    active = False
    await ticker_task
    rows = list(service.store.export_messages(Scope("test-agent", "test-owner")))
    assert len(rows) == 120
    ordered = sorted(latencies)
    lag = sorted(loop_delays)
    print(
        f"daily-capture-120 p50={statistics.median(ordered):.3f}ms "
        f"p95={ordered[int(0.95 * len(ordered)) - 1]:.3f}ms "
        f"loop-p95={lag[int(0.95 * len(lag)) - 1]:.3f}ms"
    )
    await service.close()


@pytest.mark.asyncio
async def test_accepted_original_survives_service_reopen(tmp_path):
    settings = profile(tmp_path)
    service = RawMessageService(settings)
    msg = Message.user_input("  原文\r\n不裁剪  ", "web", chat_name="web_console")
    accepted = await service.capture(msg, "test-agent")
    await service.close()
    reopened = RawMessageService(settings)
    scope = Scope("test-agent", "test-owner")
    rows = await reopened.replay(scope, accepted.get("conversation_id"))
    assert [row.content for row in rows] == [msg.content]
    assert rows[0].role == "user"
    assert rows[0].sender_id != settings.owner_id
    await reopened.close()


@pytest.mark.asyncio
async def test_pending_tasks_are_not_reported_as_completed_workers(tmp_path):
    service = RawMessageService(profile(tmp_path))
    await service.capture(Message.user_input("任务应待处理", "web", chat_name="web_console"), "test-agent")
    report = await service.diagnostics(Scope("test-agent", "test-owner"))
    assert report["derivations"] == {"pending": 1}
    assert report["oldest_pending_seconds"] >= 0
    assert report["worker_enabled"] is False
    await service.close()


def test_stable_history_key_does_not_use_display_name():
    from kernel.brain import _session_key
    assert _session_key("同名群", "wechat", "stable-wx") != _session_key("同名群", "feishu", "stable-fs")
    assert _session_key("改名前", "wechat", "stable-wx") == _session_key("改名后", "wechat", "stable-wx")


@pytest.mark.asyncio
async def test_provider_retry_preserves_raw_not_model_envelope(tmp_path):
    service = RawMessageService(profile(tmp_path))
    original = ChannelOriginal(
        content="  原话[LOCAL_IMAGE:untrusted]\n  ", provider_message_id="event-1",
        created_at="2026-09-18T10:00:00+08:00", sender_id="speaker-1",
        provider_conversation_id="local-chat",
    )
    msg = Message.user_input("安全化后的模型输入", "web", chat_name="web_console", raw_original=original)
    saved = await service.capture(msg, "test-agent")
    retry = await service.capture(replace(msg, id="other-local-id", timestamp=msg.timestamp + 1), "test-agent")
    rows = await service.replay(Scope("test-agent", "test-owner"), saved.get("conversation_id"))
    assert len(rows) == 1
    assert rows[0].content == original.content
    assert retry.get("origin_message_id") == saved.get("origin_message_id")
    assert "raw_original" not in saved.metadata
    assert service.verify(saved)
    assert not service.verify(replace(saved, content="篡改模型输入"))
    with pytest.raises(ConflictError):
        await service.capture(replace(msg, metadata={**msg.metadata, "raw_original": replace(original, content="冲突")}), "test-agent")
    await service.close()


@pytest.mark.parametrize("field,value", [("owner_id", ""), ("connectors", ()), ("derivations", ())])
def test_invalid_daily_profile_creates_nothing(tmp_path, field, value):
    with pytest.raises(RawMessageError):
        replace(profile(tmp_path), **{field: value})
    assert not (tmp_path / "daily.db").exists()


@pytest.mark.asyncio
async def test_unknown_scope_and_foreign_owner_cannot_read(tmp_path):
    service = RawMessageService(profile(tmp_path))
    msg = Message.user_input("私有原文", "web", chat_name="web_console")
    with pytest.raises(RawMessageError):
        await service.capture(msg, "unknown-agent")
    saved = await service.capture(msg, "test-agent")
    assert await service.replay(Scope("test-agent", "foreign-owner"), saved.get("conversation_id")) == []
    assert await service.replay(Scope("foreign-agent", "test-owner"), saved.get("conversation_id")) == []
    await service.close()
    with pytest.raises(RawMessageError):
        await service.capture(msg, "test-agent")


@pytest.mark.asyncio
async def test_concurrent_messages_have_strict_unique_ordinals(tmp_path):
    service = RawMessageService(profile(tmp_path))
    messages = [Message.user_input(f"合成消息{i}", "web", chat_name="web_console") for i in range(20)]
    saved = await asyncio.gather(*(service.capture(m, "test-agent") for m in messages))
    await service.close()
    rows = await service.replay(Scope("test-agent", "test-owner"), saved[0].get("conversation_id"))
    assert [r.ordinal for r in rows] == list(range(1, 21))
    assert len({r.message_id for r in rows}) == 20


@pytest.mark.asyncio
async def test_provider_conversation_keeps_two_senders_in_one_chat(tmp_path):
    settings = replace(profile(tmp_path), connectors=(ConnectorBinding(
        "test-agent", "feishu", "feishu-test", "test-account",
        (ConversationBinding("oc_group", "oc_group", "group", "测试群"),),
    ),))
    service = RawMessageService(settings)
    saved = []
    for sender, event_id in (("ou_first", "event-first"), ("ou_second", "event-second")):
        original = ChannelOriginal(
            content="合成原文", sender_id=sender, provider_message_id=event_id,
            provider_conversation_id="oc_group",
        )
        saved.append(await service.capture(Message.user_input(
            "模型信封", "feishu", chat_name=sender, raw_original=original,
        ), "test-agent"))
    assert saved[0].get("conversation_id") == saved[1].get("conversation_id")
    rows = await service.replay(Scope("test-agent", "test-owner"), saved[0].get("conversation_id"))
    assert [row.sender_id for row in rows] == ["ou_first", "ou_second"]
    await service.close()
