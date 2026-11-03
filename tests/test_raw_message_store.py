"""File-backed acceptance tests for the isolated conversation fact store."""

import pytest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import sqlite3

from kernel.raw_message_models import (
    AttachmentRef,
    ConflictError,
    MessageDraft,
    RawMessageError,
    Scope,
    ScopeError,
)
from kernel.raw_message_store import RawMessageStore


def opened(tmp_path):
    store = RawMessageStore(tmp_path / "raw_messages.db")
    scope = Scope(agent_id="ailu", user_id="owner")
    store.register_connector(scope, "wx-main", "wechat", "account-1", "微信")
    store.register_conversation(
        scope, "group-1", "wx-main", "remote-1", "group", "测试群"
    )
    return store, scope


def draft(message_id="msg-1", **changes):
    fields = dict(
        message_id=message_id,
        conversation_id="group-1",
        session_id="session-1",
        role="user",
        content="  原话\r\n不改措辞\x00结束  ",
        content_type="text",
        created_at="2026-09-18T14:00:00+08:00",
        source="wechat",
        sender_id="member-1",
    )
    fields.update(changes)
    return MessageDraft(**fields)


def _create_v3_history(path):
    from kernel.raw_message_schema import (
        APPLICATION_ID, SCHEMA_V3_SQL,
    )

    content = "旧对话原文"
    stamp = "2026-09-18T14:00:00+08:00"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA_V3_SQL)
        db.execute("PRAGMA user_version=3")
        db.execute(f"PRAGMA application_id={APPLICATION_ID}")
        db.execute("INSERT INTO connectors VALUES('wx-main','ailu','owner','wechat',"
                   "'account','Test')")
        db.execute("INSERT INTO conversations VALUES('group-1','ailu','owner',"
                   "'wx-main','remote','group','Test',?)", (stamp,))
        db.execute(
            """INSERT INTO raw_messages(message_id,agent_id,user_id,connector_id,
            conversation_id,session_id,ordinal,role,content,content_type,created_at,
            received_at,source,sender_id,content_hash,metadata_json,payload_hash,
            attachment_refs_json) VALUES ('legacy','ailu','owner','wx-main','group-1',
            'session',1,'user',?,'text',?,?,'wechat','sender',?,'{}',?,'[]')""",
            (content, stamp, stamp, digest, digest),
        )
        db.execute("INSERT INTO raw_message_revisions VALUES('legacy',1,?,?,NULL)",
                   (content, digest))
        db.execute(
            """INSERT INTO derivation_outbox(task_id,message_id,revision,processor,
            processor_version,target_domain,model,prompt_hash,status,created_at,
            updated_at) VALUES('old-task','legacy',1,'extract','v1','entity',
            'fake',?,'succeeded',?,?)""",
            ("a" * 64, stamp, stamp),
        )
        db.execute("INSERT INTO derivation_events(task_id,action,occurred_at) "
                   "VALUES('old-task','succeeded',1)")
    return content


def test_v3_history_requires_explicit_index_migration_and_preserves_original(tmp_path):
    from kernel.raw_message_schema import SCHEMA_VERSION

    path = tmp_path / "legacy-v3.db"
    backup = tmp_path / "legacy-v3.backup.db"
    content = _create_v3_history(path)

    with pytest.raises(RawMessageError, match="explicit migration required"):
        RawMessageStore(path)
    receipt = RawMessageStore.migrate_database(path, backup)
    assert receipt["backup_path"] == str(backup)
    assert receipt["backup_sha256"] == hashlib.sha256(backup.read_bytes()).hexdigest()
    scope = Scope("ailu", "owner", "wx-main", "group-1")
    reopened = RawMessageStore(path)
    assert reopened.get(scope, "legacy").content == content
    assert reopened.has_succeeded_derivation(scope)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("SELECT count(*) FROM derivation_events").fetchone()[0] == 1
    with sqlite3.connect(backup) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
        assert db.execute("SELECT content FROM raw_messages").fetchone()[0] == content

    upgraded_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    saved_backup_hash = hashlib.sha256(backup.read_bytes()).hexdigest()
    with pytest.raises(RawMessageError, match="migration source or backup unavailable|unrecognized migration source"):
        RawMessageStore.migrate_database(path, tmp_path / "second.backup.db")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == upgraded_hash
    assert hashlib.sha256(backup.read_bytes()).hexdigest() == saved_backup_hash
    assert not (tmp_path / "second.backup.db").exists()


@pytest.mark.parametrize("fault", ["after_backup", "index_creation"])
def test_v3_migration_interruption_preserves_source_and_verified_backup(
    tmp_path, monkeypatch, fault
):
    import kernel.raw_message_database as database_module

    path = tmp_path / "legacy-v3.db"
    backup = tmp_path / "legacy-v3.backup.db"
    content = _create_v3_history(path)
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    original_index = database_module.DERIVATION_EVENT_INDEX_SQL

    if fault == "after_backup":
        class RaiseAfterBackup(str):
            def strip(self, *args):
                raise RuntimeError("injected after verified backup")

        monkeypatch.setattr(database_module, "DERIVATION_EVENT_INDEX_SQL",
                            RaiseAfterBackup(original_index))
    else:
        monkeypatch.setattr(database_module, "DERIVATION_EVENT_INDEX_SQL",
                            "CREATE INDEX broken ON no_such_table(task_id);")

    with pytest.raises((RuntimeError, sqlite3.OperationalError), match="injected|no_such_table"):
        RawMessageStore.migrate_database(path, backup)
    assert backup.is_file()
    backup_hash = hashlib.sha256(backup.read_bytes()).hexdigest()
    assert backup_hash
    assert hashlib.sha256(path.read_bytes()).hexdigest() == source_hash
    for candidate in (path, backup):
        with sqlite3.connect(candidate) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 3
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert db.execute("SELECT content FROM raw_messages").fetchone()[0] == content
            assert db.execute("SELECT action FROM derivation_events").fetchone()[0] == "succeeded"
            assert not db.execute("SELECT name FROM sqlite_master WHERE name="
                                  "'derivation_event_task_action'").fetchone()

    monkeypatch.setattr(database_module, "DERIVATION_EVENT_INDEX_SQL", original_index)
    with pytest.raises(RawMessageError, match="backup unavailable"):
        RawMessageStore.migrate_database(path, backup)
    assert hashlib.sha256(backup.read_bytes()).hexdigest() == backup_hash
    receipt = RawMessageStore.migrate_database(path, tmp_path / "retry.backup.db")
    assert receipt["backup_sha256"]
    assert RawMessageStore(path).get(Scope("ailu", "owner", "wx-main", "group-1"),
                                      "legacy").content == content


def test_v3_unknown_schema_migration_refuses_before_creating_backup(tmp_path):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_V3_SQL

    path = tmp_path / "unknown-v3.db"
    backup = tmp_path / "unknown-v3.backup.db"
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA_V3_SQL)
        db.execute("PRAGMA user_version=3")
        db.execute(f"PRAGMA application_id={APPLICATION_ID}")
        db.execute("CREATE TABLE foreign_data(payload TEXT)")
    original_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RawMessageError, match="unrecognized migration source"):
        RawMessageStore.migrate_database(path, backup)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original_hash
    assert not backup.exists()


def test_original_message_survives_reopen_without_loss(tmp_path):
    store, scope = opened(tmp_path)
    saved = store.append(scope, draft())
    assert saved.ordinal == 1
    reopened = RawMessageStore(tmp_path / "raw_messages.db")
    restored = reopened.get(scope, "msg-1")
    assert restored == saved
    assert restored.content == draft().content
    assert restored.created_at == "2026-09-18T14:00:00+08:00"
    assert restored.sender_id == "member-1"
    assert reopened.replay(scope, "group-1") == [saved]


def test_identical_retry_is_success_but_changed_payload_is_conflict(tmp_path):
    store, scope = opened(tmp_path)
    original = store.append(scope, draft(idempotency_key="event-1"))
    assert store.append(scope, draft(idempotency_key="event-1")) == original
    assert (
        store.append(scope, draft("new-client-id", idempotency_key="event-1"))
        == original
    )
    with pytest.raises(ConflictError, match="conflict"):
        store.append(scope, draft(content="不同原文", idempotency_key="event-1"))
    assert store.append(scope, draft("msg-2")).ordinal == 2


def test_scope_reply_and_metadata_checks_fail_closed(tmp_path):
    store, scope = opened(tmp_path)
    store.append(scope, draft())
    other = Scope("ailu", "other-owner")
    assert store.get(other, "msg-1") is None
    assert store.replay(other, "group-1") == []
    with pytest.raises(ScopeError):
        store.append(other, draft("msg-2"))
    store.register_conversation(
        scope, "group-2", "wx-main", "remote-2", "group", "另一个群"
    )
    with pytest.raises(ScopeError):
        store.append(
            scope,
            draft("reply-1", conversation_id="group-2", reply_to_message_id="msg-1"),
        )
    with pytest.raises(RawMessageError):
        store.append(
            scope, draft("bad-meta", metadata={"api_key": "must-not-be-stored"})
        )
    with pytest.raises(RawMessageError):
        store.get(Scope("", ""), "msg-1")


def test_display_rename_does_not_change_identity_and_accounts_do_not_collide(tmp_path):
    store, scope = opened(tmp_path)
    store.register_connector(scope, "wx-main", "wechat", "account-1", "微信改名")
    store.register_conversation(
        scope, "group-1", "wx-main", "remote-1", "group", "群改名"
    )
    store.register_connector(scope, "wx-second", "wechat", "account-2", "另一个微信")
    store.register_conversation(
        scope, "group-second", "wx-second", "remote-1", "group", "群改名"
    )
    conversations = store.list_conversations(scope)
    assert {row["conversation_id"] for row in conversations} == {
        "group-1",
        "group-second",
    }
    assert (
        store.list_conversations(scope, connector_id="wx-main")[0]["display_name"]
        == "群改名"
    )
    assert len(store.list_connectors(scope)) == 2
    with pytest.raises(ConflictError):
        store.register_conversation(
            scope, "another-id", "wx-main", "remote-1", "group", "重复群"
        )
    assert store.list_connectors(Scope("other-agent", "owner")) == []


@pytest.mark.parametrize("existing", [False, True])
def test_selected_conversation_cannot_register_or_rename_unrelated_connector(
    tmp_path, existing
):
    store, scope = opened(tmp_path)
    if existing:
        store.register_connector(scope, "wx-other", "wechat", "other-account", "old")
    selected = replace(scope, conversation_id="group-1")
    with pytest.raises(ScopeError):
        store.register_connector(selected, "wx-other", "wechat", "other-account", "new")
    connectors = store.list_connectors(scope)
    assert len(connectors) == (2 if existing else 1)
    if existing:
        assert (
            next(c for c in connectors if c["connector_id"] == "wx-other")[
                "display_name"
            ]
            == "old"
        )
    store.register_connector(selected, "wx-main", "wechat", "account-1", "allowed")
    assert store.list_connectors(selected)[0]["display_name"] == "allowed"


def test_concurrent_commit_order_is_unique_and_pagination_crosses_sessions(tmp_path):
    store, scope = opened(tmp_path)

    def write(index):
        connection = RawMessageStore(tmp_path / "raw_messages.db")
        return connection.append(
            scope, draft(f"msg-{index}", session_id=f"session-{index % 2}")
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        saved = list(pool.map(write, range(32)))
    assert sorted(message.ordinal for message in saved) == list(range(1, 33))
    first = store.replay(scope, "group-1", limit=7)
    second = store.replay(scope, "group-1", after_ordinal=first[-1].ordinal, limit=7)
    assert [m.ordinal for m in first + second] == list(range(1, 15))


def test_attachment_refs_roundtrip_and_cannot_be_claimed_by_another_owner(tmp_path):
    store, scope = opened(tmp_path)
    attachment = AttachmentRef("image-1", "image/png", "a" * 64, 1024, "图片说明")
    saved = store.append(
        scope, draft(content="", content_type="image", attachment_refs=(attachment,))
    )
    assert saved.attachment_refs == (attachment,)
    assert RawMessageStore(tmp_path / "raw_messages.db").get(
        scope, "msg-1"
    ).attachment_refs == (attachment,)
    other = Scope("ailu", "other-owner")
    store.register_connector(other, "other-wx", "wechat", "other-account", "微信")
    store.register_conversation(
        other, "other-group", "other-wx", "remote-1", "group", "群"
    )
    with pytest.raises(ScopeError):
        store.append(
            other,
            draft(
                "other-message",
                conversation_id="other-group",
                attachment_refs=(attachment,),
            ),
        )
    with pytest.raises(ConflictError):
        store.append(
            scope,
            draft(
                "bad-image",
                attachment_refs=(
                    AttachmentRef("image-1", "image/png", "b" * 64, 1024),
                ),
            ),
        )


def test_neighbors_expand_only_same_session_and_hide_forgotten_messages(tmp_path):
    store, scope = opened(tmp_path)
    for index in range(6):
        store.append(
            scope,
            draft(
                f"msg-{index}",
                session_id="other-session" if index == 0 else "session-1",
            ),
        )
    store.forget(scope, "msg-2")
    assert [
        m.message_id for m in store.neighbors(scope, "msg-3", before=2, after=1)
    ] == ["msg-1", "msg-3", "msg-4"]
    assert store.neighbors(Scope("other", "owner"), "msg-3") == []
    with pytest.raises(RawMessageError):
        store.replay(scope, "group-1", limit=-1)
    with pytest.raises(RawMessageError):
        store.neighbors(scope, "msg-3", before=100)


def test_export_reconcile_and_online_backup_are_complete_and_non_destructive(tmp_path):
    store, scope = opened(tmp_path)
    store.append(scope, draft())
    report = store.reconcile(scope)
    assert report["messages"] == 1
    assert report["issues"] == []
    assert (
        list(store.export_messages(scope, "group-1"))[0]["content"] == draft().content
    )
    result = store.backup_database(tmp_path / "backup.db")
    assert len(result["sha256"]) == 64
    assert RawMessageStore(tmp_path / "backup.db").get(scope, "msg-1") == store.get(
        scope, "msg-1"
    )
    with pytest.raises(FileExistsError):
        store.backup_database(tmp_path / "backup.db")
    assert store.reconcile(scope) == report


def test_selected_connector_and_conversation_scope_cannot_read_or_write_another_chat(
    tmp_path,
):
    store, scope = opened(tmp_path)
    store.append(scope, draft())
    store.register_conversation(
        scope, "group-2", "wx-main", "remote-2", "group", "另一个群"
    )
    selected = Scope("ailu", "owner", connector_id="wx-main", conversation_id="group-2")
    assert store.get(selected, "msg-1", include_inactive=True) is None
    assert store.replay(selected, "group-1") == []
    assert store.revisions(selected, "msg-1", include_inactive=True) == []
    assert store.message_events(selected, "msg-1") == []
    assert list(store.export_messages(selected)) == []
    assert store.reconcile(selected)["messages"] == 0
    assert [c["conversation_id"] for c in store.list_conversations(selected)] == [
        "group-2"
    ]
    with pytest.raises(ScopeError):
        store.append(selected, draft("wrong-chat"))
    with pytest.raises(ScopeError):
        store.forget(selected, "msg-1")


def test_many_identical_concurrent_submissions_commit_only_once(tmp_path):
    store, scope = opened(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        messages = list(
            pool.map(
                lambda _: store.append(scope, draft(idempotency_key="one-event")),
                range(32),
            )
        )
    assert all(message == messages[0] for message in messages)
    assert len(store.replay(scope, "group-1")) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"created_at": "2026-09-18T14:00:00"},
        {"role": "administrator"},
        {"content_type": "unknown"},
        {"session_id": ""},
        {"metadata": {"token": "secret"}},
        {"metadata": {"provider_sequence": []}},
        {"content": "\ud800"},
    ],
)
def test_invalid_payload_never_silently_accepts_or_persists(tmp_path, changes):
    store, scope = opened(tmp_path)
    with pytest.raises(RawMessageError):
        store.append(scope, replace(draft(), **changes))
    assert store.replay(scope, "group-1") == []


@pytest.mark.parametrize("future", [0, 1, 99])
def test_foreign_or_newer_database_is_rejected_without_modification(tmp_path, future):
    path = tmp_path / "foreign.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE unrelated(private_text TEXT)")
        connection.execute(f"PRAGMA user_version={future}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


def test_different_timezones_preserve_provider_time_without_reordering_received_messages(
    tmp_path,
):
    store, scope = opened(tmp_path)
    first = store.append(scope, draft(created_at="2026-09-18T14:00:00+08:00"))
    second = store.append(scope, draft("msg-2", created_at="2026-09-18T01:00:00-05:00"))
    assert [m.message_id for m in store.replay(scope, "group-1")] == [
        first.message_id,
        second.message_id,
    ]
    assert second.created_at.endswith("-05:00")


@pytest.mark.parametrize(
    "defect", ["table", "column", "unique", "foreign_key", "collation", "application"]
)
def test_same_version_database_requires_application_and_critical_schema(
    tmp_path, defect
):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_SQL

    schema = SCHEMA_SQL
    if defect == "column":
        schema = schema.replace("    source TEXT NOT NULL,\n", "")
    elif defect == "unique":
        schema = schema.replace(
            "UNIQUE(agent_id,user_id,connector_id,idempotency_key)", "CHECK(1)"
        )
    elif defect == "foreign_key":
        schema = schema.replace(
            "    FOREIGN KEY(reply_to_message_id) REFERENCES raw_messages(message_id),\n",
            "",
        )
    elif defect == "collation":
        schema = schema.replace(
            "message_id TEXT PRIMARY KEY,",
            "message_id TEXT COLLATE NOCASE PRIMARY KEY,",
        )
    path = tmp_path / "malformed.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(schema)
        connection.execute("PRAGMA user_version=1")
        connection.execute(
            f"PRAGMA application_id={APPLICATION_ID if defect != 'application' else 123}"
        )
        if defect == "table":
            connection.execute("DROP TABLE raw_message_events")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert not path.with_name(path.name + "-wal").exists()


def test_complete_legacy_v1_schema_is_recognized_without_rewriting_messages(tmp_path):
    store, scope = opened(tmp_path)
    original = store.append(scope, draft())
    with sqlite3.connect(store.database) as connection:
        connection.execute("PRAGMA application_id=0")
    reopened = RawMessageStore(store.database)
    assert reopened.get(scope, "msg-1") == original
    with sqlite3.connect(store.database) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == 0


def database_file_state(path):
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    return (
        hashlib.sha256(path.read_bytes()).hexdigest(),
        journal_mode,
        tuple(
            path.with_name(path.name + suffix).exists()
            for suffix in ("-wal", "-shm", "-journal")
        ),
    )


@pytest.mark.parametrize("generation", ["VIRTUAL", "STORED"])
def test_generated_column_database_is_rejected_before_any_file_modification(
    tmp_path, generation
):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_SQL

    schema = SCHEMA_SQL.replace(
        "    attachment_refs_json TEXT NOT NULL,\n",
        "    attachment_refs_json TEXT NOT NULL,\n"
        f"    unexpected TEXT GENERATED ALWAYS AS (content) {generation},\n",
    )
    path = tmp_path / "generated.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(schema)
        connection.execute("PRAGMA user_version=1")
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
    before = database_file_state(path)
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert database_file_state(path) == before


def test_extra_check_database_is_rejected_before_any_file_modification(tmp_path):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_SQL

    schema = SCHEMA_SQL.replace(
        "    content TEXT NOT NULL,\n",
        "    content TEXT NOT NULL CHECK(length(content)<=3),\n",
        1,
    )
    path = tmp_path / "extra-check.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(schema)
        connection.execute("PRAGMA user_version=1")
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
    before = database_file_state(path)
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert database_file_state(path) == before


@pytest.mark.parametrize(
    "hook_name", ["unexpected", "sqliteXunexpected", "sqlitex_hook", "SQLITEXhook"]
)
def test_unexpected_schema_hook_is_rejected_before_any_file_modification(
    tmp_path, hook_name
):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_SQL

    path = tmp_path / "extra-hook.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA_SQL)
        connection.execute("PRAGMA user_version=1")
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
        connection.executescript(
            f"CREATE TRIGGER {hook_name} BEFORE INSERT ON raw_messages "
            "BEGIN SELECT RAISE(ABORT, 'unexpected schema'); END;"
        )
    before = database_file_state(path)
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert database_file_state(path) == before


@pytest.mark.parametrize("object_kind", ["table", "view"])
@pytest.mark.parametrize("object_name", ["sqlitex_extra", "SQLITEXextra"])
def test_non_internal_sqlite_like_object_is_rejected_without_modification(
    tmp_path, object_kind, object_name
):
    from kernel.raw_message_schema import APPLICATION_ID, SCHEMA_SQL

    path = tmp_path / "extra-object.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA_SQL)
        connection.execute("PRAGMA user_version=1")
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
        statement = (
            f"CREATE TABLE {object_name}(value TEXT)"
            if object_kind == "table"
            else f"CREATE VIEW {object_name} AS SELECT message_id FROM raw_messages"
        )
        connection.execute(statement)
    before = database_file_state(path)
    with pytest.raises(RawMessageError):
        RawMessageStore(path)
    assert database_file_state(path) == before


@pytest.mark.parametrize(
    "selection", ["missing", "wrong_owner", "contradictory_connector"]
)
def test_registry_rejects_invalid_selected_conversation_without_mutating_connector(
    tmp_path, selection
):
    store, scope = opened(tmp_path)
    selected = replace(scope, conversation_id="missing")
    if selection == "wrong_owner":
        other = replace(scope, user_id="other")
        store.register_connector(other, "other", "wechat", "other-account", "other")
        store.register_conversation(
            other, "other-chat", "other", "provider", "group", "other"
        )
        selected = replace(scope, conversation_id="other-chat")
    elif selection == "contradictory_connector":
        selected = replace(scope, connector_id="other", conversation_id="group-1")
    with pytest.raises(ScopeError):
        store.register_connector(
            selected, "wx-main", "wechat", "account-1", "not-allowed"
        )
    assert store.list_connectors(scope)[0]["display_name"] == "微信"


def test_utc_z_timestamp_can_be_edited_without_changing_original_representation(
    tmp_path,
):
    store, scope = opened(tmp_path)
    store.append(scope, draft(created_at="2026-09-18T06:00:00Z"))
    message = store.edit(
        scope, "msg-1", "编辑", expected_revision=1, edited_at="2026-09-18T06:01:00Z"
    )
    assert message.created_at == "2026-09-18T06:00:00Z"
    assert message.edited_at == "2026-09-18T06:01:00Z"
