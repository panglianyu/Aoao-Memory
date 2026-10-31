"""SQLite DDL for the isolated raw-message database; no database is opened."""

SCHEMA_VERSION = 4
APPLICATION_ID = 0x414F524D  # AORM: only new databases receive this marker.

SCHEMA_V1_SQL = """
CREATE TABLE connectors (
    connector_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    account_ref TEXT NOT NULL,
    display_name TEXT NOT NULL,
    UNIQUE(agent_id,user_id,kind,account_ref),
    UNIQUE(connector_id,agent_id,user_id)
);
CREATE TABLE conversations (
    conversation_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    provider_conversation_id TEXT NOT NULL,
    conversation_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(connector_id,provider_conversation_id),
    UNIQUE(conversation_id,agent_id,user_id),
    FOREIGN KEY(connector_id,agent_id,user_id)
        REFERENCES connectors(connector_id,agent_id,user_id)
);
CREATE TABLE raw_messages (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    content_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    edited_at TEXT,
    source TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    revision INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT NOT NULL,
    reply_to_message_id TEXT,
    metadata_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT,
    payload_hash TEXT NOT NULL,
    attachment_refs_json TEXT NOT NULL,
    FOREIGN KEY(conversation_id,agent_id,user_id)
        REFERENCES conversations(conversation_id,agent_id,user_id),
    FOREIGN KEY(reply_to_message_id) REFERENCES raw_messages(message_id),
    UNIQUE(conversation_id,ordinal),
    UNIQUE(agent_id,user_id,connector_id,idempotency_key)
);
CREATE INDEX raw_replay ON raw_messages(agent_id,user_id,conversation_id,session_id,ordinal);
CREATE TABLE attachments (
    attachment_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    content_type TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL
);
CREATE TABLE derivation_outbox (
    task_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    revision INTEGER NOT NULL,
    processor TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    target_domain TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at REAL NOT NULL DEFAULT 0,
    lease_until REAL,
    lease_owner TEXT,
    lease_token TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(message_id,revision,processor,processor_version,target_domain)
);
CREATE INDEX outbox_ready ON derivation_outbox(status,available_at,lease_until);
CREATE TABLE derivation_results (
    task_id TEXT NOT NULL REFERENCES derivation_outbox(task_id),
    memory_id TEXT NOT NULL,
    store_ref TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    PRIMARY KEY(task_id,memory_id)
);
CREATE TABLE derivation_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES derivation_outbox(task_id),
    action TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    error_code TEXT
);
CREATE TABLE raw_message_revisions (
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    revision INTEGER NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    edited_at TEXT,
    PRIMARY KEY(message_id,revision)
);
CREATE TABLE raw_message_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    action TEXT NOT NULL,
    revision INTEGER NOT NULL,
    old_status TEXT,
    new_status TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
"""

# External-content index: Raw remains the only original-text authority. Writes to
# the index are made in the same RawStore transaction as the corresponding message.
SEARCH_INDEX_SQL = """
CREATE VIRTUAL TABLE raw_search USING fts5(
    content, content='raw_messages', content_rowid='rowid', tokenize='trigram'
);
"""

PURGE_RECEIPT_SQL = """
CREATE TABLE purge_receipts (
    receipt_key TEXT PRIMARY KEY,
    purged_at TEXT NOT NULL,
    result_count INTEGER NOT NULL
);
"""

SCHEMA_V2_SQL = SCHEMA_V1_SQL + SEARCH_INDEX_SQL + PURGE_RECEIPT_SQL

# A conversation-local counter is advanced by SQLite in the same transaction as
# every mutation which can change the current derivation proof. It is never reset.
PARTITION_EPOCH_SQL = """
CREATE TABLE raw_partition_epochs (
    conversation_id TEXT PRIMARY KEY REFERENCES conversations(conversation_id),
    generation INTEGER NOT NULL CHECK(generation>=0)
);
CREATE TRIGGER raw_epoch_message_insert AFTER INSERT ON raw_messages BEGIN
    INSERT OR IGNORE INTO raw_partition_epochs(conversation_id,generation)
        VALUES(NEW.conversation_id,0);
END;
CREATE TRIGGER raw_epoch_outbox_insert AFTER INSERT ON derivation_outbox BEGIN
    UPDATE raw_partition_epochs SET generation=generation+1
        WHERE conversation_id=(SELECT conversation_id FROM raw_messages
                               WHERE message_id=NEW.message_id);
END;
CREATE TRIGGER raw_epoch_outbox_update AFTER UPDATE ON derivation_outbox BEGIN
    UPDATE raw_partition_epochs SET generation=generation+1
        WHERE conversation_id=(SELECT conversation_id FROM raw_messages
                               WHERE message_id=NEW.message_id);
END;
CREATE TRIGGER raw_epoch_outbox_delete AFTER DELETE ON derivation_outbox BEGIN
    UPDATE raw_partition_epochs SET generation=generation+1
        WHERE conversation_id=(SELECT conversation_id FROM raw_messages
                               WHERE message_id=OLD.message_id);
END;
"""

SCHEMA_V3_SQL = SCHEMA_V2_SQL + PARTITION_EPOCH_SQL

DERIVATION_EVENT_INDEX_SQL = """
CREATE INDEX derivation_event_task_action ON derivation_events(task_id,action);
"""

SCHEMA_SQL = SCHEMA_V3_SQL + DERIVATION_EVENT_INDEX_SQL
