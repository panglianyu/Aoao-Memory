"""Trusted local opt-in settings. Missing settings retain legacy behavior."""

from dataclasses import dataclass
from pathlib import Path

import yaml

from kernel.raw_message_models import DerivationSpec
from kernel.raw_message_profile import ConnectorBinding, ConversationBinding, RawMessageProfile


@dataclass(frozen=True)
class LegacySourceBinding:
    agent_id: str
    owner_id: str
    connector_id: str
    provider_id: str
    legacy_chat: str
    memory_root: Path


@dataclass(frozen=True)
class DailyMemorySettings:
    database: Path
    derived_root: Path
    owner_id: str
    connectors: tuple[ConnectorBinding, ...]
    legacy_sources: tuple[LegacySourceBinding, ...] = ()

    def __post_init__(self):
        seen = set()
        for mapping in self.legacy_sources:
            connector = next((item for item in self.connectors if item.agent_id == mapping.agent_id
                and item.connector_id == mapping.connector_id and item.source in {"web", "console"}), None)
            chat = next((item for item in connector.conversations if item.provider_id == mapping.provider_id), None) if connector else None
            key = (mapping.agent_id, mapping.connector_id, mapping.provider_id)
            if (mapping.owner_id != self.owner_id or chat is None or chat.kind == "group" or key in seen
                    or not mapping.memory_root.is_absolute() or not isinstance(mapping.legacy_chat, str)
                    or not mapping.legacy_chat.strip() or len(mapping.legacy_chat) > 256):
                raise ValueError("invalid daily legacy mapping")
            seen.add(key)

    def make_profile(self, derivations: tuple[DerivationSpec, ...]) -> RawMessageProfile:
        return RawMessageProfile(self.database, self.owner_id, self.connectors,
                                 derivations, self.derived_root)


class _SettingsLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if type(key) is not str or key in mapping:
                raise ValueError("ambiguous daily memory settings")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _fields(value: object, required: set[str], optional: set[str] | None = None) -> dict:
    if type(value) is not dict or not required.issubset(value) or set(value) - required - (optional or set()):
        raise ValueError("invalid daily memory settings fields")
    return value


def _connector(value: object) -> ConnectorBinding:
    item = _fields(value, {"agent_id", "source", "connector_id", "account_ref", "conversations"})
    if type(item["conversations"]) is not list:
        raise ValueError("invalid daily conversation settings")
    conversations = []
    for value in item["conversations"]:
        chat = _fields(value, {"local_key", "provider_id", "kind", "display_name"}, {"proactive_send"})
        conversations.append(ConversationBinding(**chat))
    return ConnectorBinding(item["agent_id"], item["source"], item["connector_id"],
                             item["account_ref"], tuple(conversations))


def load_daily_memory_settings(path: Path | None = None) -> DailyMemorySettings | None:
    selected = path if path is not None else Path(__file__).resolve().parents[1] / "config" / "daily_memory.yaml"
    try:
        with selected.open("rb") as source:
            body = source.read(65537)
    except FileNotFoundError:
        return None
    if len(body) > 65536:
        raise ValueError("daily memory settings too large")
    try:
        data = _fields(yaml.load(body, Loader=_SettingsLoader), {"schema", "enabled"},
                       {"database", "derived_root", "owner_id", "connectors", "legacy_sources"})
        if type(data["schema"]) is not int or data["schema"] != 1 or type(data["enabled"]) is not bool:
            raise ValueError("invalid daily settings version")
        if not data["enabled"]:
            return None
        _fields(data, {"schema", "enabled", "database", "derived_root", "owner_id", "connectors"}, {"legacy_sources"})
        if type(data["connectors"]) is not list:
            raise ValueError("invalid daily connector settings")
        legacy = data.get("legacy_sources", [])
        if type(legacy) is not list:
            raise ValueError("invalid daily legacy sources")
        mappings = []
        for value in legacy:
            item = _fields(value, {"agent_id", "owner_id", "connector_id", "provider_id", "legacy_chat", "memory_root"})
            mappings.append(LegacySourceBinding(**{**item, "memory_root": Path(item["memory_root"])}))
        settings = DailyMemorySettings(Path(data["database"]), Path(data["derived_root"]),
            data["owner_id"], tuple(_connector(item) for item in data["connectors"]), tuple(mappings))
        settings.make_profile((DerivationSpec("settings-validation", "v1", "vector"),))
        return settings
    except (ValueError, TypeError, KeyError, yaml.YAMLError, RecursionError):
        # No pasted configuration, credentials, identifiers or YAML excerpts in errors.
        raise ValueError("invalid daily memory settings") from None


def resolve_daily_memory(settings: DailyMemorySettings, bus, brain):
    """Resolve the already discovered provider; no plugin imports or cloud client."""
    if type(settings) is not DailyMemorySettings:
        raise ValueError("explicit daily memory settings required")
    if getattr(brain, "_active_agent_id", None) not in {item.agent_id for item in settings.connectors}:
        raise ValueError("daily memory active agent not configured")
    name = bus.find_memory_provider()
    provider = bus.available_apps.get(name)
    build = getattr(provider, "create_daily_memory", None)
    if not callable(build):
        raise ValueError("daily memory provider assembly unavailable")
    profile, factory = build(settings, client=brain.client, model=brain.model_name)
    if type(profile) is not RawMessageProfile or not callable(factory):
        raise ValueError("invalid daily memory provider assembly")
    return profile, factory


def daily_boot_options() -> dict:
    """Shared console/Web opt-in; the absent configuration leaves boot() unchanged."""
    settings = load_daily_memory_settings()
    return {"daily_settings": settings} if settings is not None else {}
