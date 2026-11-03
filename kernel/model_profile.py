"""Small provider policy shared by foreground and background LLM callers."""

from urllib.parse import urlsplit


def is_deepseek(model_name: str) -> bool:
    return isinstance(model_name, str) and model_name.lower().startswith("deepseek-")


def configured_model(config: dict, *, reviewer: bool = False) -> str:
    """Reject incomplete/mixed DeepSeek settings without printing credentials."""
    primary_value = config.get("model_name")
    if not isinstance(primary_value, str) or not primary_value.strip():
        raise ValueError("Model configuration is missing model_name")
    primary = primary_value.strip()
    if is_deepseek(primary):
        primary = primary.lower()
    reviewer_value = config.get("reviewer_model_name")
    model_value = (reviewer_value or primary) if reviewer else primary
    if not isinstance(model_value, str) or not model_value.strip():
        raise ValueError("Model configuration is missing model_name")
    model = model_value.strip()
    if is_deepseek(model):
        model = model.lower()
    endpoint = urlsplit(str(config.get("base_url") or ""))
    api_key = config.get("api_key")
    if is_deepseek(primary) or is_deepseek(model) or endpoint.hostname == "api.deepseek.com":
        if (
            not is_deepseek(primary) or model != primary
            or endpoint.scheme != "https" or endpoint.hostname != "api.deepseek.com"
            or endpoint.port not in (None, 443)
            or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment
            or endpoint.path.rstrip("/") not in ("", "/v1")
            or not isinstance(api_key, str) or not api_key.strip()
        ):
            raise ValueError("DeepSeek requires matching official endpoint, model and credentials")
    return model


def request_options(model_name: str, *, thinking_mode: str = "disabled") -> dict:
    """Return provider parameters, never a second model or tool executor."""
    if thinking_mode not in ("enabled", "disabled"):
        raise ValueError("Invalid thinking mode")
    if is_deepseek(model_name):
        return {"extra_body": {"thinking": {"type": thinking_mode}}}
    return {}


def mode_groups(messages: list, *, stable: bool = False) -> list[tuple[str, list]]:
    """Split contiguous mode segments inside each existing source/chat bucket."""
    buckets: dict[tuple, list[list]] = {}
    for message in messages:
        metadata = message.metadata or {}
        mode = metadata.get("thinking_mode", "enabled")
        if mode not in ("enabled", "disabled"):
            raise ValueError("Invalid thinking mode")
        chat = str(metadata.get("conversation_id") or metadata.get("chat_name", "")) if stable else str(metadata.get("chat_name", ""))
        segments = buckets.setdefault((message.source, chat), [])
        if not segments or (segments[-1][0].metadata or {}).get("thinking_mode", "enabled") != mode:
            segments.append([])
        segments[-1].append(message)
    return [(chat, segment) for (_, chat), segments in buckets.items() for segment in segments]
