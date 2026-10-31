"""Classify persisted Feishu originals using the same text rule as ingress."""

import json
import re


def extract_feishu_business_text(
        provider_type: str, content: str, *, provider_limited: bool = False,
        provider_timestamp_limited: bool = False) -> str | None:
    if (provider_limited or provider_timestamp_limited
            or provider_type not in {"text", "post", "image"}
            or not isinstance(content, str)):
        return None
    try:
        decoded = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return None
    if provider_type == "text":
        value = decoded.get("text") if isinstance(decoded, dict) else None
        return value if isinstance(value, str) and value.strip() else None
    found = re.findall(r'"text"\s*:\s*"([^"]+)"', content)
    value = " ".join(part.replace("\\n", "\n") for part in found).strip()
    return value or None
