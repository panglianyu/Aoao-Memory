"""有界 JSON 信封和状态常量；业务 payload 由独立闭集协议验证。"""

import json

from kernel.wechat_rpc.tls import WeChatTransportError


MAX_WIRE_BYTES = 256 * 1024
MAX_HEADER_BYTES = 8192


def transport_status() -> dict:
    return {
        "state": "transport_only",
        "transport_ready": True,
        "wechat_ready": False,
        "execution_enabled": False,
    }


def encode_message(payload: dict) -> bytes:
    data = json.dumps(
        payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    if len(data) > MAX_WIRE_BYTES:
        raise WeChatTransportError("RPC 信封过大")
    return data


def _unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError


def decode_message(data: bytes) -> dict:
    if not 0 < len(data) <= MAX_WIRE_BYTES:
        raise WeChatTransportError("RPC 信封大小非法")
    try:
        value = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, RecursionError):
        raise WeChatTransportError("RPC 信封不是有效 JSON 对象") from None
