"""Host 只读画面校验；不保存图片，调用方应在线程中执行并等候清理。"""

import base64
import json
import re
import time

from kernel.tool_result import ToolResult
from kernel.wechat_artifact_content import prepare_artifact
from protocols.wechat_artifacts import MAX_ARTIFACT_BYTES
from protocols.wechat_preview import PREVIEW_TTL_SECONDS, PreviewFrame


def project_preview(result: ToolResult) -> dict:
    try:
        if (
            type(result) is not ToolResult or result.app != "wechat" or result.ok is not True
            or result.kind != "text" or len(result.text) > 4 * ((MAX_ARTIFACT_BYTES + 2) // 3) + 2048
        ):
            raise ValueError
        payload = json.loads(result.text)
        if type(payload) is not dict or set(payload) not in ({"frame", "data_b64"}, {"frame", "data_b64", "click_token"}):
            raise ValueError
        token = payload.get("click_token")
        if token is not None and (type(token) is not str or not re.fullmatch(r"wxclick_[0-9a-f]{32}", token)):
            raise ValueError
        frame = PreviewFrame.from_payload(payload["frame"])
        frame.require_fresh()
        encoded = payload["data_b64"]
        if type(encoded) is not str or len(encoded) != 4 * ((frame.artifact.size_bytes + 2) // 3):
            raise ValueError
        raw = base64.b64decode(encoded, validate=True)
        if base64.b64encode(raw).decode("ascii") != encoded:
            raise ValueError
        image = prepare_artifact(frame.artifact, raw)
        if image.content_type != "image/png":
            raise ValueError
        frame.require_fresh()
        return {
            "schema_version": 1, "published_at": frame.published_at,
            "expires_at": min(frame.published_at, time.time()) + PREVIEW_TTL_SECONDS,
            "width": image.width, "height": image.height,
            "data_url": "data:image/png;base64," + base64.b64encode(image.data).decode("ascii"),
            "execution_enabled": False,
            "manual_click_available": token is not None, "click_token": token,
        }
    except Exception:
        raise ValueError("画面未通过有效性校验") from None
