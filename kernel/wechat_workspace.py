"""将只读 VM 预检投影为有限 UI 状态，不转发配置、原始错误或执行能力。"""

import time

from kernel.tool_result import ToolResult
from kernel.wechat_rpc.wire import decode_message
from protocols.wechat_listening_status import parse_listening_status


def project_inspection(result: ToolResult) -> dict:
    """仅接受当前离线预检协议；协议升级必须显式复审，不能自动开放微信。"""
    if not isinstance(result, ToolResult) or result.app != "wechat_vm":
        raise ValueError("预检结果不可用")
    # 当前 AppBus 的普通结构化结果保存在 text，失败摘要不保留原结构。
    # 不解析失败摘要或猜测其中的状态；让 UI 显示检查失败并撤下旧报告。
    if result.ok is not True or result.kind != "text" or len(result.text) > 16384:
        raise ValueError("预检结果不可用")
    data = decode_message(result.text.encode("utf-8"))
    stages = {"windows": "not_verified", "mtls": "not_paired", "wechat": "not_verified"}
    if not isinstance(data, dict) or data.get("stages") != stages:
        raise ValueError("预检结果不可用")
    safe = data.get("template_safe")
    issues = data.get("issues")
    state = data.get("vm_state")
    if (
        type(safe) is not bool
        or data.get("wechat_ready") is not False
        or not isinstance(issues, list)
        or len(issues) > 128
        or any(not isinstance(item, str) or len(item) > 128 for item in issues)
        or safe != (len(issues) == 0)
        or data.get("status") != ("success" if safe else "error")
        or result.ok is not safe
        or not isinstance(state, str)
        or len(state) > 64
        or (safe and state != "poweroff")
    ):
        raise ValueError("预检结果不可用")
    return {
        "schema_version": 1,
        "checked_at": time.time(),
        "vm_state": state
        if state in {"poweroff", "running", "paused", "saved", "aborted"}
        else "unknown",
        "template_safe": safe,
        "issue_count": len(issues),
        "stages": stages,
        "wechat_ready": False,
        "execution_enabled": False,
        "preview_available": False,
    }


def project_listening_status(result: ToolResult) -> dict:
    """只投影汇总诊断；不向 UI 传目标键、名称或把本机 read_enabled 当权限。"""
    if (
        not isinstance(result, ToolResult) or result.app != "wechat"
        or result.ok is not True or result.kind != "text" or len(result.text) > 32768
    ):
        raise ValueError("监听状态结果不可用")
    data = parse_listening_status(decode_message(result.text.encode("utf-8")))
    return {
        "schema_version": 1, "checked_at": time.time(), "state": data["state"],
        "configuration_applied": data["configuration_applied"],
        "configured_target_count": len(data["targets"]),
        "applied_target_count": sum(row["applied"] for row in data["targets"]),
        "failed_target_count": sum(
            row["source_state"] == "failed" for row in data["targets"]
        ),
        "accepted_count": data["accepted_count"], "outcome_unknown": data["outcome_unknown"],
        "failure_phase": data["failure_phase"],
        "wechat_ready": False, "execution_enabled": False, "preview_available": False,
    }
