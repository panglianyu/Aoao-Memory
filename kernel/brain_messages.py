# kernel/brain_messages.py
"""[0.3.18.0 Brain 收缩 Phase 1] 消息构造与工具返回处理纯函数层。

从 brain.py 上帝类拆出的无状态纯函数 (方案: .claude/plans/brain-god-class-
shrink.plan.md): 消息构造 / 参数契约 / 截断防爆 / 错误判定。Brain 上保留
同名薄委托 (测试 monkeypatch 点零漂移), 行为逐字不变。

依赖纪律: 只 import kernel 底层 (logger/tool_result), 不反引 brain。
"""
import base64
import json
import os
from typing import Optional

from kernel.logger import get_logger
from kernel.tool_result import ToolResult

logger = get_logger(__name__)


def coerce_params(raw_params) -> dict:
    """
    [契约解毒] 取代旧的 _parse_tool_params 三级降级兜底。
    工具声明层已把 params 改为原生 object schema, 模型返回的应是 Python dict。
    此处仅做最小防御: dict 直通; 字符串走一次 JSON 解析 (兼容个别仍输出字符串的网关)。
    彻底删除 key=value 拆分与纯文本兜底这类历史妥协。
    """
    if isinstance(raw_params, dict):
        return raw_params
    if isinstance(raw_params, str):
        if not raw_params:
            return {}
        try:
            parsed = json.loads(raw_params)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        return {}
    return {}


def truncate_result(res_str: str, max_chars: int) -> str:
    """[防爆] 工具返回值软上限: 超过 max_chars 头尾各留 1000, 中间省略。"""
    if not res_str or len(res_str) <= max_chars:
        return res_str
    head = res_str[:1000]
    tail = res_str[-1000:]
    omitted = len(res_str) - 2000
    return f"{head}\n...[因防爆策略，底层已省略 {omitted} 字]...\n{tail}"


def parse_interest_list(text: str) -> list:
    """[0.2.9.0] 解析 list_interests 返回 '阿露兴趣 topN: 话题(值), ...' 的话题名。"""
    if not text or text.startswith("暂无兴趣"):
        return []
    body = text.split(":", 1)[-1] if ":" in text else text
    kws = []
    for part in body.split(","):
        topic = part.split("(")[0].strip()
        if topic:
            kws.append(topic)
    return kws


def extract_data_url_from_image_data(data: dict):
    """
    从图像类 ToolResult.data (原始 dict) 提取 data_url 供多模态注入。

    情况 1: 内联 data URL (native_controller 截屏: data["image"])。
    情况 2: 本地文件路径 (image_generator 生成图: data["file_path"])。

    Returns:
        (data_url: str | None)
    """
    if not isinstance(data, dict):
        return None
    # 情况 1: 内联 data URL
    image_url = data.get("image", "")
    if image_url and isinstance(image_url, str) and image_url.startswith("data:image/"):
        return image_url
    # 情况 2: 本地文件路径
    file_path = data.get("file_path", "")
    if file_path and os.path.exists(file_path):
        try:
            ext = os.path.splitext(file_path)[1].lower()
            mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                       ".gif": "image/gif", ".webp": "image/webp",
                       ".bmp": "image/bmp", ".png": "image/png"}
            mime = mime_map.get(ext, "image/png")
            with open(file_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            return f"data:{mime};base64,{b64}"
        except Exception as e:
            logger.warning(f"[Brain] 读取图像文件失败 {file_path}: {e}")
    return None


def build_tool_message(
    tool_call_id: str, result_str: str, tool_result: Optional[ToolResult] = None
):
    """
    构建工具返回消息。若 ToolResult.kind=="image" 则组装多模态 Content Array,
    让 LLM 直接"看见"截图/生成图而非仅看到文本路径。

    优先用结构化 tool_result.data 提取 data_url (取代旧的字符串劈裂 + json.loads 探测)。
    无 ToolResult 时降级用 result_str 文本。

    OpenAI 多模态格式:
      [
        {"type": "text", "text": "..."},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}}
      ]
    """
    data_url = None
    if tool_result is not None and tool_result.kind == "image":
        data_url = extract_data_url_from_image_data(tool_result.data)

    if data_url:
        text_summary = tool_result.text if tool_result else "image payload injected below."
        content = [
            {"type": "text", "text": text_summary},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]
        return {"role": "tool", "tool_call_id": tool_call_id, "content": content}

    # 普通文本工具结果
    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_str}


def build_user_message(user_input: str, media_payloads: list = None):
    """
    构建用户消息。如果携带图片等多模态载荷，组装为 Content Array 格式，
    让 LLM 能直接 "看见" 图片而非仅看到文本路径。

    Args:
        user_input: 用户文本输入
        media_payloads: [{"type": "image_url", "image_url": {"url": "data:..."}}]

    Returns:
        {"role": "user", "content": str | list}
    """
    if not media_payloads:
        return {"role": "user", "content": user_input}

    content = [{"type": "text", "text": user_input or "(image uploaded)"}]
    for p in media_payloads:
        if isinstance(p, dict) and p.get("type") == "image_url":
            content.append(p)
    return {"role": "user", "content": content}


def is_outbound_denial(res_str: str) -> bool:
    """[0.2.20.0] 判定工具返回是否为发送拦截通知 (P0-b/熔断拒发, 文本并未真发)。

    三处拦截措辞特征: P0-b "禁止重复发送" / 同 target 熔断 "请勿重复发送" /
    跨对话上限 "本轮已发送...再发"。供出站收集侧排除未遂发送, 治被拦复读
    污染 chat_history (下轮 LLM 误以为自己发了) 与 behavior_log (谎报出站)。
    """
    if not res_str:
        return False
    return ("禁止重复发送" in res_str or "请勿重复发送" in res_str
            or ("本轮已发送" in res_str and "再发" in res_str))


def is_tool_error(result_str: str) -> bool:
    """检测工具返回是否包含错误信息"""
    error_keywords = [
        "失败", "错误", "error", "exception", "traceback",
        "not found", "找不到", "不存在", "无法", "cannot",
        "invalid", "无效", "格式不正确", "参数错误",
        "timeout", "超时", "连接失败", "connection failed"
    ]
    result_lower = result_str.lower()
    return any(kw.lower() in result_lower for kw in error_keywords)


def build_error_digest_hint(func_name: str, args: dict, error_msg: str) -> str:
    """构建错误消化提示，引导大模型分析错误并自然解释"""
    # [0.3.28.0] 渠道离线 (老板手动断开 app) 专用分支: 旧提示引导模型"分析失败原因",
    # 模型会自调 connect_app 重连, 与老板的断开操作对抗 (2026-08-08 微信循环事故)。
    # 离线是老板的权威决定, 只有老板能重连, 消化提示必须明确禁止。
    if "尚未启动" in error_msg:
        return (
            f"<system_error_digest>\n"
            f"[渠道离线提示]\n"
            f"工具名: {func_name}\n"
            f"目标 app 当前离线 (已被老板断开)。严禁调用 connect_app 尝试重连 —— "
            f"断开是老板的决定, 只有老板有权重新连接。\n"
            f"请简短告知老板: 该渠道已离线, 这条消息/任务暂时无法送达, "
            f"需要的话请在控制台手动重新连接。不要再重试该工具。\n"
            f"</system_error_digest>"
        )
    return (
        f"<system_error_digest>\n"
        f"[工具调用失败诊断任务]\n"
        f"工具名: {func_name}\n"
        f"参数: {json.dumps(args, ensure_ascii=False)}\n"
        f"错误信息: {error_msg}\n\n"
        f"请在 <think> 区内分析：\n"
        f"1. 这个错误是因为参数传错了？还是根本不该调用这个工具？\n"
        f"2. 用户的原始意图是什么？是否被你误解了？\n"
        f"3. 如何用自然语言向用户解释发生了什么？\n\n"
        f"要求：\n"
        f"- 承认错误（如果是你的判断失误）\n"
        f"- 提供替代方案或询问用户下一步\n"
        f"- 绝不允许把原始错误信息直接扔给用户\n"
        f"- 绝不允许装作什么都没发生\n"
        f"</system_error_digest>"
    )
