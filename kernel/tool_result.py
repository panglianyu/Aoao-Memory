# G:\Aoao_OS\kernel\tool_result.py
"""
ToolResult -- 指令执行的结构化返回值。

取代旧的裸字符串 "[{name} 返回情报]: {result}" 协议。
app_bus.send_instruction 的统一产物, 供 Brain / 主循环 / web_ui 消费。

设计要点:
  - 冻结数据类 (frozen): 返回值一旦产生即不可篡改, 保证传递过程数据完整。
  - __str__ 兼容: 仍输出旧前缀字符串, 依赖 str(result) / 子串匹配的旧调用点零改动。
  - 三态语义: ok (成功与否, 熔断器据此判定) / kind (text|image|error, 多模态注入据此判定) / data (结构化负载)。

绝不允许消费点对 ToolResult 实例直接调字符串方法 (.strip/.lower); 必须走 .text/.ok/.kind。
"""
import json
import re
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class ToolResult:
    """指令执行的结构化返回值。app_bus.send_instruction 的统一产物。

    Attributes:
        app:   来源 app 名 (用于 __str__ 兼容前缀)。
        ok:    是否成功。熔断器据此判定, 取代旧的中文关键词匹配。
        kind:  "text" | "image" | "error"。
               text  -> 普通文本结果。
               image -> 含图像资产 (截图 data_url / 生成图文件路径), Brain 据此做多模态注入。
               error -> 执行失败 (含超时 / 崩溃 / 未知命令)。
        data:  结构化负载 (dict / None)。image 类保留原始 dict 供 Brain 提取 data_url/file_path。
        text:  给 LLM / 人看的纯文本摘要 (已剥离旧前缀)。
    """

    app: str
    ok: bool
    kind: str
    data: Optional[Any]
    text: str

    def __str__(self) -> str:
        """向后兼容: 旧调用点 str(result) 仍拿到 '[app 返回情报]: text'。"""
        return f"[{self.app} 返回情报]: {self.text}"

    # ==========================================
    # 字符串透明代理 (向后兼容 shim, Gate B 迁移后可逐步移除)
    # ==========================================
    # 旧消费点对 send_instruction 返回值直接用 `in` / `.split()` / `.replace()` /
    # `.strip()` 等字符串操作。本数据类返回 str→ToolResult 后, 为避免 AttributeError/TypeError,
    # 在此把字符串操作委托到 __str__() 产出的字符串形态 (即旧前缀字符串)。
    # 新代码必须走 .ok/.kind/.data/.text, 不得依赖这些 shim。

    def __contains__(self, item: Any) -> bool:
        return item in str(self)

    def __iter__(self):
        return iter(str(self))

    def __len__(self) -> int:
        return len(str(self))

    def __getattr__(self, name: str) -> Any:
        """
        未定义属性的兜底: 委托到字符串方法 (.split/.replace/.strip/.lower/.startswith/...)。
        仅对字符串方法生效; 真实字段已在 dataclass 中定义, 不会走到这里。
        """
        # 避开 dataclass/内部属性以及魔法/拷贝机制可能触发的名字
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        str_method = getattr(str(self), name, None)
        if str_method is None or not callable(str_method):
            raise AttributeError(name)
        return str_method

    # ==========================================
    # 工厂方法
    # ==========================================

    @classmethod
    def error(cls, app: str, text: str, data: Optional[Any] = None) -> "ToolResult":
        """构造失败结果 (指令失败 / 超时 / 崩溃 / 未启动 / 未知命令)。"""
        return cls(app=app, ok=False, kind="error", data=data, text=text)

    @classmethod
    def from_raw(cls, app: str, raw: Any) -> "ToolResult":
        """
        从 app.execute_command 的原始返回 (可能是 str / json-str / dict) 启发式包装。

        集中处理当前 app 返回的三派形态 (裸串 / JSON 串 / dict):
          1. dict 或可解析 JSON dict -> 看 type/status/success/error 字段定 kind 与 ok。
          2. 图像类 (type=="image" 且有 image/file_path) -> kind="image", data 保留原始 dict。
          3. 错误类 (status=="error" 或有 error 字段) -> ok=False。
          4. 无法解析 / 裸字符串 -> kind="text", ok=True, text=原值。

        Args:
            app: 来源 app 名。
            raw: execute_command 的原始返回值。

        Returns:
            结构化 ToolResult。
        """
        # 显式回执保留类型和结构化数据，不重新猜测错误状态。
        if isinstance(raw, cls):
            if (raw.app != app or type(raw.ok) is not bool
                    or not isinstance(raw.kind, str)
                    or raw.kind not in {"text", "image", "error"}
                    or not isinstance(raw.text, str)
                    or (raw.data is not None and not isinstance(raw.data, dict))
                    or (raw.kind == "image" and not isinstance(raw.data, dict))
                    or (raw.kind == "error" and raw.ok)):
                return cls.error(app, "无效的工具回执")
            return raw
        # 归一化为 dict: 接受 dict 直通, str 尝试 JSON 解析, 其他类型 str() 化
        payload: Optional[dict] = None
        raw_text = raw

        if isinstance(raw, dict):
            payload = raw
            raw_text = json.dumps(raw, ensure_ascii=False)
        elif isinstance(raw, str):
            payload = cls._try_parse_json_dict(raw)
            raw_text = raw
        else:
            # 非 dict / 非 str (int/bool/None 等) -> 纯文本, 不可结构化
            raw_text = "" if raw is None else str(raw)
            payload = None

        if payload is None:
            # 裸文本路径: 默认成功
            return cls(app=app, ok=True, kind="text", data=None, text=str(raw_text))

        # 结构化路径: 据字段判定 kind / ok
        kind = cls._classify_kind(payload)
        ok = cls._classify_ok(payload)

        # 图像类 data 保留原始 dict, 供 Brain 提取 data_url / file_path
        data = payload if kind == "image" else None
        text = cls._summarize(payload, kind, ok)
        # 工作区截图/正文的摘要也必须保留后续操作句柄，不能只把编号留在 data 中。
        # 仅投影受限的逻辑 ID；原始路径、图片字节和其他内部字段仍不进入摘要。
        if ok and kind != "error" and (
            kind == "image"
            or isinstance(payload.get("message"), str)
            or isinstance(payload.get("text"), str)
        ):
            context = cls._workspace_context(payload)
            if context:
                text = "[Workspace Context] " + json.dumps(context) + "\n" + text

        return cls(app=app, ok=ok, kind=kind, data=data, text=text)

    # ==========================================
    # 内部启发式
    # ==========================================

    @staticmethod
    def _workspace_context(payload: dict) -> dict[str, str]:
        context = {}
        for prefix in ("workspace", "session", "tab"):
            key = prefix + "_id"
            value = payload.get(key)
            if isinstance(value, str) and re.fullmatch(
                prefix + r"_(?:[0-9a-f]{16}|[0-9a-f]{32})", value
            ):
                context[key] = value
        # 只有显式工作区结果采用此投影，不改变普通图片/应用会话摘要。
        return context if "workspace_id" in context else {}

    @staticmethod
    def _try_parse_json_dict(s: str) -> Optional[dict]:
        """尝试把字符串解析为 dict; 失败或非 dict 返回 None。"""
        if not s:
            return None
        stripped = s.strip()
        # 快速排除: 明显不是 JSON 对象的开头直接放弃, 避免 json.loads 兜底误判
        if not (stripped.startswith("{") or stripped.startswith("[")):
            return None
        try:
            parsed = json.loads(stripped)
        except (json.JSONDecodeError, TypeError):
            return None
        # 容忍 {"返回情报": {...}} 形式的旧包装外层 (理论上 execute_command 不应自带前缀, 防御)
        if isinstance(parsed, dict):
            return parsed
        return None

    @staticmethod
    def _classify_kind(payload: dict) -> str:
        """据 payload 字段判定 kind: error / image / text。

        优先级: error > image > text。
        真实 image_generator 的错误返回也带 type="image", 故 status=="error" 必须先判。
        """
        # 错误标记 (最高优先级)
        status = str(payload.get("status", "")).lower()
        if status == "error" or "error" in payload:
            return "error"
        # 显式图像标记
        type_field = str(payload.get("type", "")).lower()
        if type_field == "image":
            return "image"
        return "text"

    @staticmethod
    def _classify_ok(payload: dict) -> bool:
        """据 payload 字段判定 ok: status/success/error 三信号。"""
        status = str(payload.get("status", "")).lower()
        if status == "error":
            return False
        if status == "success":
            return True
        if "error" in payload and payload.get("error"):
            return False
        success_flag = payload.get("success")
        if isinstance(success_flag, bool):
            return success_flag
        return True

    @staticmethod
    def _summarize(payload: dict, kind: str, ok: bool) -> str:
        """把结构化 payload 摘要为给 LLM 看的纯文本 (剥离旧前缀)。"""
        if kind == "error":
            err_msg = payload.get("error") or payload.get("message") or "执行失败"
            return f"失败: {err_msg}"
        if kind == "image":
            # 图像类: 给精简摘要, 真正图像负载由 Brain 从 data 提取注入多模态
            status_text = "success" if ok else "failed"
            resolution = payload.get("resolution")
            file_path = payload.get("file_path", "")
            model_used = payload.get("model_used", "")
            if resolution and isinstance(resolution, dict):
                w = resolution.get("width", "?")
                h = resolution.get("height", "?")
                return f"[Screenshot] {status_text}, resolution: {w}x{h}, image payload injected below."
            if file_path:
                import os
                return (
                    f"[Generated Image] {status_text}, model: {model_used}, "
                    f"file: {os.path.basename(file_path)}, image payload injected below."
                )
            return f"[Image] {status_text}, image payload injected below."
        # 普通文本: 若 payload 有 message/text 字段优先取, 否则整体 JSON
        if "message" in payload and isinstance(payload["message"], str):
            return str(payload["message"])
        if "text" in payload and isinstance(payload["text"], str):
            return str(payload["text"])
        return json.dumps(payload, ensure_ascii=False)
