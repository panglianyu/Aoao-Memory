# G:\Aoao_OS\kernel\message.py
"""
Aoao OS 统一消息协议 (Phase 1)

参考 AgentScope 的 Msg 设计理念，定义系统内所有通信的标准数据载体。
所有 App 模块在收发数据时，必须使用 Message 对象进行封装。
"""
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional


class MessageType(str, Enum):
    """消息语义鉴别器。使用 str 基类以保证日志/JSON 可读性。"""

    # --- 输入类 (Queue 路径, 顺序消费) ---
    USER_INPUT = "user_input"

    # --- 广播类 (Pub/Sub 路径, 并行分发) ---
    USER_SPEAK = "user_speak"
    AGENT_SPEAK = "agent_speak"
    TOOL_RESULT = "tool_result"

    # --- 系统类 (Pub/Sub 路径, 并行分发) ---
    SYSTEM_ERROR = "system_error"
    REMINDER_FIRE = "reminder_fire"
    SYSTEM_BOOT = "system_boot"

    # --- 文档管线类 (Pub/Sub 路径, 插件间解耦通信) ---
    DOCUMENT_PARSE_REQUEST = "document_parse_request"
    KNOWLEDGE_INGEST_REQUEST = "knowledge_ingest_request"

    # --- 文件生成与多渠道分发类 (Pub/Sub 路径) ---
    DOCUMENT_GENERATE_REQUEST = "document_generate_request"
    DOCUMENT_GENERATED = "document_generated"
    OUTBOUND_DISPATCH = "outbound_dispatch"

    # --- 语音状态类 (Pub/Sub 路径, TTS 朗读生命周期) ---
    VOICE_STATE = "voice_state"

    # --- STT 状态类 (Pub/Sub 路径, STT 听写生命周期) ---
    STT_STATE = "stt_state"

    # --- 工作状态类 (Pub/Sub 路径, [0.3.22.1 P3] Web 工作状态栏数据源) ---
    WORK_STATUS = "work_status"

    # --- 工作空间类 (Pub/Sub 路径, 浏览器/文件/终端工作台统一事件) ---
    WORKSPACE_EVENT = "workspace_event"

    # --- 预留 ---
    CONTROL = "control"


# 事件名到消息类型的映射 (向后兼容旧 emit 接口)
_EVENT_TYPE_MAP: Dict[str, MessageType] = {
    "on_user_speak": MessageType.USER_SPEAK,
    "on_agent_speak": MessageType.AGENT_SPEAK,
    # [0.2.20.0 流式输出] 四个流事件复用 AGENT_SPEAK 类型 (逐字渲染生命周期)。
    "on_agent_stream_start": MessageType.AGENT_SPEAK,
    "on_agent_stream_token": MessageType.AGENT_SPEAK,
    "on_agent_stream_reset": MessageType.AGENT_SPEAK,
    "on_agent_stream_end": MessageType.AGENT_SPEAK,
    "on_tool_finish": MessageType.TOOL_RESULT,
    "on_system_error": MessageType.SYSTEM_ERROR,
    "on_reminder_fire": MessageType.REMINDER_FIRE,
    "on_document_parse_request": MessageType.DOCUMENT_PARSE_REQUEST,
    "on_knowledge_ingest_request": MessageType.KNOWLEDGE_INGEST_REQUEST,
    "on_document_generate_request": MessageType.DOCUMENT_GENERATE_REQUEST,
    "on_document_generated": MessageType.DOCUMENT_GENERATED,
    "on_outbound_dispatch": MessageType.OUTBOUND_DISPATCH,
    # [0.3.0.0 语音系统] TTS 朗读状态事件 (system_voice 发射, 桌面被动订阅)。
    "on_voice_state": MessageType.VOICE_STATE,
    # [0.3.3.0 STT 听写] STT 听写状态事件 (system_voice 发射, 桌面被动订阅)。
    "on_stt_state": MessageType.STT_STATE,
    # [0.3.22.1 P3 工作状态栏] brain/任务队列发射, web/bridge 转 WS 帧。
    "on_work_status": MessageType.WORK_STATUS,
    "on_workspace_event": MessageType.WORKSPACE_EVENT,
}

# 事件名到默认角色的映射
_EVENT_ROLE_MAP: Dict[str, str] = {
    "on_user_speak": "user",
    "on_agent_speak": "agent",
    # [0.2.20.0 流式输出] 流事件 role 同 agent。
    "on_agent_stream_start": "agent",
    "on_agent_stream_token": "agent",
    "on_agent_stream_reset": "agent",
    "on_agent_stream_end": "agent",
    "on_tool_finish": "tool",
    "on_system_error": "system",
    "on_reminder_fire": "system",
    "on_document_parse_request": "system",
    "on_knowledge_ingest_request": "system",
    "on_document_generate_request": "system",
    "on_document_generated": "system",
    "on_outbound_dispatch": "system",
    # [0.3.0.0 语音系统] 语音状态属系统事件。
    "on_voice_state": "system",
    # [0.3.3.0 STT 听写] 听写状态属系统事件。
    "on_stt_state": "system",
    # [0.3.22.1 P3 工作状态栏] 工作状态属系统事件。
    "on_work_status": "system",
    "on_workspace_event": "system",
}


def _new_id() -> str:
    """生成唯一消息标识。"""
    return uuid.uuid4().hex


def event_name_to_type(event_name: str) -> MessageType:
    """将旧式事件名转换为 MessageType。未知事件降级为 CONTROL。"""
    return _EVENT_TYPE_MAP.get(event_name, MessageType.CONTROL)


def event_name_to_role(event_name: str) -> str:
    """将旧式事件名转换为默认角色。"""
    return _EVENT_ROLE_MAP.get(event_name, "system")


@dataclass(frozen=True)
class Message:
    """
    系统内所有通信的标准载体。

    不可变设计 (frozen=True): 消息一旦创建即不可篡改，保证传递过程中的数据完整性。
    """

    id: str
    timestamp: float
    source: str
    target: str
    role: str
    msg_type: MessageType
    content: str
    event_name: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """冻结 metadata，防止外部修改，保证不可变语义。"""
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    # ==========================================
    # 工厂方法: 简化消息构造，自动填充 id/timestamp/默认值
    # ==========================================

    @staticmethod
    def user_input(content: str, source: str, **metadata: Any) -> "Message":
        """构造用户输入消息 (控制台/飞书/调度器等 -> 主循环)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="main",
            role="user",
            msg_type=MessageType.USER_INPUT,
            content=content,
            event_name=None,
            metadata=metadata,
        )

    @staticmethod
    def user_speak(content: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造用户发言广播 (通知后台监听器记录/分析)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="user",
            msg_type=MessageType.USER_SPEAK,
            content=content,
            event_name="on_user_speak",
            metadata=metadata,
        )

    @staticmethod
    def agent_speak(content: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造代理发言广播 (AI 回复通知后台监听器)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="agent",
            msg_type=MessageType.AGENT_SPEAK,
            content=content,
            event_name="on_agent_speak",
            metadata=metadata,
        )

    # ==========================================
    # [0.2.20.0] 流式输出事件工厂 (逐字渲染生命周期)
    # 契约: brain 仅对 console 源 (桌面/控制台当面) 发流事件; 微信/飞书等
    # 渠道走整条 on_agent_speak, 零行为变化。metadata 约定携带 turn_id
    # (本轮 think 唯一标识), stream_end 额外携带 status (ok/error/timeout)。
    # ==========================================

    @staticmethod
    def agent_stream_start(content: str = "", source: str = "brain", **metadata: Any) -> "Message":
        """构造流开始广播 (本轮首个净化可渲染 token 到达, UI 据此建气泡)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="agent",
            msg_type=MessageType.AGENT_SPEAK,
            content=content,
            event_name="on_agent_stream_start",
            metadata=metadata,
        )

    @staticmethod
    def agent_stream_token(content: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造流 token 广播 (已净化的可追加渲染文本片段)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="agent",
            msg_type=MessageType.AGENT_SPEAK,
            content=content,
            event_name="on_agent_stream_token",
            metadata=metadata,
        )

    @staticmethod
    def agent_stream_reset(content: str = "", source: str = "brain", **metadata: Any) -> "Message":
        """构造流重置广播 (之前渲染作废, UI 清空气泡内容等后续 token)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="agent",
            msg_type=MessageType.AGENT_SPEAK,
            content=content,
            event_name="on_agent_stream_reset",
            metadata=metadata,
        )

    @staticmethod
    def agent_stream_end(content: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造流结束广播 (content=最终净化全文, UI 整体替换气泡; metadata.status 三态)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="agent",
            msg_type=MessageType.AGENT_SPEAK,
            content=content,
            event_name="on_agent_stream_end",
            metadata=metadata,
        )

    # ==========================================
    # [0.3.0.0] 语音状态事件工厂 (TTS 朗读生命周期)
    # 契约: system_voice 插件在合成/播放/空闲/服务不可用时广播; 桌面 UI
    # 纯被动订阅 (状态灯/控制条), 不参与状态机。metadata 约定携带
    # turn_id (本轮标识) / voice (音色) / path (wav 路径) / sentence_idx (句序)。
    # ==========================================

    @staticmethod
    def voice_state(state: str, source: str = "system_voice", **metadata: Any) -> "Message":
        """
        构造语音状态广播 (TTS 生命周期状态迁移)。

        Args:
            state: 状态取值 idle / synthesizing / playing / service_down。
            source: 发射方标识, 默认 system_voice 插件。
            **metadata: 约定键 turn_id / voice / path / sentence_idx。
        """
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.VOICE_STATE,
            content=state,
            event_name="on_voice_state",
            metadata={"state": state, **metadata},
        )

    # ==========================================
    # [0.3.3.0] STT 状态事件工厂 (STT 听写生命周期)
    # 契约: system_voice 插件在监听/转写/完成/出错时广播; 桌面 UI 纯被动
    # 订阅 (麦克风灯/转写进度), 不参与状态机。metadata 约定携带
    # text (转写结果) / elapsed (转写耗时秒) / engine (引擎标识) / turn_id。
    # ==========================================

    @staticmethod
    def stt_state(state: str, source: str = "system_voice", **metadata: Any) -> "Message":
        """
        构造 STT 状态广播 (听写生命周期状态迁移)。

        Args:
            state: 状态取值 idle / listening / transcribing / done / error。
            source: 发射方标识, 默认 system_voice 插件。
            **metadata: 约定键 text / elapsed / engine / turn_id。
        """
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.STT_STATE,
            content=state,
            event_name="on_stt_state",
            metadata={"state": state, **metadata},
        )

    # ==========================================
    # [0.3.22.1 P3] 工作状态事件工厂 (Web 工作状态栏数据源)
    # 契约: brain/任务队列在 think 入出/工具批/入队时广播; web/bridge 纯被动
    # 订阅转 WS 帧, 前端状态栏渲染。metadata 约定携带 state (idle/thinking/tool) /
    # activity (人话文案) / queue (工作列表排队条目摘要)。
    # ==========================================

    @staticmethod
    def work_status(state: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造工作状态广播 (content=state, metadata 带 state/activity/queue)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.WORK_STATUS,
            content=state,
            event_name="on_work_status",
            metadata={"state": state, **metadata},
        )

    @staticmethod
    def workspace_event(event: Any, source: str = "browser_workspace") -> "Message":
        """构造工作空间广播；event 必须先通过 WorkspaceEvent 契约校验。"""
        from kernel.workspace_events import WorkspaceEvent

        if not isinstance(event, WorkspaceEvent):
            raise TypeError("event 必须是 WorkspaceEvent")
        payload = event.to_payload()
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.WORKSPACE_EVENT,
            content=str(payload["kind"]),
            event_name="on_workspace_event",
            metadata=payload,
        )

    @staticmethod
    def tool_result(content: str, source: str, **metadata: Any) -> "Message":
        """构造工具执行结果广播。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="tool",
            msg_type=MessageType.TOOL_RESULT,
            content=content,
            event_name="on_tool_finish",
            metadata=metadata,
        )

    @staticmethod
    def system_error(content: str, source: str, **metadata: Any) -> "Message":
        """构造系统错误通知。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.SYSTEM_ERROR,
            content=content,
            event_name="on_system_error",
            metadata=metadata,
        )

    @staticmethod
    def reminder_fire(content: str, source: str = "system_scheduler", **metadata: Any) -> "Message":
        """构造日程提醒触发事件。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.REMINDER_FIRE,
            content=content,
            event_name="on_reminder_fire",
            metadata=metadata,
        )

    @staticmethod
    def document_parse_request(file_path: str, source: str = "brain", **metadata: Any) -> "Message":
        """构造文档解析请求事件 (触发 doc_parser 插件解析指定文件)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="doc_parser",
            role="system",
            msg_type=MessageType.DOCUMENT_PARSE_REQUEST,
            content=file_path,
            event_name="on_document_parse_request",
            metadata={"file_path": file_path, **metadata},
        )

    @staticmethod
    def knowledge_ingest_request(text: str, source_file: str, source: str = "doc_parser", **metadata: Any) -> "Message":
        """构造知识入库请求事件 (触发 knowledge_base 插件将文本切块并写入向量库)。"""
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="knowledge_base",
            role="system",
            msg_type=MessageType.KNOWLEDGE_INGEST_REQUEST,
            content=text,
            event_name="on_knowledge_ingest_request",
            metadata={"source_file": source_file, **metadata},
        )

    @staticmethod
    def document_generate_request(spec: Dict[str, Any], source: str = "brain", **metadata: Any) -> "Message":
        """
        构造文档生成请求事件 (触发 doc_gen 插件生成文件)。

        spec 字段约定:
          - doc_type: 文档类型 (pdf/docx/xlsx)
          - title: 文档标题
          - content: 结构化内容 (根据 doc_type 不同而不同)
          - target_channel: (可选) 目标分发渠道
          - target_user: (可选) 目标用户
        """
        import json as _json
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="doc_gen",
            role="system",
            msg_type=MessageType.DOCUMENT_GENERATE_REQUEST,
            content=_json.dumps(spec, ensure_ascii=False),
            event_name="on_document_generate_request",
            metadata={"spec": spec, **metadata},
        )

    @staticmethod
    def document_generated(file_path: str, file_id: str, source: str = "doc_gen", **metadata: Any) -> "Message":
        """
        构造文档生成完毕通知事件。

        metadata 中应携带会话上下文:
          - channel_type: 消息源渠道 (feishu/console/...)
          - session_id: 会话标识
          - target_channel: (可选) 目标分发渠道
          - target_user: (可选) 目标用户
        """
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target="*",
            role="system",
            msg_type=MessageType.DOCUMENT_GENERATED,
            content=file_path,
            event_name="on_document_generated",
            metadata={"file_path": file_path, "file_id": file_id, **metadata},
        )

    @staticmethod
    def outbound_dispatch(
        target_channel: str,
        target_user: str,
        message_type: str,
        payload: Dict[str, Any],
        source: str = "outbound_router",
        **metadata: Any,
    ) -> "Message":
        """
        构造通用出站分发指令 (触发对应渠道插件发送内容给用户)。

        各渠道插件订阅此事件后，通过 target_channel 防御检查判断是否属于自身职责。

        Args:
            target_channel: 目标渠道标识 (feishu/wechat/...)
            target_user: 目标用户标识
            message_type: 消息类型 (file/text/image/...)
            payload: 载荷字典 (file_path, file_id 等)
        """
        import json as _json
        return Message(
            id=_new_id(),
            timestamp=time.time(),
            source=source,
            target=target_channel,
            role="system",
            msg_type=MessageType.OUTBOUND_DISPATCH,
            content=_json.dumps(payload, ensure_ascii=False),
            event_name="on_outbound_dispatch",
            metadata={
                "target_channel": target_channel,
                "target_user": target_user,
                "message_type": message_type,
                "payload": payload,
                **metadata,
            },
        )

    # ==========================================
    # 向后兼容访问器: 模拟 dict 的 get() 行为
    # ==========================================

    def get(self, key: str, default: Any = None) -> Any:
        """
        提供 dict 风格的字段访问，便于旧代码平滑过渡。

        支持的 key:
          - "content"     -> self.content
          - "source_app"  -> self.source  (旧字段名映射)
          - 其他 key      -> self.metadata.get(key, default)
        """
        if key == "content":
            return self.content
        if key == "source_app":
            return self.source
        if key == "text":
            return self.content
        if key == "res":
            return self.content
        if key == "error":
            return self.content
        if key == "message":
            return self.content
        if key == "source":
            return self.source
        if key == "file_path":
            return self.metadata.get("file_path", self.content)
        if key == "source_file":
            return self.metadata.get("source_file", default)
        return self.metadata.get(key, default)

    def __repr__(self) -> str:
        meta_str = f", meta={self.metadata}" if self.metadata else ""
        return (
            f"Message(id={self.id[:8]}..., type={self.msg_type.value}, "
            f"src={self.source}, content={self.content[:50]!r}{meta_str})"
        )
