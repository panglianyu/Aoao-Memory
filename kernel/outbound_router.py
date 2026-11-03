# G:\Aoao_OS\kernel\outbound_router.py
"""
中枢出站路由器 (OutboundRouter)

监听文档生成事件 (on_document_generated)，提取会话上下文与目标渠道信息，
组装通用出站分发指令 (on_outbound_dispatch) 并向 AppBus 广播。

设计原则:
  - OutboundRouter 是内核级组件，不依赖任何具体渠道插件
  - 不知道飞书、微信等渠道的存在，只做事件转换与路由
  - 与各插件之间零循环依赖，所有通信通过 Message + AppBus 单向流动
"""
from typing import TYPE_CHECKING

from kernel.logger import get_logger
import os

from kernel.message import Message

if TYPE_CHECKING:
    from kernel.app_bus import AppBus

logger = get_logger(__name__)

_IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".avif", ".tiff", ".tif"})

def _is_image_path(file_path: str) -> bool:
    if not file_path:
        return False
    return os.path.splitext(file_path)[1].lower() in _IMAGE_EXTENSIONS


class OutboundRouter:
    """
    中枢出站路由器。

    职责:
      - 监听 on_document_generated 事件
      - 从事件 metadata 中提取 target_channel / target_user / channel_type
      - 若 target_channel 已明确指定，直接路由到该渠道
      - 若未指定，使用 channel_type (消息源渠道) 作为 fallback
      - 组装 OUTBOUND_DISPATCH 事件并广播，由渠道插件自行认领
    """

    def __init__(self, bus: "AppBus"):
        self.bus = bus

    def attach(self) -> None:
        """将路由器挂载到事件总线，注册事件订阅。"""
        self.bus.subscribe("on_document_generated", self.on_document_generated)
        logger.info("[OutboundRouter] 已挂载，订阅 on_document_generated 事件。")

    async def on_document_generated(self, msg: Message) -> None:
        """
        文档生成完毕后的路由逻辑。

        从 msg.metadata 中提取:
          - target_channel: 用户/LLM 明确指定的目标渠道 (优先级最高)
          - channel_type: 消息源渠道 (由 doc_gen 从 trigger_msg 中传递)
          - target_user: 目标用户标识
          - session_id: 会话标识 (透传给渠道插件)
          - file_path: 生成的文件物理路径
          - file_id: 文件唯一标识

        路由策略:
          1. 若 target_channel 已指定 -> 使用它
          2. 若未指定但 channel_type 存在 -> 回发到源渠道
          3. 若两者皆无 -> 记录警告，不广播 (无法路由)
        """
        metadata = msg.metadata

        target_channel = metadata.get("target_channel", "")
        channel_type = metadata.get("channel_type", "")
        target_user = metadata.get("target_user", "")
        session_id = metadata.get("session_id", "")
        file_path = metadata.get("file_path", msg.content)
        file_id = metadata.get("file_id", "")

        # 路由决策: 优先使用明确指定的 target_channel，否则回退到源渠道
        resolved_channel = target_channel or channel_type
        if not resolved_channel:
            logger.warning(
                f"[OutboundRouter] 无法路由: file_id={file_id} "
                f"既未指定 target_channel 也无 channel_type，丢弃分发。"
            )
            return

        # 组装出站分发事件
        payload = {
            "file_path": file_path,
            "file_id": file_id,
            "session_id": session_id,
        }

        resolved_type = "image" if _is_image_path(file_path) else "file"

        dispatch_msg = Message.outbound_dispatch(
            target_channel=resolved_channel,
            target_user=target_user,
            message_type=resolved_type,
            payload=payload,
            source="outbound_router",
            session_id=session_id,
        )

        await self.bus.emit_msg(dispatch_msg)

        logger.info(
            f"[OutboundRouter] 分发指令已广播: "
            f"channel={resolved_channel}, user={target_user}, type={resolved_type}, "
            f"file_id={file_id}, path={file_path}"
        )
