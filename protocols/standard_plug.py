from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kernel.message import Message


class AoaoPlug(ABC):
    """
    [阿露标准电源接口 v1.1]
    所有 App (包括 VTuber、视觉、编程) 必须继承此类。

    Phase 1 通信重构: execute_command 新增可选 msg 参数，
    支持接收触发该指令的标准 Message 对象。
    """

    def __init__(self):
        self.is_connected = False  # 默认断电状态

    @property
    @abstractmethod
    def manifest(self) -> Dict[str, Any]:
        """
        [说明书]
        必须返回字典，包含:
        - name: 唯一标识符 (如 "vision_eye")
        - description: 给 LLM 看的功能描述
        - usage: 什么时候该用我

        [0.3.27.0 工具架构改造 规格五.3] commands 列表可选字段 (默认保守):
        - param_schema: dict, 命令参数 schema (缺省不校验, 由内核硬校验兜底)
        - risk: "low"|"medium"|"high", 未声明时沿用现有权限逻辑
        - parallel_safe: bool, 默认 false (只读工具声明后才允许并发)
        - idempotent: bool, 默认 false (声明支持幂等键, AppBus 透传)
        - timeout: float, 单命令执行超时 (秒)
        - progress_label: str, 状态栏安全描述 (工具参数绝不原样进状态栏)
        - owner_required: bool, 需要老板身份的额外声明 (与 require_owner 配合)

        coordinated_lifecycle: 可选顶层字段，只有精确 True 加入 AppBus 在途协调。
        加入者必须支持取消连接后清理部分初始化、幂等 on_disconnect；返回 False 表示
        清理未完成。该声明不授予权限，不保证强制中断线程/原生 I/O 或硬性关闭时限。
        默认缺省保留旧生命周期行为；当前仅显式 Guest 微信接入声明。
        """
        pass

    @abstractmethod
    async def on_connect(self):
        """[通电] 初始化资源 (如加载模型、打开摄像头)"""
        pass

    @abstractmethod
    async def on_disconnect(self):
        """[断电] 释放资源 (如清空显存)"""
        pass

    async def on_system_shutdown(self):
        """整套阿露正常退出后的可选收尾；普通 App 默认无额外动作。"""
        return True

    @abstractmethod
    async def execute_command(self, command: str, msg: Optional["Message"] = None, **kwargs) -> Any:
        """
        [执行] 接收指令并返回结果。
        msg: 可选的触发消息上下文 (Phase 1 统一消息协议)。
             旧式 App 的 **kwargs 会自动吸收该参数，无需改动。
        """
        pass
