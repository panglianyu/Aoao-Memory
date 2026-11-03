"""显式单次点击合同；不包含键盘、脚本、窗口查找或宿主输入参数。"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from protocols.wechat_preview import PreviewFrame, PreviewReadPort


@dataclass(frozen=True, slots=True, repr=False)
class ClickRequest:
    frame: PreviewFrame
    width: int
    height: int
    x: int
    y: int

    def __post_init__(self) -> None:
        if (
            type(self.frame) is not PreviewFrame
            or any(type(value) is not int for value in (self.width, self.height, self.x, self.y))
            or not 1 <= self.width <= 4096 or not 1 <= self.height <= 4096
            or self.width * self.height > 4 * 1024 * 1024
            or not 0 <= self.x < self.width or not 0 <= self.y < self.height
        ):
            raise ValueError("点击坐标或画面绑定非法")

    def to_payload(self) -> dict:
        return {
            "operation": "click", "frame": self.frame.to_payload(),
            "width": self.width, "height": self.height, "x": self.x, "y": self.y,
        }

    @classmethod
    def from_payload(cls, value: Mapping) -> "ClickRequest":
        if (
            not isinstance(value, Mapping)
            or set(value) != {"operation", "frame", "width", "height", "x", "y"}
            or value["operation"] != "click"
        ):
            raise ValueError("点击请求非法")
        # RPC 会冻结嵌套映射；由调用方在严格 wire 解码后提供普通帧字典。
        frame = value["frame"]
        if isinstance(frame, Mapping):
            frame = dict(frame)
            if isinstance(frame.get("artifact"), Mapping):
                frame["artifact"] = dict(frame["artifact"])
        return cls(PreviewFrame.from_payload(frame), value["width"], value["height"], value["x"], value["y"])


def parse_click_outcome(value: dict) -> str:
    if (
        type(value) is not dict or set(value) != {"schema_version", "outcome"}
        or type(value["schema_version"]) is not int or value["schema_version"] != 1
        or type(value["outcome"]) is not str or value["outcome"] not in {"submitted", "rejected", "unknown"}
    ):
        raise ValueError("点击回执非法")
    return value["outcome"]


@runtime_checkable
class ClickFramePort(PreviewReadPort, Protocol):
    def claim_click(self, frame: PreviewFrame, width: int, height: int) -> None: ...


@runtime_checkable
class ManualInputPort(Protocol):
    """可信 Guest 专用驱动；须再次核对原生窗口/尺寸/来源，完成按下和释放后返回 None。

    不得抢占宿主输入。方法必须异步且取消时负责等候底层操作收尾；库不能证明驱动合规。
    此版本无生产实现，仅以测试扩展端口验证调用合同。
    """

    async def click(self, request: ClickRequest) -> None: ...
