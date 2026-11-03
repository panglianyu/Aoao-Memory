"""Host/Guest 共享的本机监听选择；观察名称不是稳定微信身份或授权。"""

import re
from dataclasses import dataclass


class WeChatReceiveError(ValueError):
    """共享接收/监听错误；保留 Host 旧导入的异常类型。"""


@dataclass(frozen=True, slots=True, repr=False)
class WeChatListenTarget:
    """本机显式监听选择；匹配名称和类型不是对微信用户身份的认证。"""

    chat_name: str
    chat_type: str
    conversation_id: str

    def __post_init__(self) -> None:
        try:
            if (
                type(self.chat_name) is not str
                or not 0 < len(self.chat_name) <= 256
                or len(self.chat_name.encode("utf-8")) > 256
                or any(ord(c) < 32 or ord(c) == 127 for c in self.chat_name)
                or type(self.chat_type) is not str
                or self.chat_type not in {"direct", "group"}
                or type(self.conversation_id) is not str
                or not re.fullmatch(r"wxguest_[0-9a-f]{32}", self.conversation_id)
            ):
                raise ValueError
        except (ValueError, TypeError):
            raise WeChatReceiveError("微信监听选择非法") from None


def validate_listen_targets(targets: tuple[WeChatListenTarget, ...]) -> None:
    if (
        type(targets) is not tuple or not 1 <= len(targets) <= 64
        or any(type(t) is not WeChatListenTarget for t in targets)
        or len({(t.chat_name, t.chat_type) for t in targets}) != len(targets)
        or len({t.conversation_id for t in targets}) != len(targets)
    ):
        raise WeChatReceiveError("微信监听选择非法")
