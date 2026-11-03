"""只读画面引用；发布时间不证明真实采集时间、微信身份或控制权限。"""

import time
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from protocols.wechat_artifacts import ArtifactDescriptor, WeChatArtifactError


PREVIEW_TTL_SECONDS = 10


@dataclass(frozen=True, slots=True, repr=False)
class PreviewFrame:
    artifact: ArtifactDescriptor
    published_at: float

    def __post_init__(self) -> None:
        if (
            type(self.artifact) is not ArtifactDescriptor
            or type(self.published_at) not in (int, float)
            or not 0 < self.published_at <= 253402300799
        ):
            raise WeChatArtifactError("画面引用非法")

    def require_fresh(self) -> None:
        if not -2 <= time.time() - self.published_at < PREVIEW_TTL_SECONDS:
            raise WeChatArtifactError("画面已失效或时钟不一致")

    def to_payload(self) -> dict:
        return {
            "schema_version": 1, "artifact": self.artifact.to_payload(),
            "published_at": self.published_at,
        }

    @classmethod
    def from_payload(cls, value: dict) -> "PreviewFrame":
        if (
            type(value) is not dict
            or set(value) != {"schema_version", "artifact", "published_at"}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
        ):
            raise WeChatArtifactError("画面引用非法")
        return cls(ArtifactDescriptor.from_payload(value["artifact"]), value["published_at"])


@runtime_checkable
class PreviewReadPort(Protocol):
    """可信短小内存端口，不得在 latest 中采集桌面或调用 SDK。"""

    def latest(self) -> PreviewFrame: ...
