"""明确选择的 Guest 媒体到 Host 隔离库；不扫描、打开、执行或自动投递聊天。"""

import asyncio
import re
import secrets
from dataclasses import asdict, dataclass

from kernel.wechat_artifact_quarantine import HostArtifactQuarantine, ImportedArtifact
from kernel.wechat_rpc.client import WeChatRpcClient
from protocols.wechat_artifacts import ArtifactDescriptor


def parse_media_report(value: dict) -> dict:
    """只保留闭合且自洽的工作台投影，不透传第三方扩展字段。"""
    if (type(value) is not dict or set(value) != {"schema_version", "importing", "items"}
        or type(value["schema_version"]) is not int or value["schema_version"] != 1
        or type(value["importing"]) is not bool or type(value["items"]) is not list
        or not 1 <= len(value["items"]) <= 16):
        raise ValueError("媒体回执非法")
    items = []
    for item in value["items"]:
        if (type(item) is not dict or set(item) != {"token", "size_bytes", "state", "receipt"}
            or type(item["token"]) is not str or not re.fullmatch(r"wximport_[a-f0-9]{32}", item["token"])
            or type(item["size_bytes"]) is not int or not 1 <= item["size_bytes"] <= 4 * 1024 * 1024
            or type(item["state"]) is not str
            or item["state"] not in {"available", "reading", "writing", "imported", "failed", "cancelled", "unknown"}):
            raise ValueError("媒体回执非法")
        receipt = item["receipt"]
        if item["state"] == "imported":
            if type(receipt) is not dict or set(receipt) != {"asset_id", "content_type", "size_bytes", "sha256", "width", "height"}:
                raise ValueError("导入回执非法")
            receipt = asdict(ImportedArtifact(**receipt))
        elif receipt is not None:
            raise ValueError("导入回执状态不符")
        items.append({**item, "receipt": receipt})
    in_progress = sum(item["state"] in {"reading", "writing"} for item in items)
    if (len({item["token"] for item in items}) != len(items) or in_progress > 1
        or value["importing"] != (in_progress == 1)):
        raise ValueError("媒体回执状态不符")
    return {"schema_version": 1, "importing": value["importing"], "items": items}


@dataclass(frozen=True, slots=True, repr=False)
class MediaReference:
    """可信本机选择的会话/观察/完整资产绑定，不是真实来源或发送者身份证明。"""

    conversation_id: str
    event_id: str
    artifact: ArtifactDescriptor

    def __post_init__(self) -> None:
        if (
            type(self.conversation_id) is not str or not re.fullmatch(r"wxguest_[a-f0-9]{32}", self.conversation_id)
            or type(self.event_id) is not str or not re.fullmatch(r"wxevt_[a-f0-9]{32}", self.event_id)
            or type(self.artifact) is not ArtifactDescriptor
        ):
            raise ValueError("媒体引用绑定非法")


@dataclass(slots=True, repr=False)
class _Entry:
    reference: MediaReference
    token: str
    state: str = "available"
    receipt: ImportedArtifact | None = None


class MediaImportSession:
    """最多 16 项不可变本机选择；单次导入、未知禁重试，写入线程完成后保留核对回执。"""

    def __init__(self, *, client: WeChatRpcClient, quarantine: HostArtifactQuarantine,
                 references: tuple[MediaReference, ...], allowed_targets: set[str]) -> None:
        if (
            type(client) is not WeChatRpcClient or type(quarantine) is not HostArtifactQuarantine
            or type(references) is not tuple or not 1 <= len(references) <= 16
            or any(type(ref) is not MediaReference or ref.conversation_id not in allowed_targets for ref in references)
            or len({ref.artifact.asset_id for ref in references}) != len(references)
            or len({(ref.conversation_id, ref.event_id) for ref in references}) != len(references)
        ):
            raise ValueError("媒体导入需要明确选择、已知会话与本机隔离库")
        self._client, self._quarantine = client, quarantine
        self._entries = tuple(_Entry(ref, "wximport_" + secrets.token_hex(16)) for ref in references)
        if len({entry.token for entry in self._entries}) != len(self._entries):
            raise ValueError("媒体选择票据生成失败")
        self._closed = False
        self._busy = False

    def status(self) -> dict:
        return {"schema_version": 1, "importing": self._busy, "items": [
            {"token": entry.token, "size_bytes": entry.reference.artifact.size_bytes,
             "state": entry.state, "receipt": asdict(entry.receipt) if entry.receipt else None}
            for entry in self._entries
        ]}

    def close(self) -> None:
        self._closed = True
        for entry in self._entries:
            if entry.state == "available":
                entry.state = "cancelled"

    async def import_one(self, token: str) -> dict:
        if type(token) is not str or not re.fullmatch(r"wximport_[a-f0-9]{32}", token):
            raise ValueError("媒体选择票据非法")
        entry = next((item for item in self._entries if item.token == token), None)
        if entry is None or self._closed or self._busy:
            raise ValueError("媒体选择不存在、已关闭或已有导入")
        if entry.state != "available":
            return self.status()
        self._busy = True
        entry.state = "reading"
        try:
            try:
                data = await self._client.pull_artifact(entry.reference.artifact)
            except asyncio.CancelledError:
                entry.state = "cancelled"
                raise
            except Exception:
                entry.state = "failed"
                self._busy = False
                return self.status()
            if self._closed:
                entry.state = "cancelled"
                self._busy = False
                return self.status()
            entry.state = "writing"
            work = asyncio.create_task(asyncio.to_thread(
                self._quarantine.import_bytes, entry.reference.artifact, data,
            ))
            cancelled = False
            while not work.done():
                try:
                    await asyncio.wait({work})
                except asyncio.CancelledError:
                    cancelled = True
            try:
                receipt = work.result()
                if type(receipt) is not ImportedArtifact:
                    raise ValueError
                entry.receipt, entry.state = receipt, "imported"
            except (Exception, asyncio.CancelledError):
                entry.state = "unknown"
            if cancelled:
                raise asyncio.CancelledError
        finally:
            self._busy = False
        return self.status()
