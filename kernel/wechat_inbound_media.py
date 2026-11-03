"""微信入站附件接线：复用校验、Host 资产库和原消息附件合同。"""

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol

from kernel import asset_store
from kernel.raw_message_models import AttachmentRef
from kernel.wechat_artifact_content import normalize_content, prepare_artifact
from protocols.wechat_artifacts import ArtifactDescriptor
from protocols.wechat_messages import WeChatMediaEvent


class ArtifactReader(Protocol):
    async def pull_artifact(self, descriptor: ArtifactDescriptor) -> bytes: ...


@dataclass(frozen=True, slots=True)
class InboundMedia:
    path: str | None = None
    attachment: AttachmentRef | None = None
    capture_mode: str = "unavailable"
    error: str | None = None
    vision_path: str | None = None


class WeChatInboundMedia:
    """保留一个未确认批次的结果；取消或确认重试不重复写入同一附件。"""

    def __init__(
        self, client: ArtifactReader, *, image_preprocessor: Callable[[bytes], bytes] | None = None,
    ) -> None:
        self._client = client
        self._image_preprocessor = image_preprocessor
        self._lock = asyncio.Lock()
        self._entries: dict[tuple[str, str], tuple[str, InboundMedia]] = {}

    async def resolve(self, event: WeChatMediaEvent, conversation_id: str) -> InboundMedia:
        if (type(event) is not WeChatMediaEvent or type(conversation_id) is not str
                or not re.fullmatch(r"wxguest_[a-f0-9]{32}", conversation_id)):
            raise ValueError("微信媒体来源绑定非法")
        key = conversation_id, event.event_id
        fingerprint = hashlib.sha256(json.dumps(
            event.to_payload(), ensure_ascii=False, sort_keys=True,
        ).encode("utf-8")).hexdigest()
        async with self._lock:
            previous = self._entries.get(key)
            if previous is not None:
                if previous[0] != fingerprint:
                    raise ValueError("微信媒体事件标识冲突")
                return previous[1]
            if len(self._entries) >= 16:
                raise ValueError("微信媒体批次尚未确认")
            work = asyncio.create_task(self._load(event, conversation_id))
            cancelled = False
            while not work.done():
                try:
                    await asyncio.shield(work)
                except asyncio.CancelledError:
                    cancelled = True
            result = work.result()
            self._entries[key] = fingerprint, result
            if cancelled:
                raise asyncio.CancelledError
            return result

    async def _load(self, event: WeChatMediaEvent, conversation_id: str) -> InboundMedia:
        if event.artifact is None:
            return InboundMedia(error=event.media_error or "guest_unavailable")
        try:
            data = await self._client.pull_artifact(event.artifact)
        except Exception:
            return InboundMedia(error="download_failed")
        try:
            prepared = await asyncio.to_thread(prepare_artifact, event.artifact, data)
            if prepared.content_type != "image/png":
                return InboundMedia(error="invalid_image")
        except ValueError:
            return InboundMedia(error="invalid_image")
        vision_data: bytes | None = None
        if event.kind == "emotion" and self._image_preprocessor is not None:
            try:
                enhanced = await asyncio.to_thread(self._image_preprocessor, prepared.data)
                candidate = await asyncio.to_thread(normalize_content, enhanced)
                if candidate.content_type == "image/png" and candidate.data != prepared.data:
                    vision_data = candidate.data
            except Exception:
                # 预处理只改善预览，失败时仍保存已经完成核验的原预览。
                pass
        # ID 由本机目标和事件共同派生，不把 Guest 资产 ID 当跨会话持久权限。
        identity = hashlib.sha256(
            (conversation_id + ":" + event.event_id).encode("utf-8"),
        ).hexdigest()
        content_sha256 = hashlib.sha256(prepared.data).hexdigest()
        try:
            path = await asyncio.to_thread(
                asset_store.save_verified_from_bytes,
                prepared.data, identity + ".png", "wechat",
                stable_key=identity, expected_sha256=content_sha256,
            )
        except (OSError, ValueError):
            return InboundMedia(error="storage_failed")
        vision_path = None
        if vision_data is not None:
            vision_sha256 = hashlib.sha256(vision_data).hexdigest()
            try:
                vision_path = await asyncio.to_thread(
                    asset_store.save_verified_from_bytes,
                    vision_data, identity + "_vision.png", "wechat",
                    stable_key=identity + "_vision", expected_sha256=vision_sha256,
                )
            except (OSError, ValueError):
                # 派生图失败不影响已经核验并保存的源附件。
                vision_path = None
        attachment = AttachmentRef(
            attachment_id="wxin_" + identity, content_type=prepared.content_type,
            sha256=content_sha256, size_bytes=len(prepared.data),
            description="微信媒体预览" if event.capture_mode == "preview" else "微信图片",
        )
        return InboundMedia(
            path=path, attachment=attachment, capture_mode=event.capture_mode,
            vision_path=vision_path,
        )

    def release(self) -> None:
        """仅在整批已投递并确认后释放缓存；文件继续归 Host 资产库所有。"""
        self._entries.clear()
