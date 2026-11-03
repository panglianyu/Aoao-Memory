"""Host 导入入口：绑定资产描述符并复用 Host/Guest 图片字节核验。"""

import hashlib

from protocols.wechat_artifacts import ArtifactDescriptor
from protocols.wechat_image_content import (
    ArtifactImportError, MAX_IMAGE_PIXELS as MAX_IMAGE_PIXELS,
    MAX_IMAGE_SIDE as MAX_IMAGE_SIDE, PreparedArtifact,
    normalize_image_content,
)


def prepare_artifact(descriptor: ArtifactDescriptor, data: bytes) -> PreparedArtifact:
    """远端内容必须重新与元数据绑定，不能把传输成功当永久可信标记。"""
    if (
        type(descriptor) is not ArtifactDescriptor or type(data) is not bytes
        or len(data) != descriptor.size_bytes
        or hashlib.sha256(data).hexdigest() != descriptor.sha256
    ):
        raise ArtifactImportError("资产内容与元数据不符")
    return normalize_content(data)


def normalize_content(data: bytes) -> PreparedArtifact:
    """Host 保持原 JPEG/PNG 合同，其他格式仍为不透明字节。"""
    return normalize_image_content(data)
