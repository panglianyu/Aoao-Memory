# G:\Aoao_OS\kernel\image_utils.py
"""
图片格式识别与编码的共享工具层。

集中维护 Magic Bytes -> MIME 映射, 杜绝各处把 MIME 写死成 jpeg。
被 kernel/vision_parser.py (视觉网关) 与 system_memory_god (视觉记忆编码) 复用,
确保两边对图片真实格式的判定永远一致。
"""
import base64
import os
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

# 图片格式 Magic Bytes 映射表 (按 magic 字节长度降序匹配更稳妥)
_MAGIC_MAP = {
    b"\x89PNG\r\n\x1a\n": "image/png",
    b"\xff\xd8\xff": "image/jpeg",
    b"GIF89a": "image/gif",
    b"GIF87a": "image/gif",
    b"BM": "image/bmp",
    b"II*\x00": "image/tiff",
    b"MM\x00*": "image/tiff",
}

# 读取文件头用于判定的字节数
_HEADER_BYTES = 12


def detect_image_mime(header: bytes) -> str:
    """
    通过文件头 Magic Bytes 判定真实图片格式, 返回 MIME 或空串。
    header 长度不足或无任何 magic 命中时返回 "" (格式非法)。
    """
    if not header:
        return ""
    for magic, mime in _MAGIC_MAP.items():
        if header.startswith(magic):
            return mime
    # WEBP: RIFF....WEBP
    if header.startswith(b"RIFF") and b"WEBP" in header:
        return "image/webp"
    # AVIF: ISOBMFF container, ftyp box with avif brand
    if len(header) >= 12 and header[4:8] == b"ftyp" and header[8:12] == b"avif":
        return "image/avif"
    return ""


def detect_image_mime_from_path(path: str) -> str:
    """
    读取文件头并返回真实 MIME, 不可读或格式非法返回空串。
    同步接口; 调用方若在异步上下文应自行 asyncio.to_thread 包装。
    """
    try:
        with open(path, "rb") as f:
            header = f.read(_HEADER_BYTES)
    except (OSError, PermissionError):
        return ""
    return detect_image_mime(header)


# 视觉通道图片上限 (10MB), 超限缩放到 1920px 长边
_VISION_MAX_BYTES = 10 * 1024 * 1024
_VISION_MAX_DIM = 1920

def resize_image_bytes(raw_bytes: bytes, mime: str) -> bytes:
    """若原图超过 _VISION_MAX_BYTES, 用 PIL 缩放到 _VISION_MAX_DIM 长边再返回;
    缩放失败或反而变大则返回原字节。"""
    if len(raw_bytes) <= _VISION_MAX_BYTES:
        return raw_bytes
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(raw_bytes))
        w, h = img.size
        if max(w, h) <= _VISION_MAX_DIM:
            # 尺寸未超限但字节数超限, 转 JPEG 压缩
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=75)
            result = buf.getvalue()
            if len(result) < len(raw_bytes):
                return result
            return raw_bytes
        ratio = _VISION_MAX_DIM / max(w, h)
        new_size = (int(w * ratio), int(h * ratio))
        img = img.resize(new_size, Image.LANCZOS)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        return buf.getvalue()
    except Exception:
        return raw_bytes


def read_image_as_data_url(path: str) -> Optional[str]:
    """
    读取图片并编码为 data URL (含真实 MIME, 不再写死 jpeg)。
    返回形如 data:image/png;base64,xxxx 的字符串。
    文件不可读或格式非法时返回 None。
    """
    if not path or not os.path.exists(path):
        return None
    mime = detect_image_mime_from_path(path)
    if not mime:
        logger.warning(f"[image_utils] 文件格式非法, 拒绝编码: {os.path.basename(path)}")
        return None
    try:
        with open(path, "rb") as img_file:
            base64_data = base64.b64encode(img_file.read()).decode("utf-8")
    except (OSError, PermissionError) as e:
        logger.error(f"[image_utils] 图片读取异常: {e}")
        return None
    return f"data:{mime};base64,{base64_data}"
