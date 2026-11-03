"""Host/Guest 共用的图片字节规范化边界，不访问文件或桌面。"""

import io
import struct
import zlib
from dataclasses import dataclass
from typing import Any

from PIL import Image, ImageFile

from protocols.wechat_artifacts import MAX_ARTIFACT_BYTES


MAX_IMAGE_PIXELS = 4 * 1024 * 1024
MAX_IMAGE_SIDE = 4096
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class ArtifactImportError(ValueError):
    """固定错误，不暴露资产内容、路径或解码器异常。"""


@dataclass(frozen=True, slots=True, repr=False)
class PreparedArtifact:
    data: bytes
    content_type: str = "application/octet-stream"
    width: int | None = None
    height: int | None = None


class _BoundedOutput(io.BytesIO):
    def write(self, data: Any) -> int:
        if self.tell() + len(data) > MAX_ARTIFACT_BYTES:
            raise ArtifactImportError("重新编码图片超过大小上限")
        return super().write(data)


def _png_for_decode(data: bytes) -> bytes:
    offset, output = 8, bytearray(_PNG_SIGNATURE)
    for number in range(1024):
        if offset + 12 > len(data):
            break
        size = int.from_bytes(data[offset:offset + 4], "big")
        end = offset + size + 12
        if end > len(data):
            break
        kind = data[offset + 4:offset + 8]
        body = data[offset + 8:end - 4]
        if zlib.crc32(kind + body) != int.from_bytes(data[end - 4:end], "big"):
            break
        if number == 0:
            if kind != b"IHDR" or size != 13:
                break
            width, height = struct.unpack(">II", body[:8])
            if (
                not 0 < width <= MAX_IMAGE_SIDE or not 0 < height <= MAX_IMAGE_SIDE
                or width * height > MAX_IMAGE_PIXELS
            ):
                break
        elif kind == b"IHDR":
            break
        if kind in {b"acTL", b"fcTL", b"fdAT"}:
            break
        if kind in {b"IHDR", b"PLTE", b"tRNS", b"IDAT", b"IEND"}:
            output.extend(data[offset:end])
        elif not all(65 <= byte <= 90 or 97 <= byte <= 122 for byte in kind) or not kind[0] & 32:
            break
        if kind == b"IEND":
            if size != 0:
                break
            return bytes(output)
        offset = end
    raise ArtifactImportError("PNG 结构或尺寸不符合导入边界")


def _jpeg_for_decode(data: bytes) -> bytes:
    offset, output, frame = 2, bytearray(b"\xff\xd8"), False
    for _ in range(1024):
        if offset + 4 > len(data) or data[offset] != 255:
            break
        marker = data[offset + 1]
        size = int.from_bytes(data[offset + 2:offset + 4], "big")
        end = offset + size + 2
        if size < 2 or end > len(data):
            break
        body = data[offset + 4:end]
        if marker == 0xDA:
            if frame and data.endswith(b"\xff\xd9"):
                return bytes(output) + data[offset:]
            break
        if marker in {0xC0, 0xC1, 0xC2}:
            if frame or len(body) < 6 or body[0] != 8 or body[5] not in {1, 3, 4}:
                break
            height, width = struct.unpack(">HH", body[1:5])
            if (
                not 0 < width <= MAX_IMAGE_SIDE or not 0 < height <= MAX_IMAGE_SIDE
                or width * height > MAX_IMAGE_PIXELS
            ):
                break
            frame = True
        if marker == 0xE2 and body.startswith(b"MPF\x00"):
            break
        if marker == 0xEE:
            if len(body) != 12 or not body.startswith(b"Adobe"):
                break
            output.extend(data[offset:end])
        elif 0xE0 <= marker <= 0xEF or marker == 0xFE:
            pass
        elif marker in {0xC0, 0xC1, 0xC2, 0xC4, 0xDB, 0xDD}:
            output.extend(data[offset:end])
        else:
            break
        offset = end
    raise ArtifactImportError("JPEG 结构或尺寸不符合导入边界")


def _normalize_image(data: bytes, image_format: str) -> PreparedArtifact:
    try:
        if ImageFile.LOAD_TRUNCATED_IMAGES:
            raise ValueError
        with Image.open(io.BytesIO(data), formats=[image_format]) as image:
            width, height = image.size
            if (
                not 0 < width <= MAX_IMAGE_SIDE or not 0 < height <= MAX_IMAGE_SIDE
                or width * height > MAX_IMAGE_PIXELS or getattr(image, "n_frames", 1) != 1
                or image.format != image_format
            ):
                raise ValueError
            image.verify()
        with Image.open(io.BytesIO(data), formats=[image_format]) as image:
            image.load()
            with image.convert("RGBA") as rgba:
                with Image.frombytes("RGBA", rgba.size, rgba.tobytes()) as clean:
                    with _BoundedOutput() as output:
                        clean.save(output, "PNG")
                        return PreparedArtifact(output.getvalue(), "image/png", width, height)
    except Exception:
        raise ArtifactImportError("图片无法通过有界解码核对") from None


def normalize_image_content(
    data: bytes, *, allow_webp: bool = False, allow_jpeg_trailer: bool = False,
) -> PreparedArtifact:
    """只让显式允许的图片进解码器；Host 的 WebP 仍保持不透明。"""
    if type(data) is not bytes or not 1 <= len(data) <= MAX_ARTIFACT_BYTES:
        raise ArtifactImportError("资产内容大小非法")
    if data.startswith(_PNG_SIGNATURE):
        return _normalize_image(_png_for_decode(data), "PNG")
    if data.startswith(b"\xff\xd8\xff"):
        jpeg = data
        if allow_jpeg_trailer and not jpeg.endswith(b"\xff\xd9"):
            marker = jpeg.rfind(b"\xff\xd9")
            if marker <= 2:
                raise ArtifactImportError("JPEG 结束标记未确认")
            jpeg = jpeg[:marker + 2]
        return _normalize_image(_jpeg_for_decode(jpeg), "JPEG")
    if allow_webp and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        if len(data) < 20 or int.from_bytes(data[4:8], "little") + 8 > len(data):
            raise ArtifactImportError("WEBP 结构或大小不符合导入边界")
        return _normalize_image(data, "WEBP")
    return PreparedArtifact(data)
