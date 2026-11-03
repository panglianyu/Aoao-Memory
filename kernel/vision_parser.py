# G:\Aoao_OS\kernel\vision_parser.py
import re
import base64
import os
import asyncio
from collections import OrderedDict
from typing import Any

from kernel.logger import get_logger
from kernel.image_utils import detect_image_mime, resize_image_bytes
from protocols.wechat_image_content import normalize_image_content

logger = get_logger(__name__)


def _valid_small_image(path: str) -> bool:
    """小 PNG/JPEG 不等于坏图；复用有界解码检查而非单凭字节数拒绝。"""
    try:
        with open(path, "rb") as stream:
            data = stream.read(1000)
        return normalize_image_content(data).content_type == "image/png"
    except (OSError, ValueError):
        return False


class VisionMiddleware:
    # 图片 base64 缓存: path -> (mtime, data_url), LRU 上限 10 张
    _cache: OrderedDict = OrderedDict()
    _CACHE_MAX = 10

    # [0.3.13.0 P0 架构审查整改] 路径围栏根: 仅允许这些目录内的本地图片注入。
    # 默认仅项目母文件夹 (代码集中管理红线: 系统可信资产不应在母文件夹之外);
    # realpath 比对防 ../ / junction / symlink 逃逸; 用户文本与 LLM 输出不得
    # 借此协议到达任意文件系统。测试经此属性放宽 (夹具 monkeypatch)。
    _FENCE_ROOTS: tuple = (
        os.path.realpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )

    @classmethod
    def _fence_allowed(cls, path: str) -> bool:
        """路径围栏判定: realpath 落在任一 _FENCE_ROOTS 内才放行。"""
        try:
            real = os.path.realpath(path)
        except (OSError, ValueError):
            return False
        return any(real == root or real.startswith(root + os.sep) for root in cls._FENCE_ROOTS)

    @classmethod
    def _cache_get(cls, path: str) -> str:
        entry = cls._cache.get(path)
        if not entry:
            return ""
        cached_mtime, data_url = entry
        try:
            current_mtime = os.path.getmtime(path)
        except OSError:
            cls._cache.pop(path, None)
            return ""
        if abs(current_mtime - cached_mtime) < 0.1:
            # 命中：移到末尾 (LRU)
            cls._cache.move_to_end(path)
            return data_url
        cls._cache.pop(path, None)
        return ""

    @classmethod
    def _cache_set(cls, path: str, data_url: str) -> None:
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return
        if path in cls._cache:
            cls._cache.move_to_end(path)
        else:
            if len(cls._cache) >= cls._CACHE_MAX:
                cls._cache.popitem(last=False)
            cls._cache[path] = (mtime, data_url)

    @classmethod
    async def _get_image_mime(cls, path: str) -> str:
        """
        通过文件头 Magic Bytes 判断真实图片格式，返回 MIME 类型或空字符串。
        实际判定逻辑下沉到 kernel.image_utils, 与视觉记忆编码复用同一份 magic 表,
        彻底杜绝 MIME 写死 jpeg。
        """
        def _read_header():
            try:
                with open(path, "rb") as f:
                    return f.read(12)
            except (OSError, PermissionError):
                return None
        header = await asyncio.to_thread(_read_header)
        if header is None:
            return ""
        return detect_image_mime(header)

    @classmethod
    async def inject_vision(cls, messages: list) -> list:
        # [P1 0.2.4.6 修复] 历史轮 user 图片标签抹除: 只保留最后一条 user 的图片注入,
        # 之前的 user 消息图片标签替换为占位 (治每轮重复注入旧图片干扰主脑; 实证
        # aoao.log 21:11-21:15 同三张 emoji 每轮 think 都重新 base64 注入). 当轮保留.
        last_user_idx = -1
        for i, msg in enumerate(messages):
            role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", "user")
            if role == "user":
                last_user_idx = i

        processed_messages = []
        for i, msg in enumerate(messages):
            # 兼容字典和对象格式
            if isinstance(msg, dict):
                content = msg.get("content", "")
                role = msg.get("role", "user")
            else:
                content = getattr(msg, "content", "")
                role = getattr(msg, "role", "user")

            if content is None:
                content = ""

            # 工具循环已归一为 dict；只改内容，不丢调用/回执关联字段，也不改原消息。
            envelope = dict(msg) if isinstance(msg, dict) else {"role": role}

            # 历史记录里的 assistant / system 角色禁止塞图片，直接抹除标签
            if role in ["system", "assistant"]:
                if isinstance(content, str) and "[LOCAL_IMAGE:" in content:
                    content = re.sub(r"\[LOCAL_IMAGE:.+?\]", "[图片已发送]", content)
                processed_messages.append({**envelope, "content": content})
                continue

            # [P1] 历史轮 user (非最后一条) 图片标签抹除, 当轮 user 保留注入.
            # tool 角色始终保留 (工具返回非对话轮, 无重复注入问题).
            is_history_user = (role == "user" and i != last_user_idx)
            if is_history_user and isinstance(content, str) and "[LOCAL_IMAGE:" in content:
                content = re.sub(r"\[LOCAL_IMAGE:.+?\]", "[图片已发送]", content)
                processed_messages.append({**envelope, "content": content})
                continue

            # 只有 user 和 tool 角色可以合法注入原生视觉信号
            if isinstance(content, str) and "[LOCAL_IMAGE:" in content:
                img_paths = re.findall(r"\[LOCAL_IMAGE:(.+?)\]", content)
                if not img_paths:
                    processed_messages.append({**envelope, "content": content})
                    continue

                clean_text = re.sub(r"\[LOCAL_IMAGE:.+?\]", "", content).strip()
                if not clean_text:
                    clean_text = "请看这张图片。"

                new_content: list[dict[str, Any]] = [{"type": "text", "text": clean_text}]

                for path in img_paths:
                    path = path.strip()
                    if not cls._fence_allowed(path):
                        logger.warning(f"[视觉网关] 路径围栏拒绝越界图片 (不在项目母文件夹内): {os.path.basename(path)}")
                        new_content.append({"type": "text", "text": "\n（图片路径越界，已拒绝注入）"})
                        continue
                    if not os.path.exists(path):
                        continue

                    # 文件大小初筛
                    size = await asyncio.to_thread(os.path.getsize, path)
                    if size < 1000 and not await asyncio.to_thread(_valid_small_image, path):
                        new_content.append({"type": "text", "text": "\n（图片损坏或下载失败）"})
                        continue

                    # 严格的 Magic Bytes 格式验证
                    mime = await cls._get_image_mime(path)
                    if not mime:
                        logger.warning(f"[视觉网关] 文件格式非法，拒绝注入: {os.path.basename(path)}")
                        new_content.append({"type": "text", "text": "\n（图片格式非法，无法识别）"})
                        continue

                    try:
                        # P5: 先查 LRU 缓存 (path+mtime), 命中跳过读文件+base64 编码
                        cached = cls._cache_get(path)
                        if cached:
                            data_url = cached
                        else:
                            def _encode_image():
                                with open(path, "rb") as img_file:
                                    raw = img_file.read()
                                # P1: 超过 10MB 自动缩放 (resize_image_bytes 内部判定)
                                raw = resize_image_bytes(raw, mime)
                                # 缩放后格式可能变 JPEG, 重新探测真实 mime 防前缀错配
                                final_mime = detect_image_mime(raw[:12]) or mime
                                return final_mime, base64.b64encode(raw).decode("utf-8")
                            final_mime, base64_data = await asyncio.to_thread(_encode_image)
                            data_url = f"data:{final_mime};base64,{base64_data}"
                            cls._cache_set(path, data_url)
                        new_content.append({
                            "type": "image_url",
                            "image_url": {"url": data_url}
                        })
                        logger.info(f"[视觉网关] 已合法为 '{role}' 注入原生视觉信号 [{mime}]: {os.path.basename(path)}")
                    except Exception as e:
                        logger.error(f"[视觉网关] 图片转码异常: {e}")

                processed_messages.append({**envelope, "content": new_content})
            else:
                processed_messages.append(msg)

        return processed_messages
