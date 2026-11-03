# -*- coding: utf-8 -*-
"""统一媒体资产落盘层 (kernel 级纯函数模块, 非 app)。

任何组件 (微信/飞书/截屏/生图/未来浏览器) 产出媒体文件, 统一调 save()
落盘到 data/assets/inbox/%Y%m%d/。不分类 -- 分类由阿露通过
local_file_manager 的 create_folder/move_file 自主决定。

设计原则:
- 极薄: 只做"安全写入 + 防重名 + 返绝对路径", 不做分类/索引/清理
- 无状态: 纯函数, 无类实例, 无副作用 (除文件系统写入)
- 来源追溯: 文件名前缀标来源 (wechat_/feishu_/screen_/gen_), 方便阿露识别
"""
import os
import hashlib
import re
import stat
import shutil
import tempfile
import time
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)

# 资产库根目录 (相对项目根)
ASSETS_ROOT = os.path.join("data", "assets")
# 收件箱: 所有新资产先落这里, 阿露自己归类
INBOX_DIR = os.path.join(ASSETS_ROOT, "inbox")


def _project_root() -> str:
    """项目根目录 (kernel/ 的上级)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _today_dir() -> str:
    """今日收件箱子目录: data/assets/inbox/%Y%m%d/."""
    return os.path.join(INBOX_DIR, time.strftime("%Y%m%d"))


def _safe_filename(prefix: str, original_name: str) -> str:
    """生成安全文件名: 来源前缀 + 时间戳 + 原名 (截断+消毒).

    例: wechat_1721750400_照片.jpg
    """
    # 消毒: 只保留字母数字下划线点横线, 截断到 80 字符
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in original_name)
    safe = safe[:80] if safe else "unnamed"
    ts = int(time.time())
    return f"{prefix}_{ts}_{safe}"


def save_from_path(
    source_path: str,
    source: str = "misc",
    original_name: Optional[str] = None,
) -> str:
    """把已有文件 copy 到资产库收件箱, 返回新绝对路径。

    Args:
        source_path: 源文件绝对路径 (必须存在)。
        source: 来源标识 (wechat/feishu/screen/gen/misc), 用于文件名前缀。
        original_name: 原始文件名 (可选, 默认取 source_path 的 basename)。

    Returns:
        资产库中的绝对路径。

    Raises:
        FileNotFoundError: 源文件不存在。
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"源文件不存在: {source_path}")

    root = _project_root()
    dest_dir = os.path.join(root, _today_dir())
    os.makedirs(dest_dir, exist_ok=True)

    orig = original_name or os.path.basename(source_path)
    dest_name = _safe_filename(source, orig)
    dest_path = os.path.join(dest_dir, dest_name)

    # 防重名: 已存在则追加序号
    if os.path.exists(dest_path):
        base, ext = os.path.splitext(dest_path)
        for i in range(1, 100):
            candidate = f"{base}_{i}{ext}"
            if not os.path.exists(candidate):
                dest_path = candidate
                break

    shutil.copy2(source_path, dest_path)
    logger.info("[AssetStore] 资产复制完成。")
    return dest_path


def save_from_bytes(
    data: bytes,
    filename: str,
    source: str = "misc",
) -> str:
    """把内存数据写入资产库收件箱, 返回绝对路径。

    Args:
        data: 文件内容字节。
        filename: 文件名 (含扩展名)。
        source: 来源标识。

    Returns:
        资产库中的绝对路径。
    """
    root = _project_root()
    dest_dir = os.path.join(root, _today_dir())
    os.makedirs(dest_dir, exist_ok=True)

    dest_name = _safe_filename(source, filename)
    dest_path = os.path.join(dest_dir, dest_name)

    if os.path.exists(dest_path):
        base, ext = os.path.splitext(dest_path)
        for i in range(1, 100):
            candidate = f"{base}_{i}{ext}"
            if not os.path.exists(candidate):
                dest_path = candidate
                break

    with open(dest_path, "wb") as f:
        f.write(data)
    logger.info("[AssetStore] 资产写入完成。")
    return dest_path


def _verified_file_matches(path: str, expected_sha256: str, size_bytes: int) -> bool:
    """核对一个既有普通文件；不跟随符号链接接受外部替换。"""
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            return False
        with open(path, "rb") as stream:
            before = os.fstat(stream.fileno())
            hasher = hashlib.sha256()
            while chunk := stream.read(1024 * 1024):
                hasher.update(chunk)
            digest = hasher.hexdigest()
            after = os.fstat(stream.fileno())
        return (
            before.st_size == size_bytes
            and after.st_size == size_bytes
            and before.st_mtime_ns == after.st_mtime_ns
            and digest == expected_sha256
        )
    except OSError:
        return False


def save_verified_from_bytes(
    data: bytes,
    filename: str,
    source: str = "misc",
    *,
    stable_key: str,
    expected_sha256: str,
) -> str:
    """以稳定键原子保存已核验字节；同键只允许完全相同的内容。

    普通 ``save_from_bytes`` 继续保留按日期和防重名的旧语义。本入口只供已经
    具有可信本机幂等键和内容摘要的适配器使用，稳定资产直接位于原 inbox 下，
    不创建索引或旁路图库。

    Raises:
        ValueError: 参数或内容摘要不匹配。
        FileExistsError: 稳定键已被不同内容或非普通文件占用。
    """
    if type(data) is not bytes or type(filename) is not str or not filename:
        raise ValueError("稳定资产内容或文件名非法")
    if (
        type(stable_key) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", stable_key) is None
        or type(expected_sha256) is not str
        or re.fullmatch(r"[a-f0-9]{64}", expected_sha256) is None
    ):
        raise ValueError("稳定资产键或摘要非法")
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError("稳定资产内容摘要不匹配")
    safe_source = "".join(c if (c.isalnum() or c in "_-") else "_" for c in source)[:32]
    if not safe_source:
        safe_source = "misc"
    extension = os.path.splitext(os.path.basename(filename))[1].lower()
    if re.fullmatch(r"\.[a-z0-9]{1,10}", extension) is None:
        extension = ".bin"
    dest_dir = os.path.join(_project_root(), INBOX_DIR)
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.abspath(os.path.join(dest_dir, f"{safe_source}_{stable_key}{extension}"))
    if os.path.lexists(dest_path):
        if _verified_file_matches(dest_path, expected_sha256, len(data)):
            return dest_path
        raise FileExistsError("稳定资产键已被不同内容占用")

    descriptor, temp_path = tempfile.mkstemp(prefix=".asset_", suffix=".tmp", dir=dest_dir)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if not _verified_file_matches(temp_path, expected_sha256, len(data)):
            raise OSError("稳定资产临时文件核验失败")
        try:
            os.link(temp_path, dest_path)
        except FileExistsError:
            if not _verified_file_matches(dest_path, expected_sha256, len(data)):
                raise FileExistsError("稳定资产键并发写入不同内容") from None
        logger.info("[AssetStore] 稳定资产写入或复用完成。")
        return dest_path
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def get_assets_root() -> str:
    """返回资产库根目录绝对路径 (供 local_file_manager 搜索)."""
    return os.path.join(_project_root(), ASSETS_ROOT)


def get_inbox_dir() -> str:
    """返回收件箱根目录绝对路径."""
    return os.path.join(_project_root(), INBOX_DIR)
