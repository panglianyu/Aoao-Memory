import logging
import logging.handlers
import os
import sys
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "apps"))

# [0.3.6.1] 测试日志卫生: 摘掉生产日志文件 handler, pytest 夹具不再污染
# data/logs/aoao.log (老板 2026-07-28 实战痛点: live 启动抓日志满屏测试群/
# Zhang San/MockMagicMock/"LLM 炸了", 真实 boot 段难辨)。控制台 handler
# 保留 (pytest 自捕获), 生产日志只收真实运行。
from kernel import logger as _kernel_logger  # noqa: E402

_kernel_logger._init_root_logger()  # 幂等: 先确保 handler 装好再摘文件 handler
_aoao_root = logging.getLogger("aoao")
for _h in list(_aoao_root.handlers):
    if isinstance(_h, logging.handlers.TimedRotatingFileHandler):
        _h.close()
        _aoao_root.removeHandler(_h)


@pytest.fixture
def sample_jpeg_path(tmp_path):
    """Create a minimal valid JPEG file (>1000 bytes)."""
    path = tmp_path / "test.jpg"
    header = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    body = b"\x00" * 2000
    path.write_bytes(header + body)
    return str(path)


@pytest.fixture
def sample_png_path(tmp_path):
    """Create a minimal valid PNG file (>1000 bytes)."""
    path = tmp_path / "test.png"
    header = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    body = b"\x00" * 2000
    path.write_bytes(header + body)
    return str(path)


@pytest.fixture
def sample_gif_path(tmp_path):
    """Create a minimal valid GIF87a file (>1000 bytes)."""
    path = tmp_path / "test.gif"
    header = b"GIF87a"
    body = b"\x00" * 2000
    path.write_bytes(header + body)
    return str(path)


@pytest.fixture
def sample_tiny_file(tmp_path):
    """Create a file smaller than 1000 bytes (should be rejected)."""
    path = tmp_path / "tiny.jpg"
    path.write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)
    return str(path)


@pytest.fixture
def sample_invalid_file(tmp_path):
    """Create a file with no valid image magic bytes."""
    path = tmp_path / "invalid.bin"
    path.write_bytes(b"not an image file at all" + b"\x00" * 2000)
    return str(path)


@pytest.fixture
def sample_text_file(tmp_path):
    """Create a text file (not an image at all)."""
    path = tmp_path / "readme.txt"
    path.write_text("Hello world")
    return str(path)


@pytest.fixture
def non_existent_path(tmp_path):
    """Return a path that does not exist."""
    return str(tmp_path / "does_not_exist.jpg")
