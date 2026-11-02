import os
import sys
import logging
import logging.handlers


_loggers: dict[str, logging.Logger] = {}
_initialized: bool = False


def _ensure_log_dir() -> str:
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "logs")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir


def _init_root_logger():
    global _initialized
    if _initialized:
        return
    _initialized = True

    log_dir = _ensure_log_dir()

    root = logging.getLogger("aoao")
    root.setLevel(logging.DEBUG)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    console.setFormatter(console_fmt)
    root.addHandler(console)

    # File handler — daily rotation, keep 30 days
    file_path = os.path.join(log_dir, "aoao.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=file_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)
    root.addHandler(file_handler)

    # Suppress noisy library loggers
    for lib in ("httpx", "httpcore", "openai", "urllib3", "chromadb"):
        logging.getLogger(lib).setLevel(logging.WARNING)

    # [0.2.4.8] 启动版本号打点: 读 VERSION 文件打一行, 日志开头即可确认跑的是哪版代码。
    # 治"改了代码没重启跑旧版"排查困境 (实证 0.2.4.7 后用户没重启, 日志全旧特征)。
    try:
        version_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "VERSION")
        with open(version_path, "r", encoding="utf-8") as f:
            version = f.read().strip() or "unknown"
    except Exception:
        version = "unknown"
    logging.getLogger("aoao.runtime").info(f"[启动] Aoao OS 版本 {version} (日志已就绪, DEBUG级落盘)")


def get_logger(name: str = "") -> logging.Logger:
    _init_root_logger()

    if not name:
        return logging.getLogger("aoao")

    full_name = f"aoao.{name}" if not name.startswith("aoao.") else name
    if full_name not in _loggers:
        _loggers[full_name] = logging.getLogger(full_name)
    return _loggers[full_name]
