import os
import time
import glob
import asyncio
import threading

from kernel.logger import get_logger

logger = get_logger(__name__)


class FileIO:
    """
    日志归档层。仅负责 journal 日志的追加与冷数据检索。

    [次世代大修] 历史遗留的 SOUL.md / RULES.md 双写逻辑已物理清除:
      - 灵魂身份已迁移到 EntityStore (entities.json, 由 SystemMemory.read_soul_async 读取)
      - 性格规矩已迁移到 RuleStore (rules.json)
    旧的 append_soul/rewrite_soul/read_rules/add_rule/remove_rule 及对应的幻影文件
    创建逻辑全部删除, 消除"三套文件并存"的误导。
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._journal_lock = threading.Lock()

    # === [日志操作] ===
    def append_journal(self, text: str):
        """追加 UTF-8 日志并返回稳定的 ``文件名#字节偏移`` 引用。"""
        basename = f"journal_{time.strftime('%Y-%m-%d')}.txt"
        today_log = os.path.join(self.data_dir, basename)
        payload = f"[{time.strftime('%H:%M:%S')}] {text}\n".encode("utf-8")
        with self._journal_lock:
            os.makedirs(self.data_dir, exist_ok=True)
            with open(today_log, "ab") as f:
                offset = f.tell()
                f.write(payload)
                f.flush()
        return f"{basename}#offset{offset}"

    async def count_journal_entries(self) -> int:
        """统计非空日志行；文件扫描移出事件循环。"""
        def _count():
            total = 0
            for path in glob.glob(os.path.join(self.data_dir, "journal_*.txt")):
                try:
                    with open(path, "rb") as f:
                        total += sum(1 for line in f if line.strip())
                except OSError:
                    continue
            return total
        return await asyncio.to_thread(_count)

    def read_recent_journal(self, limit: int) -> str:
        today_log = os.path.join(self.data_dir, f"journal_{time.strftime('%Y-%m-%d')}.txt")
        if not os.path.exists(today_log): return "无记录"
        with open(today_log, 'r', encoding='utf-8') as f: return "".join(f.readlines()[-limit:])

    async def deep_archive_search(self, keyword: str, limit: int = 10) -> str:
        if not keyword:
            return "失败：需关键词"
        # P4: 同步扫描包 asyncio.to_thread 防阻塞事件循环; for line in f 流式读替 readlines() 全量入内存;
        # limit 参数传入修复调用方解析了却从不使用的死代码。
        def _do_search():
            files = sorted(glob.glob(os.path.join(self.data_dir, "journal_*.txt")), reverse=True)
            results = []
            for file_path in files:
                date_str = os.path.basename(file_path).replace("journal_", "").replace(".txt", "")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if keyword in line:
                                results.append(f"[{date_str}] {line.strip()}")
                                if len(results) >= limit:
                                    return "冷数据档案:\n" + "\n".join(results)
                except (UnicodeDecodeError, PermissionError, OSError) as e:
                    logger.error(f"[FileIO] 读取日志文件 {file_path} 失败: {e}")
                    continue
            if results:
                return "冷数据档案:\n" + "\n".join(results)
            return ""
        return await asyncio.to_thread(_do_search)
