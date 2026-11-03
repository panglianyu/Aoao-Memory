# G:\Aoao_OS\protocols\memory_provider.py
"""
MemoryProvider 协议接口 -- Brain 依赖的记忆读取抽象。

设计目的:
  解除 Brain 与 system_memory 的硬耦合。Brain 不再硬编码 "system_memory" 名称，
  而是通过 manifest["memory_provider"] = True 自动发现记忆后端。
  任何实现了这些命令的 app 都可以作为记忆提供者。

使用方式:
  - 插件在 manifest 中声明 "memory_provider": True
  - Brain 在启动时通过 _resolve_memory_provider() 自动发现
  - Brain 通过 bus.send_instruction(provider_name, command) 调用记忆命令
  - 如果找不到 memory_provider，Brain 优雅降级（跳过记忆注入，不崩溃）
"""
from typing import Protocol, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from kernel.daily_memory_contracts import DailyRecallOutcome, MemorySaveReceipt
    from kernel.raw_message_models import MessageDraft, Scope, StoredMessage


@runtime_checkable
class MemoryProvider(Protocol):
    """
    记忆提供者协议。任何 app 只要通过 execute_command 支持以下命令，
    即可作为 Brain 的记忆后端，无需显式继承此类。

    必须支持的命令:
      - read_soul:         读取核心身份记忆（实体图谱核心事实）
      - spotlight_search:  基于用户输入的实体匹配 + 规则扫描
      - smart_recall:      向量语义检索（ChromaDB 查询）
      - get_time_anchor:   当前时间感知 + 下一个提醒倒计时
      - archival_search:   全量日志关键字搜索（grep across journals）
    """
    pass


@runtime_checkable
class MemoryServiceContract(Protocol):
    """Trusted in-process API; not an authentication mechanism or LLM tool."""

    profile_id: str
    scopes: tuple["Scope", ...]

    async def add(self, scope: "Scope", original: "MessageDraft") -> "MemorySaveReceipt": ...
    async def replay(self, scope: "Scope", **options) -> list["StoredMessage"]: ...
    async def search(self, scope: "Scope", query: str, **options) -> "DailyRecallOutcome": ...
    async def status(self, scope: "Scope", *, deadline: float) -> dict: ...
    async def wait_until_published(self, scope: "Scope", *, deadline: float) -> dict: ...
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def health(self) -> dict: ...
