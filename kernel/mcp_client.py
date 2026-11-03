# G:\Aoao_OS\kernel\mcp_client.py
"""
Aoao OS MCP (Model Context Protocol) 客户端抽象层

封装 MCP SDK v1.15.0 的传输层 (stdio/SSE) 和会话层 (ClientSession)。
负责连接生命周期管理、工具发现、工具调用、Schema 编译。

[架构红线 - 外轨组件]
本模块属于 MCP 外轨基础设施，与 Native 内轨组件 (brain/system_memory_god/feishu) 物理隔离。
所有 I/O 均通过 MCP SDK 的 anyio 异步实现，严禁阻塞主事件循环。
"""
import json
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any, Dict, List, Optional

from mcp import ClientSession, StdioServerParameters, Tool
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import (
    CallToolResult,
    TextContent,
    ImageContent,
    AudioContent,
    ResourceLink,
    EmbeddedResource,
)

from kernel.logger import get_logger

logger = get_logger(__name__)

# MCP 协议单次读取超时 (秒)
_DEFAULT_TIMEOUT: int = 30


class MCPClient:
    """
    MCP 协议客户端，封装单个 MCP Server 的连接与交互。

    支持三种传输协议:
      - stdio: 本地子进程 (STDIN/STDOUT 管道)
      - sse: 远程 HTTP 长连接 (Server-Sent Events)
      - streamable_http: 远程 Streamable HTTP (MCP 2025-03-26 新传输,
        官方注册表 remote 条目多为此类型)

    生命周期:
      connect() -> [call_tool() ...] -> disconnect()
    """

    def __init__(self, server_config: Dict[str, Any]) -> None:
        self._name: str = server_config.get("name", "unknown")
        self._transport: str = server_config.get("transport", "stdio")
        self._config: Dict[str, Any] = server_config
        self._timeout: int = server_config.get("timeout", _DEFAULT_TIMEOUT)

        # 运行时状态
        self._exit_stack: Optional[AsyncExitStack] = None
        self._session: Optional[ClientSession] = None
        self._tools: List[Tool] = []
        self._connected: bool = False

    # ==========================================
    # 属性
    # ==========================================

    @property
    def is_connected(self) -> bool:
        """当前是否已成功建立 MCP 会话。"""
        return self._connected and self._session is not None

    @property
    def tools(self) -> List[Tool]:
        """已缓存的工具列表 (connect 后可用)。"""
        return list(self._tools)

    @property
    def tool_count(self) -> int:
        """已缓存的工具数量。"""
        return len(self._tools)

    @property
    def name(self) -> str:
        return self._name

    @property
    def transport(self) -> str:
        """传输协议类型 (stdio / sse / streamable_http)。"""
        return self._transport

    @property
    def config(self) -> Dict[str, Any]:
        """原始配置字典 (只读引用, 不应修改)。"""
        return self._config

    @property
    def timeout(self) -> int:
        """单次工具调用超时 (秒)。"""
        return self._timeout

    # ==========================================
    # 连接生命周期
    # ==========================================

    async def connect(self) -> bool:
        """
        建立与 MCP Server 的连接。

        流程:
          1. 创建 AsyncExitStack 管理嵌套异步上下文
          2. 根据 transport 类型进入 stdio_client 或 sse_client
          3. 创建 ClientSession 并执行 initialize 握手
          4. 拉取并缓存工具列表

        返回 True 表示连接成功, False 表示失败 (已记录日志并清理资源)。
        """
        try:
            self._exit_stack = AsyncExitStack()

            # 根据传输协议建立底层连接
            if self._transport == "stdio":
                read_stream, write_stream = await self._connect_stdio()
            elif self._transport == "sse":
                read_stream, write_stream = await self._connect_sse()
            elif self._transport in ("streamable_http", "http", "streamable-http"):
                read_stream, write_stream = await self._connect_streamable_http()
            else:
                logger.error(
                    f"[MCP:{self._name}] 不支持的传输协议: {self._transport}"
                )
                await self._cleanup()
                return False

            # 创建会话
            self._session = await self._exit_stack.enter_async_context(
                ClientSession(
                    read_stream,
                    write_stream,
                    read_timeout_seconds=timedelta(seconds=self._timeout),
                )
            )

            # 协议握手
            await self._session.initialize()

            # 拉取工具列表
            tools_result = await self._session.list_tools()
            self._tools = tools_result.tools

            self._connected = True
            logger.info(
                f"[MCP:{self._name}] 连接成功, "
                f"传输={self._transport}, "
                f"可用工具={len(self._tools)}"
            )
            return True

        except Exception as e:
            logger.error(
                f"[MCP:{self._name}] 连接失败: {e}",
                exc_info=True,
            )
            await self._cleanup()
            return False

    async def _connect_stdio(self) -> tuple:
        """
        建立 STDIO 传输连接 (本地子进程)。

        通过 asyncio 兼容的 anyio 子进程管理, MCP SDK 内部使用
        asyncio.create_subprocess_exec 风格的非阻塞 I/O。

        返回 (read_stream, write_stream) 元组。
        """
        params = StdioServerParameters(
            command=self._config.get("command", ""),
            args=self._config.get("args", []),
            env=self._config.get("env") or None,
        )
        read_stream, write_stream = await self._exit_stack.enter_async_context(
            stdio_client(params)
        )
        logger.debug(
            f"[MCP:{self._name}] STDIO 子进程已启动: "
            f"{params.command} {' '.join(params.args)}"
        )
        return read_stream, write_stream

    async def _connect_sse(self) -> tuple:
        """
        建立 SSE 传输连接 (远程 HTTP 长连接)。

        使用 httpx + httpx_sse 实现异步非阻塞 HTTP 通信。

        返回 (read_stream, write_stream) 元组。
        """
        url = self._config.get("url", "")
        headers = self._config.get("headers") or None

        read_stream, write_stream = await self._exit_stack.enter_async_context(
            sse_client(
                url=url,
                headers=headers,
                timeout=float(self._timeout),
            )
        )
        logger.debug(f"[MCP:{self._name}] SSE 连接已建立: {url}")
        return read_stream, write_stream

    async def _connect_streamable_http(self) -> tuple:
        """
        建立 Streamable HTTP 传输连接 (远程, MCP 2025-03-26 新标准)。

        SDK streamablehttp_client 产出三元组 (read, write, get_session_id),
        本层只需前两者, session_id 回调丢弃。headers 走配置 (含 ${VAR}
        环境变量展开后的密钥), 与 sse 分支同源。

        返回 (read_stream, write_stream) 元组。
        """
        url = self._config.get("url", "")
        headers = self._config.get("headers") or None

        read_stream, write_stream, _get_session_id = (
            await self._exit_stack.enter_async_context(
                streamablehttp_client(
                    url=url,
                    headers=headers,
                    timeout=float(self._timeout),
                )
            )
        )
        logger.debug(f"[MCP:{self._name}] Streamable HTTP 连接已建立: {url}")
        return read_stream, write_stream

    async def disconnect(self) -> None:
        """断开连接, 释放所有资源。"""
        if self._connected or self._exit_stack is not None:
            logger.info(f"[MCP:{self._name}] 正在断开连接...")
            await self._cleanup()

    async def _cleanup(self) -> None:
        """物理清理: 关闭 AsyncExitStack, 重置所有状态。"""
        if self._exit_stack is not None:
            try:
                await self._exit_stack.aclose()
            except Exception as e:
                logger.warning(
                    f"[MCP:{self._name}] 资源释放时出现警告: {e}",
                    exc_info=True,
                )
        self._exit_stack = None
        self._session = None
        self._tools = []
        self._connected = False

    # ==========================================
    # 工具管理
    # ==========================================

    async def refresh_tools(self) -> int:
        """
        重新从 MCP Server 拉取工具列表。

        返回更新后的工具数量。未连接时返回 0。
        """
        if not self.is_connected:
            return 0
        tools_result = await self._session.list_tools()
        self._tools = tools_result.tools
        logger.info(
            f"[MCP:{self._name}] 工具列表已刷新, "
            f"当前 {len(self._tools)} 个工具"
        )
        return len(self._tools)

    # ==========================================
    # 工具调用
    # ==========================================

    async def call_tool(
        self,
        tool_name: str,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        调用 MCP Server 暴露的工具。

        返回工具执行结果的文本表示。错误情况返回描述性错误字符串
        (由 Brain 的错误消化机制处理)。
        """
        if not self.is_connected:
            return f"MCP 工具调用失败: {self._name} 未连接"

        try:
            result: CallToolResult = await self._session.call_tool(
                name=tool_name,
                arguments=arguments,
                read_timeout_seconds=timedelta(seconds=self._timeout),
            )

            if result.isError:
                error_text = self._extract_text(result)
                logger.warning(
                    f"[MCP:{self._name}] 工具 {tool_name} 返回错误: {error_text[:200]}"
                )
                return f"MCP 工具错误 [{tool_name}]: {error_text}"

            return self._extract_text(result)

        except TimeoutError:
            msg = f"MCP 工具超时: {tool_name} ({self._timeout}s)"
            logger.warning(f"[MCP:{self._name}] {msg}")
            return msg

        except Exception as e:
            msg = f"MCP 工具调用失败: {tool_name} -> {e}"
            logger.error(f"[MCP:{self._name}] {msg}", exc_info=True)
            return msg

    def _extract_text(self, result: CallToolResult) -> str:
        """
        从 CallToolResult 中提取文本内容。

        处理策略:
          - TextContent: 直接取文本
          - ImageContent: 插入占位符 (图片不直传给 LLM 文本通道)
          - AudioContent: 插入占位符
          - ResourceLink: 插入 URI 引用
          - EmbeddedResource: 尝试提取文本内容
          - 其他类型: 插入类型标记
        """
        parts: List[str] = []
        for block in result.content:
            if isinstance(block, TextContent):
                parts.append(block.text)
            elif isinstance(block, ImageContent):
                parts.append(f"[IMAGE:{block.mimeType}]")
            elif isinstance(block, AudioContent):
                parts.append(f"[AUDIO:{getattr(block, 'mimeType', 'unknown')}]")
            elif isinstance(block, ResourceLink):
                uri = getattr(block, "uri", "unknown")
                parts.append(f"[RESOURCE_LINK:{uri}]")
            elif isinstance(block, EmbeddedResource):
                res = block.resource
                if hasattr(res, "text"):
                    parts.append(res.text)
                elif hasattr(res, "blob"):
                    parts.append(f"[BLOB:{getattr(res, 'mimeType', 'unknown')}]")
                else:
                    parts.append("[EmbeddedResource]")
            else:
                type_name = getattr(block, "type", "unknown")
                parts.append(f"[{type_name}]")

        text = "\n".join(parts) if parts else "(空结果)"
        return text

    # ==========================================
    # Schema 编译 (MCP -> OpenAI 函数调用格式)
    # ==========================================

    def compile_tools_for_openai(self) -> List[Dict[str, Any]]:
        """
        将 MCP 工具列表编译为 OpenAI function-calling JSON Schema 格式。

        MCP inputSchema 和 OpenAI parameters 均为 JSON Schema, 可直接映射。
        外层包装为 OpenAI 要求的 {type: function, function: {...}} 结构。
        """
        openai_tools: List[Dict[str, Any]] = []
        for tool in self._tools:
            openai_tool = {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or tool.name,
                    "parameters": tool.inputSchema,
                },
            }
            openai_tools.append(openai_tool)
        return openai_tools

    # ==========================================
    # Manifest 辅助
    # ==========================================

    def build_manifest_usage(self) -> str:
        """
        根据已缓存的工具列表生成 manifest.usage 字符串。

        格式示例:
          可用工具:
          - command=get_forecast params={"city":"string"} -- 查询天气预报
          - command=get_alerts params={"region":"string"} -- 查询气象预警
        """
        if not self._tools:
            return "暂无可用工具 (未连接)"

        lines = ["可用工具:"]
        for tool in self._tools:
            # 从 inputSchema 提取参数概要
            schema = tool.inputSchema or {}
            props = schema.get("properties", {})
            required = set(schema.get("required", []))

            param_parts = []
            for pname, pinfo in props.items():
                ptype = pinfo.get("type", "any")
                marker = "*" if pname in required else ""
                param_parts.append(f'"{pname}":"{ptype}{marker}"')

            params_str = "{" + ", ".join(param_parts) + "}" if param_parts else "{}"
            desc = tool.description or ""
            lines.append(f"  - command={tool.name} params={params_str} -- {desc}")

        return "\n".join(lines)
