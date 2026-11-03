# G:\Aoao_OS\kernel\mcp_adapter.py
"""
Aoao OS MCP 适配器 -- 将单个 MCP Server 桥接为标准 AoaoPlug 插件

每个 MCPServerAdapter 实例对应一个外部 MCP Server。
对 AppBus 而言, 它与 Native 插件 (system_clock 等) 外观完全一致。

[架构红线 - 外轨组件]
本模块属于 MCP 外轨, 绝不修改 Native 组件 (brain/app_bus/system_memory) 源码。
MCP 工具名称使用 mcp_ 前缀隔离, 防止与 Native 组件名称冲突。

数据流:
  AppBus.send_instruction("mcp_weather", "get_forecast", **kwargs)
    -> MCPServerAdapter.execute_command("get_forecast", **kwargs)
      -> _extract_tool_args(kwargs)  # 三层参数提取
      -> MCPClient.call_tool("get_forecast", arguments)
        -> MCP SDK -> 外部 MCP Server
      -> 返回结果字符串
"""
import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from protocols.standard_plug import AoaoPlug
from kernel.mcp_client import MCPClient
from kernel.logger import get_logger

if TYPE_CHECKING:
    from kernel.message import Message

logger = get_logger(__name__)


class MCPServerAdapter(AoaoPlug):
    """
    MCP Server 适配器, 将单个 MCP Server 包装为 AppBus 可管理的插件。

    对 AppBus 暴露的接口:
      - manifest: 包含 mcp_<name> 标识符、工具描述、使用说明
      - on_connect: 启动 MCP 子进程 / 建立 HTTP 连接
      - on_disconnect: 断开连接, 释放资源
      - execute_command: 将 command 映射为 MCP tool call
    """

    def __init__(self, server_config: Dict[str, Any]) -> None:
        super().__init__()
        self._server_name: str = server_config.get("name", "unknown")
        self._app_name: str = f"mcp_{self._server_name}"
        self._description: str = server_config.get(
            "description", f"MCP 外部工具: {self._server_name}"
        )
        self._client: MCPClient = MCPClient(server_config)

    # ==========================================
    # 公开只读属性 (供网关管理指令使用)
    # ==========================================

    @property
    def server_name(self) -> str:
        """原始 Server 名称 (不含 mcp_ 前缀)。"""
        return self._server_name

    @property
    def transport(self) -> str:
        """传输协议类型 (stdio 或 sse)。"""
        return self._client.transport

    @property
    def tool_count(self) -> int:
        """当前已缓存的工具数量。"""
        return self._client.tool_count

    @property
    def timeout(self) -> int:
        """单次工具调用超时 (秒)。"""
        return self._client.timeout

    @property
    def server_config(self) -> Dict[str, Any]:
        """原始 Server 配置字典 (只读引用)。"""
        return self._client.config

    # ==========================================
    # 标准接口: manifest
    # ==========================================

    @property
    def manifest(self) -> Dict[str, Any]:
        """
        返回插件说明书。

        连接前: 使用配置中的静态描述。
        连接后: [0.2.11.0] 生成 commands 列表 (从 MCP tool inputSchema),
        让 ToolBuilder 走 enum 分支 -- command 参数类型化为 enum 选择器,
        每个 MCP 工具的参数名/类型/必填写进 per-command doc, 治散文降级
        下模型猜参数名幻觉调用。
        """
        if self._client.is_connected and self._client.tools:
            commands = self._build_commands_from_tools()
            desc = f"{self._description} [MCP 外轨, {len(commands)} 个工具已就绪]"
            return {
                "name": self._app_name,
                "description": desc,
                "commands": commands,
            }
        else:
            desc = f"{self._description} [MCP 外轨, 未连接]"
            return {
                "name": self._app_name,
                "description": desc,
                "usage": (
                    "连接后可用。使用 connect_app 启动后, "
                    "通过 instruct_app 发送 command=工具名, params=JSON参数。"
                ),
            }

    def _build_commands_from_tools(self) -> list:
        """[0.2.11.0] 把 MCP tool 列表转成 manifest commands 格式。

        每个 MCP tool -> {name, desc, params}:
        - name: MCP tool 名 (如 get_forecast)
        - desc: MCP tool description
        - params: 从 inputSchema 提取的参数概要 (如 "city:string* units:string")
          * 号标必填, 与 build_manifest_usage 同约定。
        """
        commands = []
        for tool in self._client.tools:
            schema = tool.inputSchema or {}
            props = schema.get("properties", {})
            required_set = set(schema.get("required", []))
            param_parts = []
            for pname, pinfo in props.items():
                ptype = pinfo.get("type", "any")
                marker = "*" if pname in required_set else ""
                param_parts.append(f"{pname}:{ptype}{marker}")
            params_str = " ".join(param_parts) if param_parts else "无"
            commands.append({
                "name": tool.name,
                "desc": tool.description or "",
                "params": params_str,
            })
        return commands

    # ==========================================
    # 标准接口: 生命周期
    # ==========================================

    async def on_connect(self) -> bool:
        """
        通电: 建立 MCP 连接, 拉取工具列表。

        返回 True 表示成功, False 表示失败 (AppBus 会报告启动失败)。
        """
        logger.info(f"[MCP:{self._server_name}] 正在通电, 建立 MCP 连接...")
        success = await self._client.connect()

        if success:
            self.is_connected = True
            tools = self._client.tools
            tool_names = [t.name for t in tools]
            logger.info(
                f"[MCP:{self._server_name}] 通电成功, "
                f"可用工具: {tool_names}"
            )
            return True

        logger.error(f"[MCP:{self._server_name}] 通电失败")
        return False

    async def on_disconnect(self) -> None:
        """断电: 断开 MCP 连接, 释放子进程/HTTP 资源。"""
        logger.info(f"[MCP:{self._server_name}] 正在断电, 释放 MCP 资源...")
        await self._client.disconnect()
        self.is_connected = False
        logger.info(f"[MCP:{self._server_name}] 断电完成")

    # ==========================================
    # 标准接口: 指令执行
    # ==========================================

    async def execute_command(
        self,
        command: str,
        msg: Optional["Message"] = None,
        **kwargs: Any,
    ) -> Any:
        """
        执行指令。

        管理指令 (下划线前缀):
          _list_tools   -- 列出所有可用工具
          _refresh_tools -- 重新拉取工具列表
          _status       -- 连接状态报告

        MCP 工具调用:
          command 即为 MCP 工具名, kwargs 经三层提取后作为工具参数。
        """
        # 管理指令路由
        if command.startswith("_"):
            return await self._handle_management(command)

        # MCP 工具调用
        if not self._client.is_connected:
            return f"MCP 服务 {self._app_name} 未连接, 请先使用 connect_app 启动。"

        # 验证工具是否存在
        tool_names = [t.name for t in self._client.tools]
        if command not in tool_names:
            return (
                f"MCP 工具 '{command}' 不存在于 {self._app_name}。"
                f"可用工具: {', '.join(tool_names)}。"
                f"请使用 instruct_app(command='_list_tools') 查看详情。"
            )

        # 三层参数提取
        arguments = self._extract_tool_args(kwargs)
        logger.debug(
            f"[MCP:{self._server_name}] 调用工具: {command}, "
            f"参数: {json.dumps(arguments, ensure_ascii=False)}"
        )

        return await self._client.call_tool(command, arguments)

    # ==========================================
    # 管理指令
    # ==========================================

    async def _handle_management(self, command: str) -> str:
        """处理内部管理指令。"""
        if command == "_list_tools":
            return self._format_tool_list()

        if command == "_refresh_tools":
            if not self._client.is_connected:
                return f"{self._app_name} 未连接, 无法刷新。"
            try:
                count = await self._client.refresh_tools()
                return (
                    f"工具列表已刷新, 当前 {count} 个工具:\n"
                    + self._format_tool_list()
                )
            except Exception as e:
                return f"刷新工具列表失败: {e}"

        if command == "_status":
            return self._format_status()

        return f"未知管理指令: {command}。可用: _list_tools, _refresh_tools, _status"

    def _format_tool_list(self) -> str:
        """格式化已缓存的工具列表。"""
        tools = self._client.tools
        if not tools:
            return f"{self._app_name}: 暂无可用工具"

        lines = [f"{self._app_name} 可用工具 ({len(tools)} 个):"]
        for tool in tools:
            schema = tool.inputSchema or {}
            props = schema.get("properties", {})
            required = set(schema.get("required", []))

            param_parts = []
            for pname, pinfo in props.items():
                ptype = pinfo.get("type", "any")
                marker = " (必填)" if pname in required else ""
                param_parts.append(f"{pname}:{ptype}{marker}")

            params_str = ", ".join(param_parts) if param_parts else "无参数"
            desc = tool.description or ""
            lines.append(f"  - {tool.name}({params_str}): {desc}")

        return "\n".join(lines)

    def _format_status(self) -> str:
        """格式化连接状态报告。"""
        state = "已连接" if self._client.is_connected else "未连接"
        return (
            f"[{self._app_name}] 状态报告\n"
            f"  连接状态: {state}\n"
            f"  传输协议: {self.transport}\n"
            f"  工具数量: {self.tool_count}\n"
            f"  超时设置: {self.timeout}s"
        )

    # ==========================================
    # 三层参数提取策略
    # ==========================================

    def _extract_tool_args(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        三层参数提取策略, 应对 brain.py 的 params 字符串解析行为。

        brain.py (行 325-331) 解析 LLM 返回的 params 字符串:
          - 若 params 含 "=" 且不含 "|": 按第一个 "=" 拆分为 key=value
          - 否则: 整个 params 字符串塞入 kwargs["text"]

        本方法逐层尝试还原 LLM 的原始参数意图:

        Layer 1: kwargs 仅含 "text" 键且值为合法 JSON -> 直接解析
          例: kwargs = {"text": '{"city":"Beijing"}'} -> {"city": "Beijing"}

        Layer 2: 检测 brain.py "=" 拆分器损坏的 JSON
          例: kwargs = {'{"query"': '"a=b"}'} -> 重建 -> {"query": "a=b"}

        Layer 3: 直接透传 kwargs (移除 msg 键)
          例: kwargs = {"city": "Beijing"} -> {"city": "Beijing"}
        """
        # 移除可能残留的 msg 键
        clean_kwargs = {k: v for k, v in kwargs.items() if k != "msg"}

        if not clean_kwargs:
            return {}

        # Layer 1: 单 "text" 键 + JSON 字符串
        if len(clean_kwargs) == 1 and "text" in clean_kwargs:
            raw = clean_kwargs["text"]
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        return parsed
                except (json.JSONDecodeError, ValueError):
                    pass
            # text 不是 JSON, 作为单参数传递
            return {"text": raw}

        # Layer 2: 检测被 "=" 拆分器损坏的 JSON
        # 特征: 存在以 { 或 " 开头的键名
        mangled_keys = [
            k for k in clean_kwargs
            if isinstance(k, str) and k.strip().startswith(("{", '"'))
        ]
        if mangled_keys:
            reconstructed = self._reconstruct_mangled_kwargs(clean_kwargs)
            if reconstructed is not None:
                return reconstructed

        # Layer 3: 直接透传
        return clean_kwargs

    @staticmethod
    def _reconstruct_mangled_kwargs(
        kwargs: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        尝试重建被 brain.py "=" 拆分器损坏的 JSON 参数。

        brain.py 对 '{"query":"a=b"}' 的处理:
          split("=", 1) -> ['{"query":"a', 'b"}']
          结果: kwargs = {'{"query":"a': 'b"}'}

        重建策略: 用 "=" 重新拼接所有键值对, 尝试 JSON 解析。
        """
        parts = []
        for k, v in kwargs.items():
            parts.append(f"{k}={v}")
        reconstructed = "=".join(parts)

        try:
            parsed = json.loads(reconstructed)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass

        return None
