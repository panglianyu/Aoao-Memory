# G:\Aoao_OS\kernel\tool_builder.py
"""
ToolBuilder -- 从已注册应用生成 OpenAI function-calling 工具定义。

[次世代工具树 v2] 取代旧的单一不透明 RPC instruct_app。
为每个可见 app 生成独立工具 use_<appname>(command: enum, params),
把藏在 execute_command if/elif 分支里的命令契约显式化为 enum, 让 LLM 不再瞎猜命令名。

设计原则:
  - 通过 manifest["hidden"] 判断是否对 LLM 隐藏 (不再硬编码 app 名)。
  - 优先读 manifest["commands"] 结构化声明; 无声明则降级回 description/usage 散文 (渐进迁移)。
  - 保留 instruct_app 作为隐藏兼容别名, 不破坏既有模型行为记忆。
"""
from typing import Any, Dict, List

from kernel.logger import get_logger

logger = get_logger(__name__)

# per-app 工具函数名前缀 (dispatch 路由此前缀解析 app 名)
APP_TOOL_PREFIX = "use_"


def tools_for_model(tools: list, model_name: str) -> list:
    """Compatibility API; public research is an ordinary provider-independent app."""
    return tools


def tool_name_for_app(app_name: str) -> str:
    """
    把 app 名清洗为合法的 OpenAI function 名 (只允许 a-z A-Z 0-9 _ -)。
    非 ASCII (如中文) 会被替换为下划线。
    """
    return APP_TOOL_PREFIX + "".join(
        ch if (ch.isascii() and (ch.isalnum() or ch in "_-")) else "_"
        for ch in app_name
    )


# 旧导入点兼容；新代码统一使用公开名称。
_sanitize_tool_name = tool_name_for_app


class ToolBuilder:
    """
    从 AppBus 的已注册应用生成 OpenAI function-calling 工具定义。

    设计原则:
    - 通过 manifest["hidden"] 字段判断是否隐藏, 不再硬编码 app 名。
    - 工具定义是纯数据生成, 不依赖 AppBus 实例状态。
    """

    def __init__(self):
        self._force_hidden: set = set()

    def force_hide(self, app_name: str):
        """强制隐藏某个 app (兼容旧的硬编码黑名单逻辑)。"""
        self._force_hidden.add(app_name)

    def _is_hidden(self, name: str, manifest: dict) -> bool:
        """判断一个 app 是否应该对 LLM 隐藏。"""
        if name in self._force_hidden:
            return True
        return manifest.get("hidden", False)

    def visible_app_names(self, available_apps: dict) -> List[str]:
        """返回允许进入工具树和系统提示的应用名。"""
        return [
            name
            for name, app in available_apps.items()
            if not self._is_hidden(name, app.manifest)
        ]

    def collect_app_rules(self, available_apps: dict) -> str:
        """
        从已注册应用的 manifest 中收集 "rules" 字段, 拼接为系统提示文本。

        每个 app 可以在 manifest 中声明 "rules" 字段, 描述该设备的使用须知。
        这些规则会自动注入到 system prompt 中, 任何加载该 app 的人格都会获得。

        Returns:
            拼接后的规则文本, 无规则时返回空字符串
        """
        rules_list = []
        for name, app in available_apps.items():
            manifest = app.manifest
            if self._is_hidden(name, manifest):
                continue
            rule = manifest.get("rules", "")
            if rule:
                rules_list.append(f"- 【{name}】: {rule}")
        return "\n".join(rules_list)

    @staticmethod
    def _render_command_doc(commands: List[Dict[str, Any]]) -> str:
        """把 manifest["commands"] 列表渲染为逐命令的文档字符串。

        [0.3.27.0 阶段3 规格五.3] 命令级可选元数据渲染:
        - risk: 高风险标注 (high 提醒谨慎);
        - timeout: 执行超时提示;
        - parallel_safe: 只读工具声明后才允许并发;
        - idempotent: 声明支持幂等键;
        - progress_label: 状态栏安全描述 (工具参数绝不原样进状态栏)。
        """
        lines = []
        for cmd in commands:
            name = cmd.get("name", "?")
            desc = cmd.get("desc", "")
            params = cmd.get("params", "")
            line = f"  - command='{name}': {desc}"
            if params:
                line += f" | params: {params}"
            if str(cmd.get("risk", "")).lower() in ("high", "高风险"):
                line += " | [高风险] 谨慎执行"
            if cmd.get("timeout"):
                line += f" | 超时 {cmd['timeout']}s"
            if cmd.get("parallel_safe"):
                line += " | 只读可并行"
            if cmd.get("idempotent"):
                line += " | 支持幂等"
            if cmd.get("progress_label"):
                line += f" | 进度: {cmd['progress_label']}"
            lines.append(line)
        return "\n".join(lines)

    def _build_app_tool(self, name: str, manifest: dict) -> Dict:
        """
        为单个可见 app 生成 use_<appname>(command, params) 工具。

        Args:
            name: app 注册名。
            manifest: app 的 manifest dict。

        Returns:
            OpenAI tool 定义 dict。
        """
        func_name = tool_name_for_app(name)
        description = manifest.get("description", "无描述")
        commands = manifest.get("commands", [])

        # 命令参数 schema: 有 commands -> enum 化; 无 -> 纯字符串 (向后兼容散文)
        if commands:
            cmd_names = [c.get("name", "") for c in commands if c.get("name")]
            command_param = {
                "type": "string",
                "enum": cmd_names,
                "description": "要执行的命令名, 必须从 enum 中选取。",
            }
            cmd_doc = self._render_command_doc(commands)
            full_desc = (
                f"{description}\n"
                f"通过 command 参数选择具体操作, params 投递该命令的参数。\n"
                f"可用命令:\n{cmd_doc}"
            )
        else:
            # 降级: 无结构化 commands 声明, 回退 usage 散文
            command_param = {
                "type": "string",
                "description": "要执行的命令名。",
            }
            usage = manifest.get("usage", "无调用说明")
            full_desc = f"{description}\n调用说明:\n{usage}"

        return {
            "type": "function",
            "function": {
                "name": func_name,
                "description": full_desc,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": command_param,
                        "params": {
                            "type": "object",
                            "description": (
                                "该命令的参数键值对, 直接以 JSON 对象投递。"
                                "具体可用键见上方命令文档。"
                            ),
                            "additionalProperties": True,
                        },
                    },
                    "required": ["command", "params"],
                },
            },
        }

    def build_tools(self, available_apps: dict) -> List[Dict]:
        """
        从已注册应用列表生成 OpenAI function-calling 工具定义。

        工具树构成:
          1. 每个可见 app 一个 use_<appname>(command: enum, params) 工具 (主入口)。
          2. connect_app / disconnect_app (生命周期)。
          3. instruct_app (隐藏兼容别名, 旧模型行为记忆兜底)。
          4. memory_recall / memory_remember / memory_forget / learn_knowledge_from_file (第一公民工具)。

        Args:
            available_apps: {name: AoaoPlug instance} 字典

        Returns:
            OpenAI tools 列表, 可直接传给 chat.completions.create(tools=...)
        """
        # 1. per-app 工具
        app_tools: List[Dict] = []
        visible_names = set(self.visible_app_names(available_apps))
        all_app_names: List[str] = []
        for name, app in available_apps.items():
            manifest = app.manifest
            if name not in visible_names:
                continue
            all_app_names.append(name)
            app_tools.append(self._build_app_tool(name, manifest))

        # 兜底: 无任何可见 app
        if not all_app_names:
            all_app_names = ["none_app"]

        # 2. 生命周期工具 (enum 为全部可见 app 名)
        lifecycle_tools = [
            {
                "type": "function",
                "function": {
                    "name": "connect_app",
                    "description": "【系统指令】给外部设备通电/开机。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "app_name": {"type": "string", "enum": all_app_names}
                        },
                        "required": ["app_name"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "disconnect_app",
                    "description": "【系统指令】关闭设备, 释放资源。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "app_name": {"type": "string", "enum": all_app_names}
                        },
                        "required": ["app_name"],
                    },
                },
            },
            # 3. instruct_app 隐藏兼容别名 (旧模型习惯, 优先用 use_<appname>)
            {
                "type": "function",
                "function": {
                    "name": "instruct_app",
                    "description": (
                        "【兼容接口】向外部设备发送操作指令。"
                        "优先使用 use_<appname> 工具, 此接口仅作向后兼容保留。"
                        "params 直接以 JSON 对象投递键值对, 严禁包成字符串。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "app_name": {"type": "string", "enum": all_app_names},
                            "command": {"type": "string"},
                            "params": {
                                "type": "object",
                                "description": "工具调用参数, 直接以 JSON 对象投递键值对。",
                                "additionalProperties": True,
                            },
                        },
                        "required": ["app_name", "command", "params"],
                    },
                },
            },
        ]

        # 4. 第一公民工具: 内存自治 + 知识吸收
        first_class_tools = [
            {
                "type": "function",
                "function": {
                    "name": "memory_recall",
                    "description": (
                        "【记忆主动翻阅】当意识到需要查阅往事、核实某个承诺、或确认某个规矩时调用。"
                        "会同时进行向量语义检索 + 实体/规则探照灯匹配, 返回相关记忆碎片。"
                        "非必要时不要滥用, 仅在你真的需要回忆时调用。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "想回忆的关键词或主题, 例如: 胖脸鱼的生日 / 上次提到的项目",
                            },
                            "when_filter": {
                                "type": "string",
                                "description": "可选时间过滤, 例如: 最近一周 / 去年",
                            },
                        },
                        "required": ["query"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "behavior_recall",
                    "description": (
                        "【行为自查】查阅自己近期的出站行为(说了什么话/对谁发)和记忆变更(remember/forget 改了啥)。"
                        "当不确定自己刚发过什么、老板问'你给谁发了什么'、或要核实是否真做了某动作时调用, 禁止瞎编。"
                        "返回近期行为记录(最近在前)。可选 query 子串过滤, n 限制条数(默认10)。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "可选过滤关键词 (匹配内容或目标对象), 例如: 胖脸鱼 / 你好",
                            },
                            "n": {
                                "type": "integer",
                                "description": "返回条数上限, 默认10",
                            },
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_remember",
                    "description": (
                        "【主动刻入长期记忆】当老板说出重要承诺、关键事实、长期偏好或禁忌时, "
                        "立即调用此工具将其刻入长期记忆, 确保未来不会遗忘。"
                        "仅记录真正重要、值得长期保留的信息, 闲聊不要记录。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "text": {
                                "type": "string",
                                "description": "要记住的具体内容, 例如: 胖脸鱼答应周三前给方案 / 老板对海鲜过敏",
                            },
                            "importance_score": {
                                "type": "number",
                                "description": "重要度 0.0~1.0, 越重要越高 (默认 0.8)",
                            },
                        },
                        "required": ["text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_forget",
                    "description": (
                        "【主动遗忘】当发现某条记忆已过时、被推翻或属于误记时调用, "
                        "将其从长期记忆中淘汰。传入规则文本或实体名称。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "要遗忘的记忆标识: 规则文本 或 实体名称 或 规则ID",
                            },
                        },
                        "required": ["target"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "learn_knowledge_from_file",
                    "description": (
                        "【知识吸收工具】当老板通过飞书或其他渠道发送本地文件路径给你，"
                        "让你学习、阅读、总结或记住该文件内容时，请调用此工具。"
                        "工具会自动解析文档 (PDF/DOCX/TXT/MD) 并将知识点入库到知识库。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "文件的绝对路径或相对路径，例如 G:\\资料\\企划书.pdf",
                            }
                        },
                        "required": ["file_path"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_adjust_impression",
                    "description": (
                        "【VIP 审批式调整双轴】仅胖脸鱼(老板)可用, 调整阿露对某人的好感度/注意度。"
                        "好感度(长期关系, 粘性)/注意度(近期熟络度) 均为 0-100, 传增量 delta(可负)。"
                        "reason 必填(为何调整)。非老板调用会被拒绝。阿露自己不可调(防情绪化乱改)。"
                        "调整会留审批记录供阿露感知。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "person": {
                                "type": "string",
                                "description": "要调整看法的人名, 如 '张三' / '胖脸鱼'。",
                            },
                            "favorability_delta": {
                                "type": "number",
                                "description": "好感度增量(可负), 如 -10 表示降10。不调传 0。",
                            },
                            "attention_delta": {
                                "type": "number",
                                "description": "注意度增量(可负), 如 +5 表示升5。不调传 0。",
                            },
                            "reason": {
                                "type": "string",
                                "description": "调整理由(必填), 如 '他总迟到'。",
                            },
                        },
                        "required": ["person", "reason"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "boost_interest",
                    "description": (
                        "【兴趣关注调整】仅胖脸鱼(老板)可用: 让阿露多关注某话题 (涨兴趣值 + 设衰减阻尼)。"
                        "兴趣值驱动群聊准入: 群里聊到高兴趣话题阿露会过闸看并自主决定回不回。"
                        "兴趣值一律随时间衰减, damping 越小衰减越慢 (仍衰减, 无永久兴趣)。"
                        "reason 必填。非老板调用会被拒绝。"
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "topic": {"type": "string", "description": "话题名, 如 '三国杀'/'直播'。"},
                            "reason": {"type": "string", "description": "关注理由(必填)。"},
                            "delta": {"type": "number", "description": "涨幅, 默认 1.0。"},
                            "damping": {"type": "number", "description": "衰减阻尼乘数 0~1, 默认 0.5 (越小衰减越慢)。"},
                        },
                        "required": ["topic", "reason"],
                    },
                },
            },
        ]

        # [0.3.27.0 阶段4 规格六] ask_user 内核工具 (不属于任何插件):
        # 仅老板 1v1/本地控制台可用; 等待期间任务挂起释放 Brain。
        # safe_default 只允许只读/可逆操作提供 (发消息/删文件/改配置/付款
        # 等不可逆操作留空 -> 超时自动取消任务)。
        ask_user_tool = {
            "type": "function",
            "function": {
                "name": "ask_user",
                "description": (
                    "【询问用户】任务需要用户拍板时调用, 系统把问题发给老板 "
                    "(仅老板私聊/本地控制台可用, 群聊或第三人调用会被拒绝)。"
                    "调用后任务挂起等待回答, 期间不影响其他对话。"
                    "同一对话最多一个待答问题。"
                    "safe_default: 只读/可逆操作 (查资料/临时状态) 才能给超时安全默认值; "
                    "发消息/删文件/改配置/付款等不可逆操作必须留空 (超时自动取消任务, 绝不默认执行)。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "要问老板的问题 (简洁明确)",
                        },
                        "options": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "可选答案列表 (可空)",
                        },
                        "safe_default": {
                            "type": "string",
                            "description": "超时安全默认值 (仅只读/可逆操作可给, 否则留空)",
                        },
                        "timeout": {
                            "type": "number",
                            "description": "等待秒数, 默认 60",
                        },
                    },
                    "required": ["question"],
                },
            },
        }
        return app_tools + lifecycle_tools + first_class_tools + [ask_user_tool]
