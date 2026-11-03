# -*- coding: utf-8 -*-
"""kernel/mcp_market.py -- MCP 官方注册表客户端 (市场货源 + 安全校验).

货源: registry.modelcontextprotocol.io/v0/servers (MCP 官方注册表, 免密钥,
数万条目, search + cursor 分页)。能力落 kernel 通用层, UI 与 mcp_gateway
共用 (守 capability-generic-not-coupled: 不焊进特定 app)。

功能:
  - search_servers(query, limit, cursor): 异步搜索 + 归一化条目
  - 24h 本地缓存 data/tmp/mcp_market_cache.json; 断网回退缓存, 无缓存回退精选
  - validate_install_config(config): 命令白名单 / 包名正则 / https 校验 /
    拒 shell 元字符 (允许 ${VAR} 环境变量占位, 由 gateway 展开)
  - build_install_config(entry, secrets, server_name): 归一化条目 ->
    mcp_servers.yaml schema 的 config dict
  - runtime_check(entry): npx/uvx 等运行时是否就位 (UI preflight)

归一化条目字段: full_name / display_name / description / version /
install_kind (remote|npm|pypi|unknown) / transport / url / headers /
command / args / required_secrets / needs_key / repository_url。
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from kernel.logger import get_logger

logger = get_logger("mcp_market")

REGISTRY_URL = "https://registry.modelcontextprotocol.io/v0/servers"
REQUEST_TIMEOUT = 15.0
CACHE_TTL_SECONDS = 86400  # 24h
DEFAULT_LIMIT = 24
MAX_LIMIT = 100

# 安全边界: 本地 stdio 只允许包管理器启动器。
# 不放行裸 python/node 解释器: args=["-c","<任意代码>"] 不含黑名单字符,
# 经 stdio_client 无 shell exec 即成任意代码执行 (review CRITICAL-1)。
ALLOWED_COMMANDS = frozenset({"npx", "uvx"})

# 解释器求值旗标黑名单 (纵深防御: 即便白名单被放宽, -c/-e/-m 直接拒)
_EVAL_FLAGS = frozenset({"-c", "-e", "-m", "--eval", "--print", "-p"})

# 包名白名单正则 (npm 含 @scope/pkg, pypi 普通名)
_PKG_NAME_RE = re.compile(
    r"^(@[a-z0-9][a-z0-9._-]*/)?[a-z0-9][a-z0-9._-]*$", re.IGNORECASE
)
# server 名白名单 (写进 mcp_<name> app 名, 必须干净)
SERVER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
# 环境变量名
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# 远程 header 模板变量 {smithery_api_key} -> 环境变量名
_HEADER_VAR_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")
# shell 元字符黑名单 (剔除 ${VAR} 占位后检查)
_ENV_PLACEHOLDER_RE = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")
_FORBIDDEN_CHARS = set(";&|`<>$\n\r")


# ----------------------------------------------------------------
# 精选回退货源 (断网且无缓存时用, 全部 npm registry 验真免密钥)
# ----------------------------------------------------------------
CURATED_FALLBACK: List[Dict[str, Any]] = [
    {
        "full_name": "io.modelcontextprotocol/filesystem",
        "display_name": "filesystem",
        "description": "安全读写限定目录的文件",
        "version": "latest",
        "install_kind": "npm",
        "transport": "stdio",
        "url": "",
        "headers": {},
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem"],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": "https://github.com/modelcontextprotocol/servers",
    },
    {
        "full_name": "io.modelcontextprotocol/sequential-thinking",
        "display_name": "sequential-thinking",
        "description": "结构化多步推理, 复杂问题拆解",
        "version": "latest",
        "install_kind": "npm",
        "transport": "stdio",
        "url": "",
        "headers": {},
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": "https://github.com/modelcontextprotocol/servers",
    },
    {
        "full_name": "io.modelcontextprotocol/puppeteer",
        "display_name": "puppeteer",
        "description": "浏览器自动化 截图/点击/填表 (首次自动下 Chromium, 较重)",
        "version": "latest",
        "install_kind": "npm",
        "transport": "stdio",
        "url": "",
        "headers": {},
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-puppeteer"],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": "https://github.com/modelcontextprotocol/servers",
    },
    {
        "full_name": "dev.playwright/playwright-mcp",
        "display_name": "playwright",
        "description": "Playwright 浏览器自动化, 比 puppeteer 稳 (首次需装浏览器)",
        "version": "latest",
        "install_kind": "npm",
        "transport": "stdio",
        "url": "",
        "headers": {},
        "command": "npx",
        "args": ["-y", "@playwright/mcp"],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": "https://github.com/microsoft/playwright-mcp",
    },
    {
        "full_name": "com.upstash/context7",
        "display_name": "context7",
        "description": "实时查开源库官方文档, 接工具/写代码查 API 不瞎编",
        "version": "latest",
        "install_kind": "npm",
        "transport": "stdio",
        "url": "",
        "headers": {},
        "command": "npx",
        "args": ["-y", "@upstash/context7-mcp"],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": "https://github.com/upstash/context7",
    },
]


# ----------------------------------------------------------------
# 缓存
# ----------------------------------------------------------------

def _cache_path() -> str:
    """缓存文件路径: <项目根>/data/tmp/mcp_market_cache.json.

    [0.3.37.0] 归位 data/tmp/ (24h 易失缓存属临时件, data/ 顶层零散
    文件规范见 data/README.md); 旧路径残留由 data_layout.migrate_once 迁移。
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "data", "tmp", "mcp_market_cache.json")


def _load_cache() -> Dict[str, Any]:
    path = _cache_path()
    if not os.path.exists(path):
        return {}
    try:
        import json
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception as e:  # 缓存损坏不挡搜索主流程
        logger.warning("[mcp_market] 缓存读取失败, 按无缓存处理: %s", e)
        return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    import json
    path = _cache_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 只保留未过期条目, 控制体积
        now = time.time()
        trimmed = {
            k: v for k, v in cache.items()
            if isinstance(v, dict) and now - float(v.get("ts", 0)) < CACHE_TTL_SECONDS
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(trimmed, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("[mcp_market] 缓存写入失败: %s", e)


# ----------------------------------------------------------------
# 归一化
# ----------------------------------------------------------------

def _env_var_name(raw: str) -> str:
    """{smithery_api_key} -> SMITHERY_API_KEY 风格环境变量名。"""
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", raw.strip())
    return (cleaned or "MCP_SECRET").upper()


def _pick_remote(remotes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """选最优 remote: 无密钥要求的 streamable-http/sse 优先。"""
    if not remotes:
        return None
    typed = [r for r in remotes if r.get("url")]
    if not typed:
        return None

    def score(r: Dict[str, Any]) -> int:
        headers = r.get("headers") or []
        secret = any(h.get("isSecret") or h.get("isRequired") for h in headers)
        rtype = (r.get("type") or "").lower()
        prefer_type = 1 if "streamable" in rtype or rtype == "sse" else 0
        return (0 if secret else 2) + prefer_type

    return sorted(typed, key=score, reverse=True)[0]


def normalize_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    """注册表单条 server 记录 -> 归一化市场条目。

    raw 形如 {"name": "com.x/y", "title": ..., "description": ...,
    "version": ..., "packages": [...]|null, "remotes": [...], "repository": {...}}。
    安装方式优先级: 免密钥 remote > npm 包 > pypi 包 > 需密钥 remote。
    """
    server = raw.get("server", raw)  # 兼容搜索结果外层包裹
    full_name = str(server.get("name") or "").strip()
    display = str(server.get("title") or "").strip()
    if not display:
        display = full_name.rsplit("/", 1)[-1] or "unknown"

    entry: Dict[str, Any] = {
        "full_name": full_name,
        "display_name": display,
        "description": str(server.get("description") or "").strip(),
        "version": str(server.get("version") or ""),
        "install_kind": "unknown",
        "transport": "",
        "url": "",
        "headers": {},
        "command": "",
        "args": [],
        "required_secrets": [],
        "needs_key": False,
        "repository_url": str((server.get("repository") or {}).get("url") or ""),
    }

    # --- remote 候选 ---
    remote = _pick_remote(server.get("remotes") or [])
    remote_needs_key = False
    if remote:
        headers_out: Dict[str, str] = {}
        secrets: List[Dict[str, str]] = []
        for h in remote.get("headers") or []:
            hname = str(h.get("name") or "").strip()
            hval = str(h.get("value") or "")
            if not hname:
                continue
            if h.get("isSecret") or h.get("isRequired"):
                vars_in_val = _HEADER_VAR_RE.findall(hval)
                var = _env_var_name(vars_in_val[0]) if vars_in_val else _env_var_name(hname)
                secrets.append({
                    "var": var,
                    "description": str(h.get("description") or hname),
                })
                # 值模板 {x} 换 ${VAR}, 安装时由用户填值, 连接时 gateway 展开
                hval = _HEADER_VAR_RE.sub(f"${{{var}}}", hval) if vars_in_val \
                    else f"${{{var}}}"
                remote_needs_key = True
            headers_out[hname] = hval
        entry.update({
            "url": str(remote.get("url") or ""),
            "headers": headers_out,
            "required_secrets": secrets,
            "needs_key": remote_needs_key,
            "transport": "streamable_http",
            "install_kind": "remote",
        })
        if not remote_needs_key:
            return entry  # 免密钥 remote 最优, 直接定案

    # --- 包候选 (npm/pypi) ---
    packages = server.get("packages") or []
    npm = next((p for p in packages
                if str(p.get("registry_name") or "").lower() == "npm"
                and p.get("name")), None)
    pypi = next((p for p in packages
                 if str(p.get("registry_name") or "").lower() in ("pypi", "pip")
                 and p.get("name")), None)
    pkg = npm or pypi
    if pkg:
        pkg_name = str(pkg.get("name") or "")
        if _PKG_NAME_RE.match(pkg_name):
            extra_args: List[str] = []
            for a in pkg.get("package_arguments") or []:
                v = a.get("value") if isinstance(a, dict) else a
                # 注册表是不可信外部数据: 除元字符黑名单外, 再拒 URL/空白/引号
                # (挡 --registry=http://evil 这类改写 npx 行为的供应链载荷)
                if (isinstance(v, str) and v
                        and not set(v) & _FORBIDDEN_CHARS
                        and "://" not in v
                        and not any(c in v for c in ' \t"\'')):
                    extra_args.append(v)
            env_secrets: List[Dict[str, str]] = []
            for ev in pkg.get("environment_variables") or []:
                if ev.get("is_required") and ev.get("is_secret"):
                    env_secrets.append({
                        "var": _env_var_name(str(ev.get("name") or "")),
                        "description": str(ev.get("description") or ""),
                    })
            if npm:
                args_out = ["-y", pkg_name] + extra_args
            else:
                args_out = [pkg_name] + extra_args
            entry.update({
                "install_kind": "npm" if npm else "pypi",
                "transport": "stdio",
                "command": "npx" if npm else "uvx",
                "args": args_out,
                "required_secrets": env_secrets,
                "needs_key": bool(env_secrets),
            })
            return entry

    # --- 只剩需密钥 remote 或无安装方式 ---
    if remote:
        return entry  # 保留需密钥 remote 条目, UI 标"需密钥"
    entry["install_kind"] = "unknown"
    return entry


# ----------------------------------------------------------------
# 搜索
# ----------------------------------------------------------------

async def search_servers(
    query: str = "",
    limit: int = DEFAULT_LIMIT,
    cursor: str = "",
) -> Dict[str, Any]:
    """搜索官方注册表。

    返回 {items, next_cursor, total_hint, source, error}:
      source = network | cache | curated
    网络失败依次回退 24h 缓存 -> 精选列表 (只对首页, 带 cursor 的分页
    失败不回退精选, 直接报 error)。
    """
    limit = max(1, min(MAX_LIMIT, int(limit or DEFAULT_LIMIT)))
    params: Dict[str, Any] = {"limit": limit}
    if query:
        params["search"] = query
    if cursor:
        params["cursor"] = cursor

    cache_key = f"{query}|{limit}"
    cache = await asyncio.to_thread(_load_cache)  # 同步 IO 不阻塞事件循环

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(REGISTRY_URL, params=params)
            resp.raise_for_status()
            payload = resp.json()
        raw_servers = payload.get("servers") or []
        items = [normalize_entry(r) for r in raw_servers if isinstance(r, dict)]
        meta = payload.get("metadata") or {}
        result = {
            "items": items,
            "next_cursor": str(meta.get("nextCursor") or ""),
            "total_hint": meta.get("count"),
            "source": "network",
            "error": None,
        }
        # 只缓存首页 (无 cursor)
        if not cursor:
            cache[cache_key] = {"ts": time.time(), "payload": result}
            await asyncio.to_thread(_save_cache, cache)
        return result
    except Exception as e:
        # Python 3 的 except 块尾自动 del e, 先固化消息 (review HIGH-5:
        # 否则带 cursor 的失败分支引用 e 会抛 NameError 毁掉错误契约)
        err_msg = str(e)
        logger.warning("[mcp_market] 注册表请求失败: %s", err_msg)

    # --- 回退链 ---
    cached = cache.get(cache_key)
    if cached and isinstance(cached, dict):
        payload = cached.get("payload")
        if isinstance(payload, dict) and payload.get("items") is not None:
            stale = time.time() - float(cached.get("ts", 0)) > CACHE_TTL_SECONDS
            return {
                **payload,
                "source": "cache",
                "error": "离线缓存" + (" (已过期)" if stale else ""),
            }
    if not cursor:
        q = query.lower()
        items = [
            e for e in CURATED_FALLBACK
            if not q or q in e["display_name"].lower() or q in e["description"].lower()
        ]
        return {
            "items": items,
            "next_cursor": "",
            "total_hint": len(items),
            "source": "curated",
            "error": "注册表不可达, 显示内置精选货源",
        }
    return {
        "items": [],
        "next_cursor": "",
        "total_hint": None,
        "source": "network",
        "error": f"注册表请求失败: {err_msg}",
    }


# ----------------------------------------------------------------
# 安装配置构建与校验
# ----------------------------------------------------------------

def sanitize_server_name(raw: str) -> str:
    """从注册表名生成合法 server 名 (写 yaml, 挂 mcp_ 前缀前的裸名)。"""
    last = raw.rsplit("/", 1)[-1] if raw else ""
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", last).strip("-_")
    return cleaned.lower() or "server"


def build_install_config(
    entry: Dict[str, Any],
    secrets: Optional[Dict[str, str]] = None,
    server_name: Optional[str] = None,
) -> Dict[str, Any]:
    """归一化条目 -> mcp_servers.yaml 的 server config dict。

    secrets: {ENV_VAR: value}, 统一落 env 字段:
      - stdio: 子进程直接拿环境变量
      - remote: headers 里 ${VAR} 占位由 gateway _deep_expand 从 env 展开
        (gateway 现有 _expand_env_vars 读 os.environ, add_server 时同步
        把 env 键写进 os.environ, 见 mcp_gateway.add_server)
    非法条目抛 ValueError (校验失败原因在消息里, UI 直接展示)。
    """
    kind = entry.get("install_kind", "unknown")
    if kind not in ("npm", "pypi", "remote"):
        raise ValueError(f"该条目没有可用的安装方式 (install_kind={kind})")

    name = (server_name or "").strip() or sanitize_server_name(
        entry.get("full_name") or entry.get("display_name") or ""
    )
    if not SERVER_NAME_RE.match(name):
        raise ValueError(f"server 名非法: {name} (仅允许字母数字 _ -)")

    env: Dict[str, str] = {}
    for sec in entry.get("required_secrets") or []:
        var = sec.get("var", "")
        val = (secrets or {}).get(var, "")
        if not val:
            raise ValueError(f"缺少密钥: {var} ({sec.get('description', '')})")
        env[var] = val

    config: Dict[str, Any] = {
        "name": name,
        "description": entry.get("description") or entry.get("display_name") or name,
        "auto_connect": True,
        "timeout": 30,
    }
    if kind == "remote":
        config.update({
            "transport": "streamable_http",
            "url": entry.get("url", ""),
            "headers": dict(entry.get("headers") or {}),
            "env": env,
        })
    else:
        config.update({
            "transport": "stdio",
            "command": entry.get("command", ""),
            "args": list(entry.get("args") or []),
            "env": env,
        })
        # filesystem 特例: 必须至少一个允许目录参数, 缺省放行项目 data/
        if config["command"] == "npx" and \
                any("server-filesystem" in str(a) for a in config["args"]) and \
                not any(os.path.isabs(str(a)) for a in config["args"]):
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config["args"].append(os.path.join(root, "data"))

    ok, reason = validate_install_config(config)
    if not ok:
        raise ValueError(f"配置校验失败: {reason}")
    return config


def _check_shell_safe(value: str, field: str) -> Optional[str]:
    """剔除 ${VAR} 占位后检查 shell 元字符, 返回错误原因或 None。"""
    if not isinstance(value, str):
        return f"{field} 必须是字符串"
    stripped = _ENV_PLACEHOLDER_RE.sub("", value)
    bad = set(stripped) & _FORBIDDEN_CHARS
    if bad:
        return f"{field} 含非法字符: {''.join(sorted(bad))}"
    return None


def validate_install_config(config: Dict[str, Any]) -> Tuple[bool, str]:
    """校验将要写入 mcp_servers.yaml 的 server config。

    红线:
      - stdio: command 在白名单, args 无 shell 元字符
      - sse/streamable_http: url 必须 https (本地 http://127.0.0.1/localhost 放行)
      - env 键必须是合法环境变量名
    """
    if not isinstance(config, dict):
        return False, "配置必须是 dict"
    name = config.get("name") or ""
    if not SERVER_NAME_RE.match(str(name)):
        return False, f"server 名非法: {name}"

    transport = str(config.get("transport") or "stdio")
    if transport == "stdio":
        command = str(config.get("command") or "")
        if command not in ALLOWED_COMMANDS:
            return False, f"command 不在白名单: {command} (允许 {sorted(ALLOWED_COMMANDS)})"
        args = config.get("args") or []
        if not isinstance(args, list):
            return False, "args 必须是列表"
        for i, a in enumerate(args):
            if str(a) in _EVAL_FLAGS:
                return False, f"args[{i}] 含求值旗标 {a} (拒绝解释器执行)"
            err = _check_shell_safe(str(a), f"args[{i}]")
            if err:
                return False, err
    elif transport in ("sse", "streamable_http"):
        url = str(config.get("url") or "")
        if not url:
            return False, "远程 server 缺少 url"
        # 本地回环放行必须精确比对 hostname (review HIGH-4:
        # startswith("http://127.0.0.1") 会被 http://127.0.0.1.evil.com 绕过,
        # 含密钥 headers 将明文发往攻击者域名)
        from urllib.parse import urlsplit
        parts = urlsplit(url)
        hostname = (parts.hostname or "").lower()
        local_ok = parts.scheme == "http" and hostname in ("127.0.0.1", "localhost", "::1")
        if not url.startswith("https://") and not local_ok:
            return False, f"url 必须 https (或本地回环): {url}"
        err = _check_shell_safe(url, "url")
        if err:
            return False, err
        for hname, hval in (config.get("headers") or {}).items():
            err = _check_shell_safe(str(hval), f"headers[{hname}]")
            if err:
                return False, err
    else:
        return False, f"不支持的 transport: {transport}"

    for k in config.get("env") or {}:
        if not _ENV_KEY_RE.match(str(k)):
            return False, f"env 键非法: {k}"
    return True, "ok"


def runtime_check(entry: Dict[str, Any]) -> Tuple[bool, str]:
    """检查条目所需运行时 (npx/uvx) 是否在 PATH。remote 条目直接通过。"""
    kind = entry.get("install_kind", "unknown")
    if kind == "remote":
        return True, "远程服务, 无需本地运行时"
    command = entry.get("command", "")
    if not command:
        return False, "未知安装方式"
    if shutil.which(command):
        return True, f"{command} 已就位"
    hint = "Node.js (含 npx)" if command == "npx" else \
           "uv (含 uvx)" if command == "uvx" else command
    return False, f"缺少 {hint}, 装好再添加 (PATH 里找不到 {command})"
