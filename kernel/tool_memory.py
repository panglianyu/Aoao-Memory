# -*- coding: utf-8 -*-
"""kernel/tool_memory.py -- [0.3.31.3 Batch 3] 工具调用记忆 (codex 式任务上下文)。

老板痛点: 查刀塔战绩有 skill 有之前用过的账号 ID, 但阿露想不起来调用。
断点 = 成功调用无可检索记录, 模型每次重想参数。修法 (搭配记忆体系):
成功的工具调用记账 behavior_log (action_type=tool_call), 相似任务时按
关键词重叠召回 top-k 范例注入系统提示词, 账号/ID/路径类参数直接复用。
零命中零注入 (不打扰闲聊)。全部 fail-safe, 任何异常不穿透主链。
"""
from __future__ import annotations

import json
import os
import re

from kernel.logger import get_logger

logger = get_logger(__name__)

# [0.3.37.0] 单一真源: 路径与 behavior_log 共享 (治双路径漂移),
# 归位 data/logs/ 由 kernel.data_layout 规范统一。
from kernel.behavior_log import _log_path

_DEFAULT_LOG = _log_path()
SCAN_LINES = 500          # 只扫最近 N 行 (防日志膨胀拖慢召回)
MIN_OVERLAP = 2           # 关键词重叠低于此值视为不相关, 不注入
EXAMPLE_CONTENT_CAP = 200  # 单条范例注入长度上限
ARGS_CAP = 120             # 记账参数截断
RESULT_CAP = 150           # 记账结果截断


def record_tool_example(func_name: str, args, res_str: str,
                        turn_id: str = "") -> None:
    """[0.3.31.3] 成功工具调用记账 (截断防膨胀, fail-safe 不穿透)。"""
    try:
        from kernel import behavior_log
        if isinstance(args, dict):
            _args = json.dumps(args, ensure_ascii=False)[:ARGS_CAP]
        else:
            _args = str(args)[:ARGS_CAP]
        behavior_log.append_behavior(
            "tool_call",
            content=f"{func_name}({_args}) -> {str(res_str)[:RESULT_CAP]}",
            app=str(func_name), turn_id=turn_id,
        )
    except Exception as e:
        logger.debug(f"[tool_memory] 记账失败(非致命): {e}")


def _tokens(text: str) -> set:
    """轻量分词: ascii 词 (>=2 字符) + 汉字二元组 (不引分词依赖)。"""
    toks = set(re.findall(r"[A-Za-z0-9_]{2,}", text.lower()))
    for seg in re.findall(r"[一-鿿]+", text):
        toks.update(seg[i:i + 2] for i in range(max(len(seg) - 1, 0)))
    return toks


def recall_tool_examples(query: str, k: int = 3,
                         log_path: str = "") -> list:
    """从近期 behavior_log 捞与 query 相关的成功工具调用范例。

    返回范例内容列表 (最多 k 条, 按相关度降序); 无命中返空列表。
    """
    path = log_path or _DEFAULT_LOG
    if not query or not os.path.isfile(path):
        return []
    qtoks = _tokens(query)
    if len(qtoks) < MIN_OVERLAP:
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()[-SCAN_LINES:]
    except OSError as e:
        logger.debug(f"[tool_memory] 日志读取失败(非致命): {e}")
        return []
    scored: list = []
    for line in reversed(lines):
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if entry.get("action_type") != "tool_call":
            continue
        content = str(entry.get("content", "") or "")
        overlap = len(qtoks & _tokens(content))
        if overlap >= MIN_OVERLAP:
            scored.append((overlap, content[:EXAMPLE_CONTENT_CAP]))
        if len(scored) >= k * 3:
            break
    scored.sort(key=lambda x: -x[0])
    return [content for _, content in scored[:k]]


def build_task_context_block(query: str, k: int = 3, log_path: str = "") -> str:
    """装配 [任务经验] 注入块; 零命中返空串 (调用方不注入)。"""
    examples = recall_tool_examples(query, k=k, log_path=log_path)
    if not examples:
        return ""
    lines = [
        "[任务经验: 以下是过去类似任务的成功工具调用, 参考其用法与参数 "
        "(账号/ID/路径类参数可直接复用)]"
    ]
    lines.extend(f"- {content}" for content in examples)
    lines.append(
        "[边界: 范例仅供参考参数, 本次任务仍以用户当前诉求为准; "
        "参数可能过期, 拿不准先问用户。]"
    )
    return "\n".join(lines)
