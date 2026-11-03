# -*- coding: utf-8 -*-
"""kernel/stream_sanitizer.py -- 字符流增量净化器 (0.2.20.0 流式输出配套).

与 Brain._sanitize_output (整段版) 语义对齐, 但工作在字符流上, 供流式输出
逐字渲染时保证草稿/标签绝不泄漏到用户侧:

  - think / tool_call / skill_recipe 三类块内全量扣留, 闭合后才放行后续文本;
  - chunk 边界劈裂标签时 (前半 chunk 以开标签缺末字符结尾, 后半补全),
    部分标签缓冲防止半个标签漏出;
  - 孤立闭合 think 标签 (模型抽风只写闭合): 砍掉之前全部已发射并返回 reset
    信号, 对齐整段版第 3 步 (split 取最后一段);
  - split markers (Refined response: 等英文自省分段标记): 用已发射尾窗口
    检测跨 chunk marker, 命中已渲染区则 reset 重来; 命中未渲染区则静默
    丢弃前缀 (对齐整段版取 marker 后最后一段);
  - 英文自省启发式 (关键词命中 + 中文占比) 是整段判定, 字符流上做不了:
    由流结束时的最终 _sanitize_output 兜底, UI 在 stream_end 用最终净化
    文本整体替换气泡 (兜底铁律, 见 conversation_view.end_agent_stream)。

零外部依赖, 纯内存状态机, 线程不安全 (单次 think 独占一个实例)。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

# 压制块: (开标签, 闭标签)。块内全量扣留, 闭合后放行。与 _sanitize_output 的
# tool_call / think / skill_recipe 清理语义一致。
_SUPPRESSED_BLOCKS: Tuple[Tuple[str, str], ...] = (
    ("<think>", "</think>"),
    ("<tool_call>", "</tool_call>"),
    ("<skill_recipe>", "</skill_recipe>"),
)

# 块开标签 -> 闭标签 查找表。
_BLOCK_CLOSE: Dict[str, str] = dict(_SUPPRESSED_BLOCKS)

# 全部标签 (开 + 闭), 供部分标签后缀检测。
_ALL_TAGS: Tuple[str, ...] = tuple(_BLOCK_CLOSE.keys()) + tuple(_BLOCK_CLOSE.values())

# split markers (对齐 Brain._sanitize_output), 模型自省的"思考->正式回复"分段结构。
SPLIT_MARKERS: Tuple[str, ...] = (
    "Refined response:",
    "Final response:",
    "Self-Correction:",
    "Thinking:",
    "I'll write the response now.",
    "Actually, looking at",
    "Wait, looking at",
    "So I will",
    "If so, I need",
    "But the result says",
)

# 已发射尾窗口大小: 最长 marker 长度 + 少量余量, 保证跨两个 chunk 劈裂的
# marker 仍能经尾窗口拼接命中。
_MARKER_HOLD: int = max(len(m) for m in SPLIT_MARKERS) + 4


@dataclass(frozen=True)
class SanitizeEvent:
    """增量净化器输出事件。

    kind:
      - "emit": 可追加渲染的文本 (已净化);
      - "reset": 之前渲染的内容全部作废 (孤立闭合标签 / marker 命中已渲染区),
        调用方应清空气泡, 随后继续按 emit 事件渲染。
    """

    kind: str
    text: str


def _prefix_suffix_len(text: str, target: str) -> int:
    """text 末尾作为 target 真前缀的最长后缀长度 (0 = 无)。

    用于块内扣留半截闭标签: 如块内 chunk 以闭标签缺尾部字符的形式结尾,
    必须留住等下个 chunk 补齐, 否则劈裂的闭标签永久错过, 块外文本被吞。
    """
    max_n = min(len(target) - 1, len(text))
    for n in range(max_n, 0, -1):
        if text.endswith(target[:n]):
            return n
    return 0


def _partial_tag_hold(text: str) -> int:
    """text 末尾作为某压制标签真前缀的最长后缀长度 (0 = 无)。

    如尾部是开 think 标签缺最后一个字符的形式时必须扣留: 下个 chunk 可能
    把它补成完整标签。逐标签从长到短试, 取最大命中。
    """
    best = 0
    for tag in _ALL_TAGS:
        max_n = min(len(tag) - 1, len(text))
        for n in range(max_n, 0, -1):
            if text.endswith(tag[:n]):
                if n > best:
                    best = n
                break
    return best


class IncrementalSanitizer:
    """字符流增量净化器, 契约与边界语义见模块 docstring。"""

    def __init__(self) -> None:
        self._buf: str = ""
        self._active_close: str = ""    # 当前所处压制块的闭标签 (""=不在块内)
        self._emitted_tail: str = ""    # 已发射文本尾窗口, 供 split marker 检测

    def feed(self, chunk: str) -> List[SanitizeEvent]:
        """喂入一段字符流, 返回本次产生的事件列表 (可能为空)。"""
        if chunk:
            self._buf += chunk
        return self._drain(hold_window=True)

    def flush(self) -> List[SanitizeEvent]:
        """流结束时调用: 释放扣留尾料。尾部部分标签按垃圾丢弃; 未闭合的
        压制块整块吞掉 (草稿绝不泄漏)。"""
        return self._drain(hold_window=False)

    def _drain(self, hold_window: bool) -> List[SanitizeEvent]:
        events: List[SanitizeEvent] = []
        while self._buf:
            if self._active_close:
                idx = self._buf.find(self._active_close)
                if idx < 0:
                    # 块未闭合: 压制内容整体丢弃, 但尾部若形似半截闭标签
                    # 必须扣留 (下个 chunk 可能补齐), 治闭标签劈裂吞掉块外文本。
                    keep = _prefix_suffix_len(self._buf, self._active_close)
                    self._buf = self._buf[len(self._buf) - keep:] if keep else ""
                    break
                self._buf = self._buf[idx + len(self._active_close):]
                self._active_close = ""
                continue

            # 找最早的完整标签。
            e_idx, e_tag = -1, ""
            for tag in _ALL_TAGS:
                i = self._buf.find(tag)
                if i >= 0 and (e_idx < 0 or i < e_idx):
                    e_idx, e_tag = i, tag

            # region = 本次打算放行文本 (标签前 / 整个缓冲), 拼已发射尾窗口
            # 一起做 marker 检测。
            region_end = len(self._buf) if e_idx < 0 else e_idx
            combined = self._emitted_tail + self._buf[:region_end]
            m_idx, m_len = self._find_marker(combined)
            if m_idx >= 0:
                tail_len = len(self._emitted_tail)
                if m_idx < tail_len:
                    # marker 已进入已发射文本: 全部作废, reset 重来。
                    events.append(SanitizeEvent("reset", ""))
                    self._emitted_tail = ""
                    self._buf = combined[m_idx + m_len:] + self._buf[region_end:]
                else:
                    # marker 全在未发射 region 内: 静默丢前缀 (无需 reset)。
                    rel = m_idx - tail_len
                    self._buf = self._buf[rel + m_len:]
                continue

            if e_idx >= 0:
                if e_tag not in _BLOCK_CLOSE:
                    # 孤立闭合标签: 砍掉之前全部 (对齐整段版第 3 步)。
                    events.append(SanitizeEvent("reset", ""))
                    self._emitted_tail = ""
                    self._buf = self._buf[e_idx + len(e_tag):]
                    continue
                region = self._buf[:e_idx]
                if region:
                    events.append(SanitizeEvent("emit", region))
                    self._emitted_tail = (self._emitted_tail + region)[-_MARKER_HOLD:]
                self._buf = self._buf[e_idx + len(e_tag):]
                self._active_close = _BLOCK_CLOSE[e_tag]
                continue

            # 无完整标签: 扣留尾部部分标签后缀后放行。扣留的部分:
            # feed 路径留给下个 chunk 判定; flush 路径随循环结束被丢弃。
            hold = _partial_tag_hold(self._buf)
            emit_part = self._buf[:len(self._buf) - hold] if hold else self._buf
            self._buf = self._buf[len(emit_part):]
            if emit_part:
                events.append(SanitizeEvent("emit", emit_part))
                self._emitted_tail = (self._emitted_tail + emit_part)[-_MARKER_HOLD:]
            break
        return events

    @staticmethod
    def _find_marker(text: str) -> Tuple[int, int]:
        """返回 text 中最早出现的 marker 的 (位置, 长度); 无则 (-1, 0)。"""
        best_i, best_len = -1, 0
        for marker in SPLIT_MARKERS:
            i = text.find(marker)
            if i >= 0 and (best_i < 0 or i < best_i):
                best_i, best_len = i, len(marker)
        return best_i, best_len
