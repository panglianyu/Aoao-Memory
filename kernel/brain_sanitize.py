# kernel/brain_sanitize.py
"""[0.3.18.0 Brain 收缩 Phase 1] 输出净化器纯函数。

从 brain.py 拆出的 _sanitize_output (方案: .claude/plans/brain-god-class-
shrink.plan.md)。六层净化: 工具残留/技能配方/think 配对/孤立闭合/自省
分隔符/英文自省启发式 (0.3.18.0 Phase 0 已收紧误杀面)。Brain 保留同名
薄委托, 行为逐字不变。

标签模式走 chr(60)/chr(62) 运行期拼接: 字面尖括号标签会被 Write 管线
当 HTML 剥除/截断 (同 tests/test_system_voice_text_clean_speakable.py 坑记)。
"""
import re

from kernel.logger import get_logger

logger = get_logger(__name__)

_LT = chr(60)   # <
_GT = chr(62)   # >

# 1/1.5/2/4 层标签清理模式 (运行期编译, 避开管线剥除)
_RE_TOOL_CALL = re.compile(
    _LT + r"tool_call" + _GT + r".*?" + _LT + r"/tool_call" + _GT,
    re.DOTALL)
_RE_SKILL_RECIPE = re.compile(
    _LT + r"skill_recipe" + _GT + r".*?" + _LT + r"/skill_recipe" + _GT,
    re.DOTALL)
_RE_SKILL_TAG_ORPHAN = re.compile(_LT + r"/?skill_recipe" + _GT)
_RE_THINK_PAIR = re.compile(
    _LT + r"think" + _GT + r".*?" + _LT + r"/think" + _GT, re.DOTALL)
_RE_THINK_TAG_ORPHAN = re.compile(_LT + r"/?think" + _GT)
_CLOSE_THINK = _LT + "/think" + _GT


def sanitize_output(text: str) -> str:
    """极其暴力的物理级净化器，保证底层代码绝对不泄露"""
    # 1. 清理工具调用残留
    text = _RE_TOOL_CALL.sub("", text)
    text = re.sub(r'_icall_[^\)]+\)', '', text)

    # 1.5 清理技能配方标签 (不应暴露给用户)
    text = _RE_SKILL_RECIPE.sub("", text)
    text = _RE_SKILL_TAG_ORPHAN.sub("", text)

    # 2. 暴力清理 think 标签（标准配对）
    text = _RE_THINK_PAIR.sub("", text)

    # 3. 兜底防漏：如果大模型抽风只写了结尾的 </think>，直接砍掉它前面的所有草稿！
    if _CLOSE_THINK in text:
        text = text.split(_CLOSE_THINK)[-1]

    # 4. 清理残余的孤立标签
    text = _RE_THINK_TAG_ORPHAN.sub("", text)

    # 截断思考分隔符：模型自省时常见的 "思考→修正→正式回复" 结构
    #    只保留最后一段（通常是正式回复），前面全部丢弃
    split_markers = [
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
    ]
    last_segment = text
    for marker in split_markers:
        if marker in last_segment:
            # 取该标记之后的最后一段
            parts = last_segment.split(marker)
            # 跳过标记前所有内容，只保留最后一段
            last_segment = parts[-1]
    text = last_segment.strip()

    # 6. 兜底：如果整段是英文自省内容（典型特征：规划如何回复的元话语），直接清空
    #    [0.3.18.0 Phase 0 修报告 HIGH 误杀] 关键词从泛用第一人称短语收紧为
    #    自省专属元话语; 判定条件不变 (命中 >=3 且中文占比 <10%)。
    if text and len(text) > 50:
        english_introspection_keywords = [
            "let me think", "let me analyze", "let me check",
            "i need to respond", "i will respond", "i should respond",
            "the user's request", "the user is asking", "the user wants",
            "my response", "i will answer", "step by step",
            "i will stick",
        ]
        keyword_hits = sum(1 for kw in english_introspection_keywords if kw.lower() in text.lower())
        # 统计中文字符占比
        chinese_chars = sum(1 for c in text if '一' <= c <= '鿿')
        chinese_ratio = chinese_chars / len(text)
        if keyword_hits >= 3 and chinese_ratio < 0.1:
            logger.warning(f"[净化器] 检测到英文思考泄漏（{keyword_hits} 个关键词命中，中文占比 {chinese_ratio:.1%}），已拦截。")
            return ""

    return text.strip()
