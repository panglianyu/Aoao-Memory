# kernel/reply_filters.py
"""[0.2.9.1] 回复文本过滤器: 叙述性元话语 / 沉默决策判定。

自 agent_runtime 抽出共享: 主循环兜底路由与 think_batch 按组代发 (0.2.9.1) 共用
同一套判定, 避免双实现漂移。词表是 0.1.10~0.1.15 时代化石, 待契约灰度稳后按
0.2.10.x 计划拆除 (见 STATE.md); 拆除前它挡住内心独白泄漏到群。
"""
from typing import Tuple

# [主播模式] 沉默决策元话语关键词 -- 阿露把"我决定闭嘴/继续待命"这类自省
# 当成文字输出时, 系统识别后判定为沉默, 不外发。
_SILENCE_META_KEYWORDS: Tuple[str, ...] = (
    "闭嘴", "硬凑", "不如闭嘴", "没话说", "无话可说", "保持沉默",
    "没素材", "不尬聊", "继续待命", "进入待命", "继续待机", "进入待机",
)

# [叙述性元话语关键词] 阿露把"已读不回/已回复群/已添加监听"这类描述自己动作的话
# 当成文字 reply 返回时, 路由兜底会把它们误发到渠道 (尤其群聊)。
_NARRATION_MARKERS: Tuple[str, ...] = (
    "已读不回", "已读 ", "已读-", "已回复", "已发送", "已添加", "已移除",
    "已成功", "已处理", "已记录", "已关注", "已主动", "已收到", "已加入",
    "已经成功", "已经添加", "已经回复", "已经发送",
    "内心独白", "自言自语", "内心想法",
    # [Fix 0.1.15.2 根因A] 阿露群聊常用元话语 (描述自己潜水/旁观决策), 旧词表漏识别
    # -> narration=False -> 兜底路由把内心独白发群里。补常见词。
    "潜水", "灌水", "旁观", "围观", "不打扰", "划水", "静观", "继续观察",
    "已读不回就好", "不回也好",
)


def is_narration_statement(text: str) -> bool:
    """判定 reply 是否为"叙述性元话语" (阿露描述自己通信动作/决策的话)。"""
    s = text.strip()
    if not s:
        return False
    if s.startswith("[外部感知"):
        s = s.split("]", 1)[-1].strip()
    if s.startswith("已") or s.startswith("已经"):
        return True
    return any(m in s for m in _NARRATION_MARKERS)


def is_silence_meta_statement(text: str) -> bool:
    """判定 reply 是否为"沉默决策元话语" (阿露解释自己为何闭嘴的话)。"""
    s = text.strip()
    if not s:
        return True
    if len(s) > 30:
        return False
    return any(kw in s for kw in _SILENCE_META_KEYWORDS)


# [0.3.27.1] 中断元话语关键词 -- 阿露把"回复被打断/还没发出/下一轮再回"这类
# 描述本轮出站状态与后续计划的话当成文字 reply 返回时, 属内部调度状态, 不是
# 给对话方看的内容。2026-08-06 22:38 事故实证: 抢占收尾 content 被按组代发
# 轰进群。词表只收中断元话语句式, 普通"下一轮团战"等游戏语境不命中。
_INTERRUPTED_META_MARKERS: Tuple[str, ...] = (
    "被打断", "还没发出", "还没发出去", "没来得及发出", "没发出去",
    "等老板的新消息", "等老板新消息", "下一轮再回", "下轮再回", "再回群里",
)


def is_interrupted_meta_statement(text: str) -> bool:
    """判定 reply 是否为"中断元话语" (描述回复被打断/未发出/下轮再回的话)。

    [0.3.27.1] 与 narration 同待遇: 群聊不外发, 老板 1v1 私聊保留
    (跨对话出站确认语义)。单关键词命中即判, 词表本身只收元话语句式。
    """
    s = text.strip()
    if not s:
        return False
    return any(m in s for m in _INTERRUPTED_META_MARKERS)
