# kernel/text_signals.py
"""[阶段2 0.1.12.5] 通用文本信号检测 (纯函数, 不焊任一 app)。

纠正/撤回信号检测: 判定用户消息是否为对上一句的纠正或撤回
(如 "说错了其实是这样" / "改成X" / "算了")。
供 ChatScheduler 据此时序信号延长老板攒批窗口, 覆盖连发/纠正合并场景
(治老板连发两句被 0.8s 窗口拆成两次回复)。

守 capability-generic-not-coupled 红线: kernel 通用工具, 不依赖 wechat/feishu
等任一 app, 不调 LLM, 不读数据文件, 纯字符串匹配。时序层只把纠正信号当
"该延长攒批窗口"的时序调度信号 (同 is_master), 不做内容审核 (topic/weight
交 MessageReviewer)。
"""
import re

# 信封头正则 (与 brain._ENVELOPE_RE 同款, 不 import brain 避免重依赖/循环引用)
_ENVELOPE_RE = re.compile(r"^\[外部感知:[^\]]*\]\s*")

# 纠正/撤回信号短语 (精确短语, 避开 "不对"/"不是这样"/"应该是" 等易与疑问句
# "对不对"/"是不是这样"/"你应该" 碰撞的模糊词; 纠正核心信号 = 前文作废/改/澄清)
_CORRECTION_PHRASES: tuple[str, ...] = (
    "说错了", "写错了", "发错了", "打错了", "错了",
    "改成", "改为", "改一下", "更正",
    "其实是这样", "其实是说", "我的意思是",
    "算了吧", "算了算了", "撤回", "当我没说",
)


def is_correction_signal(text: str) -> bool:
    """判定文本是否为纠正/撤回信号。

    先剥 [外部感知:...] 信封头再判正文。命中任一纠正短语即 True。
    空文本/无匹配返回 False。
    """
    if not text:
        return False
    body = _ENVELOPE_RE.sub("", text, count=1).strip()
    if not body:
        return False
    return any(phrase in body for phrase in _CORRECTION_PHRASES)
