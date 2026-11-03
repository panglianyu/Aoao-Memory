# kernel/speak_gate.py
"""[P1 0.2.7.0 阶段1] 发言闸门: 渠道无关规则层, 决定"回不回"(决策层)。

把"回不回"从 LLM 的 prompt 自由(发送层)移到确定性规则(决策层):
  - 老板1v1 (is_dm_with_master): 必回 (回合制)。
  - 群聊默认潜水, 仅强触发过闸: @/名字提及阿露 或 命中兴趣人设关键词。
  - 其余群消息: dive (跳过 think, 零发送, 省 LLM 成本)。

阿露"想回就回"的自主性在此固化为规则 (关键词/@提及放行), 不再靠 prompt 求模型自律
(自律不可靠: 阿露 memory_remember 立"一轮一意图一发送"照样连发3遍, 2026-07-22 奶狗事件)。

渠道无关 (capability-generic-not-coupled): 只读 Message.metadata 通用字段
(is_dm_with_master/is_master/sender/content) + config/gate.yaml, 不焊任一通道。
通道侧负责在 metadata 标 is_dm_with_master (wechat main.py:466 已标), 闸门不猜私聊。

与 reviewer 分工 (决策1A): 闸门是硬决策(回不回, 纯规则零 LLM 延迟); reviewer 降级为
可选富化(过闸后打 Tag 参考)。闸门不依赖 reviewer。

用法:
  gate = SpeakGate.load()                       # 读 config/gate.yaml, 缺省回退默认
  d = gate.evaluate(msgs, chat, src, is_master_group, char_name)
  if not d.passed: skip think (潜水)
"""
import os
import re
from dataclasses import dataclass
from typing import List, Optional

import yaml

from kernel.logger import get_logger

logger = get_logger(__name__)


# 与 brain.py:50 _ENVELOPE_RE 同一信封约定 (外设消息 "[外部感知:...] 正文")。
# 此处复制而非 import brain: brain 会 import speak_gate, 互引成环。提及/关键词判定
# 只对剥信封后的正文做, 防"对话:阿露的群"这类信封头里的名字造成误触发。
_ENVELOPE_RE = re.compile(r"^\[外部感知:[^\]]*\]\s*")


def _strip_envelope(text: str) -> str:
    """剥离 [外部感知:...] 信封头返回正文; 无信封原样返回 (同 brain._strip_envelope)。"""
    if not text:
        return ""
    return _ENVELOPE_RE.sub("", text, count=1)


# 缺省配置: 缺 config/gate.yaml / 缺键时全回退此默认 (默认开闸门, 管辖 wechat, 群默认潜水)。
# [0.2.7.2 老板拍板] 触发关键词默认空: 没喊阿露名字的群消息一律潜水, "阿露"提及是
# 唯一门铃 (与 config/gate.yaml 策略一致, 缺配置文件时行为不漂移)。
_DEFAULT_CONFIG = {
    "enabled": True,
    "gated_sources": ["wechat"],
    "trigger_keywords": [],
    "group_dive_default": True,
    # [0.2.22.0 白名单必回] 对话名白名单 (无视潜水/触发恒过闸); 缺省空。
    "always_reply_chats": [],
    # [0.2.8.0 出站收口] submit_reply 单次代发契约开关 (与闸门共置 P1 回滚开关文件)。
    "structured_reply": True,
}


@dataclass(frozen=True)
class GateDecision:
    """闸门判定结果 (不可变)。passed=True 过闸进 think; False 潜水跳过。"""

    passed: bool
    reason: str


class SpeakGate:
    """发言闸门 (规则层, 纯确定性判定, 不调 LLM)。

    决策树 (短路顺序):
      1. 未启用 -> pass (默认放行, 回退 P0 行为)
      2. src 不在 gated_sources -> pass (闸门只管配置的聊天通道; console/idle/boot 等直连源放行)
      3. 任一消息 is_dm_with_master -> pass (老板1v1必回-回合制)
      4. 强触发 (提及 char_name/@char_name 或 命中 trigger_keywords) -> pass
      5. group_dive_default -> dive (群聊无强触发默认潜水); 否则 pass
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        cfg = dict(_DEFAULT_CONFIG)
        if config:
            cfg.update({k: v for k, v in config.items() if v is not None})
        self._enabled: bool = bool(cfg.get("enabled", True))
        self._gated_sources: set = set(cfg.get("gated_sources") or ["wechat"])
        self._trigger_keywords: List[str] = [
            str(k).strip() for k in (cfg.get("trigger_keywords") or []) if str(k).strip()
        ]
        self._group_dive_default: bool = bool(cfg.get("group_dive_default", True))
        self._always_reply_chats: set = {
            str(x).strip() for x in (cfg.get("always_reply_chats") or []) if str(x).strip()
        }
        self._structured_reply: bool = bool(cfg.get("structured_reply", True))
        # [0.2.9.0] 动态兴趣关键词 (记忆侧 top-N, brain 定期 set; 与静态 yaml 表并集)
        self._dynamic_keywords: List[str] = []

    @property
    def structured_reply(self) -> bool:
        """[0.2.8.0] 出站收口开关: true=LLM 回复走 submit_reply 契约代发, 管辖通道
        裸 send_text 文本回复降级为拒发+引导; false=回退 P0 send_text 路径。"""
        return self._structured_reply

    @property
    def gated_sources(self) -> set:
        """受闸门管辖的聊天通道集 (副本, 供发送层 send_text 降级判定用)。"""
        return set(self._gated_sources)

    @classmethod
    def load(cls) -> "SpeakGate":
        """读 config/gate.yaml 构造; 缺文件/解析异常回退默认配置 (不抛, 不阻塞 think)。"""
        try:
            path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "gate.yaml",
            )
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return cls(data)
        except Exception as e:
            logger.warning(f"[SpeakGate] 配置读取失败, 回退默认: {e}")
            return cls(None)

    def evaluate(
        self,
        msgs: list,
        chat: str,
        src: str,
        is_master_group: bool,
        char_name: str,
    ) -> GateDecision:
        """判定本组消息回不回。只读通用 metadata, 渠道无关。"""
        if not self._enabled:
            return GateDecision(True, "闸门未启用(默认放行)")
        if src not in self._gated_sources:
            return GateDecision(True, f"非管辖通道({src or '空'})放行")
        # 老板1v1 必回 (回合制): 通道已在 metadata 标 is_dm_with_master。
        if any(self._is_dm_with_master(m) for m in msgs):
            return GateDecision(True, "老板1v1必回")
        # [0.2.22.0 白名单必回] 指定对话(家人1v1/重要群)无视潜水与触发, 恒过闸。
        # 匹配对话名(chat); 1v1 时 chat=对方备注名。治"我爸单聊被潜水丢"。
        if chat and chat in self._always_reply_chats:
            return GateDecision(True, f"白名单对话必回({chat})")
        # 强触发: 提及阿露(含@) 或 命中兴趣关键词 (剥信封后判, 防群名误触发)。
        if self._mentioned(msgs, char_name):
            return GateDecision(True, f"提及{char_name or '阿露'}过闸")
        # 兴趣关键词先判 (0.2.10.2 排序修正): 带图又含兴趣词的消息走"兴趣关键词过闸"
        # (非只读, 兴趣话题照常随口回); 只有纯图片无文字信号才落"图片消息过闸"只读批。
        if self._keyword_hit(msgs):
            return GateDecision(True, "兴趣关键词过闸")
        # [0.2.9.3] 图片/表情包强触发: 准许看。图批潜水 + 后到的喊她批无图 -> 她永远
        # 看不见那张图 (12:38 巨人网络群实证)。回不回仍由契约定, 闸门只管看;
        # 0.2.10.1 起纯图片批为只读 (随口不代发, 蓄意调 submit_reply 才回)。
        if self._has_image(msgs):
            return GateDecision(True, "图片消息过闸")
        # 群聊默认潜水 (含老板在群回别人无触发 -> 潜水, 治凑话)。
        if self._group_dive_default:
            return GateDecision(False, "群聊无强触发,默认潜水")
        return GateDecision(True, "潜水关闭,放行")

    # ---- 纯判定辅助 (可单测) ----

    @staticmethod
    def _meta(m) -> dict:
        md = getattr(m, "metadata", None)
        return dict(md) if md else {}

    def _is_dm_with_master(self, m) -> bool:
        return bool(self._meta(m).get("is_dm_with_master"))

    def _mentioned(self, msgs: list, char_name: str) -> bool:
        """正文(剥信封)含 char_name 或 @char_name 即视为提及 (同 reviewer._looks_called 范式)。"""
        if not char_name:
            return False
        names = {char_name, f"@{char_name}"}
        for m in msgs:
            body = _strip_envelope(getattr(m, "content", "") or "")
            if any(n in body for n in names):
                return True
        return False

    @staticmethod
    def _has_image(msgs: list) -> bool:
        """[0.2.9.3] 批内任一消息带图片/表情包标记 ([LOCAL_IMAGE:], 渠道无关内容判定)。"""
        for m in msgs:
            if "[LOCAL_IMAGE:" in (getattr(m, "content", "") or ""):
                return True
        return False

    def set_dynamic_keywords(self, kws: list) -> None:
        """[0.2.9.0] 注入动态兴趣关键词 (记忆侧 top-N), 与静态 yaml 表并集过闸。

        闸门不实时查记忆: brain 侧 TTL 缓存刷新调本方法; 失败留旧值不阻塞。
        空列表 = 清空动态集 (top_n=0 一键回退纯提及门铃)。
        """
        self._dynamic_keywords = [str(k).strip() for k in (kws or []) if str(k).strip()]

    def _keyword_hit(self, msgs: list) -> bool:
        """正文(剥信封)命中任一关键词即过闸 (静态 yaml 表 + 0.2.9.0 动态兴趣 top-N)。"""
        keywords = self._trigger_keywords + self._dynamic_keywords
        if not keywords:
            return False
        for m in msgs:
            body = _strip_envelope(getattr(m, "content", "") or "")
            if any(kw in body for kw in keywords):
                return True
        return False
