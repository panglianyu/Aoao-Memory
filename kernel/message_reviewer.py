# kernel/message_reviewer.py
"""[阶段2 0.1.12.4 方案A] 分身审核 agent: 只打 Tag+权重, 不拦截消息。

阿露的分身(同家族极速版 + 注入阿露核心记忆+性格偏好), 不是外部裁判。
在 think_batch 每组 think 前审本批消息, 输出每条消息的 Tag+权重+建议, **不拦截消息**:
完整批次全送阿露 think, snapshot 注入 review_hint(每条 Tag), 阿露按 Tag 自己决定回不回。
4点决策第3点: 只打 Tag 不删消息, 最终决定权在阿露 (防秘书发疯)。

治 0.1.12.2/0.1.12.3 三症状(延迟/脱节/没拦截):
- 旧 deliver/pending 划分+pending 回灌 scheduler 循环 -> 延迟累加(MAX_DEFER=3轮才回)。
- 旧兜底全 deliver -> 没真正拦住只拖延。
- 旧 reviewer 判定与阿露回复脱节。
方案A: reviewer 只打 Tag, 完整批次全送 think, 无循环无延迟无脱节。
拦截靠阿露 prompt 契约([低权重/已读不回] Tag 的消息阿露可不回)。

老板 is_master 批次免审(4点决策4) fallback 全标 should_reply=True。
失败 fallback: LLM超时/JSON失败 -> 全标 weight 0.5 should_reply True, 不阻塞。
红绿灯(0.1.12.3): config/reviewer.yaml enabled=false 跳过 LLM。
"""
import asyncio
import json
import os
import re

import yaml
from openai import AsyncOpenAI

from kernel.config_loader import load_api_config
from kernel.logger import get_logger
from kernel.message import Message
from kernel.model_profile import configured_model, request_options

logger = get_logger(__name__)


class MessageReviewer:
    """分身审核 agent (语义层, 阿露的潜意识分身, 只打 Tag 不拦截)。

    用法:
      reviewer = MessageReviewer(bus)
      result = await reviewer.review(msgs, context)
      hint = reviewer.build_snapshot_hint(result, msgs)  # 注入 snapshot
    """

    # [0.1.x.x 修复] 复用主脑 qwen3.7-plus 当分身模型(旧 qwen-turbo 404), 单次审核~29s,
    #   原 8s 必超时 fallback (分身从未真审过)。放宽到 45s 对齐主脑 think LLM_TIMEOUT,
    #   群聊批阅本就在心跳内串行, 不比 think 本身更拖。老板批免审/单条仍跳过不调 LLM。
    LLM_TIMEOUT: float = 45.0

    def __init__(self, bus=None) -> None:
        self._bus = bus
        try:
            cfg = load_api_config()
            self._model = configured_model(cfg, reviewer=True)
            self._client = AsyncOpenAI(
                base_url=cfg.get("base_url", ""),
                api_key=cfg.get("api_key", ""),
            )
        except Exception as exc:
            logger.warning(
                "[MessageReviewer] 初始化失败(将全程 fallback, type=%s)",
                type(exc).__name__)
            self._client = None
            self._model = ""  # Invalid configuration cannot select a fallback provider.

    async def review(self, msgs: list, context: dict) -> dict:
        """审核本批消息, 输出每条 Tag+权重+建议 (不拦截, 完整批次全送阿露)。

        Args:
            msgs: 本组消息(同 chat, 保时间序)
            context: {char_name, identity, soul_memory, social_context, warm_recall, known_members}
        Returns:
            {messages:[{msg_id,topic,weight,tag,should_reply,reason}],
             new_faces:[sender], overall:str, fallback:bool}
        老板批免审 / 单条 / client不可用 / 失败 -> fallback 全标 should_reply=True (不拦截)。
        """
        if not self._load_enabled():
            return self._fallback(msgs, reason="审核已关闭(红绿灯off)")
        if any(bool(m.metadata.get("is_master")) if m.metadata else False for m in msgs):
            return self._fallback(msgs, reason="老板批免审")
        if not self._client:
            return self._fallback(msgs, reason="client不可用")
        # [P2 0.2.4.6] 单条也审, 但仅当疑似点名阿露时才调 LLM (控成本), 否则 fallback.
        # 治单条非老板点名阿露(如"阿露，群里谁的淫商最高")旧逻辑直接 fallback 无 Tag 提示,
        # 阿露失去"这条是点我叫我"的权重参考 -> 该回不回. 点名单条 fallthrough 真审.
        if len(msgs) <= 1:
            char_name = (context.get("char_name") if context else "") or "阿露"
            if not self._looks_called(msgs, char_name):
                return self._fallback(msgs, reason="单条且未点名阿露")

        try:
            result = await asyncio.wait_for(
                self._review_llm(msgs, context),
                timeout=self.LLM_TIMEOUT,
            )
            # [0.2.4.8] 成功路径打 log: 治"只记 fallback 不记审了什么", 看出有审 vs 没审。
            tags = [(m.get("tag",""), m.get("weight")) for m in result.get("messages", [])]
            logger.info(f"[MessageReviewer] 审核完成 ({len(msgs)}条): tags={tags} overall={result.get('overall','')!r}")
            return result
        except asyncio.TimeoutError:
            logger.warning(f"[MessageReviewer] LLM 超时 {self.LLM_TIMEOUT}s, fallback")
            return self._fallback(msgs, reason="LLM超时")
        except Exception as exc:
            logger.warning(
                "[MessageReviewer] 审核失败 fallback (type=%s)",
                type(exc).__name__)
            return self._fallback(msgs, reason=f"异常:{type(exc).__name__}")

    async def _review_llm(self, msgs: list, context: dict) -> dict:
        """调极速版 LLM 审核每条消息 Tag+权重。"""
        prompt = self._build_prompt(msgs, context)
        resp = await self._client.chat.completions.create(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            timeout=self.LLM_TIMEOUT,
            **request_options(self._model),
        )
        raw = resp.choices[0].message.content or ""
        return self._parse_result(raw, msgs)

    def _build_prompt(self, msgs: list, context: dict) -> str:
        """构造审核 prompt: 注入阿露核心记忆+性格偏好+本批消息, 输出每条 Tag。"""
        char_name = context.get("char_name", "阿露")
        identity = context.get("identity", "")
        soul = (context.get("soul_memory") or "")[:500]
        social = context.get("social_context", "")
        warm = (context.get("warm_recall") or "")[:300]
        known = context.get("known_members", []) or []

        lines = []
        for i, m in enumerate(msgs):
            mid = m.id[:8]
            sender = m.metadata.get("sender", "?") if m.metadata else "?"
            content = m.content[:200]
            lines.append(f"[{i}] id={mid} sender={sender}: {content}")
        msgs_str = "\n".join(lines)
        known_str = ", ".join(known) if known else "(无)"

        return f"""你是 {char_name} 的潜意识分身, 用 {char_name} 的性格偏好审核本批消息。
{char_name} 身份: {identity}
核心规矩摘要: {soul}
近期上下文: {warm}
本对话对象亲疏: {social}
本群已知成员: {known_str}

本批消息(保时间序):
{msgs_str}

审核任务: 给每条消息打 Tag+权重+建议 (不拦截, 完整批次全送 {char_name}, 她按 Tag 自己决定回不回)。
- topic: 话题(同主题消息归同 topic, 如多人讨论吃西瓜归一起)
- weight 0-1: {char_name} 关心/想回的高权重, 无关噪音/招呼低权重
- tag: 标签(闲聊/游戏/招呼/工作/三国杀 等)
- should_reply: true=建议回, false=建议已读不回 ({char_name} 可无视, 最终决定权在她)
- sender 不在"本群已知成员"里的列入 new_faces

只输出 JSON, 不要其他文字:
{{
  "messages": [
    {{"msg_id": "0", "topic": "吃西瓜", "weight": 0.9, "tag": "闲聊", "should_reply": true, "reason": "多人讨论"}},
    {{"msg_id": "1", "topic": "吃西瓜", "weight": 0.9, "tag": "闲聊", "should_reply": true, "reason": "同主题"}},
    {{"msg_id": "2", "topic": "boss", "weight": 0.3, "tag": "游戏", "should_reply": false, "reason": "与当前话题无关"}}
  ],
  "new_faces": ["新sender"],
  "overall": "多人讨论吃西瓜, boss 话题无关可暂缓"
}}
msg_id 用上面 [i] 的索引数字字符串, 每条消息都要列。"""

    def _parse_result(self, raw: str, msgs: list) -> dict:
        """解析 LLM JSON 输出, 索引->msg id。失败 fallback。"""
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if not m:
            logger.warning(f"[MessageReviewer] JSON 未找到, fallback. raw={raw[:200]}")
            return self._fallback(msgs, reason="JSON未找到")
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            logger.warning(f"[MessageReviewer] JSON 解析失败: {e}, fallback. raw={raw[:200]}")
            return self._fallback(msgs, reason="JSON解析失败")

        messages = []
        for item in data.get("messages", []) or []:
            try:
                idx = int(item.get("msg_id", "-1"))
            except (ValueError, TypeError):
                continue
            if not (0 <= idx < len(msgs)):
                continue
            messages.append({
                "msg_id": msgs[idx].id,
                "topic": item.get("topic", ""),
                "weight": float(item.get("weight", 0.5)),
                "tag": item.get("tag", ""),
                "should_reply": bool(item.get("should_reply", True)),
                "reason": item.get("reason", ""),
            })

        # 兜底: messages 为空 -> fallback (不丢消息, 全标默认)
        if not messages:
            return self._fallback(msgs, reason="messages为空兜底")

        new_faces = list(data.get("new_faces", []) or [])
        overall = data.get("overall", "")

        return {
            "messages": messages,
            "new_faces": new_faces,
            "overall": overall,
            "fallback": False,
        }

    def _looks_called(self, msgs: list, char_name: str) -> bool:
        """[P2 0.2.4.6] 本批消息是否疑似点名阿露 (含名字/@阿露)。

        单条非老板点名阿露才值得调 LLM 审 (控成本), 闲聊灌水单条仍 fallback。
        """
        if not char_name:
            return False
        names = {char_name, f"@{char_name}"}
        for m in msgs:
            content = getattr(m, "content", "") or ""
            if any(n in content for n in names):
                return True
        return False

    def _load_enabled(self) -> bool:
        """读 config/reviewer.yaml enabled (红绿灯, 阿露经 system_settings 动态关)。默认 True。"""
        try:
            path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "reviewer.yaml",
            )
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return bool(data.get("enabled", True))
        except Exception:
            return True  # 文件不存在/异常默认开

    def _fallback(self, msgs: list, reason: str = "") -> dict:
        """fallback: 全标 weight 0.5 should_reply True (不拦截, 完整送阿露)。"""
        logger.info(f"[MessageReviewer] fallback({reason}): {len(msgs)}条全标默认(不拦截)")
        return {
            "messages": [{
                "msg_id": m.id,
                "topic": "",
                "weight": 0.5,
                "tag": "",
                "should_reply": True,
                "reason": f"fallback:{reason}",
            } for m in msgs],
            "new_faces": [],
            "overall": f"fallback:{reason}",
            "fallback": True,
        }

    def build_snapshot_hint(self, review: dict, msgs: list) -> str:
        """构造 snapshot 注入提示: 每条消息 Tag+权重+建议 (阿露参考, 有权无视)。

        [方案A 0.1.12.4] 只打标不拦截: hint 展示每条 Tag, 阿露按 Tag 自己决定回不回。
        """
        if review.get("fallback"):
            return ""
        msg_meta = {m["msg_id"]: m for m in review.get("messages", []) or []}
        lines = ["[潜意识分身审核参考 - 你有权无视, 最终决定权在你]"]
        for m in msgs:
            meta = msg_meta.get(m.id)
            if not meta:
                continue
            tag = meta.get("tag", "")
            weight = meta.get("weight", 0.5)
            topic = meta.get("topic", "")
            should = meta.get("should_reply", True)
            suggest = "建议回" if should else "建议已读不回"
            lines.append(
                f"  [{tag or '未标'}|权重{weight}|{topic}] {suggest}: {m.content[:60]}"
            )
        if review.get("new_faces"):
            lines.append(f"  新面孔: {review['new_faces']} (可主动询问记录)")
        if review.get("overall"):
            lines.append(f"  分身总评: {review['overall']}")
        return "\n".join(lines) + "\n"

    def build_master_addressing_hint(self, msgs: list, context: dict) -> str:
        """[老板识别 v0.2.6.0 档1] 规则化判定"老板在群回别人没喊阿露", 返回潜水 hint。

        独立于 LLM 审核: 老板批免审(review:78) + build_snapshot_hint fallback 返空(:257),
        故老板批拿不到 LLM tag; 本方法是确定性规则, 老板批也能拿到 hint。
        方案A 合规: 只打标不拦截, 给阿露"默认潜水别凑上去回老板, 但有意思可接话"的参考,
        最终决定权在阿露 (保"想回就回"的自主性)。
        档1 提及识别: 老板消息含 char_name/@char_name 即视为喊阿露 -> 不打潜水 hint(正常回)。

        Returns:
            潜水 hint 字符串; 无老板/纯老板1v1/老板喊阿露 -> 返空(不打)。
        """
        char_name = (context.get("char_name") if context else "") or "阿露"
        master_msgs = [m for m in msgs if m.metadata and m.metadata.get("is_master")]
        non_master = [m for m in msgs if not (m.metadata and m.metadata.get("is_master"))]
        # 无老板 或 纯老板1v1(没有他人消息) -> 不打: 老板1v1正常回, 无老板与本hint无关
        if not master_msgs or not non_master:
            return ""
        # 老板消息里含 char_name/@char_name -> 老板喊阿露, 正常回, 不打潜水hint
        names = {char_name, f"@{char_name}"}
        for m in master_msgs:
            content = getattr(m, "content", "") or ""
            if any(n in content for n in names):
                return ""
        # 群里有老板但老板没喊阿露 -> 老板在跟别人说话, 提示阿露默认潜水(但保留接话自由)
        return (
            f"[老板在场但没喊你: 老板正在群里跟别人说话, 消息里没点 {char_name}。"
            f"默认潜水, 别凑上去接老板的话; 但内容若真有你觉得有意思的, 你想接话也可以。]\n"
        )
