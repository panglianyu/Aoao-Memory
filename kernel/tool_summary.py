"""工具轮次耗尽后的有界总结；不派发工具，也不重试已有操作。"""
import asyncio

from kernel.logger import get_logger
from kernel.execution_evidence import with_execution_evidence


logger = get_logger(__name__)
SUMMARY_TIMEOUT_SECONDS = 20
SUMMARY_UNAVAILABLE = (
    "本轮工具执行已达到轮次上限，已停止继续调用，但结果总结未生成。"
    "已执行的操作不会因此撤销；请核对当前结果后再继续。"
)


async def summarize_tool_results(brain, messages: list) -> str:
    """只把已回填结果交给模型总结；异常、空答复或额外工具请求均不当作成功。"""
    from kernel.web_research.runtime import terminal_text
    research_text = terminal_text(brain)
    if research_text is not None:
        return research_text
    # 仅裁去本次请求之前的人格/历史，保留当前请求后的全部调用、回执与中断提示。
    # 恢复流程的 ask_user 答复是 tool 消息，仍随原请求一起保留，不改变调用配对。
    current_start = next(
        (i for i in range(len(messages) - 1, -1, -1) if messages[i].get("role") == "user"),
        0,
    )
    summary_messages = [*messages[current_start:], {
        "role": "system",
        "content": (
            "本轮工具执行轮次已用尽，现在只总结已有工具回执，不再执行任何操作。"
            "请简短说明已完成的结果、失败和仍未完成的部分。"
            "只有回执明确确认的操作才能称为成功，不得编造编号、文件、关闭或发送结果。"
            "最后一项工具成功不代表整个任务完成；没有完成的部分要明确告知用户。"
            "不要输出工具调用、技能配方或承诺后台继续，也不要把轮次上限称为思考超时。"
        ),
    }]
    run = getattr(brain, "_current_run", None)
    summary_messages = with_execution_evidence(run, summary_messages)
    if run is not None:
        run.llm_rounds += 1
    brain._turn_stage = "tool_result_summary"
    try:
        # 整段有界，包括视觉预处理；非流式避免无效总结先向用户展示半截文字。
        response = await asyncio.wait_for(
            brain._call_llm(summary_messages, summary_only=True),
            timeout=SUMMARY_TIMEOUT_SECONDS,
        )
        message = response.choices[0].message
        if message.tool_calls or not isinstance(message.content, str):
            return ""
        return message.content.strip()
    except Exception as exc:
        logger.warning("[tool_summary] 结果总结未完成: %s", type(exc).__name__)
        return ""
