# G:\Aoao_OS\apps\system_memory\utils\llm_client.py
import httpx
from typing import Optional
from openai import OpenAI, AsyncOpenAI
from kernel.config_loader import load_api_config
from kernel.logger import get_logger
from kernel.model_profile import configured_model, request_options

logger = get_logger(__name__)


# 精确超时配置：
#   connect=10s  — 建立 TCP 连接最多等 10 秒
#   read=90s     — 读取响应体最多等 90 秒（覆盖长 prompt 场景）
#   write=10s    — 上传请求体最多等 10 秒
#   pool=5s      — 从连接池取连接最多等 5 秒
_HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=5.0)

class LLMClient:
    def __init__(self):
        api_config = load_api_config()
        base_url = api_config.get("base_url")
        api_key = api_config.get("api_key")
        self.model_name = configured_model(api_config)

        # 同步客户端（保留兼容性，限制真实超时）
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key,
            http_client=httpx.Client(timeout=_HTTPX_TIMEOUT),
        )
        # 异步客户端（供 reflection.py 流式调用）
        self.async_client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            http_client=httpx.AsyncClient(timeout=_HTTPX_TIMEOUT),
        )

    async def generate_async(self, prompt: str) -> Optional[str]:
        """
        异步流式生成接口 — 专供 reflection.py 梦境引擎使用。
        流式读取可以让 asyncio.wait_for 真正感知每一个 token 到达，
        一旦外层 cancel，这里的 async for 循环立即退出，不会继续阻塞。

        失败语义 (错误类型化): 任何失败 (空 prompt / 网络/解析异常 / 流式空响应)
        一律返回 None。调用方必须显式判空, 绝不会把错误哨兵字符串当成合法内容,
        从源头杜绝 "Error: xxx" 被写入规则库这类毒化事故。
        成功时返回去空白后的生成文本。
        """
        if not prompt or not isinstance(prompt, str):
            logger.warning("[LLM] generate_async 收到空或非法 prompt，返回 None。")
            return None

        collected = []
        try:
            stream = await self.async_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6,
                stream=True,
                **request_options(self.model_name),
            )
            if stream is None:
                logger.warning("[LLM] generate_async: stream 为 None，返回 None。")
                return None

            async for chunk in stream:
                if chunk is None:
                    continue
                if not hasattr(chunk, 'choices') or not chunk.choices:
                    continue
                choice = chunk.choices[0]
                if not hasattr(choice, 'delta') or choice.delta is None:
                    continue
                delta = choice.delta.content
                if delta:
                    collected.append(delta)

            result = "".join(collected).strip()
            if not result:
                logger.warning("[LLM] generate_async: 流式响应为空，返回 None。")
                return None
            return result
        except Exception as e:
            logger.error(f"[LLM Error] generate_async 异常，返回 None: {e}")
            return None

    # 保留同步接口，兼容其他可能的调用方
    def generate_sync(self, prompt: str) -> Optional[str]:
        """失败一律返回 None，成功返回生成文本。"""
        if not prompt or not isinstance(prompt, str):
            logger.warning("[LLM] generate_sync 收到空或非法 prompt，返回 None。")
            return None

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6,
                **request_options(self.model_name),
            )
            if response is None:
                logger.warning("[LLM] generate_sync: response 为 None，返回 None。")
                return None
            if not hasattr(response, 'choices') or not response.choices:
                logger.warning("[LLM] generate_sync: response.choices 为空，返回 None。")
                return None
            choice = response.choices[0]
            if not hasattr(choice, 'message') or choice.message is None:
                logger.warning("[LLM] generate_sync: choice.message 为空，返回 None。")
                return None
            content = choice.message.content
            if content is None:
                logger.warning("[LLM] generate_sync: message.content 为 None，返回 None。")
                return None
            return content.strip()
        except Exception as e:
            logger.error(f"[LLM Error] generate_sync 异常，返回 None: {e}")
            return None

