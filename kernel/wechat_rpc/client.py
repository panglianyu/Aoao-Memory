"""状态、受限文本和不透明资产客户端；验证证书后才发送签名请求。"""

import asyncio
import hashlib
import math
from protocols.wechat_artifacts import (
    MAX_CHUNK_BYTES,
    ArtifactChunk,
    ArtifactDescriptor,
    ArtifactUploadChunk,
    ArtifactUploadRequest,
    artifact_upload_control,
)
from protocols.wechat_listening_status import parse_listening_status
from protocols.wechat_preview import PreviewFrame
from protocols.wechat_input import ClickRequest, parse_click_outcome
from protocols.wechat_send import (
    MediaSendRequest,
    TextSendRequest,
    parse_chat_status,
    parse_send_result,
    validate_send_query,
)
from protocols.wechat_inbox import (
    InboxBatch,
    parse_acknowledgement,
    validate_inbox_request,
)

from kernel.wechat_rpc.tls import PeerIdentity, WeChatTransportError, endpoint_address
from kernel.wechat_rpc.wire import (
    MAX_HEADER_BYTES,
    MAX_WIRE_BYTES,
    decode_message,
    encode_message,
    transport_status,
)
from protocols.sandbox_rpc import (
    SandboxCapability,
    SandboxProtocolError,
    SandboxRpcRequest,
    SandboxRpcResponse,
    SandboxSigner,
)


class WeChatRpcClient:
    def __init__(
        self,
        *,
        endpoint: str,
        identity: PeerIdentity,
        hmac_key: bytes,
        timeout_seconds: float = 5,
    ) -> None:
        self._host, self._port = endpoint_address(endpoint)
        if (
            type(timeout_seconds) not in (int, float)
            or not 0.1 <= timeout_seconds <= 30
        ):
            raise WeChatTransportError("RPC 超时必须在 0.1 到 30 秒之间")
        self._timeout = float(timeout_seconds)
        self._identity = identity
        self._signer = SandboxSigner(hmac_key)

    async def status(self) -> dict:
        try:
            payload = await self._request(SandboxCapability.WECHAT_STATUS, {})
            if payload != transport_status() or any(
                type(payload.get(key)) is not bool
                for key in ("transport_ready", "wechat_ready", "execution_enabled")
            ):
                raise WeChatTransportError("RPC 连接状态响应不符合只读契约")
            return payload
        except (
            OSError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            ValueError,
            RecursionError,
        ):
            raise WeChatTransportError(
                "微信安全连接状态检查失败；没有执行微信操作"
            ) from None

    async def listening_status(self) -> dict:
        """显式只读查询，不绑定、不读取消息、不缓存为执行授权。"""
        try:
            payload = await self._request(
                SandboxCapability.WECHAT_STATUS, {"operation": "listening_status"},
            )
            return parse_listening_status(payload)
        except ValueError:
            raise WeChatTransportError("微信监听状态未取得有效回执") from None

    async def preview_frame(self) -> tuple[PreviewFrame, bytes]:
        """单次获取同一引用的完整画面；无采集命令、自动刷新或输入授权。"""
        async def receive() -> tuple[PreviewFrame, bytes]:
            frame = PreviewFrame.from_payload(await self._request(
                SandboxCapability.WECHAT_STATUS, {"operation": "preview_frame"},
            ))
            frame.require_fresh()
            data = await self._pull_artifact(frame.artifact)
            frame.require_fresh()
            return frame, data

        try:
            return await asyncio.wait_for(receive(), timeout=self._timeout)
        except (ValueError, asyncio.TimeoutError):
            raise WeChatTransportError("未取得有效画面；没有自动采集或重试") from None

    async def manual_click(self, action: ClickRequest) -> str:
        """显式一次输入；任何传输错误均由调用方视为未知，不重试。"""
        if type(action) is not ClickRequest:
            raise WeChatTransportError("单次点击请求非法")
        action.frame.require_fresh()
        try:
            return parse_click_outcome(await self._request(SandboxCapability.WECHAT_INPUT, action.to_payload()))
        except ValueError:
            raise WeChatTransportError("单次点击结果未知；不得自动重试") from None

    async def read_inbox(self, *, limit: int = 16) -> InboxBatch:
        request = {"operation": "read", "limit": limit}
        validate_inbox_request(request)
        try:
            payload = await self._request(SandboxCapability.WECHAT_COMMAND, request)
            return InboxBatch.from_payload(payload)
        except ValueError:
            raise WeChatTransportError("微信文本收信读取失败；未自动确认批次") from None

    async def chat_status(self) -> dict:
        return parse_chat_status(await self._request(
            SandboxCapability.WECHAT_STATUS, {"operation": "chat_status"},
        ))

    async def send_text(self, request: TextSendRequest) -> str:
        if type(request) is not TextSendRequest:
            raise WeChatTransportError("微信发送请求非法")
        payload = await self._request(SandboxCapability.WECHAT_COMMAND, request.to_payload())
        return parse_send_result(payload, request.operation_id, request.conversation_id)

    async def send_media(self, request: MediaSendRequest) -> str:
        if type(request) is not MediaSendRequest:
            raise WeChatTransportError("微信附件发送请求非法")
        payload = await self._request(
            SandboxCapability.WECHAT_COMMAND, request.to_payload(),
        )
        return parse_send_result(
            payload, request.operation_id, request.conversation_id,
        )

    async def send_status(self, *, operation_id: str, conversation_id: str) -> str:
        payload = {"operation": "send_status", "operation_id": operation_id,
                   "conversation_id": conversation_id}
        validate_send_query(payload)
        result = await self._request(SandboxCapability.WECHAT_COMMAND, payload)
        return parse_send_result(result, operation_id, conversation_id)

    async def acknowledge_inbox(self, *, stream_id: str, batch_id: str) -> bool:
        """不自动重试；取消或回包失败不代表缓冲尚未确认。"""
        request = {
            "operation": "ack",
            "stream_id": stream_id,
            "batch_id": batch_id,
        }
        validate_inbox_request(request)
        try:
            payload = await self._request(SandboxCapability.WECHAT_COMMAND, request)
            return parse_acknowledgement(
                payload, stream_id=stream_id, batch_id=batch_id
            )
        except ValueError:
            raise WeChatTransportError(
                "微信收信批次确认结果未知；请核对原批次"
            ) from None

    async def pull_artifact(self, descriptor: ArtifactDescriptor) -> bytes:
        """总时限内完整校验后才返回；不自动重试、落盘、解析、预览或执行。"""
        if type(descriptor) is not ArtifactDescriptor:
            raise WeChatTransportError("资产读取需要有效元数据")
        try:
            return await asyncio.wait_for(self._pull_artifact(descriptor), self._timeout)
        except (ValueError, asyncio.TimeoutError):
            raise WeChatTransportError("资产未取得完整校验内容；未自动保存或重试") from None

    async def upload_artifact(self, request: ArtifactUploadRequest, data: bytes) -> str:
        """在单个总时限内顺序上传；不携带 Host 路径，也不触发微信发送。"""
        if (
            type(request) is not ArtifactUploadRequest
            or type(data) is not bytes
            or len(data) != request.artifact.size_bytes
            or hashlib.sha256(data).hexdigest() != request.artifact.sha256
        ):
            raise WeChatTransportError("附件上传内容与元数据不匹配")
        try:
            return await asyncio.wait_for(self._upload_artifact(request, data), self._timeout)
        except asyncio.CancelledError:
            await self._abort_artifact(request.artifact.asset_id)
            raise
        except (ValueError, asyncio.TimeoutError):
            await self._abort_artifact(request.artifact.asset_id)
            raise WeChatTransportError("附件未完整上传；未触发微信发送或自动重试") from None

    async def _abort_artifact(self, asset_id: str) -> None:
        try:
            payload = await asyncio.wait_for(
                self._request(
                    SandboxCapability.ARTIFACT_PUSH,
                    artifact_upload_control("abort", asset_id),
                ),
                timeout=min(2, self._timeout),
            )
            self._require_upload_receipt(payload, "abort", asset_id, "aborted")
        except (ValueError, asyncio.TimeoutError):
            return

    async def _upload_artifact(self, request: ArtifactUploadRequest, data: bytes) -> str:
        asset_id = request.artifact.asset_id
        self._require_upload_receipt(
            await self._request(SandboxCapability.ARTIFACT_PUSH, request.to_payload()),
            "begin", asset_id, "accepted",
        )
        for offset in range(0, len(data), MAX_CHUNK_BYTES):
            chunk = ArtifactUploadChunk(
                request.artifact, offset, data[offset:offset + MAX_CHUNK_BYTES],
            )
            self._require_upload_receipt(
                await self._request(SandboxCapability.ARTIFACT_PUSH, chunk.to_payload()),
                "chunk", asset_id, "accepted",
            )
        self._require_upload_receipt(
            await self._request(
                SandboxCapability.ARTIFACT_PUSH,
                artifact_upload_control("commit", asset_id),
            ),
            "commit", asset_id, "stored",
        )
        return asset_id

    @staticmethod
    def _require_upload_receipt(payload: dict, operation: str, asset_id: str, outcome: str) -> None:
        if payload != {
            "schema_version": 2,
            "operation": operation,
            "asset_id": asset_id,
            "outcome": outcome,
        }:
            raise WeChatTransportError("附件上传回执不匹配")

    async def _pull_artifact(self, descriptor: ArtifactDescriptor) -> bytes:
        data = bytearray()
        while len(data) < descriptor.size_bytes:
            limit = min(MAX_CHUNK_BYTES, descriptor.size_bytes - len(data))
            chunk = ArtifactChunk.from_payload(await self._request(
                SandboxCapability.ARTIFACT_PULL,
                {"asset_id": descriptor.asset_id, "offset": len(data), "limit": limit},
            ))
            if chunk.descriptor != descriptor or chunk.offset != len(data) or len(chunk.data) != limit:
                raise WeChatTransportError("资产分块与请求不符")
            data.extend(chunk.data)
        if hashlib.sha256(data).hexdigest() != descriptor.sha256:
            raise WeChatTransportError("资产完整性校验失败")
        return bytes(data)

    async def _request(self, capability: SandboxCapability, payload: dict) -> dict:
        try:
            return await asyncio.wait_for(
                self._exchange(capability, payload), timeout=self._timeout
            )
        except (
            OSError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            ValueError,
            RecursionError,
        ):
            raise WeChatTransportError("微信安全连接请求失败；请求结果未确认") from None

    async def _exchange(self, capability: SandboxCapability, payload: dict) -> dict:
        context = await self._identity.context()
        reader, writer = await asyncio.open_connection(
            self._host,
            self._port,
            ssl=context,
            server_hostname=self._host,
            ssl_handshake_timeout=self._timeout,
            limit=MAX_HEADER_BYTES,
        )
        try:
            self._identity.verify_peer(writer.get_extra_info("ssl_object"))
            request = self._signer.sign_request(
                SandboxRpcRequest.create(
                    capability,
                    payload,
                    deadline_seconds=math.ceil(self._timeout),
                )
            )
            body = encode_message(request.to_payload())
            headers = (
                f"POST /rpc HTTP/1.1\r\nHost: {self._host}:{self._port}\r\n"
                f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            writer.write(headers + body)
            await writer.drain()
            response = SandboxRpcResponse.from_payload(
                await self._read_response(reader)
            )
            self._signer.verify_response(
                response, expected_request_id=request.request_id
            )
            if not response.ok or response.code != "ok":
                raise WeChatTransportError("RPC 请求未成功")
            return response.to_payload()["payload"]
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1)
            except (OSError, asyncio.TimeoutError):
                writer.transport.abort()

    @staticmethod
    async def _read_response(reader: asyncio.StreamReader) -> dict:
        try:
            header = await reader.readuntil(b"\r\n\r\n")
        except asyncio.LimitOverrunError:
            raise WeChatTransportError("RPC HTTP 响应头过大") from None
        if len(header) > MAX_HEADER_BYTES:
            raise WeChatTransportError("RPC HTTP 响应头过大")
        lines = header.decode("ascii").split("\r\n")
        if not lines[0].startswith("HTTP/1.1 200 "):
            raise WeChatTransportError("RPC HTTP 请求被拒绝")
        fields = {}
        for line in lines[1:-2]:
            key, separator, value = line.partition(":")
            key = key.lower()
            if not separator or key in fields:
                raise WeChatTransportError("RPC HTTP 响应头非法")
            fields[key] = value.strip()
        length = fields.get("content-length", "")
        if (
            not length.isascii()
            or not length.isdecimal()
            or not 0 < int(length) <= MAX_WIRE_BYTES
            or fields.get("content-type", "").split(";")[0] != "application/json"
            or "transfer-encoding" in fields
            or "content-encoding" in fields
        ):
            raise SandboxProtocolError("RPC HTTP 正文格式非法")
        return decode_message(await reader.readexactly(int(length)))
