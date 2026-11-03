"""默认状态服务；显式内存端口可提供文本、诊断和不透明资产，无路径接口。"""

import asyncio
import logging
import time

from aiohttp import web
from protocols.wechat_inbox import InboxBatch, TextInboxPort, validate_inbox_request
from protocols.wechat_listening_status import ListeningStatusPort, parse_listening_status
from protocols.wechat_artifacts import (
    ArtifactChunk,
    ArtifactReadPort,
    ArtifactUploadChunk,
    ArtifactUploadPort,
    ArtifactUploadRequest,
    validate_artifact_request,
    validate_artifact_upload_payload,
)
from protocols.wechat_preview import PreviewFrame, PreviewReadPort
from protocols.wechat_input import ClickRequest, ClickFramePort, ManualInputPort
from protocols.wechat_send import (
    MediaSendPort,
    MediaSendRequest,
    TextSendPort,
    TextSendRequest,
    parse_chat_status,
    parse_send_result,
    validate_send_query,
)

from kernel.wechat_rpc.replay import SqliteReplayStore
from kernel.wechat_rpc.revocation import StatusRevocation
from kernel.wechat_rpc.tls import PeerIdentity, WeChatTransportError, private_ipv4
from kernel.wechat_rpc.wire import (
    MAX_WIRE_BYTES,
    decode_message,
    encode_message,
    transport_status,
)
from protocols.sandbox_rpc import (
    SandboxCapability,
    SandboxProtocolError,
    SandboxRequestVerifier,
    SandboxRpcRequest,
    SandboxRpcResponse,
    SandboxSigner,
)


class WeChatRpcServer:
    def __init__(
        self,
        *,
        identity: PeerIdentity,
        hmac_key: bytes,
        bind_host: str = "127.0.0.1",
        port: int = 0,
        replay_store: SqliteReplayStore | None = None,
        revocation: StatusRevocation | None = None,
        inbox: TextInboxPort | None = None,
        listening_status: ListeningStatusPort | None = None,
        artifacts: ArtifactReadPort | None = None,
        artifact_uploads: ArtifactUploadPort | None = None,
        preview: PreviewReadPort | None = None,
        input_port: ManualInputPort | None = None,
        text_sender: TextSendPort | None = None,
        media_sender: MediaSendPort | None = None,
    ) -> None:
        self._host = private_ipv4(bind_host)
        if revocation is not None and not isinstance(revocation, StatusRevocation):
            raise WeChatTransportError("状态撤销闸门非法")
        self._revocation = revocation if revocation is not None else StatusRevocation()
        if type(port) is not int or not 0 <= port <= 65535:
            raise WeChatTransportError("RPC 监听端口非法")
        self._port = port
        self._identity = identity
        if artifacts is not None and (
            not isinstance(artifacts, ArtifactReadPort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("资产读取需要已准备的账本与受限端口")
        self._artifacts = artifacts
        if artifact_uploads is not None and (
            not isinstance(artifact_uploads, ArtifactUploadPort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("附件上传需要已准备的账本与受限端口")
        self._artifact_uploads = artifact_uploads
        if preview is not None and (
            not isinstance(preview, PreviewReadPort) or artifacts is not preview
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("画面需要同一受限资产源和已准备账本")
        self._preview = preview
        if input_port is not None and (
            not isinstance(input_port, ManualInputPort) or not isinstance(preview, ClickFramePort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("单次点击需要显式驱动、可认领画面和账本")
        self._input = input_port
        self._input_busy = False
        self._input_unknown = False
        if listening_status is not None and (
            not isinstance(listening_status, ListeningStatusPort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("监听状态需要已准备的账本与受限端口")
        self._listening_status = listening_status
        if inbox is not None and (
            not isinstance(inbox, TextInboxPort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("文本收信需要已准备的账本与受限端口")
        if replay_store is not None:
            replay_store.verify_key(hmac_key)
        self._replay_store = replay_store
        self._inbox = inbox
        if text_sender is not None and (
            not isinstance(text_sender, TextSendPort) or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("文本发送需要显式端口和持久化认证账本")
        self._text_sender = text_sender
        if media_sender is not None and (
            not isinstance(media_sender, MediaSendPort)
            or not isinstance(replay_store, SqliteReplayStore)
        ):
            raise WeChatTransportError("附件发送需要显式端口和持久化认证账本")
        self._media_sender = media_sender
        allowed = {SandboxCapability.WECHAT_STATUS}
        if inbox is not None or text_sender is not None or media_sender is not None:
            allowed.add(SandboxCapability.WECHAT_COMMAND)
        if artifacts is not None:
            allowed.add(SandboxCapability.ARTIFACT_PULL)
        if artifact_uploads is not None:
            allowed.add(SandboxCapability.ARTIFACT_PUSH)
        if input_port is not None:
            allowed.add(SandboxCapability.WECHAT_INPUT)
        self._signer = SandboxSigner(hmac_key)
        self._verifier = SandboxRequestVerifier(hmac_key, allowed_capabilities=allowed)
        self._runner: web.AppRunner | None = None
        self._lifecycle = asyncio.Lock()
        self._slots = asyncio.Semaphore(8)

    @property
    def endpoint(self) -> str:
        if self._runner is None or not self._runner.addresses:
            raise WeChatTransportError("RPC 服务未启动")
        return f"https://{self._host}:{self._runner.addresses[0][1]}"

    async def __aenter__(self) -> "WeChatRpcServer":
        async with self._lifecycle:
            await self._revocation.verify()
            if self._runner is not None:
                raise WeChatTransportError("RPC 服务已经启动")
            context = await self._identity.context(server=True)
            await self._revocation.verify()
            app = web.Application(client_max_size=MAX_WIRE_BYTES)
            app.router.add_route("*", "/{tail:.*}", self._handle)
            # 禁止框架记录原始 HTTP 输入；只有进入应用处理器的错误保证固定响应。
            quiet_logger = logging.Logger("wechat_rpc.http")
            quiet_logger.addHandler(logging.NullHandler())
            quiet_logger.propagate = False
            runner = web.AppRunner(
                app,
                access_log=None,
                logger=quiet_logger,
                auto_decompress=False,
                keepalive_timeout=1,
                shutdown_timeout=1,
                max_line_size=4096,
                max_field_size=4096,
            )
            # 一旦拥有 runner 就保留引用；清理失败后仍须能够重试回收监听。
            self._runner = runner
            try:
                await runner.setup()
                await web.TCPSite(
                    runner, self._host, self._port, ssl_context=context, backlog=16
                ).start()
                await self._revocation.verify()
            except BaseException:
                await self._cleanup_runner(runner)
                self._runner = None
                raise
            return self

    async def __aexit__(self, *args) -> None:
        async with self._lifecycle:
            if self._runner is not None:
                cancelled = await self._cleanup_runner(self._runner)
                self._runner = None
                if cancelled:
                    raise asyncio.CancelledError

    @staticmethod
    async def _cleanup_runner(runner: web.AppRunner) -> bool:
        """取消可以传回调用方，但不能打断已开始的连接回收。"""
        cleanup = asyncio.create_task(runner.cleanup())
        cancelled = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled = True
        cleanup.result()
        return cancelled

    async def _handle(self, request: web.Request) -> web.Response:
        try:
            self._revocation.check()
            self._identity.verify_peer(
                request.transport.get_extra_info("ssl_object")
                if request.transport
                else None
            )
        except WeChatTransportError:
            return self._error(403)
        if request.method != "POST" or request.raw_path != "/rpc":
            return self._error(404)
        if (
            request.content_type != "application/json"
            or request.charset not in (None, "utf-8")
            or "Content-Encoding" in request.headers
            or "Transfer-Encoding" in request.headers
            or request.content_length is None
            or not 0 < request.content_length <= MAX_WIRE_BYTES
        ):
            return self._error(400)
        if self._slots.locked():
            return self._error(503)
        async with self._slots:
            try:
                body = await asyncio.wait_for(
                    request.content.readexactly(request.content_length), timeout=3
                )
                envelope, deadline = self._verify_request(body)
                await self._revocation.verify()
                await self._consume_persisted_nonce(envelope.nonce)
                await self._revocation.verify()
                if envelope.capability == SandboxCapability.WECHAT_INPUT:
                    payload = await self._click(envelope, deadline)
                elif envelope.capability == SandboxCapability.ARTIFACT_PUSH:
                    payload = await self._upload(envelope, deadline)
                elif envelope.capability == SandboxCapability.WECHAT_COMMAND and envelope.payload.get("operation") in {"send_text", "send_media", "send_status"}:
                    payload = await self._send(envelope, deadline)
                else:
                    payload = self._dispatch(envelope, deadline)
                return self._signed_response(envelope.request_id, payload)
            except (
                SandboxProtocolError,
                ValueError,
                asyncio.TimeoutError,
                asyncio.IncompleteReadError,
                RecursionError,
            ):
                return self._error(403)

    def _verify_request(self, body: bytes) -> tuple[SandboxRpcRequest, float]:
        envelope = SandboxRpcRequest.from_payload(decode_message(body))
        self._revocation.check()
        self._verifier.verify(envelope)
        deadline = (
            time.monotonic()
            + envelope.deadline_seconds
            - max(0.0, time.time() - envelope.timestamp)
        )
        if envelope.capability == SandboxCapability.WECHAT_STATUS and envelope.payload:
            listening = self._listening_status is not None and envelope.payload == {"operation": "listening_status"}
            preview = self._preview is not None and envelope.payload == {"operation": "preview_frame"}
            chat = self._text_sender is not None and envelope.payload == {"operation": "chat_status"}
            if not (listening or preview or chat):
                raise SandboxProtocolError("状态查询参数非法或端口未启用")
        if envelope.capability == SandboxCapability.WECHAT_COMMAND:
            operation = envelope.payload.get("operation")
            if self._text_sender is not None and operation == "send_text":
                TextSendRequest.from_payload(envelope.payload)
            elif self._media_sender is not None and operation == "send_media":
                MediaSendRequest.from_payload(envelope.payload)
            elif self._text_sender is not None and operation == "send_status":
                validate_send_query(envelope.payload)
            else:
                validate_inbox_request(envelope.payload)
        if envelope.capability == SandboxCapability.ARTIFACT_PULL:
            validate_artifact_request(envelope.payload)
        if envelope.capability == SandboxCapability.ARTIFACT_PUSH:
            if self._artifact_uploads is None:
                raise SandboxProtocolError("附件上传接口未启用")
            validate_artifact_upload_payload(envelope.payload)
        if envelope.capability == SandboxCapability.WECHAT_INPUT:
            ClickRequest.from_payload(envelope.payload)
        return envelope, deadline

    async def _send(self, envelope: SandboxRpcRequest, deadline: float) -> dict:
        self._revocation.check()
        if time.monotonic() >= deadline:
            raise SandboxProtocolError("发送请求已过期")
        payload = envelope.payload
        if payload["operation"] == "send_text":
            if self._text_sender is None:
                raise SandboxProtocolError("文本发送不可用")
            result = await self._text_sender.send_text(TextSendRequest.from_payload(payload), deadline=deadline)
        elif payload["operation"] == "send_media":
            if self._media_sender is None:
                raise SandboxProtocolError("附件发送不可用")
            result = await self._media_sender.send_media(
                MediaSendRequest.from_payload(payload), deadline=deadline,
            )
        else:
            if self._text_sender is None:
                raise SandboxProtocolError("发送状态不可用")
            result = await self._text_sender.send_status(payload["operation_id"], payload["conversation_id"])
        parse_send_result(result, payload["operation_id"], payload["conversation_id"])
        return result

    async def _upload(self, envelope: SandboxRpcRequest, deadline: float) -> dict:
        self._revocation.check()
        if self._artifact_uploads is None or time.monotonic() >= deadline:
            raise SandboxProtocolError("附件上传不可用或请求已过期")
        payload = dict(envelope.payload)
        operation = payload["operation"]
        try:
            if operation == "begin":
                request = ArtifactUploadRequest.from_payload(payload)
                await self._artifact_uploads.begin(request)
                asset_id, outcome = request.artifact.asset_id, "accepted"
            elif operation == "chunk":
                chunk = ArtifactUploadChunk.from_payload(payload)
                await self._artifact_uploads.write(chunk)
                asset_id, outcome = chunk.artifact.asset_id, "accepted"
            elif operation == "commit":
                asset_id, outcome = payload["asset_id"], "stored"
                await self._artifact_uploads.commit(asset_id)
            else:
                asset_id, outcome = payload["asset_id"], "aborted"
                await self._artifact_uploads.abort(asset_id)
        except Exception:
            raise SandboxProtocolError("附件上传操作失败") from None
        return {
            "schema_version": 2,
            "operation": operation,
            "asset_id": asset_id,
            "outcome": outcome,
        }

    async def _click(self, envelope: SandboxRpcRequest, deadline: float) -> dict:
        if self._input is None or self._input_busy or self._input_unknown:
            return {"schema_version": 1, "outcome": "rejected"}

        async def apply() -> str:
            try:
                age = time.time() - envelope.timestamp
                self._revocation.check()
                if time.monotonic() >= deadline or not -30 <= age <= envelope.deadline_seconds:
                    raise ValueError
                action = ClickRequest.from_payload(envelope.payload)
                action.frame.require_fresh()
                self._preview.claim_click(action.frame, action.width, action.height)
            except ValueError:
                return "rejected"
            self._input_unknown = True
            result = await self._input.click(action)
            if result is not None:
                return "unknown"
            self._input_unknown = False
            return "submitted"

        self._input_busy = True
        work = asyncio.create_task(apply())
        cancelled = False
        try:
            while not work.done():
                try:
                    await asyncio.shield(work)
                except asyncio.CancelledError:
                    if not cancelled:
                        work.cancel()
                    cancelled = True
                except Exception:
                    break
            try:
                outcome = work.result()
            except Exception:
                outcome = "unknown"
                self._input_unknown = True
            if cancelled:
                self._input_unknown = True
                raise asyncio.CancelledError
            return {"schema_version": 1, "outcome": outcome}
        finally:
            self._input_busy = False

    def _signed_response(self, request_id: str, payload: dict) -> web.Response:
        response = self._signer.sign_response(
            SandboxRpcResponse.create(request_id, ok=True, code="ok", payload=payload)
        )
        result = web.Response(
            body=encode_message(response.to_payload()), content_type="application/json"
        )
        result.force_close()
        return result

    def _dispatch(self, envelope: SandboxRpcRequest, deadline: float) -> dict:
        if envelope.capability == SandboxCapability.WECHAT_STATUS and not envelope.payload:
            return transport_status()
        # 所有登记/nonce await 后再检查；不重新认领 nonce，也不延长未来时间戳的预算。
        age = time.time() - envelope.timestamp
        if time.monotonic() > deadline or age < -30 or age > envelope.deadline_seconds:
            raise SandboxProtocolError("受限请求已过期")
        payload = envelope.payload
        if envelope.capability == SandboxCapability.ARTIFACT_PULL:
            if self._artifacts is None:
                raise SandboxProtocolError("资产读取接口未启用")
            try:
                chunk = self._artifacts.read(**payload)
            except Exception:
                raise SandboxProtocolError("资产分块不可用") from None
            if type(chunk) is not ArtifactChunk:
                raise SandboxProtocolError("资产分块不可用")
            if (
                chunk.descriptor.asset_id != payload["asset_id"]
                or chunk.offset != payload["offset"]
                or len(chunk.data) != min(payload["limit"], chunk.descriptor.size_bytes - chunk.offset)
            ):
                raise SandboxProtocolError("资产分块与请求不符")
            return chunk.to_payload()
        if envelope.capability == SandboxCapability.WECHAT_STATUS:
            if payload == {"operation": "chat_status"} and self._text_sender is not None:
                return parse_chat_status(self._text_sender.chat_status())
            if payload == {"operation": "preview_frame"} and self._preview is not None:
                try:
                    frame = self._preview.latest()
                    if type(frame) is not PreviewFrame:
                        raise ValueError
                    frame.require_fresh()
                    return frame.to_payload()
                except Exception:
                    raise SandboxProtocolError("画面暂不可用") from None
            if self._listening_status is None:
                raise SandboxProtocolError("监听状态接口未启用")
            report = {"schema_version": 1, "listening": self._listening_status.status()}
            return {"schema_version": 1, "listening": parse_listening_status(report)}
        if self._inbox is None:
            raise SandboxProtocolError("文本收信接口未启用")
        if payload["operation"] == "read":
            batch = self._inbox.read(limit=payload["limit"])
            if type(batch) is not InboxBatch:
                raise SandboxProtocolError("文本收信批次不可用")
            return batch.to_payload()
        acknowledged = self._inbox.acknowledge(
            stream_id=payload["stream_id"], batch_id=payload["batch_id"]
        )
        if type(acknowledged) is not bool:
            raise SandboxProtocolError("文本收信确认结果未知")
        return {
            "schema_version": 1,
            "stream_id": payload["stream_id"],
            "batch_id": payload["batch_id"],
            "acknowledged": acknowledged,
        }

    async def _consume_persisted_nonce(self, nonce: str) -> None:
        if self._replay_store is None:
            return
        # SQLite 不占事件循环；取消也先等本次写入结束，再释放并发槽。
        work = asyncio.create_task(asyncio.to_thread(self._replay_store.consume, nonce))
        cancelled = False
        while not work.done():
            try:
                await asyncio.shield(work)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break  # 下方 result 原样传播写入失败，不将其变成成功。
        try:
            work.result()
        finally:
            if cancelled:
                raise asyncio.CancelledError

    @staticmethod
    def _error(status: int) -> web.Response:
        result = web.Response(status=status, text="RPC request rejected")
        result.force_close()
        return result
