"""共享 Docker CLI 与持续 attach RPC 传输，不依赖任何 App。"""
from __future__ import annotations

import asyncio
import base64
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from protocols.sandbox_rpc import MAX_RPC_PAYLOAD_BYTES


_DOCKER_CONTEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class DockerSandboxError(RuntimeError):
    """Docker engine、容器或 guest RPC 无法安全完成请求。"""


class DockerSandboxTimeout(DockerSandboxError):
    """宿主未在预算内收到 CLI 或 RPC 回执，不代表远端操作未执行。"""


@dataclass(frozen=True, slots=True)
class DockerCommandResult:
    exit_code: int
    stdout: bytes
    stderr: bytes


class DockerCommandRunner(Protocol):
    async def run(
        self,
        argv: list[str],
        *,
        stdin: bytes = b"",
        timeout: float = 30,
    ) -> DockerCommandResult: ...


class SandboxTransport(Protocol):
    async def start(self) -> None: ...

    async def exchange(
        self,
        payload: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class AsyncDockerCommandRunner:
    """始终以 argv 调 Docker CLI，不经过宿主 shell。"""

    async def run(
        self,
        argv: list[str],
        *,
        stdin: bytes = b"",
        timeout: float = 30,
    ) -> DockerCommandResult:
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (OSError, ValueError) as exc:
            raise DockerSandboxError("无法启动 Docker CLI") from exc
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            await process.wait()
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise DockerSandboxTimeout("Docker CLI 调用超时") from exc
        return DockerCommandResult(
            int(process.returncode or 0),
            stdout[:MAX_RPC_PAYLOAD_BYTES],
            stderr[:MAX_RPC_PAYLOAD_BYTES],
        )


class DockerAttachTransport:
    """单容器持续 attach 通道；stdout 只承载逐行 JSON RPC。"""

    def __init__(
        self,
        container_name: str,
        key: bytes,
        *,
        docker_cli: str = "docker",
        docker_context: str = "",
        max_output_bytes: int = 96 * 1024,
        max_timeout_seconds: float = 300,
        max_workspace_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        self.container_name = container_name
        self._key = key
        self._docker_cli = docker_cli
        if docker_context and not _DOCKER_CONTEXT_RE.fullmatch(docker_context):
            raise DockerSandboxError("Docker context 名称非法")
        self._docker_context = docker_context
        self._max_output_bytes = max_output_bytes
        self._max_timeout_seconds = max_timeout_seconds
        self._max_workspace_bytes = max_workspace_bytes
        self._process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._broken = False

    async def start(self) -> None:
        if self._broken:
            raise DockerSandboxError("Docker guest 通道已失效，禁止复用")
        if self._process is not None:
            return
        docker_argv = [self._docker_cli]
        if self._docker_context:
            docker_argv.extend(("--context", self._docker_context))
        try:
            process = await asyncio.create_subprocess_exec(
                *docker_argv,
                "attach",
                "--sig-proxy=false",
                self.container_name,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=MAX_RPC_PAYLOAD_BYTES + 4096,
            )
        except (OSError, ValueError) as exc:
            raise DockerSandboxError("无法建立 Docker guest 通道") from exc
        self._process = process
        assert process.stderr is not None
        self._stderr_task = asyncio.create_task(self._drain_stderr(process.stderr))
        bootstrap = {
            "type": "bootstrap",
            "key": base64.b64encode(self._key).decode("ascii"),
            "max_output_bytes": self._max_output_bytes,
            "max_timeout_seconds": self._max_timeout_seconds,
            "max_workspace_bytes": self._max_workspace_bytes,
        }
        await self._write_line(bootstrap)
        ready = await self._read_line(timeout=10)
        if ready != {"type": "ready"}:
            await self.close()
            raise DockerSandboxError("Docker guest 启动握手失败")

    async def exchange(
        self,
        payload: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        async with self._lock:
            worker = asyncio.create_task(self._exchange_once(payload, timeout=timeout))
            return await self._settle_owned_exchange(worker)

    async def _exchange_once(
        self,
        payload: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        try:
            await self._write_line(payload)
            return await self._read_line(timeout=timeout)
        except BaseException:
            await self._invalidate()
            raise

    async def _invalidate(self) -> None:
        self._broken = True
        try:
            await self.close()
        except BaseException:
            # _process 已在 close 入口清空；即使 OS 清理失败也不能复用该流。
            pass

    @staticmethod
    async def _settle_owned_exchange(worker: asyncio.Task[dict[str, Any]]) -> dict[str, Any]:
        """调用方取消后仍读完已发送请求，避免持续流响应错位。"""
        cancelled = False
        while True:
            try:
                result = worker.result() if worker.done() else await asyncio.shield(worker)
            except asyncio.CancelledError:
                if worker.cancelled():
                    raise
                cancelled = True
                continue
            except BaseException as exc:
                if cancelled:
                    raise asyncio.CancelledError() from exc
                raise
            if cancelled:
                raise asyncio.CancelledError()
            return result

    async def close(self) -> None:
        process, self._process = self._process, None
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
                try:
                    await process.stdin.wait_closed()
                except (BrokenPipeError, ConnectionError):
                    pass
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        task, self._stderr_task = self._stderr_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _write_line(self, payload: Mapping[str, Any]) -> None:
        process = self._process
        if process is None or process.stdin is None or process.returncode is not None:
            raise DockerSandboxError("Docker guest 通道未连接")
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > MAX_RPC_PAYLOAD_BYTES:
            raise DockerSandboxError("Docker guest 请求超过协议上限")
        process.stdin.write(encoded + b"\n")
        try:
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionError) as exc:
            raise DockerSandboxError("Docker guest 通道已断开") from exc

    async def _read_line(self, *, timeout: float) -> dict[str, Any]:
        process = self._process
        if process is None or process.stdout is None:
            raise DockerSandboxError("Docker guest 通道未连接")
        try:
            raw = await asyncio.wait_for(process.stdout.readline(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise DockerSandboxTimeout("Docker guest 响应超时") from exc
        if not raw or len(raw) > MAX_RPC_PAYLOAD_BYTES + 1:
            raise DockerSandboxError("Docker guest 响应为空或过大")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DockerSandboxError("Docker guest 响应不是合法 JSON") from exc
        if not isinstance(value, dict):
            raise DockerSandboxError("Docker guest 响应必须是对象")
        return value

    @staticmethod
    async def _drain_stderr(stream: asyncio.StreamReader) -> None:
        while await stream.read(8192):
            pass
