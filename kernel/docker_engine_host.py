"""Docker engine 宿主 seam：本机或受审查的 VirtualBox Linux VM。"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Protocol

from kernel.docker_transport import (
    AsyncDockerCommandRunner,
    DockerCommandRunner,
    DockerSandboxError,
)
from protocols.docker_host import DockerHostSettings


_VBOX_VERSION_RE = re.compile(r"^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)")
_ALLOWED_START_STATES = {"poweroff", "saved", "aborted", "aborted-saved"}
_OFF_VALUES = {"", "0", "disabled", "false", "none", "off"}


class DockerEngineHost(Protocol):
    """在 Docker runtime 启动前准备并审查 engine 宿主。"""

    async def prepare(self) -> None: ...

    async def close(self) -> None: ...


class LocalDockerEngineHost:
    """本地 Docker engine adapter；宿主检查由 Docker runtime 完成。"""

    async def prepare(self) -> None:
        return None

    async def close(self) -> None:
        return None


class VirtualBoxDockerEngineHost:
    """审查并以 headless 方式启动固定 Linux VM。"""

    def __init__(
        self,
        settings: DockerHostSettings,
        *,
        virtualbox_runner: DockerCommandRunner | None = None,
        docker_runner: DockerCommandRunner | None = None,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        if settings.engine_host != "virtualbox" or settings.virtualbox is None:
            raise DockerSandboxError("VirtualBox Docker host 配置不完整")
        self.settings = settings
        self.virtualbox = settings.virtualbox
        self._virtualbox_runner = virtualbox_runner or AsyncDockerCommandRunner()
        self._docker_runner = docker_runner or AsyncDockerCommandRunner()
        self._poll_interval_seconds = max(0.01, poll_interval_seconds)

    async def prepare(self) -> None:
        await self._verify_virtualbox_version()
        await self._verify_docker_context()
        machine = await self._inspect_machine()
        self._verify_machine_profile(machine)
        state = machine.get("VMState", "").lower()
        if state == "paused":
            await self._run_virtualbox(
                ["controlvm", self.virtualbox.machine_name, "resume"],
                timeout=20,
                failure="VirtualBox Linux VM 无法恢复",
            )
        elif state in _ALLOWED_START_STATES:
            await self._run_virtualbox(
                ["startvm", self.virtualbox.machine_name, "--type", "headless"],
                timeout=30,
                failure="VirtualBox Linux VM 无法启动",
            )
        elif state != "running":
            raise DockerSandboxError(
                f"VirtualBox Linux VM 状态不可启动: {state or 'unknown'}"
            )
        if state != "running":
            await self._wait_until_running()
        await self._wait_until_docker_ready()

    async def close(self) -> None:
        # Linux VM 是持久宿主，App 断开时不强制关机。
        return None

    async def _verify_virtualbox_version(self) -> None:
        result = await self._virtualbox_runner.run(
            [self.virtualbox.cli, "--version"],
            timeout=10,
        )
        version = result.stdout.decode("ascii", errors="ignore").strip()
        match = _VBOX_VERSION_RE.match(version)
        if result.exit_code != 0 or match is None:
            raise DockerSandboxError("VirtualBox CLI 不可用或版本无法识别")
        version_pair = (int(match.group("major")), int(match.group("minor")))
        if version_pair < (7, 1):
            raise DockerSandboxError("VirtualBox 版本低于受审查基线 7.1")

    async def _verify_docker_context(self) -> None:
        result = await self._docker_runner.run(
            [
                self.settings.docker_cli,
                "context",
                "inspect",
                self.settings.docker_context,
                "--format",
                "{{.Endpoints.docker.Host}}",
            ],
            timeout=10,
        )
        endpoint = result.stdout.decode("utf-8", errors="replace").strip()
        if result.exit_code != 0 or endpoint != self.settings.expected_docker_endpoint:
            raise DockerSandboxError("Docker context 未指向预期 Host-only VM 端点")

    async def _inspect_machine(self) -> dict[str, str]:
        result = await self._virtualbox_runner.run(
            [
                self.virtualbox.cli,
                "showvminfo",
                self.virtualbox.machine_name,
                "--machinereadable",
            ],
            timeout=15,
        )
        if result.exit_code != 0:
            raise DockerSandboxError("VirtualBox Linux VM 不存在或无法读取")
        return parse_machine_info(result.stdout)

    def _verify_machine_profile(self, machine: dict[str, str]) -> None:
        if machine.get("name") != self.virtualbox.machine_name:
            raise DockerSandboxError("VirtualBox VM 名称与配置不匹配")
        os_type = machine.get("ostype", "")
        if not re.fullmatch(
            r"(?:Ubuntu|Debian).*(?:_64|\(64-bit\))",
            os_type,
        ):
            raise DockerSandboxError("VirtualBox Docker host 必须是受支持的 64 位 Linux")
        self._require_off(machine, "clipboard", "共享剪贴板")
        self._require_off(
            machine,
            ("clipboard_file_transfers", "clipboardfiletransfers"),
            "剪贴板文件传输",
        )
        self._require_off(machine, "draganddrop", "拖放")
        self._require_off(machine, "vrde", "VRDE")
        self._require_off(machine, ("usb", "usbohci"), "USB OHCI 控制器")
        self._require_off(machine, ("ehci", "usbehci"), "USB EHCI 控制器")
        self._require_off(machine, ("xhci", "usbxhci"), "USB xHCI 控制器")
        self._require_off(machine, "audio", "音频控制器")
        self._require_off(machine, ("audio_in", "audioin"), "音频输入")
        self._require_off(machine, ("audio_out", "audioout"), "音频输出")
        self._require_off(
            machine,
            ("recording_enabled", "recording"),
            "屏幕录制",
        )
        if any(key.lower().startswith("sharedfoldername") for key in machine):
            raise DockerSandboxError("VirtualBox Docker host 不得配置共享文件夹")

        nic1 = machine.get("nic1", "").lower()
        if nic1 not in {"hostonly", "hostonlynet"}:
            raise DockerSandboxError("VirtualBox NIC1 必须为 Host-only")
        host_only_network = machine.get("hostonlynet1") or machine.get(
            "hostonlyadapter1"
        )
        if host_only_network != self.virtualbox.host_only_network:
            raise DockerSandboxError("VirtualBox Host-only 网络与配置不匹配")
        for index in range(2, 9):
            key = f"nic{index}"
            if key not in machine:
                raise DockerSandboxError(f"VirtualBox VM 信息缺少 NIC{index} 状态")
            mode = machine[key].lower()
            if mode not in {"none", "null"}:
                raise DockerSandboxError(
                    f"VirtualBox NIC{index} 不得使用 {mode or 'unknown'} 网络"
                )

    async def _wait_until_running(self) -> None:
        deadline = asyncio.get_running_loop().time() + (
            self.virtualbox.start_timeout_seconds
        )
        while asyncio.get_running_loop().time() < deadline:
            machine = await self._inspect_machine()
            if machine.get("VMState", "").lower() == "running":
                return
            await asyncio.sleep(self._poll_interval_seconds)
        raise DockerSandboxError("VirtualBox Linux VM 启动超时")

    async def _wait_until_docker_ready(self) -> None:
        deadline = asyncio.get_running_loop().time() + (
            self.virtualbox.start_timeout_seconds
        )
        while asyncio.get_running_loop().time() < deadline:
            try:
                result = await self._docker_runner.run(
                    [
                        self.settings.docker_cli,
                        "--context",
                        self.settings.docker_context,
                        "info",
                        "--format",
                        "{{.ServerVersion}}",
                    ],
                    timeout=5,
                )
            except DockerSandboxError:
                pass
            else:
                version = result.stdout.decode("ascii", errors="ignore").strip()
                if result.exit_code == 0 and _VBOX_VERSION_RE.match(version):
                    return
            await asyncio.sleep(self._poll_interval_seconds)
        raise DockerSandboxError("VirtualBox Linux VM 内 Docker engine 启动超时")

    async def _run_virtualbox(
        self,
        args: list[str],
        *,
        timeout: float,
        failure: str,
    ) -> None:
        result = await self._virtualbox_runner.run(
            [self.virtualbox.cli, *args],
            timeout=timeout,
        )
        if result.exit_code != 0:
            if b'VERR_SVM_IN_USE' in result.stderr:
                raise DockerSandboxError('AMD-V 正被其他虚拟化程序占用，请退出模拟器后重试')
            raise DockerSandboxError(failure)

    @staticmethod
    def _require_off(
        machine: dict[str, str],
        keys: str | tuple[str, ...],
        label: str,
    ) -> None:
        candidates = (keys,) if isinstance(keys, str) else keys
        present = [key for key in candidates if key in machine]
        if not present:
            raise DockerSandboxError(f"VirtualBox VM 信息缺少{label}状态")
        if any(machine[key].lower() not in _OFF_VALUES for key in present):
            raise DockerSandboxError(f"VirtualBox Docker host 必须关闭{label}")


def parse_machine_info(payload: bytes | str) -> dict[str, str]:
    """解析 VBoxManage --machinereadable 的 key=value 输出。"""
    text = (
        payload.decode("utf-8", errors="replace")
        if isinstance(payload, bytes)
        else payload
    )
    parsed: dict[str, str] = {}
    for raw_line in text.splitlines():
        if "=" not in raw_line:
            continue
        key, raw_value = raw_line.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key:
            continue
        if key == "VMStateFile":
            # VBox 的保存状态路径使用原始反斜杠，其格式与其他 JSON 引号字段不同。
            match = re.fullmatch(r'"([^"\x00-\x1f]*)"', raw_value)
            if match is None:
                raise DockerSandboxError("VirtualBox 保存状态路径格式非法")
            value = match.group(1)
        elif raw_value.startswith('"'):
            try:
                value, end = json.JSONDecoder().raw_decode(raw_value)
            except json.JSONDecodeError as exc:
                raise DockerSandboxError("VirtualBox VM 信息格式非法") from exc
            if not isinstance(value, str):
                raise DockerSandboxError("VirtualBox VM 信息值必须是字符串")
            suffix = raw_value[end:].strip()
            if suffix:
                if key != "VideoMode" or not re.fullmatch(
                    r"@-?[0-9]+,-?[0-9]+ [0-9]+",
                    suffix,
                ):
                    raise DockerSandboxError("VirtualBox VM 信息格式非法")
                value = f"{value}{suffix}"
        else:
            value = raw_value
        parsed[key] = value
    if not parsed:
        raise DockerSandboxError("VirtualBox VM 信息为空")
    return parsed


def create_docker_engine_host(
    settings: DockerHostSettings,
    *,
    docker_runner: DockerCommandRunner,
) -> DockerEngineHost:
    if settings.engine_host == "local":
        return LocalDockerEngineHost()
    return VirtualBoxDockerEngineHost(settings, docker_runner=docker_runner)
