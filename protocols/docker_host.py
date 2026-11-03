"""浏览器和文件/终端共用的 Docker engine host 配置契约。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class VirtualBoxHostSettings:
    cli: str
    machine_name: str
    host_only_network: str
    start_timeout_seconds: float


class DockerHostSettings(Protocol):
    docker_cli: str
    docker_context: str
    expected_docker_endpoint: str
    engine_host: str
    virtualbox: VirtualBoxHostSettings | None
