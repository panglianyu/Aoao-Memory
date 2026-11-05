"""Run only the approved competition adapter on a local loopback port."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import csv
import io
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import time
from urllib.parse import urlsplit
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from kernel.challenge_diagnostics import DiagnosticSink
    from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTRY_VERSION = "SNAP-CAP-1"
_STORAGE_MARKER_VERSION = "AMC-ENTRY-1.0"
_MARKER = ".amc-entry-v1"
_LEASE_FILE = ".amc-entry.lock"
_ACTIVE_OWNERS: set[_StartupOwner] = set()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independent Aoao Memory competition API")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--api-config", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8795)
    parser.add_argument("--add-timeout", type=float, default=1700.0)
    parser.add_argument("--shared-embedding", action="store_true")
    parser.add_argument("--runtime-lanes", type=int, choices=(1, 2), default=1)
    parser.add_argument("--diagnostics-file", type=Path)
    parser.add_argument("--check-config", action="store_true")
    return parser


def _private_path(path: Path) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("explicit ordinary absolute path required")
    absolute = Path(os.path.abspath(path))
    if absolute.resolve() != absolute:
        raise ValueError("linked path denied")
    return absolute


def _validate_root(root: Path) -> Path:
    root = _private_path(root)
    if root.is_relative_to(PROJECT_ROOT) or PROJECT_ROOT.is_relative_to(root):
        raise ValueError("independent competition directory required")
    if root.exists():
        marker = root / _MARKER
        if not root.is_dir():
            raise ValueError("ordinary competition directory required")
        entries = {entry.name for entry in root.iterdir()}
        lease_only = entries == {_LEASE_FILE} and _ordinary_file(root / _LEASE_FILE)
        if entries and not lease_only and (
                not _ordinary_file(marker)
                or marker.read_text(encoding="ascii") != _STORAGE_MARKER_VERSION + "\n"):
            raise ValueError("fresh or owned competition directory required")
    return root


def _ordinary_file(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    return (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
            and not getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)


class _DataRootLease:
    """Hold one non-inheritable Windows file descriptor for this data root."""

    def __init__(self, root: Path, descriptor: int):
        self.root = root
        self.descriptor: int | None = descriptor

    @classmethod
    def acquire(cls, root: Path) -> _DataRootLease:
        if os.name != "nt":
            raise OSError("Windows data-root lease required")
        import msvcrt

        root = _validate_root(root)
        root.mkdir(parents=True, exist_ok=True)
        root = _validate_root(root)
        path = _private_path(root / _LEASE_FILE)
        if path.exists() and not _ordinary_file(path):
            raise ValueError("ordinary lease file required")
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.set_inheritable(descriptor, False)
            opened, named = os.fstat(descriptor), path.lstat()
            if (not _ordinary_file(path) or opened.st_nlink != 1
                    or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
                raise ValueError("stable ordinary lease file required")
            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        except BaseException:
            os.close(descriptor)
            raise
        return cls(root, descriptor)

    def close(self) -> None:
        if self.descriptor is None:
            return
        import msvcrt

        os.lseek(self.descriptor, 0, os.SEEK_SET)
        msvcrt.locking(self.descriptor, msvcrt.LK_UNLCK, 1)
        os.close(self.descriptor)
        self.descriptor = None


class _StartupOwner:
    """Keep failed cleanup owners alive so their lease cannot be garbage-collected."""

    def __init__(self, lease: _DataRootLease):
        self.lease, self.runtime, self.client = lease, None, None
        self.diagnostics: DiagnosticSink | None = None
        self.embedding_owner: SharedEmbeddingOwner | None = None
        _ACTIVE_OWNERS.add(self)

    def release_if_empty(self) -> None:
        if (self.runtime is None and self.embedding_owner is None and self.client is None
                and self.diagnostics is None):
            self.lease.close()
            _ACTIVE_OWNERS.discard(self)

    async def close(self) -> None:
        errors: list[BaseException] = []
        if self.runtime is not None:
            try:
                await self.runtime.close()
            except BaseException as error:
                errors.append(error)
            else:
                self.runtime = None
        if self.embedding_owner is not None:
            try:
                await self.embedding_owner.close()
            except BaseException as error:
                errors.append(error)
            else:
                self.embedding_owner = None
        if self.client is not None:
            try:
                await self.client.close()
            except BaseException as error:
                errors.append(error)
            else:
                self.client = None
        if (self.diagnostics is not None and self.runtime is None
                and self.embedding_owner is None and self.client is None):
            try:
                await self.diagnostics.close()
            except BaseException as error:
                errors.append(error)
            else:
                self.diagnostics = None
        self.release_if_empty()
        if errors:
            raise errors[0]


def _api_settings(path: Path) -> dict[str, str]:
    path = _private_path(path)
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("bounded explicit API configuration required")
    content = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        value = json.loads(content)
    else:
        import yaml

        try:
            value = yaml.safe_load(content)
        except yaml.YAMLError:
            raise ValueError("invalid API configuration") from None
    api = value.get("openai") if type(value) is dict else None
    if type(api) is not dict or any(type(api.get(key)) is not str or not api[key].strip()
                                    for key in ("base_url", "api_key", "model_name")):
        raise ValueError("explicit existing provider and model required")
    address = urlsplit(api["base_url"])
    if (address.scheme not in {"http", "https"} or not address.hostname or address.username
            or address.password or address.query or address.fragment):
        raise ValueError("invalid provider address")
    return {key: api[key] for key in ("base_url", "api_key", "model_name")}


def _protect_new_credential_directory(path: Path) -> None:
    if os.name != "nt":
        return
    result = subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"],
                            check=True, capture_output=True, text=True, timeout=10,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    rows = list(csv.reader(io.StringIO(result.stdout.strip())))
    sid = rows[0][-1] if len(rows) == 1 and rows[0] else ""
    if not re.fullmatch(r"S-\d+(?:-\d+)+", sid):
        raise ValueError("credential owner unavailable")
    subprocess.run(["icacls", str(path), "/inheritance:r", "/grant:r", f"*{sid}:(OI)(CI)F"],
                   check=True, capture_output=True, timeout=10,
                   creationflags=subprocess.CREATE_NO_WINDOW)


def _token(path: Path, *, create: bool) -> str | None:
    path = _private_path(path)
    if not path.exists() and not create:
        return None
    if not path.exists():
        # Restrict inherited access before the credential's first byte exists.
        _protect_new_credential_directory(path.parent)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii", newline="\n") as stream:
            stream.write(secrets.token_urlsafe(32) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 4097:
        raise ValueError("ordinary private credential file required")
    value = path.read_text(encoding="ascii").rstrip("\r\n")
    if (not 32 <= len(value) <= 4096 or any(ord(char) < 33 or ord(char) > 126 for char in value)):
        raise ValueError("invalid dedicated credential")
    return value


def _prepare_root(root: Path) -> None:
    # Acquiring the lease creates a file, but does not accept foreign data as owned.
    root = _validate_root(root)
    marker = root / _MARKER
    if not marker.exists():
        with marker.open("x", encoding="ascii", newline="\n") as stream:
            stream.write(_STORAGE_MARKER_VERSION + "\n")


async def _serve(owner: _StartupOwner, args, api: dict[str, str], bearer: str) -> None:
    try:
        diagnostic_path = getattr(args, "diagnostics_file", None)
        if diagnostic_path is not None:
            from kernel.challenge_diagnostics import DiagnosticSink

            owner.diagnostics = DiagnosticSink(diagnostic_path)
            await owner.diagnostics.start()
        import uvicorn
        from openai import AsyncOpenAI
        from system_memory_god.challenge_api import create_app
        from system_memory_god.challenge_runtime import ChallengeRuntime

        owner.client = AsyncOpenAI(api_key=api["api_key"], base_url=api["base_url"], max_retries=0,
                                   timeout=60.0)
        runtime_options: dict[str, Any] = {}
        if args.shared_embedding:
            from system_memory_god.storage.shared_embedding_owner import SharedEmbeddingOwner

            owner.embedding_owner = (SharedEmbeddingOwner(max_generations=2)
                                     if args.runtime_lanes == 2 else SharedEmbeddingOwner())
            await owner.embedding_owner.start(time.monotonic_ns() + 120_000_000_000)
            runtime_options["embedding_owner"] = owner.embedding_owner
        runtime_type: Any = ChallengeRuntime
        if args.runtime_lanes == 2:
            from system_memory_god.challenge_lanes import ChallengeLanes

            runtime_type = ChallengeLanes
        owner.runtime = runtime_type(owner.lease.root, client=owner.client,
                                         model=api["model_name"], add_timeout=args.add_timeout,
                                         **runtime_options)
        app = create_app(owner.runtime, token=bearer)
        runtime_lifespan = app.router.lifespan_context

        @asynccontextmanager
        async def lifespan(application):
            # Admission stops and drains before the outer owner releases the
            # runtime, client and data-root lease. Startup failures still use
            # the outer finally even if this lifespan never begins.
            async with runtime_lifespan(application):
                yield

        app.router.lifespan_context = lifespan
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port, workers=1,
            access_log=False, log_level="warning", timeout_keep_alive=5))
        await server.serve()
    finally:
        await owner.close()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.runtime_lanes == 2 and not args.shared_embedding:
        raise ValueError("two runtime lanes require shared embedding")
    root = _validate_root(args.data_root)
    if args.diagnostics_file is not None:
        diagnostic_file = _private_path(args.diagnostics_file)
        if (diagnostic_file.parent != root / "diagnostics"
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,80}\.jsonl", diagnostic_file.name)):
            raise ValueError("diagnostic must be a JSONL file in the owned diagnostics directory")
        args.diagnostics_file = diagnostic_file
    credential_file = _private_path(args.token_file)
    if not credential_file.is_relative_to(root) or credential_file == root:
        raise ValueError("credential must belong to the private competition root")
    if (not 1024 <= args.port <= 65535 or not math.isfinite(args.add_timeout)
            or not 0 < args.add_timeout <= 1700):
        raise ValueError("invalid server limits")
    if args.check_config:
        _api_settings(args.api_config)
        _token(credential_file, create=False)
        print(f"{ENTRY_VERSION} configuration valid; no model or service started.")
        return 0
    owner = _StartupOwner(_DataRootLease.acquire(root))
    try:
        _prepare_root(root)
        api = _api_settings(args.api_config)
        credential_file.parent.mkdir(parents=True, exist_ok=True)
        bearer = _token(credential_file, create=True)
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        for path in (PROJECT_ROOT, PROJECT_ROOT / "apps"):
            sys.path.insert(0, str(path))
        asyncio.run(_serve(owner, args, api, cast(str, bearer)))
    finally:
        # Before async resources exist this also handles configuration/import failure.
        # Failed runtime/client closes retain both their owner and the same lease FD.
        owner.release_if_empty()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError):
        print("Challenge service could not start; configuration or owned storage unavailable.",
              file=sys.stderr)
        raise SystemExit(2) from None
