"""Run only the approved competition adapter on a local loopback port."""

from __future__ import annotations

import argparse
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
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTRY_VERSION = "AMC-ENTRY-1.0"
_MARKER = ".amc-entry-v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independent Aoao Memory competition API")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--api-config", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8795)
    parser.add_argument("--add-timeout", type=float, default=1700.0)
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
        if not root.is_dir() or (any(root.iterdir()) and (
                marker.is_symlink() or not marker.is_file()
                or marker.read_text(encoding="ascii") != ENTRY_VERSION + "\n")):
            raise ValueError("fresh or owned competition directory required")
    return root


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
    root.mkdir(parents=True, exist_ok=True)
    marker = root / _MARKER
    if not marker.exists():
        with marker.open("x", encoding="ascii", newline="\n") as stream:
            stream.write(ENTRY_VERSION + "\n")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _validate_root(args.data_root)
    credential_file = _private_path(args.token_file)
    if not credential_file.is_relative_to(root) or credential_file == root:
        raise ValueError("credential must belong to the private competition root")
    if (not 1024 <= args.port <= 65535 or not math.isfinite(args.add_timeout)
            or not 0 < args.add_timeout <= 1700):
        raise ValueError("invalid server limits")
    api = _api_settings(args.api_config)
    if args.check_config:
        _token(credential_file, create=False)
        print("AMC-ENTRY-1.0 configuration valid; no model or service started.")
        return 0
    _prepare_root(root)
    credential_file.parent.mkdir(parents=True, exist_ok=True)
    bearer = _token(credential_file, create=True)
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    for path in (PROJECT_ROOT, PROJECT_ROOT / "apps"):
        sys.path.insert(0, str(path))

    import uvicorn
    from openai import AsyncOpenAI
    from system_memory_god.challenge_api import create_app
    from system_memory_god.challenge_runtime import ChallengeRuntime

    client = AsyncOpenAI(api_key=api["api_key"], base_url=api["base_url"], max_retries=0,
                         timeout=60.0)
    runtime = ChallengeRuntime(root, client=client, model=api["model_name"],
                               add_timeout=args.add_timeout)
    app = create_app(runtime, token=bearer)
    runtime_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        try:
            async with runtime_lifespan(application):
                yield
        finally:
            await client.close()

    app.router.lifespan_context = lifespan
    uvicorn.run(app, host="127.0.0.1", port=args.port, workers=1,
                access_log=False, log_level="warning", timeout_keep_alive=5)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError):
        print("Challenge service could not start; configuration or owned storage unavailable.",
              file=sys.stderr)
        raise SystemExit(2) from None
