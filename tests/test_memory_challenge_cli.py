"""Competition CLI ownership checks with real OS locks and fake service owners."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


CLI_PATH = Path(__file__).resolve().parents[1] / "Tools" / "serve_memory_challenge.py"
SPEC = importlib.util.spec_from_file_location("challenge_cli_v11", CLI_PATH)
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)

LOCK_CHILD = """
import importlib.util, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('lease_cli', sys.argv[1])
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)
try:
    lease = cli._DataRootLease.acquire(Path(sys.argv[2]))
except OSError:
    sys.exit(9)
print('acquired', flush=True)
if len(sys.argv) > 3:
    sys.stdin.readline()
lease.close()
"""


def _probe(root):
    return subprocess.run([sys.executable, "-c", LOCK_CHILD, str(CLI_PATH), str(root)],
                          capture_output=True, text=True, timeout=10).returncode


def _args(root, config):
    return ["--data-root", str(root), "--token-file", str(root / "credentials" / "test.key"),
            "--api-config", str(config)]


@pytest.fixture
def api_config(tmp_path):
    path = tmp_path / "provider.json"
    path.write_text(json.dumps({"openai": {"base_url": "https://example.invalid/v1",
        "api_key": "test-only-placeholder", "model_name": "test-only-model"}}), encoding="utf-8")
    return path


@pytest.mark.skipif(os.name != "nt", reason="Windows deployment lock contract")
def test_second_process_rejected_before_configuration_and_can_acquire_after_release(
        tmp_path, api_config, monkeypatch):
    root = tmp_path / "state"
    child = subprocess.Popen([sys.executable, "-c", LOCK_CHILD, str(CLI_PATH), str(root), "hold"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "acquired"
        monkeypatch.setattr(cli, "_api_settings", lambda _: pytest.fail("configuration touched"))
        with pytest.raises(OSError):
            cli.main(_args(root, api_config))
    finally:
        child.communicate("release\n", timeout=10)
    assert child.returncode == 0
    assert _probe(root) == 0


def test_lease_only_interrupted_root_recovers_and_keeps_v1_storage_marker(tmp_path):
    root = tmp_path / "interrupted"
    root.mkdir()
    (root / cli._LEASE_FILE).touch()
    assert cli._validate_root(root) == root
    lease = cli._DataRootLease.acquire(root)
    try:
        assert not os.get_inheritable(lease.descriptor)
        cli._prepare_root(root)
        assert (root / cli._MARKER).read_bytes() == b"AMC-ENTRY-1.0\n"
        assert cli.ENTRY_VERSION == "AMC-ENTRY-1.2"
    finally:
        lease.close()
    assert (root / cli._LEASE_FILE).is_file()
    assert _probe(root) == 0


def test_prepare_rechecks_root_after_lock_before_ownership_acceptance(tmp_path):
    root = tmp_path / "changed"
    lease = cli._DataRootLease.acquire(root)
    try:
        (root / "foreign-data").write_text("not challenge owned", encoding="ascii")
        with pytest.raises(ValueError):
            cli._prepare_root(root)
        assert not (root / cli._MARKER).exists()
    finally:
        lease.close()


def test_check_config_is_readonly_and_never_acquires_lock(tmp_path, api_config, monkeypatch):
    root = tmp_path / "untouched"
    monkeypatch.setattr(cli._DataRootLease, "acquire",
                        lambda _: pytest.fail("check-config attempted lock"))
    assert cli.main(_args(root, api_config) + ["--check-config"]) == 0
    assert not root.exists()


def _fake_service(monkeypatch, root, events, *, startup_failure=False, cleanup_failure=False):
    control = {"cleanup_failure": cleanup_failure}

    class Client:
        def __init__(self, **kwargs):
            events.append("client_created")

        async def close(self):
            assert _probe(root) == 9
            if control["cleanup_failure"] == "client":
                raise RuntimeError("fake close failure")
            events.append("client_closed")

    class Runtime:
        def __init__(self, *args, **kwargs):
            events.append("runtime_created")

        async def close(self):
            assert _probe(root) == 9
            events.append("runtime_close")
            if control["cleanup_failure"] == "runtime":
                raise RuntimeError("fake close failure")

    def create_app(*args, **kwargs):
        if startup_failure:
            raise RuntimeError("fake early startup failure")
        return SimpleNamespace(router=SimpleNamespace(lifespan_context=None))

    class Server:
        def __init__(self, config):
            pass

        async def serve(self):
            events.append("serve_without_lifespan")

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(
        Config=lambda *args, **kwargs: None, Server=Server))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(AsyncOpenAI=Client))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_api", SimpleNamespace(
        create_app=create_app))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_runtime", SimpleNamespace(
        ChallengeRuntime=Runtime))
    monkeypatch.setattr(cli, "_token", lambda *args, **kwargs: "test-only-" + "x" * 32)
    return control


@pytest.mark.parametrize("startup_failure", [False, True])
def test_service_cleanup_owns_lock_even_before_asgi_lifespan(
        tmp_path, api_config, monkeypatch, startup_failure):
    root, events = tmp_path / "owned", []
    _fake_service(monkeypatch, root, events, startup_failure=startup_failure)
    if startup_failure:
        with pytest.raises(RuntimeError, match="fake early startup failure"):
            cli.main(_args(root, api_config))
    else:
        assert cli.main(_args(root, api_config)) == 0
    assert events[-2:] == ["runtime_close", "client_closed"]
    assert _probe(root) == 0


@pytest.mark.parametrize("failed_owner", ["runtime", "client"])
def test_failed_cleanup_retains_owner_and_lock_until_explicit_successful_cleanup(
        tmp_path, api_config, monkeypatch, failed_owner):
    root, events = tmp_path / "failed-close", []
    control = _fake_service(monkeypatch, root, events, cleanup_failure=failed_owner)
    with pytest.raises(RuntimeError, match="fake close failure"):
        cli.main(_args(root, api_config))
    assert _probe(root) == 9
    owner = next(owner for owner in cli._ACTIVE_OWNERS if owner.lease.root == root)
    assert "client_closed" not in events
    try:
        control["cleanup_failure"] = False
        asyncio.run(owner.close())
    finally:
        if owner in cli._ACTIVE_OWNERS:
            owner.lease.close()
            cli._ACTIVE_OWNERS.remove(owner)
    assert _probe(root) == 0
