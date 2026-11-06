"""Competition CLI ownership checks with real OS locks and fake service owners."""

import asyncio
from contextlib import asynccontextmanager
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
        assert cli.ENTRY_VERSION == "ARU-RECOVERY-2"
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


def test_two_lanes_without_shared_model_reject_before_lock_or_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cli._DataRootLease, "acquire", lambda _: pytest.fail("lock touched"))
    monkeypatch.setattr(cli, "_api_settings", lambda _: pytest.fail("config touched"))
    with pytest.raises(ValueError, match="require shared embedding"):
        cli.main(_args(tmp_path / "unused", tmp_path / "unused.json") + ["--runtime-lanes", "2"])


def _fake_service(monkeypatch, root, events, *, startup_failure=False,
                  cleanup_failure=False, enter_lifespan=False):
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

        @asynccontextmanager
        async def app_lifespan(application):
            events.append("admission_started")
            try:
                yield
            finally:
                events.append("admission_drained")

        return SimpleNamespace(router=SimpleNamespace(lifespan_context=app_lifespan))

    class Server:
        def __init__(self, config):
            self.app = config.app

        async def serve(self):
            if enter_lifespan:
                async with self.app.router.lifespan_context(self.app):
                    events.append("serve_with_lifespan")
            else:
                events.append("serve_without_lifespan")

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(
        Config=lambda app, **kwargs: SimpleNamespace(app=app), Server=Server))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(AsyncOpenAI=Client))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_api", SimpleNamespace(
        create_app=create_app))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_runtime", SimpleNamespace(
        ChallengeRuntime=Runtime))
    monkeypatch.setattr(cli, "_token", lambda *args, **kwargs: "test-only-" + "x" * 32)
    return control


def test_cli_preserves_admission_lifespan_before_releasing_runtime_and_lease(
        tmp_path, api_config, monkeypatch):
    root, events = tmp_path / "lifespan", []
    _fake_service(monkeypatch, root, events, enter_lifespan=True)
    assert cli.main(_args(root, api_config)) == 0
    assert events.index("admission_started") < events.index("serve_with_lifespan")
    assert events.index("serve_with_lifespan") < events.index("admission_drained")
    assert events.index("admission_drained") < events.index("runtime_close")
    assert events.index("runtime_close") < events.index("client_closed")
    assert _probe(root) == 0


def test_two_lane_cli_explicitly_selects_two_generations_and_closes_in_order(
        tmp_path, api_config, monkeypatch):
    root, events = tmp_path / "two-lanes", []
    _fake_service(monkeypatch, root, events, enter_lifespan=True)

    class Shared:
        def __init__(self, *, max_generations):
            assert max_generations == 2
            events.append("shared_created")

        async def start(self, deadline):
            assert type(deadline) is int
            events.append("shared_started")

        async def close(self):
            assert _probe(root) == 9
            events.append("shared_closed")

    class Lanes:
        def __init__(self, *args, **kwargs):
            assert isinstance(kwargs["embedding_owner"], Shared)
            events.append("lanes_created")

        async def close(self):
            assert _probe(root) == 9
            events.append("lanes_closed")

    monkeypatch.setitem(sys.modules, "system_memory_god.storage.shared_embedding_owner",
                        SimpleNamespace(SharedEmbeddingOwner=Shared))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_lanes",
                        SimpleNamespace(ChallengeLanes=Lanes))
    assert cli.main(_args(root, api_config) + ["--shared-embedding", "--runtime-lanes", "2"]) == 0
    assert events.index("shared_started") < events.index("lanes_created")
    assert events.index("admission_drained") < events.index("lanes_closed")
    assert events.index("lanes_closed") < events.index("shared_closed") < events.index("client_closed")
    assert _probe(root) == 0


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
    assert ("client_closed" in events) is (failed_owner == "runtime")
    try:
        control["cleanup_failure"] = False
        asyncio.run(owner.close())
    finally:
        if owner in cli._ACTIVE_OWNERS:
            owner.lease.close()
            cli._ACTIVE_OWNERS.remove(owner)
    assert _probe(root) == 0


@pytest.mark.parametrize("startup_failure", [False, True])
def test_shared_model_is_owned_before_start_and_closed_after_runtime(
        tmp_path, api_config, monkeypatch, startup_failure):
    root, events = tmp_path / "shared-owner", []
    _fake_service(monkeypatch, root, events)

    class Embedding:
        def __init__(self):
            events.append("embedding_created")

        async def start(self, deadline):
            assert type(deadline) is int and _probe(root) == 9
            events.append("embedding_start")
            if startup_failure:
                raise RuntimeError("fake embedding startup failure")

        async def close(self):
            assert _probe(root) == 9
            events.append("embedding_closed")

    monkeypatch.setitem(sys.modules, "system_memory_god.storage.shared_embedding_owner",
                        SimpleNamespace(SharedEmbeddingOwner=Embedding))
    if startup_failure:
        with pytest.raises(RuntimeError, match="fake embedding startup failure"):
            cli.main(_args(root, api_config) + ["--shared-embedding"])
        assert "runtime_created" not in events
    else:
        assert cli.main(_args(root, api_config) + ["--shared-embedding"]) == 0
        assert events.index("runtime_close") < events.index("embedding_closed")
    assert events.index("embedding_closed") < events.index("client_closed")
    assert _probe(root) == 0


def test_failed_embedding_close_retains_owner_and_lease_but_attempts_client(tmp_path, api_config, monkeypatch):
    root, events = tmp_path / "failed-embedding-close", []
    _fake_service(monkeypatch, root, events)
    failed = [True]

    class Embedding:
        async def start(self, deadline):
            pass

        async def close(self):
            if failed[0]:
                raise RuntimeError("fake embedding cleanup failure")

    monkeypatch.setitem(sys.modules, "system_memory_god.storage.shared_embedding_owner",
                        SimpleNamespace(SharedEmbeddingOwner=Embedding))
    with pytest.raises(RuntimeError, match="fake embedding cleanup failure"):
        cli.main(_args(root, api_config) + ["--shared-embedding"])
    owner = next(item for item in cli._ACTIVE_OWNERS if item.lease.root == root)
    assert owner.runtime is None and owner.embedding_owner is not None and owner.client is None
    assert "client_closed" in events
    assert _probe(root) == 9
    failed[0] = False
    asyncio.run(owner.close())
    assert _probe(root) == 0
