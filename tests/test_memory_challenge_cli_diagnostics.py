"""Private diagnostics belong to the CLI's real resource lifetime."""

import asyncio
from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


CLI_PATH = Path(__file__).resolve().parents[1] / "Tools" / "serve_memory_challenge.py"
SPEC = importlib.util.spec_from_file_location("challenge_cli_diagnostics", CLI_PATH)
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class Lease:
    def __init__(self, root, events):
        self.root, self.events = root, events
        self.descriptor = 1

    def close(self):
        self.events.append("lease_closed")
        self.descriptor = None


def test_diagnostic_argument_is_explicit_and_check_config_does_not_create_it(tmp_path, monkeypatch):
    root = tmp_path / "state"
    path = root / "diagnostics" / "service.jsonl"
    monkeypatch.setattr(cli, "_api_settings", lambda _: {})
    monkeypatch.setattr(cli, "_token", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli._DataRootLease, "acquire", lambda _: pytest.fail("lock acquired"))
    assert cli.main(["--data-root", str(root), "--token-file", str(root / "test.key"),
                     "--api-config", str(tmp_path / "config.json"),
                     "--diagnostics-file", str(path), "--check-config"]) == 0
    assert not root.exists()


def test_diagnostic_cannot_target_credentials_or_an_unowned_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_api_settings", lambda _: pytest.fail("config read"))
    root = tmp_path / "state"
    for path in (root / "credentials" / "service.jsonl", tmp_path / "outside.jsonl"):
        with pytest.raises(ValueError, match="diagnostic"):
            cli.main(["--data-root", str(root), "--token-file", str(root / "test.key"),
                      "--api-config", str(tmp_path / "config.json"),
                      "--diagnostics-file", str(path), "--check-config"])


def test_failed_resource_cleanup_keeps_diagnostic_and_root_lease_owned(tmp_path):
    events = []
    owner = cli._StartupOwner(Lease(tmp_path, events))

    class Runtime:
        async def close(self):
            raise RuntimeError("synthetic cleanup failure")

    class Sink:
        async def close(self):
            events.append("diagnostic_closed")

    owner.runtime, owner.diagnostics = Runtime(), Sink()
    with pytest.raises(RuntimeError):
        asyncio.run(owner.close())
    assert not events
    assert owner.lease.descriptor is not None
    assert owner in cli._ACTIVE_OWNERS
    owner.runtime = None
    asyncio.run(owner.close())
    assert events == ["diagnostic_closed", "lease_closed"]


def test_sink_writer_close_failure_retains_the_root_lease(tmp_path):
    owner = cli._StartupOwner(Lease(tmp_path, []))

    class Sink:
        async def close(self):
            raise RuntimeError("synthetic sink close failure")

    owner.diagnostics = Sink()
    with pytest.raises(RuntimeError):
        asyncio.run(owner.close())
    assert owner.lease.descriptor is not None
    assert owner in cli._ACTIVE_OWNERS
    owner.diagnostics = None
    owner.release_if_empty()


def test_diagnostic_starts_before_runtime_and_closes_after_it(tmp_path, monkeypatch):
    events = []

    class Sink:
        def __init__(self, path):
            assert path == tmp_path / "diagnostics" / "service.jsonl"

        async def start(self):
            events.append("diagnostic_started")

        async def close(self):
            events.append("diagnostic_closed")

    class Client:
        def __init__(self, **kwargs):
            events.append("client_created")

        async def close(self):
            events.append("client_closed")

    class Runtime:
        def __init__(self, *args, **kwargs):
            events.append("runtime_created")

        async def close(self):
            events.append("runtime_closed")

    @asynccontextmanager
    async def lifespan(_):
        yield

    class Server:
        def __init__(self, config):
            pass

        async def serve(self):
            events.append("served")

    monkeypatch.setitem(sys.modules, "kernel.challenge_diagnostics", SimpleNamespace(DiagnosticSink=Sink))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(AsyncOpenAI=Client))
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(
        Config=lambda *args, **kwargs: None, Server=Server))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_runtime", SimpleNamespace(ChallengeRuntime=Runtime))
    monkeypatch.setitem(sys.modules, "system_memory_god.challenge_api", SimpleNamespace(
        create_app=lambda *args, **kwargs: SimpleNamespace(router=SimpleNamespace(lifespan_context=lifespan))))
    owner = cli._StartupOwner(Lease(tmp_path, events))
    args = SimpleNamespace(shared_embedding=False, runtime_lanes=1, add_timeout=1700, port=8795,
                           diagnostics_file=tmp_path / "diagnostics" / "service.jsonl")
    asyncio.run(cli._serve(owner, args, {"api_key": "synthetic", "base_url": "https://example.invalid",
                                       "model_name": "synthetic"}, "synthetic"))
    assert events.index("diagnostic_started") < events.index("runtime_created")
    assert events.index("runtime_closed") < events.index("diagnostic_closed") < events.index("lease_closed")
