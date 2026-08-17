from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from agent.plugins import channel_generation_host
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus


ROOT = Path(__file__).parents[1]


class FakeProviderClient:
    def __init__(self) -> None:
        self.closed = 0

    def credential(self, ref) -> str:
        if ref.path == ("appId",):
            return "formal-app-id"
        if ref.path == ("appSecret",):
            return "formal-app-secret"
        raise KeyError(ref.path)

    async def aclose(self) -> None:
        self.closed += 1


class FakeProviderFactory:
    def __init__(self) -> None:
        self.client = FakeProviderClient()
        self.create_calls = 0
        self.close_calls = 0

    async def create(self, credentials):
        self.create_calls += 1
        return self.client

    async def aclose(self) -> None:
        self.close_calls += 1


def _stage(tmp_path: Path) -> tuple[Path, Path]:
    plugin_root = tmp_path / "plugins" / "feishu"
    plugin_root.mkdir(parents=True)
    for filename in (
        "plugin.py",
        "channel.py",
        "config.py",
        "cards.py",
        "akashic.plugin.toml",
        "requirements.txt",
    ):
        shutil.copy2(ROOT / filename, plugin_root / filename)
    # Core's static-manifest admission requires the install-owned runtime
    # marker.  The test keeps the dependency install out of the manager gate
    # and points that marker at this already prepared test interpreter.
    runtime_python = plugin_root / ".venv" / "bin" / "python"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.symlink_to(sys.executable)
    workspace = tmp_path / "workspace"
    data_dir = workspace / "plugin-data" / "feishu-builtin"
    data_dir.mkdir(parents=True)
    (data_dir / "config.local.toml").write_text(
        'appId = "formal-app-id"\nappSecret = "formal-app-secret"\n',
        encoding="utf-8",
    )
    return plugin_root, workspace


@pytest.mark.asyncio
async def test_manager_formal_candidate_discard_promote_and_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the real Manager/Host path with a fake provider and no network."""

    plugin_root, workspace = _stage(tmp_path)
    factory = FakeProviderFactory()
    original_resolver = channel_generation_host._resolve_sync_factory

    def resolve_factory(module, export):
        factory_callable = original_resolver(module, export)

        def wrapped(context):
            adapter = cast(Any, factory_callable(context))
            # Keep the real adapter.start/stop and replace only the provider socket loop.
            adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
            return adapter

        return wrapped

    monkeypatch.setattr(
        channel_generation_host,
        "_resolve_sync_factory",
        resolve_factory,
    )
    manager = PluginManager(
        plugin_dirs=[plugin_root.parent],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=workspace,
        installed_cache_root=tmp_path / "home" / "cache",
    )
    manager.bind_channel_provider_factory_resolver(lambda snapshot: {"feishu": factory})

    await manager.load_all()
    stable = manager.current_snapshot
    runtime = manager.active_channel_generation
    assert stable is not None and stable.state == "committed"
    assert runtime is not None and runtime.channel("feishu").admission_open
    assert factory.create_calls == 1
    assert factory.client.closed == 0

    candidate = await manager.prepare_candidate("feishu")
    assert candidate is not None and candidate.runtime_snapshot is not None
    assert manager.current_snapshot is stable
    assert factory.create_calls == 1  # candidate never calls the formal factory
    assert candidate.validation_workspace is not None
    validation_root = candidate.validation_workspace.parent
    for path in validation_root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            assert b"formal-app-secret" not in path.read_bytes()
    config_path = workspace / "plugin-data" / "feishu-builtin" / "config.local.toml"
    assert config_path.read_text(encoding="utf-8") == (
        'appId = "formal-app-id"\nappSecret = "formal-app-secret"\n'
    )
    await manager.discard_prepared("feishu")
    assert manager.current_snapshot is stable
    assert factory.create_calls == 1

    candidate = await manager.prepare_candidate("feishu")
    assert candidate is not None
    publication = await manager.publish_prepared("feishu")
    assert publication["publication_state"] == "committed"
    assert manager.current_snapshot is not stable
    assert factory.create_calls == 2
    assert manager.active_channel_generation is not None
    assert manager.active_channel_generation.channel("feishu").admission_open

    await manager.terminate_all()
    assert manager.active_channel_generation is None
    assert factory.close_calls == 2
    assert factory.client.closed == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config_text", "missing_credential"),
    (
        ('appSecret = "formal-app-secret"\n', "app_id"),
        ('appId = "formal-app-id"\n', "app_secret"),
    ),
)
async def test_formal_start_rejects_missing_credential_before_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    config_text: str,
    missing_credential: str,
) -> None:
    """Reject an incomplete formal credential pair before any channel resource starts."""

    plugin_root, workspace = _stage(tmp_path)
    config_path = workspace / "plugin-data" / "feishu-builtin" / "config.local.toml"
    config_path.write_text(config_text, encoding="utf-8")
    factory = FakeProviderFactory()
    adapters: list[Any] = []
    original_resolver = channel_generation_host._resolve_sync_factory

    def resolve_factory(module, export):
        factory_callable = original_resolver(module, export)

        def wrapped(context):
            adapter = cast(Any, factory_callable(context))
            adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
            adapters.append(adapter)
            return adapter

        return wrapped

    monkeypatch.setattr(
        channel_generation_host,
        "_resolve_sync_factory",
        resolve_factory,
    )
    manager = PluginManager(
        plugin_dirs=[plugin_root.parent],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=workspace,
        installed_cache_root=tmp_path / "home" / "cache",
    )
    manager.bind_channel_provider_factory_resolver(lambda snapshot: {"feishu": factory})

    with pytest.raises(RuntimeError, match=missing_credential):
        await manager.load_all()

    assert len(adapters) == 1
    adapter = adapters[0]
    assert factory.create_calls == 1
    assert factory.client.closed
    assert adapter._provider_client is None
    assert adapter._client is None
    assert adapter._stream_subscription is None
    assert adapter._ws_client is None
    assert adapter._ws_loop is None
    assert adapter._ws_thread is None
    assert not adapter._ws_thread_started
    assert not adapter._started
    assert manager.active_channel_generation is None
