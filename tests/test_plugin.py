from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_plugin_module():
    path = Path(__file__).parents[1] / "plugin.py"
    spec = importlib.util.spec_from_file_location(
        "test_feishu_plugin",
        path,
        submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()
FeishuConfigModel = module.FeishuConfigModel
FeishuPlugin = module.FeishuPlugin


def test_feishu_plugin_without_config_returns_no_channels() -> None:
    plugin = FeishuPlugin()
    plugin.context = type("Ctx", (), {"config": None})()
    assert plugin.channels() == []


def test_feishu_plugin_with_config_returns_channel() -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {
            "config": FeishuConfigModel(
                app_id="app",
                app_secret="secret",
                allow_from=[],
                domain="https://open.feishu.cn",
            )
        },
    )()
    assert len(plugin.channels()) == 1


@pytest.mark.asyncio
async def test_inbound_future_is_cancelled_during_stop() -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": FeishuConfigModel(app_id="app", app_secret="secret")},
    )()
    channel = plugin.channels()[0]
    channel._loop = asyncio.get_running_loop()
    channel._ws_stopped.clear()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handle(_event: object) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    channel._handle_message_event = handle
    channel._on_sdk_message(object())
    await started.wait()
    channel._ws_stopped.set()
    await channel._drain_inbound_tasks()

    assert cancelled.is_set()
    assert channel._inbound_tasks == set()


@pytest.mark.asyncio
async def test_channel_can_start_stop_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": FeishuConfigModel(app_id="app", app_secret="secret")},
    )()
    channel = plugin.channels()[0]
    channel_module = sys.modules[type(channel).__module__]
    starts = 0

    class IdentityIndex:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def rebuild(self) -> int:
            return 0

    def run_ws_client() -> None:
        nonlocal starts
        starts += 1
        channel._ws_stopped.wait()

    monkeypatch.setattr(channel_module, "SessionIdentityIndex", IdentityIndex)
    channel._run_ws_client = run_ws_client
    registry = SimpleNamespace(
        on=lambda *_args: object(),
        register_channel=lambda *_args, **_kwargs: object(),
        subscribe_outbound=lambda *_args: object(),
    )
    context = SimpleNamespace(
        bus=registry,
        event_bus=registry,
        push_tool=registry,
        interrupt_controller=None,
        attachment_store=None,
        session_manager=None,
    )

    await channel.start(context)
    await channel.stop()
    await channel.start(context)
    await channel.stop()

    assert starts == 2
    assert channel._ws_thread is None


@pytest.mark.asyncio
async def test_disconnect_stops_sdk_event_loop() -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": FeishuConfigModel(app_id="app", app_secret="secret")},
    )()
    channel = plugin.channels()[0]
    ws_loop = asyncio.new_event_loop()
    disconnected = threading.Event()

    class _WsClient:
        async def _disconnect(self) -> None:
            disconnected.set()

    thread = threading.Thread(target=ws_loop.run_forever)
    thread.start()
    channel._ws_client = _WsClient()
    channel._ws_loop = ws_loop

    await channel._disconnect_ws()
    await asyncio.to_thread(thread.join, 2)
    ws_loop.close()

    assert disconnected.is_set()
    assert not thread.is_alive()
