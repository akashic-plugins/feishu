from __future__ import annotations

import asyncio
import importlib.util
import logging
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.tools.message_push import MessagePushTool
from bus.events import (
    AttachmentKind,
    ChannelAttachment,
    ChannelMessage,
    DeliveryStatus,
)


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
SdkShutdownLogFilter = sys.modules[FeishuPlugin.__module__.removesuffix(".plugin") + ".channel"]._SdkShutdownLogFilter


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


def test_sdk_shutdown_filter_only_hides_errors_after_stop() -> None:
    stopped = threading.Event()
    log_filter = SdkShutdownLogFilter(stopped)
    record = logging.LogRecord(
        "Lark",
        logging.ERROR,
        __file__,
        1,
        "receive message loop exit, err: closed",
        (),
        None,
    )

    assert log_filter.filter(record)
    stopped.set()
    assert not log_filter.filter(record)


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
        subscribe_outbound=lambda *_args: object(),
    )
    push_tools = [MessagePushTool(), MessagePushTool()]
    context = SimpleNamespace(
        bus=registry,
        event_bus=registry,
        push_tool=push_tools[0],
        interrupt_controller=None,
        attachment_store=None,
        session_manager=None,
    )

    await channel.start(context)
    await channel.stop()
    context.push_tool = push_tools[1]
    await channel.start(context)
    await channel.stop()

    assert starts == 2
    assert channel._ws_thread is None
    assert all("feishu" in tool._adapters for tool in push_tools)


@pytest.mark.asyncio
async def test_delivery_adapter_submits_complete_message() -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": FeishuConfigModel(app_id="app", app_secret="secret")},
    )()
    channel = plugin.channels()[0]
    calls: list[tuple[object, ...]] = []

    async def send_text(chat_id: str, content: str) -> None:
        calls.append(("text", chat_id, content))

    async def send_file(
        chat_id: str,
        path: str,
        name: str | None = None,
        caption: str | None = None,
    ) -> None:
        calls.append(("file", chat_id, path, name, caption))

    async def send_image(chat_id: str, path: str) -> None:
        calls.append(("image", chat_id, path))

    channel.send = send_text
    channel.send_file = send_file
    channel.send_image = send_image
    receipt = await channel._deliver_message(
        ChannelMessage(
            channel="feishu",
            chat_id="ou_1",
            content="正文",
            attachments=(
                ChannelAttachment(AttachmentKind.FILE, "/tmp/a.txt", "a.txt"),
                ChannelAttachment(AttachmentKind.IMAGE, "/tmp/a.png"),
            ),
        )
    )

    assert receipt.status is DeliveryStatus.SUCCESS
    assert calls == [
        ("text", "ou_1", "正文"),
        ("file", "ou_1", "/tmp/a.txt", "a.txt", None),
        ("image", "ou_1", "/tmp/a.png"),
    ]


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
