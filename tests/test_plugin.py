from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.plugin_composition.channels import (
    ChannelDeliveryReceipt,
    ChannelFactoryContext,
    ChannelInboundMessage,
    ChannelPresentationPorts,
    ControlReceipt,
    CredentialRef,
    DeliveryStatus,
    PresentationReceipt,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    RawInbound,
    StreamDeltaPresentation,
    ToolPresentation,
    TurnOutputCompletedPresentation,
    TurnStartedPresentation,
    TurnStreamEvent,
    TurnStreamEventKind,
)


ROOT = Path(__file__).parents[1]


def _load_plugin_module():
    spec = importlib.util.spec_from_file_location(
        "feishu_v3_test_plugin",
        ROOT / "plugin.py",
        submodule_search_locations=[str(ROOT)],
    )
    if spec is None or spec.loader is None:
        raise ImportError("unable to load Feishu plugin")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()


class FakeProviderClient:
    def __init__(self) -> None:
        self.closed = False
        self.requested: list[tuple[str, ...]] = []

    def credential(self, ref: CredentialRef) -> str:
        self.requested.append(ref.path)
        if ref.path == ("appId",):
            return "app"
        if ref.path == ("appSecret",):
            return "secret"
        raise KeyError(ref.path)

    async def aclose(self) -> None:
        self.closed = True


class FakeProviderFactory:
    def __init__(self) -> None:
        self.client = FakeProviderClient()
        self.create_calls = 0
        self.received: dict[str, CredentialRef] | None = None
        self.closed = False

    async def create(self, credentials):
        self.create_calls += 1
        self.received = dict(credentials)
        return self.client

    async def aclose(self) -> None:
        self.closed = True


class FakeIngress:
    def __init__(self, accepted: bool = True) -> None:
        self.accepted = accepted
        self.raw: list[RawInbound] = []

    async def admit(self, raw: RawInbound) -> bool:
        self.raw.append(raw)
        return self.accepted


class FakeIdentity:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}
        self.lookups: list[str] = []

    def resolve(self, provider_identity: str) -> str | None:
        self.lookups.append(provider_identity)
        return self.values.get(provider_identity)


class FakeControl:
    def __init__(self) -> None:
        self.raw: RawInbound | None = None
        self.bodies = None

    async def interrupt(self, raw: RawInbound, *, response_bodies) -> ControlReceipt:
        self.raw = raw
        self.bodies = response_bodies
        return ControlReceipt(
            accepted=True,
            reason="interrupted",
            response=ChannelDeliveryReceipt("control-delivery", DeliveryStatus.DELIVERED),
        )


class FakeSubscription:
    def __init__(self, callback) -> None:
        self.callback = callback
        self.admission_closed = False
        self.closed = False

    def close_admission(self) -> None:
        self.admission_closed = True

    async def await_quiescence(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


class FakeTurnStream:
    def __init__(self) -> None:
        self.subscription: FakeSubscription | None = None

    def subscribe(self, callback) -> FakeSubscription:
        self.subscription = FakeSubscription(callback)
        return self.subscription


def _context(
    *,
    factory: FakeProviderFactory | None = None,
    ingress: FakeIngress | None = None,
    identity: FakeIdentity | None = None,
    control: FakeControl | None = None,
    stream: FakeTurnStream | None = None,
    config: dict[str, object] | None = None,
) -> ChannelFactoryContext:
    return ChannelFactoryContext(
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token="binding-1",
        config=(
            config
            if config is not None
            else {"allow_from": ("ou_sender",), "domain": "https://example.test"}
        ),
        credentials={
            "appId": CredentialRef(("appId",)),
            "appSecret": CredentialRef(("appSecret",)),
        },
        provider_client_factory=factory or FakeProviderFactory(),
        ingress=ingress or FakeIngress(),
        identity=identity or FakeIdentity(),
        control=control or FakeControl(),
        turn_stream=stream or FakeTurnStream(),
    )


def _message(*, message_type: str = "text", content: str = '{"text":"hello"}'):
    return SimpleNamespace(
        chat_type="p2p",
        message_id="msg-1",
        chat_id="oc_chat",
        message_type=message_type,
        content=content,
        parent_id="",
        create_time="1700000000000",
    )


def _event(*, content: str = '{"text":"hello"}', open_id: str = "ou_sender"):
    return SimpleNamespace(
        event=SimpleNamespace(
            message=_message(content=content),
            sender=SimpleNamespace(
                sender_id=SimpleNamespace(open_id=open_id, user_id="", union_id="")
            ),
        )
    )


def test_plugin_is_pure_v3_and_declares_exact_feishu_channel() -> None:
    from agent.plugins.composable import ComposablePlugin
    from agent.plugins.static_manifest import load_static_plugin_manifest

    instance = ComposablePlugin.from_module(module)
    assert instance.api_version == 3
    assert not hasattr(module, "FeishuPlugin")
    manifest = load_static_plugin_manifest(ROOT)
    assert manifest.api_version == 3
    assert manifest.channel_credentials == (
        ("feishu", ("appId", "appSecret", "app_id", "app_secret")),
    )


def test_config_accepts_only_opaque_credential_refs() -> None:
    from pydantic import ValidationError

    config = module.Config.model_validate(
        {
            "appId": CredentialRef(("appId",)),
            "appSecret": CredentialRef(("appSecret",)),
        }
    )
    assert config.app_id == CredentialRef(("appId",))
    with pytest.raises(ValidationError):
        module.Config.model_validate({"appId": "secret"})


def test_config_accepts_legacy_allow_from_alias_and_forbids_unknown_keys() -> None:
    from pydantic import ValidationError

    config = module.Config.model_validate(
        {
            "appId": CredentialRef(("appId",)),
            "appSecret": CredentialRef(("appSecret",)),
            "allowFrom": ["ou_sender"],
        }
    )
    assert config.allow_from == ("ou_sender",)
    with pytest.raises(ValidationError):
        module.Config.model_validate({"unknown": True})


@pytest.mark.asyncio
async def test_apply_registers_definition_through_exact_root_service() -> None:
    calls = []

    class Channels:
        async def register(self, ctx, definition) -> None:
            calls.append((ctx, definition))

    class Context:
        runtime = SimpleNamespace(config=module.Config())

        def require(self, key):
            assert key.name == "core.channels"
            return Channels()

    await module.apply(Context(), module.Config())
    definition = calls[0][1]
    assert definition.name == "feishu"
    assert {item.value for item in definition.capabilities} == {
        "inbound",
        "outbound",
        "control",
        "turn_stream",
    }
    assert definition.factory_export == "build_feishu_channel"
    assert definition.inbound_identity.value == "provider_message_id"


def test_candidate_factory_does_not_create_client_or_resolve_credentials() -> None:
    factory = FakeProviderFactory()
    adapter = module.build_feishu_channel(_context(factory=factory))
    assert factory.create_calls == 0
    assert getattr(adapter, "_client") is None
    assert getattr(adapter, "_provider_client") is None
    assert getattr(adapter, "_app_secret") is None


@pytest.mark.asyncio
async def test_formal_start_deliver_and_stop_use_controlled_provider_client() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    ready = await adapter.start()
    assert ready.binding_token == "binding-1"
    assert not ready.admission_open
    assert factory.create_calls == 1
    assert factory.received == {
        "appId": CredentialRef(("appId",)),
        "appSecret": CredentialRef(("appSecret",)),
    }
    assert adapter._app_id is None
    assert adapter._app_secret is None

    calls: list[tuple[str, str, str]] = []

    async def post(recipient: str, message_type: str, content: str):
        calls.append((recipient, message_type, content))
        return {"message_id": f"provider-{len(calls)}"}

    adapter._post_message_once = post
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="binding-1",
            delivery_id="delivery-1",
            recipient="oc_chat",
            body="hello",
        )
    )
    assert receipt.status is DeliveryStatus.DELIVERED
    assert receipt.provider_ids == ("provider-1",)
    assert calls[0][1] == "interactive"

    stop = await adapter.stop()
    assert stop.resources_closed
    assert factory.client.closed
    assert stream.subscription is not None and stream.subscription.closed


@pytest.mark.asyncio
async def test_start_failure_does_not_join_unstarted_websocket_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )

    def fail_start(self: threading.Thread) -> None:
        if self.name == "feishu-ws":
            raise RuntimeError("thread start blocked")
        raise AssertionError(f"unexpected thread: {self.name}")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError, match="thread start blocked"):
        await adapter.start()

    assert factory.client.closed
    assert adapter._ws_thread is None
    assert not adapter._ws_thread_started
    stop = await adapter.stop()
    assert stop.resources_closed


@pytest.mark.asyncio
async def test_stop_failure_retains_provider_owner_for_exact_retry() -> None:
    class FlakyClient(FakeProviderClient):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def aclose(self) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("provider close interrupted")
            await super().aclose()

    class FlakyFactory(FakeProviderFactory):
        def __init__(self) -> None:
            super().__init__()
            self.client = FlakyClient()

    factory = FlakyFactory()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    await adapter.start()
    first = await adapter.stop()
    assert not first.resources_closed
    assert any(item.resource == "provider-client" for item in first.failures)
    second = await adapter.stop()
    assert second.resources_closed
    assert factory.client.attempts == 2


@pytest.mark.asyncio
async def test_attachment_delivery_is_deterministic_rejected_without_provider_effect() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    from agent.plugin_composition.channels import AttachmentKind, AttachmentRef

    attachment = AttachmentRef(
        artifact_id="artifact-1",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256="0" * 64,
    )
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="binding-1",
            delivery_id="delivery-attachment",
            recipient="oc_chat",
            body="body",
            attachments=(attachment,),
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED
    assert factory.create_calls == 0


@pytest.mark.asyncio
async def test_delivery_fallback_only_runs_after_deterministic_card_rejection() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    await adapter.start()
    calls: list[str] = []

    async def rejected_card(recipient: str, message_type: str, content: str):
        calls.append(message_type)
        if message_type == "interactive":
            raise module.channel.FeishuApiError(123, "card rejected")
        return {"message_id": "text-fallback"}

    adapter._post_message_once = rejected_card
    fallback = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-fallback", "oc_chat", "hello")
    )
    assert fallback.status is DeliveryStatus.DELIVERED
    assert calls == ["interactive", "text"]

    calls.clear()

    async def uncertain(recipient: str, message_type: str, content: str):
        calls.append(message_type)
        raise TimeoutError("provider effect unknown")

    adapter._post_message_once = uncertain
    unknown = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-unknown", "oc_chat", "hello")
    )
    assert unknown.status is DeliveryStatus.UNKNOWN
    assert calls == ["interactive"]
    await adapter.stop()


@pytest.mark.asyncio
async def test_text_inbound_admits_raw_message_and_attachment_is_rejected() -> None:
    ingress = FakeIngress()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(ingress=ingress, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    status = await adapter._ingest_message(
        _message(), "msg-1", "oc_chat", "ou_sender", "", ""
    )
    assert status is DeliveryStatus.DELIVERED
    assert ingress.raw[0].provider_identity == "ou_sender"
    assert ingress.raw[0].recipient == "oc_chat"
    assert ingress.raw[0].message.content == "hello"
    rejected = await adapter._ingest_message(
        _message(message_type="image", content='{"image_key":"img"}'),
        "msg-2",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )
    assert rejected is DeliveryStatus.REJECTED
    assert len(ingress.raw) == 1


@pytest.mark.asyncio
async def test_unauthorized_inbound_and_control_are_fail_closed() -> None:
    ingress = FakeIngress()
    control = FakeControl()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(
        _context(
            ingress=ingress,
            control=control,
            stream=stream,
            config={"allowFrom": (), "domain": "https://example.test"},
        )
    )
    adapter.attach_presentation(
        ChannelPresentationPorts(control=control, turn_stream=stream)
    )

    inbound = await adapter._handle_message_event(_event())
    control_attempt = await adapter._handle_message_event(
        _event(content='{"text":"/stop"}')
    )

    assert inbound is DeliveryStatus.REJECTED
    assert control_attempt is DeliveryStatus.REJECTED
    assert ingress.raw == []
    assert control.raw is None


@pytest.mark.asyncio
async def test_legacy_allow_from_alias_reaches_inbound_allowlist() -> None:
    ingress = FakeIngress()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(
        _context(
            ingress=ingress,
            stream=stream,
            config={"allowFrom": ("ou_sender",), "domain": "https://example.test"},
        )
    )
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )

    status = await adapter._handle_message_event(_event())

    assert status is DeliveryStatus.DELIVERED
    assert len(ingress.raw) == 1


@pytest.mark.asyncio
async def test_stop_uses_exact_core_control_port() -> None:
    control = FakeControl()
    adapter = module.build_feishu_channel(_context(control=control))
    stream = FakeTurnStream()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=control, turn_stream=stream)
    )
    status = await adapter._ingest_message(
        _message(content='{"text":"/stop"}'),
        "stop-1",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )
    assert status is DeliveryStatus.DELIVERED
    assert control.raw is not None
    assert control.raw.message.content == "/stop"


@pytest.mark.asyncio
async def test_turn_stream_keeps_one_preview_id_and_final_summary_patch() -> None:
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    calls: list[tuple[str, str, str]] = []

    async def post(recipient: str, message_type: str, content: str):
        calls.append(("post", recipient, content))
        return DeliveryStatus.DELIVERED, "preview-1", None

    async def patch(message_id: str, content: str):
        calls.append(("patch", message_id, content))
        return DeliveryStatus.DELIVERED, None

    adapter._send_one = post  # type: ignore[method-assign]
    adapter._patch_one = patch  # type: ignore[method-assign]
    adapter._inbound_recipients["msg-1"] = "oc_chat"

    started = TurnStreamEvent(
        "preview:turn-1",
        TurnStreamEventKind.TURN_STARTED,
        TurnStartedPresentation("turn-1", "msg-1"),
    )
    delta = TurnStreamEvent(
        "preview:turn-1",
        TurnStreamEventKind.STREAM_DELTA,
        StreamDeltaPresentation("turn-1", 1, "hello", ""),
    )
    tool = TurnStreamEvent(
        "preview:turn-1",
        TurnStreamEventKind.TOOL_STARTED,
        ToolPresentation("turn-1", 2, "tool-1", "shell"),
    )
    completed = TurnStreamEvent(
        "preview:turn-1",
        TurnStreamEventKind.TURN_OUTPUT_COMPLETED,
        TurnOutputCompletedPresentation("turn-1", 3),
    )
    assert (await adapter._on_turn_stream(started)).status is DeliveryStatus.DELIVERED
    assert (await adapter._on_turn_stream(delta)).status is DeliveryStatus.DELIVERED
    assert (await adapter._on_turn_stream(tool)).status is DeliveryStatus.DELIVERED
    assert (await adapter._on_turn_stream(completed)).status is DeliveryStatus.DELIVERED
    assert [item[0] for item in calls] == ["post", "patch", "patch", "patch"]
    assert all(item[1] in {"oc_chat", "preview-1"} for item in calls)
    assert adapter._inbound_recipients == {}
    assert adapter._turn_recipients == {}
    assert adapter._presentation_client_messages == {}
    assert adapter._reply_buffers == {}
    assert adapter._thinking_buffers == {}
    assert adapter._tool_lines == {}
    assert adapter._preview_messages == {}
