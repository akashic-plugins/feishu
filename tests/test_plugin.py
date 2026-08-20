from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import httpx

from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
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
    def __init__(self, callback, *, fail_close_attempts: int = 0) -> None:
        self.callback = callback
        self.fail_close_attempts = fail_close_attempts
        self.close_calls = 0
        self.admission_closed = False
        self.closed = False

    def close_admission(self) -> None:
        self.admission_closed = True

    async def await_quiescence(self) -> None:
        return None

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_calls <= self.fail_close_attempts:
            raise RuntimeError("stream close interrupted")
        self.closed = True


class FakeTurnStream:
    def __init__(self, *, fail_close_attempts: int = 0) -> None:
        self.fail_close_attempts = fail_close_attempts
        self.subscription: FakeSubscription | None = None

    def subscribe(self, callback) -> FakeSubscription:
        self.subscription = FakeSubscription(
            callback,
            fail_close_attempts=self.fail_close_attempts,
        )
        return self.subscription


class FakeAttachmentReadLease:
    def __init__(self, ref: AttachmentRef, data: bytes) -> None:
        self.ref = ref
        self.data = data
        self.closed = False
        self.max_bytes: int | None = None

    async def read_bytes(self, *, max_bytes: int) -> bytes:
        self.max_bytes = max_bytes
        if len(self.data) > max_bytes:
            raise ValueError("read exceeded bound")
        return self.data

    async def aclose(self) -> None:
        self.closed = True


class FakeAttachmentRead:
    def __init__(self, values: dict[str, tuple[AttachmentRef, bytes]] | None = None) -> None:
        self.values = values or {}
        self.leases: list[FakeAttachmentReadLease] = []

    async def acquire(self, ref: AttachmentRef) -> FakeAttachmentReadLease:
        actual_ref, data = self.values.get(ref.artifact_id, (ref, b""))
        lease = FakeAttachmentReadLease(actual_ref, data)
        self.leases.append(lease)
        return lease


class FakeAttachmentImport:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, AttachmentKind, str | None, str | None]] = []

    async def import_bytes(
        self,
        data: bytes,
        *,
        kind: AttachmentKind,
        filename: str | None,
        media_type: str | None,
    ) -> AttachmentRef:
        self.calls.append((data, kind, filename, media_type))
        return AttachmentRef(
            artifact_id=f"imported-{len(self.calls)}",
            kind=kind,
            filename=filename,
            media_type=media_type,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )


def _context(
    *,
    factory: FakeProviderFactory | None = None,
    ingress: FakeIngress | None = None,
    identity: FakeIdentity | None = None,
    control: FakeControl | None = None,
    stream: FakeTurnStream | None = None,
    attachment_read: FakeAttachmentRead | None = None,
    attachment_import: FakeAttachmentImport | None = None,
    config: dict[str, object] | None = None,
) -> ChannelFactoryContext:
    return ChannelFactoryContext(
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token="binding-1",
        config=(
            config
            if config is not None
            else {"allow_from": ("ou_sender",), "domain": "https://open.feishu.cn"}
        ),
        credentials={
            "appId": CredentialRef(("appId",)),
            "appSecret": CredentialRef(("appSecret",)),
        },
        provider_client_factory=factory or FakeProviderFactory(),
        ingress=ingress or FakeIngress(),
        identity=identity or FakeIdentity(),
        attachment_import=attachment_import or FakeAttachmentImport(),
        attachment_read=attachment_read or FakeAttachmentRead(),
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
async def test_start_failure_retains_stream_owner_for_later_close_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream(fail_close_attempts=1)
    adapter = module.build_feishu_channel(_context(factory=factory, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )

    def fail_start(self: threading.Thread) -> None:
        if self.name == "feishu-ws":
            raise RuntimeError("thread start blocked")
        raise AssertionError(f"unexpected thread: {self.name}")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError, match="thread start blocked") as raised:
        await adapter.start()

    subscription = stream.subscription
    assert subscription is not None
    assert subscription.close_calls == 1
    assert not subscription.closed
    assert adapter._stream_subscription is subscription
    assert any("stream close interrupted" in note for note in raised.value.__notes__)

    receipt = await adapter.stop()
    assert receipt.resources_closed
    assert receipt.failures == ()
    assert subscription.close_calls == 2
    assert subscription.closed
    assert adapter._stream_subscription is None


@pytest.mark.asyncio
async def test_persistent_start_cleanup_failure_keeps_stream_owner_and_reports_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream(fail_close_attempts=2)
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

    subscription = stream.subscription
    assert subscription is not None
    first_retry = await adapter.stop()
    assert not first_retry.resources_closed
    assert any(item.resource == "turn-stream" for item in first_retry.failures)
    assert adapter._stream_subscription is subscription
    assert not subscription.closed

    second_retry = await adapter.stop()
    assert second_retry.resources_closed
    assert second_retry.failures == ()
    assert subscription.close_calls == 3
    assert subscription.closed
    assert adapter._stream_subscription is None


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
async def test_attachment_delivery_reads_exact_bytes_and_preserves_text_file_order() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-1",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )
    read = FakeAttachmentRead({attachment.artifact_id: (attachment, data)})
    adapter = module.build_feishu_channel(
        _context(factory=factory, stream=stream, attachment_read=read)
    )
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    await adapter.start()
    calls: list[tuple[str, str]] = []

    async def post(recipient: str, message_type: str, content: str):
        calls.append(("post", message_type))
        return {"message_id": f"provider-{len(calls)}"}

    async def upload(data: bytes, filename: str):
        assert data == b"x"
        assert filename == "a.txt"
        calls.append(("upload", filename))
        return "file-key"

    adapter._post_message_once = post
    adapter._upload_file = upload
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="binding-1",
            delivery_id="delivery-attachment",
            recipient="oc_chat",
            body="body",
            attachments=(attachment,),
        )
    )
    assert receipt.status is DeliveryStatus.DELIVERED
    assert calls == [("post", "interactive"), ("upload", "a.txt"), ("post", "file")]
    assert read.leases[0].max_bytes == 1
    assert read.leases[0].closed
    await adapter.stop()


@pytest.mark.asyncio
async def test_attachment_upload_business_error_is_unknown_without_attachment_message() -> None:
    stream = FakeTurnStream()
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-failure",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    adapter = module.build_feishu_channel(
        _context(
            stream=stream,
            attachment_read=FakeAttachmentRead({"artifact-failure": (attachment, data)}),
        )
    )
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))
    await adapter.start()
    calls: list[str] = []

    async def post(recipient: str, message_type: str, content: str):
        calls.append(message_type)
        return {"message_id": "text-id"}

    async def upload(data: bytes, filename: str):
        raise module.channel.FeishuApiError(123, "invalid media")

    adapter._post_message_once = post
    adapter._upload_file = upload
    receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-failure", "oc_chat", "", (attachment,))
    )
    assert receipt.status is DeliveryStatus.UNKNOWN
    assert calls == []
    await adapter.stop()


@pytest.mark.asyncio
async def test_attachment_delivery_cancel_settles_unknown_and_closes_read_lease() -> None:
    stream = FakeTurnStream()
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-cancel",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    read = FakeAttachmentRead({"artifact-cancel": (attachment, data)})
    adapter = module.build_feishu_channel(_context(stream=stream, attachment_read=read))
    adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))
    await adapter.start()
    started = asyncio.Event()

    async def upload(data: bytes, filename: str):
        started.set()
        await asyncio.Event().wait()

    adapter._upload_file = upload
    task = asyncio.create_task(
        adapter.deliver(
            ProviderDeliveryRequest(
                "binding-1", "delivery-cancel", "oc_chat", "", (attachment,)
            )
        )
    )
    await started.wait()
    task.cancel()
    receipt = await task
    assert receipt.status is DeliveryStatus.UNKNOWN
    assert read.leases[0].closed
    await adapter.stop()


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
            request = httpx.Request("POST", "https://open.feishu.cn/open-apis/im/v1/messages")
            response = httpx.Response(400, request=request)
            raise httpx.HTTPStatusError("card rejected", request=request, response=response)
        return {"message_id": "text-fallback"}

    adapter._post_message_once = rejected_card
    fallback = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-fallback", "oc_chat", "hello")
    )
    assert fallback.status is DeliveryStatus.DELIVERED
    assert calls == ["interactive", "text"]

    calls.clear()

    async def unknown_business_code(recipient: str, message_type: str, content: str):
        calls.append(message_type)
        raise module.channel.FeishuApiError(123, "provider effect unspecified")

    adapter._post_message_once = unknown_business_code
    business_unknown = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-business", "oc_chat", "hello")
    )
    assert business_unknown.status is DeliveryStatus.UNKNOWN
    assert calls == ["interactive"]

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
async def test_delivery_after_prior_success_aggregates_later_rejection_as_unknown() -> None:
    adapter = module.build_feishu_channel(_context())
    adapter._client = object()
    first = AttachmentRef(
        "aggregate-1",
        AttachmentKind.FILE,
        "a.txt",
        "text/plain",
        1,
        hashlib.sha256(b"a").hexdigest(),
    )
    second = AttachmentRef(
        "aggregate-2",
        AttachmentKind.FILE,
        "b.txt",
        "text/plain",
        1,
        hashlib.sha256(b"b").hexdigest(),
    )

    async def read(_refs):
        return [(first, b"a"), (second, b"b")]

    outcomes = iter(
        [
            (DeliveryStatus.DELIVERED, "provider-1", None),
            (DeliveryStatus.REJECTED, None, "HTTP 400"),
        ]
    )

    async def send(_recipient, _ref, _data):
        return next(outcomes)

    adapter._read_attachments = read
    adapter._send_attachment = send
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1", "aggregate-delivery", "oc_chat", "", (first, second)
        )
    )
    assert receipt.status is DeliveryStatus.UNKNOWN
    assert receipt.provider_ids == ("provider-1",)


@pytest.mark.asyncio
async def test_connection_setup_error_is_rejected_before_any_feishu_effect() -> None:
    adapter = module.build_feishu_channel(_context())
    adapter._client = object()

    async def fail_connect(*_args, **_kwargs):
        raise httpx.ConnectError("connect failed")

    adapter._post_message_once = fail_connect
    receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "connect-error", "oc_chat", "hello")
    )
    assert receipt.status is DeliveryStatus.REJECTED


@pytest.mark.asyncio
async def test_inbound_download_streams_and_enforces_bound_before_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.channel, "_MAX_ATTACHMENT_BYTES", 3)
    adapter = module.build_feishu_channel(_context())
    calls: list[tuple[str, str]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        async def aiter_bytes(self):
            yield b"xx"
            yield b"xx"

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return None

    class Client:
        def stream(self, method: str, url: str, **kwargs):
            calls.append((method, url))
            assert kwargs["follow_redirects"] is False
            return Stream()

    async def token() -> str:
        return "token"

    adapter._client = Client()
    adapter._get_access_token = token
    with pytest.raises(ValueError, match="超过剩余批次额度"):
        await adapter._download_resource_bytes(
            "message-1", "file-1", "file", max_bytes=3
        )
    assert calls == [
        (
            "GET",
            "https://open.feishu.cn/open-apis/im/v1/messages/message-1/resources/file-1",
        )
    ]


def test_malicious_domain_is_rejected_before_credentials_or_http() -> None:
    factory = FakeProviderFactory()
    context = _context(
        factory=factory,
        config={
            "allow_from": ("ou_sender",),
            "domain": "https://open.feishu.cn@evil.example/steal",
        },
    )
    with pytest.raises(ValueError, match="官方 HTTPS API 域名"):
        module.build_feishu_channel(context)
    assert factory.create_calls == 0
    assert factory.client.requested == []


@pytest.mark.asyncio
async def test_post_batch_applies_remaining_budget_before_second_stream_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.channel, "_MAX_ATTACHMENT_BATCH_BYTES", 3)
    imported = FakeAttachmentImport()
    adapter = module.build_feishu_channel(_context(attachment_import=imported))
    streamed: list[tuple[str, bool]] = []

    class Response:
        def __init__(self, key: str) -> None:
            self.key = key
            self.headers = {"content-length": "2"}

        def raise_for_status(self) -> None:
            return None

        async def aiter_bytes(self):
            streamed.append((self.key, True))
            yield b"xx"

    class Stream:
        def __init__(self, key: str) -> None:
            self.response = Response(key)

        async def __aenter__(self) -> Response:
            return self.response

        async def __aexit__(self, *_args) -> None:
            return None

    class Client:
        def stream(self, _method: str, url: str, **_kwargs) -> Stream:
            key = url.rsplit("/", 1)[-1]
            streamed.append((key, False))
            return Stream(key)

    async def token() -> str:
        return "token"

    adapter._client = Client()
    adapter._get_access_token = token
    message = _message(
        message_type="post",
        content=json.dumps(
            {
                "content": [
                    [
                        {"tag": "img", "image_key": "first"},
                        {"tag": "img", "image_key": "second"},
                    ]
                ]
            }
        ),
    )
    with pytest.raises(ValueError, match="剩余批次额度"):
        await adapter._extract_inbound_payload(message, "message-1")
    assert streamed == [("first", False), ("first", True), ("second", False)]
    assert imported.calls == []


@pytest.mark.asyncio
async def test_runtime_lifecycle_rejects_before_open_and_stop_drains_accepted_task() -> None:
    adapter = module.build_feishu_channel(_context())
    context = adapter._context
    adapter.attach_runtime(SimpleNamespace(binding_token=context.binding_token))
    adapter.open_admission()
    adapter.close_admission()
    assert not adapter._admission_open

    released = asyncio.Event()

    async def accepted_before_close() -> None:
        await released.wait()

    task = asyncio.create_task(accepted_before_close())
    adapter._inbound_tasks.add(task)
    task.add_done_callback(adapter._inbound_tasks.discard)
    stopping = asyncio.create_task(adapter.stop())
    await asyncio.sleep(0)
    assert not stopping.done()
    released.set()
    assert (await stopping).resources_closed


@pytest.mark.asyncio
async def test_feishu_stop_with_post_attachment_rejects_before_import() -> None:
    control = FakeControl()
    imported = FakeAttachmentImport()
    adapter = module.build_feishu_channel(
        _context(control=control, attachment_import=imported)
    )
    message = _message(
        message_type="post",
        content=json.dumps(
            {
                "title": "/stop",
                "content": [[{"tag": "img", "image_key": "image-key"}]],
            }
        ),
    )

    async def fail_download(*_args):
        raise AssertionError("带附件 /stop 不得下载 provider media")

    adapter._download_resource_bytes = fail_download
    status = await adapter._ingest_message(
        message, "stop-media", "oc_chat", "ou_sender", "", ""
    )
    assert status is DeliveryStatus.REJECTED
    assert imported.calls == []
    assert control.raw is None


@pytest.mark.asyncio
async def test_feishu_invalid_recipient_path_is_rejected_before_provider_call() -> None:
    adapter = module.build_feishu_channel(_context())
    adapter._client = object()
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1", "invalid-recipient", "oc_chat/escape", "hello"
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED


@pytest.mark.asyncio
async def test_text_and_image_inbound_import_core_attachment_before_admission() -> None:
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
    async def download(
        message_id: str,
        file_key: str,
        resource_type: str,
        *,
        max_bytes: int,
    ) -> bytes:
        assert (message_id, file_key, resource_type) == ("msg-2", "img", "image")
        assert max_bytes == min(
            module.channel._MAX_ATTACHMENT_BYTES,
            module.channel._MAX_ATTACHMENT_BATCH_BYTES,
        )
        return b"image-bytes"

    adapter._download_resource_bytes = download
    image = await adapter._ingest_message(
        _message(message_type="image", content='{"image_key":"img"}'),
        "msg-2",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )
    assert image is DeliveryStatus.DELIVERED
    assert len(ingress.raw) == 2
    assert ingress.raw[1].message.content == "[图片]"
    assert ingress.raw[1].message.attachments[0].size_bytes == len(b"image-bytes")


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
            config={"allowFrom": (), "domain": "https://open.feishu.cn"},
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
            config={"allowFrom": ("ou_sender",), "domain": "https://open.feishu.cn"},
        )
    )
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    adapter.open_admission()

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
async def test_reply_stop_bypasses_parent_fetch_and_uses_exact_control_port() -> None:
    control = FakeControl()
    adapter = module.build_feishu_channel(_context(control=control))
    stream = FakeTurnStream()
    adapter.attach_presentation(
        ChannelPresentationPorts(control=control, turn_stream=stream)
    )
    message = _message(content='{"text":"/stop"}')
    message.parent_id = "parent-1"

    async def fail_parent_fetch(message_id: str) -> str:
        raise AssertionError(f"reply parent must not be fetched for {message_id}")

    adapter._fetch_message_text = fail_parent_fetch  # type: ignore[method-assign]
    status = await adapter._ingest_message(
        message,
        "stop-reply-1",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )

    assert status is DeliveryStatus.DELIVERED
    assert control.raw is not None
    assert control.raw.message.content == "/stop"
    assert "reply_to_message_id" not in control.raw.message.metadata


@pytest.mark.asyncio
async def test_multiline_inbound_is_admitted_with_visible_control_markers() -> None:
    ingress = FakeIngress()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(ingress=ingress, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )

    status = await adapter._ingest_message(
        _message(content='{"text":"hello\\nworld\\t!"}'),
        "msg-lines",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )

    assert status is DeliveryStatus.DELIVERED
    assert ingress.raw[0].message.content == r"hello\nworld\t!"
    assert all(ord(char) >= 32 for char in ingress.raw[0].message.content)


@pytest.mark.asyncio
async def test_reply_context_is_admitted_with_visible_control_markers() -> None:
    ingress = FakeIngress()
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(ingress=ingress, stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    message = _message(content='{"text":"reply"}')
    message.parent_id = "parent-1"

    async def fetch_parent(message_id: str) -> str:
        assert message_id == "parent-1"
        return "parent\nline\t!"

    adapter._fetch_message_text = fetch_parent  # type: ignore[method-assign]
    status = await adapter._ingest_message(
        message,
        "msg-reply",
        "oc_chat",
        "ou_sender",
        "",
        "",
    )

    assert status is DeliveryStatus.DELIVERED
    assert ingress.raw[0].message.content == (
        r"【你正在回复一条历史消息】\n"
        r"被回复消息：\nparent\nline\t!\n\n"
        r"【你当前新消息】\nreply"
    )
    assert ingress.raw[0].message.metadata["reply_to_message_id"] == "parent-1"


@pytest.mark.asyncio
async def test_stop_clears_all_transient_state_even_when_resources_already_closed() -> None:
    adapter = module.build_feishu_channel(_context())
    adapter._inbound_recipients["msg-1"] = "oc_chat"
    adapter._turn_recipients["turn-1"] = "oc_chat"
    adapter._presentation_client_messages["preview-1"] = "msg-1"
    adapter._reply_buffers["preview-1"] = "reply"
    adapter._thinking_buffers["preview-1"] = "thinking"
    adapter._tool_lines["preview-1"] = []
    adapter._preview_messages["preview-1"] = "provider-1"
    adapter._failed_presentations.add("preview-1")
    adapter._rejected_presentations.add("preview-2")

    receipt = await adapter.stop()

    assert receipt.resources_closed
    assert adapter._inbound_recipients == {}
    assert adapter._turn_recipients == {}
    assert adapter._presentation_client_messages == {}
    assert adapter._reply_buffers == {}
    assert adapter._thinking_buffers == {}
    assert adapter._tool_lines == {}
    assert adapter._preview_messages == {}
    assert adapter._failed_presentations == set()
    assert adapter._rejected_presentations == set()

    adapter._stopping = True
    adapter._inbound_recipients["msg-2"] = "oc_chat"
    second = await adapter.stop()
    assert second.resources_closed
    assert adapter._inbound_recipients == {}


@pytest.mark.asyncio
async def test_cancelled_turn_stream_clears_preview_and_recipient_state() -> None:
    stream = FakeTurnStream()
    adapter = module.build_feishu_channel(_context(stream=stream))
    adapter.attach_presentation(
        ChannelPresentationPorts(control=FakeControl(), turn_stream=stream)
    )
    adapter._inbound_recipients["msg-cancel"] = "oc_chat"
    waiting = asyncio.Event()

    async def block_preview(event, recipient: str, *, live: bool):
        await waiting.wait()

    adapter._sync_preview = block_preview  # type: ignore[method-assign]
    started = TurnStreamEvent(
        "preview:cancel",
        TurnStreamEventKind.TURN_STARTED,
        TurnStartedPresentation("turn-cancel", "msg-cancel"),
    )
    task = asyncio.create_task(adapter._on_turn_stream(started))
    await asyncio.sleep(0)
    assert adapter._turn_recipients == {"turn-cancel": "oc_chat"}
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert adapter._inbound_recipients == {}
    assert adapter._turn_recipients == {}
    assert adapter._presentation_client_messages == {}
    assert adapter._reply_buffers == {}
    assert adapter._thinking_buffers == {}
    assert adapter._tool_lines == {}
    assert adapter._preview_messages == {}
    assert adapter._failed_presentations == set()
    assert adapter._rejected_presentations == set()


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
