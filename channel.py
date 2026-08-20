"""Pure v3 Feishu channel adapter.

The adapter owns only Feishu protocol translation. Core owns admission, identity,
control, delivery identity, presentation lifecycle, and all persistent state.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import mimetypes
import threading
import time
import warnings
from collections.abc import AsyncIterable, Callable, Coroutine, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

import httpx

from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
    ChannelAdapter,
    ChannelCleanupFailure,
    ChannelFactoryContext,
    ChannelInboundMessage,
    ChannelPresentationPorts,
    ChannelReady,
    ControlResponseBodies,
    CredentialRef,
    DeliveryStatus,
    InboundIdentity,
    PresentationReceipt,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    RawInbound,
    StopReceipt,
    StreamDeltaPresentation,
    ToolPresentation,
    TurnOutputCompletedPresentation,
    TurnStartedPresentation,
    TurnStreamEvent,
    TurnStreamEventKind,
)

from .cards import (
    ToolLiveLine,
    build_live_card,
    build_markdown_card,
    build_summary_card,
)

logger = logging.getLogger(__name__)

_CHANNEL = "feishu"
_CARD_TEXT_LIMIT = 4000
_WS_RECONNECT_DELAY_S = 5.0
_WS_STOP_TIMEOUT_S = 2.0
_REJECTED_HTTP_STATUSES = frozenset({400, 401, 403, 404, 405, 413, 415, 422})
_RATE_LIMIT_CODES = frozenset({99991400, 99991661, 230020, 230027, 11232})
_CREDENTIAL_ALIASES = {
    "app_id": ("appId", "app_id"),
    "app_secret": ("appSecret", "app_secret"),
}
_MAX_ATTACHMENT_BYTES = 50 * 1024 * 1024
_MAX_ATTACHMENT_COUNT = 16
_MAX_ATTACHMENT_BATCH_BYTES = 100 * 1024 * 1024
_MAX_PROVIDER_SEGMENT_LENGTH = 256


@dataclass(slots=True)
class _TokenCache:
    token: str
    expires_at: float


class FeishuApiError(RuntimeError):
    """Represent a provider response with a non-zero Feishu business code."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"飞书 API 失败 code={code} msg={message}")
        self.code = code


class _SdkShutdownLogFilter(logging.Filter):
    """Hide only the SDK error emitted by an intentional socket shutdown."""

    def __init__(self, stopped: threading.Event) -> None:
        super().__init__()
        self._stopped = stopped

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            self._stopped.is_set()
            and record.getMessage().startswith("receive message loop exit")
        )


def build_feishu_channel(context: ChannelFactoryContext) -> ChannelAdapter:
    """Build a side-effect-free Feishu adapter for Core's exact binding."""

    if not isinstance(context, ChannelFactoryContext):
        raise TypeError("Feishu channel factory 只接受 ChannelFactoryContext")
    if context.identity is None:
        raise RuntimeError("Feishu v3 channel 需要 Core ChannelIdentityPort")
    if context.ingress is None:
        raise RuntimeError("Feishu v3 channel 需要 Core ChannelIngressPort")
    if context.control is None:
        raise RuntimeError("Feishu v3 channel 需要 Core ChannelControlPort")
    if context.turn_stream is None:
        raise RuntimeError("Feishu v3 channel 需要 Core TurnStreamPort")
    return FeishuAdapter(context)


class FeishuAdapter:
    """Translate Feishu text, control, delivery, and preview events to C14 ports."""

    name = _CHANNEL

    def __init__(self, context: ChannelFactoryContext) -> None:
        """Freeze only Core references; no credentials, client, SDK, or network is touched."""

        self._context = context
        self._identity = context.identity
        self._ingress = context.ingress
        self._provider_factory = context.provider_client_factory
        self._credentials = context.credentials
        self._config = context.config
        self._binding_token = context.binding_token
        self._domain = _domain(self._config)
        self._allow_from = _allow_from(self._config)

        self._presentation: ChannelPresentationPorts | None = None
        self._stream_subscription: Any | None = None
        self._provider_client: Any | None = None
        self._client: httpx.AsyncClient | None = None
        self._app_id: str | None = None
        self._app_secret: str | None = None
        self._token: _TokenCache | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._stopping = False
        self._admission_open = False
        self._runtime: Any | None = None

        self._ws_client: Any | None = None
        self._ws_loop: asyncio.AbstractEventLoop | None = None
        self._ws_thread: threading.Thread | None = None
        self._ws_thread_started = False
        self._ws_stopped = threading.Event()
        self._sdk_logger: logging.Logger | None = None
        self._sdk_shutdown_filter: logging.Filter | None = None
        self._inbound_tasks: set[asyncio.Task[DeliveryStatus | None]] = set()

        self._inbound_recipients: dict[str, str] = {}
        self._turn_recipients: dict[str, str] = {}
        self._presentation_client_messages: dict[str, str] = {}
        self._reply_buffers: dict[str, str] = {}
        self._thinking_buffers: dict[str, str] = {}
        self._tool_lines: dict[str, list[ToolLiveLine]] = {}
        self._preview_messages: dict[str, str] = {}
        self._failed_presentations: set[str] = set()
        self._rejected_presentations: set[str] = set()

    def attach_presentation(self, ports: ChannelPresentationPorts) -> None:
        """Bind the exact Core control and turn-stream facades before start."""

        if self._presentation is not None:
            raise RuntimeError("Feishu presentation ports 不能重复绑定")
        if ports.control is None or ports.turn_stream is None:
            raise RuntimeError("Feishu v3 必须同时绑定 control 与 turn_stream")
        self._presentation = ports

    def attach_runtime(self, runtime: Any) -> None:
        """Bind the exact Host runtime lifecycle owner without replacing context ports."""

        if self._runtime is not None:
            raise RuntimeError("Feishu runtime 不能重复绑定")
        if runtime is None:
            raise TypeError("Feishu runtime 不能为空")
        if getattr(runtime, "binding_token", None) != self._binding_token:
            raise RuntimeError("Feishu runtime binding token 不匹配")
        self._runtime = runtime

    def open_admission(self) -> None:
        """Allow provider ingress only after Core has published this binding."""

        if self._stopping:
            raise RuntimeError("Feishu adapter 正在停止")
        self._admission_open = True

    def close_admission(self) -> None:
        """Reject new provider ingress while accepted callback tasks drain."""

        self._admission_open = False

    async def start(self) -> ChannelReady:
        """Resolve formal credentials, create the provider client, and start closed."""

        if self._started or self._stopping:
            raise RuntimeError("Feishu adapter 已启动或正在停止")
        if self._presentation is None:
            raise RuntimeError("Feishu adapter 缺少 presentation ports")
        self._loop = asyncio.get_running_loop()
        try:
            # 1. Only the formal Host invokes ProviderClientFactory and unwraps refs.
            self._provider_client = await self._provider_factory.create(self._credentials)
            self._read_credential("app_id")
            self._read_credential("app_secret")
            self._client = httpx.AsyncClient(timeout=30.0)

            # 2. Subscribe through the exact Core stream and keep admission closed.
            turn_stream = self._presentation.turn_stream
            if turn_stream is None:
                raise RuntimeError("Feishu turn stream port 未绑定")
            self._stream_subscription = turn_stream.subscribe(self._on_turn_stream)
            self._ws_stopped.clear()
            self._ws_thread = threading.Thread(
                target=self._run_ws_client,
                name="feishu-ws",
                daemon=True,
            )
            self._ws_thread_started = False
            self._ws_thread.start()
            self._ws_thread_started = True
            self._started = True
            logger.info("[feishu] v3 channel started binding=%s", self._binding_token)
            return ChannelReady(
                binding_token=self._binding_token,
                subscriptions=("feishu.websocket", "feishu.turn_stream"),
                admission_open=False,
            )
        except BaseException as error:
            cleanup_failures = await self._close_resources_after_start_failure()
            if cleanup_failures:
                error.add_note(
                    "Feishu start cleanup failed: "
                    + "; ".join(
                        str(failure) or type(failure).__name__
                        for failure in cleanup_failures
                    )
                )
            raise

    async def deliver(self, request: ProviderDeliveryRequest) -> ProviderDeliveryReceipt:
        """Read exact Core attachments, send them in order, and settle one receipt."""

        if not isinstance(request, ProviderDeliveryRequest):
            raise TypeError("Feishu deliver 只接受 ProviderDeliveryRequest")
        if request.binding_token != self._binding_token:
            raise RuntimeError("Feishu delivery binding token 不匹配")
        if not request.body.strip() and not request.attachments:
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error="Feishu 空消息被拒绝",
            )
        if self._client is None:
            raise RuntimeError("Feishu adapter 尚未 start")

        # 1. Resolve and verify every Core-owned attachment before any provider effect.
        try:
            attachment_data = await self._read_attachments(request.attachments)
        except asyncio.CancelledError:
            raise
        except (RuntimeError, TypeError, ValueError) as error:
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error=f"Feishu 附件读取失败: {error}",
            )

        # 2. Split before provider effect; every chunk retains the same delivery id.
        provider_ids: list[str] = []
        delivered_any = False
        if request.body.strip():
            for chunk in _split_markdown(request.body, _CARD_TEXT_LIMIT):
                status, provider_id, error = await self._send_one(
                    request.recipient,
                    "interactive",
                    build_markdown_card(chunk),
                )
                if status is DeliveryStatus.REJECTED:
                    # 2. Only a proven pre-effect card rejection permits the old text fallback.
                    status, provider_id, error = await self._send_one(
                        request.recipient,
                        "text",
                        json.dumps({"text": chunk}, ensure_ascii=False),
                    )
                if provider_id:
                    provider_ids.append(provider_id)
                if status is DeliveryStatus.DELIVERED:
                    delivered_any = True
                if status is not DeliveryStatus.DELIVERED:
                    if delivered_any and status is DeliveryStatus.REJECTED:
                        status = DeliveryStatus.UNKNOWN
                    return ProviderDeliveryReceipt(
                        request.delivery_id,
                        status,
                        tuple(provider_ids),
                        error=error,
                    )
        # 3. Upload and send attachments in the exact request order.
        for ref, data in attachment_data:
            status, provider_id, error = await self._send_attachment(
                request.recipient,
                ref,
                data,
            )
            if provider_id:
                provider_ids.append(provider_id)
            if status is DeliveryStatus.DELIVERED:
                delivered_any = True
            if status is not DeliveryStatus.DELIVERED:
                if delivered_any and status is DeliveryStatus.REJECTED:
                    status = DeliveryStatus.UNKNOWN
                return ProviderDeliveryReceipt(
                    request.delivery_id,
                    status,
                    tuple(provider_ids),
                    error=error,
                )
        return ProviderDeliveryReceipt(
            request.delivery_id,
            DeliveryStatus.DELIVERED,
            tuple(provider_ids),
        )

    async def _read_attachments(
        self,
        refs: tuple[AttachmentRef, ...],
    ) -> list[tuple[AttachmentRef, bytes]]:
        """Read and hash-check Core attachments while retaining no path access."""

        if not refs:
            return []
        if len(refs) > _MAX_ATTACHMENT_COUNT:
            raise ValueError("Feishu 附件数量超过上限")
        declared_total = sum(ref.size_bytes for ref in refs)
        if declared_total > _MAX_ATTACHMENT_BATCH_BYTES:
            raise ValueError("Feishu 附件批次超过总大小上限")
        attachment_read = self._context.attachment_read
        if attachment_read is None:
            raise RuntimeError("Feishu outbound 附件缺少 Core attachment_read")
        result: list[tuple[AttachmentRef, bytes]] = []
        actual_total = 0
        for ref in refs:
            lease = await attachment_read.acquire(ref)
            try:
                if lease.ref != ref:
                    raise RuntimeError("Feishu attachment read lease ref 不匹配")
                data = await lease.read_bytes(
                    max_bytes=min(max(ref.size_bytes, 1), _MAX_ATTACHMENT_BYTES)
                )
                if len(data) != ref.size_bytes:
                    raise ValueError(
                        f"附件大小不匹配: expected={ref.size_bytes} actual={len(data)}"
                    )
                if hashlib.sha256(data).hexdigest() != ref.sha256:
                    raise ValueError("附件 sha256 不匹配")
                actual_total += len(data)
                if actual_total > _MAX_ATTACHMENT_BATCH_BYTES:
                    raise ValueError("Feishu 附件批次超过总大小上限")
                result.append((ref, data))
            finally:
                await _close_attachment_lease(lease)
        return result

    async def _send_attachment(
        self,
        recipient: str,
        ref: AttachmentRef,
        data: bytes,
    ) -> tuple[DeliveryStatus, str | None, str | None]:
        """Upload one verified attachment then send its provider message."""

        try:
            provider_key = await self._upload_attachment(ref, data)
        except asyncio.CancelledError:
            raise
        except ValueError as error:
            return DeliveryStatus.REJECTED, None, str(error)
        except Exception as error:
            return _feishu_error_status(error), None, str(error) or type(error).__name__
        try:
            payload = await self._post_message_once(
                recipient,
                "image" if ref.kind is AttachmentKind.IMAGE else "file",
                json.dumps(
                    {
                        "image_key" if ref.kind is AttachmentKind.IMAGE else "file_key": provider_key
                    },
                    ensure_ascii=False,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # The upload already changed provider state; a send failure is not a
            # deterministic no-effect rejection even when the HTTP status is 4xx.
            return DeliveryStatus.UNKNOWN, None, str(error) or type(error).__name__
        provider_id = str(payload.get("message_id") or "").strip()
        if not provider_id:
            return DeliveryStatus.UNKNOWN, None, "Feishu response 缺少 message_id"
        try:
            _provider_segment(provider_id, "Feishu message_id")
        except ValueError as error:
            return DeliveryStatus.UNKNOWN, None, str(error)
        return DeliveryStatus.DELIVERED, provider_id, None

    async def _upload_attachment(self, ref: AttachmentRef, data: bytes) -> str:
        """Upload verified bytes using the provider media endpoint."""

        if ref.kind is AttachmentKind.IMAGE:
            provider_key = await self._upload_image(data)
        else:
            provider_key = await self._upload_file(data, ref.filename or "attachment")
        if not provider_key:
            raise RuntimeError("Feishu media upload response 缺少 provider key")
        _provider_segment(provider_key, "Feishu file_key")
        return provider_key

    async def stop(self) -> StopReceipt:
        """Close stream, websocket, tasks, HTTP client, and provider client exactly once."""

        if (
            self._stopping
            and self._stream_subscription is None
            and self._ws_client is None
            and self._ws_thread is None
            and self._client is None
            and self._provider_client is None
        ):
            self._clear_transient_state()
            return StopReceipt(self._binding_token, resources_closed=True)
        self._stopping = True
        failures: list[ChannelCleanupFailure] = []

        # 1. Close Core callback admission and drain accepted callbacks first.
        subscription = self._stream_subscription
        if subscription is not None:
            try:
                subscription.close_admission()
                await subscription.await_quiescence()
                await subscription.close()
                self._stream_subscription = None
            except BaseException as error:
                failures.append(self._cleanup_failure("turn-stream", error))

        # 2. Stop the provider receive loop before closing its HTTP resources.
        self._ws_stopped.set()
        self._admission_open = False
        try:
            await self._disconnect_ws()
            self._ws_client = None
            self._ws_loop = None
        except BaseException as error:
            failures.append(self._cleanup_failure("websocket-disconnect", error))
        thread = self._ws_thread
        if thread is not None and self._ws_thread_started:
            try:
                await asyncio.to_thread(thread.join, _WS_STOP_TIMEOUT_S)
                if thread.is_alive():
                    raise RuntimeError("飞书长连接线程停止超时")
                self._ws_thread = None
                self._ws_thread_started = False
            except BaseException as error:
                failures.append(self._cleanup_failure("websocket-thread", error))
        elif thread is not None:
            self._ws_thread = None
            self._ws_thread_started = False
        self._remove_sdk_shutdown_filter()

        # 3. Complete in-process callback cleanup before returning the receipt.
        tasks = tuple(self._inbound_tasks)
        try:
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        failures.append(self._cleanup_failure("inbound-task", result))
        finally:
            self._inbound_tasks.clear()
            self._clear_transient_state()

        # 4. Release adapter-owned formal resources; Core closes the factory separately.
        if self._client is not None:
            try:
                await self._client.aclose()
            except BaseException as error:
                failures.append(self._cleanup_failure("http-client", error))
            else:
                self._client = None
        if self._provider_client is not None:
            try:
                await self._provider_client.aclose()
            except BaseException as error:
                failures.append(self._cleanup_failure("provider-client", error))
            else:
                self._provider_client = None

        self._app_id = None
        self._app_secret = None
        self._token = None
        closed = not failures
        if closed:
            self._ws_client = None
            self._ws_loop = None
            self._ws_thread = None
            self._ws_thread_started = False
            self._stream_subscription = None
            self._started = False
            self._stopping = False
            logger.info("[feishu] v3 channel stopped binding=%s", self._binding_token)
        return StopReceipt(self._binding_token, closed, tuple(failures))

    # ------------------------------------------------------------------
    # Formal provider receive loop

    def _run_ws_client(self) -> None:
        """Run the SDK receive loop in its own thread without Core state access."""

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._ws_loop = loop
        loop.set_exception_handler(self._handle_ws_loop_exception)
        try:
            while not self._ws_stopped.is_set():
                try:
                    client = self._build_ws_client()
                    if self._ws_stopped.is_set():
                        break
                    client.start()
                except Exception as error:
                    if self._ws_stopped.is_set():
                        break
                    logger.warning("[feishu] 长连接退出，准备重连: %s", error)
                if self._ws_stopped.is_set():
                    break
                time.sleep(_WS_RECONNECT_DELAY_S)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def _build_ws_client(self) -> Any:
        """Create the Feishu SDK socket only after formal credential admission."""

        app_id = self._read_credential("app_id")
        app_secret = self._read_credential("app_secret")
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"^pkg_resources is deprecated as an API\.",
                category=UserWarning,
            )
            from lark_oapi.core.enum import LogLevel  # pyright: ignore[reportMissingImports]
            from lark_oapi.core.log import logger as sdk_logger  # pyright: ignore[reportMissingImports]
            from lark_oapi.event.dispatcher_handler import EventDispatcherHandler  # pyright: ignore[reportMissingImports]
            from lark_oapi.ws import Client as WsClient  # pyright: ignore[reportMissingImports]

        self._remove_sdk_shutdown_filter()
        shutdown_filter = _SdkShutdownLogFilter(self._ws_stopped)
        sdk_logger.addFilter(shutdown_filter)
        self._sdk_logger = sdk_logger
        self._sdk_shutdown_filter = shutdown_filter
        handler = (
            EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._on_sdk_message)
            .build()
        )
        client = WsClient(
            app_id,
            app_secret,
            log_level=LogLevel.INFO,
            event_handler=handler,
            domain=self._domain,
            auto_reconnect=False,
        )
        self._ws_client = client
        return client

    def _handle_ws_loop_exception(
        self,
        loop: asyncio.AbstractEventLoop,
        context: dict[str, Any],
    ) -> None:
        if not self._ws_stopped.is_set():
            loop.default_exception_handler(context)

    def _remove_sdk_shutdown_filter(self) -> None:
        if self._sdk_logger is not None and self._sdk_shutdown_filter is not None:
            self._sdk_logger.removeFilter(self._sdk_shutdown_filter)
        self._sdk_logger = None
        self._sdk_shutdown_filter = None

    async def _disconnect_ws(self) -> None:
        client = self._ws_client
        loop = self._ws_loop
        if client is None:
            return
        if loop is None:
            raise RuntimeError("飞书长连接缺少事件循环")
        disconnect = getattr(client, "_disconnect", None)
        if not callable(disconnect):
            raise RuntimeError("飞书 SDK 不支持主动断开长连接")
        disconnect_coro = cast(Coroutine[Any, Any, Any], disconnect())
        future = asyncio.run_coroutine_threadsafe(disconnect_coro, loop)
        try:
            await asyncio.wait_for(asyncio.wrap_future(future), _WS_STOP_TIMEOUT_S)
        finally:
            loop.call_soon_threadsafe(loop.stop)

    def _on_sdk_message(self, event: Any) -> None:
        loop = self._loop
        if loop is None or self._ws_stopped.is_set() or not self._admission_open:
            return
        loop.call_soon_threadsafe(self._start_inbound_task, event)

    def _start_inbound_task(self, event: Any) -> None:
        if self._ws_stopped.is_set() or not self._admission_open:
            return
        task = asyncio.create_task(
            self._handle_message_event(event),
            name="feishu-inbound",
        )
        self._inbound_tasks.add(task)
        task.add_done_callback(self._inbound_tasks.discard)

    async def _handle_message_event(self, event: Any) -> DeliveryStatus | None:
        """Project one SDK event into text ingress or an exact Core control port."""

        if not self._admission_open:
            logger.warning("[feishu] Core admission 尚未打开，拒绝 provider 入站")
            return DeliveryStatus.REJECTED

        data = getattr(event, "event", None)
        message = getattr(data, "message", None)
        sender = getattr(data, "sender", None)
        if message is None or sender is None:
            return None
        if str(getattr(message, "chat_type", "") or "") != "p2p":
            return None
        message_id = str(getattr(message, "message_id", "") or "").strip()
        if not message_id:
            logger.warning("[feishu] 丢弃缺少 provider message id 的事件")
            return DeliveryStatus.REJECTED
        try:
            _provider_segment(message_id, "Feishu message_id")
        except ValueError as error:
            logger.warning("[feishu] 丢弃非法 provider message id: %s", error)
            return DeliveryStatus.REJECTED
        sender_id = getattr(sender, "sender_id", None)
        open_id = str(getattr(sender_id, "open_id", "") or "").strip()
        user_id = str(getattr(sender_id, "user_id", "") or "").strip()
        union_id = str(getattr(sender_id, "union_id", "") or "").strip()
        identities = {open_id, user_id, union_id} - {""}
        if not self._allow_from or not identities.intersection(self._allow_from):
            logger.warning("[feishu] 拒绝未授权私聊用户 open_id=%s", open_id)
            return DeliveryStatus.REJECTED
        chat_id = str(getattr(message, "chat_id", "") or "").strip()
        if not chat_id:
            return DeliveryStatus.REJECTED
        return await self._ingest_message(
            message,
            message_id,
            chat_id,
            open_id,
            user_id,
            union_id,
        )

    async def _ingest_message(
        self,
        message: Any,
        message_id: str,
        chat_id: str,
        open_id: str,
        user_id: str,
        union_id: str,
    ) -> DeliveryStatus:
        """Download provider media into Core artifacts before one ingress admission."""

        # 1. Decide /stop before touching provider media. An attachment-bearing
        # control message is rejected without creating a Core artifact.
        stop_candidate, has_media = _stop_candidate(message)
        if stop_candidate.strip() == "/stop":
            if has_media:
                logger.warning(
                    "[feishu] 拒绝带附件的 /stop message_id=%s",
                    message_id,
                )
                return DeliveryStatus.REJECTED
            sender = open_id or user_id or union_id
            if not sender:
                return DeliveryStatus.REJECTED
            raw = RawInbound(
                message_id=message_id,
                message=ChannelInboundMessage(
                    channel=_CHANNEL,
                    sender=sender,
                    chat_id=chat_id,
                    content="/stop",
                    timestamp=_message_timestamp(message),
                    metadata={
                        "chat_type": "private",
                        "provider_message_id": message_id,
                        "open_id": open_id,
                        "user_id": user_id,
                        "union_id": union_id,
                    },
                ),
                provider_identity=sender,
                recipient=chat_id,
            )
            return await self._interrupt(raw)

        # 2. Download and import only ordinary inbound messages.
        try:
            content, attachments = await self._extract_inbound_payload(message, message_id)
        except asyncio.CancelledError:
            raise
        except (
            httpx.HTTPStatusError,
            httpx.RequestError,
            FeishuApiError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as error:
            logger.warning(
                "[feishu] 入站附件未能导入 message_id=%s err=%s",
                message_id,
                error,
            )
            return DeliveryStatus.REJECTED
        if not content and not attachments:
            return DeliveryStatus.REJECTED
        sender = open_id or user_id or union_id
        if not sender:
            return DeliveryStatus.REJECTED
        content = _visible_text(content)
        is_stop = content.strip() == "/stop" and not attachments
        if is_stop:
            # Control messages must not fetch or merge reply context first.
            inbound_text = content.strip()
            reply_meta: dict[str, str] = {}
        else:
            inbound_text, reply_meta = await self._merge_reply_context(message, content)
            inbound_text = _visible_text(inbound_text)
        raw_message = ChannelInboundMessage(
            channel=_CHANNEL,
            sender=sender,
            chat_id=chat_id,
            content=inbound_text,
            timestamp=_message_timestamp(message),
            metadata={
                "chat_type": "private",
                "provider_message_id": message_id,
                "open_id": open_id,
                "user_id": user_id,
                "union_id": union_id,
                **reply_meta,
            },
            attachments=tuple(attachments),
        )
        raw = RawInbound(
            message_id=message_id,
            message=raw_message,
            provider_identity=sender,
            recipient=chat_id,
        )
        if is_stop:
            return await self._interrupt(raw)
        if self._ingress is None:
            raise RuntimeError("Feishu ingress port 未绑定")
        accepted = await self._ingress.admit(raw)
        if accepted:
            self._inbound_recipients[message_id] = chat_id
            return DeliveryStatus.DELIVERED
        return DeliveryStatus.REJECTED

    async def _extract_inbound_payload(
        self,
        message: Any,
        message_id: str,
    ) -> tuple[str, list[AttachmentRef]]:
        """Translate Feishu text, image, file, or post content into Core values."""

        message_type = str(getattr(message, "message_type", "") or "")
        content_raw = str(getattr(message, "content", "") or "")
        if message_type == "text":
            return _extract_text(content_raw), []
        if message_type == "image":
            image_key = _extract_key(content_raw, "image_key")
            data = await self._download_resource_bytes(message_id, image_key, "image")
            _check_inbound_batch((data,))
            return "[图片]", [
                await self._import_inbound_attachment(
                    data,
                    kind=AttachmentKind.IMAGE,
                    filename="image.jpg",
                    media_type="image/jpeg",
                )
            ]
        if message_type == "file":
            file_name = _extract_key(content_raw, "file_name") or "file"
            file_key = _extract_key(content_raw, "file_key")
            data = await self._download_resource_bytes(message_id, file_key, "file")
            _check_inbound_batch((data,))
            media_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
            return f"[文件: {file_name}]", [
                await self._import_inbound_attachment(
                    data,
                    kind=AttachmentKind.FILE,
                    filename=file_name,
                    media_type=media_type,
                )
            ]
        if message_type == "post":
            text, image_keys = _extract_post(content_raw)
            if len(image_keys) > _MAX_ATTACHMENT_COUNT:
                raise ValueError("Feishu 入站附件数量超过上限")
            attachments: list[AttachmentRef] = []
            downloaded: list[bytes] = []
            for index, image_key in enumerate(image_keys, start=1):
                data = await self._download_resource_bytes(message_id, image_key, "image")
                downloaded.append(data)
            _check_inbound_batch(downloaded)
            for index, data in enumerate(downloaded, start=1):
                attachments.append(
                    await self._import_inbound_attachment(
                        data,
                        kind=AttachmentKind.IMAGE,
                        filename=f"image-{index}.jpg",
                        media_type="image/jpeg",
                    )
                )
            return text or "[富文本]", attachments
        logger.debug("[feishu] 暂不支持的消息类型 msg_type=%s", message_type)
        return "", []

    async def _download_resource_bytes(
        self,
        message_id: str,
        file_key: str,
        resource_type: str,
    ) -> bytes:
        """Download one provider resource with a fixed memory bound."""

        _provider_segment(message_id, "Feishu message_id")
        _provider_segment(file_key, "Feishu file_key")
        if self._client is None:
            raise RuntimeError("Feishu HTTP client 尚未 start")
        token = await self._get_access_token()
        async with self._client.stream(
            "GET",
            f"{self._domain}/open-apis/im/v1/messages/{message_id}/resources/{file_key}",
            params={"type": resource_type},
            headers={"Authorization": f"Bearer {token}"},
            follow_redirects=False,
        ) as response:
            response.raise_for_status()
            return await _bounded_response_bytes(response)

    async def _import_inbound_attachment(
        self,
        data: bytes,
        *,
        kind: AttachmentKind,
        filename: str,
        media_type: str,
    ) -> AttachmentRef:
        """Import provider bytes through the exact Core attachment port."""

        attachment_import = self._context.attachment_import
        if attachment_import is None:
            raise RuntimeError("Feishu 入站附件缺少 Core attachment_import")
        ref = await attachment_import.import_bytes(
            data,
            kind=kind,
            filename=filename,
            media_type=media_type,
        )
        if not isinstance(ref, AttachmentRef):
            raise TypeError("Feishu attachment_import 必须返回 AttachmentRef")
        return ref

    async def _interrupt(self, raw: RawInbound) -> DeliveryStatus:
        """Delegate /stop to Core's exact control facade; never call an old controller."""

        if self._presentation is None or self._presentation.control is None:
            raise RuntimeError("Feishu control port 未绑定")
        result = await self._presentation.control.interrupt(
            raw,
            response_bodies=ControlResponseBodies(
                interrupted="已停止当前任务。",
                idle="当前没有正在运行的任务。",
            ),
        )
        if result.response is None:
            return DeliveryStatus.REJECTED
        return result.response.status

    async def _merge_reply_context(
        self,
        message: Any,
        text: str,
    ) -> tuple[str, dict[str, str]]:
        parent_id = str(getattr(message, "parent_id", "") or "").strip()
        if not parent_id:
            return text, {}
        parent_text = await self._fetch_message_text(parent_id)
        if not parent_text:
            return text, {"reply_to_message_id": parent_id}
        return (
            (
                "【你正在回复一条历史消息】\n"
                f"被回复消息：\n{parent_text}\n\n"
                "【你当前新消息】\n"
                f"{text}"
            ).strip(),
            {"reply_to_message_id": parent_id},
        )

    # ------------------------------------------------------------------
    # Typed turn stream and remote preview

    async def _on_turn_stream(self, event: TurnStreamEvent) -> PresentationReceipt:
        """Render one typed event into the same Feishu preview artifact."""

        if event.presentation_id in self._failed_presentations:
            receipt = self._presentation_receipt(
                event,
                DeliveryStatus.UNKNOWN,
                "preview 已终止",
            )
            if event.kind is TurnStreamEventKind.TURN_OUTPUT_COMPLETED:
                self._clear_presentation(event.presentation_id, _turn_id(event))
            return receipt
        if event.presentation_id in self._rejected_presentations:
            receipt = self._presentation_receipt(
                event,
                DeliveryStatus.REJECTED,
                "preview 已拒绝",
            )
            if event.kind is TurnStreamEventKind.TURN_OUTPUT_COMPLETED:
                self._clear_presentation(event.presentation_id, _turn_id(event))
            return receipt
        try:
            if event.kind is TurnStreamEventKind.TURN_STARTED:
                payload = cast(TurnStartedPresentation, event.payload)
                recipient = self._inbound_recipients.get(payload.client_message_id)
                if recipient is None and self._identity is not None:
                    recipient = self._identity.resolve(payload.client_message_id)
                if not recipient:
                    return self._reject_presentation(
                        event,
                        "turn.started 缺少已接受 inbound recipient",
                    )
                self._turn_recipients[payload.turn_id] = recipient
                self._presentation_client_messages[event.presentation_id] = (
                    payload.client_message_id
                )
                self._reply_buffers[event.presentation_id] = ""
                self._thinking_buffers[event.presentation_id] = ""
                self._tool_lines[event.presentation_id] = []
                return await self._sync_preview(event, recipient, live=True)

            recipient = self._turn_recipients.get(_turn_id(event))
            if not recipient:
                return self._reject_presentation(event, "turn stream 缺少 turn.started")
            if event.kind is TurnStreamEventKind.STREAM_DELTA:
                payload = cast(StreamDeltaPresentation, event.payload)
                self._reply_buffers[event.presentation_id] = (
                    self._reply_buffers.get(event.presentation_id, "") + payload.text_delta
                )
                self._thinking_buffers[event.presentation_id] = (
                    self._thinking_buffers.get(event.presentation_id, "")
                    + payload.reasoning_delta
                )
                return await self._sync_preview(event, recipient, live=True)
            if event.kind in {
                TurnStreamEventKind.TOOL_STARTED,
                TurnStreamEventKind.TOOL_COMPLETED,
            }:
                payload = cast(ToolPresentation, event.payload)
                lines = self._tool_lines.setdefault(event.presentation_id, [])
                line = next((item for item in lines if item.call_id == payload.tool_call_id), None)
                if line is None:
                    line = ToolLiveLine(
                        call_id=payload.tool_call_id,
                        tool_name=payload.tool_name,
                        intent="",
                        target="",
                    )
                    lines.append(line)
                line.status = "running" if event.kind is TurnStreamEventKind.TOOL_STARTED else "done"
                return await self._sync_preview(event, recipient, live=True)

            payload = cast(TurnOutputCompletedPresentation, event.payload)
            try:
                return await self._sync_preview(event, recipient, live=False)
            finally:
                self._clear_presentation(event.presentation_id, payload.turn_id)
        except asyncio.CancelledError:
            self._clear_presentation(event.presentation_id, _turn_id(event))
            raise
        except Exception as error:
            self._failed_presentations.add(event.presentation_id)
            logger.warning(
                "[feishu] preview failed presentation=%s err=%s",
                event.presentation_id,
                error,
            )
            return self._presentation_receipt(event, DeliveryStatus.UNKNOWN, str(error))

    async def _sync_preview(
        self,
        event: TurnStreamEvent,
        recipient: str,
        *,
        live: bool,
    ) -> PresentationReceipt:
        card = (
            build_live_card(
                self._thinking_buffers.get(event.presentation_id, ""),
                self._tool_lines.get(event.presentation_id, []),
                self._reply_buffers.get(event.presentation_id, ""),
            )
            if live
            else build_summary_card(
                self._thinking_buffers.get(event.presentation_id, ""),
                self._tool_lines.get(event.presentation_id, []),
            )
        )
        message_id = self._preview_messages.get(event.presentation_id)
        if message_id is None:
            status, provider_id, error = await self._send_one(
                recipient,
                "interactive",
                card,
            )
            if status is DeliveryStatus.DELIVERED and provider_id:
                self._preview_messages[event.presentation_id] = provider_id
            elif status is DeliveryStatus.UNKNOWN:
                self._failed_presentations.add(event.presentation_id)
            elif status is DeliveryStatus.REJECTED:
                self._rejected_presentations.add(event.presentation_id)
            return self._presentation_receipt(event, status, error, provider_id)

        status, error = await self._patch_one(message_id, card)
        if status is DeliveryStatus.UNKNOWN:
            self._failed_presentations.add(event.presentation_id)
        elif status is DeliveryStatus.REJECTED:
            self._rejected_presentations.add(event.presentation_id)
        return self._presentation_receipt(event, status, error, message_id)

    def _presentation_receipt(
        self,
        event: TurnStreamEvent,
        status: DeliveryStatus,
        error: str | None = None,
        provider_id: str | None = None,
    ) -> PresentationReceipt:
        return PresentationReceipt(
            presentation_id=event.presentation_id,
            status=status,
            provider_ids=(provider_id,) if provider_id else (),
            error=error,
        )

    def _reject_presentation(self, event: TurnStreamEvent, error: str) -> PresentationReceipt:
        self._rejected_presentations.add(event.presentation_id)
        return self._presentation_receipt(event, DeliveryStatus.REJECTED, error)

    def _clear_presentation(self, presentation_id: str, turn_id: str) -> None:
        """Release one completed preview and its temporary inbound binding."""

        client_message_id = self._presentation_client_messages.pop(
            presentation_id,
            None,
        )
        if client_message_id is not None:
            self._inbound_recipients.pop(client_message_id, None)
        self._turn_recipients.pop(turn_id, None)
        self._reply_buffers.pop(presentation_id, None)
        self._thinking_buffers.pop(presentation_id, None)
        self._tool_lines.pop(presentation_id, None)
        self._preview_messages.pop(presentation_id, None)
        self._failed_presentations.discard(presentation_id)
        self._rejected_presentations.discard(presentation_id)

    def _clear_transient_state(self) -> None:
        """Release all in-memory inbound and preview state during channel stop."""

        self._inbound_recipients.clear()
        self._turn_recipients.clear()
        self._presentation_client_messages.clear()
        self._reply_buffers.clear()
        self._thinking_buffers.clear()
        self._tool_lines.clear()
        self._preview_messages.clear()
        self._failed_presentations.clear()
        self._rejected_presentations.clear()

    # ------------------------------------------------------------------
    # REST and delivery classifier

    async def _send_one(
        self,
        recipient: str,
        message_type: str,
        content: str,
    ) -> tuple[DeliveryStatus, str | None, str | None]:
        try:
            payload = await self._post_message_once(recipient, message_type, content)
        except asyncio.CancelledError:
            raise
        except ValueError as error:
            return DeliveryStatus.REJECTED, None, str(error)
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status in _REJECTED_HTTP_STATUSES:
                return DeliveryStatus.REJECTED, None, f"HTTP {status}"
            return DeliveryStatus.UNKNOWN, None, f"HTTP {status}"
        except FeishuApiError as error:
            if error.code in _RATE_LIMIT_CODES:
                return DeliveryStatus.UNKNOWN, None, str(error)
            return DeliveryStatus.REJECTED, None, str(error)
        except httpx.RequestError as error:
            return _feishu_error_status(error), None, str(error) or type(error).__name__
        except Exception as error:
            return DeliveryStatus.UNKNOWN, None, str(error) or type(error).__name__
        provider_id = str(payload.get("message_id") or "").strip()
        if not provider_id:
            return DeliveryStatus.UNKNOWN, None, "Feishu response 缺少 message_id"
        try:
            _provider_segment(provider_id, "Feishu message_id")
        except ValueError as error:
            return DeliveryStatus.UNKNOWN, None, str(error)
        return DeliveryStatus.DELIVERED, provider_id, None

    async def _patch_one(
        self,
        message_id: str,
        content: str,
    ) -> tuple[DeliveryStatus, str | None]:
        try:
            _provider_segment(message_id, "Feishu message_id")
            await self._patch_message_once(message_id, content)
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status in _REJECTED_HTTP_STATUSES:
                return DeliveryStatus.REJECTED, f"HTTP {status}"
            return DeliveryStatus.UNKNOWN, f"HTTP {status}"
        except FeishuApiError as error:
            if error.code in _RATE_LIMIT_CODES:
                return DeliveryStatus.UNKNOWN, str(error)
            return DeliveryStatus.REJECTED, str(error)
        except Exception as error:
            return DeliveryStatus.UNKNOWN, str(error) or type(error).__name__
        return DeliveryStatus.DELIVERED, None

    async def _post_message_once(
        self,
        recipient: str,
        message_type: str,
        content: str,
    ) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Feishu HTTP client 尚未 start")
        receive_id, receive_id_type = self._resolve_receive(recipient)
        token = await self._get_access_token()
        response = await self._client.post(
            f"{self._domain}/open-apis/im/v1/messages",
            params={"receive_id_type": receive_id_type},
            headers={"Authorization": f"Bearer {token}"},
            json={
                "receive_id": receive_id,
                "msg_type": message_type,
                "content": content,
            },
        )
        return self._check_response(response)

    async def _upload_image(self, data: bytes) -> str:
        """Upload one Core-owned image and return Feishu's opaque image key."""

        if self._client is None:
            raise RuntimeError("Feishu HTTP client 尚未 start")
        token = await self._get_access_token()
        response = await self._client.post(
            f"{self._domain}/open-apis/im/v1/images",
            headers={"Authorization": f"Bearer {token}"},
            data={"image_type": "message"},
            files={"image": ("image", data)},
        )
        payload = self._check_response(response)
        return str(payload.get("image_key") or "").strip()

    async def _upload_file(self, data: bytes, filename: str) -> str:
        """Upload one Core-owned file and return Feishu's opaque file key."""

        if self._client is None:
            raise RuntimeError("Feishu HTTP client 尚未 start")
        token = await self._get_access_token()
        response = await self._client.post(
            f"{self._domain}/open-apis/im/v1/files",
            headers={"Authorization": f"Bearer {token}"},
            data={"file_type": "stream", "file_name": filename},
            files={"file": (filename, data)},
        )
        payload = self._check_response(response)
        return str(payload.get("file_key") or "").strip()

    async def _patch_message_once(self, message_id: str, content: str) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Feishu HTTP client 尚未 start")
        token = await self._get_access_token()
        response = await self._client.patch(
            f"{self._domain}/open-apis/im/v1/messages/{message_id}",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": content},
        )
        return self._check_response(response)

    async def _fetch_message_text(self, message_id: str) -> str:
        if self._client is None:
            return ""
        try:
            _provider_segment(message_id, "Feishu message_id")
            token = await self._get_access_token()
            response = await self._client.get(
                f"{self._domain}/open-apis/im/v1/messages/{message_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            payload = self._check_response(response)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.debug("[feishu] 拉取父消息失败 id=%s err=%s", message_id, error)
            return ""
        items = payload.get("items")
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            return ""
        body = items[0].get("body")
        if not isinstance(body, dict):
            return ""
        return _extract_text(str(body.get("content") or ""))

    def _resolve_receive(self, recipient: str) -> tuple[str, str]:
        value = recipient.strip()
        if value.startswith(f"{_CHANNEL}:"):
            value = value[len(_CHANNEL) + 1 :]
        _provider_segment(value, "Feishu recipient")
        if value.startswith("oc_"):
            return value, "chat_id"
        if self._identity is not None:
            resolved = self._identity.resolve(value)
            if resolved:
                _provider_segment(resolved, "Feishu resolved recipient")
                return resolved, "chat_id"
        if value.startswith("ou_"):
            return value, "open_id"
        if value.startswith("on_"):
            return value, "union_id"
        return value, "chat_id"

    async def _get_access_token(self) -> str:
        if self._client is None or self._provider_client is None:
            raise RuntimeError("Feishu formal credentials/client 未就绪")
        if self._token is not None and self._token.expires_at > time.time() + 60:
            return self._token.token
        app_id = self._read_credential("app_id")
        app_secret = self._read_credential("app_secret")
        response = await self._client.post(
            f"{self._domain}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": app_secret},
        )
        payload = self._check_response(response)
        token = str(payload.get("tenant_access_token") or "").strip()
        expire = int(payload.get("expire") or 0)
        if not token or expire <= 0:
            raise RuntimeError("飞书 token response 缺少有效 token/expire")
        self._token = _TokenCache(token, time.time() + expire)
        return token

    def _check_response(self, response: httpx.Response) -> dict[str, Any]:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Feishu response 必须是 object")
        code = int(payload.get("code") or 0)
        if code != 0:
            raise FeishuApiError(code, str(payload.get("msg") or ""))
        data = payload.get("data")
        return cast(dict[str, Any], data) if isinstance(data, dict) else payload

    # ------------------------------------------------------------------
    # Resource helpers

    def _read_credential(self, name: str) -> str:
        if self._provider_client is None:
            raise RuntimeError("Feishu provider client 尚未创建")
        matches = [
            (path, ref)
            for path, ref in self._credentials.items()
            if path in _CREDENTIAL_ALIASES[name]
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"Feishu credential {name} 必须恰好有一个 physical alias"
            )
        _, ref = matches[0]
        value = self._provider_client.credential(ref)
        if not isinstance(value, str) or not value:
            raise RuntimeError(f"Feishu credential {name} 为空")
        return value

    def _cleanup_failure(self, resource: str, error: BaseException) -> ChannelCleanupFailure:
        return ChannelCleanupFailure(
            stage="channel-stop",
            plugin_id=_CHANNEL,
            generation_id=self._context.generation_id,
            binding_token=self._binding_token,
            resource=resource,
            error_type=type(error).__name__,
            message=str(error) or type(error).__name__,
            retry_action="retry_generation_cleanup",
        )

    async def _close_resources_after_start_failure(self) -> tuple[BaseException, ...]:
        """Release start-owned resources while retaining every failed owner for retry."""

        failures: list[BaseException] = []
        self._ws_stopped.set()
        try:
            await self._disconnect_ws()
        except BaseException as error:
            failures.append(error)
        else:
            self._ws_client = None
            self._ws_loop = None
        thread = self._ws_thread
        if thread is not None and self._ws_thread_started:
            try:
                await asyncio.to_thread(thread.join, _WS_STOP_TIMEOUT_S)
                if thread.is_alive():
                    raise RuntimeError("飞书长连接线程停止超时")
            except BaseException as error:
                failures.append(error)
            else:
                self._ws_thread = None
                self._ws_thread_started = False
        elif thread is not None:
            self._ws_thread = None
            self._ws_thread_started = False
        self._remove_sdk_shutdown_filter()
        subscription = self._stream_subscription
        if subscription is not None:
            try:
                subscription.close_admission()
                await subscription.await_quiescence()
                await subscription.close()
            except BaseException as error:
                failures.append(error)
            else:
                self._stream_subscription = None
        client = self._client
        if client is not None:
            try:
                await client.aclose()
            except BaseException as error:
                failures.append(error)
            else:
                self._client = None
        provider_client = self._provider_client
        if provider_client is not None:
            try:
                await provider_client.aclose()
            except BaseException as error:
                failures.append(error)
            else:
                self._provider_client = None
        if failures:
            self._stopping = True
        self._app_id = None
        self._app_secret = None
        self._token = None
        return tuple(failures)


def _domain(config: Mapping[str, object]) -> str:
    value = config.get("domain", "https://open.feishu.cn")
    if not isinstance(value, str) or not value.strip():
        return "https://open.feishu.cn"
    return value.rstrip("/")


def _allow_from(config: Mapping[str, object]) -> frozenset[str]:
    value = config.get("allow_from", config.get("allowFrom", ()))
    if isinstance(value, str):
        return frozenset({value}) if value.strip() else frozenset()
    if not isinstance(value, (tuple, list)):
        return frozenset()
    return frozenset(item.strip() for item in value if isinstance(item, str) and item.strip())


def _provider_segment(value: object, field_name: str) -> str:
    """Validate an opaque provider value before putting it in a path or request."""

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} 不能为空")
    if value != value.strip():
        raise ValueError(f"{field_name} 不能包含首尾空白")
    if len(value) > _MAX_PROVIDER_SEGMENT_LENGTH:
        raise ValueError(f"{field_name} 超过长度上限")
    if "/" in value or "\\" in value:
        raise ValueError(f"{field_name} 不能包含路径分隔符")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field_name} 不能包含控制字符")
    return value


def _check_inbound_batch(data: tuple[bytes, ...] | list[bytes]) -> None:
    """Enforce inbound attachment count and aggregate byte limits before import."""

    if len(data) > _MAX_ATTACHMENT_COUNT:
        raise ValueError("Feishu 入站附件数量超过上限")
    total = 0
    for item in data:
        if not isinstance(item, bytes):
            raise TypeError("Feishu 入站附件必须是 bytes")
        total += len(item)
    if total > _MAX_ATTACHMENT_BATCH_BYTES:
        raise ValueError("Feishu 入站附件批次超过总大小上限")


def _stop_candidate(message: Any) -> tuple[str, bool]:
    """Extract control text and media presence without downloading provider bytes."""

    message_type = str(getattr(message, "message_type", "") or "")
    content = str(getattr(message, "content", "") or "")
    if message_type == "text":
        return _extract_text(content), False
    if message_type in {"image", "file"}:
        return "", True
    if message_type == "post":
        text, image_keys = _extract_post(content)
        return text, bool(image_keys)
    return "", False


def _message_timestamp(message: Any) -> datetime:
    raw = getattr(message, "create_time", None)
    try:
        seconds = float(raw) / 1000.0 if raw not in (None, "") else 0.0
        if seconds > 0:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        pass
    return datetime.now(timezone.utc)


def _turn_id(event: TurnStreamEvent) -> str:
    payload = event.payload
    return cast(str, getattr(payload, "turn_id"))


def _extract_text(content: str) -> str:
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return content.strip()
    if not isinstance(parsed, dict):
        return content.strip()
    return str(parsed.get("text") or "").strip()


def _extract_key(content: str, key: str) -> str:
    """Read one string key from a Feishu message content object."""

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return ""
    if not isinstance(parsed, dict):
        return ""
    value = parsed.get(key)
    return value.strip() if isinstance(value, str) else ""


def _extract_post(content: str) -> tuple[str, list[str]]:
    """Flatten Feishu post paragraphs while collecting embedded image keys."""

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return content.strip(), []
    if not isinstance(parsed, dict):
        return "", []
    body = parsed
    if "content" not in body:
        for value in body.values():
            if isinstance(value, dict) and "content" in value:
                body = value
                break
    texts: list[str] = []
    images: list[str] = []
    title = body.get("title")
    if isinstance(title, str) and title.strip():
        texts.append(title.strip())
    paragraphs = body.get("content")
    if isinstance(paragraphs, list):
        for paragraph in paragraphs:
            if not isinstance(paragraph, list):
                continue
            parts: list[str] = []
            for segment in paragraph:
                if not isinstance(segment, dict):
                    continue
                tag = str(segment.get("tag") or "")
                if tag == "text":
                    parts.append(str(segment.get("text") or ""))
                elif tag in {"a", "link"}:
                    parts.append(str(segment.get("text") or segment.get("href") or ""))
                elif tag == "at":
                    parts.append("@" + str(segment.get("user_name") or segment.get("user_id") or ""))
                elif tag == "img":
                    image_key = segment.get("image_key")
                    if isinstance(image_key, str) and image_key.strip():
                        images.append(image_key.strip())
            line = "".join(parts).strip()
            if line:
                texts.append(line)
    return "\n".join(texts).strip(), images


def _visible_text(value: str) -> str:
    """Escape control characters into visible markers accepted by Core text DTOs."""

    pieces: list[str] = []
    for char in value:
        codepoint = ord(char)
        if codepoint >= 32:
            pieces.append(char)
        elif char == "\n":
            pieces.append(r"\n")
        elif char == "\r":
            pieces.append(r"\r")
        elif char == "\t":
            pieces.append(r"\t")
        else:
            pieces.append(f"\\x{codepoint:02x}")
    return "".join(pieces)


def _split_markdown(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines(keepends=True):
        if current and current_len + len(line) > limit:
            chunks.append("".join(current))
            current = []
            current_len = 0
        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        current.append(line)
        current_len += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


def _feishu_error_status(error: BaseException) -> DeliveryStatus:
    """Classify a provider error without treating transport failure as rejection."""

    if isinstance(error, httpx.HTTPStatusError):
        return (
            DeliveryStatus.REJECTED
            if error.response.status_code in _REJECTED_HTTP_STATUSES
            else DeliveryStatus.UNKNOWN
        )
    if isinstance(error, FeishuApiError):
        return (
            DeliveryStatus.UNKNOWN
            if error.code in _RATE_LIMIT_CODES
            else DeliveryStatus.REJECTED
        )
    if isinstance(error, httpx.RequestError) and _is_pre_effect_request_error(error):
        return DeliveryStatus.REJECTED
    return DeliveryStatus.UNKNOWN


def _is_pre_effect_request_error(error: httpx.RequestError) -> bool:
    """Classify connection setup failures that cannot have reached Feishu."""

    return isinstance(
        error,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ProxyError,
            httpx.UnsupportedProtocol,
            httpx.InvalidURL,
        ),
    )


async def _close_attachment_lease(lease: Any) -> None:
    """Finish attachment lease cleanup even when the caller is cancelled."""

    task = asyncio.create_task(lease.aclose(), name="feishu-attachment-lease-close")
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
    if task.cancelled():
        raise asyncio.CancelledError
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _bounded_response_bytes(response: Any) -> bytes:
    """Collect a provider response without exceeding the attachment memory bound."""

    chunks: list[bytes] = []
    total = 0
    aiter_bytes = getattr(response, "aiter_bytes", None)
    if not callable(aiter_bytes):
        raise TypeError("Feishu provider response 必须提供 aiter_bytes")
    aiter_bytes = cast(Callable[[], AsyncIterable[bytes]], aiter_bytes)
    async for chunk in aiter_bytes():
        if not isinstance(chunk, bytes):
            raise TypeError("Feishu provider response chunk 必须是 bytes")
        total += len(chunk)
        if total > _MAX_ATTACHMENT_BYTES:
            raise ValueError("Feishu 入站附件超过大小上限")
        chunks.append(chunk)
    return b"".join(chunks)
