from __future__ import annotations

from agent.plugin_composition import (
    CHANNELS,
    ChannelCapability,
    ChannelDefinition,
    Context,
    InboundIdentity,
    PluginChannels,
)

from .channel import FeishuAdapter, build_feishu_channel
from .config import FeishuConfig


api_version = 3
name = "feishu"
version = "3.0.0"
desc = "飞书私聊 v3 channel adapter"
author = "Akashic"
inject = (CHANNELS,)
Config = FeishuConfig


async def apply(ctx: Context, config: FeishuConfig) -> None:
    """Register the immutable Feishu channel definition in the exact Root."""

    channels: PluginChannels = ctx.require(CHANNELS)
    await channels.register(
        ctx,
        ChannelDefinition(
            name="feishu",
            capabilities=frozenset(
                {
                    ChannelCapability.INBOUND,
                    ChannelCapability.OUTBOUND,
                    ChannelCapability.CONTROL,
                    ChannelCapability.TURN_STREAM,
                }
            ),
            factory_export="build_feishu_channel",
            inbound_identity=InboundIdentity.PROVIDER_MESSAGE_ID,
            credential_paths=("appId", "appSecret", "app_id", "app_secret"),
        ),
    )


__all__ = [
    "Config",
    "FeishuAdapter",
    "api_version",
    "apply",
    "author",
    "build_feishu_channel",
    "desc",
    "inject",
    "name",
    "version",
]
