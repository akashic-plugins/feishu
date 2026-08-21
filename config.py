from __future__ import annotations

from typing import Annotated

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from agent.plugin_composition import CredentialRef


class FeishuConfig(BaseModel):
    """Validate Feishu's redacted Core config projection."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        validate_by_alias=True,
        validate_by_name=False,
    )

    app_id: Annotated[
        CredentialRef | None,
        Field(validation_alias=AliasChoices("appId", "app_id")),
    ] = None
    app_secret: Annotated[
        CredentialRef | None,
        Field(validation_alias=AliasChoices("appSecret", "app_secret")),
    ] = None
    allow_from: Annotated[
        tuple[str, ...],
        Field(validation_alias=AliasChoices("allow_from", "allowFrom")),
    ] = ()
    domain: str = "https://open.feishu.cn"
