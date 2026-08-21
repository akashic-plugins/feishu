# Feishu v3 channel

Feishu is a pure v3 `ChannelDefinition`/`ChannelAdapter` plugin. Core owns
inbound admission, identity mapping, `/stop`, turn-stream lifecycle, delivery
identity, and persistent state; this repository only translates Feishu's
provider protocol.

The v3 adapter accepts Core-owned image and file references for outbound delivery:
it verifies the exact reference and bounded bytes before Feishu upload, then
sends text and attachments in request order. Incoming image, file, and rich-post
media are downloaded through the provider API and imported into Core's artifact
store before one ingress admission. Provider effects use `DELIVERED`,
`REJECTED`, and `UNKNOWN` without exposing workspace paths.
The previous v2 installation and its data remain available for an explicit,
append-only migration; no v2 class or ABI is loaded by this artifact.
Feishu owns no plugin database or attachment store, so this migration has no
plugin-side copy/delete step: the existing `config.local.toml` remains the
formal source and Core owns identity/session data.

Formal startup is the only path that resolves `CredentialRef` through Core's
provider client factory and creates the HTTP/WebSocket client. Candidate
construction and validation never read formal credentials, create an SDK
client, or contact Feishu.
