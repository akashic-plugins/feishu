# Feishu v3 channel

Feishu is a pure v3 `ChannelDefinition`/`ChannelAdapter` plugin. Core owns
inbound admission, identity mapping, `/stop`, turn-stream lifecycle, delivery
identity, and persistent state; this repository only translates Feishu's
provider protocol.

The first v3 adapter is deliberately text-only. Incoming and outgoing image,
file, and rich-post attachments are returned as deterministic `REJECTED`
without reading a workspace path, importing bytes, or uploading provider data.
The previous v2 installation and its data remain available for an explicit,
append-only migration; no v2 class or ABI is loaded by this artifact.
Feishu owns no plugin database or attachment store, so this migration has no
plugin-side copy/delete step: the existing `config.local.toml` remains the
formal source and Core owns identity/session data.

Formal startup is the only path that resolves `CredentialRef` through Core's
provider client factory and creates the HTTP/WebSocket client. Candidate
construction and validation never read formal credentials, create an SDK
client, or contact Feishu.
