# Agent Router

Agent Router is a Rust-first channel edge for agent sessions. It is meant to
connect chat channels such as Slack and QQ to one or more agent runtimes without
making those runtimes own every channel integration.

The project starts from a small scope:

- accept inbound messages from channel adapters
- normalize them into a shared session event model
- initialize each session with a default executor
- route each session to exactly one active executor backend at a time
- support explicit executor switching inside the same session context
- share context across executor switches through safe transcript projection
- send normalized outbound messages back through the originating channel

## Initial Direction

Agent Router should be its own project instead of another Hermes subsystem. The
router is infrastructure around channels, sessions, executor routing, and
handoff policy. Hermes, OpenClaw, Codex, Kimi, or future agents should be
pluggable executor backends behind a narrow protocol interface.

The first backend protocol is ACP. Codex app-server is supported as a separate
backend protocol adapter.

Slack and QQ are the first channels that matter. Slack should use the official
Slack API model. QQ should use Tencent's official QQ Bot API, with ZeroClaw's QQ
adapter as a protocol reference rather than a runtime dependency.

## MVP

The current MVP runs Slack Socket Mode and QQ Official Bot Gateway as the first
channels, `kimi acp` as the first ACP executor backend, and `codex app-server`
as the first app-server executor backend:

```bash
cargo run -- --config config/agent-router.example.yaml
```

Secrets are read from environment variables. The runner also tries these dotenv
locations, in order, so a local Hermes install can be reused without copying
tokens into this repository:

1. `.env`
2. `../.env`
3. `$HERMES_HOME/.env`
4. `~/.hermes/.env`

```bash
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
QQ_APP_ID=...
QQ_CLIENT_SECRET=...
```

Slack and QQ are enabled automatically when all required credentials for that
channel are present, unless the config explicitly sets `enabled: false`. QQ can
be restricted with `QQ_ALLOWED_USERS` and `QQ_ALLOWED_GROUPS`, both as
comma-separated openid lists. The MVP supports `/agent status` and
`/agent <name>` from channel messages. A session starts with `default_executor`,
has one `active_executor`, and shares context between executor switches by
projecting canonical transcript into the target executor.

Example switches:

```text
/agent kimi
/agent codex
```

## Non-Goals

- Replacing Hermes or OpenClaw as an agent runtime.
- Importing ZeroClaw's channel crate wholesale.
- Building a full customer-support inbox product.
- Coupling channel adapters to LLM provider, memory, tool, or orchestration
  implementations.

## Documentation

- [Architecture](docs/architecture.md)
- [Session Executor Routing Workflow](docs/workflows/session-executor-routing.md)
- [ADR 0001: Split Channel Router From Agent Runtime](docs/adr/0001-router-runtime-boundary.md)
- [ADR 0002: Use ACP as the First Backend Protocol](docs/adr/0002-acp-first-backend-protocol.md)
- [ADR 0003: Default Executor, Active Executor, and Shared Context](docs/adr/0003-default-active-executor-context.md)
