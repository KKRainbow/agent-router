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

The first backend protocol is ACP. Codex app-server support can be added later
as a separate backend protocol adapter.

Slack and QQ are the first channels that matter. Slack should use the official
Slack API model. QQ should use Tencent's official QQ Bot API, with ZeroClaw's QQ
adapter as a protocol reference rather than a runtime dependency.

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
