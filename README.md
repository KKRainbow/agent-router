# Agent Router

Agent Router is a Rust-first channel edge for agent sessions. It is meant to
connect chat channels such as Slack and QQ to one or more agent runtimes without
making those runtimes own every channel integration.

The project starts from a small scope:

- accept inbound messages from channel adapters
- normalize them into a shared session event model
- route each session to one or more agents
- support agent handoff inside the same session context
- send normalized outbound messages back through the originating channel

## Initial Direction

Agent Router should be its own project instead of another Hermes subsystem. The
router is infrastructure around channels, sessions, and handoff policy. Hermes,
OpenClaw, or future agents should be pluggable backends behind a narrow runtime
interface.

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
- [ADR 0001: Split Channel Router From Agent Runtime](docs/adr/0001-router-runtime-boundary.md)
