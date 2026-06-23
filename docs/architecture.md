# Architecture

This document records the initial architecture choices for Agent Router.

## Problem

Hermes and OpenClaw both need to connect agents to many channels. If every agent
runtime owns its own Slack, Telegram, QQ, email, and webhook integrations, the
same channel work is repeated and session handoff becomes tied to one runtime's
internal abstractions.

Agent Router exists to make channel integration and session routing reusable.

## Core Boundary

Agent Router owns:

- channel adapter lifecycle
- inbound event normalization
- outbound message delivery
- session identity and channel thread mapping
- session default executor and active executor state
- executor switching and handoff policy
- shared session context that can be projected to executor backends

Executor backends own:

- model selection
- tool execution
- long-term memory
- agent-specific prompts and policies
- domain behavior

The boundary should be a small backend protocol interface, not a shared
dependency on a specific agent implementation.

## First-Class Concepts

### Channel Adapter

A channel adapter connects one external transport to the router. Examples:
Slack, QQ, Telegram, Discord, webhook, and CLI.

Adapters should translate platform-specific events into normalized router
events. They should not know about LLM providers, tools, memory systems, or
agent handoff logic.

### Session

A session is the durable conversation identity used by the router. It may map to
a Slack thread, a QQ user openid, a QQ group openid, or a webhook conversation
key.

Each session starts with a configured `default_executor`. The `active_executor`
is initialized to the default executor and can change later through explicit
commands or routing policy. There is exactly one active executor per session at
any time.

The session is the unit of shared handoff context. Shared context is owned by
the router and projected into backend-specific prompts or resume calls; executor
private state is not shared directly.

### Executor Backend

An executor backend is anything that can accept a normalized session turn and
produce router-compatible output. Hermes, Codex, and Kimi are examples of
executor identities from the user's point of view.

The first supported backend protocol is ACP. A configured executor such as
`kimi` or `codex` should initially connect through ACP. Codex app-server can be
added later as a separate protocol adapter without changing the session routing
model.

### Handoff

Handoff lets one session call multiple agents while preserving shared context.
The router should own the cross-agent routing state. Only one executor is active
at a time, but multiple executor bindings may exist in an idle resumable state.
Each backend remains responsible for its own execution semantics.

### Shared Context

The shared context source of truth is the router's canonical transcript plus
router-owned session metadata. When switching executors, the router builds a safe
context projection for the target backend.

For ACP backends, the first turn can include recent transcript and session
context in the prompt. A resumed backend should receive only transcript entries
it has not already seen, tracked by stable message fingerprints or an equivalent
cursor. Backend output is projected back into the canonical transcript as safe
user-visible assistant content plus tool/progress summaries.

Raw backend logs, stderr, secrets, and internal reasoning are not shared context.
Executor-provided reasoning summaries and tool-call summaries may be projected as
router-owned channel events when they are safe to show to the user. Channel
adapters deliver those events but do not interpret backend protocol events.

## Channel Strategy

The first two production channels are Slack and QQ.

Slack is the higher-confidence channel because the official API and event model
are well documented and stable.

QQ is important enough to implement directly in Rust. ZeroClaw already has a
Rust adapter for Tencent's official QQ Bot API, so Agent Router should borrow
the protocol shape:

- OAuth token acquisition and refresh
- WebSocket gateway identify, heartbeat, resume, and reconnect
- `C2C_MESSAGE_CREATE` and `GROUP_AT_MESSAGE_CREATE` event mapping
- `user:<openid>` and `group:<openid>` recipient addressing
- markdown message sending through the v2 message APIs
- media upload as a second-phase feature

ZeroClaw should be treated as a reference implementation, not a dependency.
Its channel crate is coupled to its runtime, config, memory, provider, and
orchestration layers.

## Dependency Direction

Channel adapters depend on router-core abstractions.

Router core depends on no channel-specific crates.

Backend protocol integrations depend on router-core abstractions.

Router core should not depend on Hermes, ZeroClaw, Slack, QQ, or any LLM
provider SDK.

The initial backend protocol set is intentionally narrow:

- `acp` in phase 1
- `codex_app_server` in a later phase

## Phasing

### Phase 1

- Define normalized inbound and outbound message types.
- Define `ChannelAdapter` and executor backend traits.
- Implement session executor routing with a stable user-visible session,
  configured default executor, single active executor, unified transcript, and
  per-executor private state.
- Implement shared-context projection across executor switches.
- Implement ACP as the only backend protocol.
- Implement Slack text message ingress and egress.
- Implement QQ text message ingress and egress.
- Support explicit `/agent` switching inside one session.

### Phase 2

- Add Codex app-server as a backend protocol adapter.
- Add media upload and download.
- Add stronger session persistence.
- Add retries, rate-limit handling, and delivery receipts.
- Add adapter health checks.

### Phase 3

- Add more channels only after Slack and QQ stabilize.
- Add compatibility shims for Hermes and OpenClaw if useful.
- Add optional human approval and operator handoff workflows.
