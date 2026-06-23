# ADR 0001: Split Channel Router From Agent Runtime

## Status

Accepted.

## Context

Hermes has an agent handoff feature that allows one session to call multiple
agents and share context. The same handoff need can exist outside Hermes.

At the same time, channel integration is becoming a separate problem. Slack, QQ,
Telegram, Discord, email, and webhook adapters should not have to be rewritten
inside every agent runtime.

ZeroClaw has useful channel implementations, including QQ, but its
`zeroclaw-channels` crate is not a narrow channel SDK. It is coupled to
ZeroClaw's runtime, config, memory, model provider, tool, approval, and
orchestration layers.

## Decision

Create Agent Router as a separate Rust project.

Agent Router will own channel adapters, normalized session events, outbound
delivery, session routing, and agent handoff state. Agent runtimes such as Hermes
will connect through a narrow runtime interface.

The first channel targets are Slack and QQ. ZeroClaw's QQ implementation will be
used as a protocol reference for Tencent's official QQ Bot API, but Agent Router
will not depend on ZeroClaw crates.

## Consequences

This keeps channel work reusable across runtimes and prevents Hermes from
becoming the owner of every external chat integration.

It also means Agent Router needs its own small abstractions for sessions,
messages, adapters, runtime calls, and handoff state.

The project should resist importing model provider, memory, tool execution, or
agent orchestration concerns into the router core.
