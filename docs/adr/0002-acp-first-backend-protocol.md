# ADR 0002: Use ACP as the First Backend Protocol

## Status

Accepted.

## Context

Agent Router needs to support the session executor routing workflow that was
first prototyped in Hermes. That workflow lets one user-visible session switch
between executors such as Hermes, Codex, Kimi, and future agents.

Supporting multiple backend protocols at the start would make the router core
carry protocol compatibility concerns before the state model is proven.

## Decision

Version 1 supports ACP as the only executor backend protocol.

Codex app-server support can be added later as a separate backend protocol
adapter. User-facing executor names should not depend on protocol names, so
`/agent codex` can point to an ACP backend first and a Codex app-server backend
later through configuration.

## Consequences

The first implementation can focus on the router's stable session key, mutable
current executor, unified transcript, per-executor private state, permission
events, cancellation, and safe event projection.

Executors that do not expose ACP need an ACP wrapper until their protocol is
implemented.

The router core must keep protocol-specific details behind backend adapters so
adding Codex app-server later does not change the session routing model.
