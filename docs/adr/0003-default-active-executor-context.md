# ADR 0003: Default Executor, Active Executor, and Shared Context

## Status

Accepted.

## Context

The Hermes prototype stores one current executor per session and projects
transcript context into external ACP sessions. In Agent Router, the same workflow
should be expressed as a first-class session model rather than Hermes-specific
state.

The router needs to support executor switching without creating a new
user-visible session and without requiring different backends to share internal
state objects.

## Decision

Each session has a `default_executor` and at most one `active_executor`.
`active_executor` can be empty while automatic routing is pending.

Without automatic routing, a new session initializes `active_executor` from
`default_executor`. Automatic routing may leave `active_executor` empty until a
normal message is routed. Switching executors updates only `active_executor`; it
does not create a new user-visible session.

Multiple executor bindings may exist for one session, but only one binding is
active. Idle bindings preserve backend-private state such as ACP session id,
cwd, command identity, health, and seen-context cursor.

Context sharing is implemented through router-owned projection:

- canonical transcript is the source of truth
- first handoff to a backend receives a safe transcript seed
- resumed backends receive only transcript entries not yet acknowledged
- backend outputs are projected back into canonical transcript as safe visible
  assistant entries and tool/progress summaries
- failed or cancelled turns do not advance the seen-context cursor

## Consequences

The router can hand a session between executors while keeping one user-visible
conversation and a single selected executor when the session is not auto-pending.

Backends remain isolated. They do not need to share caches, tool state, model
reasoning items, approval internals, or raw protocol events.

The router needs durable per-session state for `default_executor`,
`active_executor`, executor bindings, and context cursors. It also needs a
projector that can turn backend events into safe transcript entries and channel
events.
