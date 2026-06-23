# Session Executor Routing Workflow

This workflow is adapted from Hermes'
`hermes-agent/docs/design/session-executor-routing.md`, but the ownership moves
from Hermes into Agent Router.

## Goal

The user sees one continuous session. Under that session, the current executor
can switch between configured agents such as Hermes, Codex, Kimi, or future
backends.

Agent Router owns the session routing state and the user-visible transcript.
Each executor backend keeps its own private state.

## Core Rules

- The user-visible session key is stable.
- The current executor is mutable.
- The canonical transcript is unified and safe to show to the user.
- Each executor can keep private state, such as an ACP session id, cwd,
  permission mode, model hint, or protocol-specific cache.
- Backend raw logs, stderr, secrets, and full internal reasoning are not
  forwarded directly to user channels.
- Switching executor does not require lossless migration of private backend
  state.

## Backend Protocols

Version 1 supports only ACP as the backend protocol.

Configured executors should therefore look like ACP backends first:

```yaml
executors:
  kimi:
    protocol: acp
    command: kimi-agent
    args: ["--acp", "--stdio"]

  codex:
    protocol: acp
    command: codex-agent
    args: ["--acp", "--stdio"]
```

Codex app-server support can be added later as a separate backend protocol:

```yaml
executors:
  codex:
    protocol: codex_app_server
    endpoint: http://127.0.0.1:PORT
```

The user-facing command can remain `/agent codex`; configuration decides which
backend protocol serves that executor.

If an executor does not support ACP in version 1, it needs an ACP wrapper or it
must wait until its protocol adapter is implemented.

## Commands

The first command surface is explicit executor switching:

```text
/agent hermes
/agent codex
/agent kimi
/agent status
/agent done
```

`/agent <name>` creates or resumes the executor's private backend session for
the current user-visible session.

`/agent done` ends the current external takeover and returns to the configured
home executor.

Natural-language completion, such as "这个事情做完了", can be added after the
explicit command path is stable.

## State Model

Each router session stores:

- `session_key`
- `home_executor`
- `current_executor`
- canonical user-visible transcript
- per-executor backend bindings
- safe projected event log

Each executor binding stores protocol-specific private state:

- protocol name
- external session id
- backend process or connection identity
- cwd
- permission mode
- MCP server selection
- model hint
- last known status

The router may use projected transcript or a summary when switching executors.
It should not inject raw backend event streams into another executor's context.

## Message Flow

1. A channel adapter receives an inbound platform event.
2. The adapter normalizes it into a router event and resolves a `session_key`.
3. The router checks whether the message is a router command.
4. If the message switches executor, the router updates `current_executor`,
   creates or resumes that executor's backend binding, records the switch in the
   transcript, and emits a short user-visible status event.
5. If the message is a normal user turn, the router sends it to the current
   executor backend.
6. ACP backends receive the turn through ACP session calls.
7. ACP `session/update` events are converted into router output events.
8. Permission requests are converted into the router's approval workflow.
9. Outbound router events are sent through the originating channel adapter.

## Failure And Cancellation

External executor crashes, failed startup, protocol errors, and user
cancellation should leave the user-visible session intact.

The router should mark the backend binding as failed, emit a short safe status
message, and return control to the configured home executor.

Long-running work must not be killed by a fixed timeout while the backend is
still active. Liveness should come from protocol events, process state,
heartbeats, explicit cancel results, user deadlines, or another state signal
that shows whether work is still active.

## Automatic Routing

Automatic routing is a later layer on top of the same state machine.

The first implementation should use deterministic rules and configuration. A
skill or model may provide routing hints later, but it should not own the
routing state machine.
