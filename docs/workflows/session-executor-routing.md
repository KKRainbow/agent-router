# Session Executor Routing Workflow

This workflow is adapted from Hermes'
`hermes-agent/docs/design/session-executor-routing.md`, but the ownership moves
from Hermes into Agent Router.

## Goal

The user sees one continuous session. When the session starts, the router
chooses a `default_executor`. The session then has exactly one
`active_executor`, initialized from that default. The active executor can switch
between configured agents such as Hermes, Codex, Kimi, or future backends.

Agent Router owns the session routing state and the user-visible transcript.
Each executor backend keeps its own private state.

## Core Rules

- The user-visible session key is stable.
- Each session has a `default_executor`.
- Each session has exactly one `active_executor`.
- `active_executor` is initialized to `default_executor`.
- Executor switching changes `active_executor`; it does not create a new
  user-visible session.
- The canonical transcript is unified and safe to show to the user.
- Shared context is derived from the canonical transcript and router-owned
  session metadata.
- When `workspace.root` is configured, each session gets a stable Router
  Workspace for router-owned artifacts. Each Machine may also create a session
  Machine Workspace that is visible to executors on that Machine.
- Each executor can keep private state, such as an ACP session id,
  permission mode, model hint, or protocol-specific cache.
- Backend raw logs, stderr, secrets, and full internal reasoning are not
  forwarded directly to user channels.
- Switching executor does not require lossless migration of private backend
  state.
- A failed or cancelled external turn must not mark projected context as seen.

## Backend Protocols

Version 1 supports only ACP as the backend protocol.

Configured executors should therefore look like ACP backends first:

```yaml
workspace:
  root: /data/project/hermes-workspaces

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
default executor.

Natural-language completion, such as "这个事情做完了", can be added after the
explicit command path is stable.

## Slash Command Passthrough

Slash commands have three distinct owners and must not be collapsed into one
plain-text prompt path:

- Router-owned commands, such as `/stop`, `/agent`, `/yolo`, `/approve`, and
  `/deny`, are consumed by Agent Router.
- Channel-platform slash commands, such as Slack slash command payloads, are
  normalized by the channel adapter before routing.
- Agent-owned slash commands, such as an executor's `/status`, belong to the
  current active executor.

The router should preserve agent-owned slash command semantics when forwarding
them to an executor. If a non-router command starts with `/`, the router should
parse it into a structured command turn instead of sending it as a normal user
prompt. A minimal representation is:

```rust
pub struct ExecutorSlashCommand {
    pub raw: String,
    pub name: String,
    pub args: String,
}
```

Agent slash commands should flow through a separate executor API from normal
prompts. They should not be wrapped in the handoff projection as:

```text
Current user message:
/status
```

That wrapping preserves the bytes but loses the command semantics. It also
pollutes future transcript projection with what was intended as executor
control input.

If the active executor's backend protocol can preserve slash command semantics,
the adapter should send the structured command through the protocol-native or
negotiated command path. If the backend protocol cannot preserve those
semantics, the router should return an explicit unsupported-command reply
rather than silently downgrading the command into a text prompt.

The dispatch rule is:

```text
router-owned slash command -> router handles it
agent-owned slash command + backend supports command passthrough -> structured executor command
agent-owned slash command + backend cannot preserve command semantics -> explicit unsupported reply
literal text beginning with "/" -> escaped by the user, for example "//status" or "\/status"
```

Agent slash commands are control input, not ordinary chat content. By default
they should not be appended to the canonical transcript and should not advance a
backend's seen-context cursor. A future implementation may choose to record a
safe audit event, but that event must remain distinct from normal user-visible
conversation.

Agent slash commands also have different turn-lifecycle semantics from normal
messages. A command such as `/status` should not implicitly replace or interrupt
an in-flight task unless the target backend declares that behavior. Version 1
should be conservative: only route agent slash commands while the executor is
idle, or when the backend explicitly advertises that the command is safe during
an active turn.

## State Model

Each router session stores:

- `session_key`
- `default_executor`
- `active_executor`
- Router Workspace, when `workspace.root` is configured
- per-Machine Workspace records, when executors run on configured Machines
- canonical user-visible transcript
- per-executor backend bindings
- safe projected event log

Each executor binding stores protocol-specific private state:

- protocol name
- Machine id
- external session id
- backend process or connection identity
- effective executor-visible cwd used for that backend session
- permission mode
- MCP server selection
- model hint
- last known status
- seen context cursor, such as message fingerprints

Only one binding is active at a time. Other bindings can remain idle so a later
switch can resume their private backend session.

## Shared Context

Context sharing is a router-level projection, not a shared mutable object passed
between backends.

The router's source of truth is:

- canonical user-visible transcript
- router-owned session metadata
- safe summaries of backend tool/progress events

When switching to a backend for the first time, the router should seed it with a
safe handoff prompt containing recent transcript, relevant session context, and
the current user message.

When resuming a backend that already has private state, the router should send
only transcript entries that backend has not seen yet. Hermes' prototype tracks
this with message fingerprints in the executor binding metadata. The new project
can use the same idea or an equivalent monotonic cursor, but the invariant is
the same: do not replay already-acknowledged context unless a reset or recovery
requires it.

After a backend turn succeeds, the router projects the backend result back into
canonical transcript:

- append the user turn as user-visible input
- append a safe assistant entry attributed to the executor
- include visible final reply
- include safe tool/progress summaries
- record the backend session id and updated seen-context cursor

If a backend turn is cancelled or fails before producing a usable result, the
router should preserve the previous seen-context cursor so a future retry can
receive the missed context.

The router should not inject raw backend event streams, stderr, secrets, or full
internal reasoning into another executor's context.

## Message Flow

1. A channel adapter receives an inbound platform event.
2. The adapter normalizes it into a router event and resolves a `session_key`.
3. The router checks whether the message is a router command.
4. If the message switches executor, the router updates `active_executor`,
   creates or resumes that executor's backend binding, records the switch in the
   transcript, and emits a short user-visible status event.
5. If the message is an agent-owned slash command, the router forwards it as a
   structured executor command only when the active backend can preserve command
   semantics; otherwise it returns an explicit unsupported-command reply.
6. If the message is a normal user turn, the router sends it to the current
   active executor backend.
7. ACP backends receive the turn through ACP session calls. The prompt includes
   the shared-context projection needed by that backend.
8. ACP `session/update` events are converted into router output events.
9. Permission requests are converted into the router's approval workflow.
10. Outbound router events are sent through the originating channel adapter.

## Failure And Cancellation

External executor crashes, failed startup, protocol errors, and user
cancellation should leave the user-visible session intact.

The router should mark the backend binding as failed, emit a short safe status
message, and return control to the configured default executor.

Long-running work must not be killed by a fixed timeout while the backend is
still active. Liveness should come from protocol events, process state,
heartbeats, explicit cancel results, user deadlines, or another state signal
that shows whether work is still active.

## Automatic Routing

Automatic routing is a later layer on top of the same state machine.

The first implementation should use deterministic rules and configuration. A
skill or model may provide routing hints later, but it should not own the
routing state machine.

The draft implementation plan is documented separately in
`docs/workflows/orchestrator-initial-routing.md`.

That draft treats an orchestrator-enabled session as temporarily unassigned
before the first normal user message, so the orchestrator can choose the first
real `active_executor` without making `default_executor` ambiguous.
