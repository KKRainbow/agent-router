# Session Persistence

## Background

Agent Router models a session as the durable user-visible conversation identity.
Today the production entrypoint constructs `InMemorySessionStore`, so router
session state only lives inside the current process. After a restart, the router
loses the selected agent, the canonical transcript, and backend resume
identifiers.

This document defines the first implementation scope. It is intentionally small:
persist the state needed to keep the visible conversation and resume executor
backend sessions, without introducing a database or persisting broader runtime
state.

## Design Principle

Persist committed router-owned conversation semantics. Do not persist live
execution state, process state, network connection state, pending security
decisions, or channel gateway state.

In practical terms:

- Committed transcript entries are durable.
- The selected `active_executor` is durable.
- Executor backend resume identifiers are durable when they are already part of
  `ExecutorBinding`.
- Context projection cursors are durable so resumed backends do not receive the
  same transcript entries again.
- In-flight turns are interrupted by restart.
- The router remains the source of truth for canonical transcript and handoff
  context.

## Goals

- Restore a session on demand from its `session_key` after router restart.
- Preserve the selected `active_executor` for each session.
- Preserve the canonical user-visible transcript.
- Preserve executor binding fields needed for backend resume.
- Preserve `seen_context` so context projection does not resend already seen
  transcript entries to a resumed backend.
- Keep interrupted or stale turns out of durable transcript.
- Keep the implementation simple enough to live inside the current
  `SessionStore` boundary.

## Non-Goals

- Resuming an active turn after process restart.
- Restoring cancellation tokens, turn generations, or turn registry entries.
- Restoring executor child processes, PIDs, stdio handles, or WebSocket handles.
- Restoring Slack, QQ, or other channel gateway connection state.
- Persisting pending approval requests.
- Persisting per-session approval mode overrides in the first implementation.
- Persisting context artifact indexes in the first implementation.
- Persisting machine workspace records in the first implementation.
- Persisting partial streamed output.
- Persisting raw backend logs, stderr, request headers, secrets, or tokens.

Channel adapters may later implement their own transport resume state, but that
state is separate from router session persistence.

## Storage Location

Use one JSON snapshot per router session workspace:

```text
<workspace.root>/<session-workspace-dir>/.agent-router/session.json
```

Rules:

- `workspace.root` is the root already used for router-owned session workspaces.
- `<session-workspace-dir>` uses the same deterministic directory name derived
  from `session_key` that workspace materialization uses.
- The `.agent-router` directory is router-private metadata inside the session
  workspace.
- If `workspace.root` is not configured, session persistence is disabled and the
  router keeps using in-memory session state.
- The store loads a session lazily by computing its snapshot path from
  `session_key`; a global session index is not required for the first
  implementation.

This keeps persistence near the transcript artifacts users already expect to
belong to the session, and avoids adding SQLite or a migration framework before
there is a concrete query need.

## Persistent Snapshot

The first snapshot schema is a single JSON object:

```json
{
  "schema_version": 1,
  "session_key": "slack:channel:C123:T456",
  "default_executor": "kimi",
  "active_executor": "codex",
  "routing_mode": "manual",
  "created_at_ms": 1782973000000,
  "updated_at_ms": 1782973060000,
  "transcript": [],
  "executor_bindings": {}
}
```

Required fields:

```text
schema_version
session_key
default_executor
created_at_ms
updated_at_ms
transcript
executor_bindings
```

Defaulted fields:

```text
active_executor
routing_mode
```

Rules:

- `session_key` is the primary identity and must match the requested session.
- `default_executor` records the session's original default executor.
- `active_executor` records the executor currently selected for the session. It
  can be missing or null while automatic routing is pending.
- `routing_mode` records whether `active_executor` is a manual override or an
  automatic router selection. Missing values default to `auto` when loading
  older snapshots.
- `created_at_ms` and `updated_at_ms` are store-owned timestamps.
- `schema_version` supports future migrations.
- The router can reconstruct `SessionState.cwd` from the workspace path when the
  snapshot is loaded.

Loading rules:

- If `default_executor` is no longer configured, use the router configured
  default and log a warning.
- If a non-empty `active_executor` is no longer configured, fall back to the
  resolved `default_executor` and log a warning.
- If the snapshot `session_key` does not match the requested key, treat the file
  as corrupt and do not overwrite it.
- Malformed required state should return a clear error instead of silently
  creating a new session over the corrupt snapshot.

## Transcript

The transcript is the router's canonical user-visible context source. The JSON
snapshot stores committed transcript entries in array order:

```json
{
  "role": "assistant",
  "content": "Done.",
  "timestamp_ms": 1782973050000,
  "executor": "codex",
  "external_session_id": "backend-session-id"
}
```

Rules:

- Only committed turns are persisted.
- Interrupted turns must not persist final responses or partial streamed output.
- Array order is the recovery order for the first implementation.
- `content` is safe user-visible text.
- Raw backend events, internal reasoning, tool payloads, and stderr are not
  transcript content.

The router may continue to expose transcript through `SessionState`.

## Executor Bindings

The snapshot stores a slim binding record for each executor key:

```json
{
  "executor_bindings": {
    "codex": {
      "protocol": "app_server",
      "machine_id": "local",
      "external_session_id": "backend-session-id",
      "cwd": "/data/project/agent-router-workspaces/slack-channel-c123-t456",
      "seen_context": []
    }
  }
}
```

Persisted fields:

```text
protocol
machine_id
external_session_id
cwd
seen_context
```

Rules:

- `external_session_id` is persisted so the adapter can ask the backend to
  resume.
- `seen_context` is persisted so context projection sends only unseen transcript
  entries to a resumed backend.
- `machine_id` and `cwd` record where the executor-visible session ran.
- `protocol` records the backend protocol that created the binding.
- `health` is not persisted. Loaded bindings always start as
  `ExecutorHealth::Unknown`.
- `metadata` is not persisted in the first implementation. If an adapter later
  needs non-secret resume metadata, add it deliberately to this JSON schema.

### Context Cursor

The context cursor is `ExecutorBinding.seen_context`.

It contains fingerprints of transcript entries that have already been projected
to a given executor backend. Without persisting it, a restarted router could
resume an existing backend session through `external_session_id` and then inject
old transcript again. Persisting `seen_context` keeps resume behavior stable and
prevents duplicated handoff context.

## Runtime-Only State

The following state must stay out of the session persistence layer:

- `TurnRegistry` active turn map.
- Turn generation counters.
- `TurnCancellation` state.
- Pending approval requests.
- Per-session approval mode overrides for the first implementation.
- Context artifact records for the first implementation.
- Machine workspace records for the first implementation.
- In-flight executor prompt response.
- Partial streamed channel output.
- Executor process handles and PIDs.
- Gateway WebSocket handles and heartbeat state.
- Access tokens, API keys, signing secrets, and approval secrets.
- Raw backend logs and stderr.

On restart, any active turn is treated as interrupted. Users may send a new
message, and the router should route it using the restored session state.

## Store Interface

The existing `SessionStore` trait remains the conceptual persistence boundary:

```rust
async fn load(&self, session_key: &str) -> Option<SessionState>;
async fn load_or_create(&self, session_key: &str, default_executor: &str) -> SessionState;
async fn save(&self, state: SessionState);
```

The first persistent implementation should add a workspace JSON store while
keeping `InMemorySessionStore` for tests. If strict corrupt-snapshot handling
cannot be expressed through the current `Option`-returning methods, evolve the
trait to return store errors rather than treating corrupt state as a missing
session.

Required behavior:

- `load` reconstructs a `SessionState` aggregate from the JSON snapshot.
- `load_or_create` creates a new in-memory state only when no snapshot exists.
- `save` replaces the durable snapshot for one session.
- A committed turn must persist transcript updates and executor binding updates
  together in one snapshot.
- A stale turn must not overwrite a newer committed state.

The router should continue to enforce turn freshness through `TurnGuard`.
The persistent store must not introduce alternate write paths that bypass that
freshness check.

## Write Semantics

The router is a single process and already serializes same-session commits with
session locks. The JSON store should still avoid partial files:

1. Ensure the session workspace and `.agent-router` directory exist using the
   same path safety checks as workspace materialization.
2. Serialize the snapshot to a temporary sibling file.
3. Flush the temporary file.
4. Replace `session.json` with the temporary file.

If a save fails during a turn, the router should return an error and must not
send a successful final reply.

## Loading and Normalization

Persistent load should reconstruct `SessionState` and then normalize it:

- Fill missing optional collections with defaults.
- Set `SessionState.cwd` to the session workspace path.
- Reset all binding health to `Unknown`.
- Validate `default_executor` and `active_executor` against configured task
  executors; the reserved orchestrator executor is not a valid
  `active_executor`.
- Validate path fields with the same safety checks used for workspace
  materialization.
- Preserve transcript order from the JSON array.
- Use empty binding metadata.

Malformed required session state should produce a clear store error rather than
silently creating a new session over the corrupted snapshot.

## Security

Session persistence must not become a secret store.

Do not persist:

- Slack bot tokens or app tokens.
- QQ app secrets or access tokens.
- OpenAI, Anthropic, Codex, Claude, or ACP credentials.
- Approval broker secrets.
- HTTP headers or raw webhook payloads.
- Backend stderr.
- Internal reasoning.
- Adapter-private metadata unless it is explicitly documented as non-secret.

If an executor adapter needs secret material, it must reference configured
secret sources rather than storing the secret in `SessionState`.

## Failure Handling

Recommended behavior:

- Snapshot cannot be read because the file is missing: create a new session.
- Snapshot cannot be parsed or has mismatched `session_key`: return a clear
  error for that session and do not overwrite it.
- Save fails during a turn: return an error and do not send a successful final
  reply.
- `workspace.root` is not configured: keep the current in-memory behavior and
  log that session persistence is disabled.
- Unknown `active_executor`: fall back to the resolved default executor and log
  a warning.
- Unknown `default_executor`: fall back to the configured router default and log
  a warning.

The router should prefer failing loudly over silently resetting a user's durable
session when a snapshot exists but is corrupt.

## Tests

Required store tests:

- `save` then create a new store instance and `load` the same session.
- `load_or_create` returns existing state without overwriting it.
- Transcript order survives restart.
- `active_executor` survives restart.
- `routing_mode` survives restart.
- Legacy snapshots without `routing_mode` load as `auto`.
- Executor bindings survive restart.
- Binding health is reset to `Unknown` on load.
- `seen_context` survives restart.
- Malformed required session snapshot returns an error.
- Mismatched snapshot `session_key` returns an error.

Required router integration tests:

- Completed turn persists user and assistant transcript entries.
- Interrupted turn does not persist partial output.
- Stale turn cannot overwrite a newer committed turn.
- `/agent <executor>` manual routing survives restart.
- Executor receives `previous_session_id` after restart.
- Context projection after restart only includes unseen context.

## Implementation Plan

1. Add a workspace JSON-backed `SessionStore` implementation.
2. Adjust `SessionStore` error reporting if needed so corrupt snapshots are not
   treated as missing sessions.
3. Share or move the deterministic session workspace path helper so the store
   and router compute the same path.
4. Change `main.rs` to use the JSON store when `workspace.root` is configured,
   otherwise keep `InMemorySessionStore`.
5. Keep the JSON snapshot schema at version `1`.
6. Add store-level persistence tests.
7. Add focused router restart tests for transcript, active executor, backend
   session id, and `seen_context`.
8. Update the example config or README only if a new configuration knob is
   introduced.

## Future Work

- Persist per-session approval mode overrides.
- Persist context artifact indexes if manifest recovery is not enough.
- Persist machine workspace records if restart-time diagnostics need them.
- Add a session index if the web UI needs server-side session listing.
- Add a database backend if querying, compaction, or high write concurrency
  becomes a real requirement.
- Add a session export/import command.
- Add configurable transcript retention.

## Conclusion

The first session persistence implementation should be a small JSON snapshot in
the router session workspace. It should persist the selected executor,
canonical transcript, backend resume identifiers, and `seen_context`. Everything
else remains runtime-only or future work until there is a concrete need.
