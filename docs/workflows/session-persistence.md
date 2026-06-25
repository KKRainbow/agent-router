# Session Persistence

## Background

Agent Router models a session as the durable user-visible conversation identity.
Today the production entrypoint constructs `InMemorySessionStore`, so router
session state only lives inside the current process. After a restart, the router
loses the active executor, transcript, backend session bindings, context cursors,
session cwd, machine workspace records, and per-session approval override.

This document defines what session persistence should record and what it should
intentionally leave as runtime state.

## Design Principle

Persist the router-owned session semantics that users expect to survive a
restart. Do not persist live execution state, process state, network connection
state, or pending security decisions.

In practical terms:

- Committed session state is durable.
- In-flight turns are interrupted by restart.
- Executor backends may resume only through persisted backend identifiers.
- The router remains the source of truth for canonical transcript and handoff
  context.

## Goals

- Restore each known `session_key` after router restart.
- Preserve the selected `active_executor` for each session.
- Preserve the canonical user-visible transcript.
- Preserve executor binding state needed for backend resume.
- Preserve context projection cursors so handoff does not resend already seen
  transcript entries.
- Preserve router workspace and machine workspace records.
- Preserve per-session approval mode overrides.
- Keep interrupted or stale turns out of durable transcript.

## Non-Goals

- Resuming an active turn after process restart.
- Restoring cancellation tokens, turn generations, or turn registry entries.
- Restoring executor child processes, PIDs, stdio handles, or WebSocket handles.
- Restoring Slack, QQ, or other channel gateway connection state as part of the
  router session store.
- Persisting pending approval requests.
- Persisting partial streamed output.
- Persisting raw backend logs, stderr, request headers, secrets, or tokens.

Channel adapters may later implement their own transport resume state, but that
state is separate from router session persistence.

## Persistent State

### Session Aggregate

Each `session_key` owns one durable session aggregate.

Required fields:

```text
session_key
default_executor
active_executor
approval_mode_override
cwd
created_at_ms
updated_at_ms
schema_version
```

Rules:

- `session_key` is the primary identity.
- `default_executor` is the session's original default executor.
- `active_executor` records the executor currently selected for the session.
- `approval_mode_override` records `/yolo on`, `/yolo off`, and `/yolo inherit`
  state.
- `cwd` records the router session workspace path when workspace support is
  enabled.
- `created_at_ms` and `updated_at_ms` are store-owned timestamps.
- `schema_version` supports future migrations.

Loading rules:

- If `default_executor` is no longer configured, use the router configured
  default and log a warning.
- If `active_executor` is no longer configured, fall back to the resolved
  `default_executor` and log a warning.
- If `cwd` is present, it must pass the same path safety checks used by the
  workspace materialization path.

### Transcript

The transcript is the router's canonical user-visible context source.

Each committed transcript message should be stored as its own record:

```text
id
session_key
sequence
role
content
timestamp_ms
executor
external_session_id
```

Rules:

- Only committed turns are persisted.
- Interrupted turns must not persist final responses or partial streamed output.
- `sequence` is monotonic within one session and defines recovery order.
- `content` is safe user-visible text.
- Raw backend events, internal reasoning, tool payloads, and stderr are not
  transcript content.

The router may continue to expose transcript through `SessionState`, but the
storage layer should preserve ordering explicitly rather than depending on JSON
array position alone.

### Executor Bindings

Each `(session_key, executor)` pair owns one binding record.

Required fields:

```text
session_key
executor
protocol
machine_id
external_session_id
cwd
health
seen_context
metadata
updated_at_ms
```

Rules:

- `external_session_id` is persisted so the adapter can ask the backend to
  resume.
- `seen_context` is persisted so context projection sends only unseen transcript
  entries to a resumed backend.
- `machine_id` and `cwd` record where the executor-visible session ran.
- `metadata` may store adapter-private non-secret resume data.
- `protocol` records the backend protocol that created the binding.

Health handling:

- `ExecutorHealth::Healthy` is runtime-derived and must not be trusted after
  restart.
- On load, previous `Healthy` and `Unhealthy` values should become `Unknown`.
- The next prepare or prompt call should refresh health.

### Context Artifacts

Context artifact records index files in the router session workspace.

Required fields should match `ContextArtifactRecord`, including:

```text
session_key
source
id
kind
paths
updated_at_ms
metadata
```

Rules:

- The file contents live in the session workspace.
- The persisted index is the primary source of truth.
- Manifest recovery remains a recovery mechanism for missing or stale store
  records.
- If persisted records and recovered manifest records disagree, prefer the newer
  source by timestamp and log the decision.

### Machine Workspaces

Each session can have workspace records for one or more machines.

Required fields should match `MachineWorkspaceRecord`, including:

```text
machine_id
cwd
materialization
artifact_fingerprint
```

`session_key` belongs to the enclosing `SessionState`; machine workspace records
are keyed by Machine id inside `SessionState.machine_workspaces`.

Rules:

- A persisted machine workspace record does not prove the remote path still
  exists.
- The next executor prepare should verify or rematerialize the workspace through
  the `MachineRegistry` boundary.
- Router persistence should not embed SSH command details.

## Runtime-Only State

The following state must stay out of the session persistence layer:

- `TurnRegistry` active turn map.
- Turn generation counters.
- `TurnCancellation` state.
- Pending approval requests.
- In-flight executor prompt response.
- Partial streamed channel output.
- Executor process handles and PIDs.
- Gateway WebSocket handles and heartbeat state.
- Access tokens, API keys, signing secrets, and approval secrets.
- Raw backend logs and stderr.

On restart, any active turn is treated as interrupted. Users may send a new
message, and the router should route it using the restored session state.

## Store Interface

The existing `SessionStore` trait is the correct boundary:

```rust
async fn load(&self, session_key: &str) -> Option<SessionState>;
async fn load_or_create(&self, session_key: &str, default_executor: &str) -> SessionState;
async fn save(&self, state: SessionState);
```

The first persistent implementation should keep this trait and add a new
`SqliteSessionStore`.

Required behavior:

- `load` reconstructs a full `SessionState` aggregate.
- `load_or_create` atomically creates the session if it does not exist.
- `save` atomically replaces the durable aggregate for one session.
- A committed turn must persist transcript updates and executor binding updates
  in the same transaction.
- A stale turn must not overwrite a newer committed state.

The router should continue to enforce turn freshness through `TurnGuard`.
The persistent store must not introduce alternate write paths that bypass that
freshness check.

## Storage Backend

Use SQLite for the first persistent backend.

Reasons:

- Cross-platform single-file storage.
- Transaction support for committed turns.
- Simple deployment and backup.
- Clear migration story.
- Better behavior than JSON files under concurrent writes and partial failures.

Configuration:

```yaml
storage:
  type: sqlite
  path: ~/.local/share/agent-router/sessions.sqlite3
```

If `storage.path` is omitted, use the platform data directory:

- Linux: `$XDG_DATA_HOME/agent-router/sessions.sqlite3`, falling back to
  `~/.local/share/agent-router/sessions.sqlite3`.
- macOS: `~/Library/Application Support/agent-router/sessions.sqlite3`.
- Windows: `%APPDATA%\agent-router\sessions.sqlite3`.

## Schema Shape

The exact schema can evolve, but the first version should separate the aggregate
into queryable tables rather than one opaque JSON blob.

Suggested tables:

```text
schema_migrations(version, applied_at_ms)

sessions(
  session_key primary key,
  default_executor not null,
  active_executor not null,
  approval_mode_override,
  cwd,
  created_at_ms not null,
  updated_at_ms not null,
  schema_version not null
)

transcript_messages(
  id primary key,
  session_key not null,
  sequence not null,
  role not null,
  content not null,
  timestamp_ms not null,
  executor,
  external_session_id,
  unique(session_key, sequence)
)

executor_bindings(
  session_key not null,
  executor not null,
  protocol not null,
  machine_id,
  external_session_id,
  cwd,
  health not null,
  seen_context_json not null,
  metadata_json not null,
  updated_at_ms not null,
  primary key(session_key, executor)
)

context_artifacts(
  session_key not null,
  source not null,
  id not null,
  kind not null,
  paths_json not null,
  metadata_json not null,
  updated_at_ms not null,
  primary key(session_key, source, id)
)

machine_workspaces(
  session_key not null,
  machine_id not null,
  workspace_json not null,
  updated_at_ms not null,
  primary key(session_key, machine_id)
)
```

Foreign keys should cascade from `sessions.session_key` to child tables.

JSON columns are acceptable for nested fields that are already strongly typed in
Rust and do not need independent querying in the first version.

## Transactions

Every `save(SessionState)` should run in one transaction:

1. Upsert the `sessions` row.
2. Replace transcript rows for the session.
3. Replace executor binding rows for the session.
4. Replace context artifact rows for the session.
5. Replace machine workspace rows for the session.
6. Commit.

This is simple and correct for the current aggregate-style `SessionStore`.
If write volume later becomes a problem, the store can add narrower update
methods after the router has explicit commit operations.

## Loading and Normalization

Persistent load should reconstruct `SessionState` and then normalize it:

- Fill missing optional collections with defaults.
- Reset binding health to `Unknown`.
- Validate configured executors.
- Validate path fields.
- Sort transcript by `sequence`.
- Sort context artifact records using the existing deterministic ordering.
- Ignore malformed optional metadata after logging a warning.

Malformed required session state should produce a clear store error rather than
silently creating a new session over the corrupted record.

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

Executor `metadata` must be documented as non-secret. If an executor adapter
needs secret material, it must reference configured secret sources rather than
storing the secret in `SessionState`.

## Failure Handling

Recommended behavior:

- Database cannot open: fail startup.
- Migration fails: fail startup.
- Save fails during a turn: return an error and do not send a successful final
  reply.
- Single session cannot load because required state is corrupt: return a clear
  error for that session and do not overwrite it.
- Optional metadata cannot deserialize: drop that metadata field for the loaded
  aggregate and log a warning.
- Context files are present but store rows are missing: use manifest recovery as
  already implemented.

The router should prefer failing loudly over silently resetting a user's durable
session.

## Migration Strategy

Add a `schema_migrations` table:

```text
version
applied_at_ms
```

Rules:

- Version `1` creates the initial schema.
- Migrations run at store startup.
- Each migration runs in a transaction.
- Failed migration aborts startup.
- Downgrades are not automatic.

## Tests

Required tests for `SqliteSessionStore`:

- `save` then create a new store instance and `load` the same session.
- `load_or_create` returns existing state without overwriting it.
- transcript message order survives restart.
- executor bindings survive restart.
- binding health is reset to `Unknown` on load.
- context artifacts survive restart.
- machine workspaces survive restart.
- malformed required session record returns an error.
- empty database is migrated to schema version `1`.

Required router integration tests:

- Completed turn persists user and assistant transcript entries.
- Interrupted turn does not persist partial output.
- Stale turn cannot overwrite a newer committed turn.
- `/agent <executor>` survives restart.
- `/agent done` survives restart.
- `/yolo on`, `/yolo off`, and `/yolo inherit` survive restart.
- Executor receives `previous_session_id` after restart.
- Context projection after restart only includes unseen context.

## Implementation Plan

1. Add `StorageConfig` to `AppConfig`.
2. Add platform-specific default storage path resolution.
3. Add `SqliteSessionStore` behind the existing `SessionStore` trait.
4. Add SQLite migrations.
5. Change `main.rs` to construct `SqliteSessionStore` by default.
6. Keep `InMemorySessionStore` for tests.
7. Add store-level persistence tests.
8. Add router restart integration tests.
9. Update README and example config.

## Open Decisions

- Whether to keep `save(SessionState)` as full aggregate replacement long term
  or introduce explicit commit methods for transcript, bindings, and context.
- Whether to add a session export/import command.
- Whether transcript retention should be unlimited or configurable.
- Whether old executor binding records should be pruned when executors are
  removed from configuration.

## Conclusion

Agent Router should persist committed router session semantics, not runtime
execution. The durable record should cover the full `SessionState` aggregate:
identity, active executor, transcript, context artifacts, machine workspaces,
executor bindings, and approval override. SQLite is the right first backend
because it gives atomic commits, migration support, and cross-platform behavior
without introducing a service dependency.
