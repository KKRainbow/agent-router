# Domain Context

This file records Agent Router domain terms that should stay stable across
architecture documents, implementation names, and tests.

## Terms

### Agent Router

The process that owns channel adapters, user-visible sessions, executor
routing, shared context projection, and reply delivery.

### Channel Adapter

An adapter that translates an external channel, such as Slack or QQ, into
router inputs and delivers router outputs back to that channel.

### Session

A durable user-visible conversation identity. A session may map to a Slack
thread, QQ user, QQ group, CLI conversation, or webhook conversation key.

### Turn

One accepted user message being routed through Agent Router to an Executor. A
Turn covers router-side preparation, context projection, backend preparation,
prompt execution, output projection, and final state commit.

### Turn Reservation

An opaque router-owned claim on the next Turn slot for a Session. A Channel
Adapter may create a Turn Reservation before slow channel-side context fetch so
that a newer message can supersede older work immediately. A Turn Reservation
does not expose generation arithmetic to Channel Adapters.

### Turn Guard

An opaque router-owned handle proving that a Turn is still the current Turn for
its Session. Durable transcript updates, context cursor updates, Executor
Binding updates, and final replies must be committed through a Turn Guard.

### Executor

A configured agent identity that can handle a session turn, such as Codex,
Kimi, Hermes, or a future runtime. The user switches executors by name; the
configuration decides which backend protocol and Machine serve that executor.

### Executor Binding

Per-session private state for one executor. It records backend-private data such
as protocol name, external session id, Machine id, effective cwd, health, and
seen-context cursor.

### Backend Session

A live protocol-private conversation or process owned by an Executor backend
adapter, such as an ACP session or Codex app-server thread. Backend Sessions may
outlive one Turn and must not be destroyed by a superseded Turn after they have
been published for reuse.

### Machine

A named execution environment that Agent Router can operate through a stable
interface. A Machine may be local or remote. It owns how commands are spawned,
how session workspaces are created, and how configured skill roots are collected
inside that environment.

### Local Machine

The Machine where Agent Router itself is running. It can spawn processes and
read files directly through the local filesystem.

### Remote Machine

A Machine reached through a remote adapter such as SSH. Router callers should
not need to know SSH flags, remote path creation, shell quoting, or remote
workspace layout.

### Router Workspace

The local workspace where Agent Router materializes router-owned artifacts, such
as synced Slack thread files, manifests, and extracted file text.

### Machine Workspace

The session workspace as seen from a specific Machine. A local Machine Workspace
may be the same path as the Router Workspace. A Remote Machine Workspace is a
remote path and must not be treated as a local `PathBuf`.

### Workspace Materialization

The act of making router-owned artifacts available in a Machine Workspace. For a
remote Machine this may require copying files before an executor turn or skill
operation.

### Skill Root

A configured directory on a Machine that may contain skills. Skill management
collects from Machine skill roots instead of duplicating remote access settings.
