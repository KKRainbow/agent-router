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

### Executor

A configured agent identity that can handle a session turn, such as Codex,
Kimi, Hermes, or a future runtime. The user switches executors by name; the
configuration decides which backend protocol and Machine serve that executor.

### Executor Binding

Per-session private state for one executor. It records backend-private data such
as protocol name, external session id, Machine id, effective cwd, health, and
seen-context cursor.

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
