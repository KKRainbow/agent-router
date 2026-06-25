# Remote Machines, Workspaces, and Skill Collection

## Goal

Support executors and skill management on local and remote Machines through one
shared Machine interface.

The immediate use case is ACP over SSH. A remote ACP process must run with a cwd
that exists on the remote Machine, and it may need access to router-owned
workspace artifacts. The same Machine definition should later support collecting
skills from that remote environment.

## Non-Goals

- Do not hide SSH inside `executors.*.command` wrappers.
- Do not make SSH an ACP-specific transport concept.
- Do not treat a remote path as a local `PathBuf`.
- Do not add transparent bidirectional workspace sync. Remote writes become
  router-owned artifacts only through an explicit future workflow.

## Configuration Shape

Machines are configured separately from executors:

```yaml
workspace:
  root: /data/project/agent-router-workspaces

machines:
  local:
    type: local
    workspace_root: /data/project/agent-router-workspaces
    skill_roots:
      - /home/admin/.codex/skills

  zbs-dev:
    type: ssh
    host: admin@172.17.0.2
    workspace_root: /data/project/agent-router-workspaces
    env:
      PATH: /home/admin/.nvm/versions/node/v24.14.0/bin:$PATH
    skill_roots:
      - /home/admin/.codex/skills
      - /data/project/ai/skills

executors:
  codex:
    protocol: app_server
    machine: local
    command: codex

  codex-zbs:
    protocol: acp
    machine: zbs-dev
    command: /home/admin/.nvm/versions/node/v24.14.0/bin/codex-acp
```

`machines.local` is implicit when omitted. Existing executors default to
`machine: local` unless configured otherwise.

Machine-level `env` applies to commands spawned on that Machine. Executor-level
environment, if supported, is layered on top of Machine-level environment.

## State Model

Each session stores:

- `router_workspace`: local path for router-owned artifacts
- `machine_workspaces`: map of Machine id to Machine Workspace record
- existing executor routing state from the session executor workflow

Each Machine Workspace record stores:

- Machine id
- executor-visible cwd string
- materialization status
- last materialized artifact cursor or fingerprint set, when available

Each Executor Binding stores:

- protocol name
- Machine id
- external session id
- effective executor-visible cwd
- health
- seen-context cursor

The Router Workspace and a local Machine Workspace may refer to the same
directory. A remote Machine Workspace is only a remote path string plus Machine
identity; callers must go through the Machine interface to operate on it.

When a Machine has a configured `workspace_root`, the session Machine Workspace
is created under a Machine-scoped directory inside that root:

```text
<workspace_root>/<machine-id-component>/<session-component>
```

Both path components are router-generated stable names. This keeps two Machines
with the same `workspace_root` from sharing a session workspace accidentally.

## Machine Interface Semantics

The Machine interface should hide the operational details that callers otherwise
repeat:

- resolve or create a session Machine Workspace
- spawn a stdio command inside a Machine Workspace
- copy or materialize router-owned artifacts into a Machine Workspace
- collect Skill Roots
- report Machine-scoped errors

Callers should not assemble `ssh` commands themselves. They should not quote
remote shell fragments, create remote directories, or infer remote paths.

## ACP Execution

The ACP backend asks the Machine registry for the executor's Machine, then asks
that Machine to create a Machine Workspace for the session.

For a local Machine, ACP process spawning remains equivalent to:

- create local cwd
- `Command::new(command).args(args).current_dir(cwd)`
- send ACP `session/new` or `session/load` with the local cwd

For an SSH Machine, the SSH adapter owns:

- opening a non-interactive SSH command
- using no TTY allocation
- creating the remote Machine Workspace before starting the backend command
- starting the remote command with cwd set to the remote Machine Workspace
- passing only backend protocol JSON-RPC over stdout/stdin
- logging remote stderr separately

The cwd passed in ACP `session/new`, `session/load`, or `session/resume` is the
Machine Workspace path visible to the ACP process.

The SSH adapter must fail clearly if remote login scripts, banners, or shell
configuration write to stdout before the ACP protocol begins.

## Workspace Materialization

Router-owned artifacts are written first to the Router Workspace. Examples:

- Slack current-thread markdown and jsonl
- Slack manifest
- downloaded file metadata
- extracted text

Before an executor turn, the router decides whether the target executor needs
workspace files. If yes, it asks the Machine to materialize the Router Workspace
into the session Machine Workspace.

For a local Machine, materialization may be a no-op when both workspaces are the
same directory.

For an SSH Machine, materialization can initially use a simple whole-workspace
copy. A later implementation can add incremental sync with artifact
fingerprints. Silent truncation or skipped files are not allowed; failures must
be represented in the manifest or fail the turn before executor start.

## Skill Collection

Skill management uses the same Machine definitions.

For each configured Machine, the skill collector asks the Machine adapter to
collect configured Skill Roots. The result should include:

- Machine id
- root path
- root-relative skill path
- skill metadata
- readable instruction content or a structured read error

This keeps SSH credentials, remote path handling, and remote command execution
inside the Machine adapter instead of duplicating them in skill management.

Collected skill identities should include Machine id and root-relative path.
This avoids collisions when different Machines expose a skill with the same
directory name.

## Failure Modes

Machine failures should be reported before backend protocol errors when the
backend never started. Examples:

- Machine is not configured
- SSH connection failed
- remote workspace could not be created
- remote command could not be found
- workspace materialization failed
- skill root does not exist or cannot be read

Once the backend process starts and speaks ACP, ACP protocol errors remain ACP
errors.

## Implementation Order

1. Add Machine configuration and a Machine registry with a local adapter.
2. Default existing executors to `machine: local`.
3. Move session cwd resolution for ACP and app-server executors behind Machine
   Workspace creation.
4. Add the SSH Machine adapter for stdio process spawning and remote workspace
   creation.
5. Add workspace materialization from Router Workspace to Machine Workspace.
6. Add skill collection through Machine Skill Roots.
7. Add incremental materialization and remote health checks after the simple
   path is stable.

## Test Coverage

The Machine interface is the main test surface.

Required coverage:

- config parsing accepts local and SSH Machines
- existing executors default to the local Machine
- session Machine Workspace names are stable per session and per Machine
- ACP receives executor-visible cwd, not the Router Workspace path, for remote
  Machines
- SSH command construction uses non-interactive stdio and no TTY
- workspace materialization reports failures before executor start
- skill collection includes Machine id in collected skill identity
