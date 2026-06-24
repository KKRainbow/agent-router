# ADR 0004: Make Machine a First-Class Execution Resource

## Status

Accepted.

## Context

Agent Router needs to run executors in environments that may not be the local
host. A near-term example is ACP over SSH: the router can connect stdio to a
remote ACP process, but the process must receive a cwd that exists on the remote
host.

The same remote access problem is not limited to ACP. Skill management also
needs to collect configured skill roots from the environment where an executor
runs. If SSH configuration, remote paths, workspace creation, and remote file
collection are hidden inside executor-specific command wrappers, each feature
will need to rediscover the same remote Machine facts.

The current local-only model treats `cwd` as one local `PathBuf`. That is not a
valid interface for remote execution because the Router Workspace and executor
visible cwd can be different paths on different Machines.

## Decision

Agent Router will model `Machine` as a first-class configured resource.

Executors reference a Machine by id instead of embedding remote connection
details in `command` or `args`. The default Machine is `local`, preserving the
existing local execution model for executors that do not opt into a different
Machine.

A Machine adapter owns:

- creating or resolving a session Machine Workspace
- spawning stdio commands inside that Machine Workspace
- applying Machine-level environment configuration
- collecting configured Skill Roots
- reporting connection, permission, and workspace errors in Machine terms

ACP remains an executor backend protocol. SSH is a Machine adapter detail, not a
new executor protocol and not an ACP-specific transport concept.

Session state must distinguish:

- the Router Workspace where router-owned artifacts are written
- per-Machine Workspaces where executors and skill collectors operate
- the Machine id and effective cwd recorded in each Executor Binding

The path sent to an ACP backend as `cwd` is the Machine Workspace path visible to
that executor. It must not be assumed to be a local path unless the executor
uses the local Machine.

## Consequences

Machine becomes the shared seam for local and remote execution. ACP, future
backend protocols, workspace materialization, and skill management can reuse the
same Machine interface instead of carrying duplicated SSH and path semantics.

The first implementation should add a local Machine adapter and an SSH Machine
adapter. The local adapter can keep using the existing process spawning behavior.
The SSH adapter should use non-interactive stdio, avoid allocating a TTY, create
the remote workspace before command start, and reserve stdout for the backend
protocol.

Workspace materialization becomes explicit. Router-owned artifacts remain local
source-of-truth data, then are copied or otherwise made available to a Machine
Workspace when that Machine needs file access. Reverse synchronization is not
implicit; remote outputs become router-owned artifacts only through an explicit
future workflow.

Skill collection should use Machine skill roots. A collected skill identity must
include the Machine id and root-relative path so two Machines can expose skills
with the same filename without collision.

This decision does not change the router's active executor model from ADR 0003
or ACP's role from ADR 0002. It deepens the execution environment interface that
backend protocol adapters use.
