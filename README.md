# Agent Router

> A router between IM and your agent team.

Agent Router connects instant messaging channels such as Slack and QQ to your
agent team: Codex, Claude Code, Kimi, and other agent runtimes that can be
exposed through executor adapters. Users can talk to those agents from the chat
tools they already use, while Agent Router owns channel integration, session
routing, executor switching, and reply delivery.

## Development Status

Agent Router is still in active development. Configuration, commands, backend
protocols, channel behavior, and supported agent adapters may change before the
project is considered stable. It is currently best suited for experiments,
internal workflows, and early integrations.

## What It Does

- Receives messages from supported IM channels.
- Normalizes channel messages into a shared session model.
- Starts each session with a configured default agent.
- Routes a session to one active agent at a time.
- Lets users switch agents inside the same chat session.
- Projects conversation context when a session switches agents.
- Sends normalized replies back to the originating channel.

## Supported Today

Channels:

- Slack Socket Mode
- Tencent QQ Official Bot Gateway

Agent integrations:

- Kimi through ACP, for example `kimi acp`
- Codex through app-server

Other agents, such as Claude Code, are intended to be added through executor
adapters as their protocol integrations land.

User commands:

```text
/agent status
/agent <agent-name>
```

For example:

```text
/agent kimi
/agent codex
```

## Quick Start

Requirements:

- Rust toolchain
- Credentials for at least one supported channel
- At least one configured agent command available on `PATH`

Run with the example configuration:

```bash
cargo run -- --config config/agent-router.example.yaml
```

For local changes, copy the example configuration and edit the agent names,
commands, and channel options:

```bash
cp config/agent-router.example.yaml config/agent-router.yaml
cargo run -- --config config/agent-router.yaml
```

## Secrets

Prefer environment variables for secrets. The runner also tries these dotenv
locations, in order, so a local Hermes install can be reused without copying
tokens into this repository:

1. `.env`
2. `../.env`
3. `$HERMES_HOME/.env`
4. `~/.hermes/.env`

Slack:

```bash
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
```

QQ:

```bash
QQ_APP_ID=...
QQ_CLIENT_SECRET=...
QQ_SANDBOX=false
```

Slack and QQ are enabled automatically when all required credentials for that
channel are present, unless the config explicitly sets `enabled: false`.
When an executor reports safe tool-call or reasoning-summary updates, Agent
Router streams those updates back to the originating channel while the turn is
still running, before the final assistant reply.

QQ access can be restricted with comma-separated openid lists:

```bash
QQ_ALLOWED_USERS=...
QQ_ALLOWED_GROUPS=...
```

## Configuration Overview

The example config defines a default agent, available agent backends, and
channel options:

```yaml
router:
  default_executor: kimi

executors:
  kimi:
    protocol: acp
    command: kimi
    args: ["acp"]
  codex:
    protocol: app_server
    command: codex

slack:
  require_mention: true
  allowed_channels: []
  free_response_channels: []

qq:
  sandbox: false
  allowed_users: []
  allowed_groups: []
```

Each chat session has one active agent. New sessions start with
`router.default_executor`. Users can switch the active agent with
`/agent <agent-name>`.

## Non-Goals

- Replacing Hermes, OpenClaw, Codex, Claude Code, Kimi, or other agent runtimes.
- Importing a channel adapter wholesale as a runtime dependency.
- Building a full customer-support inbox product.
- Coupling channel adapters to LLM provider, memory, tool, or orchestration
  implementations.

## Documentation

- [Architecture](docs/architecture.md)
- [Session Executor Routing Workflow](docs/workflows/session-executor-routing.md)
- [Remote Machines, Workspaces, and Skill Collection](docs/workflows/remote-machines.md)
- [ADR 0001: Split Channel Router From Agent Runtime](docs/adr/0001-router-runtime-boundary.md)
- [ADR 0002: Use ACP as the First Backend Protocol](docs/adr/0002-acp-first-backend-protocol.md)
- [ADR 0003: Default Executor, Active Executor, and Shared Context](docs/adr/0003-default-active-executor-context.md)
- [ADR 0004: Make Machine a First-Class Execution Resource](docs/adr/0004-machine-first-remote-execution.md)
