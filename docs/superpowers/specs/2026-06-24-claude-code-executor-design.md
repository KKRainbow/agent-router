# Claude Code Executor Backend Design

## Goal

Add first-class support for Anthropic's Claude Code CLI to agent-router as a native executor backend. Users will be able to configure a `claude` executor and switch to it with `/agent claude`, just like existing ACP and Codex app-server executors.

## Background

Claude Code CLI does not expose a documented server protocol such as ACP or Codex app-server. It does, however, support a machine-readable stdio mode:

- `--input-format stream-json`
- `--output-format stream-json`
- `--permission-prompt-tool stdio`
- `--replay-user-messages`
- `--resume <session_id>`

This lets a host process spawn a long-running `claude` child process and drive it by writing JSON lines to stdin and reading JSON lines from stdout. The integration strategy is to add a new executor backend protocol (`claude_stream_json`) that speaks this line protocol.

This approach mirrors how agent-router already hosts ACP and Codex app-server backends, and is similar in spirit to the implementation in `cc-connect` (Go), but scoped to agent-router's existing abstractions.

## Non-Goals

- Do not add a separate wrapper process or external ACP shim.
- Do not support Claude Agent SDK; this design targets the `claude` CLI only.
- Do not support every Claude Code CLI feature on day one (e.g. plugins, OS-user isolation, custom provider proxies are out of scope).

## Architecture

Add a third backend implementation alongside the existing two:

```text
src/executor/
├── mod.rs
├── acp.rs
├── codex_app_server.rs
├── claude_stream_json.rs   <-- new
└── registry.rs
```

The router core and channel adapters remain unchanged. The new backend implements the `ExecutorBackend` trait:

- `get` / `list`
- `prepare`
- `prompt`
- `interrupt`

### New Configuration Protocol

```rust
pub enum ExecutorProtocol {
    Acp,
    AppServer,
    ClaudeStreamJson,
}
```

YAML usage:

```yaml
router:
  default_executor: claude

executors:
  claude:
    protocol: claude_stream_json
    command: claude
    args: []
    cwd: /data/project/hermes
    env:
      ANTHROPIC_API_KEY: ...
```

The config parser accepts `claude_stream_json` as the protocol string.

## Components

### `ClaudeStreamJsonManager`

- Owns the `BTreeMap<String, ExecutorConfig>` filtered to `ClaudeStreamJson` executors.
- Maintains a `HashMap<(session_key, executor_name), Arc<Mutex<ClaudeSession>>>` of active sessions.
- Implements `ExecutorBackend`.
- On `prepare`:
  - Looks up the executor config.
  - Finds an existing matching session or spawns a new `ClaudeSession`.
  - Resumes with `--resume <external_session_id>` if a previous session id is available.
- On `prompt`:
  - Acquires the existing session, sends the user prompt as a stream-json `user` event, and consumes events until a `result` event with `Done=true`.
- On `interrupt`:
  - Closes the session's stdin pipe and/or kills the process group.

### `ClaudeSession`

Wraps one `claude` child process and its stdio pipes:

- Spawns `claude` with the flags listed in Background.
- Writes JSON lines to stdin under a mutex.
- Runs a background task that reads stdout line-by-line and dispatches events.
- Emits `ExecutorUpdate` events through a broadcast channel or directly to the event sink.
- Tracks the current Claude session id reported in `system` / `result` events.
- Handles clean shutdown: close stdin, wait for graceful exit, escalate to SIGTERM/SIGKILL.

### Event Mapping

Incoming stream-json events are mapped to agent-router's executor model:

| Claude event | Router action |
|--------------|---------------|
| `system` with `session_id` | Store external session id |
| `assistant` `text` | Emit `ExecutorUpdate` with `kind="agent_message_chunk"` |
| `assistant` `thinking` | Emit `ExecutorUpdate` with `kind="reasoning_summary"` |
| `assistant` `tool_use` | Emit `ExecutorUpdate` with `kind="tool_call"` |
| `user` `tool_result` | Emit `ExecutorUpdate` with `kind="tool_result"` (or fold into tool call status) |
| `result` non-compaction | Return `ExecutorResponse` with the final `result` text |
| `control_request` | Translate to `ApprovalRequest`, forward to `ApprovalBroker`, write `control_response` back to stdin |
| `control_cancel_request` | Drop pending approval |

### Permission Handling

Claude Code's `--permission-prompt-tool stdio` emits `control_request` events when a tool needs approval. The backend:

1. Parses the request id, tool name, and suggested input.
2. Builds an `ApprovalRequest` using the existing approval types.
3. Calls `approvals.request_until_cancelled(...)` with the turn's cancellation token.
4. Writes a `control_response` JSON line back to Claude's stdin with `behavior: allow` or `behavior: deny`.

## Data Flow

1. Router calls `prepare` with the previous external session id.
2. `ClaudeStreamJsonManager` returns an existing session or spawns a new one; if resuming, passes `--resume <id>`.
3. Router calls `prompt` with the user text.
4. `ClaudeSession` writes a `user` event to stdin.
5. Claude streams events to stdout; the backend forwards them to the `ExecutorEventSink`.
6. On `result` with no `subtype` (or `subtype` other than `compact`/`compaction`), the backend returns `ExecutorPromptOutcome::Completed`. Compaction results continue the turn.
7. If the turn is cancelled, the backend closes stdin / kills the process and returns `ExecutorPromptOutcome::Cancelled`.

## Context Projection

Claude Code maintains its own conversation state when resumed with `--resume`. The router still owns the canonical transcript, but for the common case the backend can rely on Claude's session continuity:

- First time: spawn fresh process.
- Subsequent turns: resume with `--resume <session_id>` and let Claude replay its own history (`--replay-user-messages`).

When switching **away** from Claude and then back, the router may need to project missed transcript entries. The backend can accept a handoff prompt that includes a compact summary of what happened while Claude was inactive, injected as the user message or appended system prompt. The exact handoff prompt format is left as an implementation detail to be refined during development.

## Error Handling

- **Process fails to start**: return `ExecutorPromptOutcome::Failed` on prompt, mark binding unhealthy on prepare.
- **Non-JSON stdout line**: log at debug level and continue (Claude's `--verbose` may occasionally emit plain text).
- **Unexpected process exit during turn**: emit an error event and return `Failed`.
- **Cancellation**: close stdin first; if the process does not exit within a timeout, send SIGTERM then SIGKILL. Use process groups to avoid leaving MCP bridge grandchildren behind.
- **Permission timeout / cancellation**: respond with `behavior: deny`.

## Testing Strategy

- Unit tests for the stream-json event parser using captured example payloads.
- Unit tests for permission request/response serialization.
- A fake `claude` helper script (Python or shell) that emits predetermined stream-json lines, used to test the full `prepare`/`prompt`/`interrupt` lifecycle.
- Manual end-to-end test with a real `claude` CLI and a test Slack/QQ channel.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Stream-json protocol is undocumented and may change | Pin supported CLI version range in docs; keep parser lenient; add tests that fail visibly on unexpected shapes |
| `--verbose` can emit non-JSON lines | Either drop `--verbose` or filter non-JSON lines in the read loop |
| Permission model differences | Map `Normal` to `--permission-mode default`; map `Yolo` to `--permission-mode bypassPermissions` (fall back to `auto` when `bypassPermissions` is rejected, e.g. running as root) |
| Session resume may fail if Claude's local state is lost | Fall back to spawning a fresh session and let context projection fill the gap |

## Configuration Example

```yaml
workspace:
  root: /data/project/hermes-workspaces

router:
  default_executor: claude

executors:
  claude:
    protocol: claude_stream_json
    command: claude
    args: []
    cwd: /data/project/hermes
    env:
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}

  kimi:
    protocol: acp
    command: kimi
    args: ["acp"]
```

## Work Estimate

Approximately **1 week** of focused work:

- Config + registry plumbing: 0.5 day
- Session spawn / stdio / lifecycle: 1.5 days
- Stream-json event parser and mapping: 1.5 days
- Permission bridge + cancellation: 1.5 days
- Tests + manual validation: 1 day
- Buffer for edge cases: 1 day

## References

- `/tmp/cc-connect/agent/claudecode/` — a Go implementation that drives Claude Code CLI with the same `--input-format stream-json` / `--output-format stream-json` / `--permission-prompt-tool stdio` approach. Useful reference for process spawning, event parsing, permission handling, and session resume semantics.

## Open Questions

1. Should `--verbose` be enabled by default? It adds useful logs but risks corrupting the JSON stream.
2. Should the backend support `--model` / `--effort` overrides via executor config, or leave that to `CLAUDE.md`/user settings?
3. How should we handle multi-modal attachments? Out of scope for MVP, but the event format supports base64 images.
