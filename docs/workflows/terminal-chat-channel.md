# Terminal Chat Channel Implementation

## Goal

Agent Router should be useful as a local command-line chat client without
requiring Slack, QQ, Telegram, or Web to be configured. A user should be able to
run the binary in a terminal, type prompts, receive streamed output, and type a
new prompt while the current turn is still running.

The first implementation should be a terminal REPL channel, not a full-screen
`ratatui` application. The REPL keeps the initial surface small and validates
the router/channel boundary. A richer full-screen UI can be layered on later
without changing router semantics.

## User-Facing Commands

The command surface should separate interactive local chat from long-running
external channel service mode.

```text
agent-router
agent-router --config a.yaml
agent-router chat
agent-router chat --config a.yaml
agent-router serve
agent-router serve --config a.yaml
```

Rules:

- `agent-router` in a TTY defaults to `agent-router chat`.
- `agent-router --config a.yaml` in a TTY also defaults to
  `agent-router chat --config a.yaml`.
- `agent-router chat` starts only the local terminal channel.
- `agent-router serve` starts configured external channels, such as Slack, QQ,
  Telegram, and Web.
- `--config` selects configuration input only. It must not imply service mode.
- If no subcommand is provided and stdin or stdout is not a TTY, return a clear
  error asking the caller to choose `chat` or `serve`.
- If `chat` is selected while external channels are enabled in the config, do
  not start them.
- If `serve` is selected, do not start the terminal chat channel.

This keeps accidental background channel startup out of the common local
workflow and makes scripts explicit.

## Non-Goals

- Do not implement a full-screen TUI in the first pass.
- Do not introduce `ratatui` until there is a product requirement for a stable
  input box, scrollback pane, status bar, or keyboard-driven layout.
- Do not bypass the router by calling executors directly.
- Do not create terminal-specific routing logic inside the router.
- Do not implement compatibility shims for legacy no-subcommand service startup.
  `serve` is the explicit service entrypoint.

## Architecture

The terminal chat must be a normal channel adapter.

```text
stdin reader task
        |
        v
TerminalChatChannel
        |
        v
ChannelInput
        |
        v
RouterService::begin_channel_input
        |
        v
RouterService::finish_channel_input
        |
        v
TerminalOutputSink
        |
        v
stdout/stderr
```

The channel owns terminal I/O and user-visible session identity. The router owns
turn replacement, slash commands, executor selection, approval behavior,
transcript persistence, and stale output suppression.

## Module Layout

Add a new channel module:

```text
src/channel/tui.rs
```

The module name may be `tui` for user-facing terminology even though the first
implementation is a REPL. Internally, prefer concrete names such as
`TerminalChatChannel` and `TerminalOutputSink` to avoid implying a full-screen
UI exists.

Update:

```text
src/channel/mod.rs
src/config.rs
src/main.rs
README.md
config/agent-router.example.yaml
```

`README.md` and the example config should document the new command split and
the local chat workflow.

## Configuration

Add:

```rust
pub struct TuiConfig {
    pub enabled: bool,
    pub session_id: String,
    pub channel_events: ChannelEventMode,
}
```

Suggested defaults:

- `enabled`: `false`
- `session_id`: `local`
- `channel_events`: `compact`

Environment variables:

```text
TUI_ENABLED=true
TUI_SESSION_ID=local
TUI_CHANNEL_EVENTS=compact
```

YAML:

```yaml
tui:
  enabled: false
  session_id: local
  channel_events: compact
```

`chat` mode should start the terminal channel even if `tui.enabled` is false.
The `enabled` flag exists for symmetry and for any future `serve` mode decision,
but explicit `chat` is a stronger user intent than config.

Session key:

```text
tui:user:local:<session_id>
```

The source should be:

```text
tui
```

The user id should be:

```text
local
```

## CLI Parsing

Replace the single flat CLI with subcommands:

```rust
#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, env = "AGENT_ROUTER_CONFIG")]
    config: Option<PathBuf>,

    #[arg(long, env = "AGENT_ROUTER_ENV_FILE")]
    env_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Chat,
    Serve,
}
```

Startup mode resolution:

```rust
enum RunMode {
    Chat,
    Serve,
}
```

Resolution rules:

1. Explicit `chat` maps to `RunMode::Chat`.
2. Explicit `serve` maps to `RunMode::Serve`.
3. No command with TTY stdin and stdout maps to `RunMode::Chat`.
4. No command without TTY stdin or stdout is an error.

Use a small helper for this decision so it can be unit-tested without launching
the runtime:

```rust
fn resolve_run_mode(command: Option<Command>, stdin_is_tty: bool, stdout_is_tty: bool)
    -> anyhow::Result<RunMode>
```

Use `std::io::IsTerminal` for TTY detection.

## Logging

Interactive chat must not print normal tracing logs into the chat transcript.

For `chat` mode:

- write tracing logs to a file under the user's state directory;
- print only fatal startup/config errors to stderr;
- keep stdout reserved for chat UI output.

Suggested path:

```text
$XDG_STATE_HOME/agent-router/agent-router.log
```

Fallbacks:

- Linux/macOS: `~/.local/state/agent-router/agent-router.log`
- Windows: use an appropriate local app data state path if a directory helper
  is already present in the codebase; otherwise use the same config/workspace
  family of paths and document the choice.

For `serve` mode, existing terminal tracing can remain unchanged unless it
conflicts with service deployment requirements.

If file logging is too large for the first patch, implement a minimal split:

- `chat`: tracing goes to stderr and chat rendering uses stdout;
- `serve`: current behavior.

The preferred final behavior is file logging for `chat`.

## Terminal Input Model

Do not implement the naive loop:

```text
read prompt
await turn completion
read next prompt
```

That loop cannot receive a new prompt while the current turn is running.

Instead:

1. Spawn an input task that continuously reads lines from stdin.
2. Send each line to the channel event loop through `tokio::sync::mpsc`.
3. Spawn each routed turn as its own task.
4. Let the router's normal turn replacement semantics handle interrupts.

The reader task should:

- trim trailing `\r\n`;
- ignore empty lines unless a later multiline input mode gives them meaning;
- treat `/exit` and `/quit` as terminal-channel local exit commands;
- send every other line as a prompt.

`/stop`, `/new`, `/agent`, `/yolo`, `/approve`, and `/deny` should go through
the router as normal text so existing command semantics are preserved.

## Interrupt Semantics

The terminal channel does not need a special interrupt API.

For normal prompts, `begin_channel_input` creates a replacement reservation
through the existing intake path. This maps to `TurnBeginMode::ReplaceActive`
for non-approval, non-router-control prompts.

Expected behavior:

1. User sends prompt A.
2. Terminal channel starts turn A in a background task.
3. User sends prompt B before turn A finishes.
4. Terminal channel starts turn B through the same `ChannelInput` path.
5. Router cancels/replaces active turn A.
6. Stale output from turn A is suppressed by router-owned turn output handling.
7. Turn B output becomes the visible active output.

The terminal channel should not track router generations or decide which output
is stale. That belongs to the router.

## Output Sink

Implement a terminal-specific sink:

```rust
struct TerminalOutputSink {
    channel_events: ChannelEventMode,
    activity_events: Vec<RouterChannelEvent>,
}
```

It implements `RouterOutputSink`.

Behavior:

- `send_reply_chunk`: print streamed reply text to stdout and flush.
- `send_reply_break`: print a message break using existing text semantics where
  practical.
- `send_channel_event`:
  - `off`: ignore;
  - `compact`: keep recent activity and render compact live summaries;
  - `verbose`: print each safe router channel event as it arrives.
- `discard_reply_stream`: stop displaying the current stream and print a short
  local notice only if needed to keep the terminal understandable.
- `send_final_reply`: ensure final text is printed exactly once.

Avoid double-printing final output. If chunks have already streamed the full
reply, `send_final_reply` should only close formatting and draw the next prompt.
If no chunks were printed, it should print the final reply.

Because normal stdout can interleave with typed input, the first version may
accept imperfect line redraw. Correctness is that input is accepted and router
turns are replaced. A full-screen redraw model can be added later.

## Prompt Rendering

The REPL prompt can be minimal:

```text
agent-router
executor: <default_executor>
session: <session_id>

> 
```

After each submitted line, print a separator or assistant prefix:

```text
assistant>
```

Keep UI copy short. Do not print a help page on every startup. `/agent status`
already exposes useful router state.

## Router Integration

Terminal prompt routing should mirror Web's `route_web_text` shape:

```rust
let outcome = router
    .begin_channel_input(ChannelInput {
        session_key,
        text,
        user_id: Some("local".to_string()),
        source: "tui".to_string(),
        intent: ChannelInputIntent::Route,
        context_policy: ChannelContextPolicy::disabled("tui"),
    })
    .await?;

let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
    return Ok(());
};

router.finish_channel_input(ticket, None, output).await
```

Do not add terminal-only shortcuts for router-owned commands. If a command is
global to all channels, it belongs in router command handling, not in the
terminal channel.

Local-only commands are limited to process/UI lifecycle, such as:

```text
/exit
/quit
```

## Service Mode Integration

Move existing channel spawning into a function:

```rust
async fn run_serve_mode(config: AppConfig, router: Arc<dyn RouterService>, ...)
    -> anyhow::Result<()>
```

Add chat startup:

```rust
async fn run_chat_mode(config: AppConfig, router: Arc<dyn RouterService>)
    -> anyhow::Result<()>
```

Keep shared router construction in one path so chat and serve use identical
executor, approval, workspace, orchestrator, and session persistence behavior.

The current `anyhow::ensure!(!channels.is_empty(), ...)` should apply only to
`serve` mode. Chat mode has exactly one local channel and does not need external
channel credentials.

Update the service-mode error to mention `serve`:

```text
no external channels enabled; configure Slack, Telegram, QQ, or web credentials,
or enable a channel, then run `agent-router serve`
```

## Cross-Platform Notes

The first version should use portable terminal primitives:

- `std::io::IsTerminal` for TTY detection;
- Tokio stdin reading for async input;
- stdout flushing after streamed chunks;
- no Unix-only signal dependencies in the channel.

Avoid raw terminal mode until a full-screen UI is implemented. Raw mode creates
extra cleanup requirements on panic, Ctrl-C, resize, and process termination.

## Testing Plan

Unit tests:

- run-mode resolution:
  - no subcommand + TTY => chat;
  - no subcommand + non-TTY => error;
  - `chat` => chat regardless of TTY;
  - `serve` => serve regardless of TTY.
- config parsing:
  - default `tui` config;
  - YAML `tui` section;
  - `TUI_*` environment overrides;
  - invalid `tui.channel_events`.
- terminal session key construction.
- terminal output sink:
  - chunks are printed in order;
  - final reply is not duplicated after chunks;
  - final reply prints when no chunks were streamed;
  - compact/verbose/off channel event policies behave as expected.

Integration-style tests:

- a fake router records `ChannelInput` from a prompt and verifies source,
  session key, user id, intent, and disabled context policy.
- two prompts submitted while the first fake turn is blocked cause two routed
  inputs; the second prompt uses the normal replacement path.
- `/stop` is forwarded to router rather than consumed locally.
- `/exit` exits the terminal channel locally and is not routed.

Manual verification:

```bash
cargo test
cargo run -- chat --config config/agent-router.example.yaml
cargo run -- --config config/agent-router.example.yaml
cargo run -- serve --config config/agent-router.example.yaml
```

During manual chat verification:

1. Send a long-running prompt.
2. Type a second prompt while output is streaming.
3. Confirm the second prompt is accepted immediately.
4. Confirm old-turn output does not continue as the active answer.
5. Run `/agent status`.
6. Run `/stop`.
7. Run `/exit`.

## Implementation Steps

1. Add CLI subcommands and run-mode resolution tests.
2. Split router construction from channel startup in `main.rs`.
3. Add `TuiConfig` parsing and tests.
4. Add `src/channel/tui.rs` with session key construction and output sink tests.
5. Implement terminal input reader and channel event loop.
6. Wire `chat` mode to `TerminalChatChannel`.
7. Move existing Slack/QQ/Telegram/Web startup behind `serve`.
8. Adjust logging so chat output is not polluted by normal tracing logs.
9. Update README and example config.
10. Run tests and manual smoke checks.

## Future Full-Screen TUI

`ratatui` becomes worthwhile when the product requires a stable interactive
layout:

- persistent input box while output streams;
- scrollable transcript;
- status bar for active executor, session, approval mode, and current turn;
- keyboard shortcuts;
- structured rendering for tool activity and approvals;
- clean redraw on resize.

When adding that layer, keep the same router integration. Replace only the
terminal input/output presentation:

```text
TerminalChatChannel event loop stays
RouterService integration stays
RouterOutputSink semantics stay
stdout REPL renderer becomes ratatui renderer
```

The first REPL version should therefore keep state transitions explicit and
testable so a later renderer can subscribe to the same internal events.

## Acceptance Criteria

- `agent-router` opens local chat in an interactive terminal.
- `agent-router --config a.yaml` opens local chat with that config.
- `agent-router chat --config a.yaml` opens only local chat.
- `agent-router serve --config a.yaml` starts only configured external channels.
- Non-TTY no-subcommand startup fails with a clear mode-selection error.
- Chat mode does not require Slack, QQ, Telegram, or Web credentials.
- User prompts are routed through `ChannelInput` and `RouterService`.
- New prompts can be entered while a turn is running.
- Router-owned slash commands keep existing behavior.
- `/exit` and `/quit` close only the local terminal channel.
- Normal logs do not corrupt chat output.
- Tests cover mode resolution, config parsing, routing shape, output sink
  behavior, and local exit handling.
