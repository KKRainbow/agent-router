# Message Interrupt Workflow

## Goal

Agent Router must support interrupting an in-flight agent turn when a new user
message arrives for the same user-visible session.

For chat agents, interrupt is a core usability requirement. A user must be able
to correct, redirect, or stop a long-running turn without waiting for the old
turn to finish. The router should treat this as a first-class turn lifecycle
state, not as a timeout, queue overflow, or process-kill fallback.

## Implementation State

The original implementation was a synchronous per-session turn model:

- `AgentRouter::handle()` holds the per-session lock across
  `executor.prompt()`.
- Slack has its own per-session routing lock.
- QQ uses a per-session FIFO work queue.
- `ExecutorBackend` exposes `prepare()` and `prompt()`, but no interrupt or
  cancel capability.
- The ACP backend sends `session/prompt`, but does not send `session/cancel`.
- The Codex app-server backend sends `turn/start`; its existing
  `cancel_pending()` only removes a local JSON-RPC pending response and does
  not interrupt the backend turn.

The practical result is that a second message for the same session waits behind
the first turn instead of interrupting it.

Current work has moved several of these items forward:

- `ExecutorBackend` now has `prepare()`, `prompt()`, and `interrupt()` requests
  carrying one `ExecutorTurnRef`.
- ACP now uses soft `session/cancel` for prompt cancellation and `/stop` or
  replacement interrupt, without closing a healthy ACP process.
- ACP active prompt metadata is visible to `interrupt()` without waiting for the
  long prompt lock, and stale generation interrupts are ignored.
- ACP holds replacement prompts behind a cancel barrier until the cancelled
  prompt's JSON-RPC response is observed, so stale output and late permission
  requests cannot be projected into the replacement turn.
- Codex app-server still needs its equivalent protocol-level cancel path.

## Target Semantics

If session `S` has active turn `A`, and the user sends message `B`:

1. The router detects that `S` has an active turn.
2. The router interrupts `A`.
3. The executor backend sends a real protocol-level cancel signal for `A`.
4. `A` must not commit a final assistant message after it has been superseded.
5. `A` must not advance the context cursor or mark projected context as seen.
6. The router starts a replacement turn for `B` immediately.
7. Only the latest active turn generation may commit session state.

The replacement turn is a new prompt call against the same executor binding. It
is not an attempt to inject `B` into the old in-flight prompt. If the backend can
reuse the same private session or thread after cancel, it should. If the backend
cannot guarantee a clean state after cancel, the adapter may close and recreate
the backend process while preserving enough binding information to resume when
possible.

`/stop` is a related but different command: it interrupts the active turn and
does not start a replacement turn.

## Non-Goals

Version 1 should not implement:

- merging multiple queued user messages into one prompt;
- preserving interrupted partial assistant output in the canonical transcript;
- editing or deleting platform messages that were already streamed before the
  interrupt;
- backend-specific recovery heuristics beyond a clearly defined cancel or
  close-and-recreate path.

## Core Model

The router owns an active-turn registry keyed by `session_key`.

```rust
struct ActiveTurn {
    generation: u64,
    session_key: String,
    executor: String,
    cancel: TurnCancellation,
    started_at: Instant,
}
```

`generation` is the state-commit arbiter. Each new turn increments the session
generation. A prompt result, stream event, or cleanup step from an old
generation may be logged, but it must not mutate durable session state.

## Router Changes

The session lock must no longer cover the whole prompt execution. It should
protect only short critical sections:

- loading and saving session state;
- allocating a new turn generation;
- installing or clearing the active-turn record;
- checking whether a completed turn is still current before committing.

The high-level flow for a normal user message should be:

1. Acquire the session lock.
2. Load or create the session state.
3. If an active turn exists, request interrupt for that turn.
4. Allocate a new generation and install the new active-turn record.
5. Build the prompt projection for the new user message.
6. Release the session lock.
7. Run `executor.prepare()` and `executor.prompt()` with a cancellation handle.
8. Reacquire the session lock.
9. If the generation is still current, commit transcript, binding, and context
   cursor updates.
10. If the generation is stale, discard the result and leave durable state
    unchanged.

`prepare()` should be considered part of the turn lifecycle. A new message
should also be able to supersede a turn that is preparing a backend session,
loading a previous external session, or starting a backend process.

## Executor API Changes

The executor abstraction needs to express cancellable turns.

One possible shape is:

```rust
pub struct ExecutorInterruptRequest {
    pub session_key: String,
    pub executor: String,
    pub generation: u64,
    pub reason: InterruptReason,
}

pub enum InterruptReason {
    ReplacedByNewMessage,
    UserStop,
    Shutdown,
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor>;
    fn list(&self) -> Vec<ExecutorDescriptor>;

    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor>;

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome;

    async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()>;
}
```

`prompt()` should distinguish cancellation from failure:

```rust
pub enum ExecutorPromptOutcome {
    Completed(ExecutorResponse),
    Cancelled,
    Failed(anyhow::Error),
}
```

Cancelled turns should not flow through the same state-update path as failed
turns. They are expected lifecycle events.

## Cancellation Handle

The router should provide a cloneable cancellation handle for each turn.

Required behavior:

- `cancel()` is idempotent.
- tasks can await cancellation;
- cancellation carries a reason;
- pending approval requests can subscribe to the same cancellation source;
- backend prompt loops can select on cancellation and protocol messages.

This can be implemented with `tokio::sync::watch`, `Notify`, or the existing
approval cancellation primitive generalized into a turn-level primitive.

## ACP Backend

ACP should use protocol-level session cancellation.

The backend should send this notification when interrupting an active turn:

```json
{
  "jsonrpc": "2.0",
  "method": "session/cancel",
  "params": {
    "sessionId": "<external-session-id>"
  }
}
```

Implementation requirements:

- `JsonRpcClient::notify(method, params)` sends ACP notifications.
- The manager records the active prompt's external `sessionId`, JSON-RPC
  request id, generation, and notify handle outside the long prompt lock.
- `AcpExecutorManager::interrupt()` sends `session/cancel` through that active
  prompt handle only when the interrupted generation matches the active prompt.
- `AcpSession::prompt()` selects on the turn cancellation handle.
- ACP cancelled results, such as `stopReason = "cancelled"`, map to
  `ExecutorPromptOutcome::Cancelled`.
- Pending approval requests are cleared when the turn is cancelled.
- After local cancellation, the next prompt waits for the cancelled prompt's
  JSON-RPC response. While that barrier is active, late `session/update` events
  are dropped and late `session/request_permission` requests receive a cancelled
  result without creating approval prompts.

The preferred path is soft cancel with `session/cancel`. Closing the ACP process
should be a fallback only when the backend does not acknowledge cancellation or
the adapter cannot guarantee that the private session remains usable.

## Codex App-Server Backend

Codex app-server interruption uses the real app-server protocol:

```json
{
  "method": "turn/interrupt",
  "params": {
    "threadId": "<thread id>",
    "turnId": "<turn id>"
  }
}
```

The adapter stores active turn metadata in a manager-level map outside the long
`run_turn()` session lock. `interrupt()` looks up that metadata, checks the
interrupted generation, and sends `turn/interrupt` without waiting for the
prompt lock. If the interrupt arrives before `turn/start` has returned a
`turnId`, the adapter records a pending interrupt and sends `turn/interrupt`
as soon as the `turnId` is known. The active turn registry is also the
at-most-once gate for `turn/interrupt`, so router cancellation and direct
backend `interrupt()` cannot send duplicate interrupts for the same turn.

Important constraints:

- `cancel_pending()` only removes a local JSON-RPC pending response; it is not
  a backend interrupt.
- Prompt cancellation sends `turn/interrupt`; it does not close a healthy
  app-server process.
- If `turn/interrupt` fails, times out, or returns a JSON-RPC error, the
  adapter closes the app-server as unhealthy recovery before allowing another
  turn to use that backend session.
- If `interrupt()` sent `turn/interrupt` asynchronously, the prompt loop waits
  for that request to be acknowledged or fail before releasing the backend
  session.
- Direct `interrupt()` cancels the local turn scope, so pending approval
  prompts are removed even before the backend emits `turn/completed`.
- If Codex sends approval requests before `turn/start` returns a `turnId`,
  cancellation still answers those requests with `decline` so Codex can finish
  `turn/start` and receive the pending `turn/interrupt`.
- `turn/completed` statuses `interrupted`, `cancelled`, and `canceled` map to
  `ExecutorPromptOutcome::Cancelled`.
- Cancelled prepare calls do not remove or close an already published shared
  Codex session. Lifecycle RPCs such as `initialize` and `thread/start` are
  still driven to their response while the session lock is held, so the shared
  session is not left in an unknown initialization state.
- Codex reports `started_new_session` by comparing the actual `threadId` with
  the router's `previous_session_id`, not by whether the adapter happened to
  create the thread during the current prepare call. This prevents a cancelled
  prepare from making the next prompt drop required context.
- Process close remains reserved for unhealthy paths such as startup failure or
  request timeout, including a failed `turn/interrupt` request. It is not the
  normal message interrupt path.

## Transcript and Context Cursor Rules

Interrupted turns must not be persisted as completed turns.

For interrupted turn `A`:

- do not append user message `A` to the canonical transcript;
- do not append assistant message `A`;
- do not update `seen_context`;
- do not advance context fingerprints;
- do not treat partial streamed text as durable assistant output.

For replacement turn `B`, normal success rules apply.

This keeps the next prompt grounded in the last successfully committed turn, not
in speculative or partial work from an interrupted turn.

## Streaming Rules

Platform stream output is a side effect. Some partial text may already have been
sent before the interrupt. The router does not need to retract it in version 1.

Required behavior:

- old-generation final replies must be dropped;
- old-generation stream events after cancellation must not update the new turn;
- channel output sinks should receive a turn-end or segment-stop event so loading
  indicators can stop;
- replacement turn events must carry enough identity, directly or indirectly, to
  avoid mixing them with stale old-turn events.

## Channel Changes

Channels must stop using long-lived per-session serialization as the mechanism
that protects router state. The router should own turn concurrency semantics.

### Slack

Slack should route ordinary user messages into the router without holding a
channel-level session lock for the duration of backend execution.

Approval commands remain special:

- if a message resolves a pending approval, it should route to the approval
  broker;
- otherwise ordinary messages should be able to interrupt active turns.

### QQ

QQ currently uses `SessionWorkQueue`, which serializes messages per session.
This prevents a new message from reaching the router in time to interrupt.

Version 1 should either:

- bypass the FIFO queue for normal interruptible messages; or
- reduce the queue worker to a lightweight delivery mechanism that does not
  wait for the previous backend turn to finish.

Queue capacity must not determine whether a user can stop or redirect an active
turn.

## Race Handling

The implementation must handle these races explicitly:

- old turn completes successfully while a new message requests interrupt;
- old turn cancellation acknowledgement arrives after the replacement turn
  starts;
- old turn notifications continue after replacement turn starts;
- backend cancellation fails;
- approval request is pending when the turn is interrupted;
- `/stop` races with normal completion.

Rules:

- only the current generation may commit durable session state;
- cancellation is idempotent;
- stale generation events can be logged but should not be forwarded as current
  turn output;
- pending approvals are cancelled when their owning turn is cancelled;
- if protocol-level cancel fails, the adapter may close the backend process and
  mark the turn cancelled from the router's perspective.

## User-Visible Behavior

For a replacement message:

- the old turn should stop producing visible loading state quickly;
- the new turn should begin without waiting for the old final result;
- the final reply should correspond to the latest accepted user message.

For `/stop`:

- the old turn is cancelled;
- no replacement prompt is started;
- the channel may send a short acknowledgement, depending on product choice.

## Test Plan

Router tests:

- a new message interrupts an active turn and starts a replacement turn;
- a stale old-generation success result does not commit transcript;
- only the replacement turn updates transcript and context cursor;
- `/stop` cancels without starting a replacement turn;
- normal completion racing with interrupt commits at most one turn;
- failed cancel does not allow the stale turn to commit.

ACP tests:

- interrupt sends `session/cancel`;
- ACP cancelled stop reason maps to `ExecutorPromptOutcome::Cancelled`;
- cancelled turns remove pending approvals;
- next turn can reuse or resume the external session according to adapter
  policy.

Codex app-server tests:

- interrupt sends the real app-server cancel RPC;
- local `cancel_pending()` is not treated as backend interrupt;
- active turn id is recorded and cleared correctly;
- pending approvals and server request workers stop on cancellation;
- stale notifications do not pollute replacement turn streams.

Channel tests:

- Slack second message for the same session reaches the router before the first
  backend turn finishes;
- QQ second message for the same session reaches the router before the first
  backend turn finishes;
- approval resolution still works while a session has an active turn.

## Implementation Order

1. Add `TurnCancellation`, `ExecutorPromptOutcome`, and active-turn generation
   state.
2. Shrink router session-lock scope and add generation-based commit checks.
3. Update `ExecutorBackend` to support cancellable prepare and prompt.
4. Implement ACP `session/cancel`.
5. Implement Codex app-server real turn cancel, or an explicit close/recreate
   fallback if the protocol does not yet expose soft cancel.
6. Refactor Slack and QQ delivery so new messages can reach the router during an
   active turn.
7. Add race-focused router tests.
8. Add backend cancellation tests.
9. Add channel delivery tests.
10. Add `/stop` as a command-only cancellation path.

## Acceptance Criteria

A session with an active long-running turn must accept a new user message and
request interruption of the old turn within one second under normal local
conditions. The replacement turn must begin without waiting for the old turn's
final result.

The durable transcript must contain only committed turns. An interrupted turn's
final response, late events, or partial streamed output must not update durable
session state or context cursors.

The implementation must rely on protocol-level cancellation where available.
Process termination is acceptable only as a documented backend fallback when no
soft-cancel protocol exists.
