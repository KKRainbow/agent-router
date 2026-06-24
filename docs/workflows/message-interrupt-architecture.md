# Message Interrupt Architecture Plan

## Purpose

`docs/workflows/message-interrupt.md` defines the user-visible workflow for
interrupting an in-flight Turn. This document defines the architecture plan for
implementing that workflow without continuing to patch races one call site at a
time.

The current direction in ADR 0001, ADR 0002, ADR 0003, and ADR 0004 remains
valid:

- Agent Router owns Channel Adapters, Sessions, Executor routing, shared
  context projection, and reply delivery.
- backend protocol details stay behind Executor adapters.
- the canonical transcript and context cursor are router-owned state.
- Backend Sessions are isolated per Executor Binding and may be resumed across
  Turns.
- Machine concerns remain below the Executor adapter seam.

The missing architecture is a deep Turn lifecycle Module. Interrupt currently
requires many shallow caller-side rules: compare generations, check cancellation,
decide whether to commit, decide whether channel output is stale, decide whether
Backend Sessions may be closed, and decide whether context sync should roll
back. Those rules must be owned by one Module with a small Interface.

## Current Friction

The implementation already has pieces of the target behavior, but the rules are
distributed:

- `src/router.rs` owns `active_turns` and generation allocation, but callers
  still manually check whether a Turn is current before prepare, context sync,
  event forwarding, success commit, failure commit, and cancellation cleanup.
- `src/channel/slack.rs` keeps adapter-local generation watermarks to protect
  Slack context and file-sync caches from stale updates.
- `src/channel/qq.rs` keeps adapter-local reply generations to avoid stale reply
  target updates.
- `src/session/context.rs` provides transactional context file installation,
  but router call sites decide when a transaction is allowed to finish.
- `src/executor/acp.rs` and `src/executor/codex_app_server.rs` currently mix
  Backend Session lifecycle, per-Turn prompt lifecycle, cancellation, and
  process recovery.

The strongest symptom is the prepare-cancellation race: once an Executor
adapter publishes a Backend Session into a shared map, that Backend Session is
no longer owned by the cancelled prepare call. A replacement Turn may already
have adopted the same Backend Session. A cancelled prepare path that closes or
removes that shared session can therefore destroy the replacement Turn's
resource.

This is a local architecture problem around Turn lifecycle and Backend Session
ownership, not a reason to rewrite the whole Agent Router.

## Target Invariants

These invariants are non-negotiable:

1. A Session has at most one current Turn.
2. Each accepted interruptible user message creates or adopts a new current
   Turn before slow channel-side work begins.
3. A superseded Turn may finish locally, but it cannot commit durable router
   state or send a final reply.
4. Only a current Turn Guard can commit transcript entries, context cursor
   changes, Executor Binding updates, and final replies.
5. Channel Adapters do not compare router generations and do not decide whether
   router state is current.
6. `prepare()` is part of the Turn lifecycle, but a cancelled prepare call does
   not own a shared Backend Session after publication.
7. Backend Session lifecycle is separate from Turn lifecycle. Turn cancellation
   cancels active backend work first; process close and session replacement are
   explicit health/recovery decisions.
8. Context file installation and context state commit happen through one
   Turn-guarded transaction.
9. Cancellation is an expected lifecycle result, not an error path.
10. Late events from an old Turn may be logged, but they cannot be projected as
    current channel output.

## Deepening Opportunities

### 1. Turn Lifecycle Module

**Files**

- new `src/router/turn.rs` or `src/router/turns.rs`
- `src/router.rs`
- `src/executor/mod.rs`

**Problem**

The active Turn registry exists, but its Interface is shallow. Callers must know
generation arithmetic, stale checks, cancellation behavior, and commit ordering.
Deleting the current helper methods would move this complexity back into the
same router call sites, which means the Module is not deep enough yet.

**Solution**

Introduce a Turn Lifecycle Module that owns:

- active Turn registry keyed by Session key;
- monotonic generation allocation;
- Turn Reservation creation for replacement messages;
- adoption of a reserved Turn once the active Executor is known;
- cancellation of the replaced Turn;
- currentness checks;
- guarded state commit;
- guarded output projection;
- guarded cleanup.

The external Interface should expose opaque values rather than raw generation
rules:

```rust
pub struct TurnRegistry;
pub struct TurnReservation;
pub struct TurnGuard;

pub enum TurnBeginMode {
    ReplaceActive,
    CommandOnly,
    NoPreempt,
}

impl TurnRegistry {
    async fn reserve(
        &self,
        session_key: &str,
        mode: TurnBeginMode,
        reason: InterruptReason,
    ) -> anyhow::Result<Option<TurnReservation>>;

    async fn begin(
        &self,
        session_key: &str,
        executor: String,
        mode: TurnBeginMode,
    ) -> anyhow::Result<TurnGuard>;

    async fn adopt(
        &self,
        reservation: TurnReservation,
        executor: String,
    ) -> anyhow::Result<Option<TurnGuard>>;
}
```

The exact Rust names can change during implementation, but the Interface should
keep these properties:

- Channel Adapters can hold and pass `TurnReservation`, but cannot inspect
  generation numbers.
- router execution code uses `TurnGuard`, not raw `(session_key, generation)`.
- all stale checks are methods on `TurnGuard`.
- all durable commits are methods taking `&TurnGuard`.

Useful Turn Guard operations:

```rust
impl TurnGuard {
    fn session_key(&self) -> &str;
    fn executor(&self) -> &str;
    fn cancellation(&self) -> TurnCancellation;

    async fn is_current(&self) -> bool;
    async fn cancel_if_current(&self, reason: InterruptReason) -> bool;

    async fn commit_session<T>(
        &self,
        f: impl FnOnce(&mut SessionState) -> anyhow::Result<T>,
    ) -> anyhow::Result<Option<T>>;

    async fn finish_cancelled(self) -> anyhow::Result<()>;
    async fn finish_failed(self, f: impl FnOnce(&mut SessionState) -> anyhow::Result<()>)
        -> anyhow::Result<bool>;
    async fn finish_success(self, f: impl FnOnce(&mut SessionState) -> anyhow::Result<()>)
        -> anyhow::Result<bool>;
}
```

`Option<T>` is the stale-Turn signal. A stale Turn returns `Ok(None)` instead of
forcing every caller to log and branch on generation.

**Benefits**

- **Locality**: Turn currentness bugs are fixed inside one Module.
- **Leverage**: router, Slack, QQ, context sync, and output gating all reuse the
  same currentness Interface.
- **Test surface**: Turn Registry tests can cover races without spawning real
  Channel Adapters or Executor adapters.

### 2. Router Turn Runner

**Files**

- `src/router.rs`
- possible new `src/router/runner.rs`

**Problem**

`route_to_active_executor()` currently performs command handling, context sync,
state loading, Turn installation/adoption, executor prepare, projection,
prompting, event collection, success commit, failure commit, and final reply
delivery. That makes Turn lifecycle rules hard to audit.

**Solution**

Split the router implementation into a small command/intake layer and a Turn
Runner. The Turn Runner owns the normal prompt flow after command routing:

1. Resolve or adopt a `TurnGuard`.
2. Load Session state through the Session lock.
3. Resolve active Executor and Executor Binding.
4. Build context projection.
5. Call Executor `prepare()` and `prompt()` with the Turn cancellation handle.
6. Commit success, failure, or cancellation through `TurnGuard`.
7. Send final reply only through guarded output.

The Turn Runner should not expose raw generation. Its Interface is the test
surface for router-level interrupt races.

**Benefits**

- **Locality**: route flow becomes readable without chasing helper methods.
- **Leverage**: success, failure, and cancellation paths share one commit
  protocol.
- tests can exercise the Turn Runner with fake Executor adapters and in-memory
  Session stores.

### 3. Executor Backend Turn Interface

**Files**

- `src/executor/mod.rs`
- `src/executor/acp.rs`
- `src/executor/codex_app_server.rs`

**Problem**

`ExecutorBackend::prepare()` and `prompt()` accept cancellation, but the
Interface does not state Backend Session ownership rules. ACP and Codex
therefore leak local lifecycle decisions such as "was this session newly
created" to cancellation cleanup. That is not safe once a Backend Session is
published for reuse.

**Solution**

Make the Executor backend seam explicit about per-Turn work versus Backend
Session lifecycle.

Recommended contracts:

- `prepare()` may create, initialize, or reuse a Backend Session.
- if cancellation happens before a Backend Session is published, the adapter may
  drop that unpublished resource.
- once a Backend Session is published in the adapter's shared session registry,
  a cancelled Turn cannot remove or close it merely because that Turn was
  cancelled.
- `prompt()` owns active backend work for one Turn.
- `interrupt()` requests cancellation of active backend work for the specified
  Turn or Backend Session; it does not mean "destroy the shared Backend
  Session" unless the adapter has entered an explicit unhealthy recovery state.
- cancellation maps to `ExecutorPromptOutcome::Cancelled`.
- prepare cancellation should be represented as a cancelled Turn outcome at the
  router layer, not as an unhealthy Executor Binding.

The request types should carry an opaque Turn reference:

```rust
pub struct ExecutorTurnRef {
    pub session_key: String,
    pub executor: String,
    pub generation: u64,
    pub cancellation: TurnCancellation,
}

pub struct ExecutorPrepareRequest {
    pub turn: ExecutorTurnRef,
    pub cwd: Option<PathBuf>,
    pub previous_session_id: Option<String>,
}

pub struct ExecutorPromptRequest {
    pub turn: ExecutorTurnRef,
    pub prompt: String,
    pub user_id: Option<String>,
}
```

This still exposes generation to backend adapters for logging and protocol
correlation, but not for router state commit.

**Benefits**

- **Locality**: Backend Session ownership rules live in Executor adapters, not
  scattered through router cleanup.
- **Leverage**: ACP, Codex app-server, and future protocols share the same Turn
  semantics.
- tests can assert that a cancelled prepare never closes a replacement Turn's
  published Backend Session.

### 4. Backend Session Manager Inside Each Executor Adapter

**Files**

- `src/executor/acp.rs`
- `src/executor/codex_app_server.rs`

**Problem**

ACP and Codex currently use coarse shared session locks and ad hoc cancellation
cleanup. Interrupt may need to send a protocol-level cancel while the prompt
loop is holding the session lock, and prepare cancellation may race with a
replacement Turn reusing the same published session.

**Solution**

Each Executor adapter should have an internal Backend Session Manager with a
deep Interface:

```rust
struct BackendSessionManager;
struct BackendSessionHandle;
struct ActiveBackendTurn;

impl BackendSessionManager {
    async fn get_or_create(
        &self,
        key: BackendSessionKey,
        cfg: &ExecutorConfig,
        cwd: &Path,
    ) -> anyhow::Result<BackendSessionHandle>;

    async fn mark_unhealthy_and_replace(
        &self,
        key: &BackendSessionKey,
        expected: &BackendSessionHandle,
        reason: &str,
    ) -> anyhow::Result<()>;
}

impl BackendSessionHandle {
    async fn ensure_ready(&self, turn: &ExecutorTurnRef) -> anyhow::Result<PreparedExecutor>;
    async fn run_prompt(
        &self,
        turn: &ExecutorTurnRef,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
    ) -> ExecutorPromptOutcome;
    async fn interrupt(&self, turn: &ExecutorInterruptRequest) -> anyhow::Result<()>;
}
```

The implementation should avoid requiring `interrupt()` to take the same long
held lock used by `run_prompt()`. A prompt can register an `ActiveBackendTurn`
containing the protocol cancellation data:

- ACP: external `sessionId`, active request id if needed, and a notify sender
  capable of sending `session/cancel`.
- Codex app-server: active `turnId` or thread-level cancellation identity, and
  a client handle capable of sending the cancel request.

`interrupt()` reads that active backend turn handle and sends soft cancel. If no
active backend turn has been registered yet, the router cancellation handle still
causes prepare or prompt to stop before committing.

Process close is a separate recovery decision:

- use soft protocol cancel as the normal path;
- close and replace the process only if the adapter marks the Backend Session
  unhealthy or if the protocol has no usable cancel operation;
- remove from the shared registry only with pointer/identity equality and only
  from the Backend Session Manager recovery path.

**Benefits**

- **Locality**: all publish/reuse/replace rules are in one adapter-internal
  Module.
- **Leverage**: prompt, prepare, interrupt, and health recovery share the same
  Backend Session ownership model.
- tests can cover replacement reuse and unhealthy replacement without involving
  router internals.

### 5. Context Commit Module

**Files**

- `src/session/context.rs`
- `src/router.rs`

**Problem**

`ContextSyncPlan` and `InstalledContextSync` provide useful transaction
primitives, but the router still manually decides when a context transaction is
current, when to drop it for rollback, when to save old state, and when to call
`finish()`.

**Solution**

Create a router-facing Context Commit Module that takes a `TurnGuard`:

```rust
struct ContextCommitPlan;

impl ContextCommitPlan {
    async fn commit_if_current(
        self,
        turn: &TurnGuard,
        store: &dyn SessionStore,
    ) -> anyhow::Result<CommitStatus>;
}
```

The Module owns:

- preparing context records from current Session state;
- installing staged files;
- saving updated context artifacts;
- rolling back installed files when the Turn becomes stale;
- restoring old Session state if state save succeeds but the Turn becomes stale
  before finalization.

The router should not hand-roll context rollback around generation checks.

**Benefits**

- **Locality**: context state/file consistency bugs stay in context commit code.
- **Leverage**: Slack context sync and future channel context sync reuse one
  transaction path.
- tests can simulate stale Turn transitions at each commit phase.

### 6. Channel Adapter Turn Reservation

**Files**

- `src/router.rs`
- `src/channel/slack.rs`
- `src/channel/qq.rs`

**Problem**

Slack and QQ need new messages to supersede old Turns before slow work begins.
The current Interface exposes this as `preempt(session_key) -> generation` plus
`handle_preempted_with_context(...)`. That makes Channel Adapters aware of
router generation identity and encourages adapter-local watermarks.

**Solution**

Replace raw generation preemption with an opaque reservation Interface:

```rust
#[async_trait]
pub trait RouterService {
    async fn reserve_turn(
        &self,
        session_key: &str,
        mode: TurnBeginMode,
    ) -> anyhow::Result<Option<TurnReservation>>;

    async fn handle_reserved(
        &self,
        reservation: TurnReservation,
        input: RouterInput,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()>;

    async fn handle(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()>;
}
```

Slack flow:

1. If the message resolves approval, route to the approval path without
   reserving a replacement Turn.
2. If the message is `/stop`, ask router to cancel active Turn without creating
   replacement work.
3. For ordinary messages, call `reserve_turn(..., ReplaceActive)` immediately.
4. Fetch Slack context.
5. Pass the opaque reservation to `handle_reserved()`.

QQ flow:

1. Deduplicate inbound event.
2. Approval messages bypass replacement reservation.
3. Ordinary messages reserve replacement immediately.
4. Spawn route task with the opaque reservation.

Channel Adapter caches may still need adapter-local ordering for external data,
but that ordering must not decide router state commit. Where cache mutation must
be tied to a Turn, pass an opaque Turn Guard or Turn Reservation token into the
cache helper instead of comparing raw generation values in the adapter.

**Benefits**

- **Locality**: router owns Turn currentness.
- **Leverage**: every Channel Adapter gets interrupt semantics through the same
  Interface.
- tests can verify that Channel Adapters never expose or compare router
  generations.

### 7. Output Projection Module

**Files**

- `src/router.rs`

**Problem**

`RouterExecutorEventSink` currently decides whether to forward channel events by
checking active generation. Final replies have their own guarded path. This
duplicates stale-output policy.

**Solution**

Create a Turn-scoped output sink:

```rust
struct TurnOutputSink<'a> {
    turn: &'a TurnGuard,
    output: &'a mut dyn RouterOutputSink,
}

impl TurnOutputSink<'_> {
    async fn send_event(&mut self, event: RouterChannelEvent) -> anyhow::Result<()>;
    async fn send_final_reply(self, text: String) -> anyhow::Result<()>;
}
```

Executor updates are always collected for the owning Turn. Projection to
channel output goes through `TurnOutputSink`, which drops stale output
consistently.

**Benefits**

- **Locality**: stale output policy lives in one Module.
- **Leverage**: stream events, tool summaries, and final replies share one
  currentness rule.
- tests can assert output behavior without inspecting active Turn internals.

## End-to-End Target Flow

### Ordinary Replacement Message

1. Channel Adapter receives message `B` for Session `S`.
2. Channel Adapter calls `reserve_turn(S, ReplaceActive)`.
3. Turn Registry installs a new Turn Reservation and cancels previous Turn `A`.
4. Turn Registry asks the Executor adapter to interrupt `A` if `A` had adopted
   an Executor.
5. Channel Adapter performs slow channel context fetch for `B`.
6. Channel Adapter calls `handle_reserved(reservation, input B, context, sink)`.
7. Router adopts the reservation into a `TurnGuard` after loading active
   Executor from Session state.
8. Router commits context sync through the `TurnGuard`.
9. Router calls `prepare()` with the Turn cancellation handle.
10. Router builds prompt projection from committed transcript and context.
11. Router calls `prompt()` with a Turn-scoped output sink.
12. If prompt completes and the Turn is still current, router commits transcript,
    Executor Binding, seen-context cursor, and final reply.
13. If prompt completes after being superseded, router logs and drops the
    result.

### `/stop`

1. Channel Adapter identifies `/stop`.
2. Router cancels the current Turn with reason `UserStop`.
3. Router does not create a replacement Turn.
4. Executor adapter sends soft protocol cancel if possible.
5. No transcript or context cursor changes are committed.

### Approval Message

1. Channel Adapter checks whether the message resolves a pending approval.
2. If yes, it routes to the approval broker and does not reserve a replacement
   Turn.
3. If no approval is pending, the message is ordinary input and follows the
   replacement flow.

## Implementation Plan

### Phase 0: Freeze the Current Patch Line

Before changing code, return the working tree to a consistent baseline:

- keep unrelated user edits such as config changes untouched;
- decide whether the current partial ACP prepare cleanup should be reverted or
  folded into the new Executor Session lifecycle refactor;
- do not add more local cancellation cleanup patches before the Turn Lifecycle
  Module exists.

### Phase 1: Add Turn Lifecycle Module

- Introduce `TurnRegistry`, `TurnReservation`, and `TurnGuard`.
- Move `active_turns` and `next_turn_generation` out of `AgentRouter`.
- Implement replacement reservation, adoption, cancellation, stale finish, and
  guarded commit helpers.
- Keep existing router behavior by adapting old call sites to the new Module.
- Add Turn Registry unit tests for:
  - replacing an active Turn cancels the old Turn;
  - stale reservation cannot be adopted;
  - stale guard cannot commit;
  - `/stop` cancels without creating replacement;
  - cancellation is idempotent.

### Phase 2: Move Router Commit Paths Behind Turn Guard

- Replace manual `active_turn_is_current()` and
  `take_active_turn_if_current()` call sites with `TurnGuard` operations.
- Convert success, failure, cancellation, and prepare-failure paths to guarded
  commit methods.
- Convert `RouterExecutorEventSink` to use a Turn-scoped output sink.
- Add router tests for:
  - old successful Turn cannot commit after replacement;
  - old failed Turn cannot mark Executor Binding unhealthy after replacement;
  - old stream events are dropped after replacement;
  - final reply belongs to the latest Turn only.

### Phase 3: Replace Channel Raw Generation Interface

- Replace `preempt()` plus `handle_preempted_with_context()` with opaque Turn
  Reservation methods.
- Update Slack to reserve before slow thread/file context fetch.
- Update QQ to reserve before spawning route work for ordinary messages.
- Keep approval and command routing explicit.
- Remove adapter dependence on router generation values.
- Add Channel Adapter tests for:
  - Slack second message reaches router before old Turn finishes;
  - QQ second message reaches router before old Turn finishes;
  - approval resolution does not interrupt active Turn;
  - ordinary text without pending approval does interrupt active Turn.

### Phase 4: Make Context Commit Turn-Guarded

- Wrap `ContextSyncPlan` commit behind a router-facing Context Commit Module.
- Ensure staged file install, Session state save, and finalization are one
  guarded transaction from the router's perspective.
- Remove scattered context currentness checks from `src/router.rs`.
- Add tests for stale Turn transitions:
  - stale before install;
  - stale after install before state save;
  - stale after state save before finalization;
  - failed install restores replaced files and old state.

### Phase 5: Refactor Executor Backend Turn Interface

- Add `ExecutorTurnRef` or equivalent Turn reference to prepare, prompt, and
  interrupt requests.
- Document prepare cancellation and Backend Session ownership contracts in
  `src/executor/mod.rs`.
- Map cancelled prepare to a router cancellation outcome rather than generic
  unhealthy state.
- Add fake Executor tests that model:
  - prepare cancelled before Backend Session publication;
  - prepare cancelled after Backend Session publication;
  - prompt cancelled before first backend event;
  - prompt cancelled after backend events.

### Phase 6: Refactor ACP Backend Session Lifecycle

- Introduce ACP Backend Session Manager.
- Split long prompt execution from interrupt-readable active backend turn
  metadata.
- Implement soft `session/cancel` through active backend turn metadata.
- Ensure cancelled prepare does not close a published Backend Session.
- Close and replace ACP process only through explicit unhealthy recovery.
- Add ACP tests for:
  - interrupt sends `session/cancel` even while prompt is active;
  - cancelled ACP stop reason maps to `ExecutorPromptOutcome::Cancelled`;
  - cancelled prepare after publication keeps replacement session alive;
  - unhealthy session replacement does not remove a newer session.

### Phase 7: Refactor Codex App-Server Backend Session Lifecycle

- Introduce Codex Backend Session Manager matching the ACP ownership model.
- Store active `turnId` or thread cancel identity outside the long prompt lock.
- Use the real cancel protocol if available.
- If no soft cancel exists, isolate close/recreate as documented technical debt
  under unhealthy recovery.
- Add Codex tests for:
  - interrupt sends real cancel when turn identity exists;
  - local pending-response cancellation is not treated as backend interrupt;
  - cancelled prepare after publication keeps replacement thread alive;
  - process close fallback cannot close a newer published session.

### Phase 8: Delete Old Scattered State Rules

- Remove raw generation checks from Channel Adapters.
- Remove duplicated context-cache currentness code that the Turn Guard replaces.
- Remove `created_session` cancellation cleanup from Executor adapters unless
  it only refers to unpublished resources.
- Remove router helper methods that expose active generation directly.

### Phase 9: Final Verification

- Run formatting and all local tests.
- Add race-focused tests before fixing each newly discovered race.
- After every code-change commit, run the required subagent review.
- Final review must inspect:
  - behavioral regressions;
  - concurrency and race risks;
  - cache invalidation and state consistency;
  - permission and approval implications;
  - unintended broad coupling or over-complexity.

## Acceptance Criteria

Architecture is acceptable when:

- Channel Adapters never inspect router generation values.
- router state commits require a `TurnGuard`.
- context sync finalization requires a current `TurnGuard`.
- final replies and channel events go through Turn-scoped output gating.
- ACP and Codex prepare cancellation cannot close a Backend Session that a
  replacement Turn may reuse.
- Executor interrupt can send soft cancel without waiting for the long prompt
  lock.
- cancellation outcomes do not mark healthy Executor Bindings unhealthy.
- stale Turn results cannot update transcript, context cursor, Executor Binding,
  reply target, file-sync state, or final reply.

## Explicit Non-Solutions

Do not fix interrupt by:

- adding more generation checks at individual call sites;
- adding more adapter-local watermarks that duplicate router currentness;
- treating cancelled prepare as a generic backend failure;
- closing shared Backend Sessions from cancelled Turn cleanup;
- relying on process kill as the normal interrupt path;
- serializing per-session Channel Adapter delivery until old prompts finish;
- committing interrupted user messages into the canonical transcript.

## Open Decisions

1. The exact Rust module path for Turn lifecycle: `src/router/turn.rs` versus
   `src/router/turns.rs`.
2. Whether `TurnGuard::commit_session()` should take a closure over
   `SessionState` or expose narrower typed operations for transcript, binding,
   and context commits.
3. Whether channel-side cache updates should use an opaque Turn token directly
   or be moved behind router-owned context commit callbacks.
4. Codex app-server's true soft-cancel protocol. If it has no turn cancel
   method, close/recreate must be isolated as documented technical debt.
5. How much of Backend Session Manager should be shared between ACP and Codex
   versus duplicated initially for clarity.
