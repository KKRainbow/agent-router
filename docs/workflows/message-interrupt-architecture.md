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

## Document Status

This document is the architecture baseline for message interrupt. It is meant to
guide implementation and review, not to justify one more local race fix.

The repository already has part of this architecture:

- `TurnCancellation`, `InterruptReason`, and `ExecutorPromptOutcome` exist in
  the Executor interface.
- `TurnRegistry`, `TurnReservation`, and `TurnGuard` exist under
  `src/router/turns.rs`.
- Slack and QQ can reserve replacement Turns through an opaque Router Service
  interface instead of passing raw generation values.
- router context commit logic has started moving behind a guarded
  `PreparedContextSync` commit path.
- Executor prepare, prompt, and interrupt requests now carry an
  `ExecutorTurnRef` so backend adapters receive one Turn identity across the
  per-Turn lifecycle.
- ACP prompt cancellation now uses soft `session/cancel`, keeps the Backend
  Session process alive, exposes a generation-checked interrupt-readable active
  prompt handle, and holds the next prompt behind a cancel barrier until the
  cancelled prompt's JSON-RPC response is observed.

The remaining architectural work is still substantial:

- the router route flow is not yet a clearly separated Turn Runner;
- not every durable router commit is expressed as a `TurnGuard` operation;
- output projection is still only partially centralized;
- Executor adapters still need explicit Backend Session Manager boundaries;
- ACP still needs the manager boundary cleanup, while Codex app-server still
  needs protocol-level soft cancel semantics that do not destroy
  replacement-owned shared sessions;
- context commit still lives in `src/router.rs` instead of a dedicated context
  commit Module.

That status matters because the right next step is not another local
`is_current()` call. The right next step is to finish pushing ownership into the
Modules below, then delete the old scattered rules.

## Current Friction

The implementation already has pieces of the target behavior, but the rules are
distributed:

- `src/router/turns.rs` owns active Turn storage and generation allocation, but
  `src/router.rs` still contains route-flow code that manually sequences
  currentness checks, interrupt requests, guarded output, context commit,
  success commit, failure commit, and cleanup.
- `src/channel/slack.rs` still keeps adapter-local generation watermarks to
  protect Slack context and file-sync caches from stale external data. These
  should remain adapter-cache ordering only and must not decide router state.
- `src/channel/qq.rs` still keeps adapter-local reply generations to avoid stale
  reply target updates. This is platform-output bookkeeping, not the router Turn
  identity.
- `src/session/context.rs` provides transactional context file installation,
  while the router-facing commit wrapper is still young and needs complete
  stale-phase tests.
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

## Root Cause

The current weak point is not that the router lacks a generation counter. The
weak point is that several subsystems each own a fragment of the same lifecycle:

- Channel Adapters decide when to reserve or serialize input.
- Router call sites decide whether a Turn is current.
- context sync decides file rollback, but router call sites decide whether the
  transaction may finish.
- Executor adapters publish reusable Backend Sessions, but cancelled prepare
  paths still carry local cleanup decisions.
- output sinks decide whether a late event is still visible.

Interrupt is therefore easy to make work in one happy path and easy to break in
the next race. The architecture must make "current Turn" a capability, not a
number that many call sites interpret independently.

## Ownership Boundary

The design keeps the existing ADR boundaries and deepens the Turn lifecycle
boundary inside the router.

| Concern | Owner | Non-owner |
| --- | --- | --- |
| user-visible Session key | Router | Executor adapter |
| active Turn identity and cancellation | Turn Registry | Channel Adapter |
| durable transcript and context cursor | Router Session state | Executor adapter |
| context artifact file transaction | Context Commit Module | Channel Adapter |
| platform event intake and reply delivery | Channel Adapter | Executor adapter |
| backend-private session/process/thread | Executor adapter Backend Session Manager | Router |
| protocol-level cancel | Executor adapter | Channel Adapter |
| approval lifecycle | approval broker plus owning Turn cancellation | Channel Adapter local queues |

Any code path that crosses these boundaries should pass an opaque capability:

- Channel Adapter to router: `TurnReservation`.
- router to guarded commit code: `TurnGuard`.
- router to Executor adapter: `ExecutorTurnRef` or equivalent data derived from
  `TurnGuard`.
- Executor adapter internals: `BackendSessionHandle` and `ActiveBackendTurn`.

Raw generation values may exist for logging and protocol correlation, but they
must not be the public decision primitive outside the owning Module.

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

## Message Classes

The router should classify inbound messages before deciding whether to reserve a
replacement Turn:

- **ordinary user input** reserves a replacement Turn immediately and interrupts
  the previous current Turn for the same Session;
- **approval resolution** is delivered to the approval broker and does not create
  a replacement Turn;
- **`/stop`** cancels the current Turn and does not create replacement work;
- **agent routing commands** update router state through short guarded critical
  sections and should not be hidden behind a long prompt lock;
- **system shutdown** cancels active Turns with a shutdown reason and should not
  be reported as a backend failure.

The Channel Adapter may detect platform syntax, but the router owns the effect
on the Turn lifecycle.

## Commit Protocol

Every successful prompt follows one commit protocol:

1. A current `TurnGuard` exists before slow work begins.
2. Slow work uses a clone of the Turn cancellation handle.
3. Backend events are collected for the owning Turn.
4. Before any durable router state change, the code asks the `TurnGuard` to
   prove that the Turn is still current.
5. The final reply is projected only through Turn-scoped output gating.
6. Finishing the Turn clears the active Turn only if the same Turn is still
   current.

Cancellation follows a separate non-error protocol:

1. The Turn cancellation handle is set with an `InterruptReason`.
2. The Executor adapter sends soft cancel if it has enough active backend turn
   metadata.
3. The old Turn may return `ExecutorPromptOutcome::Cancelled`.
4. The router does not append the cancelled user message, assistant response, or
   context cursor updates.
5. The replacement Turn proceeds independently.

Backend failure is different from cancellation. Failure may mark an Executor
Binding unhealthy only when the failed Turn is still current. A stale failed Turn
cannot poison the replacement Turn's binding.

## Transaction Boundaries

The architecture has three transaction boundaries.

### Router Session Transaction

The Session lock protects only short state transitions:

- load or create Session state;
- update active executor and binding metadata;
- save transcript and context cursor for a current Turn;
- install or remove active Turn records.

It must not be held across channel context fetches, Executor prepare, Executor
prompt, platform API calls, or long approval waits.

### Context Artifact Transaction

Context sync is a file and Session-state transaction:

- prepare computes records from current Session state;
- install stages and replaces files;
- state save records the new context artifacts;
- finish makes the file replacement durable;
- drop/rollback restores replaced files if the Turn becomes stale before finish.

The transaction is successful only if the owning Turn is current at each phase.
If the Turn becomes stale after state save but before finish, old Session state
must be restored.

### Backend Session Transaction

Backend Session publication is an Executor-adapter transaction:

- an unpublished Backend Session is local to the prepare call and may be dropped
  if prepare is cancelled;
- a published Backend Session is shared adapter state and may be reused by a
  replacement Turn;
- cancelled prepare cleanup cannot close or remove a published shared Backend
  Session unless the Backend Session Manager has marked that exact handle
  unhealthy;
- unhealthy replacement uses handle identity checks so an old Turn cannot remove
  a newer published handle.

This boundary is the main fix for the prepare-cancellation race.

## Deepening Opportunities

### 1. Turn Lifecycle Module

**Files**

- `src/router/turns.rs`
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
The older Interface exposed this as `preempt(session_key) -> generation` plus
`handle_preempted_with_context(...)`. That made Channel Adapters aware of router
generation identity and encouraged adapter-local watermarks.

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

The implementation should proceed in phases. A phase can be considered complete
only when the old ownership rule it replaces has been deleted or made private to
the owning Module.

### Phase 0: Baseline and Scope `[done for architecture]`

- Keep unrelated user edits, such as local config changes, out of interrupt
  commits.
- Stop adding one-off cancellation cleanup patches before checking whether the
  issue belongs to Turn lifecycle, context transaction, or Backend Session
  ownership.
- Treat every newly discovered race as a missing Module invariant first, not as
  a missing `if stale { return }` branch.

### Phase 1: Add Turn Lifecycle Module `[mostly implemented]`

Already present:

- `TurnRegistry`, `TurnReservation`, and `TurnGuard`;
- active Turn storage and generation allocation outside `AgentRouter`;
- replacement reservation, adoption, cancellation, stale finish, and `/stop`;
- tests for replacement cancellation, stale reservation behavior, and command
  cancellation.

Remaining work:

- make `TurnGuard` the only router-state commit capability, not just the
  currentness probe;
- reduce raw generation exposure to logging and Executor correlation only;
- move repeated route-flow sequencing out of `src/router.rs` once the Turn
  Runner exists.

### Phase 2: Move Router Commit Paths Behind Turn Guard `[partial]`

Already present:

- success, cancellation, and stale paths use `TurnGuard` currentness checks;
- old Turn events and final replies are gated against currentness;
- route errors clear reserved placeholder Turns instead of leaving stale active
  records.

Remaining work:

- replace remaining manual commit sequencing with typed `TurnGuard` operations;
- separate the Turn Runner from command/intake code;
- make failure-to-unhealthy binding updates require a current `TurnGuard`;
- centralize stream event and final reply projection in one Turn-scoped output
  Module.

Required tests:

- old successful Turn cannot commit after replacement;
- old failed Turn cannot mark Executor Binding unhealthy after replacement;
- old stream events are dropped after replacement;
- final reply belongs to the latest Turn only;
- route validation failures never create ghost active Turns.

### Phase 3: Replace Channel Raw Generation Interface `[mostly implemented]`

Already present:

- `preempt()` and `handle_preempted_with_context()` have been replaced with
  opaque reservation routing;
- Slack reserves before slow thread/file context fetch for ordinary messages;
- QQ reserves before spawning route work for ordinary messages;
- approval and command paths stay explicit.

Remaining work:

- keep Slack context generations and QQ reply generations clearly scoped to
  platform-cache ordering;
- prevent adapter-local generation concepts from leaking back into router
  state decisions;
- consider replacing cache generation bookkeeping with opaque reservation or
  Turn tokens if cache mutation must be tied to router lifecycle.

Required tests:

- Slack second message reaches router before old Turn finishes;
- QQ second message reaches router before old Turn finishes;
- approval resolution does not interrupt active Turn;
- ordinary text without pending approval does interrupt active Turn;
- stale adapter cache updates cannot update router Session state.

### Phase 4: Make Context Commit Turn-Guarded `[mostly implemented]`

Already present:

- context sync commit logic is localized in `PreparedContextSync`;
- reserved context commit requires an adopted current `TurnGuard`;
- route errors discard reserved placeholder Turns.
- stale after file install rolls back files without saving new context state;
- stale after Session state save rolls back files and restores old Session
  state.

Remaining work:

- decide whether `PreparedContextSync` should move into a dedicated context
  commit Module instead of living in `src/router.rs`;
- add any missing lower-level failed-install coverage that is not already
  covered by `src/session/context.rs`;
- keep context rollback semantics independent from Channel Adapter cache
  ordering.

Required tests:

- stale before install;
- stale after install before state save `[covered]`;
- stale after state save before finalization `[covered]`;
- failed install restores replaced files and old state;
- reserved context validation failure clears placeholder active Turns
  `[covered]`.

### Phase 5: Refactor Executor Backend Turn Interface `[partial]`

Already present:

- `ExecutorTurnRef` carries `session_key`, `executor`, and `generation`;
- prepare, prompt, and interrupt request types carry `ExecutorTurnRef`;
- router builds `ExecutorTurnRef` from `TurnGuard` and `InterruptedTurn`;
- `ExecutorRegistry`, ACP, and Codex app-server route through
  `request.turn.executor`;
- router tests verify prepare and prompt share one Turn identity, and interrupt
  carries the interrupted Turn identity.

Remaining work:

- Document prepare cancellation and Backend Session ownership contracts in
  `src/executor/mod.rs`.
- Map cancelled prepare to a router cancellation outcome rather than generic
  unhealthy state.
- Add fake Executor tests that model:
  - prepare cancelled before Backend Session publication;
  - prepare cancelled after Backend Session publication;
  - prompt cancelled before first backend event;
  - prompt cancelled after backend events.

Phase 5 is the required bridge before ACP and Codex can be made robust. Without
it, adapter code will keep guessing whether a cancelled prepare owns the
Backend Session it touched.

### Phase 6: Refactor ACP Backend Session Lifecycle `[partial]`

- Implemented:
  - soft `session/cancel` for local prompt cancellation without closing the ACP
    process;
  - generation-checked interrupt-readable active prompt metadata outside the
    long session lock;
  - a cancel barrier while a cancelled prompt's JSON-RPC response is still
    pending, preventing late cancelled-turn `session/update` and permission
    requests from being projected into the replacement prompt;
  - cancelled ACP stop reason maps to `ExecutorPromptOutcome::Cancelled`;
  - pending approvals are removed when their owning prompt is cancelled;
  - tests for active-prompt interrupt, stale interrupt isolation, late-update
    and late-permission isolation, soft cancel session reuse, cancelled stop
    reason, and approval cleanup.
- Remaining:
  - introduce a named ACP Backend Session Manager instead of keeping the
    manager behavior directly in `AcpExecutorManager`;
  - make unhealthy process replacement an explicit handle-identity operation;
  - add cancelled-prepare publication tests at the ACP adapter boundary;
  - unhealthy session replacement does not remove a newer session.

### Phase 7: Refactor Codex App-Server Backend Session Lifecycle `[pending]`

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

### Phase 8: Delete Old Scattered State Rules `[pending]`

- Remove raw generation checks from Channel Adapters unless they are strictly
  local platform-cache sequence numbers.
- Remove duplicated context-cache currentness code that the Turn Guard replaces.
- Remove `created_session` cancellation cleanup from Executor adapters unless
  it only refers to unpublished resources.
- Remove router helper methods that expose active generation directly.
- Remove any fallback path where cancellation is handled as ordinary backend
  failure.

### Phase 9: Final Verification `[per phase and final]`

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

1. Whether to split the route execution path into `src/router/runner.rs` or keep
   it in `src/router.rs` until Executor Backend Session Managers are done.
2. Whether `TurnGuard::commit_session()` should take a closure over
   `SessionState` or expose narrower typed operations for transcript, binding,
   and context commits.
3. Whether channel-side cache updates should keep local platform sequence
   numbers or use an opaque Turn token directly when cache mutation is tied to
   router lifecycle.
4. Codex app-server's true soft-cancel protocol. If it has no turn cancel
   method, close/recreate must be isolated as documented technical debt.
5. How much of Backend Session Manager should be shared between ACP and Codex
   versus duplicated initially for clarity.
6. Whether context commit should remain as `PreparedContextSync` in the router
   or move into `src/session/context.rs` behind a router-facing commit type.
