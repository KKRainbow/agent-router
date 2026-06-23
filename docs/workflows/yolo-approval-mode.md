# YOLO Approval Mode Workflow

## Goal

YOLO mode lets Agent Router automatically approve permission requests for a
selected approval scope. It is intended for trusted operator-controlled sessions
where interruption is more expensive than manual review.

The implementation should keep approval policy centralized in the router layer.
Channel adapters and executor backends should not each implement their own
YOLO shortcuts.

## Terminology

- `normal`: permission requests are sent through the existing approval prompt
  flow.
- `yolo`: permission requests are automatically approved when the request has a
  safe allow option.
- `global_default`: operator-configured default approval mode for all sessions.
- `session_override`: per-session override. When absent, the session inherits
  `global_default`.
- `effective_mode`: `session_override` if set, otherwise `global_default`.

## Scope Model

Approval mode should be evaluated per router session:

```text
effective_mode(session_key) =
  session.approval_mode_override.unwrap_or(config.approval.default_mode)
```

Global YOLO is allowed, but it should be an operator configuration rather than
a regular chat command. A chat-visible global toggle would let one allowed
message sender affect unrelated sessions.

The first implementation should support:

```yaml
approval:
  default_mode: normal  # normal | yolo
```

The same value can be overridden by `AGENT_ROUTER_APPROVAL_DEFAULT_MODE` or
`AGENT_ROUTER_APPROVAL_MODE`.

and a per-session override stored in `SessionState`:

```text
approval_mode_override: inherit | normal | yolo
```

## Commands

Per-session commands should mutate only the current `session_key`:

```text
/yolo status
/yolo on
/yolo off
/yolo inherit
```

- `/yolo status` reports the global default, session override, and effective
  mode.
- `/yolo on` sets the current session override to `yolo`.
- `/yolo off` sets the current session override to `normal`.
- `/yolo inherit` clears the override and returns to the global default.

When `/new` is implemented, it must clear the session override so a reset
session returns to the operator-configured default.

Slack slash commands should not be used to change a thread-specific YOLO mode in
version 1 because Slack slash command payloads are channel-scoped, not
thread-scoped. Normal thread messages such as `/yolo on` in the target session
are the clear behavior.

## Approval Policy Boundary

The approval broker should own pending approvals, but it should not need to know
the structure of `SessionState`. Add a small policy boundary:

```rust
#[async_trait]
trait ApprovalPolicy: Send + Sync {
    async fn auto_selection(&self, request: &ApprovalRequest) -> Option<ApprovalSelection>;
}
```

`ApprovalBroker::request()` should ask this policy before creating a pending
request and before publishing an approval prompt:

```text
executor requests approval
  -> ApprovalBroker::request(request)
  -> policy.auto_selection(request)
     -> Some(selection): return selection immediately
     -> None: create pending approval and publish prompt
```

A `SessionApprovalPolicy` can depend on the session store and router config to
compute `effective_mode`. The broker remains independent from concrete session
storage.

## Auto-Approval Rules

YOLO mode should only select an existing allow option from the request.

- Prefer one-shot allow options such as `allow_once`.
- Do not auto-select persistent options such as `always_allow`.
- If no allow option exists, keep the normal manual approval flow or fail
  closed according to the existing request semantics.
- Do not synthesize approval options that the executor did not offer.

This keeps YOLO from escalating beyond the executor's declared permission
surface.

## User Feedback

Automatic approvals should be visible enough to audit:

- Emit a router channel event when a request is auto-approved.
- Include the request id or short title when available.
- Log the session key, executor, and selected option at info level.

The channel event should be concise; it should not expose secrets or raw tool
payloads.

## State And Isolation

Session approval mode belongs to router-owned session state. It should not live
inside Slack, QQ, ACP, or Codex-specific adapters.

Expected behavior:

- Slack channel thread A can be `yolo` while thread B stays `normal`.
- Slack DM top-level messages are separate sessions and can have separate
  overrides.
- QQ's current session key policy can share one mode for each QQ session.
- Executor-private sessions inherit behavior only through the router's approval
  decisions; they should not cache approval mode independently.

If session persistence is added later, approval overrides should persist with
the session record unless a product decision says resets should always clear
them.

## Security Notes

Global YOLO should be configured by the operator through config, environment, or
startup flags. Chat commands should not toggle global YOLO until Agent Router has
an explicit admin authorization model.

Per-session YOLO commands should still respect the channel's existing access
policy. If a user cannot send normal commands to a session, they cannot enable
YOLO for it.

Approval resolution must keep the current requester/session checks. YOLO changes
how a request is answered before it becomes pending; it must not allow one
session to approve another session's pending request.

## Test Plan

Unit tests should cover:

- Default `normal` mode publishes a pending approval prompt.
- Global default `yolo` auto-selects an allow-once option.
- A session override of `normal` disables global YOLO for that session.
- A session override of `yolo` enables auto-approval only for that session.
- `/yolo inherit` returns the session to the configured global default.
- Requests without an allow option do not get fabricated approvals.
- Two Slack DM top-level sessions can have different effective modes.
- Existing requester and session checks still protect pending manual approvals.

Integration-style router tests should cover:

- `/yolo on` followed by an executor permission request in the same session is
  auto-approved.
- A different session still receives a manual approval prompt.
- The future `/new` session reset clears the session override.
