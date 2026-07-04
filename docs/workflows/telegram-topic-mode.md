# Telegram Topic Mode

Telegram support is implemented as a channel adapter that treats a private chat,
group chat, or forum topic as the router session boundary. The channel uses the
Bot API `getUpdates` long-polling API. Webhooks, media handling, and topic
administration are outside this workflow.

## Update Intake

The channel validates credentials with `getMe` at startup and records the bot
username. Before polling it calls `deleteWebhook` with
`drop_pending_updates=false` so long polling can receive the existing update
stream without discarding messages.

Polling uses `getUpdates` with `allowed_updates=["message", "callback_query"]`,
a configured timeout, and a monotonically increasing offset. Each processed
`update_id` advances the offset to `update_id + 1`. Empty text messages,
messages sent by the bot itself, duplicate updates, unsupported chat types, and
disallowed users or chats are ignored.

## Session Keys

Telegram private chats are single-user sessions:

```text
telegram:private:<chat_id>
```

Telegram groups and supergroups without a forum topic share a chat session:

```text
telegram:chat:<chat_id>
```

Telegram forum topics are isolated by `message_thread_id`:

```text
telegram:chat:<chat_id>:topic:<message_thread_id>
```

`message_thread_id` is the Telegram equivalent of Slack `thread_ts` for Agent
Router purposes. Two topics in the same Telegram group must never share a
router session unless Telegram omits `message_thread_id`.

## Routing Rules

Private chats route by default. Group, supergroup, and topic messages route only
when at least one of these is true:

- `telegram.require_mention` is `false`.
- The message mentions the bot username, for example `@agent_router_bot`.
- The message is a reply to a bot message.
- The message starts with a router-owned command.

Router-owned Telegram commands are normalized before routing so bot username
suffixes do not affect command parsing:

```text
/agent@botname   -> /agent
/stop@botname    -> /stop
/new@botname     -> /new
/yolo@botname    -> /yolo
/approve@botname -> /approve
/deny@botname    -> /deny
```

Mentions of the bot username are stripped from routed text. If the remaining
text is empty, the message is ignored.

## Output Targeting

Replies use the original `chat_id`. Topic replies include the original
`message_thread_id` in `sendMessage` so new replies remain inside the same
forum topic. `editMessageText` and `deleteMessage` identify an existing message
with `chat_id` and `message_id`; they do not carry `message_thread_id`.

Normal assistant output is sent as plain text or Telegram HTML converted from
router markdown. `sendMessage`, `editMessageText`, and `deleteMessage` all
target the original chat. Final replies longer than Telegram's 4096 character
message limit are split into multiple messages, and every `sendMessage` fragment
keeps the same topic target.

## Approval Buttons

Approval prompts are still rendered as text with `/approve <id>` and `/deny
<id>` commands so old clients and manual fallback remain usable. When the prompt
can fit Telegram's inline keyboard constraints, the channel also attaches
Approve/Deny buttons to the final `sendMessage` fragment.

Telegram callback data is limited to 64 bytes. The channel therefore stores only
compact routing data in `callback_data`:

```text
ar:a:<approval_id>
ar:a:<approval_id>:<option_id>
ar:d:<approval_id>
```

The session key is not stored in callback data. On click, the channel derives the
session from the callback message's `chat_id` and `message_thread_id`, then calls
the shared approval broker with the clicking Telegram user id. This preserves the
same resolver checks as text commands: requester-only approvals can only be
resolved by the requester, and chat/user allow lists are still enforced.

After the broker accepts a click, the channel answers the callback query so the
Telegram client stops showing the loading spinner, then edits the approval
message to append `Approved by Telegram user <id>.` or `Denied by Telegram user
<id>.` and removes the inline keyboard. If Telegram rejects the inline keyboard
payload, the channel retries the prompt as plain text.

## Configuration

Telegram configuration can come from YAML or environment variables. A bot token
auto-enables the channel unless `enabled: false` or `TELEGRAM_ENABLED=false` is
set.

```yaml
telegram:
  enabled: true
  bot_token: 123456:...
  require_mention: true
  channel_events: compact
  allowed_users: []
  allowed_chats: []
  poll_timeout_secs: 30
```

Environment variables:

```text
TELEGRAM_ENABLED
TELEGRAM_BOT_TOKEN
TELEGRAM_REQUIRE_MENTION
TELEGRAM_CHANNEL_EVENTS
TELEGRAM_ALLOWED_USERS
TELEGRAM_ALLOWED_CHATS
TELEGRAM_POLL_TIMEOUT_SECS
```

## Test Strategy

Configuration tests cover default-disabled behavior, token auto-enable, env
overrides, and channel event mode parsing. Parser tests cover private, group,
and topic session keys; mention stripping; command suffix normalization; allowed
user and chat filtering; and topic isolation within a single `chat_id`. Output
tests cover topic-aware `sendMessage` bodies and long final-reply splitting
while preserving `message_thread_id`.
