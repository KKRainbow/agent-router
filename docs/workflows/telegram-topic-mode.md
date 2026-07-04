# Telegram Topic Mode

Telegram support is implemented as a channel adapter that treats a private chat,
group chat, or forum topic as the router session boundary. The first version
uses the Bot API `getUpdates` long-polling API. Webhooks, media handling, inline
keyboard approvals, and topic administration are outside this workflow.

## Update Intake

The channel validates credentials with `getMe` at startup and records the bot
username. Before polling it calls `deleteWebhook` with
`drop_pending_updates=false` so long polling can receive the existing update
stream without discarding messages.

Polling uses `getUpdates` with `allowed_updates=["message"]`, a configured
timeout, and a monotonically increasing offset. Each processed `update_id`
advances the offset to `update_id + 1`. Empty text messages, messages sent by
the bot itself, duplicate updates, unsupported chat types, and disallowed users
or chats are ignored.

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
/yolo@botname    -> /yolo
/approve@botname -> /approve
/deny@botname    -> /deny
```

Mentions of the bot username are stripped from routed text. If the remaining
text is empty, the message is ignored.

## Output Targeting

Replies use the original `chat_id`. Topic replies include the original
`message_thread_id` in every Telegram API call so replies remain inside the
same forum topic.

The first implementation sends plain text only. `sendMessage`, `editMessageText`,
and `deleteMessage` all target the chat and include `message_thread_id` when the
session is a topic. Final replies longer than Telegram's 4096 character message
limit are split into multiple plain-text messages, and every fragment keeps the
same topic target.

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
