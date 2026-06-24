# Slack User Token Proxy 方案

## 状态

草案。本文档记录单用户 Slack user token proxy 的设计方向，用于支持授权用户
在自己和同事的 1:1 DM thread 中显式 `@bot` 触发 Agent Router。

## 背景

Slack bot token 无法可靠处理“两个真人之间的 DM 里 `@bot`”这个场景：

- `app_mention` 不会作为 DM 中的 app mention 入口使用。
- bot token 只能看到 bot 所在或被授权可见的 conversations。
- bot token 不能随意把消息发进两个真人之间的 1:1 DM。

Slack user token 表示某个 workspace member。它可以在授权 scope 范围内访问这个
用户可见的 conversations，并以该用户身份执行写操作。因此，要支持这个场景，
Agent Router 需要显式增加一个单用户 proxy 模式，而不是把 `xoxp-...` 塞进现有
`SLACK_BOT_TOKEN`。

Slack 官方文档：

- User token 表示授权用户，写操作按该用户身份执行：
  <https://docs.slack.dev/authentication/tokens>
- `message.im` 是 direct message channel 的消息事件，要求 `im:history`：
  <https://docs.slack.dev/reference/events/message.im>
- `chat:write` 支持 Bot token 和 User token，可用于 `chat.postMessage`：
  <https://docs.slack.dev/reference/scopes/chat.write>
- `chat.postMessage` 的消息身份取决于 token 类型：
  <https://docs.slack.dev/reference/methods/chat.postMessage>

## 设计结论

Agent Router 应新增一个默认关闭的 `slack.user_dm` 能力：

- 保留现有 bot-token Slack channel 行为。
- 使用单个 `SLACK_USER_TOKEN` 监听授权用户可见的 1:1 DM events。
- 只允许授权用户本人在 1:1 DM thread 中显式 `@bot` 触发 agent。
- 首次 `@bot` 激活的 scope 是当前 thread，不是整条 DM。
- 激活后的同一 thread 中，未 `@bot` 的后续消息可以 `observe`，用于补充上下文，
  但不触发回复。
- 回复使用 user token 发回同一个 Slack thread，因此在 Slack 客户端中显示为
  授权用户本人发送。

这个能力不做多人 OAuth，不保存多个 user token，也不让同事的消息直接触发以授权
用户身份发出的 agent 回复。

## 目标

- 支持授权用户在自己和同事的 1:1 DM thread 中 `@bot` 后触发 agent。
- 回复发送到同一个 thread，并显示为授权用户本人发送。
- 未激活的 DM/thread 不 route、不 observe。
- 已激活的同一 thread 中，后续未 `@bot` 的消息只 observe。
- 普通 Slack channel、private channel 和 bot DM 行为保持不变。
- Slack context sync 在 user-proxy session 中使用 user token 读取当前 thread。
- 明确隐私边界，避免把授权用户所有 DM 都纳入 router 上下文。

## 非目标

- 不支持多人 OAuth 授权和多 user token 存储。
- 不支持 `message.mpim` group DM，第一版只做 1:1 DM。
- 不允许同事在未激活 thread 中 `@bot` 后触发以授权用户身份发消息。
- 不把 `free_response_channels` 语义扩展到 user DM。
- 不把 user token 当作 bot token 使用。
- 不做 Slack 全量 DM inbox、搜索或客服系统。

## Slack App 配置

在 Slack App 的 OAuth & Permissions 中配置 User Token Scopes：

```text
im:history
im:read
chat:write
```

Event Subscriptions 中订阅 user 视角事件：

```text
message.im
```

如果未来支持 group DM，才考虑增加：

```text
mpim:history
mpim:read
message.mpim
```

重新安装或重新授权 app 后，Slack 会生成 `User OAuth Token`，形如：

```text
xoxp-...
```

该 token 应作为独立的 user token 配置，不应替代现有 bot token。

## 本地配置

建议新增 YAML 配置：

```yaml
slack:
  user_dm:
    enabled: false
    require_mention: true
    allowed_users: []
    allowed_channels: []
```

对应环境变量：

```bash
SLACK_USER_DM_ENABLED=true
SLACK_USER_TOKEN=xoxp-...
SLACK_USER_DM_ALLOWED_USERS=U_COLLEAGUE_1,U_COLLEAGUE_2
SLACK_USER_DM_ALLOWED_CHANNELS=D1234567890
```

配置含义：

- `enabled`：默认关闭。不开启时不初始化 user token，也不处理 user DM events。
- `SLACK_USER_TOKEN`：授权用户的 `xoxp-...` token。
- `require_mention`：第一版固定等价于 `true`，只作为显式配置项保留。
- `allowed_users`：允许 proxy 模式处理的同事 user id 列表。
- `allowed_channels`：允许 proxy 模式处理的 DM channel id 列表，便于无法稳定解析
  peer user 时做显式授权。

第一版应要求 `allowed_users` 或 `allowed_channels` 至少配置一个，避免误处理授权
用户所有 DM。

## Token 角色

运行时需要维护两个 Slack token 视角：

```rust
enum SlackTokenKind {
    Bot,
    UserProxy,
}
```

- `Bot`：现有 `SLACK_BOT_TOKEN`，用于普通 channel、private channel、bot DM。
- `UserProxy`：新增 `SLACK_USER_TOKEN`，仅用于单用户 1:1 DM proxy session。

启动时分别调用 `auth.test`：

- bot token 得到 `bot_user_id`。
- user token 得到 `proxy_user_id`。

这两个 id 都需要参与事件分类，不能只靠 channel id 是否以 `D` 开头判断。

## Session 语义

现有 Slack session key 继续保留：

```text
slack:channel:<channel>:<thread_ts>
slack:dm:<channel>:<thread_ts>
```

user-proxy session 使用独立 namespace：

```text
slack:user-dm:<proxy_user_id>:<channel>:<thread_root_ts>
```

示例：

```text
slack:user-dm:U_ME:D1234567890:1710000000.000100
```

这样可以避免授权用户和 bot 的普通 DM、channel thread、approval 状态和 context
artifact 混在一起。

## 触发和 Observe 规则

user-proxy 模式按 thread 粒度激活：

1. 未激活的 1:1 DM thread 中，只有授权用户本人发送且包含 `<@bot_user_id>` 的消息
   才 route。
2. 首次 route 成功后，该 session key 标记为已激活。
3. 已激活的同一 thread 中：
   - 授权用户本人再次发送包含 `<@bot_user_id>` 的消息时 route。
   - 未 `@bot` 的消息 observe，但不触发回复。
   - 同事发送的消息 observe，但不触发回复，即使文本包含 `<@bot_user_id>`。
4. 另一个 top-level DM 或另一个 thread 需要重新由授权用户本人显式 `@bot` 激活。

这条规则保证 agent 不会因为同事在 DM 中提到 bot，就自动以授权用户身份发消息；
同时，已经明确邀请 agent 参与的 thread 能持续获得后续上下文。

## 事件分类

`parse_message_event` 需要保留更多 Slack envelope 信息：

- event type
- channel
- channel type
- event user
- message ts
- thread ts
- authorizations
- bot id
- subtype
- files

user-proxy event 的必要条件：

- `slack.user_dm.enabled == true`
- event type 是 `message`
- `event.channel_type == "im"`
- `authorizations` 中包含 `user_id == proxy_user_id` 且 `is_bot == false`
- event 没有真实 `bot_id`
- channel 或 peer user 命中 allowlist
- event 不是 router 通过 user token 刚刚发出的消息

只有满足这些条件后，才进入 user-proxy route/observe 判断。其他 Slack message
继续走现有 bot-token 逻辑。

## DM Peer 解析

为了支持 `allowed_users`，adapter 需要知道 `D...` channel 对应的另一个用户。

第一版可以通过 user token 调用 `conversations.info` 并缓存结果：

- key：DM channel id。
- value：peer user id 或 unresolved reason。
- token：`SLACK_USER_TOKEN`。

如果 `conversations.info` 权限不足或返回结构不能稳定解析 peer，则允许通过
`allowed_channels` 显式授权 `D...` channel。权限错误应记录为结构化日志，但不要把
DM 消息正文写入日志。

## 回复发送

`SlackReplyTarget` 需要携带 token 视角：

```rust
struct SlackReplyTarget {
    channel: String,
    thread_ts: Option<String>,
    token_kind: SlackTokenKind,
}
```

发送策略：

- `SlackTokenKind::Bot`：使用 `SLACK_BOT_TOKEN`。
- `SlackTokenKind::UserProxy`：使用 `SLACK_USER_TOKEN`。

user-proxy 回复必须带上 Slack event 对应的 `thread_ts`：

- 如果触发消息本身是 thread reply，使用原始 `thread_ts`。
- 如果触发消息是 top-level DM，使用该消息 `ts` 作为 thread root。

`chat.postMessage` 成功后，记录返回的 message `ts`，用于防止收到自己刚发出的
event 后重复处理。

## 防循环

不能简单丢弃 `event.user == proxy_user_id`，因为授权用户本人发送的 `@bot` 消息
正是触发入口。

需要两层防护：

1. user-proxy route 只接受包含 `<@bot_user_id>` 的授权用户消息。
2. adapter 维护短期 sent-message dedupe，按 `(channel, ts)` 或
   `(channel, thread_ts, ts)` 忽略通过 user token 刚发出的回复。

被忽略的自发消息既不 route，也不 observe。

## Context Sync

user-proxy session 中的 Slack context sync 必须使用 user token。以下 API 和文件
下载路径不能继续硬编码 bot token：

- `conversations.replies`
- `conversations.info`
- `files.info`
- Slack private file download URL

第一次由授权用户 `@bot` 激活 thread 时，可以同步当前 thread 的既有历史。这个
行为等价于授权用户把当前 thread 上下文显式交给 agent，而不是授权整条 DM。

未激活 thread 不主动拉取历史，也不 observe 后续消息。

## 隐私和日志

user-proxy 模式处理的是授权用户可见的私人 DM，默认策略必须保守：

- 默认关闭。
- `allowed_users` 或 `allowed_channels` 必须至少配置一个。
- 未激活 thread 不 observe。
- 未命中 allowlist 的 event 直接丢弃。
- 日志只记录 channel、user id、session key、text length、decision，不记录正文。
- Context artifacts 只在明确激活后的 session workspace 中生成。
- 文档和配置必须明确说明：user token 回复在 Slack 中显示为授权用户本人发送。

## 实现步骤

1. 扩展配置解析：
   - 新增 `SlackUserDmConfig`。
   - 支持 YAML 和环境变量。
   - 增加配置单测。
2. 扩展 Slack token 运行时状态：
   - 启动时可选 `auth.test(user_token)`。
   - 保存 `proxy_user_id`。
3. 扩展 event model：
   - 保留 `channel_type` 和 `authorizations`。
   - 区分 bot-token event 和 user-proxy event。
4. 实现 user-proxy route/observe 规则：
   - 只由授权用户本人 mention 激活。
   - 激活后同 thread observe。
   - 独立 session key namespace。
5. 扩展 reply target：
   - 携带 `SlackTokenKind`。
   - `post_message` 按 token kind 选择 bearer token。
6. 增加 sent-message dedupe：
   - user token 发出的回复不 route、不 observe。
7. 扩展 context sync：
   - Slack API 调用和 file download 接受 token kind。
   - user-proxy session 使用 user token。
8. 更新 README 和 example config。

## 测试计划

单元测试：

- 配置能解析 `SLACK_USER_DM_ENABLED`、`SLACK_USER_TOKEN`、allowlist。
- user-proxy `message.im` 中授权用户 `@bot` 会 route。
- 未激活 user-proxy thread 中未 `@bot` 的消息不 route、不 observe。
- 已激活 user-proxy thread 中未 `@bot` 的消息 observe。
- 同事在未激活 thread 中 `@bot` 不 route。
- 同事在已激活 thread 中 `@bot` 只 observe，不 route。
- user-proxy session key 与普通 `slack:dm` session key 隔离。
- user-proxy reply target 使用 `SlackTokenKind::UserProxy`。
- user token 刚发送的返回 `ts` 被 dedupe 忽略。
- context sync 在 user-proxy session 中选择 user token。

集成验证：

- 在 Slack 中选择一个 allowlist 内的 1:1 DM。
- 在某条消息 thread 中由授权用户发送 `<@bot> ...`。
- 确认 router 日志显示 user-proxy route。
- 确认 agent 回复出现在同一 thread，且显示为授权用户本人发送。
- 在同一 thread 继续发送不带 mention 的消息，确认只 observe。
- 在另一个 thread 不带 mention 发消息，确认不 route、不 observe。

