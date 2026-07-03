# ACP Session-Level Async Output

## 背景

当前 ACP executor 的输出被绑定在 router turn 生命周期里。`session/prompt`
的 JSON-RPC response 一返回，router 就认为本轮 prompt 已结束：它会短暂
drain 当前 receiver 中已经到达的 `session/update`，随后提交 transcript、
发送 final reply，并丢弃 turn-scoped output sink。

这会导致一个实际问题：Kimi ACP 可能在主回复中说明“子代理还在继续扫描”，
然后先返回 `session/prompt` response。子代理之后产出的 `session/update`
仍然会从 ACP stdout 到达，但此时 router 已经没有任何活跃 turn sink 订阅它，
所以这些结果被静默丢弃。

## 根因

根因不是 Slack 没刷新，也不是 UI 没渲染，而是 router 的抽象把 ACP
`session/prompt` response 当成了“backend session 不会再输出”的信号。
对很多 agent runtime 来说，这个假设不成立：

- `session/prompt` response 只表示当前 prompt RPC 返回。
- backend session 仍然可能有后台子代理、异步工具或延迟总结继续输出。
- ACP `session/update` 不一定携带 `turn_id`、`request_id` 或 `subtask_id`，
  router 无法可靠判断一条 late update 属于哪个历史 prompt。

因此，修复方向不能是“等待明确的 turn 完成条件”。在没有协议级 turn
归属信息时，强行等待或按 generation 丢弃都会制造新的错误。

## 设计目标

1. 正常 prompt response 返回后，backend session 的后续 assistant output
   仍然能投递到原会话所在的 channel。
2. 不发明不存在的 turn 归属关系。ACP update 没有 id 时，router 只能按
   backend session 级别处理。
3. 保持 channel 边界：ACP adapter 不直接调用 Slack/QQ/Web，所有输出仍经过
   router 的统一输出抽象。
4. 只过滤 router 能确定不再 authoritative 的输出，例如已 discard/restart
   的 backend session。
5. `/stop` 或新 prompt 替换只取消当前 prompt RPC，不默认静音整个 backend
   session 的未来后台输出。
6. transcript 按实际投递顺序持久化，避免“用户看到了但 router 不记得”。

## 非目标

- 不试图从文本内容猜测 late update 属于哪个 prompt。
- 不把 ACP backend 改成 master-worker 架构。
- 不要求所有 ACP provider 立刻增加 `turn_id`。
- 不用固定 sleep 当作“等待子代理完成”的协议语义。

## 核心原则

### Turn 完成不等于 Session 静默

router turn 是用户消息被接收、路由、prompt、提交的一次事务。
ACP backend session 是 executor 内部可复用的会话。一个 turn 正常完成后，
backend session 仍然可能继续输出。

### Session Update 默认按 Backend Session 处理

当 `session/update` 没有 `turn_id`、`request_id` 或 `subtask_id` 时，router
不得把它归属到某个历史 prompt。它只能判断这条 update 是否来自当前仍然有效的
backend session。

### Soft Cancel 不等于 Hard Reset

`/stop`、停止按钮、或新消息替换旧 prompt，只能表示取消当前 prompt RPC。
如果用户想彻底不要该 executor 后续所有后台输出，需要一个 hard reset/discard
语义：关闭并替换 backend session。

## Authoritative Backend Session

router 需要为每个 executor binding 维护一个 authoritative backend session
identity。至少包含：

- router session key
- executor name
- protocol
- external backend session id
- process/session instance id
- machine id 和 cwd（用于审计和排错）

ACP stdout reader 投递 `session/update` 时，需要附带它所属的 backend session
identity。router 收到 async update 后，只做可证明的过滤：

- identity 与当前 executor binding 不匹配：丢弃。
- backend process 已被 discard/restart：丢弃。
- 当前处在 ACP cancel barrier 期间：临时丢弃，直到被取消 prompt 的 response
  settle 或 session 被关闭。

不要按旧 turn generation 做永久丢弃。没有 turn id 时，generation 过滤会把
合法的后台子代理输出一起丢掉。

## Cancel Barrier 语义

现有 ACP cancel barrier 仍然有价值，但范围必须很窄。

当 router 取消一个 pending `session/prompt` request 时：

1. router 发送 `session/cancel`。
2. 进入 cancel barrier。
3. 在被取消 prompt 的 response 到达前，忽略 `session/update`，并对
   `session/request_permission` 返回 cancelled。
4. response settle 后退出 barrier。
5. 如果同一个 backend session 之后继续输出，按 session-level async output
   正常投递。

这个 barrier 只保护“取消响应还未 settle 的短窗口”，不是永久静音。

## 输出路由

需要新增一个 session-level async output 路径，和 turn-scoped output 并列。

### Active Turn 内

当同一 backend session 正在处理 active turn 时，已有行为保持：

- `agent_message_chunk` 进入当前 reply stream。
- progress/tool/reasoning update 进入 channel event。
- turn 完成后，当前 final assistant text 写入 transcript。

如果后台旧任务的 update 恰好在 active turn 期间到达，而 ACP 没有 id，router
无法区分它和当前 prompt 的 output。此时只能按到达顺序输出。这是协议信息不足
带来的限制，不能通过本地猜测可靠修复。

### 没有 Active Turn 时

当 backend session 没有 active turn，但收到 authoritative `session/update`：

- `agent_message_chunk` 作为 async assistant follow-up 输出到同一个 channel
  session。
- progress/tool/reasoning update 按 channel event policy 投递。
- assistant text 成功投递后，追加为 canonical transcript 中的 assistant
  message。
- 该 transcript entry 不需要配对新的 user message。

### 有其他 Active Turn 时

如果同一个 router session 已有新的 active turn，而同一个 ACP backend session
又产生 update，ACP 无 id 时仍无法判断它属于旧后台任务还是新 prompt。

第一版采用简单规则：按到达顺序输出，不按 generation 丢弃。channel 上可把这类
输出标记为 async follow-up，避免用户误以为它一定是对最新消息的回答。

## Async Reply Buffer

后台 assistant output 可能以多个 chunk 到达。由于没有“后台输出结束”信号，不能
无限等待完整消息。需要把“完成判断”和“投递批处理”分开：

- 如果 update 带 `reply_message_id`，按 message id 聚合；message id 变化时
  flush 前一个 buffer。
- 如果没有 id，按 backend session 维护一个 rolling buffer。
- buffer 通过短 inactivity debounce、最大字节数、或后续 activity event 触发
  flush。
- flush 后又来的 chunk 作为新的 follow-up message 投递。

debounce 不是 turn 完成语义，只是避免把每个 token/chunk 单独发到 Slack。

## Router/Channel 边界

ACP adapter 只负责：

- 维护 backend process 和 JSON-RPC 状态。
- 把 ACP message project 成 `ExecutorUpdate`。
- 为 update 附带 backend session identity。

router 负责：

- 判断 identity 是否仍 authoritative。
- 判断当前是否有 active turn sink。
- 将 update 路由到 turn output 或 async output。
- 按 session lock 串行提交 transcript。

channel adapter 负责：

- 为 router session 提供可复用的 output target。
- 按 channel policy 投递 final reply、reply chunk、channel event。
- 不理解 ACP 协议细节。

## Transcript 规则

### Turn Reply

active turn 内的 assistant final text 仍按现有逻辑写入 transcript：

- 追加 user message。
- 追加 assistant message。
- 更新 executor binding 的 seen-context cursor。

### Async Follow-Up

turn 完成后的 async assistant text 写入 transcript 时：

- 只追加 assistant message。
- 使用当前 authoritative executor name 和 external backend session id。
- 不创建 synthetic user message。
- 不推进其他 executor 的 seen-context cursor。
- 与并发 turn commit 使用同一个 session lock，按实际投递顺序写入。

如果 async output 只有 progress/tool event 且没有 assistant text，第一版默认不写
transcript，除非 channel 已经向用户投递了可读总结并且产品上希望持久化这些活动。

## `/stop` 和 Hard Reset

`/stop` 的语义保持为 soft cancel：

- 取消当前 active turn。
- 对 ACP 发送 `session/cancel`。
- cancel barrier 期间丢弃残留 update。
- barrier 退出后，同一 backend session 的未来 async output 仍可投递。

如果用户要彻底停止后台子代理和后续输出，需要新增或复用 hard reset 命令：

- discard 当前 backend session。
- 关闭 ACP process。
- 清除当前 executor binding 的 live session authority。
- 后续来自旧 process/session 的 output 全部丢弃。

这个语义应在用户可见文案里明确区别于 `/stop`。

## Permission 请求

后台 async 工作仍可能触发 `session/request_permission`。

- 如果 backend session authoritative，approval 可以正常进入现有 approval broker。
- 如果没有 active turn，requester user id 可以为空，但 approval 仍绑定
  router session key 和 executor。
- cancel barrier 期间的 permission request 返回 cancelled。
- hard reset/discard 后的 permission request 不再被接受。

不要因为没有 active turn 就自动 approve。

## 实现步骤

1. 引入 backend session identity，并在 ACP session 创建、resume、discard 时维护。
2. 将 ACP `session/update` 从纯 turn-scoped receiver 改成进入 router 级 dispatcher。
3. 为 active turn 注册临时 sink；turn 结束时解除注册，但保留 session-level
   async output target。
4. 增加 async output broker：负责 identity 校验、buffer、channel 投递和 transcript
   commit。
5. 保留现有 prompt response 结束 turn 的行为，不等待后台输出。
6. 增加 hard reset/discard 用户语义，作为“彻底不要后台输出”的明确操作。

## 测试计划

需要覆盖以下场景：

- fake ACP 在 `session/prompt` response 后延迟发送 assistant update；router 应投递
  async follow-up，并写 transcript。
- fake ACP 在正常 prompt response 后延迟发送 tool/progress update；按 channel
  event policy 投递，不进入 turn final text。
- cancel barrier 期间的 update 被丢弃；barrier settle 后同一 authoritative session
  的 update 可以投递。
- 新 prompt 替换旧 prompt 后，不按旧 generation 永久丢弃后续 authoritative update。
- discard/restart 后，旧 process/session identity 的 update 被丢弃。
- async assistant chunk 没有 message id 时按 debounce/size flush，不逐 chunk spam。
- async assistant chunk 有 message id 时按 id 聚合和分段。
- async transcript commit 与 active turn commit 并发时，session lock 保证顺序一致。

## 协议增强建议

如果未来 ACP provider 能提供更多 metadata，router 应优先使用：

- `turn_id` 或 `request_id`
- `message_id`
- `parent_task_id` / `subtask_id`
- `is_async_followup`
- `background_task_completed`

有这些字段后，router 可以精确归属 output、区分旧后台任务和新 prompt，并减少
arrival-order 输出的歧义。但第一版不能依赖这些字段存在。

## 结论

Kimi ACP 子代理结果丢失的根因是 router 缺少 session-level async output 路径。
正确修复不是延长 turn，也不是按 generation 丢 late update，而是承认 ACP
backend session 可以在 prompt response 之后继续输出。

第一版应按 authoritative backend session 投递 async output；只丢弃明确来自旧
process/session 或 cancel barrier 窗口内的 update。这样可以恢复后台子代理结果，
同时保持 router/channel/backend 的边界清晰。
