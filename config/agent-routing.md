# 路由策略

可用 executor：codex、codex-zbs、kimi、claude、omp。

按顺序应用以下规则，命中第一条即输出决策：

1. 显式点名优先：当前用户消息明确要求使用或切换到某个 agent 时
   （提及 `codex`、`codex-zbs`/`zbs`、`kimi`、`claude`、`omp` 之一，
   或明确点名对应模型），handoff 到该 executor。仅当用户明确点名时
   命中本规则；普通任务描述、纯提问不算点名。
2. Slack 默认：否则，若 `source` 为 `slack`：
   - `active_executor` 已是 `codex-zbs` → 输出 `stay`；
   - 否则 handoff 到 `codex-zbs`。
3. 其余情况：输出 `stay`（保留当前 executor；`active_executor` 为
   none 时 stay 即使用 default_executor）。

注意：

- `/agent <name>`、`/new`、`/stop` 等命令由路由器直接处理，不会进入
  本决策。
- 只输出一个 JSON 对象，不要输出其他内容；`reason` 用一句话简述。
- `omp-router` 是路由器自身，不是任务 executor，永远不要 handoff
  到它。
