---
name: agent-router
description: Create, inspect, update, pause, resume, or delete scheduled agent tasks in Agent Router. Use for reminders, recurring reports, and periodic follow-up that should continue after the current conversation turn.
---

<!-- agent-router:managed-skill -->

# Scheduled tasks

Agent Router owns task persistence, timing, execution, and reply delivery.
Use its HTTP API through the shell. No MCP server or additional client package
is needed. Read `AGENT_ROUTER_API_URL` and `AGENT_ROUTER_API_TOKEN` from the
process environment. Send `Authorization: Bearer <token>` on every request.
Do not print the token or include it in a task prompt.

If these variables are absent, this session has no scheduling API configured.
Do not claim a task was created unless the API returned success and a task ID.

## API

All paths below are relative to `AGENT_ROUTER_API_URL`. JSON requests use
`Content-Type: application/json`.

| Method and path | Purpose |
| --- | --- |
| `GET /api/context` | Current session, default executor, configured executors, and server time (`now_ms`) |
| `GET /api/schedules/tasks` | List this conversation's tasks, next run times, and recent execution records |
| `POST /api/schedules/tasks` | Create a task |
| `GET /api/schedules/tasks/{id}` | Inspect a task and its last 20 runs |
| `PATCH /api/schedules/tasks/{id}` | Change `name`, `prompt`, `schedule`, `executor`, or `enabled` |
| `DELETE /api/schedules/tasks/{id}` | Delete a task |
| `POST /api/schedules/tasks/{id}/run` | Queue one immediate run, including for a paused task |

Create with `name`, `prompt`, and `schedule`. `executor` is optional and defaults
to the current agent. Task ownership and notification destination come from the
session credential; do not supply session IDs or channel targets.
If `/api/context` works but `/api/schedules/tasks` returns 404, scheduling is
disabled on this Router. Ask the user to enable `schedules.enabled`.

The schedule has one of these shapes:

- Once: `{"type":"once","at":"2030-01-02T09:00:00+08:00"}`.
  The timestamp must be in the future and include an explicit UTC offset.
- Interval: `{"type":"interval","seconds":1800}`.
  The first run starts after this delay.
- Cron: `{"type":"cron","expression":"0 9 * * 1-5","timezone":"Asia/Shanghai"}`.
  Use five fields: minute, hour, day of month, month, weekday. Use the user's
  intended IANA timezone; clarify when it cannot be inferred.

For example, save the following body in a temporary JSON file, using the actual
project or resource the user wants followed:

```json
{
  "name": "Check CI",
  "prompt": "Check the latest CI run for https://github.com/KKRainbow/agent-router. Report its status. When it finishes, pause this task using AGENT_ROUTER_TASK_ID and summarize the result.",
  "schedule": {"type":"interval","seconds":1800}
}
```

On POSIX shells, send that file with:

```sh
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $AGENT_ROUTER_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @task.json "$AGENT_ROUTER_API_URL/api/schedules/tasks"
```

On Windows, use PowerShell's `Invoke-RestMethod` with `$env:AGENT_ROUTER_API_URL`
and `$env:AGENT_ROUTER_API_TOKEN`, or an available HTTP client. Avoid shell
interpolation of the JSON body; send a file or serialize a native object.

## Execution behavior

- Write a self-contained prompt with relevant URLs, absolute project paths,
  intended work, and stopping conditions. Each task has a separate conversation
  and workspace; it does not inherit the original chat or its synced files.
- The selected executor is fixed until the task is updated. Switching agents in
  the original conversation does not change existing tasks.
- Results return to the originating conversation. Normal executor permission
  rules still apply; tool approval requests return there too.
- During a scheduled run, `AGENT_ROUTER_TASK_ID` identifies the current task.
  The agent can update or pause its task when the user's requested condition is
  met. Respect the scope of the user's original request.
- Pause with `{"enabled":false}`; resume with `{"enabled":true}`. A completed
  one-time task needs a new future schedule before it can be resumed.
- Pause and delete prevent future runs; they do not cancel a run already in
  progress. Concurrent runs of the same task are rejected.
- Router must be running. Missed recurring occurrences are skipped on restart;
  an overdue one-time task runs once. A run interrupted by a restart is recorded
  as interrupted and is not automatically replayed.
- Failed runs and notification errors remain in execution records. Do not
  recreate or rerun a task blindly after an ambiguous HTTP failure: inspect the
  task list first, since work may already have been accepted.

After a successful change, report the task name/ID, schedule with timezone, and
next execution time. Do not expose internal credentials.
