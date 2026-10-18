---
name: agent-router
description: Send messages to configured channels, publish skills shared by all Router sessions, and manage scheduled agent tasks in Agent Router. Use for cross-channel reports, sharing reusable workflows, reminders, and periodic follow-up.
---

<!-- agent-router:managed-skill -->

# Agent Router

Agent Router owns channel delivery, task persistence, timing, and execution.
Use its HTTP API through the shell. No MCP server or additional client package
is needed. Read `AGENT_ROUTER_API_URL` and `AGENT_ROUTER_API_TOKEN` from the
process environment. Send `Authorization: Bearer <token>` on every request.
Do not print the token or include it in a task prompt.

If these variables are absent, this session has no Agent Router API configured.
Do not claim a task was created unless the API returned success and a task ID.

## Send messages

Use messages when the user asks to notify a channel or when a progress update
is useful. Respect the user's requested audience and avoid repeating an already
sent notification in the final reply. Message sending does not require scheduling.

1. `GET /api/channels` lists `current` and named `targets`. Use these returned
   platform IDs; do not guess IDs or claim a target is available without checking.
2. `POST /api/messages` with `{"text":"Task finished"}` sends to the current
   conversation. Scheduled tasks default to their originating conversation.
3. To select a destination, include both `channel` and `target`, plus optional
   `thread`: `{"channel":"slack","target":"C123456","text":"Build passed"}`.
   A returned target with a `thread` is limited to that thread. A configured
   target without a thread allows the channel and its threads. The current
   conversation alone does not grant access to its parent channel or siblings.

`channel` supports `slack`, `telegram`, `qq`, `web`, and `tui`. QQ targets are
`c2c:<openid>` or `group:<group_openid>`. Web/terminal targets are session-key
suffixes such as `user:local:my-session`. QQ, Web, and terminal targets cannot
have a thread. Only the current conversation and configured targets are allowed.
All enabled agent sessions share the configured outbound grants.

Text must be nonblank and at most 32768 UTF-8 bytes. Channels use their existing
formatting (Slack/Telegram Markdown, QQ text). Save the JSON to a file and send:

```sh
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $AGENT_ROUTER_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @message.json "$AGENT_ROUTER_API_URL/api/messages"
```

On Windows, use `Invoke-RestMethod` and `$env:AGENT_ROUTER_API_URL` /
`$env:AGENT_ROUTER_API_TOKEN` with a serialized object or JSON file.

A successful send returns `delivery: "accepted"`, `target`, and `recorded`.
Acceptance does not mean the user read the message. Current-conversation and
Web/terminal sends are recorded in the destination transcript; external
cross-channel sends are not. Web refreshes notifications after its active turn;
terminal notifications appear while the terminal is open.

403 means the destination is not granted; 503 means its channel is disabled.
502 means delivery failed or is uncertain, possibly after partial delivery;
`recorded` says whether history was already saved. Do not automatically retry
a failed/interrupted request or claim successful delivery. QQ proactive-message
permissions and quota still apply. If the needed target is absent, ask the user
to configure `agent_api.message_targets` rather than bypassing the restriction.

## API

All paths below are relative to `AGENT_ROUTER_API_URL`. JSON requests use
`Content-Type: application/json`.

| Method and path | Purpose |
| --- | --- |
| `GET /api/context` | Current session, default executor, configured executors, and server time (`now_ms`) |
| `GET /api/channels` | Current destination and named, enabled outbound targets |
| `POST /api/messages` | Send text to the current conversation or a configured target |
| `GET /api/skills` | List skills shared by all sessions on this Router |
| `POST /api/skills` | Publish a complete new shared skill |
| `GET /api/skills/{name}` | Read a shared skill and its packaged files |
| `GET /api/schedules/tasks` | List this conversation's tasks, next run times, and recent execution records |
| `POST /api/schedules/tasks` | Create a task |
| `GET /api/schedules/tasks/{id}` | Inspect a task and its last 20 runs |
| `PATCH /api/schedules/tasks/{id}` | Change `name`, `prompt`, `schedule`, `executor`, `notify`, or `enabled` |
| `DELETE /api/schedules/tasks/{id}` | Delete a task |
| `POST /api/schedules/tasks/{id}/run` | Queue one immediate run, including for a paused task |

## Publish shared skills

Publish a skill when the user wants to share a reusable workflow with other
sessions on this Router. The library persists across `/new` and Router restarts.
Local and SSH executors receive published skills before their next turn; the
turn's skill index gives local paths for reading them. To discover a skill
published during the current turn, use `GET /api/skills` and read it with
`GET /api/skills/{name}`; its local directory may not exist until the next turn.

Prepare a directory named after the skill, with `SKILL.md` containing YAML
frontmatter `name` and `description`. Names use lowercase letters, digits and
hyphens, up to 64 characters. Include only supporting files the workflow needs,
such as `scripts/`, `references/`, and `assets/`. Do not bundle credentials or
conversation-specific files. The built-in `agent-router` skill is reserved.

Publish JSON with `name` and `files`. Each file is keyed by its relative path
and has `content_base64` plus an optional `executable` boolean. This carries
binary assets as well as text, and preserves executable scripts on Unix.
Packages allow up to 64 files and 1 MiB of decoded content; HTTP bodies are
limited to 2 MiB. Paths must be portable relative paths without symlinks,
parent traversal, Windows reserved names, or case-insensitive collisions.

For example, package a prepared directory with Python 3:

```sh
python3 - ./my-skill > publish-skill.json <<'PY'
import base64, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
files = {}
for path in sorted(root.rglob('*')):
    if path.is_symlink():
        raise SystemExit(f'Symlinks cannot be published: {path}')
    if path.is_file():
        files[path.relative_to(root).as_posix()] = {
            'content_base64': base64.b64encode(path.read_bytes()).decode('ascii'),
            'executable': bool(path.stat().st_mode & 0o111),
        }
json.dump({'name': root.name, 'files': files}, sys.stdout)
PY
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $AGENT_ROUTER_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @publish-skill.json "$AGENT_ROUTER_API_URL/api/skills"
```

On Windows, use the same JSON schema with a file packer and
`Invoke-RestMethod`. A 201 response means the package was saved. A 409 means
the name already exists; inspect it before choosing a different name. This
API supports creation, listing and reading only. It does not overwrite or
delete existing skills. Treat installed shared skill directories as read-only;
publish changes under a new name when appropriate.

## Create scheduled tasks

Create a scheduled task with `name`, `prompt`, and `schedule`. `executor` is optional and defaults
to the current agent. Task ownership and notification destination come from the
session credential; do not supply session IDs or channel targets.
`notify` decides which run results are published to that destination: `always`
(the default) publishes every run, `on_failure` publishes failed runs only, and
`never` publishes nothing. Use `on_failure` or `never` for a frequent task that
already sends its own progress messages, so routine runs stay quiet; every run
still records its status and output for `GET /api/schedules/tasks/{id}`.
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

## Scheduled execution behavior

- Write a self-contained prompt with relevant URLs, absolute project paths,
  intended work, and stopping conditions. Each task has a separate conversation
  and workspace; it does not inherit the original chat or its synced files.
- The selected executor is fixed until the task is updated. Switching agents in
  the original conversation does not change existing tasks.
- Results return to the originating conversation, subject to the task's `notify`
  policy. Normal executor permission
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
