#!/bin/sh
# Fake Claude Code CLI for integration tests.
read -r _first_line
cat <<'JSON'
{"type":"system","session_id":"fake-sid-123","model":"claude-fake"}
{"type":"assistant","message":{"content":[{"type":"text","text":"Hello from fake Claude"}]}}
{"type":"result","result":"Hello from fake Claude"}
JSON
