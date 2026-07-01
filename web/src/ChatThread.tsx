import {
  AuiIf,
  ComposerPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  useAuiState,
} from "@assistant-ui/react";
import {
  ArrowDown,
  ArrowUp,
  Circle,
  Loader2,
  Plus,
  Square,
  Terminal,
} from "lucide-react";

import { useChatContext } from "./ChatContext";
import { MarkdownText } from "./MarkdownText";
import type {
  ActivityAttention,
  ActivityCount,
  ActivityKind,
  ActivitySnapshot,
  ChatActivity,
  ChatMessage,
} from "./types";

export function ChatShell() {
  const {
    sessions,
    currentSessionId,
    authToken,
    authRequired,
    connectionLabel,
    isRunning,
    createSession,
    selectSession,
    updateAuthToken,
  } = useChatContext();

  return (
    <div className="app-shell">
      <aside className="session-rail">
        <div className="rail-header">
          <div className="brand-lockup">
            <Terminal aria-hidden="true" size={20} />
            <span>Agent Router</span>
          </div>
          <button
            className="icon-button"
            type="button"
            title="New chat"
            aria-label="New chat"
            disabled={isRunning}
            onClick={createSession}
          >
            <Plus size={18} />
          </button>
        </div>
        <nav className="session-list" aria-label="Sessions">
          {sessions.map((session) => (
            <button
              key={session.id}
              className={
                session.id === currentSessionId
                  ? "session-item is-active"
                  : "session-item"
              }
              type="button"
              disabled={isRunning}
              onClick={() => selectSession(session.id)}
            >
              <span>{session.title}</span>
            </button>
          ))}
        </nav>
      </aside>

      <main className="chat-panel">
        <header className="top-bar">
          <div className="status-pill">
            <Circle size={10} aria-hidden="true" />
            <span>{connectionLabel}</span>
          </div>
          {(authRequired || authToken) && (
            <input
              className="token-input"
              type="password"
              value={authToken}
              placeholder="Bearer token"
              onChange={(event) => updateAuthToken(event.target.value)}
              aria-label="Bearer token"
            />
          )}
        </header>
        <ThreadView />
      </main>
    </div>
  );
}

function ThreadView() {
  return (
    <ThreadPrimitive.Root className="thread-root">
      <ThreadPrimitive.Viewport className="thread-viewport">
        <div className="message-stack">
          <ThreadPrimitive.Messages components={{ Message: ThreadMessage }} />
        </div>
        <div className="viewport-spacer" />
        <ThreadPrimitive.ScrollToBottom asChild>
          <button
            className="scroll-button"
            type="button"
            title="Scroll to bottom"
            aria-label="Scroll to bottom"
          >
            <ArrowDown size={16} />
          </button>
        </ThreadPrimitive.ScrollToBottom>
        <ThreadPrimitive.ViewportFooter className="viewport-footer">
          <RunningActivityPanel />
          <Composer />
        </ThreadPrimitive.ViewportFooter>
      </ThreadPrimitive.Viewport>
    </ThreadPrimitive.Root>
  );
}

function ThreadMessage() {
  const role = useAuiState((state) => state.message.role);
  return role === "user" ? <UserMessage /> : <AssistantMessage />;
}

function UserMessage() {
  return (
    <MessagePrimitive.Root className="message-row user-row">
      <div className="message-bubble user-bubble">
        <MessagePrimitive.Parts>
          {({ part }) =>
            part.type === "text" ? (
              <span className="plain-text">{part.text}</span>
            ) : null
          }
        </MessagePrimitive.Parts>
      </div>
    </MessagePrimitive.Root>
  );
}

function AssistantMessage() {
  return (
    <MessagePrimitive.Root className="message-row assistant-row">
      <div className="assistant-gutter">
        <Terminal size={16} aria-hidden="true" />
      </div>
      <div className="assistant-body">
        <div className="assistant-text">
          <MessagePrimitive.Parts>
            {({ part }) => (part.type === "text" ? <MarkdownText /> : null)}
          </MessagePrimitive.Parts>
        </div>
      </div>
    </MessagePrimitive.Root>
  );
}

function RunningActivityPanel() {
  const { messages, isRunning } = useChatContext();
  const message = isRunning ? currentStreamingAssistant(messages) : undefined;
  if (!message) return null;

  if (message.activitySnapshot) {
    return <ActivitySnapshotPanel snapshot={message.activitySnapshot} />;
  }

  const activities = message.activities ?? [];
  if (!activities.length) return null;

  return (
    <div className="activity-panel verbose" role="status" aria-live="polite">
      <ActivityRows activities={activities} />
    </div>
  );
}

function ActivitySnapshotPanel({ snapshot }: { snapshot: ActivitySnapshot }) {
  return (
    <div className="activity-panel compact" role="status" aria-live="polite">
      <div className="activity-panel-header">
        <span className="activity-panel-title">Activity</span>
        <span className="activity-panel-executor">{snapshot.executor}</span>
      </div>
      {snapshot.latest_reasoning ? (
        <div className="activity-reasoning">
          <span>Reasoning</span>
          <strong>{snapshot.latest_reasoning}</strong>
        </div>
      ) : null}
      <ActivityCountSection
        className="commands"
        code
        items={snapshot.commands}
        label="Commands"
        remaining={snapshot.command_remaining}
      />
      <ActivityCountSection
        className="tools"
        items={snapshot.tools}
        label="Tools"
        remaining={snapshot.tool_remaining}
      />
      {snapshot.attention.length ? (
        <div className="activity-section attention">
          <div className="activity-section-label">Attention</div>
          <div className="activity-attention-list">
            {snapshot.attention.map((item, index) => (
              <ActivityAttentionItem
                item={item}
                key={`${index}:${item.label}:${item.status}`}
              />
            ))}
            {snapshot.attention_remaining > 0 ? (
              <span className="activity-more">+{snapshot.attention_remaining}</span>
            ) : null}
          </div>
        </div>
      ) : null}
      {snapshot.progress.length ? (
        <div className="activity-section progress">
          <div className="activity-section-label">Progress</div>
          <ol className="activity-progress-list">
            {snapshot.progress_omitted > 0 ? (
              <li className="activity-progress-muted">
                {snapshot.progress_omitted} earlier
              </li>
            ) : null}
            {snapshot.progress.map((item, index) => (
              <li key={`${index}:${item}`}>{item}</li>
            ))}
          </ol>
        </div>
      ) : null}
    </div>
  );
}

function ActivityCountSection({
  className,
  code = false,
  items,
  label,
  remaining,
}: {
  className: string;
  code?: boolean;
  items: ActivityCount[];
  label: string;
  remaining: number;
}) {
  if (!items.length) return null;

  return (
    <div className={`activity-section ${className}`}>
      <div className="activity-section-label">{label}</div>
      <div className="activity-chip-list">
        {items.map((item) => (
          <span className="activity-chip" key={item.label}>
            {code ? <code>{item.label}</code> : <span>{item.label}</span>}
            {item.count > 1 ? <span>x{item.count}</span> : null}
          </span>
        ))}
        {remaining > 0 ? <span className="activity-more">+{remaining}</span> : null}
      </div>
    </div>
  );
}

function ActivityAttentionItem({ item }: { item: ActivityAttention }) {
  return (
    <span className="activity-attention-item">
      {item.code ? <code>{item.label}</code> : <span>{item.label}</span>}
      <strong>{item.status}</strong>
    </span>
  );
}

function ActivityRows({ activities }: { activities: ChatActivity[] }) {
  return (
    <div className="activity-list">
      {activities.slice(-6).map((activity) => (
        <div className="activity-item" key={activity.id}>
          <span className={`activity-kind ${activity.kind}`}>
            {activityLabel(activity.kind)}
          </span>
          <span className="activity-executor">{activity.executor}</span>
          <span className="activity-title">{activity.title}</span>
          <span className="activity-text">{activity.text}</span>
        </div>
      ))}
    </div>
  );
}

function currentStreamingAssistant(messages: ChatMessage[]) {
  return messages
    .slice()
    .reverse()
    .find(
      (message) => message.role === "assistant" && message.status === "streaming",
    );
}

function Composer() {
  return (
    <ComposerPrimitive.Root className="composer-root">
      <ComposerPrimitive.Input
        className="composer-input"
        placeholder="Message Agent Router"
        rows={1}
      />
      <AuiIf condition={(state) => !state.thread.isRunning}>
        <ComposerPrimitive.Send asChild>
          <button
            className="send-button"
            type="button"
            title="Send"
            aria-label="Send"
          >
            <ArrowUp size={18} />
          </button>
        </ComposerPrimitive.Send>
      </AuiIf>
      <AuiIf condition={(state) => state.thread.isRunning}>
        <ComposerPrimitive.Cancel asChild>
          <button
            className="send-button cancel"
            type="button"
            title="Cancel"
            aria-label="Cancel"
          >
            <Square size={14} />
          </button>
        </ComposerPrimitive.Cancel>
      </AuiIf>
      <AuiIf condition={(state) => state.thread.isRunning}>
        <Loader2 className="composer-spinner" size={16} aria-hidden="true" />
      </AuiIf>
    </ComposerPrimitive.Root>
  );
}

function activityLabel(kind: ActivityKind): string {
  switch (kind) {
    case "agent_progress":
      return "Progress";
    case "reasoning_summary":
      return "Reasoning";
    case "tool_call":
      return "Tool";
  }
}
