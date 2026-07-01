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
import type { ActivityKind } from "./types";

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
  const messageId = useAuiState((state) => state.message.id);
  return (
    <MessagePrimitive.Root className="message-row assistant-row">
      <div className="assistant-gutter">
        <Terminal size={16} aria-hidden="true" />
      </div>
      <div className="assistant-body">
        <ActivityList messageId={messageId} />
        <div className="assistant-text">
          <MessagePrimitive.Parts>
            {({ part }) => (part.type === "text" ? <MarkdownText /> : null)}
          </MessagePrimitive.Parts>
        </div>
      </div>
    </MessagePrimitive.Root>
  );
}

function ActivityList({ messageId }: { messageId: string }) {
  const { messages } = useChatContext();
  const message = messages.find((item) => item.id === messageId);
  const activities = message?.activities ?? [];
  if (!activities.length) return null;

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
